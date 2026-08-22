from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse, urlsplit

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _base_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def _normalize_date(raw: Any) -> str:
    txt = _clean(raw)
    if not txt:
        return ""
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(txt, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    if "T" in txt:
        return txt[:10]
    return txt[:10]


def _company_id_from_url(careers_url: str) -> Optional[str]:
    """Read the tenant id straight off the careers URL.

    careers.smartrecruiters.com/Vitol -> Vitol
    jobs.smartrecruiters.com/Vitol/744000... -> Vitol
    """
    parsed = urlsplit(careers_url)
    if "smartrecruiters.com" not in (parsed.netloc or "").lower():
        return None
    segments = [s for s in (parsed.path or "").split("/") if s]
    if not segments:
        return None
    first = _clean(segments[0])
    # Skip locale/utility prefixes that precede the tenant slug.
    if first.lower() in {"jobs", "careers", "en", "en-us", "companies"} and len(segments) > 1:
        first = _clean(segments[1])
    return first or None


def _company_id_from_html(html: str) -> Optional[str]:
    for pattern in (
        r'data-company-identifier\s*=\s*["\']([^"\']+)',
        r'companyIdentifier\s*[:=]\s*["\']([^"\']+)',
        r'dcr_ci\s*=\s*["\']?([^"\'\s&]+)',
    ):
        match = re.search(pattern, html, flags=re.I)
        if match:
            value = _clean(match.group(1))
            if value:
                return value
    return None


def _decode_job_id(raw_url: str) -> str:
    parsed = urlsplit(raw_url)
    q = parse_qs(parsed.query)
    for key in ("id", "jobId", "job_id", "requisitionId"):
        values = q.get(key)
        if values:
            return _clean(values[0])
    candidate = parsed.path.rstrip("/").rsplit("/", 1)[-1]
    return _clean(candidate)


SG_COUNTRY_CODE = "sg"


def _extract_work_location(additional_text: str) -> str:
    """Pull the location out of a listing's ".c-list__additional" text.

    Never invent a location: an unparseable value returns "" so downstream code can
    drop the row instead of asserting a country the source never stated.
    """
    text = _clean(additional_text)
    if not text:
        return ""
    parts = [part.strip() for part in re.split(r"\s-\s", text) if part.strip()]
    if not parts:
        return ""
    return parts[-1]


def _location_from_api(location_obj: Any) -> str:
    """Build a readable location from the v1 postings `location` object."""
    if not isinstance(location_obj, dict):
        return _clean(location_obj)

    full = _clean(location_obj.get("fullLocation"))
    if full:
        # v1 emits "Singapore, , Singapore" when region is blank.
        return ", ".join(p for p in (x.strip() for x in full.split(",")) if p)

    parts = [
        _clean(location_obj.get("city")),
        _clean(location_obj.get("region")),
        _clean(location_obj.get("country")).upper(),
    ]
    return ", ".join(p for p in parts if p and p.upper() != "NULL")


def _is_singapore_location(location_obj: Any, location_text: str = "") -> bool:
    """True only when the source itself says Singapore."""
    if isinstance(location_obj, dict):
        country = _clean(location_obj.get("country")).lower()
        if country:
            return country in {SG_COUNTRY_CODE, "singapore"}
    return "singapore" in (location_text or "").lower()


class SmartRecruitersApiCollector(BaseCollector):
    name = "smartrecruiters_api"

    def _fetch_html(self, url: str, headers: Dict[str, str]) -> str:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text

    def _parse_api_items(self, payload: Any, company_id: str = "") -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            container = payload.get("content")
            if isinstance(container, list):
                items = container
            else:
                for key in ("data", "postings", "jobs", "positions"):
                    value = payload.get(key)
                    if isinstance(value, list):
                        items = value
                        break
        elif isinstance(payload, list):
            items = payload

        parsed: List[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            title = _clean(item.get("title") or item.get("name") or item.get("jobTitle"))
            if not title:
                continue
            location_obj = item.get("location")
            location = _location_from_api(location_obj)
            if not location:
                location = _clean(item.get("country") or item.get("city") or item.get("locationName"))

            # The source is authoritative: skip anything not in Singapore rather than
            # relabelling it (this is what produced Brazil/Texas rows tagged "Singapore").
            if not _is_singapore_location(location_obj, location):
                continue

            job_id = _clean(item.get("id") or item.get("jobId") or item.get("requisitionId"))
            if not job_id:
                job_id = _clean(item.get("ref") or item.get("postingId"))
            url = _clean(item.get("url") or item.get("applyUrl") or item.get("postingUrl") or item.get("jobUrl") or "")
            if not url:
                url = _clean(item.get("contentHtmlUrl") or item.get("positionUrl") or "")
            if not url and company_id and job_id:
                # v1 omits the public URL; it follows a stable pattern.
                url = f"https://jobs.smartrecruiters.com/{company_id}/{job_id}"
            created_date = _clean(
                item.get("releasedDate")
                or item.get("createdOn")
                or item.get("datePosted")
                or item.get("publishedOn")
            )
            parsed.append(
                {
                    "job_title": title,
                    "location": location,
                    "job_id": job_id,
                    "posted_date": _normalize_date(created_date),
                    "job_url": url,
                    "source": "smartrecruiters_api",
                }
            )
        return parsed

    def _parse_listing_html(self, html: str, base_url: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        jobs: List[Dict[str, Any]] = []

        item_candidates = soup.select("li.c-list__item, li.js-filters-content-item, .job-card, .c-list__item")
        if item_candidates:
            for item in item_candidates:
                link = item.select_one("a")
                if not link:
                    continue
                href = _clean(link.get("href"))
                if not href:
                    continue
                url = href if href.startswith("http") else urljoin(base_url, href)
                title = _clean(link.get_text(" ", strip=True))
                if not title:
                    continue
                detail = item.select_one(".c-list__additional")
                location = _extract_work_location(detail.get_text(" ", strip=True) if detail else "")
                if not _is_singapore_location(None, location):
                    continue
                job_id = _decode_job_id(url)
                jobs.append(
                    {
                        "job_title": title,
                        "location": location,
                        "job_id": job_id,
                        "posted_date": "",
                        "job_url": url,
                    }
                )
            if jobs:
                return jobs

        # Bare-anchor fallback: these carry no location markup at all, so we cannot
        # claim Singapore. Emit an empty location and let validation flag it.
        anchors = soup.select("a[href]")
        seen = set()
        for a in anchors:
            href = _clean(a.get("href"))
            if not href or "/job-detail" not in href and "job" not in href.lower():
                continue
            url = href if href.startswith("http") else urljoin(base_url, href)
            if url in seen:
                continue
            seen.add(url)
            title = _clean(a.get_text(" ", strip=True))
            if not title:
                continue
            jobs.append(
                {
                    "job_title": title,
                    "location": "",
                    "job_id": _decode_job_id(url),
                    "posted_date": "",
                    "job_url": url,
                }
            )
        return jobs

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        careers_url = _clean(company.careers_url)
        if not careers_url:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=[],
                meta={"error": "missing careers_url"},
                error="missing careers_url",
            )

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0 Safari/537.36"
            ),
            "Accept": "application/json, text/html, */*;q=0.8",
            "Accept-Language": "en-SG,en;q=0.9",
            "Referer": careers_url,
        }

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"page_url": careers_url, "company_identifier": None, "api_url": None, "used_fallback": False}

        try:
            html = self._fetch_html(careers_url, headers)
            company_id = _company_id_from_html(html) or _company_id_from_url(careers_url) or ""
            meta["company_identifier"] = company_id

            if company_id:
                # v1 is the public postings API; the unversioned path 404s.
                api_url = f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings"
                meta["api_url"] = api_url

                limit = 100
                offset = 0
                for _ in range(0, 20):
                    response = requests.get(
                        api_url,
                        # `country` is a real server-side filter here, so let the API
                        # do the work instead of pulling every posting worldwide.
                        params={"limit": limit, "offset": offset, "country": SG_COUNTRY_CODE},
                        headers={**headers, "Accept": "application/json"},
                        timeout=30,
                    )
                    meta.setdefault("status_codes", []).append(response.status_code)
                    if response.status_code >= 400:
                        break
                    try:
                        payload = response.json()
                    except ValueError:
                        break

                    returned = payload.get("content") if isinstance(payload, dict) else None
                    returned_count = len(returned) if isinstance(returned, list) else 0
                    if isinstance(payload, dict) and payload.get("totalFound") is not None:
                        meta["total_found"] = payload.get("totalFound")

                    raw_jobs.extend(self._parse_api_items(payload, company_id))

                    if returned_count < limit:
                        break
                    offset += returned_count
                if raw_jobs:
                    meta["count"] = len(raw_jobs)
                    return CollectResult(
                        collector=self.name,
                        company=company.company,
                        careers_url=company.careers_url,
                        raw_jobs=raw_jobs,
                        meta=meta,
                        error=None,
                    )

            # Fallback: parse the rendered job list from the public HTML page.
            parsed_html_jobs = self._parse_listing_html(html, _base_url(careers_url) or careers_url)
            meta["used_fallback"] = True
            if parsed_html_jobs:
                raw_jobs = parsed_html_jobs
            meta["count"] = len(raw_jobs)
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )
        except Exception as exc:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(exc),
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        out: List[JobRecord] = []
        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue
            title = _clean(raw.get("job_title") or raw.get("title"))
            if not title:
                continue
            location = _clean(raw.get("location") or raw.get("country"))
            job_id = _clean(raw.get("job_id") or raw.get("id") or raw.get("requisition_id") or raw.get("job_url") or title)
            job_url = _clean(raw.get("job_url") or raw.get("url") or result.careers_url)
            posted_date = _normalize_date(raw.get("posted_date") or raw.get("created_date") or raw.get("date_posted") or "")
            out.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=location,
                    job_id=job_id,
                    posted_date=posted_date,
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return out
