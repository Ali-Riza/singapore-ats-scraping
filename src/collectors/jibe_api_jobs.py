from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-GB,en;q=0.9",
}


def _normalize_date(raw: Any) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""

    # API returns ISO-ish strings like:
    # - 2025-11-25T06:13:00+0000
    # - 2025-11-25T06:13:00Z
    try:
        s2 = s.replace("Z", "+00:00")
        # Handle +0000 without colon
        if len(s2) >= 5 and (s2[-5] in {"+", "-"}) and s2[-3] != ":":
            s2 = s2[:-5] + s2[-5:-2] + ":" + s2[-2:]
        dt = datetime.fromisoformat(s2)
        return dt.date().isoformat()
    except Exception:
        pass

    for fmt in ("%Y-%m-%d", "%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(s[:30], fmt).date().isoformat()
        except Exception:
            continue

    # last-resort: return raw string (keeps some signal)
    return s


def _listing_url_to_api_params(careers_url: str) -> Dict[str, str]:
    u = urlparse(careers_url)
    qs = parse_qs(u.query, keep_blank_values=True)
    params: Dict[str, str] = {}
    for k, vs in qs.items():
        if not vs:
            continue
        params[k] = vs[0]

    # Default to external search if not explicitly internal.
    params.setdefault("internal", "false")
    return params


def _canonicalize_careers_url(careers_url: str) -> str:
    """Best-effort normalize listing URLs for known tenants.

    For Schneider Electric (careers.se.com), the listing page supports a
    `country=Singapore` query param that impacts filtering/consistency. Ensure
    it's present when missing.
    """

    u = urlparse(careers_url)
    if not u.scheme or not u.netloc:
        return careers_url

    # Schneider Electric: ensure country=Singapore is present.
    if u.netloc.casefold() == "careers.se.com" and (u.path or "").rstrip("/") == "/jobs":
        qsl = parse_qsl(u.query, keep_blank_values=True)
        has_country = any(k.casefold() == "country" for k, _ in qsl)
        if not has_country:
            qsl.append(("country", "Singapore"))
            return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(qsl), u.fragment))

    return careers_url


def _api_base(careers_url: str) -> str:
    u = urlparse(careers_url)
    return f"{u.scheme}://{u.netloc}"


def _location_from_api(job: Dict[str, Any]) -> str:
    for k in ("full_location", "short_location", "location_name"):
        v = job.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    country = job.get("country")
    return country.strip() if isinstance(country, str) and country.strip() else ""


def _normalize_location_for_company(company_name: str, location: str) -> str:
    """Company-specific normalization to keep downstream CSV/Excel stable."""

    c = (company_name or "").strip().casefold()
    loc = (location or "").strip()
    if not loc:
        return ""

    # Schneider Electric results often come back as multi-location strings like
    # "Singapore; Indonesia; ..." which can cause Excel to mis-parse columns when
    # opening CSVs in locales expecting semicolon-separated values.
    if c == "schneider electric" and "singapore" in loc.casefold():
        return "Singapore"

    return loc


def _posted_date_from_api(job: Dict[str, Any]) -> str:
    md = job.get("meta_data") or {}
    icims = md.get("icims") if isinstance(md, dict) else None
    primary = icims.get("primary_posted_site_object") if isinstance(icims, dict) else None
    if isinstance(primary, dict) and primary.get("datePosted"):
        return _normalize_date(primary.get("datePosted"))
    return _normalize_date(job.get("posted_date"))


def _job_url_from_api(job: Dict[str, Any], careers_url: str) -> str:
    md = job.get("meta_data") or {}
    if isinstance(md, dict):
        url = md.get("canonical_url")
        if isinstance(url, str) and url.strip():
            return url.strip()

    slug = job.get("slug") or job.get("req_id")
    if slug:
        base = _api_base(careers_url)
        lang = job.get("language") or "en-us"
        return f"{base}/jobs/{slug}?lang={lang}"

    return careers_url


class JibeApiJobsCollector(BaseCollector):
    """Collector for Jibe search apps exposing a JSON endpoint at /api/jobs.

    Observed on:
    - careers.se.com
    - careers.msasafety.com

    API shape:
    - GET /api/jobs?page=N&... -> { jobs: [ { data: {...} }, ... ], totalCount: int }
    """

    name = "jibe_api_jobs"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        max_pages = 50
        raw_jobs: List[Dict[str, Any]] = []

        meta: Dict[str, Any] = {
            "pages": 0,
            "status_codes": [],
            "totalCount": None,
            "api_url": None,
        }

        careers_url = company.careers_url

        try:
            careers_url = _canonicalize_careers_url(company.careers_url)
            api_url = f"{_api_base(careers_url)}/api/jobs"
            meta["api_url"] = api_url

            params = _listing_url_to_api_params(careers_url)

            session = requests.Session()
            session.headers.update(_DEFAULT_HEADERS)

            seen_ids: set[str] = set()
            total_count: Optional[int] = None

            for page_num in range(1, max_pages + 1):
                params["page"] = str(page_num)
                r = session.get(api_url, params=params, timeout=30)
                meta["pages"] += 1
                meta["status_codes"].append(r.status_code)
                r.raise_for_status()

                payload = r.json()

                if total_count is None:
                    tc = payload.get("totalCount")
                    if isinstance(tc, int):
                        total_count = tc
                        meta["totalCount"] = tc

                jobs = payload.get("jobs") or []
                if not jobs:
                    break

                page_added = 0
                for item in jobs:
                    job = item.get("data") if isinstance(item, dict) else None
                    if not isinstance(job, dict):
                        continue

                    job_id = str(job.get("req_id") or job.get("slug") or "").strip()
                    key = job_id or _job_url_from_api(job, careers_url)
                    if not key or key in seen_ids:
                        continue
                    seen_ids.add(key)

                    raw_jobs.append(job)
                    page_added += 1

                if page_added == 0:
                    break

                if total_count is not None and len(raw_jobs) >= total_count:
                    break

            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )

        except Exception as e:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(e),
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        out: List[JobRecord] = []
        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            job_id = str(raw.get("req_id") or raw.get("slug") or "").strip()
            title = str(raw.get("title") or "").strip()
            location = _normalize_location_for_company(result.company, _location_from_api(raw))
            posted_date = _posted_date_from_api(raw)
            job_url = _job_url_from_api(raw, result.careers_url)

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
