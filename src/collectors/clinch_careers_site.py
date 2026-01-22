from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple
from urllib.parse import urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _make_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return s


def _base_from_url(url: str) -> str:
    u = urlparse(url)
    if not u.scheme or not u.netloc:
        return url.rstrip("/")
    return f"{u.scheme}://{u.netloc}".rstrip("/")


def _build_search_url(base: str, query: str, country_code: str, page: int) -> str:
    params = [("page", str(page)), ("query", query), ("country_codes[]", country_code)]
    return f"{base}/jobs/search?{urlencode(params)}"


def _normalize_url(base: str, href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    abs_url = urljoin(base + "/", href)
    parsed = urlparse(abs_url)
    if parsed.scheme not in ("http", "https"):
        return ""
    return parsed._replace(fragment="").geturl()


def _extract_search_rows(base: str, html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []

    for tr in soup.select("table.table tbody tr"):
        a = tr.select_one("td.job-search-results-title a[href]")
        if not a:
            continue

        job_url = _normalize_url(base, a.get("href") or "")
        title = _clean_text(a.get_text(" ", strip=True))

        locs = [
            _clean_text(li.get_text(" ", strip=True))
            for li in tr.select("td.job-search-results-location li")
            if _clean_text(li.get_text(" ", strip=True))
        ]

        if job_url:
            out.append({"job_url": job_url, "job_title": title, "locations": locs})

    # de-dupe by URL
    seen: set[str] = set()
    uniq: List[Dict[str, Any]] = []
    for it in out:
        u = it.get("job_url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(it)
    return uniq


def _extract_search_rows_markdown(base: str, markdown: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for line in markdown.splitlines():
        line = line.strip()
        if not line.startswith("| ["):
            continue

        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) < 4:
            continue

        match = re.search(r"\[([^\]]+)\]\(([^)]+)\)", cells[0])
        if not match:
            continue

        job_url = _normalize_url(base, match.group(2))
        if not job_url:
            continue

        title = _clean_text(match.group(1))

        location_field = cells[3]
        loc_candidates = re.findall(r"\*([^*]+)", location_field)
        if not loc_candidates:
            loc_candidates = [location_field]

        locations = [
            _clean_text(part)
            for part in loc_candidates
            if _clean_text(part)
        ]

        rows.append({
            "job_url": job_url,
            "job_title": title,
            "locations": locations,
        })

    seen: set[str] = set()
    uniq: List[Dict[str, Any]] = []
    for it in rows:
        u = it.get("job_url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(it)

    return uniq


def _job_uid_from_detail(detail_html: str) -> str:
    m = re.search(r"job_uid=([0-9a-f]{32})", detail_html or "", flags=re.I)
    if m:
        return m.group(1)

    m = re.search(r"/me/jobs/([0-9a-f]{32})/favourites", detail_html or "", flags=re.I)
    if m:
        return m.group(1)

    return ""


def _public_uuid_from_url(job_url: str) -> str:
    m = re.search(
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
        urlparse(job_url).path,
        flags=re.I,
    )
    return m.group(1) if m else ""


def _extract_apply_url(detail_soup: BeautifulSoup, job_url: str) -> str:
    if detail_soup.select_one("a#apply"):
        return f"{job_url.rstrip('/')}#apply"

    for a in detail_soup.select("a[href]"):
        if (a.get("href") or "").strip() == "#apply":
            return f"{job_url.rstrip('/')}#apply"

    return ""


def _extract_apply_url_markdown(markdown: str, job_url: str) -> str:
    match = re.search(r"\[apply\s+now\]\(([^)]+)\)", markdown, flags=re.I)
    if not match:
        return ""

    raw_href = (match.group(1) or "").strip()
    if not raw_href:
        return ""

    if raw_href.startswith("#"):
        return f"{job_url.rstrip('/')}{raw_href}"

    absolute = urljoin(job_url.rstrip("/") + "/", raw_href)
    parsed = urlparse(absolute)
    if parsed.scheme not in ("http", "https"):
        return ""

    return parsed.geturl()


def _fetch_content(session: requests.Session, url: str, timeout: float = 30.0) -> Tuple[str, str, List[int]]:
    statuses: List[int] = []

    response = session.get(url, timeout=timeout)
    statuses.append(response.status_code)

    if response.status_code in (202, 403):
        fallback_url = f"https://r.jina.ai/{url}"
        fallback = requests.get(fallback_url, timeout=timeout)
        statuses.append(fallback.status_code)
        fallback.raise_for_status()
        return fallback.text, "markdown", statuses

    response.raise_for_status()
    return response.text, "html", statuses


class ClinchCareersSiteCollector(BaseCollector):
    name = "clinch_careers_site"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "status": []}

        try:
            base = _base_from_url(company.careers_url)
            query = "singapore"
            country_code = "SG"

            listing_items: List[Dict[str, Any]] = []

            with _make_session() as session:
                for page in range(1, 6):
                    search_url = _build_search_url(base, query, country_code, page)
                    content, mode, statuses = _fetch_content(session, search_url)
                    meta["status"].extend(statuses)

                    if mode == "html":
                        items = _extract_search_rows(base, content)
                    else:
                        items = _extract_search_rows_markdown(base, content)

                    if not items:
                        break

                    listing_items.extend(items)
                    meta["pages"] = page

                # Fetch details to extract stable IDs and apply anchors
                for item in listing_items:
                    job_url = item.get("job_url") or ""
                    if not job_url:
                        continue

                    detail_content, detail_mode, statuses = _fetch_content(session, job_url)
                    meta["status"].extend(statuses)

                    detail_html = detail_content if detail_mode == "html" else ""
                    detail_markdown = detail_content if detail_mode == "markdown" else ""

                    job_uid = _job_uid_from_detail(detail_html)
                    public_id = _public_uuid_from_url(job_url)

                    raw_jobs.append(
                        {
                            "job_url": job_url,
                            "job_title": item.get("job_title"),
                            "locations": item.get("locations") or [],
                            "job_uid": job_uid,
                            "public_id": public_id,
                            "detail_html": detail_html,
                            "detail_markdown": detail_markdown,
                            "detail_mode": detail_mode,
                        }
                    )

            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )
        except Exception as e:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(e),
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        out: List[JobRecord] = []

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            job_url = _clean_text(raw.get("job_url"))
            title = _clean_text(raw.get("job_title"))
            locations = raw.get("locations") if isinstance(raw.get("locations"), list) else []
            location = "; ".join([_clean_text(x) for x in locations if _clean_text(x)])

            detail_html = raw.get("detail_html") or ""
            soup = BeautifulSoup(detail_html, "html.parser") if detail_html else None

            job_id = _clean_text(raw.get("job_uid") or raw.get("public_id"))

            apply_url = ""
            if soup:
                apply_url = _extract_apply_url(soup, job_url)
            else:
                apply_url = _extract_apply_url_markdown(raw.get("detail_markdown") or "", job_url)

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=location,
                    job_id=job_id,
                    posted_date="",
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw={**raw, "apply_url": apply_url},
                )
            )

        return out
