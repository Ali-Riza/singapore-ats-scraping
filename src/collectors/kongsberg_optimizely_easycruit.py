from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

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


def _normalize_url(base_url: str, href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""

    abs_url = urljoin(base_url.rstrip("/") + "/", href)
    parsed = urlparse(abs_url)
    if parsed.scheme not in ("http", "https"):
        return ""
    if parsed.netloc and parsed.netloc not in (urlparse(base_url).netloc,):
        return ""
    return parsed._replace(fragment="").geturl()


def _extract_listing_items_from_html(listing_html: str, base_url: str) -> List[Dict[str, str]]:
    # Embedded JSON-like records, faster than detail fetching.
    pattern = re.compile(
        r'{"id":"(?P<id>\d+)","title":"(?P<title>[^"]*)","location":"(?P<location>[^"]*)","detailsUrl":"(?P<detailsUrl>[^"]+)"'
    )

    items: List[Dict[str, str]] = []

    for m in pattern.finditer(listing_html or ""):
        details_url = (m.group("detailsUrl") or "").strip()
        if "/careers/vacancies/" not in details_url:
            continue

        job_url = _normalize_url(base_url, details_url)
        if not job_url:
            continue

        items.append(
            {
                "job_id": (m.group("id") or "").strip(),
                "job_title": _clean_text(m.group("title")),
                "location": _clean_text(m.group("location")),
                "job_url": job_url,
            }
        )

    # Fallback: link extraction if regex fails
    if not items:
        soup = BeautifulSoup(listing_html or "", "html.parser")
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if "/careers/vacancies/" not in href:
                continue
            job_url = _normalize_url(base_url, href)
            if not job_url:
                continue
            items.append(
                {
                    "job_id": "",
                    "job_title": _clean_text(a.get_text(" ", strip=True)),
                    "location": "",
                    "job_url": job_url,
                }
            )

    # De-dupe by URL
    seen: set[str] = set()
    uniq: List[Dict[str, str]] = []
    for it in items:
        u = it.get("job_url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(it)

    return uniq


def _looks_singapore(loc: str) -> bool:
    return "singapore" in (loc or "").casefold()


class KongsbergOptimizelyEasycruitCollector(BaseCollector):
    name = "kongsberg_optimizely_easycruit"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None}

        try:
            careers_url = company.careers_url
            base_url = _clean_text(f"{urlparse(careers_url).scheme}://{urlparse(careers_url).netloc}")

            with _make_session() as session:
                r = session.get(careers_url, timeout=30)
                meta["status"] = r.status_code
                r.raise_for_status()

                items = _extract_listing_items_from_html(r.text, base_url)
                raw_jobs = [it for it in items if _looks_singapore(it.get("location") or "")]

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

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=_clean_text(raw.get("job_title")),
                    location=_clean_text(raw.get("location")),
                    job_id=_clean_text(raw.get("job_id")) or _clean_text(urlparse(str(raw.get("job_url") or "")).path.split("/")[-1]),
                    posted_date="",
                    job_url=_clean_text(raw.get("job_url")),
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return out
