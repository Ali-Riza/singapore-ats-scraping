from __future__ import annotations

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


def _job_id_from_url(job_url: str) -> str:
    try:
        path = urlparse(job_url).path
    except Exception:
        path = job_url

    path = (path or "").rstrip("/")
    if not path:
        return ""

    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2 and parts[-2] == "jobs":
        return parts[-1]
    return parts[-1]


def _extract_listing_jobs(html: str, base_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []

    for teaser in soup.select("div.c-job-teaser"):
        a = teaser.select_one(".c-job-list__td--position a[href]")
        if not a:
            continue

        job_url = urljoin(base_url + "/", (a.get("href") or "").strip())
        title = _clean_text(a.get_text(" ", strip=True))
        if not title or not job_url:
            continue

        loc_el = teaser.select_one(".c-job-list__td--location .c-job-teaser__text")
        location = _clean_text(loc_el.get_text(" ", strip=True) if loc_el else "")

        out.append(
            {
                "job_title": title,
                "job_url": job_url,
                "location": location,
                "job_id": _job_id_from_url(job_url),
            }
        )

    seen: set[str] = set()
    uniq: List[Dict[str, Any]] = []
    for j in out:
        if j["job_url"] in seen:
            continue
        seen.add(j["job_url"])
        uniq.append(j)

    return uniq


class AibelHtmlHrManagerCollector(BaseCollector):
    name = "aibel_html_hr_manager"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None}

        try:
            careers_url = company.careers_url
            parsed = urlparse(careers_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else careers_url

            with _make_session() as session:
                r = session.get(careers_url, timeout=30)
                meta["status"] = r.status_code
                r.raise_for_status()

                raw_jobs = _extract_listing_jobs(r.text, base_url)

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
        # Only include jobs with 'Singapore' in the location (case-insensitive)
        return [
            JobRecord(
                company=result.company,
                job_title=_clean_text(raw.get("job_title")),
                location=_clean_text(raw.get("location")),
                job_id=_clean_text(raw.get("job_id")),
                posted_date="",
                job_url=_clean_text(raw.get("job_url")),
                source=self.name,
                careers_url=result.careers_url,
                raw=raw,
            )
            for raw in result.raw_jobs
            if isinstance(raw, dict) and "singapore" in _clean_text(raw.get("location")).lower()
        ]
