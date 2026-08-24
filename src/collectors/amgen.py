from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CollectResult, CompanyItem, JobRecord


BASE_URL = "https://careers.amgen.com"
SINGAPORE_RESULTS_URL = f"{BASE_URL}/en/location/singapore-jobs/87/1880251-7535954-1880252/4"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _make_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _extract_job_id(job_url: str, fallback: str = "") -> str:
    if fallback:
        return fallback
    if not job_url:
        return ""
    last = job_url.rstrip("/").split("/")[-1]
    return last if last.isdigit() else ""


class AmgenCollector(BaseCollector):
    """Collector for Amgen's TalentBrew careers site (Singapore hardcoded)."""

    name = "amgen"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "status": [],
            "start_url": SINGAPORE_RESULTS_URL,
            "location": "Singapore",
        }

        try:
            next_url = SINGAPORE_RESULTS_URL
            seen_job_ids: set[str] = set()

            with _make_session() as session:
                for _ in range(10):
                    response = session.get(next_url, timeout=45)
                    meta["status"].append(response.status_code)
                    response.raise_for_status()
                    meta["pages"] += 1

                    soup = BeautifulSoup(response.text, "lxml")
                    ul = soup.find("ul", attrs={"id": "search-results-jobs"})
                    if ul is None:
                        break

                    items = ul.find_all("li", recursive=False)
                    for li in items:
                        a_tag = li.find("a", href=True)
                        if a_tag is None:
                            continue

                        href = _clean_text(a_tag.get("href"))
                        if not href:
                            continue

                        job_url = urljoin(BASE_URL, href)
                        job_id = _extract_job_id(job_url, _clean_text(a_tag.get("data-job-id")))
                        if not job_id or job_id in seen_job_ids:
                            continue

                        h3 = a_tag.find("h3")
                        title = _clean_text(h3.get_text()) if h3 else ""
                        if not title:
                            continue

                        date_span = a_tag.find("span", attrs={"class": "job-date-posted"})
                        posted_date = _clean_text(date_span.get_text()) if date_span else ""
                        posted_date = re.sub(r"\.\.+", ".", posted_date)

                        source_location = ""
                        location_spans = a_tag.find_all("span", attrs={"class": "job-location"})
                        if location_spans:
                            source_location = _clean_text(location_spans[0].get_text())

                        raw_jobs.append(
                            {
                                "job_id": job_id,
                                "title": title,
                                "job_url": job_url,
                                "posted_date": posted_date,
                                "source_location": source_location,
                            }
                        )
                        seen_job_ids.add(job_id)

                    next_link = soup.find("a", attrs={"class": "next"})
                    if next_link is None:
                        break
                    next_href = _clean_text(next_link.get("href"))
                    if not next_href:
                        break
                    next_url = urljoin(BASE_URL, next_href)

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
        records: List[JobRecord] = []

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            job_title = _clean_text(raw.get("title"))
            job_id = _clean_text(raw.get("job_id"))
            job_url = _clean_text(raw.get("job_url"))
            posted_date = _clean_text(raw.get("posted_date"))

            if not (job_title and job_id and job_url):
                continue

            records.append(
                JobRecord(
                    company=result.company,
                    job_title=job_title,
                    # Explicitly hardcoded as requested.
                    location="Singapore",
                    job_id=job_id,
                    posted_date=posted_date,
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return records
