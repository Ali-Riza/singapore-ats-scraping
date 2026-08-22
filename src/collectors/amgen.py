from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CollectResult, CompanyItem, JobRecord


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

MAX_PAGES = 10
DETAIL_WORKERS = 8


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


def _site_root(url: str) -> str:
    """Scheme + host of the company's own careers site, for resolving relative links."""
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        return ""
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


def _extract_job_id(job_url: str, fallback: str = "") -> str:
    if fallback:
        return fallback
    if not job_url:
        return ""
    last = job_url.rstrip("/").split("/")[-1]
    return last if last.isdigit() else ""


def _job_links(soup: BeautifulSoup) -> List[Any]:
    """Job anchors across the TalentBrew markup variants.

    Amgen wraps results in ul#search-results-jobs; AstraZeneca and Baxter use
    a.search-results-link; Takeda uses plain a[data-job-id] inside
    #search-results-list. Try each in turn and keep the first that matches.
    """
    for selector in (
        "ul#search-results-jobs > li > a[href]",
        "a.search-results-link[href]",
        "#search-results-list a[data-job-id][href]",
    ):
        links = soup.select(selector)
        if links:
            return links
    return []


def _total_pages(soup: BeautifulSoup) -> int:
    section = soup.find(id="search-results")
    if section is None:
        return 0
    try:
        return int(_clean_text(section.get("data-total-pages")))
    except (TypeError, ValueError):
        return 0


def _parse_listing(soup: BeautifulSoup, site_root: str) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []

    for a_tag in _job_links(soup):
        href = _clean_text(a_tag.get("href"))
        if not href:
            continue

        job_url = urljoin(site_root, href)
        job_id = _extract_job_id(job_url, _clean_text(a_tag.get("data-job-id")))
        if not job_id:
            continue

        # Amgen renders the title in <h3>, the other TalentBrew sites in <h2>.
        heading = a_tag.find(["h3", "h2"])
        title = _clean_text(heading.get_text()) if heading else ""
        if not title:
            continue

        date_span = a_tag.find("span", attrs={"class": "job-date-posted"})
        posted_date = _clean_text(date_span.get_text()) if date_span else ""
        posted_date = re.sub(r"\.\.+", ".", posted_date)

        # "job-location" on Amgen/AstraZeneca/Baxter, "location" on Takeda.
        location_span = a_tag.find("span", attrs={"class": "job-location"}) or a_tag.find(
            "span", attrs={"class": "location"}
        )
        source_location = _clean_text(location_span.get_text()) if location_span else ""

        jobs.append(
            {
                "job_id": job_id,
                "title": title,
                "job_url": job_url,
                "posted_date": posted_date,
                "source_location": source_location,
            }
        )

    return jobs


def _date_from_jsonld(html: str) -> str:
    """Read JobPosting.datePosted from the detail page's JSON-LD block.

    Only Amgen exposes a date in the results list; AstraZeneca, Takeda and
    Baxter do not, so the detail page is the only reliable source.
    """
    for match in re.finditer(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.S | re.I,
    ):
        try:
            data = json.loads(match.group(1))
        except (ValueError, TypeError):
            continue

        candidates = data if isinstance(data, list) else [data]
        for entry in candidates:
            if not isinstance(entry, dict) or entry.get("@type") != "JobPosting":
                continue
            posted = _clean_text(entry.get("datePosted"))
            if not posted:
                continue
            # TalentBrew emits non-padded values like "2026-8-20".
            iso = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", posted)
            if iso:
                year, month, day = iso.groups()
                return f"{year}-{int(month):02d}-{int(day):02d}"
            return posted
    return ""


class AmgenCollector(BaseCollector):
    """Collector for TalentBrew careers sites (Amgen, AstraZeneca, Takeda, Baxter).

    The company's own careers_url drives the scrape; nothing here is specific to
    a single company.
    """

    name = "amgen"

    def _fill_missing_dates(
        self, session: requests.Session, raw_jobs: List[Dict[str, Any]]
    ) -> int:
        pending = [job for job in raw_jobs if not job.get("posted_date") and job.get("job_url")]
        if not pending:
            return 0

        def fetch(job: Dict[str, Any]) -> None:
            try:
                response = session.get(job["job_url"], timeout=45)
                response.raise_for_status()
                job["posted_date"] = _date_from_jsonld(response.text)
            except Exception:
                job["posted_date"] = ""

        workers = min(DETAIL_WORKERS, len(pending))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(fetch, pending))

        return sum(1 for job in pending if job.get("posted_date"))

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        start_url = (company.careers_url or "").strip()
        meta: Dict[str, Any] = {
            "pages": 0,
            "status": [],
            "start_url": start_url,
            "location": "Singapore",
        }

        try:
            site_root = _site_root(start_url)
            if not site_root:
                raise ValueError(f"invalid careers_url: {start_url!r}")

            next_url: Optional[str] = start_url
            seen_job_ids: set[str] = set()
            page_limit = MAX_PAGES

            with _make_session() as session:
                for page_index in range(MAX_PAGES):
                    if not next_url:
                        break

                    response = session.get(next_url, timeout=45)
                    meta["status"].append(response.status_code)
                    response.raise_for_status()
                    meta["pages"] += 1

                    soup = BeautifulSoup(response.text, "lxml")

                    if page_index == 0:
                        reported = _total_pages(soup)
                        meta["reported_total_pages"] = reported
                        if reported:
                            page_limit = min(MAX_PAGES, reported)

                    page_jobs = _parse_listing(soup, site_root)
                    if not page_jobs:
                        break

                    for job in page_jobs:
                        if job["job_id"] in seen_job_ids:
                            continue
                        seen_job_ids.add(job["job_id"])
                        raw_jobs.append(job)

                    if meta["pages"] >= page_limit:
                        break

                    next_link = soup.find("a", attrs={"class": "next"})
                    next_href = _clean_text(next_link.get("href")) if next_link else ""
                    next_url = urljoin(site_root, next_href) if next_href else None

                meta["dates_from_detail"] = self._fill_missing_dates(session, raw_jobs)

            meta["total_raw"] = len(raw_jobs)
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
