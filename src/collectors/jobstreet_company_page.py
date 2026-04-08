from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


_JOBSTREET_JSON_RE = re.compile(r"window\.SEEK_APOLLO_DATA\s*=\s*(\{.*?\});\s*\n", re.DOTALL)


def _extract_jobstreet_payload(html: str) -> Tuple[Dict[str, Any], int]:

    m = _JOBSTREET_JSON_RE.search(html)
    if not m:
        return {}, 0

    data = json.loads(m.group(1))

    total = 0
    for key, val in data.items():
        if "jobSearchV6" in key and isinstance(val, dict):
            total = int(val.get("totalCount", 0) or 0)
            break

    return data, total


def _jobs_from_payload(data: Dict[str, Any], careers_url: str) -> List[Dict[str, Any]]:

    base = "https://" + (urlparse(careers_url).netloc or "sg.jobstreet.com")

    jobs: List[Dict[str, Any]] = []
    for key, val in data.items():
        if not isinstance(val, dict) or val.get("__typename") != "JobSearchV6Data":
            continue

        job_id = str(val.get("id", "") or "")
        title = str(val.get("title", "") or "")
        locations = val.get("locations") or []
        location = ""
        if isinstance(locations, list) and locations:
            loc0 = locations[0] or {}
            if isinstance(loc0, dict):
                location = str(loc0.get("label", "") or "")

        listing = val.get("listingDate") or {}
        posted_date = ""
        if isinstance(listing, dict):
            dt = str(listing.get("dateTimeUtc", "") or "")
            if dt:
                posted_date = dt[:10]

        job_url = f"{base}/job/{job_id}" if job_id else ""

        jobs.append(
            {
                "job_title": title,
                "location": location,
                "job_id": job_id,
                "posted_date": posted_date,
                "job_url": job_url,
                "careers_url": careers_url,
            }
        )

    return jobs


@dataclass(frozen=True)
class _JobStreetConfig:
    page_size: int = 32
    max_pages: int = 50


class JobStreetCompanyPageCollector(BaseCollector):

    name = "jobstreet_company_page"

    def __init__(self, cfg: Optional[_JobStreetConfig] = None) -> None:
        self.cfg = cfg or _JobStreetConfig()

    def collect_raw(self, company: CompanyItem) -> CollectResult:

        careers_url = company.careers_url
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-SG,en;q=0.9",
            }
        )

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "status_codes": [],
            "visited_urls": [],
            "total_reported": None,
        }

        total_reported: Optional[int] = None
        page_num = 1

        # 1) First try simple HTTP requests (fast path)
        try:
            while page_num <= self.cfg.max_pages:
                if page_num == 1:
                    url = careers_url
                else:
                    sep = "&" if "?" in careers_url else "?"
                    url = f"{careers_url}{sep}page={page_num}"

                r = session.get(url, timeout=30)
                meta["status_codes"].append(r.status_code)
                meta["visited_urls"].append(url)
                meta["pages"] += 1
                r.raise_for_status()

                data, total = _extract_jobstreet_payload(r.text)
                if total_reported is None and total:
                    total_reported = total
                    meta["total_reported"] = total_reported

                jobs_page = _jobs_from_payload(data, careers_url)
                if not jobs_page:
                    break

                raw_jobs.extend(jobs_page)

                if total_reported is not None and len(raw_jobs) >= total_reported:
                    break

                if len(jobs_page) < self.cfg.page_size:
                    break

                page_num += 1
        except Exception as e:  # e.g. 403/429/5xx etc.
            meta["requests_error"] = str(e)

        # 2) If the plain-requests path yielded nothing, fall back to Playwright
        if not raw_jobs:
            meta["fallback"] = "playwright"
            try:
                from playwright.sync_api import sync_playwright  # type: ignore

                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(
                        user_agent=(
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/146.0.0.0 Safari/537.36"
                        ),
                        locale="en-SG",
                    )
                    page = context.new_page()

                    page_num = 1
                    total_reported = None

                    while page_num <= self.cfg.max_pages:
                        if page_num == 1:
                            url = careers_url
                        else:
                            sep = "&" if "?" in careers_url else "?"
                            url = f"{careers_url}{sep}page={page_num}"

                        page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(1500)
                        html = page.content()
                        meta["visited_urls"].append(url)

                        data, total = _extract_jobstreet_payload(html)
                        if total_reported is None and total:
                            total_reported = total
                            meta["total_reported"] = total_reported

                        jobs_page = _jobs_from_payload(data, careers_url)
                        if not jobs_page:
                            break

                        raw_jobs.extend(jobs_page)

                        if total_reported is not None and len(raw_jobs) >= total_reported:
                            break

                        if len(jobs_page) < self.cfg.page_size:
                            break

                        page_num += 1

                    context.close()
                    browser.close()

            except Exception as pe:  # pragma: no cover - best-effort fallback
                meta["playwright_error"] = str(pe)

        meta["total_raw"] = len(raw_jobs)

        return CollectResult(
            collector=self.name,
            company=company.company,
            careers_url=company.careers_url,
            raw_jobs=raw_jobs,
            meta=meta,
            error=None,
        )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        records: List[JobRecord] = []
        for raw in result.raw_jobs:
            records.append(self._map_one(raw, result))
        return records

    def _map_one(self, raw: Dict[str, Any], result: CollectResult) -> JobRecord:
        title = str(raw.get("job_title", "") or "").strip()
        location = str(raw.get("location", "") or "").strip()
        job_id = str(raw.get("job_id", "") or "").strip()
        posted_date = str(raw.get("posted_date", "") or "").strip()
        job_url = str(raw.get("job_url", "") or "").strip()

        return JobRecord(
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
