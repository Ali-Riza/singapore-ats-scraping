from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


HITACHI_LIST_URL = (
    "https://www.hitachienergy.com/careers/open-jobs/"
    "_jcr_content/root/container/content_1/content/grid_0/joblist.listsearchresults.json"
)

_JOB_ID_RE = re.compile(r"/details/([^/?#]+)")


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
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return s


def _parse_iso_date(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""

    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date().isoformat()
        except Exception:
            continue

    if len(s) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", s[:10]):
        return s[:10]

    return ""


def _extract_job_id(job_url: str) -> str:
    m = _JOB_ID_RE.search((job_url or "").strip())
    return m.group(1) if m else ""


def _looks_singapore(location: str) -> bool:
    return "singapore" in (location or "").casefold()


class AemWorkdayJsonCollector(BaseCollector):
    name = "aem_workday_json"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "status": [], "list_url": None}

        try:
            careers_url = company.careers_url
            list_url = careers_url if careers_url.endswith(".json") else HITACHI_LIST_URL
            meta["list_url"] = list_url

            with _make_session() as session:
                offset = 0
                while offset < 5000:  # safety cap
                    params: Dict[str, Any] = {"location": "Singapore"}
                    if offset:
                        params["offset"] = offset

                    r = session.get(list_url, params=params, timeout=30)
                    meta["status"].append(r.status_code)
                    r.raise_for_status()

                    data = r.json() if isinstance(r.json(), dict) else {}
                    items = data.get("items") or []
                    if not isinstance(items, list) or not items:
                        break

                    for it in items:
                        if isinstance(it, dict):
                            raw_jobs.append(it)

                    offset += len(items)
                    meta["pages"] += 1

                    load_more = bool(data.get("loadMore"))
                    total = int(data.get("totalNumber") or 0)
                    if not load_more:
                        break
                    if total and offset >= total:
                        break

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

            job_url = _clean_text(raw.get("url") or "")
            apply_url = _clean_text(raw.get("applyNowUrl") or "")
            location = _clean_text(raw.get("location") or raw.get("primaryLocation") or "")

            if not _looks_singapore(location):
                continue

            posted_date = _parse_iso_date(_clean_text(raw.get("publicationDate") or ""))
            job_id = _extract_job_id(job_url)

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=_clean_text(raw.get("title") or ""),
                    location=location,
                    job_id=job_id,
                    posted_date=posted_date,
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw={**raw, "apply_url": apply_url},
                )
            )

        return out
