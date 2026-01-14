from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


DEFAULT_ROOT_ID = 25796
DEFAULT_LANGUAGE = "en"
DEFAULT_PAGE_SIZE = 50

_JOB_ID_FROM_PATH_RE = re.compile(r"-(\d+)/?$")


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _make_session(base_url: str) -> requests.Session:
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
            "Content-Type": "application/json",
            "Origin": base_url,
            "Referer": base_url,
        }
    )
    return s


def _base_from_url(url: str) -> str:
    u = urlparse(url)
    if not u.scheme or not u.netloc:
        return url.rstrip("/")
    return f"{u.scheme}://{u.netloc}".rstrip("/")


def _parse_iso_date(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""

    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
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


def _extract_job_id_from_lr_url(path_or_url: str) -> str:
    m = _JOB_ID_FROM_PATH_RE.search((path_or_url or "").strip())
    return m.group(1) if m else ""


class LrEpiserverApiCollector(BaseCollector):
    name = "lr_episerver_api"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "status": [], "base_url": None}

        try:
            base_url = _base_from_url(company.careers_url)
            meta["base_url"] = base_url
            api_url = f"{base_url}/api/search/careers/"

            with _make_session(base_url) as session:
                page = 1
                while page <= 200:  # safety cap
                    payload: Dict[str, Any] = {
                        "page": page,
                        "pageSize": DEFAULT_PAGE_SIZE,
                        "language": DEFAULT_LANGUAGE,
                        "rootId": DEFAULT_ROOT_ID,
                        "query": "",
                        "filters": {"JobCountry": ["Singapore"]},
                    }

                    r = session.post(api_url, json=payload, timeout=30)
                    meta["status"].append(r.status_code)
                    r.raise_for_status()
                    data = r.json() if isinstance(r.json(), dict) else {}

                    items = data.get("items") or []
                    if not isinstance(items, list) or not items:
                        break

                    raw_jobs.extend([it for it in items if isinstance(it, dict)])
                    meta["pages"] = page

                    has_more = bool(data.get("hasMore"))
                    num_pages = int(data.get("numberOfPages") or 0)

                    if not has_more:
                        break
                    if num_pages and page >= num_pages:
                        break

                    page += 1

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
        base_url = str(result.meta.get("base_url") or _base_from_url(result.careers_url))

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            lr_path = _clean_text(raw.get("url") or raw.get("pagePath") or "")
            job_url = urljoin(base_url, lr_path) if lr_path else ""
            job_id = _extract_job_id_from_lr_url(lr_path) or _extract_job_id_from_lr_url(job_url)

            posted_date = _parse_iso_date(_clean_text(raw.get("published") or raw.get("postingStartDate") or ""))

            location_parts = [
                _clean_text(raw.get("jobLocation")),
                _clean_text(raw.get("city")),
                _clean_text(raw.get("jobCountry")),
            ]
            location = ", ".join([p for p in location_parts if p])

            country = _clean_text(raw.get("jobCountry") or "")
            if country.casefold() != "singapore":
                continue

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=_clean_text(raw.get("jobTitle") or raw.get("heading") or ""),
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
