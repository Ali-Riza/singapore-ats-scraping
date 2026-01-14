from __future__ import annotations

import math
import re
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


_CSRF_RE = re.compile(r'var\s+CSRFToken\s*=\s*"([^"]+)"')


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _make_session(origin: str, referer: str) -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Origin": origin,
            "Referer": referer,
        }
    )
    return s


def _origin_from_url(url: str) -> str:
    u = urlparse(url)
    if not u.scheme or not u.netloc:
        return ""
    return f"{u.scheme}://{u.netloc}"


def _looks_singapore(s: str) -> bool:
    t = (s or "").casefold()
    return "singapore" in t or t.strip() in ("sg",)


class TuvSudRecruitingApiCollector(BaseCollector):
    """TÜV SÜD careers endpoint (requires CSRF token + POST JSON)."""

    name = "tuvsud_recruiting_api"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "status": [], "totalJobs": None}

        try:
            search_url = company.careers_url
            origin = _origin_from_url(search_url)
            if not origin:
                raise ValueError("Invalid careers_url")

            api_url = origin + "/services/recruiting/v1/jobs"

            with _make_session(origin, search_url) as session:
                # Bootstrap: fetch page to obtain cookies + CSRF
                r0 = session.get(search_url, timeout=30)
                meta["status"].append(r0.status_code)
                r0.raise_for_status()

                m = _CSRF_RE.search(r0.text or "")
                if m:
                    session.headers["X-CSRF-Token"] = m.group(1)

                page = 0
                total_jobs: int | None = None
                max_pages: int | None = None
                page_size_guess = 10

                while page < 1000:  # safety cap
                    payload: Dict[str, Any] = {
                        "locale": "en_US",
                        "pageNumber": page,
                        "sortBy": "",
                        "keywords": "",
                        "location": "",
                        "facetFilters": {
                            "cust_brand": ["TÜV SÜD"],
                            "jobLocationCountry": ["Singapore"],
                        },
                        "brand": "",
                        "skills": [],
                        "categoryId": 0,
                        "alertId": "",
                        "rcmCandidateId": "",
                    }

                    r = session.post(api_url, json=payload, timeout=30)
                    meta["status"].append(r.status_code)
                    r.raise_for_status()
                    js = r.json() if isinstance(r.json(), dict) else {}

                    if total_jobs is None:
                        try:
                            total_jobs = int(js.get("totalJobs") or 0)
                        except Exception:
                            total_jobs = 0
                        meta["totalJobs"] = total_jobs
                        if total_jobs > 0:
                            max_pages = math.ceil(total_jobs / page_size_guess)

                    results = js.get("jobSearchResult") or []
                    if not isinstance(results, list) or not results:
                        break

                    for item in results:
                        if not isinstance(item, dict):
                            continue
                        resp = item.get("response")
                        raw = resp if isinstance(resp, dict) else item
                        raw_jobs.append(raw)

                    page += 1
                    meta["pages"] = page

                    if max_pages is not None and page >= max_pages:
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
        origin = _origin_from_url(result.careers_url)

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            job_id = _clean_text(raw.get("jobId") or raw.get("jobSeqNo") or raw.get("id"))
            title = _clean_text(raw.get("jobTitle") or raw.get("title") or raw.get("postingTitle"))
            posted = _clean_text(raw.get("postingStartDate") or raw.get("postedDate") or raw.get("postingDate"))

            location = _clean_text(raw.get("jobLocation") or raw.get("location") or "")
            if not location:
                country = _clean_text(raw.get("jobLocationCountry") or raw.get("country") or "Singapore")
                location = country

            if not _looks_singapore(location):
                continue

            job_url = _clean_text(
                raw.get("jobDetailUrl")
                or raw.get("detailUrl")
                or raw.get("applyUrl")
                or raw.get("externalApplyUrl")
                or raw.get("externalUrl")
                or raw.get("url")
            )
            if job_url and origin and job_url.startswith("/"):
                job_url = urljoin(origin, job_url)
            if not job_url:
                job_url = result.careers_url

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=location,
                    job_id=job_id,
                    posted_date=posted,
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return out
