from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


PORTAL_BASE_URL = "https://www.mycareersfuture.gov.sg"
API_BASE_URL = "https://api.mycareersfuture.gov.sg"


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
            "accept": "application/json, text/plain, */*",
            "mcf-client": "jobseeker",
            "origin": PORTAL_BASE_URL,
            "referer": f"{PORTAL_BASE_URL}/",
            "user-agent": "Mozilla/5.0",
        }
    )
    return s


def _extract_uen(careers_url: str, raw_data_row: Dict[str, Any]) -> Optional[str]:
    """MyCareersFuture API needs an employer UEN.

    We try to infer it from:
    - careers_url query param `uen`
    - careers_url being the UEN itself
    - a raw data row column named `uen`
    """

    s = (careers_url or "").strip()
    if not s:
        return None

    # Query param
    try:
        qs = parse_qs(urlparse(s).query)
        for key in ("uen", "UEN"):
            if key in qs and qs[key]:
                cand = str(qs[key][0]).strip()
                if cand:
                    return cand
    except Exception:
        pass

    # URL might be the UEN itself
    if s and "//" not in s and len(s) >= 6:
        return s

    # Raw row
    for key in ("uen", "UEN"):
        v = raw_data_row.get(key)
        if v and str(v).strip():
            return str(v).strip()

    return None


def _extract_location(job: Dict[str, Any]) -> str:
    addr = job.get("address")
    if not isinstance(addr, dict):
        return "Singapore"

    if addr.get("isOverseas"):
        country = addr.get("overseasCountry")
        parts = [addr.get("foreignAddress1"), addr.get("foreignAddress2"), country]
        parts = [_clean_text(p) for p in parts if _clean_text(p)]
        return ", ".join(parts) if parts else "Singapore"

    districts = addr.get("districts")
    if isinstance(districts, list) and districts:
        first = districts[0]
        if isinstance(first, dict):
            loc = _clean_text(first.get("location"))
            if loc:
                return f"{loc}, Singapore"

    parts: List[str] = []
    for key in ("building", "block", "street", "postalCode"):
        val = _clean_text(addr.get(key))
        if val:
            parts.append(val)

    return (", ".join(parts) + ", Singapore") if parts else "Singapore"


class MyCareersFutureCollector(BaseCollector):
    name = "mycareersfuture"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "status": [], "uen": None}

        try:
            uen = _extract_uen(company.careers_url, company.raw_data_row)
            if not uen:
                raise ValueError("Missing UEN: add `?uen=<UEN>` to careers_url or add a `uen` column")
            meta["uen"] = uen

            with _make_session() as session:
                page = 0
                while page < 500:  # safety cap
                    params = {"limit": 20, "page": page, "uen": uen}
                    url = f"{API_BASE_URL}/v2/jobs"
                    r = session.get(url, params=params, timeout=45)
                    meta["status"].append(r.status_code)
                    r.raise_for_status()

                    js = r.json()
                    results = js.get("results")
                    if not isinstance(results, list) or not results:
                        break

                    for job in results:
                        if isinstance(job, dict):
                            raw_jobs.append(job)

                    page += 1
                    meta["pages"] = page

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

            metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
            posted_date = _clean_text(metadata.get("newPostingDate") or metadata.get("originalPostingDate"))
            job_url = _clean_text(metadata.get("jobDetailsUrl"))

            if not job_url:
                links = raw.get("_links")
                if isinstance(links, dict):
                    self_link = links.get("self")
                    if isinstance(self_link, dict):
                        job_url = _clean_text(self_link.get("href"))

            job_id = _clean_text(raw.get("uuid") or raw.get("id"))
            title = _clean_text(raw.get("title"))
            location = _extract_location(raw)

            if not title and not job_id and not job_url:
                continue

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
