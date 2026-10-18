from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import urlparse

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    )
    return s


class DarwinboxApiCollector(BaseCollector):
    """Darwinbox candidate portal: POST /ms/candidateapi/job/alljobs (paged), filtered to country == Singapore.

    NOTE: a tenant lists *all* group jobs; the exact legal employer is not exposed by the API.
    """

    name = "darwinbox_api"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"listing_total": None, "fetched_total": 0}
        try:
            u = urlparse(company.careers_url)
            base = f"{u.scheme}://{u.netloc}"
            session = _make_session()
            all_jobs: Dict[str, Dict[str, Any]] = {}
            total = None
            for page in range(1, 101):
                r = session.post(
                    base + "/ms/candidateapi/job/alljobs?companyId=main",
                    json={"companyId": "main", "page": page, "sort_option": "new", "limit": 100},
                    timeout=45,
                )
                r.raise_for_status()
                d = r.json()
                if d.get("status") != "success":
                    raise RuntimeError("Darwinbox request failed")
                total = d.get("job_counts", total)
                batch = d.get("data", [])
                new = {j["id"]: j for j in batch if j["id"] not in all_jobs}
                all_jobs.update(new)
                if total is not None and len(all_jobs) >= int(total):
                    break
                if not batch or not new:
                    raise RuntimeError("Darwinbox pagination incomplete")
            else:
                raise RuntimeError("Darwinbox pagination limit exceeded")
            meta.update(listing_total=total, fetched_total=len(all_jobs))

            for j in all_jobs.values():
                if j.get("country") != "Singapore":
                    continue
                posted = j.get("posted_on")
                raw_jobs.append(
                    {
                        "job_id": j["id"],
                        "title": j.get("title"),
                        "location": j.get("locations") or "; ".join(j.get("officelocations_without_area", [])) or "Singapore",
                        "posted_date": datetime.fromtimestamp(posted, timezone.utc).date().isoformat()
                        if isinstance(posted, (int, float))
                        else "",
                        "job_url": f"{base}/ms/candidatev2/main/careers/jobDetails/{j['id']}",
                    }
                )
            return CollectResult(self.name, company.company, company.careers_url, raw_jobs, meta, None)
        except Exception as e:
            return CollectResult(self.name, company.company, company.careers_url, raw_jobs, meta, str(e))

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        return [
            JobRecord(
                company=result.company,
                job_title=_clean(r.get("title")),
                location=_clean(r.get("location")),
                job_id=_clean(r.get("job_id")),
                posted_date=_clean(r.get("posted_date")),
                job_url=_clean(r.get("job_url")),
                source=self.name,
                careers_url=result.careers_url,
                raw=r,
            )
            for r in result.raw_jobs
            if isinstance(r, dict)
        ]
