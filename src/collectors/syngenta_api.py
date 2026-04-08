from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _parse_date(raw: str) -> str:
    txt = _clean(raw)
    if not txt:
        return ""
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(txt, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return txt


class SyngentaApiCollector(BaseCollector):
    name = "syngenta_api"

    def _fetch_page(self, base_url: str, page: int, country: str) -> Dict[str, Any]:
        api_url = f"{base_url.rstrip('/')}/api/jobs"
        headers = {
            "Accept": "application/json",
            "Referer": base_url.rstrip("/") + "/",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
        }
        r = requests.post(api_url, params={"page": page, "country": country}, json={}, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json()

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        careers_url = _clean(company.careers_url)
        base_url = careers_url.split("/?")[0].rstrip("/") if "//" in careers_url else "https://jobs.syngenta.com"

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "country": "SG", "total_reported": None}

        try:
            page = 1
            while True:
                data = self._fetch_page(base_url, page=page, country="SG")
                meta["pages"] += 1
                if meta["total_reported"] is None:
                    try:
                        meta["total_reported"] = int(data.get("total", 0) or 0)
                    except Exception:
                        meta["total_reported"] = 0

                jobs = data.get("jobs", [])
                if not isinstance(jobs, list) or not jobs:
                    break

                page_added = 0
                for item in jobs:
                    if not isinstance(item, dict):
                        continue
                    if _clean(item.get("location_code")).upper() != "SG":
                        continue

                    job_id = _clean(item.get("id"))
                    title = _clean(item.get("title"))
                    city = _clean(item.get("city"))
                    location = f"{city}, Singapore" if city else "Singapore"
                    posted_date = _parse_date(_clean(item.get("published")))
                    job_url = _clean(item.get("url")) or f"{base_url}/job/-:{job_id}"

                    raw_jobs.append(
                        {
                            "job_title": title,
                            "location": location,
                            "job_id": job_id,
                            "posted_date": posted_date,
                            "job_url": job_url,
                        }
                    )
                    page_added += 1

                if page_added == 0:
                    break
                page += 1

            meta["count"] = len(raw_jobs)
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
        return [
            JobRecord(
                company=result.company,
                job_title=_clean(raw.get("job_title")),
                location=_clean(raw.get("location")) or "Singapore",
                job_id=_clean(raw.get("job_id")) or _clean(raw.get("job_title")),
                posted_date=_clean(raw.get("posted_date")),
                job_url=_clean(raw.get("job_url")) or result.careers_url,
                source=self.name,
                careers_url=result.careers_url,
                raw=raw,
            )
            for raw in result.raw_jobs
            if isinstance(raw, dict)
        ]
