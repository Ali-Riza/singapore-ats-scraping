from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _to_iso_date(raw: str) -> str:
    txt = _clean(raw)
    if not txt:
        return ""
    for fmt in ("%d %b %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(txt, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return txt[:10]


class CrodaApiCollector(BaseCollector):
    name = "croda_api"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        careers_url = _clean(company.careers_url)
        base = "https://www.croda.com"
        api_url = f"{base}/api/vacancieslist/search"
        page_size = 100

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Origin": base,
            "Referer": careers_url,
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
        }

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "total_reported": None}

        try:
            page = 1
            while True:
                body = [
                    {"currentPage": str(page)},
                    {"pageSize": str(page_size)},
                    {"sortBy": "dateposted"},
                    {"lang": "en-gb"},
                ]
                r = requests.post(api_url, json=body, headers=headers, timeout=20)
                r.raise_for_status()
                data = r.json()
                meta["pages"] += 1

                results = data.get("searchResults", [])
                pagination = data.get("pagination", {}) if isinstance(data, dict) else {}
                if meta["total_reported"] is None and isinstance(pagination, dict):
                    meta["total_reported"] = int(pagination.get("totalItems", 0) or 0)

                if not isinstance(results, list) or not results:
                    break

                page_added = 0
                for item in results:
                    if not isinstance(item, dict):
                        continue
                    location = _clean(item.get("jobLocation"))
                    if "singapore" not in location.lower():
                        continue

                    raw_jobs.append(
                        {
                            "job_title": _clean(item.get("jobTitle")),
                            "location": location,
                            "job_id": _clean(item.get("jobId")),
                            "posted_date": _to_iso_date(_clean(item.get("jobDatePosted"))),
                            "job_url": _clean(item.get("jobLink")),
                        }
                    )
                    page_added += 1

                if len(results) < page_size:
                    break
                if page_added == 0 and page > 1:
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
