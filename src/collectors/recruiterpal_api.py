from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _to_iso_date(v: str) -> str:
    txt = _clean(v)
    if not txt:
        return ""
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(txt, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return txt[:10]


class RecruiterpalApiCollector(BaseCollector):
    name = "recruiterpal_api"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        careers_url = _clean(company.careers_url)
        base = careers_url.split("/career/jobs")[0].rstrip("/") if "/career/jobs" in careers_url else careers_url.rstrip("/")
        api_url = f"{base}/api/v1/tms/career/jobs"

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": careers_url,
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
        }

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"api_url": api_url, "status": None}

        try:
            r = requests.get(api_url, headers=headers, timeout=20)
            meta["status"] = r.status_code
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict) or not data.get("success"):
                raise RuntimeError(f"RecruiterPal API error: {data}")

            rows = data.get("rows", [])
            for item in rows:
                if not isinstance(item, dict):
                    continue
                title = _clean(item.get("job_title"))
                if not title:
                    continue

                board_id = _clean(item.get("board_identifier"))
                location_obj = item.get("location") or {}
                location = _clean(location_obj.get("country")) if isinstance(location_obj, dict) else ""
                job_id = _clean(item.get("id")) or _clean(item.get("job_agg_id")) or board_id
                job_url = f"{base}/career/jobs/{board_id}" if board_id else careers_url
                posted_date = _to_iso_date(_clean(item.get("created_at")) or _clean(item.get("updated_at")))

                raw_jobs.append(
                    {
                        "job_title": title,
                        "location": location or "Singapore",
                        "job_id": job_id,
                        "posted_date": posted_date,
                        "job_url": job_url,
                    }
                )

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
