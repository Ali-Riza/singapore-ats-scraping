from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


class WordpressSimpleJobBoardCollector(BaseCollector):
    name = "wordpress_simple_job_board"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
        }

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None}

        try:
            r = requests.get(company.careers_url, headers=headers, timeout=25)
            meta["status"] = r.status_code
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")
            rows = soup.select("table.job_aval tr")
            if rows:
                rows = rows[1:]

            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 5:
                    continue

                title_a = cols[1].find("a")
                if not title_a:
                    continue

                title = _clean(title_a.get_text(" ", strip=True))
                href = _clean(title_a.get("href", ""))
                job_url = urljoin(company.careers_url, href)
                job_id = _clean(job_url.rstrip("/").split("/")[-1])
                location = _clean(cols[4].get_text(" ", strip=True)) or "Singapore"

                raw_jobs.append(
                    {
                        "job_title": title,
                        "location": location,
                        "job_id": job_id,
                        "posted_date": "",
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
