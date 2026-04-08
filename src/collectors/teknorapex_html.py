from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


class TeknorApexHtmlCollector(BaseCollector):
    name = "teknorapex_html"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None}

        try:
            r = requests.get(company.careers_url, headers=headers, timeout=30)
            meta["status"] = r.status_code
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")
            section = soup.select_one("#job-list-section")
            if section is None:
                return CollectResult(
                    collector=self.name,
                    company=company.company,
                    careers_url=company.careers_url,
                    raw_jobs=[],
                    meta=meta,
                    error=None,
                )

            for card in section.find_all("div", recursive=False):
                link = card.find("a", href=True)
                if not link:
                    continue

                title_el = link.find("h3")
                title = _clean(title_el.get_text(" ", strip=True)) if title_el else ""
                href = _clean(link.get("href", ""))
                job_url = urljoin(company.careers_url, href)

                job_id = ""
                location = ""
                for span in card.select("dl span"):
                    dt = span.find("dt")
                    dd = span.find("dd")
                    if not dt or not dd:
                        continue
                    label = _clean(dt.get_text(" ", strip=True)).lower()
                    value = _clean(" ".join(dd.stripped_strings))
                    if "job ref" in label:
                        job_id = value
                    elif "location" in label:
                        location = value

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
