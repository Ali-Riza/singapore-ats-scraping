from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


_WS_RE = re.compile(r"\s+")


def _clean_text(v: Any) -> str:
    text = str(v or "").replace("\u00a0", " ")
    return _WS_RE.sub(" ", text).strip()


def _stable_job_id(*parts: str) -> str:
    material = "|".join([p.strip() for p in parts if p is not None]).strip()
    return hashlib.sha1(material.encode("utf-8")).hexdigest()


def _looks_singapore(location: str) -> bool:
    return "singapore" in (location or "").casefold()


class ClassNkStaticHtmlCollector(BaseCollector):
    name = "classnk_static_html"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None}

        try:
            r = requests.get(company.careers_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            meta["status"] = r.status_code
            r.raise_for_status()
            r.encoding = "utf-8"

            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table:
                return CollectResult(self.name, company.company, company.careers_url, [], meta, None)

            tbody = table.find("tbody")
            if not tbody:
                return CollectResult(self.name, company.company, company.careers_url, [], meta, None)

            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue

                location = _clean_text(tds[0].get_text(" ", strip=True))
                job_title = _clean_text(tds[1].get_text(" ", strip=True))

                if not _looks_singapore(location):
                    continue

                raw_jobs.append(
                    {
                        "job_title": job_title,
                        "location": location,
                        "job_id": _stable_job_id(company.company, job_title, location),
                        "job_url": company.careers_url,
                    }
                )

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
                job_title=_clean_text(raw.get("job_title")),
                location=_clean_text(raw.get("location")),
                job_id=_clean_text(raw.get("job_id")),
                posted_date="",
                job_url=_clean_text(raw.get("job_url")),
                source=self.name,
                careers_url=result.careers_url,
                raw=raw,
            )
            for raw in result.raw_jobs
            if isinstance(raw, dict)
        ]
