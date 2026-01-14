from __future__ import annotations

import re
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _extract_job_location(text: str) -> str:
    t = _normalize_ws(text)
    if not t:
        return ""

    m = re.search(r"Location:\s*(.*?)\s*Type:", t, flags=re.IGNORECASE)
    section = m.group(1) if m else t

    candidates = re.split(r"\s*[\u2022\-]\s*|\s*\|\s*|\s*;\s*", section)
    candidates = [_normalize_ws(c) for c in candidates if _normalize_ws(c)]

    for c in candidates:
        if "singapore" in c.casefold() or c.casefold() in ("sg",):
            return c

    for c in candidates:
        if len(c) >= 4 and any(k in c.casefold() for k in ["singapore", "malaysia", "uk", "ghana", "nigeria", "dubai", "uae"]):
            return c

    return ""


def _looks_singapore(job: Dict[str, Any]) -> bool:
    hay = " ".join([str(job.get("location") or ""), str(job.get("job_title") or "")]).casefold()
    return "singapore" in hay or " sg " in f" {hay} "


class WordpressElementorCollector(BaseCollector):
    name = "wordpress_elementor"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None}

        try:
            r = requests.get(company.careers_url, timeout=45, headers={"User-Agent": "Mozilla/5.0"})
            meta["status"] = r.status_code
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")

            for details in soup.find_all("details", class_=re.compile(r"\be-n-accordion-item\b")):
                title_el = details.select_one(".e-n-accordion-item-title-text")
                title = _normalize_ws(title_el.get_text(" ", strip=True) if title_el else "")
                if not title:
                    continue

                content_text = _normalize_ws(details.get_text(" ", strip=True))
                location = _extract_job_location(content_text)
                details_id = details.get("id")

                raw_jobs.append(
                    {
                        "job_title": title,
                        "location": location,
                        "job_id": details_id or title,
                        "job_url": f"{company.careers_url}#{details_id}" if details_id else company.careers_url,
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
        out: List[JobRecord] = []

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            # Local filter: keep Singapore-ish entries
            if not _looks_singapore(raw):
                continue

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=_clean_text(raw.get("job_title")),
                    location=_clean_text(raw.get("location")) or "Singapore",
                    job_id=_clean_text(raw.get("job_id")),
                    posted_date="",
                    job_url=_clean_text(raw.get("job_url")) or result.careers_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return out
