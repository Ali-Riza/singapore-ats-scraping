from __future__ import annotations

import re
import time
from typing import Any, Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _fetch_page(url: str, page_no: int, page_size: int) -> str:
    params = {"pageNo": page_no, "pageSize": page_size}
    headers = {
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    }
    r = requests.get(url, params=params, headers=headers, timeout=25)
    r.raise_for_status()
    return r.text


def _parse_page(html: str, base: str) -> Tuple[List[Dict[str, Any]], int]:
    soup = BeautifulSoup(html, "html.parser")
    total = 0
    total_el = soup.select_one("p.u-text-medium")
    if total_el:
        m = re.search(r"(\d+)", total_el.get_text(" ", strip=True))
        if m:
            total = int(m.group(1))

    out: List[Dict[str, Any]] = []
    for card in soup.select("div.c-card--job"):
        title_el = card.select_one("a.c-card__title-link")
        if not title_el:
            continue

        title = _clean(title_el.get_text(" ", strip=True))
        href = _clean(title_el.get("href", ""))
        job_url = f"{base}{href}" if href.startswith("/") else href

        meta_items = card.select("li.c-meta__item")
        location = _clean(meta_items[0].get_text(" ", strip=True)) if meta_items else ""
        if "singapore" not in location.lower():
            continue

        job_id = ""
        if "_" in href:
            job_id = _clean(href.split("_")[-1].split("/")[0])

        date_el = card.select_one("time[datetime]")
        posted_date = _clean(date_el.get("datetime", "")) if date_el else ""
        if posted_date and len(posted_date) >= 10:
            posted_date = posted_date[:10]

        out.append(
            {
                "job_title": title,
                "location": location,
                "job_id": job_id,
                "posted_date": posted_date,
                "job_url": job_url,
            }
        )

    return out, total


class IneosHtmlCollector(BaseCollector):
    name = "ineos_html"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "total_reported": None}
        base = "https://careers.ineos.com"
        page_size = 50

        try:
            page = 1
            while True:
                html = _fetch_page(base, page_no=page, page_size=page_size)
                jobs, total = _parse_page(html, base=base)
                meta["pages"] += 1
                if meta["total_reported"] is None:
                    meta["total_reported"] = total

                if not jobs:
                    break
                raw_jobs.extend(jobs)

                if total and (page * page_size) >= total:
                    break
                page += 1
                time.sleep(0.3)

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
