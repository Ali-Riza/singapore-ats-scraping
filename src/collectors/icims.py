from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeout
from playwright.sync_api import sync_playwright

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


_ICIMS_BASE = "https://asiacareers-celanese.icims.com"


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _to_iso_date(v: str) -> str:
    txt = _clean(v)
    if not txt:
        return ""
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(txt, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return txt


def _parse_jobs(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("div.iCIMS_JobsTable")
    if not table:
        return []

    jobs: List[Dict[str, Any]] = []
    for anchor in table.select("a.iCIMS_Anchor"):
        raw_title = _clean(anchor.get_text(" ", strip=True))
        if not raw_title:
            continue

        for prefix in ("Job Title 职位名称", "Job Title", "职位名称"):
            if raw_title.startswith(prefix):
                raw_title = raw_title[len(prefix) :]
                break
        title = _clean(raw_title)
        if not title:
            continue

        href = _clean(anchor.get("href", ""))
        job_url = href if href.startswith("http") else f"{_ICIMS_BASE}{href}"
        job_url = (
            job_url.replace("?in_iframe=1&", "?")
            .replace("&in_iframe=1", "")
            .replace("?in_iframe=1", "")
        )

        parts = [p for p in href.replace(_ICIMS_BASE, "").split("/") if p]
        job_id = parts[1] if len(parts) > 1 else (parts[0] if parts else "")

        row = anchor.find_parent("div", class_="iCIMS_JobHeaderGroup") or anchor.find_parent("div")
        location = ""
        posted_date = ""
        if row:
            for field in row.select("div.iCIMS_JobHeaderField"):
                tag = field.select_one("div.iCIMS_JobHeaderTag")
                data = field.select_one("div.iCIMS_JobHeaderData")
                if not tag or not data:
                    continue
                tag_text = _clean(tag.get_text(" ", strip=True)).lower()
                data_text = _clean(data.get_text(" ", strip=True))
                if "location" in tag_text:
                    location = data_text
                elif "date" in tag_text or "posted" in tag_text:
                    posted_date = _to_iso_date(data_text)

        jobs.append(
            {
                "job_title": title,
                "location": location,
                "job_id": job_id,
                "posted_date": posted_date,
                "job_url": job_url,
            }
        )

    return jobs


def _has_next_page(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("a"):
        txt = _clean(a.get_text(" ", strip=True)).lower()
        classes = {c.lower() for c in (a.get("class") or [])}
        if txt in {"next", ">", "»"} or "next" in classes:
            href = _clean(a.get("href", ""))
            if href and "javascript" not in href.lower():
                return True
    return False


class IcimsCollector(BaseCollector):
    name = "icims"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "visited_urls": []}

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/146.0.0.0 Safari/537.36"
                    )
                )
                page = context.new_page()

                page.goto(company.careers_url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(1500)

                iframe = None
                for f in page.frames:
                    if "in_iframe=1" in f.url:
                        iframe = f
                        break

                if iframe is None:
                    browser.close()
                    return CollectResult(
                        collector=self.name,
                        company=company.company,
                        careers_url=company.careers_url,
                        raw_jobs=[],
                        meta=meta,
                        error="iCIMS iframe not found",
                    )

                page_num = 1
                while True:
                    try:
                        iframe.wait_for_selector("div.iCIMS_JobsTable a.iCIMS_Anchor", timeout=15000)
                    except PlaywrightTimeout:
                        break

                    html = iframe.content()
                    jobs = _parse_jobs(html)
                    raw_jobs.extend(jobs)
                    meta["pages"] += 1
                    meta["visited_urls"].append(page.url)

                    if not jobs or not _has_next_page(html):
                        break

                    page_num += 1
                    next_url = f"{company.careers_url}{'&' if '?' in company.careers_url else '?'}mobile=false&width=1279&page={page_num}"
                    page.goto(next_url, wait_until="networkidle", timeout=30000)
                    page.wait_for_timeout(1200)

                    iframe = None
                    for f in page.frames:
                        if "in_iframe=1" in f.url:
                            iframe = f
                            break
                    if iframe is None:
                        break

                context.close()
                browser.close()

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
            out.append(
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
            )
        return out
