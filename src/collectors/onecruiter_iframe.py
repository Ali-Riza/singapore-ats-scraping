from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _extract_job_id(url: str) -> str:
    m = re.search(r"/jobs/(\d+)-", _clean(url))
    return m.group(1) if m else ""


def _parse_card_text(text: str) -> Tuple[str, str, str]:
    full = _clean(text)
    if not full:
        return "", "", ""

    title = ""
    location = ""
    posted_date = ""

    title_match = re.search(
        r"Job title:\s*(.+?)(?=\s+(?:On-site|Remote|Hybrid)\b|\s+Job category:|\s+Location:|\s+Apply by:|$)",
        full,
        re.IGNORECASE,
    )
    if title_match:
        title = _clean(title_match.group(1))

    location_match = re.search(r"Location:\s*(.+?)(?=\s+Apply by:|\s+Read more|$)", full, re.IGNORECASE)
    if location_match:
        location = _clean(location_match.group(1))

    date_match = re.search(r"Apply by:\s*(\d{4}-\d{2}-\d{2})", full, re.IGNORECASE)
    if date_match:
        posted_date = date_match.group(1)

    return title, location, posted_date


class OnecruiterIframeCollector(BaseCollector):
    name = "onecruiter_iframe"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"frame_found": False}

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(company.careers_url, wait_until="networkidle", timeout=60000)
                page.wait_for_timeout(2500)

                onecruiter_frame = None
                for f in page.frames:
                    if "onecruiter.com" in (f.url or ""):
                        onecruiter_frame = f
                        break

                if onecruiter_frame is None:
                    browser.close()
                    return CollectResult(
                        collector=self.name,
                        company=company.company,
                        careers_url=company.careers_url,
                        raw_jobs=[],
                        meta=meta,
                        error="Onecruiter iframe not found",
                    )

                meta["frame_found"] = True
                cards = onecruiter_frame.locator("div, article, li, section").evaluate_all(
                    """
                    els => els.map(el => ({
                        text: (el.innerText || '').trim(),
                        html: el.innerHTML || ''
                    }))
                    """
                )

                seen_urls = set()
                for card in cards:
                    text = _clean(card.get("text"))
                    html = card.get("html") or ""
                    if not text or "singapore" not in text.lower():
                        continue

                    hrefs = re.findall(r'href=["\\\']([^"\\\']+)["\\\']', html, re.IGNORECASE)
                    job_url = ""
                    for href in hrefs:
                        if "/jobs/" in href:
                            job_url = href
                            break
                    if not job_url:
                        continue

                    if job_url.startswith("/"):
                        job_url = "https://trelleborg.onecruiter.com" + job_url

                    if job_url in seen_urls:
                        continue
                    seen_urls.add(job_url)

                    title, location, posted_date = _parse_card_text(text)
                    raw_jobs.append(
                        {
                            "job_title": title,
                            "location": location,
                            "job_id": _extract_job_id(job_url),
                            "posted_date": posted_date,
                            "job_url": job_url,
                        }
                    )

                browser.close()

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
