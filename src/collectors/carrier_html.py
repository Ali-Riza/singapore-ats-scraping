from __future__ import annotations

import json
import subprocess
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _fetch_html(url: str) -> str:
    # Carrier is often easier via curl (TLS/bot). Try requests first, fall back to curl.
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200 and r.text:
            return r.text
    except Exception:
        pass

    proc = subprocess.run(["curl", "-L", "-sS", "--compressed", url], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"curl failed ({proc.returncode}):\n{proc.stderr}")
    return proc.stdout


def _normalize_date(date_str: str | None) -> str:
    if not date_str:
        return ""
    try:
        return datetime.fromisoformat(date_str).date().isoformat()
    except Exception:
        return date_str


def _extract_posted_date(detail_html: str) -> str:
    soup = BeautifulSoup(detail_html, "html.parser")
    for script in soup.select('script[type="application/ld+json"]'):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except Exception:
            continue

        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            return _normalize_date(data.get("datePosted"))
    return ""


class CarrierHtmlCollector(BaseCollector):
    name = "carrier_html"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None}

        try:
            careers_url = company.careers_url
            html = _fetch_html(careers_url)

            soup = BeautifulSoup(html, "html.parser")
            for a in soup.select("#search-results-list ul li a[data-job-id]"):
                job_id = (a.get("data-job-id") or "").strip()
                rel = (a.get("href") or "").strip()
                job_url = urljoin(careers_url, rel)

                title_el = a.select_one("h2")
                loc_el = a.select_one(".job-location")

                raw_jobs.append(
                    {
                        "job_id": job_id,
                        "job_title": _clean_text(title_el.get_text(strip=True) if title_el else ""),
                        "location": _clean_text(loc_el.get_text(strip=True) if loc_el else ""),
                        "job_url": job_url,
                    }
                )

            # detail fetch for posted date (best-effort)
            for j in raw_jobs:
                try:
                    detail_html = _fetch_html(j.get("job_url") or "")
                    j["posted_date"] = _extract_posted_date(detail_html)
                except Exception:
                    j["posted_date"] = ""

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

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=_clean_text(raw.get("job_title")),
                    location=_clean_text(raw.get("location")),
                    job_id=_clean_text(raw.get("job_id")),
                    posted_date=_clean_text(raw.get("posted_date")),
                    job_url=_clean_text(raw.get("job_url")),
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return out
