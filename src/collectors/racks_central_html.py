from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord

_DATE_RE = re.compile(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\b")
_LOC_RE = re.compile(r"Location:\s*(.*?)\s*(?:Reporting|Company:|Department:|Employment Type:)", re.I)


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _is_singapore(text: str) -> bool:
    return bool(re.search(r"\bsingapore\b|\bSG\b", str(text or ""), re.I))


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    )
    return s


def _get_soup(session: requests.Session, url: str, timeout: float = 25.0) -> BeautifulSoup:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    r.encoding = "utf-8"
    return BeautifulSoup(r.text, "html.parser")


class RacksCentralHtmlCollector(BaseCollector):
    """Racks Central: /job-vacancies listing -> detail pages (location via 'Location:' label or h4)."""

    name = "racks_central_html"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"listing_unique": 0}
        try:
            base = company.careers_url
            session = _make_session()
            soup = _get_soup(session, base)
            urls = {
                urljoin(base, a["href"]): _clean(a.get_text(" ", strip=True))
                for a in soup.select("a[href]")
                if "/job-vacancies/" in urljoin(base, a["href"])
            }
            if not urls:
                raise RuntimeError("Job listing missing")
            meta["listing_unique"] = len(urls)

            for u, title in urls.items():
                detail = _get_soup(session, u)
                text = detail.get_text(" ", strip=True)
                m = _LOC_RE.search(text)
                if m:
                    loc = m.group(1)
                else:
                    heads = [_clean(h.get_text(" ", strip=True)) for h in detail.select("h4")]
                    loc = next((h for h in heads if _is_singapore(h) or "Malaysia" in h), "")
                if not _is_singapore(loc):
                    continue
                dm = _DATE_RE.search(text)
                raw_jobs.append(
                    {
                        "job_id": u.rstrip("/").rsplit("/", 1)[-1],
                        "title": title,
                        "location": loc,
                        "posted_date": datetime.strptime(dm.group(), "%b %d, %Y").date().isoformat() if dm else "",
                        "job_url": u,
                    }
                )
            return CollectResult(self.name, company.company, company.careers_url, raw_jobs, meta, None)
        except Exception as e:
            return CollectResult(self.name, company.company, company.careers_url, raw_jobs, meta, str(e))

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        return [
            JobRecord(
                company=result.company,
                job_title=_clean(r.get("title")),
                location=_clean(r.get("location")),
                job_id=_clean(r.get("job_id")),
                posted_date=_clean(r.get("posted_date")),
                job_url=_clean(r.get("job_url")),
                source=self.name,
                careers_url=result.careers_url,
                raw=r,
            )
            for r in result.raw_jobs
            if isinstance(r, dict)
        ]
