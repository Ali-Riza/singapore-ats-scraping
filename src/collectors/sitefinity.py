from __future__ import annotations

import re
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


ID_RE = re.compile(r"accordion-[^-]+-([a-f0-9\-]{36})", re.I)


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _make_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return s


def _scrape_jobs_from_page(html: str, page_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    job_accordion = None
    marker = soup.find(string=re.compile(r"Job offerings", re.I))
    if marker:
        cur = marker.parent
        while cur:
            cur = cur.find_next()
            if not cur:
                break
            if getattr(cur, "name", None) and "accordion" in (cur.get("class") or []):
                job_accordion = cur
                break
            acc = cur.select_one(".accordion") if getattr(cur, "select_one", None) else None
            if acc is not None:
                job_accordion = acc
                break

    if job_accordion is None:
        job_accordion = soup.select_one(".accordion")

    out: List[Dict[str, Any]] = []
    for item in (job_accordion.select(".accordion-item") if job_accordion else []):
        title_el = item.select_one("h5")
        title = _clean_text(title_el.get_text(" ", strip=True)) if title_el else ""
        if not title:
            continue

        job_id = ""
        collapse = item.select_one(".accordion-collapse[id]")
        if collapse and collapse.get("id"):
            m = ID_RE.search(str(collapse.get("id")))
            job_id = m.group(1) if m else str(collapse.get("id"))

        description_el = item.select_one(".accordion-body .card-text")
        description_html = str(description_el) if description_el else ""

        job_url = page_url
        if collapse and collapse.get("id"):
            job_url = page_url.split("#")[0] + "#" + str(collapse.get("id"))

        out.append(
            {
                "job_title": title,
                "job_id": job_id,
                "job_url": job_url,
                "description_html": description_html,
            }
        )

    return out


class SitefinityCollector(BaseCollector):
    name = "sitefinity"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None}

        try:
            with _make_session() as session:
                r = session.get(company.careers_url, timeout=30)
                meta["status"] = r.status_code
                r.raise_for_status()

            raw_jobs = _scrape_jobs_from_page(r.text, company.careers_url)

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
                location="Singapore",
                job_id=_clean_text(raw.get("job_id")) or _clean_text(raw.get("job_title")),
                posted_date="",
                job_url=_clean_text(raw.get("job_url")) or result.careers_url,
                source=self.name,
                careers_url=result.careers_url,
                raw=raw,
            )
            for raw in result.raw_jobs
            if isinstance(raw, dict)
        ]
