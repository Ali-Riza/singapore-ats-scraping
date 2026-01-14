from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


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


def _base_from_url(url: str) -> str:
    u = urlparse(url)
    if not u.scheme or not u.netloc:
        return url.rstrip("/")
    return f"{u.scheme}://{u.netloc}".rstrip("/")


def _normalize_url(base: str, href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    return urljoin(base + "/", href)


def _slug_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
    except Exception:
        path = url
    path = (path or "").rstrip("/")
    return path.split("/")[-1] if path else ""


def _extract_location_from_detail(soup: BeautifulSoup) -> str:
    text = soup.get_text("\n", strip=True)
    for pat in (r"\bLocation\b\s*[:\-]\s*(.+)", r"\bJob\s*Location\b\s*[:\-]\s*(.+)"):
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = _clean_text(m.group(1)).split("\n")[0].strip()
            if 1 <= len(val) <= 80:
                return val
    return "Singapore"


def _parse_listing_jobs(base: str, html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, str]] = []

    for card in soup.select("section.imagecta"):
        h2 = card.select_one("h2")
        a = card.select_one("a[href]")
        if not h2 or not a:
            continue
        title = _clean_text(h2.get_text(" ", strip=True))
        url = _normalize_url(base, a.get("href") or "")
        if title and url:
            out.append({"title": title, "url": url})

    # De-dupe by URL
    seen: set[str] = set()
    uniq: List[Dict[str, str]] = []
    for j in out:
        if j["url"] in seen:
            continue
        seen.add(j["url"])
        uniq.append(j)

    return uniq


class KenticoHtmlCollector(BaseCollector):
    name = "kentico_html"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": []}

        try:
            listing_url = company.careers_url
            base = _base_from_url(listing_url)

            with _make_session() as session:
                r = session.get(listing_url, timeout=30)
                meta["status"].append(r.status_code)
                r.raise_for_status()

                listing_jobs = _parse_listing_jobs(base, r.text)

                for item in listing_jobs:
                    job_url = item["url"]
                    title = item["title"]

                    rr = session.get(job_url, timeout=30)
                    meta["status"].append(rr.status_code)
                    rr.raise_for_status()

                    raw_jobs.append({"listing_title": title, "job_url": job_url, "detail_html": rr.text})

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

            job_url = _clean_text(raw.get("job_url"))
            detail_html = raw.get("detail_html") or ""
            soup = BeautifulSoup(detail_html, "html.parser")

            h1 = soup.select_one("h1")
            title = _clean_text(h1.get_text(" ", strip=True)) if h1 else ""
            if not title:
                h2 = soup.select_one("h2")
                title = _clean_text(h2.get_text(" ", strip=True)) if h2 else ""
            if not title:
                title = _clean_text(raw.get("listing_title"))

            location = _extract_location_from_detail(soup)
            job_id = _slug_from_url(job_url)

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=location,
                    job_id=job_id,
                    posted_date="",
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return out
