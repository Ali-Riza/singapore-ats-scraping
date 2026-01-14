from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urldefrag, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


_FOLDER_ID_RE = re.compile(r"/(\d{3,})/?$")


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


def _fetch_html(session: requests.Session, url: str, timeout_s: int = 30) -> str:
    r = session.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.text


def _extract_folder_id(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    url, _ = urldefrag(url)
    m = _FOLDER_ID_RE.search(url)
    return m.group(1) if m else ""


def _api_base(listing_url: str) -> str:
    u = urlparse(listing_url)
    if not u.scheme or not u.netloc:
        return ""
    return f"{u.scheme}://{u.netloc}"


def _jobinfo_url(listing_url: str) -> str:
    base = _api_base(listing_url)
    return f"{base}/en_US/jobs/JobInfo" if base else ""


def _parse_listing(html: str, page_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")

    items: List[Dict[str, str]] = []
    for a in soup.select("a.article__header__focusable[href]"):
        title = _clean_text(a.get_text(" ", strip=True))
        href = (a.get("href") or "").strip()
        if not href:
            continue

        folder_url = urljoin(page_url, href)
        folder_url, _ = urldefrag(folder_url)
        folder_id = _extract_folder_id(folder_url) or _extract_folder_id(href)

        if not folder_id or not title:
            continue

        items.append({"folder_id": folder_id, "job_id": folder_id, "job_title": title, "job_url": folder_url})

    seen: set[str] = set()
    uniq: List[Dict[str, str]] = []
    for it in items:
        fid = it["folder_id"]
        if fid in seen:
            continue
        seen.add(fid)
        uniq.append(it)

    return uniq


def _fetch_jobinfo_fields(session: requests.Session, *, listing_url: str, folder_id: str, timeout_s: int = 30) -> Dict[str, str]:
    url = f"{_jobinfo_url(listing_url)}?jobId={folder_id}"
    html = _fetch_html(session, url, timeout_s)
    soup = BeautifulSoup(html, "html.parser")

    out: Dict[str, str] = {}
    for field in soup.select(".article__content__view__field"):
        lab = field.select_one(".article__content__view__field__label")
        val = field.select_one(".article__content__view__field__value")
        label = _clean_text(lab.get_text(" ", strip=True)) if lab else ""
        value = _clean_text(val.get_text(" ", strip=True)) if val else ""
        if label and value:
            out[label] = value

    return out


def _build_location(fields: Dict[str, str]) -> str:
    city = fields.get("City") or ""
    state = fields.get("State/Prov/County") or ""
    country = fields.get("Country / Region") or ""
    parts = [p for p in [city, state, country] if p]
    return ", ".join(parts)


def _extract_posted_date_from_folderdetail(folderdetail_html: str) -> str:
    m = re.search(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        folderdetail_html,
        flags=re.S | re.I,
    )
    if not m:
        return ""
    try:
        data = json.loads(m.group(1).strip())
    except Exception:
        return ""
    v = data.get("datePosted")
    if isinstance(v, str) and v:
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            return v.strip()[:10]
    return ""


def _additional_posting_locations_include_sg(folderdetail_html: str) -> bool:
    soup = BeautifulSoup(folderdetail_html, "html.parser")
    container = soup.select_one(".article__content__view__field.additional-posting-locations")
    if not container:
        return False
    text = _clean_text(container.get_text(" ", strip=True)).lower()
    return "singapore" in text


class AvatureCollector(BaseCollector):
    name = "avature"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"listing_items": 0, "jobinfo_fetched": 0}

        try:
            listing_url = company.careers_url
            if not listing_url:
                raise ValueError("Missing careers_url")

            with _make_session() as session:
                listing_html = _fetch_html(session, listing_url, timeout_s=30)
                listing_items = _parse_listing(listing_html, listing_url)
                meta["listing_items"] = len(listing_items)

                def fetch_fields(item: Dict[str, str]) -> Optional[Dict[str, Any]]:
                    folder_id = item.get("folder_id") or ""
                    if not folder_id:
                        return None
                    fields = _fetch_jobinfo_fields(session, listing_url=listing_url, folder_id=folder_id, timeout_s=30)
                    loc = _build_location(fields)
                    country = fields.get("Country / Region") or ""

                    keep = "singapore" in (country or "").lower() or "singapore" in (loc or "").lower()
                    folderdetail_html = ""
                    posted_date = ""
                    if not keep:
                        try:
                            folderdetail_html = _fetch_html(session, item.get("job_url") or "", timeout_s=30)
                            keep = _additional_posting_locations_include_sg(folderdetail_html)
                        except Exception:
                            keep = False

                    if keep and folderdetail_html:
                        posted_date = _extract_posted_date_from_folderdetail(folderdetail_html)
                    elif keep:
                        try:
                            folderdetail_html2 = _fetch_html(session, item.get("job_url") or "", timeout_s=30)
                            posted_date = _extract_posted_date_from_folderdetail(folderdetail_html2)
                        except Exception:
                            posted_date = ""

                    if not keep:
                        return None

                    return {
                        "job_id": folder_id,
                        "title": item.get("job_title") or "",
                        "location": loc,
                        "posted_date": posted_date,
                        "job_url": item.get("job_url") or "",
                        "fields": fields,
                    }

                max_workers = 8
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    for maybe in ex.map(fetch_fields, listing_items):
                        meta["jobinfo_fetched"] += 1
                        if maybe:
                            raw_jobs.append(maybe)

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
        company_cf = (result.company or "").casefold()
        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            location = _clean_text(raw.get("location"))
            if "siemens energy" in company_cf:
                location = "Singapore, Central Singapore"

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=_clean_text(raw.get("title")),
                    location=location,
                    job_id=_clean_text(raw.get("job_id")),
                    posted_date=_clean_text(raw.get("posted_date")),
                    job_url=_clean_text(raw.get("job_url")),
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return out
