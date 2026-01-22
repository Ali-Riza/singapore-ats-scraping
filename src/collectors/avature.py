from __future__ import annotations

import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urljoin, urldefrag, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


_FOLDER_ID_RE = re.compile(r"(?:folderId|jobId|job)=([A-Za-z0-9\-]+)", re.IGNORECASE)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _slugify(value: str | None) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", (value or "").lower())
    text = text.strip("-")
    return text or "default"


def _safe_key(fragment: str) -> str:
    slug = _slugify(fragment)
    if slug and len(slug) <= 40:
        return slug
    digest = hashlib.sha1((fragment or "").encode("utf-8")).hexdigest()[:12]
    return (slug[:28] + "-" + digest) if slug else digest


def _make_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; ATS-Scraper/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def _fetch_html(session: requests.Session, url: str, timeout_s: int = 30) -> str:
    if not url:
        raise ValueError("Missing URL")
    response = session.get(url, timeout=timeout_s)
    response.raise_for_status()
    return response.text


def _extract_folder_id(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    clean, _ = urldefrag(raw)
    parsed = urlparse(clean)
    query = parse_qs(parsed.query)
    for key in ("folderId", "folderID", "FolderId", "folderid", "jobId", "jobid", "job"):
        values = query.get(key)
        if values:
            candidate = values[0].strip()
            if candidate:
                return candidate
    path_match = re.search(r"/jobdetail/(?:[^/]+/)?([A-Za-z0-9\-]+)", parsed.path, re.IGNORECASE)
    if path_match:
        return path_match.group(1)
    regex_match = _FOLDER_ID_RE.search(clean)
    return regex_match.group(1) if regex_match else ""


def _api_base(listing_url: str) -> str:
    parsed = urlparse(listing_url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}"


def _jobinfo_url(listing_url: str) -> str:
    base = _api_base(listing_url)
    return f"{base}/en_US/jobs/JobInfo" if base else ""


def _parse_listing(html: str, page_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")

    results: List[Dict[str, str]] = []
    for anchor in soup.select("a.article__header__focusable[href]"):
        title = _clean_text(anchor.get_text(" ", strip=True))
        href = (anchor.get("href") or "").strip()
        if not href or not title:
            continue

        absolute = urljoin(page_url, href)
        absolute, _ = urldefrag(absolute)
        folder_id = _extract_folder_id(absolute) or _extract_folder_id(href)
        if not folder_id:
            continue

        results.append(
            {
                "folder_id": folder_id,
                "job_id": folder_id,
                "job_title": title,
                "job_url": absolute,
            }
        )

    unique: List[Dict[str, str]] = []
    seen: set[str] = set()
    for item in results:
        fid = item["folder_id"]
        if fid in seen:
            continue
        seen.add(fid)
        unique.append(item)

    return unique


def _fetch_jobinfo_fields(
    session: requests.Session,
    *,
    listing_url: str,
    folder_id: str,
    timeout_s: int = 30,
) -> Dict[str, str]:
    jobinfo_endpoint = f"{_jobinfo_url(listing_url)}?jobId={folder_id}"
    html = _fetch_html(session, jobinfo_endpoint, timeout_s)
    soup = BeautifulSoup(html, "html.parser")

    fields: Dict[str, str] = {}
    for field in soup.select(".article__content__view__field"):
        label_node = field.select_one(".article__content__view__field__label")
        value_node = field.select_one(".article__content__view__field__value")
        label = _clean_text(label_node.get_text(" ", strip=True)) if label_node else ""
        value = _clean_text(value_node.get_text(" ", strip=True)) if value_node else ""
        if label and value:
            fields[label] = value

    return fields


def _build_location(fields: Dict[str, str]) -> str:
    city = fields.get("City") or ""
    state = fields.get("State/Prov/County") or ""
    country = fields.get("Country / Region") or ""
    pieces = [part for part in (city, state, country) if part]
    return ", ".join(pieces)


def _extract_posted_date_from_folderdetail(html: str) -> str:
    match = re.search(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S | re.I)
    if not match:
        return ""
    try:
        payload = json.loads(match.group(1).strip())
    except Exception:
        return ""
    value = payload.get("datePosted")
    if not isinstance(value, str) or not value:
        return ""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return value.strip()[:10]


def _additional_posting_locations_include_sg(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    node = soup.select_one(".article__content__view__field.additional-posting-locations")
    if not node:
        return False
    return "singapore" in _clean_text(node.get_text(" ", strip=True)).lower()


def _cache_file_path(base_dir: str, folder_id: str, job_url: str) -> str:
    key_fragment = folder_id or job_url or "unknown"
    filename = f"{_safe_key(folder_id or '')}-{hashlib.sha1(key_fragment.encode('utf-8')).hexdigest()[:10]}.json"
    return os.path.join(base_dir, filename)


def _load_cached(path: str, ttl: int) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return None
    except Exception:
        return None

    timestamp = payload.get("timestamp")
    if ttl > 0 and isinstance(timestamp, (int, float)):
        if (time.time() - float(timestamp)) > ttl:
            return None
    return payload


def _save_cached(path: str, payload: Dict[str, Any]) -> None:
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp_path, path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


class AvatureCollector(BaseCollector):
    name = "avature"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "listing_items": 0,
            "jobinfo_attempts": 0,
            "cache_hits": 0,
            "cache_saves": 0,
            "skipped_non_sg": 0,
        }

        listing_url = company.careers_url
        if not listing_url:
            error_result = CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=[],
                meta=meta,
                error="Missing careers_url",
            )
            return error_result

        fast_mode = bool(getattr(self, "fast_mode", False))
        cache_enabled = bool(getattr(self, "cache_enabled", False))
        base_cache_dir = getattr(self, "cache_dir", None) if cache_enabled else None
        cache_ttl = int(getattr(self, "cache_ttl", 900) or 0)
        cache_ttl = max(cache_ttl, 0)

        cache_dir = None
        if cache_enabled and isinstance(base_cache_dir, str) and base_cache_dir.strip():
            cache_dir = os.path.join(base_cache_dir, "avature", _slugify(company.company))
            os.makedirs(cache_dir, exist_ok=True)
        else:
            cache_enabled = False

        configured_workers = getattr(self, "job_workers", None)

        try:
            with _make_session() as session:
                listing_html = _fetch_html(session, listing_url, timeout_s=30)
                listing_items = _parse_listing(listing_html, listing_url)
                meta["listing_items"] = len(listing_items)

                if not listing_items:
                    return CollectResult(
                        collector=self.name,
                        company=company.company,
                        careers_url=company.careers_url,
                        raw_jobs=[],
                        meta=meta,
                        error=None,
                    )

                def load_cached(folder_id: str, job_url: str) -> Optional[Dict[str, Any]]:
                    if not cache_enabled or not cache_dir:
                        return None
                    path = _cache_file_path(cache_dir, folder_id, job_url)
                    return _load_cached(path, cache_ttl)

                def save_cached(folder_id: str, job_url: str, keep: bool, record: Optional[Dict[str, Any]]) -> None:
                    if not cache_enabled or not cache_dir:
                        return
                    payload = {
                        "timestamp": time.time(),
                        "keep": bool(keep),
                        "record": record if keep else None,
                    }
                    path = _cache_file_path(cache_dir, folder_id, job_url)
                    _save_cached(path, payload)
                    meta["cache_saves"] += 1

                def fetch_fields(item: Dict[str, str]) -> Optional[Dict[str, Any]]:
                    folder_id = item.get("folder_id") or ""
                    job_url = item.get("job_url") or ""
                    if not folder_id:
                        return None

                    cached_entry = load_cached(folder_id, job_url)
                    if cached_entry is not None:
                        meta["cache_hits"] += 1
                        if cached_entry.get("keep"):
                            cached_record = cached_entry.get("record")
                            if isinstance(cached_record, dict):
                                return cached_record
                        meta["skipped_non_sg"] += 1
                        return None

                    try:
                        fields = _fetch_jobinfo_fields(session, listing_url=listing_url, folder_id=folder_id, timeout_s=30)
                        meta["jobinfo_attempts"] += 1
                    except Exception:
                        return None

                    location = _build_location(fields)
                    country = fields.get("Country / Region") or ""

                    keep = "singapore" in location.lower() or "singapore" in (country or "").lower()
                    detail_html: str = ""
                    posted_date = ""

                    if not keep and job_url:
                        try:
                            detail_html = _fetch_html(session, job_url, timeout_s=30)
                            keep = _additional_posting_locations_include_sg(detail_html)
                        except Exception:
                            keep = False

                    if keep:
                        if detail_html:
                            posted_date = _extract_posted_date_from_folderdetail(detail_html)
                        elif job_url and not fast_mode:
                            try:
                                detail_html = _fetch_html(session, job_url, timeout_s=30)
                                posted_date = _extract_posted_date_from_folderdetail(detail_html)
                            except Exception:
                                posted_date = ""

                    if not keep:
                        meta["skipped_non_sg"] += 1
                        save_cached(folder_id, job_url, keep=False, record=None)
                        return None

                    job_record = {
                        "job_id": folder_id,
                        "title": item.get("job_title") or "",
                        "location": location,
                        "posted_date": posted_date,
                        "job_url": job_url,
                        "fields": fields,
                    }

                    save_cached(folder_id, job_url, keep=True, record=job_record)
                    return job_record

                max_workers: int
                if isinstance(configured_workers, int) and configured_workers > 0:
                    max_workers = configured_workers
                else:
                    max_workers = 16 if fast_mode else 8
                max_workers = max(1, min(max_workers, len(listing_items)))

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    for record in executor.map(fetch_fields, listing_items):
                        if record:
                            raw_jobs.append(record)

            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )
        except Exception as exc:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(exc),
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        records: List[JobRecord] = []
        company_lower = (result.company or "").casefold()
        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            location = _clean_text(raw.get("location"))
            if "siemens energy" in company_lower:
                location = "Singapore, Central Singapore"

            records.append(
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
        return records
