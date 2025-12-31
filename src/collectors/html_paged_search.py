from __future__ import annotations

import email.utils
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


def _session_with_retries(headers: Optional[Dict[str, str]] = None) -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
            "Connection": "keep-alive",
        }
    )
    if headers:
        s.headers.update(headers)
    return s


def _get_int_query_param(url: str, key: str) -> Optional[int]:
    u = urlparse(url)
    q = parse_qs(u.query)
    vals = q.get(key)
    if not vals:
        return None
    try:
        return int(vals[0])
    except Exception:
        return None


def _build_offset_url(base_url: str, *, offset: int, per_page: int) -> str:
    """Generic paging via query params: folderOffset + folderRecordsPerPage."""
    u = urlparse(base_url)
    q = parse_qs(u.query)
    q["folderOffset"] = [str(offset)]
    q["folderRecordsPerPage"] = [str(per_page)]
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))


def _normalize_job_url(base_host: str, href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"https://{base_host}{href}"
    return f"https://{base_host}/" + href.lstrip("/")


def _parse_siemens_job_cards(html: str, careers_url: str) -> List[Dict[str, Any]]:
    """Parse Siemens listing HTML into raw job dicts."""
    soup = BeautifulSoup(html, "html.parser")
    articles = soup.select("article.article--result")

    base_host = urlparse(careers_url).netloc or "jobs.siemens.com"

    out: List[Dict[str, Any]] = []
    for art in articles:
        title_a = art.select_one("h3 a.link")
        if not title_a:
            continue

        job_title = _clean_text(title_a.get_text(strip=True))
        job_url = _normalize_job_url(base_host, title_a.get("href", ""))

        loc_span = art.select_one("span.list-item-location")
        location = _clean_text(loc_span.get_text(" ", strip=True)) if loc_span else ""

        jobid_span = art.select_one("span.list-item-jobId")
        job_id = ""
        if jobid_span:
            m = re.search(r"Job ID:\s*(\d+)", jobid_span.get_text(strip=True))
            if m:
                job_id = m.group(1)

        out.append(
            {
                "title": job_title,
                "location": location,
                "job_id": job_id,
                "posted_date": "",
                "job_url": job_url,
                "careers_url": careers_url,
            }
        )

    return out


def _extract_siemens_total_results(html: str) -> Optional[int]:
    """Extract total results count from Siemens HTML.

    Observed snippets include:
      - "of 52" (in the results header)
      - "52 results"
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
    except Exception:
        text = html

    for pat in (
        r"\bof\s+(\d{1,5})\b",
        r"\b(\d{1,5})\s+results\b",
    ):
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                continue
    return None


def _build_siemens_feed_url(careers_url: str) -> str:
    """Convert Siemens SearchJobs listing URL to RSS feed URL.

    Example:
      /externaljobs/SearchJobs/  -> /externaljobs/SearchJobs/feed/
    """
    u = urlparse(careers_url)
    path = u.path or ""
    if "/SearchJobs/" in path:
        path = path.replace("/SearchJobs/", "/SearchJobs/feed/", 1)
    elif path.rstrip("/").endswith("/SearchJobs"):
        path = path.rstrip("/") + "/feed/"
    else:
        return careers_url

    return urlunparse((u.scheme, u.netloc, path, u.params, u.query, u.fragment))


def _parse_rss_pubdate_to_iso(pub_date: str) -> str:
    """RSS pubDate example: 'Thu, 04 Apr 2024 00:00:00 +0000' -> '2024-04-04'."""
    raw = (pub_date or "").strip()
    if not raw:
        return ""
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt is None:
            return ""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.date().isoformat()
    except Exception:
        if re.match(r"\d{4}-\d{2}-\d{2}", raw):
            return raw[:10]
        return ""


def _extract_job_id_from_url(url: str) -> str:
    m = re.search(r"/(\d{4,})/?$", (url or "").strip())
    return m.group(1) if m else ""


def _parse_siemens_posted_since_to_iso(raw: str) -> str:
    """Siemens job detail field 'Posted since' is often like '30-Dec-2025'."""
    s = _clean_text(raw)
    if not s:
        return ""

    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return ""


def _extract_siemens_posted_date_from_detail_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for block in soup.select(".article__content__view__field"):
        lab = block.select_one(".article__content__view__field__label")
        if not lab:
            continue
        label = _clean_text(lab.get_text(" ", strip=True)).lower()
        if label == "posted since":
            val = block.select_one(".article__content__view__field__value")
            return _parse_siemens_posted_since_to_iso(val.get_text(" ", strip=True) if val else "")
    return ""


def _parse_siemens_rss(xml_text: str, careers_url: str) -> List[Dict[str, Any]]:
    """Parse Siemens RSS feed into raw job dicts."""
    root = ET.fromstring(xml_text)
    channel = root.find("channel")
    if channel is None:
        return []

    out: List[Dict[str, Any]] = []
    for item in channel.findall("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        location = (item.findtext("description") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()

        out.append(
            {
                "title": _clean_text(title),
                "location": _clean_text(location),
                "job_id": _extract_job_id_from_url(link),
                "posted_date": _parse_rss_pubdate_to_iso(pub),
                "job_url": link,
                "careers_url": careers_url,
            }
        )

    return out


@dataclass(frozen=True)
class HtmlPagedSearchConfig:
    max_pages: int = 500
    default_per_page: int = 10
    siemens_html_per_page: int = 50


class HtmlPagedSearchCollector(BaseCollector):
    """Hybrid collector for HTML-paged search pages: RSS-first, HTML fallback.

    - Try RSS feed (fast) when available
    - If RSS is incomplete/capped, fetch the full set via HTML paging

    Note: `name` stays `htmlpagedsearch` as the canonical ATS key.
    """

    name = "htmlpagedsearch"

    def __init__(self, cfg: Optional[HtmlPagedSearchConfig] = None):
        self.cfg = cfg or HtmlPagedSearchConfig()

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        meta: Dict[str, Any] = {
            "pages": 0,
            "per_page": None,
            "status_codes": [],
            "stopped_reason": None,
            "mode": None,
            "total_results": None,
            "rss_capped": None,
        }

        careers_url = (company.careers_url or "").strip()
        if not careers_url:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=[],
                meta=meta,
                error="Missing careers_url.",
            )

        session = _session_with_retries(
            headers={
                "Referer": "https://jobs.siemens.com/",
                "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, text/html;q=0.7, */*;q=0.5",
            }
        )

        per_page = _get_int_query_param(careers_url, "folderRecordsPerPage")
        if not per_page or per_page <= 0:
            per_page = self.cfg.default_per_page
        meta["per_page"] = per_page

        feed_url = _build_siemens_feed_url(careers_url)
        rss_jobs: List[Dict[str, Any]] = []
        if feed_url != careers_url:
            try:
                rss_url = _build_offset_url(feed_url, offset=0, per_page=max(per_page, 50))
                rr = session.get(rss_url, timeout=30)
                meta["pages"] += 1
                meta["status_codes"].append(rr.status_code)
                if rr.status_code < 400 and rr.status_code != 406:
                    rss_jobs = _parse_siemens_rss(rr.text, careers_url=careers_url)
            except Exception:
                rss_jobs = []

        requested_per_page = max(per_page, int(getattr(self.cfg, "siemens_html_per_page", per_page)))
        meta["mode"] = "html" if feed_url == careers_url else "rss+html"
        all_jobs: List[Dict[str, Any]] = []
        seen: set[str] = set()

        first_url = _build_offset_url(careers_url, offset=0, per_page=requested_per_page)
        r0 = session.get(first_url, timeout=30)
        meta["pages"] += 1
        meta["status_codes"].append(r0.status_code)
        if r0.status_code == 406:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=[],
                meta=meta,
                error=(
                    "HTTP 406 (Not Acceptable) from careers site. "
                    "Try varying headers/user-agent or consider a Playwright-based fetch."
                ),
            )
        r0.raise_for_status()
        first_jobs = _parse_siemens_job_cards(r0.text, careers_url=careers_url)
        total = _extract_siemens_total_results(r0.text)
        meta["total_results"] = total

        if rss_jobs and total and len(rss_jobs) < total:
            meta["rss_capped"] = True
        elif rss_jobs and total and len(rss_jobs) >= total:
            meta["rss_capped"] = False
        else:
            meta["rss_capped"] = None

        if rss_jobs and (meta.get("rss_capped") is False or (total and len(rss_jobs) == total)):
            meta["mode"] = "rss"
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=rss_jobs,
                meta=meta,
                error=None,
            )

        def _add_jobs(jobs: List[Dict[str, Any]]) -> int:
            new_count = 0
            for j in jobs:
                key = (j.get("job_id") or "").strip() or (j.get("job_url") or "")
                if not key or key in seen:
                    continue
                seen.add(key)
                all_jobs.append(j)
                new_count += 1
            return new_count

        _add_jobs(first_jobs)
        effective_page_size = len(first_jobs) if first_jobs else requested_per_page

        if not first_jobs:
            meta["stopped_reason"] = "empty_page"
        else:
            if total and total > len(first_jobs):
                offsets = list(range(effective_page_size, total, effective_page_size))

                def _fetch_offset(off: int) -> Dict[str, Any]:
                    sess = _session_with_retries(
                        headers={
                            "Referer": "https://jobs.siemens.com/",
                            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        }
                    )
                    url = _build_offset_url(careers_url, offset=off, per_page=requested_per_page)
                    rr = sess.get(url, timeout=30)
                    return {"_status": rr.status_code, "_html": rr.text}

                max_workers = min(8, max(1, len(offsets)))
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futs = {ex.submit(_fetch_offset, off): off for off in offsets}
                    for fut in as_completed(futs):
                        try:
                            payload = fut.result()
                        except Exception:
                            continue
                        status = payload.get("_status")
                        html = payload.get("_html")
                        meta["pages"] += 1
                        meta["status_codes"].append(status)
                        if status == 406 or (status and int(status) >= 400):
                            continue
                        jobs = _parse_siemens_job_cards(str(html or ""), careers_url=careers_url)
                        _add_jobs(jobs)
            else:
                offset = len(first_jobs)
                for _page_idx in range(self.cfg.max_pages - 1):
                    page_url = _build_offset_url(careers_url, offset=offset, per_page=requested_per_page)
                    r = session.get(page_url, timeout=30)
                    meta["pages"] += 1
                    meta["status_codes"].append(r.status_code)
                    if r.status_code == 406:
                        break
                    r.raise_for_status()
                    jobs = _parse_siemens_job_cards(r.text, careers_url=careers_url)
                    if not jobs:
                        meta["stopped_reason"] = "empty_page"
                        break
                    new_count = _add_jobs(jobs)
                    if new_count == 0:
                        meta["stopped_reason"] = "no_new_jobs"
                        break
                    offset += len(jobs)

        if rss_jobs:
            by_key: Dict[str, Dict[str, Any]] = {}
            for j in all_jobs:
                key = (j.get("job_id") or "").strip() or (j.get("job_url") or "")
                if key:
                    by_key[key] = j

            for rj in rss_jobs:
                key = (rj.get("job_id") or "").strip() or (rj.get("job_url") or "")
                if not key:
                    continue
                tgt = by_key.get(key)
                if tgt is None:
                    all_jobs.append(rj)
                    continue
                if not tgt.get("posted_date") and rj.get("posted_date"):
                    tgt["posted_date"] = rj.get("posted_date")
                if not tgt.get("location") and rj.get("location"):
                    tgt["location"] = rj.get("location")
                if not tgt.get("title") and rj.get("title"):
                    tgt["title"] = rj.get("title")

        # Siemens: HTML paging does not include posted_date; backfill from job detail pages.
        # RSS provides pubDate for some jobs, but when capped we may have many empty dates.
        missing = [j for j in all_jobs if not _clean_text(str(j.get("posted_date") or "")) and _clean_text(str(j.get("job_url") or ""))]
        meta["detail_fetches"] = 0
        meta["detail_filled"] = 0
        meta["detail_errors"] = 0

        def _should_detail_fetch(job: Dict[str, Any]) -> bool:
            url = _clean_text(str(job.get("job_url") or ""))
            if not url:
                return False
            host = (urlparse(url).netloc or "").lower()
            if host != "jobs.siemens.com":
                return False
            return "/jobdetail/" in url.lower()

        targets = [j for j in missing if _should_detail_fetch(j)]
        if targets:
            def _fetch_one(job_url: str) -> str:
                sess = _session_with_retries(
                    headers={
                        "Referer": "https://jobs.siemens.com/",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    }
                )
                rr = sess.get(job_url, timeout=30)
                if rr.status_code >= 400:
                    return ""
                return _extract_siemens_posted_date_from_detail_html(rr.text)

            max_workers = min(8, max(1, len(targets)))
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = {ex.submit(_fetch_one, _clean_text(str(j.get("job_url") or ""))): j for j in targets}
                for fut in as_completed(futs):
                    meta["detail_fetches"] += 1
                    j = futs[fut]
                    try:
                        posted = fut.result()
                    except Exception:
                        meta["detail_errors"] += 1
                        continue
                    if posted:
                        j["posted_date"] = posted
                        meta["detail_filled"] += 1

        return CollectResult(
            collector=self.name,
            company=company.company,
            careers_url=company.careers_url,
            raw_jobs=all_jobs,
            meta=meta,
            error=None,
        )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        out: List[JobRecord] = []
        for raw in result.raw_jobs:
            out.append(
                JobRecord(
                    company=result.company,
                    job_title=_clean_text(str(raw.get("title") or "")),
                    location=_clean_text(str(raw.get("location") or "")),
                    job_id=_clean_text(str(raw.get("job_id") or "")),
                    posted_date=_clean_text(str(raw.get("posted_date") or "")),
                    job_url=str(raw.get("job_url") or ""),
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return out


# Backwards-compatible / descriptive aliases
RssHtmlFallbackSearchCollector = HtmlPagedSearchCollector
SiemensRssSearchCollector = HtmlPagedSearchCollector
