from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


_JOB_REF_RE = re.compile(r"\b[\w-]+/TP/\d+/\d+\b", re.IGNORECASE)
_DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{2,4})\b")
_RECORD_RE = re.compile(r"[?&]record=(\d+)\b")


def _clean_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


def _with_query(url: str, **params: Any) -> str:
    """Set/override query params on url; keeps other params."""
    u = urlparse(url)
    q = parse_qs(u.query, keep_blank_values=True)
    for k, v in params.items():
        if v is None:
            continue
        q[k] = [str(v)]
    new_query = urlencode({k: vs[0] for k, vs in q.items() if vs and vs[0] not in (None, "")}, doseq=False)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


def _extract_filters(seed_url: str) -> Dict[str, str]:
    """Carry over seed filters like location_country=200; ignore empty/-1 values."""
    u = urlparse(seed_url)
    q = parse_qs(u.query, keep_blank_values=True)
    out: Dict[str, str] = {}
    for k, vs in q.items():
        v = (vs[0] if vs else "")
        if v in ("", "-1"):
            continue
        out[k] = v
    return out


def _discover_listing_url(seed_url: str) -> str:
    """Tribepad commonly supports /v2/view%20jobs as a generic listing."""
    u = urlparse(seed_url)
    base = f"{u.scheme}://{u.netloc}"
    return urljoin(base, "/v2/view%20jobs")


@dataclass(frozen=True)
class _ListingHit:
    job_url: str
    record_id: str
    job_reference: Optional[str]
    posted_date: Optional[str]


def _parse_listing_page(html: str, base_url: str) -> List[_ListingHit]:
    """Extract job links from a Tribepad listing page.

    Typical job links: /members/modules/job/detail.php?record=XYZ
    """
    soup = BeautifulSoup(html, "html.parser")

    hits: List[_ListingHit] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        if "members/modules/job/detail.php" not in href:
            continue

        abs_url = urljoin(base_url, href)
        m = _RECORD_RE.search(abs_url)
        if not m:
            continue

        record_id = m.group(1)
        text = _clean_text(a.get_text(" ", strip=True))

        job_ref = None
        mref = _JOB_REF_RE.search(text)
        if mref:
            job_ref = mref.group(0)

        dates = _DATE_RE.findall(text)
        posted = dates[-1] if dates else None

        hits.append(
            _ListingHit(
                job_url=abs_url,
                record_id=record_id,
                job_reference=job_ref,
                posted_date=posted,
            )
        )

    uniq: Dict[str, _ListingHit] = {}
    for h in hits:
        uniq[h.record_id] = h
    return list(uniq.values())


def _parse_detail_page(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract (job_title, location, job_reference) from a Tribepad detail page."""
    soup = BeautifulSoup(html, "html.parser")

    title = None
    h = soup.find(["h1", "h2"])
    if h:
        title = _clean_text(h.get_text(" ", strip=True)) or None

    location = None
    text = soup.get_text("\n", strip=True)
    mloc = re.search(r"\bLocation:\s*\n\s*([^\n]+)", text)
    if mloc:
        location = _clean_text(mloc.group(1)) or None

    job_ref = None
    mref = re.search(r"\bJob Reference\s+([^\s]+/TP/\d+/\d+)\b", text, flags=re.IGNORECASE)
    if mref:
        job_ref = _clean_text(mref.group(1)) or None

    return title, location, job_ref


class TribepadCollector(BaseCollector):
    """Collector for Tribepad career sites.

    SRP: only (Z3) fetch raw jobs and (Z4) map to JobRecord.
    """

    name = "tribepad"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        max_pages = 50

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "visited_urls": [],
            "status_codes": [],
            "total_raw": 0,
            "detail_fetches": 0,
            "used_fallback_listing": False,
        }

        try:
            session = requests.Session()
            session.headers.update(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-GB,en;q=0.9",
                    "Connection": "keep-alive",
                }
            )

            seed_url = company.careers_url
            seed_resp = session.get(seed_url, timeout=30)
            meta["status_codes"].append(seed_resp.status_code)
            seed_resp.raise_for_status()

            meta["visited_urls"].append(seed_url)
            meta["pages"] += 1

            seed_hits = _parse_listing_page(seed_resp.text, base_url=seed_url)
            filters = _extract_filters(seed_url)

            if seed_hits:
                # Use seed-based pagination (keep path, preserve filters)
                listing_base = urlunparse(urlparse(seed_url)._replace(query=""))
                listing_base = _with_query(listing_base, **filters)
            else:
                # Fall back to /v2/view%20jobs but carry the filters from seed URL
                listing_base = _discover_listing_url(seed_url)
                listing_base = _with_query(listing_base, **filters)
                meta["used_fallback_listing"] = True

            seen_records: Set[str] = set()

            for page in range(1, max_pages + 1):
                page_url = _with_query(listing_base, page=page)
                resp = session.get(page_url, timeout=30)
                meta["status_codes"].append(resp.status_code)
                resp.raise_for_status()

                meta["visited_urls"].append(page_url)
                meta["pages"] += 1

                page_hits = _parse_listing_page(resp.text, base_url=page_url)
                if not page_hits:
                    break

                for h in page_hits:
                    if h.record_id in seen_records:
                        continue
                    seen_records.add(h.record_id)

                    try:
                        d = session.get(h.job_url, timeout=30)
                        meta["status_codes"].append(d.status_code)
                        d.raise_for_status()

                        job_title, location, job_ref_detail = _parse_detail_page(d.text)
                        meta["detail_fetches"] += 1

                        job_reference = h.job_reference or job_ref_detail

                        raw_jobs.append(
                            {
                                "record_id": h.record_id,
                                "job_reference": job_reference or "",
                                "title": job_title or "",
                                "location": location or "",
                                "posted_date": h.posted_date or "",
                                "job_url": h.job_url,
                                "_page_url": page_url,
                            }
                        )
                    except Exception:
                        # Keep going; a single broken detail page shouldn't kill the company.
                        raw_jobs.append(
                            {
                                "record_id": h.record_id,
                                "job_reference": h.job_reference or "",
                                "title": "",
                                "location": "",
                                "posted_date": h.posted_date or "",
                                "job_url": h.job_url,
                                "_page_url": page_url,
                                "_detail_failed": True,
                            }
                        )
                        continue

            meta["total_raw"] = len(raw_jobs)
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )

        except Exception as e:
            meta["total_raw"] = len(raw_jobs)
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(e),
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        records: List[JobRecord] = []
        for raw in result.raw_jobs:
            records.append(self._map_one(raw, result))
        return records

    def _map_one(self, raw: Dict[str, Any], result: CollectResult) -> JobRecord:
        title = _clean_text(str(raw.get("title") or ""))
        location = _clean_text(str(raw.get("location") or ""))
        posted_date = _clean_text(str(raw.get("posted_date") or ""))
        job_url = str(raw.get("job_url") or "")

        job_reference = _clean_text(str(raw.get("job_reference") or ""))
        record_id = _clean_text(str(raw.get("record_id") or ""))
        job_id = job_reference or record_id

        return JobRecord(
            company=result.company,
            job_title=title,
            location=location,
            job_id=job_id,
            posted_date=posted_date,
            job_url=job_url,
            source=self.name,
            careers_url=result.careers_url,
            raw=raw,
        )
