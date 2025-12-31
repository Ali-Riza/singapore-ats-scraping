from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


def _session_with_retries() -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def _find_json_object_bounds(text: str, start_idx: int) -> Tuple[int, int]:
    """Return (start,end) indices (inclusive) for a JSON object starting at first '{' >= start_idx."""
    start = text.find("{", start_idx)
    if start < 0:
        raise RuntimeError("Could not find JSON object start.")

    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
                continue
            if ch == "\\":
                esc = True
                continue
            if ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return start, i
            continue

    raise RuntimeError("Could not find JSON object end.")


def _extract_preload_state(html: str) -> Dict[str, Any]:
    marker = "window.__PRELOAD_STATE__"
    idx = html.find(marker)
    if idx < 0:
        raise RuntimeError("Could not find window.__PRELOAD_STATE__ in HTML.")

    eq = html.find("=", idx)
    if eq < 0:
        raise RuntimeError("Could not parse window.__PRELOAD_STATE__ assignment.")

    start, end = _find_json_object_bounds(html, eq)
    blob = html[start : end + 1]
    return json.loads(blob)


def _build_page_url(careers_url: str, page_number: int, default_country: str) -> Tuple[str, str]:
    """Return (page_url, target_country)."""
    parsed = urlparse(careers_url)
    qs = parse_qs(parsed.query, keep_blank_values=True)

    # Maintain existing filters if present; default to Singapore-style filter.
    target_country = (qs.get("filter[country][0]") or [default_country])[0]
    qs["filter[country][0]"] = [target_country]

    qs["page_number"] = [str(page_number)]

    new_query = urlencode(qs, doseq=True)
    page_url = urlunparse(parsed._replace(query=new_query))
    return page_url, target_country


def _extract_location(job: Dict[str, Any]) -> str:
    locs = job.get("locations") or []
    texts: List[str] = []
    for loc in locs:
        if not isinstance(loc, dict):
            continue
        t = (
            loc.get("locationName")
            or loc.get("locationParsedText")
            or loc.get("locationText")
            or loc.get("cityState")
            or ""
        )
        t = _clean_text(str(t))
        if t:
            texts.append(t)

    seen: set[str] = set()
    uniq: List[str] = []
    for t in texts:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return " | ".join(uniq)


def _is_target_country_job(job: Dict[str, Any], target_country: str) -> bool:
    tc = (target_country or "").strip().lower()
    if not tc:
        return True

    for loc in (job.get("locations") or []):
        if not isinstance(loc, dict):
            continue
        if (loc.get("country") or "").strip().lower() == tc:
            return True

    for cf in (job.get("customFields") or []):
        if not isinstance(cf, dict):
            continue
        if cf.get("cfKey") == "cf_primary_location_country":
            if (cf.get("value") or "").strip().lower() == tc:
                return True

    return False


def _posted_date(job: Dict[str, Any]) -> str:
    for cf in (job.get("customFields") or []):
        if not isinstance(cf, dict):
            continue
        if cf.get("cfKey") == "cf_posting_start_date" and cf.get("value"):
            return str(cf["value"]).strip()

    d = (job.get("createDate") or job.get("updatedDate") or "")
    d = str(d)
    return d[:10] if d else ""


@dataclass(frozen=True)
class EmbeddedStateConfig:
    default_country: str = "Singapore"
    max_pages: int = 50


class EmbeddedStateCollector(BaseCollector):
    """Collector for career sites that embed a full job list in window.__PRELOAD_STATE__.

    Currently implemented for GE Vernova / GE Grid Solutions style pages:
    - Listing: HTML pages with `window.__PRELOAD_STATE__ = {...}`
    - Jobs: state['jobSearch']['jobs']
    - Pagination: query param `page_number`
    - Country filter: `filter[country][0]`
    """

    name = "embeddedstate"

    def __init__(self, cfg: Optional[EmbeddedStateConfig] = None):
        self.cfg = cfg or EmbeddedStateConfig()

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        session = _session_with_retries()

        meta: Dict[str, Any] = {
            "pages": 0,
            "status_codes": [],
            "target_country": None,
            "stopped_reason": None,
        }

        raw_jobs: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()

        base_url = company.careers_url
        if not base_url:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=[],
                meta=meta,
                error="Missing careers_url.",
            )

        for page in range(1, self.cfg.max_pages + 1):
            page_url, target_country = _build_page_url(base_url, page, self.cfg.default_country)
            meta["target_country"] = target_country

            r = session.get(page_url, timeout=30)
            meta["pages"] += 1
            meta["status_codes"].append(r.status_code)
            r.raise_for_status()

            state = _extract_preload_state(r.text)
            jobs = (state.get("jobSearch") or {}).get("jobs") or []

            if not jobs:
                meta["stopped_reason"] = "empty_page"
                break

            new_on_page = 0
            for j in jobs:
                if not isinstance(j, dict):
                    continue

                if not _is_target_country_job(j, target_country):
                    continue

                job_id = str(j.get("requisitionID") or j.get("reference") or "").strip()
                original = str(j.get("originalURL") or "").lstrip("/")
                job_url = f"https://{urlparse(base_url).netloc}/{original}" if original else page_url
                key = job_id or job_url

                if not key or key in seen_keys:
                    continue

                seen_keys.add(key)
                raw_jobs.append({"job": j, "page_url": page_url, "job_url": job_url})
                new_on_page += 1

            if new_on_page == 0:
                meta["stopped_reason"] = "no_new_jobs"
                break

        return CollectResult(
            collector=self.name,
            company=company.company,
            careers_url=company.careers_url,
            raw_jobs=raw_jobs,
            meta=meta,
            error=None,
        )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        out: List[JobRecord] = []
        for item in result.raw_jobs:
            job = (item or {}).get("job") or {}
            if not isinstance(job, dict):
                continue

            title = _clean_text(str(job.get("title") or ""))
            location = _clean_text(_extract_location(job))
            job_id = _clean_text(str(job.get("requisitionID") or job.get("reference") or ""))
            posted_date = _clean_text(_posted_date(job))
            job_url = str(item.get("job_url") or item.get("page_url") or result.careers_url)

            company_name = str(job.get("companyName") or result.company or "").strip() or result.company

            out.append(
                JobRecord(
                    company=company_name,
                    job_title=title,
                    location=location,
                    job_id=job_id,
                    posted_date=posted_date,
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw={"job": job, "page_url": item.get("page_url")},
                )
            )
        return out
