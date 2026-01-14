from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from html import unescape as html_unescape
from typing import Any, Dict, List
from urllib.parse import quote, urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


_CSRF_RE = re.compile(r'var\s+CSRFToken\s*=\s*"([^"]+)"')


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _make_session(origin: str, referer: str) -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Origin": origin,
            "Referer": referer,
        }
    )
    return s


def _origin_from_url(url: str) -> str:
    u = urlparse(url)
    if not u.scheme or not u.netloc:
        return ""
    return f"{u.scheme}://{u.netloc}"


def _looks_singapore(s: str) -> bool:
    t = (s or "").casefold()
    return "singapore" in t or t.strip() in ("sg",)


def _value_to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, dict):
        for k in ("value", "label", "text", "name", "title"):
            if k in v:
                s = _value_to_str(v.get(k))
                if s:
                    return s
        return ""
    if isinstance(v, list) and v:
        return _value_to_str(v[0])
    return ""


def _pick_str(raw: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        if k in raw:
            s = _clean_text(_value_to_str(raw.get(k)))
            if s:
                return s
    return ""


def _extract_date_str(v: Any) -> str:
    if v is None:
        return ""

    if isinstance(v, dict):
        for k in ("value", "date", "iso", "raw", "text"):
            if k in v:
                s = _extract_date_str(v.get(k))
                if s:
                    return s
        return ""

    if isinstance(v, (int, float)):
        ts = float(v)
        if ts > 10_000_000_000:  # likely ms
            ts = ts / 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        except Exception:
            return ""

    s = _clean_text(_value_to_str(v))
    if not s:
        return ""

    # Formats seen in TÜV SÜD payload: e.g. "11/7/25" or "2/27/26"
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2}", s):
        try:
            dt = datetime.strptime(s, "%m/%d/%y")
            return dt.date().isoformat()
        except Exception:
            pass
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s):
        try:
            dt = datetime.strptime(s, "%m/%d/%Y")
            return dt.date().isoformat()
        except Exception:
            pass

    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)

    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date().isoformat()
        except Exception:
            continue

    return ""


def _pick_date(raw: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        if k in raw:
            d = _extract_date_str(raw.get(k))
            if d:
                return d
    return ""


def _build_tuvsud_detail_url(*, origin: str, raw: Dict[str, Any], job_id: str) -> str:
    # Observed working format (user-provided):
    # https://jobs.tuvsud.com/job/Failure-Analysis-Consultant/2342-en_US
    if not origin or not job_id:
        return ""

    slug = _pick_str(raw, ["unifiedUrlTitle", "urlTitle", "jobUrlTitle"])
    if not slug:
        return ""

    # Normalize entities (&amp;) but preserve existing percent-escapes (%2C, %28, ...)
    slug = html_unescape(slug)
    slug_encoded = quote(slug, safe="-%_().,%")

    locale = "en_US"
    supported = raw.get("supportedLocales")
    if isinstance(supported, list) and supported:
        cand = _clean_text(_value_to_str(supported[0]))
        if cand:
            locale = cand

    return urljoin(origin.rstrip("/") + "/", f"job/{slug_encoded}/{job_id}-{locale}")


class TuvSudRecruitingApiCollector(BaseCollector):
    """TÜV SÜD careers endpoint (requires CSRF token + POST JSON)."""

    name = "tuvsud_recruiting_api"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "status": [], "totalJobs": None}

        try:
            search_url = company.careers_url
            origin = _origin_from_url(search_url)
            if not origin:
                raise ValueError("Invalid careers_url")

            api_url = origin + "/services/recruiting/v1/jobs"

            with _make_session(origin, search_url) as session:
                # Bootstrap: fetch page to obtain cookies + CSRF
                r0 = session.get(search_url, timeout=30)
                meta["status"].append(r0.status_code)
                r0.raise_for_status()

                m = _CSRF_RE.search(r0.text or "")
                if m:
                    session.headers["X-CSRF-Token"] = m.group(1)

                page = 0
                total_jobs: int | None = None
                max_pages: int | None = None
                page_size_guess = 10

                while page < 1000:  # safety cap
                    payload: Dict[str, Any] = {
                        "locale": "en_US",
                        "pageNumber": page,
                        "sortBy": "",
                        "keywords": "",
                        "location": "",
                        "facetFilters": {
                            "cust_brand": ["TÜV SÜD"],
                            "jobLocationCountry": ["Singapore"],
                        },
                        "brand": "",
                        "skills": [],
                        "categoryId": 0,
                        "alertId": "",
                        "rcmCandidateId": "",
                    }

                    r = session.post(api_url, json=payload, timeout=30)
                    meta["status"].append(r.status_code)
                    r.raise_for_status()
                    js = r.json() if isinstance(r.json(), dict) else {}

                    if total_jobs is None:
                        try:
                            total_jobs = int(js.get("totalJobs") or 0)
                        except Exception:
                            total_jobs = 0
                        meta["totalJobs"] = total_jobs
                        if total_jobs > 0:
                            max_pages = math.ceil(total_jobs / page_size_guess)

                    results = js.get("jobSearchResult") or []
                    if not isinstance(results, list) or not results:
                        break

                    for item in results:
                        if not isinstance(item, dict):
                            continue
                        resp = item.get("response")
                        raw = resp if isinstance(resp, dict) else item
                        raw_jobs.append(raw)

                    page += 1
                    meta["pages"] = page

                    if max_pages is not None and page >= max_pages:
                        break

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
        origin = _origin_from_url(result.careers_url)

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            job_id = _pick_str(
                raw,
                [
                    "jobId",
                    "jobSeqNo",
                    "jobReqId",
                    "jobRequisitionId",
                    "requisitionId",
                    "jobPostingId",
                    "postingId",
                    "id",
                ],
            )
            title = _pick_str(
                raw,
                [
                    "jobTitle",
                    "postingTitle",
                    "jobPostingTitle",
                    "positionTitle",
                    "unifiedStandardTitle",
                    "title",
                    "name",
                ],
            )
            posted = _pick_date(
                raw,
                [
                    "postingStartDate",
                    "postedDate",
                    "postingDate",
                    "datePosted",
                    "postedOn",
                    "createdAt",
                    "unifiedStandardStart",
                ],
            )

            location = _pick_str(
                raw,
                [
                    "jobLocation",
                    "jobLocationDisplay",
                    "location",
                    "jobLocationCity",
                    "jobLocationShort",
                    "jobLocationShortWithCoordinates",
                ],
            )
            if not location:
                country = _pick_str(raw, ["jobLocationCountry", "country"]) or "Singapore"
                location = country

            if not _looks_singapore(location):
                continue

            job_url = _clean_text(
                raw.get("jobDetailUrl")
                or raw.get("detailUrl")
                or raw.get("applyUrl")
                or raw.get("externalApplyUrl")
                or raw.get("externalUrl")
                or raw.get("url")
            )
            if job_url and origin and job_url.startswith("/"):
                job_url = urljoin(origin, job_url)
            if not job_url:
                job_url = _build_tuvsud_detail_url(origin=origin, raw=raw, job_id=job_id) or result.careers_url

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=location,
                    job_id=job_id,
                    posted_date=posted,
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return out
