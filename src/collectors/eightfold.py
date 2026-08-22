from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


def _strip_www(host: str) -> str:
    h = (host or "").strip().lower()
    return h[4:] if h.startswith("www.") else h


def _strip_common_subdomain(host: str) -> str:
    """Best-effort: PCS APIs typically want the corporate domain, not the careers subdomain.

    Example: jobs.worley.com -> worley.com
    """
    h = _strip_www(host)
    parts = [p for p in h.split(".") if p]
    if len(parts) >= 3 and parts[0] in {"jobs", "careers", "career", "apply"}:
        return ".".join(parts[1:])
    return h


def _domain_from_company(company: CompanyItem) -> str:
    """Eightfold requires a 'domain' query param (typically the corporate domain).

    For Eaton we get this from the Excel 'Website' column.
    """
    # Allow overriding via careers_url query param if present.
    q = parse_qs(urlparse(company.careers_url).query)
    q_domain = q.get("domain", [None])[0]
    if q_domain:
        return _strip_www(str(q_domain))

    if company.website:
        u = urlparse(company.website)
        if u.netloc:
            return _strip_www(u.netloc)
        # sometimes website is stored without scheme
        u2 = urlparse("https://" + company.website)
        if u2.netloc:
            return _strip_www(u2.netloc)

    # Fallback: use careers host (may work for some tenants, but not guaranteed).
    return _strip_common_subdomain(urlparse(company.careers_url).netloc)


def _extract_pid_from_careers_url(careers_url: str) -> Optional[str]:
    q = parse_qs(urlparse(careers_url).query)
    v = q.get("pid", [None])[0]
    return str(v) if v not in (None, "") else None


def _extract_location_from_careers_url(careers_url: str) -> str:
    q = parse_qs(urlparse(careers_url).query)
    loc = q.get("location", [None])[0]
    loc = str(loc) if loc not in (None, "") else ""
    return loc or "Singapore"


def _extract_sort_from_careers_url(careers_url: str) -> str:
    q = parse_qs(urlparse(careers_url).query)
    sort_by = q.get("sort_by", [None])[0]
    sort_by = str(sort_by) if sort_by not in (None, "") else ""
    return sort_by or "distance"


def _extract_include_remote_from_careers_url(careers_url: str) -> str:
    q = parse_qs(urlparse(careers_url).query)
    v = q.get("filter_include_remote", [None])[0]
    v = str(v) if v not in (None, "") else ""
    return v or "1"


def _extract_hl_from_careers_url(careers_url: str) -> str:
    q = parse_qs(urlparse(careers_url).query)
    v = q.get("hl", [None])[0]
    v = str(v) if v not in (None, "") else ""
    return v or "en"


def _search_api_base(careers_url: str) -> str:
    u = urlparse(careers_url)
    return f"{u.scheme}://{u.netloc}/api/pcsx/search"


def _detail_api_base(careers_url: str) -> str:
    u = urlparse(careers_url)
    return f"{u.scheme}://{u.netloc}/api/pcsx/position_details"


def _job_url_from_id(careers_url: str, job_id: str) -> str:
    u = urlparse(careers_url)
    return f"{u.scheme}://{u.netloc}/careers/job/{job_id}"


def _find_first_list_of_dicts(obj: Any) -> Optional[List[Dict[str, Any]]]:
    """Tolerant finder for the first list[dict] inside a JSON response."""
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            return obj
        for x in obj:
            res = _find_first_list_of_dicts(x)
            if res is not None:
                return res
        return None

    if isinstance(obj, dict):
        for k in (
            "positions",
            "jobs",
            "results",
            "items",
            "data",
            "searchResults",
            "search_results",
        ):
            if k in obj:
                res = _find_first_list_of_dicts(obj[k])
                if res is not None:
                    return res
        for v in obj.values():
            res = _find_first_list_of_dicts(v)
            if res is not None:
                return res

    return None


def _pick(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return default


def _location_strings(job: Dict[str, Any]) -> List[str]:
    """Collect every location string a position exposes.

    PCSX returns `locations` / `standardizedLocations` as string lists and leaves the
    singular `location` key null, so multi-location jobs only reveal Singapore here.
    """
    out: List[str] = []

    def _add(v: Any) -> None:
        if isinstance(v, str):
            s = _clean_text(v)
            if s:
                out.append(s)
        elif isinstance(v, dict):
            _add(_pick(v, ["name", "displayName", "label", "location"], default=""))
        elif isinstance(v, list):
            for x in v:
                _add(x)

    for key in ("locations", "standardizedLocations", "location", "jobLocation", "job_location"):
        _add(job.get(key))

    seen: set[str] = set()
    uniq: List[str] = []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def _matches_location(job: Dict[str, Any], location: str) -> bool:
    """True when the wanted location appears in any of the job's location strings.

    Eightfold treats `location` as a geo anchor combined with `sort_by=distance`, not as
    a hard filter, so distance-sorted results trail off into neighbouring countries.
    """
    needle = _clean_text(location).casefold()
    if not needle:
        return True
    # "Singapore, Singapore" style anchors: match on the most specific token.
    needle = needle.split(",")[0].strip() or needle
    return any(needle in s.casefold() for s in _location_strings(job))


def _normalize_location(job: Dict[str, Any], wanted: str = "") -> str:
    """Prefer the wanted location so multi-location rows don't display a foreign city."""
    locs = _location_strings(job)
    if not locs:
        return ""

    needle = _clean_text(wanted).casefold().split(",")[0].strip()
    if needle:
        preferred = [s for s in locs if needle in s.casefold()]
        if preferred:
            return " | ".join(preferred)
    return locs[0]


def _posted_date_from_posted_ts(posted_ts: Any) -> Optional[str]:
    if posted_ts is None:
        return None
    try:
        v = float(posted_ts)
        if v > 10_000_000_000:  # likely ms
            v = v / 1000.0
        dt = datetime.fromtimestamp(v, tz=timezone.utc)
        if dt.year == 1970:
            return None
        return dt.date().isoformat()
    except Exception:
        return None


@dataclass(frozen=True)
class _EightfoldJob:
    job_id: str
    title: str
    location: str
    job_url: str


class EightfoldCollector(BaseCollector):
    """Collector for Eightfold career sites.

    Uses JSON APIs:
    - /api/pcsx/search
    - /api/pcsx/position_details (for postedTs)

    SRP: only (Z3) fetch raw jobs and (Z4) map to JobRecord.
    """

    name = "eightfold"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        max_pages = 200
        sleep_s = 0.05

        search_timeout_s = 30
        detail_timeout_s = 30
        detail_workers = 10
        detail_budget = 400

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "status_codes": [],
            "search_calls": 0,
            "detail_calls": 0,
            "total_raw": 0,
            "filtered_out": 0,
            "pid": None,
            "domain": None,
            "location": None,
        }

        try:
            domain = _domain_from_company(company)
            pid = _extract_pid_from_careers_url(company.careers_url)
            location = _extract_location_from_careers_url(company.careers_url)
            sort_by = _extract_sort_from_careers_url(company.careers_url)
            include_remote = _extract_include_remote_from_careers_url(company.careers_url)
            hl = _extract_hl_from_careers_url(company.careers_url)

            meta["pid"] = pid
            meta["domain"] = domain
            meta["location"] = location
            meta["hl"] = hl

            search_api = _search_api_base(company.careers_url)
            detail_api = _detail_api_base(company.careers_url)

            session = requests.Session()
            session.headers.update(
                {
                    "accept": "application/json, text/plain, */*",
                    "user-agent": "Mozilla/5.0",
                    "referer": company.careers_url,
                }
            )

            # 1) Search pages
            start = 0
            seen_ids: set[str] = set()

            for _ in range(max_pages):
                params: Dict[str, str] = {
                    "domain": domain,
                    "query": "",
                    "location": location,
                    "start": str(start),
                    "sort_by": sort_by,
                    "filter_include_remote": include_remote,
                }
                if pid:
                    params["pid"] = pid

                url = f"{search_api}?{urlencode(params)}"
                r = session.get(url, timeout=search_timeout_s)
                meta["status_codes"].append(r.status_code)
                meta["search_calls"] += 1
                r.raise_for_status()

                data = r.json()
                jobs_raw = _find_first_list_of_dicts(data) or []
                meta["pages"] += 1

                if not jobs_raw:
                    break

                page_added = 0
                page_matched = 0
                for j in jobs_raw:
                    job_id = _pick(j, ["id", "jobId", "job_id", "reqId", "requisitionId", "requisition_id"], default=None)
                    job_id = str(job_id) if job_id is not None else ""
                    if not job_id or job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)
                    page_added += 1

                    # The API ignores `location` as a hard filter, so enforce it client-side.
                    if not _matches_location(j, location):
                        meta["filtered_out"] += 1
                        continue
                    page_matched += 1

                    raw_jobs.append(
                        {
                            "job_id": job_id,
                            "title": _clean_text(str(_pick(j, ["title", "jobTitle", "job_title", "name", "positionTitle", "position_title"], default=""))),
                            "location": _normalize_location(j, location),
                            "posted_date": "",
                            "job_url": _job_url_from_id(company.careers_url, job_id),
                            "_search_url": url,
                            "_raw": j,
                        }
                    )

                if page_added == 0:
                    break

                # Results are distance-sorted around `location`, so once a full page has no
                # match at all we are past the wanted area and can stop paging.
                if page_matched == 0 and sort_by == "distance":
                    break

                # heuristic: stop when small page
                if len(jobs_raw) < 10:
                    break

                start += len(jobs_raw)
                time.sleep(sleep_s)

            # 2) Enrich posted_date via detail API (bounded)
            def _fetch_posted_date(one_job_id: str) -> tuple[str, Optional[str]]:
                params = {
                    "position_id": one_job_id,
                    "domain": domain,
                    "hl": hl,
                    "queried_location": location,
                }
                url = f"{detail_api}?{urlencode(params)}"
                dr = session.get(url, timeout=detail_timeout_s)
                meta["status_codes"].append(dr.status_code)
                meta["detail_calls"] += 1
                dr.raise_for_status()
                details = dr.json()

                posted_ts = None
                if isinstance(details, dict):
                    data = details.get("data")
                    if isinstance(data, dict):
                        posted_ts = data.get("postedTs")

                return one_job_id, _posted_date_from_posted_ts(posted_ts)

            need_detail = [r for r in raw_jobs if not r.get("posted_date") and r.get("job_id")]
            need_detail = need_detail[:detail_budget]

            if need_detail:
                id_to_date: Dict[str, str] = {}
                with ThreadPoolExecutor(max_workers=detail_workers) as ex:
                    futures = [ex.submit(_fetch_posted_date, str(item["job_id"])) for item in need_detail]
                    for fut in as_completed(futures):
                        try:
                            job_id, posted = fut.result()
                            if posted:
                                id_to_date[job_id] = posted
                        except Exception:
                            continue

                for item in raw_jobs:
                    jid = str(item.get("job_id") or "")
                    if not jid:
                        continue
                    if not item.get("posted_date") and jid in id_to_date:
                        item["posted_date"] = id_to_date[jid]

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
        return [self._map_one(raw, result) for raw in result.raw_jobs]

    def _map_one(self, raw: Dict[str, Any], result: CollectResult) -> JobRecord:
        title = _clean_text(str(raw.get("title") or ""))
        location = _clean_text(str(raw.get("location") or ""))
        job_id = _clean_text(str(raw.get("job_id") or ""))
        posted_date = _clean_text(str(raw.get("posted_date") or ""))
        job_url = str(raw.get("job_url") or "")

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
