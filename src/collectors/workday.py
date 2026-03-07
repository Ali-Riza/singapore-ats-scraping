from __future__ import annotations  # Enables forward references in type hints (Python 3.7+ compatibility)

import re  # Regular expressions for parsing job IDs and date patterns
import time
from datetime import date, timedelta  # Date utilities for normalizing "posted on" dates
from typing import Any, Dict, List, Optional, Tuple  

from urllib.parse import urlparse, parse_qs  # Parses career URLs to extract Workday endpoints and query parameters
import requests  # HTTP client for calling Workday JSON APIs

from src.collectors.base import BaseCollector  # Abstract base class that defines collector interface
from src.core.models import CompanyItem, CollectResult, JobRecord  # Data models for company info, scraping results, and job records

# workday country ID for Singapore
SG_COUNTRY_ID = "80938777cac5440fab50d729f9634969"

# Company-specific filter configurations because Workday instances use different facet key names (e.g., "locationCountry" vs "Country")
COMPANY_FACET_OVERRIDES: Dict[str, Dict[str, str]] = {
    "MODEC": {"facet_key": "locationCountry", "facet_value": SG_COUNTRY_ID},
    "Yokogawa": {"facet_key": "locationCountry", "facet_value": SG_COUNTRY_ID},
    "Rolls-Royce Power Systems (MTU)": {"facet_key": "Country", "facet_value": "Singapore"},
    "Prysmian Group": {"facet_key": "locationCountry", "facet_value": SG_COUNTRY_ID},
    "Rockwell Automation": {"facet_key": "locationCountry", "facet_value": SG_COUNTRY_ID},
    "KSB": {"facet_key": "locationCountry", "facet_value": SG_COUNTRY_ID},
    "Sulzer": {"facet_key": "locationCountry", "facet_value": SG_COUNTRY_ID},
}

# Pre-compile regex patterns for performance (avoids recompiling on every job)
_JOB_ID_PATTERN = re.compile(r"(JR\d+|R-\d+|HRC\d+|\d{6,})", re.IGNORECASE)
_POSTED_DAYS_PATTERN = re.compile(r"posted\s+(\d+)\s+day")
_POSTED_DAYS_PLUS_PATTERN = re.compile(r"posted\s+(\d+)\+\s+day")
_GERMAN_DAYS_PATTERN = re.compile(r"vor\s+(\d+)\s+tag")
_GERMAN_DAYS_PLUS_PATTERN = re.compile(r"vor\s+mehr\s+als\s+(\d+)\s+tag")
_EXTRACT_ID_FROM_PATH = re.compile(r"_(\d+)(?:-|$)")
_EXTRACT_ID_FROM_PATH_R = re.compile(r"_(R-\d+)(?:-|$)")

class WorkdayCollector(BaseCollector):
    """ Collector for Workday-based careers sites."""
    name = "workday"

    _MULTI_LOC_LABEL_RE = re.compile(r"^\s*\d+\s+locations?\s*$", re.IGNORECASE)

    def _looks_like_multi_location_label(self, text: str) -> bool:
        t = (text or "").strip()
        if not t:
            return False
        if self._MULTI_LOC_LABEL_RE.fullmatch(t):
            return True
        return "multiple locations" in t.casefold()

    def _extract_locations_from_raw(self, raw: Dict[str, Any]) -> List[str]:
        def _clean(s: str) -> str:
            return " ".join((s or "").strip().split())

        def _from_any(v: Any) -> List[str]:
            if v is None or v is False or v is True:
                return []
            if isinstance(v, str):
                s = _clean(v)
                if not s or self._looks_like_multi_location_label(s):
                    return []
                return [s]
            if isinstance(v, list):
                out: List[str] = []
                for it in v:
                    out.extend(_from_any(it))
                return out
            if isinstance(v, dict):
                out: List[str] = []
                # Common Workday-ish patterns
                for k in ("descriptor", "name", "label", "value", "text"):
                    if k in v:
                        out.extend(_from_any(v.get(k)))
                # Some payloads nest location objects deeper
                for k in ("location", "jobLocation", "jobLocations", "locations"):
                    if k in v:
                        out.extend(_from_any(v.get(k)))
                return out
            return []

        keys_to_probe = (
            "_resolved_locationsText",
            "primaryLocation",
            "location",
            "locations",
            "additionalLocations",
            "additionalLocation",
            "jobLocation",
            "jobLocations",
        )

        candidates: List[str] = []
        for k in keys_to_probe:
            if k in raw:
                candidates.extend(_from_any(raw.get(k)))

        # Deduplicate while preserving order
        seen = set()
        uniq: List[str] = []
        for c in candidates:
            if c not in seen:
                seen.add(c)
                uniq.append(c)
        return uniq

    def _resolve_location(self, raw: Dict[str, Any], default: str = "Singapore") -> str:
        # 1) If we already resolved locations (e.g., from a previous enrichment step), use it.
        resolved = self._pick(raw, "_resolved_locationsText")
        if resolved and not self._looks_like_multi_location_label(resolved):
            return resolved

        # 2) Prefer Workday-provided text if it's a concrete location.
        loc_text = self._pick(raw, "locationsText", "primaryLocation", "location")
        if loc_text and not self._looks_like_multi_location_label(loc_text):
            return loc_text

        # 3) If it's something like "2 Locations"/"Multiple Locations", try to expand from raw fields.
        locs = self._extract_locations_from_raw(raw)
        if locs:
            sg_locs = [l for l in locs if "singapore" in l.casefold()]
            chosen = sg_locs if sg_locs else [locs[0]]
            # Preserve multiple Singapore entries if present.
            final = " | ".join(chosen)
            raw["_resolved_locationsText"] = final
            return final

        return default

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        """Z3: fetch raw jobs + meta from Workday JSON API."""
        
        limit = 20
        offset = 0
        max_pages = 200
        time_budget_s = 180

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "limit": limit,
            "offsets": [],
            "status_codes": [],
            "total_raw": 0,
            "used_facet_key": None,
            "time_budget_s": time_budget_s,
            "time_budget_exceeded": False,
        }

        try:
            resolved_url = company.careers_url

            # If branded (not a direct Workday host) -> resolve actual Workday URL from HTML
            if "myworkdayjobs.com" not in resolved_url and "myworkdaysite.com" not in resolved_url:
                wd = resolve_workday_from_branded_site(resolved_url)
                if wd:
                    resolved_url = wd

            # Parse the careers URL to extract two key URLs (one for API, one for public site):
            endpoint, public_base = _derive_workday_urls(resolved_url)

            # Special case: Rolls-Royce Power Systems (MTU) uses URL facets for filtering (e.g. ?Location_Country=80938777cac5440fab50d729f9634969) 
            url_facets = None
            if company.company == "Rolls-Royce Power Systems (MTU)":
                url_facets = parse_qs(urlparse(company.careers_url).query)

            # Start session in order to reuse HTTP connections
            session = requests.Session()

            started_at = time.monotonic()
            # After the first page we lock into a mode to avoid re-trying facet keys on every page.
            # Modes:
            # - None: unknown, try to discover a working facet
            # - "__url_facets__": use url_facets payload
            # - "__no_facet__": request without facets
            # - otherwise: facet key string
            facet_mode: Optional[str] = None

            expected_total: Optional[int] = None

            # Paginate through job postings
            for _ in range(max_pages):

                # Hard stop to avoid one tenant stalling the whole pipeline.
                if (time.monotonic() - started_at) > time_budget_s:
                    meta["time_budget_exceeded"] = True
                    break
                
                # Fetch one page
                data, used_facet_key = _fetch_page(
                    session=session,
                    endpoint=endpoint,
                    company_name=company.company,
                    offset=offset,
                    limit=limit,
                    url_facets=url_facets,
                    fixed_facet_key=facet_mode,
                )

                # Lock facet mode after first successful fetch to speed up subsequent pages.
                if facet_mode is None:
                    if used_facet_key is None:
                        facet_mode = "__no_facet__"
                    else:
                        facet_mode = used_facet_key

                # Update metadata
                meta["used_facet_key"] = used_facet_key
                meta["status_codes"].append(200)
                meta["pages"] += 1
                meta["offsets"].append(offset)

                if expected_total is None:
                    t = data.get("total")
                    if isinstance(t, int) and t >= 0:
                        expected_total = t

                # Extract job postings from API response
                # data.get("jobPostings") returns the job list if it exists, otherwise None
                # The "or []" fallback ensures postings is always a list (never None)
                postings = data.get("jobPostings") or []
                
                # Stop pagination if:
                # 1. postings is not a list (malformed response), OR
                # 2. postings is empty (no more jobs to fetch)
                if not isinstance(postings, list) or not postings:
                    break
                
                # Append fetched postings to raw_jobs
                raw_jobs.extend([p for p in postings if isinstance(p, dict)])
                offset += len(postings)

                # If Workday reports a total, stop once we've reached it.
                if expected_total is not None and offset >= expected_total:
                    break

                # Stop if fewer postings than limit were returned (last page) in order to avoid unnecessary requests
                if len(postings) < limit:
                    break
            
            # Finalize metadata
            meta["total_raw"] = len(raw_jobs)
            meta["public_site_base"] = public_base
            meta["endpoint"] = endpoint

            # Return the collected result
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )

        # Handle any exceptions during collection
        except Exception as e:
            meta["total_raw"] = len(raw_jobs)
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(e),   # Capture exception message as error
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        """Z4: map Workday raw jobs to standardized JobRecord format."""
        records = []
        for raw_job in result.raw_jobs:
            record = self._map_one_raw_job(raw_job, result)
            records.append(record)
        return records

    def _map_one_raw_job(self, raw: Dict[str, Any], result: CollectResult) -> JobRecord:
        """ Map one Workday raw job dict to JobRecord."""
        
        # Extract public base URL from meta
        meta = result.meta if result.meta is not None else {}
        public_base = meta.get("public_site_base") if meta else ""
        if not public_base:
            public_base = ""

        title = self._pick(raw, "title", "Title")
        external_path = self._pick(raw, "externalPath", "external_path")
        job_url = _build_job_url(public_base, external_path)

        job_id = _pick_job_id(raw.get("bulletFields"), external_path)
        posted_date = _parse_posted_on(str(raw.get("postedOn") or "")) or ""

        location = self._resolve_location(raw, default="Singapore")

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
    

def _parse_posted_on(text: str) -> Optional[str]:
    """ Normalize Workday 'postedOn' text to ISO date (YYYY-MM-DD)."""

    if not text:
        return None

    t = text.strip().lower()
    today = date.today()

    # English
    if "today" in t:
        return today.isoformat()
    if "yesterday" in t:
        return (today - timedelta(days=1)).isoformat()

    m = _POSTED_DAYS_PATTERN.search(t)
    if m:
        return (today - timedelta(days=int(m.group(1)))).isoformat()

    m = _POSTED_DAYS_PLUS_PATTERN.search(t)
    if m:
        return (today - timedelta(days=int(m.group(1)))).isoformat()

    # German
    if "heute" in t:
        return today.isoformat()
    if "gestern" in t:
        return (today - timedelta(days=1)).isoformat()

    m = _GERMAN_DAYS_PATTERN.search(t)
    if m:
        return (today - timedelta(days=int(m.group(1)))).isoformat()

    m = _GERMAN_DAYS_PLUS_PATTERN.search(t)
    if m:
        return (today - timedelta(days=int(m.group(1)))).isoformat()

    return None


def _derive_workday_urls(jobs_page_url: str) -> Tuple[str, str]:
    """Derive Workday JSON endpoint + public base URL from a jobs-page URL."""

    u = urlparse(jobs_page_url) # transforms URL into components
    host = u.netloc # e.g., "wd5.myworkdayjobs.com"
    
    # Split URL path by "/" and filter out empty segments
    # Example: "/recruiting/tenant/site" -> ["recruiting", "tenant", "site"]
    path_list = u.path.split("/")
    path_parts = []
    for segment in path_list:
        if segment:  # Only include non-empty segments
            path_parts.append(segment)

    # Case 2: https://wdX.myworkdaysite.com/recruiting/{tenant}/{site}...
    if "myworkdaysite.com" in host and len(path_parts) >= 3 and path_parts[0] == "recruiting":
        tenant = path_parts[1]
        site = path_parts[2]
        public_base = f"{u.scheme}://{host}/recruiting/{tenant}/{site}"
        endpoint = f"{u.scheme}://{host}/wday/cxs/{tenant}/{site}/jobs"
        return endpoint, public_base

    # Case 1: https://{tenant}.wd?.myworkdayjobs.com[/en-US]/{site}...
    tenant = host.split(".")[0]

    # Determine site and public base URL
    if len(path_parts) >= 2 and path_parts[0].lower() == "en-us":
        site = path_parts[1]
        public_base = f"{u.scheme}://{host}/en-US/{site}"
    else:
        site = path_parts[0] if path_parts else ""
        public_base = f"{u.scheme}://{host}/{site}" if site else f"{u.scheme}://{host}"

    endpoint = f"{u.scheme}://{host}/wday/cxs/{tenant}/{site}/jobs"
    return endpoint, public_base


def _build_job_url(public_site_base: str, external_path: Optional[str]) -> str:
    """ Construct full job URL from public base and external path."""
    if not external_path:
        return ""
    if external_path.startswith("http"):
        return external_path
    if not external_path.startswith("/"):
        external_path = "/" + external_path
    return public_site_base.rstrip("/") + external_path


def _pick_job_id(bullet_fields: Any, external_path: Any) -> str:
    """ Extract job ID from bulletFields or externalPath. bulletFields is usually a list of strings, where job ID may be present."""
    
    # check bulletFields first
    if isinstance(bullet_fields, list):
        for x in bullet_fields:
            s = str(x).strip()
            if not s:
                continue
            if _JOB_ID_PATTERN.fullmatch(s):
                return s
        for x in reversed(bullet_fields):
            s = str(x).strip()
            if s:
                return s
    # fallback: try to extract from externalPath
    if isinstance(external_path, str):
        m = _EXTRACT_ID_FROM_PATH.search(external_path)
        if m:
            return m.group(1)
        m = _EXTRACT_ID_FROM_PATH_R.search(external_path)
        if m:
            return m.group(1)

    return ""


def _fetch_page(
    session: requests.Session,
    endpoint: str,
    company_name: str,
    offset: int,
    limit: int,
    url_facets: Optional[Dict[str, List[str]]] = None,
    fixed_facet_key: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """ Fetch one page of job postings from Workday JSON API with given offset and limit. Fetching means making a POST request with appropriate payload and headers."""

    # Prepare headers and payload
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; ATS-scraper/1.0)",
    }

    def _facet_value_for_key(facet_key: str) -> str:
        override = COMPANY_FACET_OVERRIDES.get(company_name)
        if override and facet_key == override.get("facet_key"):
            return str(override.get("facet_value"))
        if facet_key == "Country" and not override:
            return "Singapore"
        return SG_COUNTRY_ID

    # If we already know which mode works, do a single request.
    if fixed_facet_key == "__url_facets__":
        if not url_facets:
            raise RuntimeError(f"{company_name}: fixed_facet_key='__url_facets__' but no url_facets")
        payload = {"appliedFacets": url_facets, "limit": limit, "offset": offset, "searchText": ""}
        r = session.post(endpoint, json=payload, headers=headers, timeout=(5, 20))
        r.raise_for_status()
        return r.json(), "__url_facets__"

    if fixed_facet_key == "__no_facet__":
        payload = {"limit": limit, "offset": offset, "searchText": ""}
        r = session.post(endpoint, json=payload, headers=headers, timeout=(5, 20))
        r.raise_for_status()
        return r.json(), None

    if fixed_facet_key:
        payload = {
            "appliedFacets": {fixed_facet_key: [_facet_value_for_key(fixed_facet_key)]},
            "limit": limit,
            "offset": offset,
            "searchText": "",
        }
        try:
            r = session.post(endpoint, json=payload, headers=headers, timeout=(5, 20))
            r.raise_for_status()
            return r.json(), fixed_facet_key
        except requests.HTTPError as e:
            # If the facet breaks mid-run (rare), fall back to no facet for this page.
            if e.response is not None and e.response.status_code == 400:
                payload2 = {"limit": limit, "offset": offset, "searchText": ""}
                r2 = session.post(endpoint, json=payload2, headers=headers, timeout=(5, 20))
                r2.raise_for_status()
                return r2.json(), None
            raise

    # 1) try URL facets (if any). Url facets are used by some tenants for filtering (e.g., Rolls-Royce Power Systems (MTU))
    if url_facets:
        payload = {"appliedFacets": url_facets, "limit": limit, "offset": offset, "searchText": ""}
        r = session.post(endpoint, json=payload, headers=headers, timeout=(5, 20))
        r.raise_for_status()
        return r.json(), "__url_facets__"

    # 2) try other facet keys
    override = COMPANY_FACET_OVERRIDES.get(company_name)

    # Prepare list of candidate facet keys
    facet_key_candidates: List[str] = []
    
    # If company has override, try that first
    if override:
        facet_key_candidates.append(override["facet_key"])

    # additional common facet keys
    facet_key_candidates += ["Location_Country", "locationCountry", "locationCountryId", "Country"]
    facet_key_candidates = list(dict.fromkeys(facet_key_candidates))

    
    last_exc: Optional[Exception] = None # to store last exception if all attempts fail
    best_data: Optional[Dict[str, Any]] = None # to store best response if no facets yield jobs

    # Try each facet key until one yields jobs
    for facet_key in facet_key_candidates:
        if override and facet_key == override["facet_key"]:
            facet_value = override["facet_value"]
        elif facet_key == "Country" and not override:
            facet_value = "Singapore"
        else:
            facet_value = SG_COUNTRY_ID

        # Prepare payload with current facet 
        payload = {
            "appliedFacets": {facet_key: [facet_value]},
            "limit": limit,
            "offset": offset,
            "searchText": "",
        }

        try:
            # Make POST request. POST request is required by Workday API for job searches.
            r = session.post(endpoint, json=payload, headers=headers, timeout=(5, 20))
            r.raise_for_status()
            data = r.json()

            # Extract job postings from response
            postings = data.get("jobPostings") or []
            
            # Keep best response in case no facets yield jobs
            if best_data is None and isinstance(postings, list):
                best_data = data

            # If facet yields jobs, take it
            if isinstance(postings, list) and len(postings) > 0:
                return data, facet_key

            # If total == 0 and postings empty -> valid "no jobs"
            total = data.get("total")
            if isinstance(total, int) and total == 0 and isinstance(postings, list) and len(postings) == 0:
                return data, facet_key

        # Handle exceptions and continue to next facet key. Exceptions may occur due to HTTP errors or malformed responses.
        except requests.HTTPError as e:
            
            # Special case: 400 Bad Request may indicate invalid facet value. Retry without facet.
            if e.response is not None and e.response.status_code == 400:
                payload2 = {"limit": limit, "offset": offset, "searchText": ""}
                r2 = session.post(endpoint, json=payload2, headers=headers, timeout=(5, 20))
                r2.raise_for_status()
                return r2.json(), None
            # Otherwise, store exception and continue
            last_exc = e
            continue
        # Handle other exceptions
        except Exception as e:
            last_exc = e
            continue
        
    # If no facet key yielded jobs, return best response if available. example for best response: total=0 and empty postings
    if best_data is not None:
        return best_data, None

    # If all attempts failed, raise last exception
    if last_exc is not None:
        raise last_exc

    # If no valid response at all, raise error
    raise RuntimeError(f"{company_name}: no valid response for any facet key")

def resolve_workday_from_branded_site(url: str) -> Optional[str]:
    html = requests.get(url, timeout=15).text

    patterns = [
        r"https://[a-z0-9\-]+\.wd\d+\.myworkdayjobs\.com/[^\"]+",
        r"https://[a-z0-9\-]+\.wd\d+\.myworkdaysite\.com/recruiting/[^\"]+",
    ]

    for p in patterns:
        m = re.search(p, html, re.IGNORECASE)
        if m:
            return m.group(0)

    return None