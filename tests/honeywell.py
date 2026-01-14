# tests/honeywell.py
# Oracle HCM (Honeywell careers) – Singapore LocationId scraper
# Runs: python -m tests.honeywell

from __future__ import annotations

import json
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Set

import requests


CAREERS_URL = "https://careers.honeywell.com"
API_URL = (
    "https://ibqbjb.fa.ocs.oraclecloud.com/hcmRestApi/resources/latest/"
    "recruitingCEJobRequisitions"
)

DEFAULT_PARAMS = {
    "onlyData": "true",
    "expand": ",".join(
        [
            "requisitionList.workLocation",
            "requisitionList.otherWorkLocations",
            "requisitionList.secondaryLocations",
            "flexFieldsFacet.values",
            "requisitionList.requisitionFlexFields",
        ]
    ),
    "finder": (
        "findReqs;"
        "siteNumber=CX_1,"
        "facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,"
        "limit=25,"
        "locationId=300000000469764,"
        "sortBy=POSTING_DATES_DESC"
    ),
}

DEFAULT_HEADERS = {
    "accept": "*/*",
    "accept-language": "en",
    "content-type": "application/vnd.oracle.adf.resourceitem+json;charset=utf-8",
    "ora-irc-language": "en",
    "origin": CAREERS_URL,
    "referer": f"{CAREERS_URL}/",
    # user-agent: set below in code for clarity; can be omitted if you want
}


def _safe_get(d: Any, *keys: str) -> Any:
    """
    Safe nested dict getter.
    If at any step the value isn't a dict, returns None.
    """
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _parse_posted_date(value: Any) -> Optional[str]:
    """
    Try to normalize posting date to YYYY-MM-DD.
    Oracle HCM can return various formats; we handle common ones.
    """
    if not value:
        return None

    # already date-like string "2026-01-03"
    if isinstance(value, str):
        # ISO date already
        if len(value) >= 10 and value[4] == "-" and value[7] == "-":
            return value[:10]
        # try ISO datetime
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.date().isoformat()
        except Exception:
            return None

    # sometimes it's a dict with "value" or similar
    if isinstance(value, dict):
        for k in ("value", "Value", "date", "Date"):
            v = value.get(k)
            if isinstance(v, str):
                return _parse_posted_date(v)

    return None


def _extract_location(req: Dict[str, Any]) -> Optional[str]:
    """
    Location can be:
    - PrimaryLocation: str
    - PrimaryLocation: {LocationName: "..."}
    - workLocation: {LocationName: "..."} or {LocationName: {"value": "..."}}
    """
    primary = req.get("PrimaryLocation")
    if isinstance(primary, str):
        return primary
    if isinstance(primary, dict):
        loc = primary.get("LocationName")
        if isinstance(loc, str):
            return loc
        if isinstance(loc, dict):
            v = loc.get("value") or loc.get("Value")
            if isinstance(v, str):
                return v

    # try workLocation in expanded structure
    wl_name = _safe_get(req, "workLocation", "LocationName")
    if isinstance(wl_name, str):
        return wl_name
    if isinstance(wl_name, dict):
        v = wl_name.get("value") or wl_name.get("Value")
        if isinstance(v, str):
            return v

    # sometimes "workLocation" is a plain string
    wl = req.get("workLocation")
    if isinstance(wl, str):
        return wl

    return None


def _extract_job_id(req: Dict[str, Any]) -> Optional[str]:
    """
    Job/requisition id can appear as:
    - RequisitionNumber
    - RequisitionId
    - JobId
    - Id
    """
    for k in ("RequisitionNumber", "RequisitionId", "JobId", "Id"):
        v = req.get(k)
        if v is None:
            continue
        if isinstance(v, (int, float)):
            return str(int(v))
        if isinstance(v, str):
            return v.strip() or None
        if isinstance(v, dict):
            # sometimes { "value": "123" }
            vv = v.get("value") or v.get("Value")
            if isinstance(vv, (int, float)):
                return str(int(vv))
            if isinstance(vv, str):
                return vv.strip() or None
    return None


def _extract_title(req: Dict[str, Any]) -> Optional[str]:
    for k in ("Title", "JobTitle", "RequisitionTitle"):
        v = req.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            vv = v.get("value") or v.get("Value")
            if isinstance(vv, str) and vv.strip():
                return vv.strip()
    return None


def _build_job_url(job_id: Optional[str]) -> Optional[str]:
    if not job_id:
        return None
    # Honeywell uses /us/en/job/<id>
    return f"{CAREERS_URL}/us/en/job/{job_id}"


def fetch_jobs(
    location_id: int = 300000000469764,
    site_number: str = "CX_1",
    limit: int = 25,
    max_pages: int = 50,
    timeout: int = 30,
    debug: bool = True,
) -> List[Dict[str, Any]]:
    """
    Fetch jobs from Oracle HCM Recruiting (Honeywell).
    Uses offset pagination until:
    - collected_count >= TotalJobsCount OR
    - no new jobs appear / requisitionList empty
    """
    headers = dict(DEFAULT_HEADERS)
    headers["user-agent"] = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
    )

    params = dict(DEFAULT_PARAMS)
    # rebuild finder with chosen args
    params["finder"] = (
        "findReqs;"
        f"siteNumber={site_number},"
        "facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,"
        f"limit={limit},"
        f"locationId={location_id},"
        "sortBy=POSTING_DATES_DESC"
    )

    results: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()

    total_jobs_count: Optional[int] = None

    offset = 0
    for page in range(max_pages):
        # Oracle HCM often supports offset via query param "offset"
        # Some tenants echo it inside response too.
        params_page = dict(params)
        params_page["offset"] = str(offset)

        if debug:
            print(f">>> Request offset={offset}")

        resp = requests.get(API_URL, params=params_page, headers=headers, timeout=timeout)
        if debug:
            print(f">>> Status: {resp.status_code}")

        resp.raise_for_status()
        data = resp.json()

        items = data.get("items") or []
        if debug:
            print(f">>> items len = {len(items)}")

        if not items:
            if debug:
                print(">>> items empty -> Ende")
            break

        first = items[0]
        if debug and isinstance(first, dict):
            print(f">>> first item keys: {list(first.keys())[:80]}")

        # In this API shape, requisitionList is inside the first "items" element
        req_list = []
        if isinstance(first, dict):
            req_list = first.get("requisitionList") or []
            if total_jobs_count is None:
                tjc = first.get("TotalJobsCount")
                if isinstance(tjc, (int, float)):
                    total_jobs_count = int(tjc)
                elif isinstance(tjc, str) and tjc.isdigit():
                    total_jobs_count = int(tjc)

        if debug:
            print(f">>> requisitionList len = {len(req_list)}")
            if total_jobs_count is not None:
                print(f">>> TotalJobsCount: {total_jobs_count}")
            # some extra echoes
            loc_id_echo = first.get("LocationId") if isinstance(first, dict) else None
            if loc_id_echo is not None:
                print(f">>> LocationId (response): {loc_id_echo}")
            loc_echo = first.get("Location") if isinstance(first, dict) else None
            print(f">>> Location (response): {loc_echo}")
            off_echo = first.get("Offset") if isinstance(first, dict) else None
            lim_echo = first.get("Limit") if isinstance(first, dict) else None
            if off_echo is not None and lim_echo is not None:
                print(f">>> Offset/Limit (response): {off_echo} / {lim_echo}")

        if not req_list:
            if debug:
                print(">>> jobs_in_page == 0 -> Ende")
            break

        added_this_page = 0
        for req in req_list:
            if not isinstance(req, dict):
                continue

            job_id = _extract_job_id(req)
            if not job_id:
                continue
            if job_id in seen_ids:
                continue

            seen_ids.add(job_id)
            added_this_page += 1

            job = {
                "company": "Honeywell",
                "job_title": _extract_title(req),
                "location": _extract_location(req),
                "job_id": job_id,
                "posted_date": _parse_posted_date(
                    req.get("PostedDate")
                    or req.get("PostingDate")
                    or req.get("PostingStartDate")
                    or req.get("DatePosted")
                ),
                "job_url": _build_job_url(job_id),
                "source": "oracle_hcm",
                "careers_url": CAREERS_URL,
            }
            results.append(job)

        if debug:
            print(f">>> jobs_in_page(new unique) = {added_this_page}")
            print(f">>> jobs found (unique) = {len(results)}")

        # Stop conditions
        if added_this_page == 0:
            if debug:
                print(">>> No new jobs on this page -> Ende")
            break

        if total_jobs_count is not None and len(results) >= total_jobs_count:
            if debug:
                print(f">>> Reached TotalJobsCount ({total_jobs_count}) -> Ende")
            break

        # next page
        offset += limit

    if debug:
        print(f">>> DONE. Total unique jobs: {len(results)}")
        if total_jobs_count is not None:
            print(f">>> TotalJobsCount reported by API: {total_jobs_count}")

    return results


def main() -> None:
    print(">>> honeywell.py gestartet")
    print(">>> main reached")
    print(">>> fetch_jobs() gestartet")

    jobs = fetch_jobs(
        location_id=300000000469764,  # Singapore
        limit=25,
        debug=True,
    )

    print("\n>>> SAMPLE (first 10):")
    for j in jobs[:10]:
        print(j)

    print(f"\n>>> FINAL COUNT = {len(jobs)}")


if __name__ == "__main__":
    main()