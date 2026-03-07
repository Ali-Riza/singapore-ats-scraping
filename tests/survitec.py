# tests/survitec.py
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CAREERS_URL = "https://survitec.csod.com/ux/ats/careersite/4/home?c=survitec&country=sg"

SEARCH_API_URL = "https://uk.api.csod.com/rec-job-search/external/jobs"
DETAIL_API_TEMPLATE = "https://survitec.csod.com/services/x/job-requisition/v2/requisitions/{job_id}/jobDetails?cultureId=2"

COMPANY = "Survitec"
SOURCE = "csod"

CAREER_SITE_ID = 4
CAREER_SITE_PAGE_ID = 4
CULTURE_ID = 2
CULTURE_NAME = "en-GB"
DEFAULT_COUNTRY_CODES = ["sg"]

DEBUG_DUMP_DETAIL = False

# HTTP pooling/retries
_RETRY = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504])
_API_ADAPTER = HTTPAdapter(max_retries=_RETRY, pool_connections=100, pool_maxsize=100)

API_SESSION = requests.Session()
API_SESSION.mount("https://", _API_ADAPTER)
API_SESSION.mount("http://", _API_ADAPTER)

def build_job_url(job_id: str) -> str:
    return f"https://survitec.csod.com/ux/ats/careersite/4/home/requisition/{job_id}?c=survitec"

def _first_nonempty(*vals: Any) -> str:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        if v in ([], {}):
            continue
        return str(v).strip()
    return ""

def _deep_get(obj: Any, path: List[str]) -> Any:
    cur = obj
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return None
    return cur

def _deep_find_any(obj: Any, keys: List[str]) -> Any:
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k] not in (None, "", [], {}):
                return obj[k]
        for v in obj.values():
            found = _deep_find_any(v, keys)
            if found not in (None, "", [], {}):
                return found
    elif isinstance(obj, list):
        for it in obj:
            found = _deep_find_any(it, keys)
            if found not in (None, "", [], {}):
                return found
    return None

def _stringify_location(loc_raw: Any) -> str:
    if isinstance(loc_raw, str):
        return loc_raw.strip()
    if isinstance(loc_raw, dict):
        return _first_nonempty(
            loc_raw.get("label"),
            loc_raw.get("name"),
            loc_raw.get("displayName"),
            loc_raw.get("locationName"),
            loc_raw.get("formattedAddress"),
            loc_raw.get("city"),
        )
    if isinstance(loc_raw, list) and loc_raw:
        return _stringify_location(loc_raw[0])
    return ""

def get_auth_bundle(careers_url: str, timeout_s: int = 35) -> Tuple[str, List[dict]]:
    token_holder: Dict[str, str] = {}

    def on_request(req):
        auth = req.headers.get("authorization") or req.headers.get("Authorization")
        if auth and auth.lower().startswith("bearer "):
            token_holder["token"] = auth.split(" ", 1)[1].strip()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.on("request", on_request)

        page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in {"image", "font", "media"}
            else route.continue_(),
        )

        page.goto(careers_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=12000)
        except Exception:
            pass

        start = time.time()
        while "token" not in token_holder and (time.time() - start) < timeout_s:
            time.sleep(0.15)

        cookies = context.cookies()
        context.close()
        browser.close()

    if "token" not in token_holder:
        raise RuntimeError("Bearer-Token nicht erfasst.")

    return token_holder["token"], cookies

def requests_session_with_cookies(cookies: List[dict]) -> requests.Session:
    s = requests.Session()
    s.headers.update({"user-agent": "Mozilla/5.0", "accept": "application/json"})
    s.mount("https://", _API_ADAPTER)
    s.mount("http://", _API_ADAPTER)
    for c in cookies:
        s.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
    return s

def fetch_search_page(
    token: str,
    page_number: int,
    page_size: int = 25,
    country_codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    payload = {
        "careerSiteId": CAREER_SITE_ID,
        "careerSitePageId": CAREER_SITE_PAGE_ID,
        "pageNumber": page_number,
        "pageSize": page_size,
        "cultureId": CULTURE_ID,
        "searchText": "",
        "cultureName": CULTURE_NAME,
        "states": [],
        "countryCodes": country_codes or DEFAULT_COUNTRY_CODES,
        "cities": [],
        "placeID": "",
        "radius": None,
        "postingsWithinDays": None,
        "customFieldCheckboxKeys": [],
        "customFieldDropdowns": [],
        "customFieldRadios": [],
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://survitec.csod.com",
        "referer": "https://survitec.csod.com/",
        "csod-accept-language": CULTURE_NAME,
        "authorization": f"Bearer {token}",
        "user-agent": "Mozilla/5.0",
    }

    r = API_SESSION.post(SEARCH_API_URL, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def extract_job_ids(search_json: Dict[str, Any]) -> List[str]:
    jobs = (
        _deep_get(search_json, ["data", "jobs"])
        or _deep_get(search_json, ["data", "requisitions"])
        or _deep_get(search_json, ["jobs"])
        or _deep_get(search_json, ["requisitions"])
    )

    if not isinstance(jobs, list):
        candidate = _deep_find_any(search_json, ["jobs", "requisitions", "jobRequisitions"])
        if isinstance(candidate, list):
            jobs = candidate

    if not isinstance(jobs, list):
        return []

    ids: List[str] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        jid = _first_nonempty(
            j.get("jobId"),
            j.get("requisitionId"),
            j.get("id"),
            j.get("jobReqId"),
            j.get("jobRequisitionId"),
        )
        if jid:
            ids.append(str(jid).strip())

    seen = set()
    out = []
    for jid in ids:
        if jid not in seen:
            seen.add(jid)
            out.append(jid)
    return out

def scrape_all_job_ids(token: str, country_codes: Optional[List[str]] = None) -> List[str]:
    page = 1
    page_size = 50  # larger pages to reduce calls
    seen_ids: set[str] = set()
    all_ids: List[str] = []

    while True:
        data = fetch_search_page(token, page, page_size, country_codes or DEFAULT_COUNTRY_CODES)
        ids = extract_job_ids(data)
        if not ids:
            break

        new_ids = [jid for jid in ids if jid not in seen_ids]
        if not new_ids:
            break

        for jid in new_ids:
            seen_ids.add(jid)
            all_ids.append(jid)

        if len(ids) < page_size:
            break

        page += 1
        if page > 200:
            break

    return all_ids

def fetch_job_details(session: requests.Session, token: str, job_id: str) -> Dict[str, Any]:
    url = DETAIL_API_TEMPLATE.format(job_id=job_id)
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token}",
        "x-requested-with": "XMLHttpRequest",
        "referer": build_job_url(job_id),
        "user-agent": "Mozilla/5.0",
    }
    r = session.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def parse_details_to_record(job_id: str, details: Dict[str, Any]) -> Dict[str, Any]:
    title_raw = _deep_find_any(details, [
        "jobTitle", "title", "requisitionTitle", "jobName", "displayTitle",
        "jobtitle", "positionTitle"
    ])
    if isinstance(title_raw, dict):
        title = _first_nonempty(title_raw.get("label"), title_raw.get("value"), title_raw.get("display"))
    else:
        title = _first_nonempty(title_raw)

    loc_raw = _deep_find_any(details, [
        "locations", "location", "primaryLocation", "locationName",
        "jobLocation", "workLocation", "locationCity", "city", "countryName",
        "workplace", "address"
    ])

    location = _stringify_location(loc_raw)

    if not location:
        city = _first_nonempty(_deep_find_any(details, ["city", "locationCity"]))
        country = _first_nonempty(_deep_find_any(details, ["countryName", "country", "countryCode"]))
        location = ", ".join([x for x in [city, country] if x])

    posted_date_raw = _deep_find_any(details, [
        "postedDate", "postedOn", "datePosted", "postingDate", "postedDateUtc", "createdDate",
        "postedDateUTC", "postedDateTime", "datePostedUtc", "postingStartDate", "publishDate",
        "publishedDate", "publicationDate", "startDate", "openDate"
    ])
    if not posted_date_raw:
        posted_date_raw = _deep_find_any(
            _deep_get(details, ["postingInfo"]) or {},
            ["postedDate", "postingDate", "publishDate", "publishedDate", "startDate"]
        )
    if not posted_date_raw:
        posted_date_raw = _deep_find_any(
            _deep_get(details, ["requisition"]) or {},
            ["postedDate", "postingDate", "publishDate", "publishedDate", "createdDate"]
        )

    posted_date = ""
    if isinstance(posted_date_raw, dict):
        posted_date = _first_nonempty(
            posted_date_raw.get("value"),
            posted_date_raw.get("date"),
            posted_date_raw.get("utc"),
            posted_date_raw.get("display"),
        )
    else:
        posted_date = _first_nonempty(posted_date_raw)

    return {
        "company": COMPANY,
        "job_title": title,
        "location": location,
        "job_id": job_id,
        "posted_date": posted_date,
        "job_url": build_job_url(job_id),
        "source": SOURCE,
        "careers_url": CAREERS_URL,
    }

def _fetch_and_parse_record(session: requests.Session, token: str, job_id: str) -> Optional[Dict[str, Any]]:
    details = fetch_job_details(session, token, job_id)
    if DEBUG_DUMP_DETAIL:
        try:
            with open(f"csod_detail_{job_id}.json", "w", encoding="utf-8") as f:
                json.dump(details, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    return parse_details_to_record(job_id, details)

def scrape_jobs(country_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    token, cookies = get_auth_bundle(CAREERS_URL)
    session = requests_session_with_cookies(cookies)

    ids = scrape_all_job_ids(token, country_codes=country_codes or DEFAULT_COUNTRY_CODES)
    out: List[Dict[str, Any]] = []
    if not ids:
        return out

    max_workers = min(8, len(ids))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_fetch_and_parse_record, session, token, jid): jid for jid in ids}
        for fut in as_completed(futures):
            try:
                rec = fut.result()
                if rec:
                    out.append(rec)
            except Exception:
                pass

    return out

if __name__ == "__main__":
    jobs = scrape_jobs(country_codes=["sg"])
    print(f"Found {len(jobs)} jobs")
    for j in jobs[:10]:
        print(j)