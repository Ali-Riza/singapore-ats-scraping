from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


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


def _deep_get(obj: Any, path: Sequence[str]) -> Any:
    cur = obj
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return None
    return cur


def _deep_find_any(obj: Any, keys: Sequence[str]) -> Any:
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


@dataclass(frozen=True)
class CornerstoneConfig:
    search_api_url: str
    detail_api_template: str

    career_site_id: int
    career_site_page_id: int
    culture_id: int
    culture_name: str

    default_country_codes: List[str]
    company_param: str

    job_url_template: str


def _config_for_company(company: CompanyItem) -> CornerstoneConfig:
    name = (company.company or "").strip().lower()

    # Survitec (Cornerstone OnDemand)
    if name == "survitec":
        return CornerstoneConfig(
            search_api_url="https://uk.api.csod.com/rec-job-search/external/jobs",
            detail_api_template="https://survitec.csod.com/services/x/job-requisition/v2/requisitions/{job_id}/jobDetails?cultureId=4",
            career_site_id=4,
            career_site_page_id=4,
            culture_id=4,
            culture_name="de-DE",
            default_country_codes=["sg"],
            company_param="survitec",
            job_url_template="https://survitec.csod.com/ux/ats/careersite/4/home/requisition/{job_id}?c=survitec",
        )

    raise RuntimeError(f"No Cornerstone config for company: {company.company}")


def _requests_session_with_retries() -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _get_auth_bundle_via_playwright(careers_url: str, timeout_s: int = 35) -> Tuple[str, List[dict]]:
    """Open the Cornerstone careers site and capture the Bearer token + cookies.

    Requires playwright (sync). If not available, raises ImportError.
    """
    from playwright.sync_api import sync_playwright  # type: ignore

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
        raise RuntimeError("Bearer token not captured.")

    return token_holder["token"], cookies


def _session_with_cookies(cookies: List[dict]) -> requests.Session:
    s = _requests_session_with_retries()
    s.headers.update({"user-agent": "Mozilla/5.0", "accept": "application/json"})

    for c in cookies:
        try:
            s.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"), path=c.get("path", "/"))
        except Exception:
            continue

    return s


def _fetch_search_page(
    *,
    session: requests.Session,
    cfg: CornerstoneConfig,
    token: str,
    page_number: int,
    page_size: int = 50,
    country_codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    payload = {
        "careerSiteId": cfg.career_site_id,
        "careerSitePageId": cfg.career_site_page_id,
        "pageNumber": page_number,
        "pageSize": page_size,
        "cultureId": cfg.culture_id,
        "searchText": "",
        "cultureName": cfg.culture_name,
        "states": [],
        "countryCodes": country_codes or cfg.default_country_codes,
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
        "csod-accept-language": cfg.culture_name,
        "authorization": f"Bearer {token}",
        "user-agent": "Mozilla/5.0",
        "origin": f"https://{cfg.company_param}.csod.com",
        "referer": f"https://{cfg.company_param}.csod.com/",
    }

    r = session.post(cfg.search_api_url, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def _extract_job_ids(search_json: Dict[str, Any]) -> List[str]:
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

    seen: set[str] = set()
    out: List[str] = []
    for jid in ids:
        if jid not in seen:
            seen.add(jid)
            out.append(jid)
    return out


def _fetch_job_details(*, session: requests.Session, cfg: CornerstoneConfig, token: str, job_id: str) -> Dict[str, Any]:
    url = cfg.detail_api_template.format(job_id=job_id)
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token}",
        "x-requested-with": "XMLHttpRequest",
        "referer": cfg.job_url_template.format(job_id=job_id),
        "user-agent": "Mozilla/5.0",
    }
    r = session.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def _parse_details_to_raw(cfg: CornerstoneConfig, company_name: str, job_id: str, details: Dict[str, Any]) -> Dict[str, Any]:
    title_raw = _deep_find_any(
        details,
        [
            "jobTitle",
            "title",
            "requisitionTitle",
            "jobName",
            "displayTitle",
            "jobtitle",
            "positionTitle",
        ],
    )
    if isinstance(title_raw, dict):
        title = _first_nonempty(title_raw.get("label"), title_raw.get("value"), title_raw.get("display"))
    else:
        title = _first_nonempty(title_raw)

    loc_raw = _deep_find_any(
        details,
        [
            "locations",
            "location",
            "primaryLocation",
            "locationName",
            "jobLocation",
            "workLocation",
            "locationCity",
            "city",
            "countryName",
            "workplace",
            "address",
        ],
    )
    location = _stringify_location(loc_raw)

    if not location:
        city = _first_nonempty(_deep_find_any(details, ["city", "locationCity"]))
        country = _first_nonempty(_deep_find_any(details, ["countryName", "country", "countryCode"]))
        location = ", ".join([x for x in [city, country] if x])

    posted_date_raw = _deep_find_any(
        details,
        [
            "postedDate",
            "postedOn",
            "datePosted",
            "postingDate",
            "postedDateUtc",
            "createdDate",
            "postedDateUTC",
            "postedDateTime",
            "datePostedUtc",
            "postingStartDate",
            "publishDate",
            "publishedDate",
            "publicationDate",
            "startDate",
            "openDate",
        ],
    )

    if not posted_date_raw:
        posted_date_raw = _deep_find_any(
            _deep_get(details, ["postingInfo"]) or {},
            ["postedDate", "postingDate", "publishDate", "publishedDate", "startDate"],
        )
    if not posted_date_raw:
        posted_date_raw = _deep_find_any(
            _deep_get(details, ["requisition"]) or {},
            ["postedDate", "postingDate", "publishDate", "publishedDate", "createdDate"],
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
        "job_id": job_id,
        "title": title,
        "location": location,
        "posted_date": posted_date,
        "job_url": cfg.job_url_template.format(job_id=job_id),
        "_details": details,
        "company": company_name,
    }


class CornerstoneCollector(BaseCollector):
    """Collector for Cornerstone OnDemand career sites.

    For Survitec, this uses Playwright to obtain a Bearer token and cookies,
    then hits Cornerstone search + detail APIs.
    """

    name = "cornerstone"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "job_ids": 0,
            "detail_fetches": 0,
            "token_captured": False,
        }

        try:
            cfg = _config_for_company(company)

            # 1) Capture token + cookies
            try:
                token, cookies = _get_auth_bundle_via_playwright(company.careers_url)
                meta["token_captured"] = True
            except ImportError:
                return CollectResult(
                    collector=self.name,
                    company=company.company,
                    careers_url=company.careers_url,
                    raw_jobs=[],
                    meta=meta,
                    error="Playwright is not installed; Cornerstone collector requires playwright to capture a Bearer token.",
                )

            session = _session_with_cookies(cookies)

            # 2) Gather job IDs from search
            page = 1
            page_size = 50
            seen: set[str] = set()
            ids: List[str] = []

            while True:
                data = _fetch_search_page(
                    session=session,
                    cfg=cfg,
                    token=token,
                    page_number=page,
                    page_size=page_size,
                    country_codes=cfg.default_country_codes,
                )
                meta["pages"] += 1

                page_ids = _extract_job_ids(data)
                if not page_ids:
                    break

                new_ids = [jid for jid in page_ids if jid not in seen]
                if not new_ids:
                    break

                for jid in new_ids:
                    seen.add(jid)
                    ids.append(jid)

                if len(page_ids) < page_size:
                    break

                page += 1
                if page > 200:
                    break

            meta["job_ids"] = len(ids)

            # 3) Fetch details in parallel
            if not ids:
                return CollectResult(
                    collector=self.name,
                    company=company.company,
                    careers_url=company.careers_url,
                    raw_jobs=[],
                    meta=meta,
                    error=None,
                )

            max_workers = min(8, len(ids))

            def _fetch_and_parse(job_id: str) -> Optional[Dict[str, Any]]:
                details = _fetch_job_details(session=session, cfg=cfg, token=token, job_id=job_id)
                meta["detail_fetches"] += 1
                return _parse_details_to_raw(cfg, company.company, job_id, details)

            out: List[Dict[str, Any]] = []
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {ex.submit(_fetch_and_parse, jid): jid for jid in ids}
                for fut in as_completed(futures):
                    try:
                        rec = fut.result()
                        if rec:
                            out.append(rec)
                    except Exception:
                        continue

            raw_jobs = out
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
