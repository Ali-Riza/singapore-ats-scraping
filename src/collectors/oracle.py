from __future__ import annotations # For forward compatibility with future Python versions

from typing import Any, Dict, List

from urllib.parse import urlparse, parse_qs
import requests # For making HTTP requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord

# Oracle ATS constants (Oracle-specific)

DEFAULT_HEADERS = {
    "accept": "*/*",
    "accept-language": "en",
    # Oracle HCM endpoints frequently expect this media type.
    "content-type": "application/vnd.oracle.adf.resourceitem+json;charset=utf-8",
    "ora-irc-language": "en",
    "user-agent": "Mozilla/5.0",
}

HONEYWELL_UI_HOSTS = {"careers.honeywell.com"}
HONEYWELL_DEFAULT_REST_BASE = (
    "https://ibqbjb.fa.ocs.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
)
HONEYWELL_DEFAULT_SITE_NUMBER = "CX_1"
HONEYWELL_LOCATION_LABEL = "Singapore"
HONEYWELL_LOCATION_LEVEL = "country"
HONEYWELL_MODE = "location"

EXPAND = (
    "requisitionList.workLocation,"
    "requisitionList.otherWorkLocations,"
    "requisitionList.secondaryLocations,"
    "flexFieldsFacet.values,"
    "requisitionList.requisitionFlexFields"
)

FACETS = (
    "LOCATIONS;"
    "WORK_LOCATIONS;"
    "WORKPLACE_TYPES;"
    "TITLES;"
    "CATEGORIES;"
    "ORGANIZATIONS;"
    "POSTING_DATES;"
    "FLEX_FIELDS"
)

class OracleCollector(BaseCollector):
    """ Collector for Oracle ATS."""

    name = "oracle"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        """Collect raw job objects from the ATS.

        raw_jobs is a list of ATS-specific dictionaries.
        Example structure (NOT normalized):

        - job_id: str
        - title: str
        - location: str
        - description: str
        - posted_date: str
        - apply_url: str

        Exact keys depend on the ATS and are intentionally left unstandardized.
        """
        
        # Oracle uses fixed page size of 25 jobs per page
        limit = 25
        offset = 0
        max_pages = 200

        raw_jobs: List[Dict[str, Any]] = [] 
        meta: Dict[str, Any] = {
            "pages": 0,
            "limit": limit, # e.g. 25 jobs per page
            "offsets": [], # starting offsets for each page e.g. [0, 25, 50, ...]
            "status_codes": [], # HTTP status codes for each page fetched
            "total_raw": 0, # total raw jobs collected
        }

        try:
            # Extract parameters (siteNumber, locationMode, locationValue, restBase) from the jobs page URL 
            site = _site_number_from_ui(company.careers_url)
            location_mode, location_value = _location_from_ui(company.careers_url)
            rest_base = _rest_base_from_ui(company.careers_url)

            # initiate session (Cookies)
            session = requests.Session()
            # Best-effort warmup: some tenants need cookies, but SSL / blocking
            # on the UI host should not prevent API collection.
            try:
                session.get(
                    company.careers_url,
                    headers={"accept": "text/html", "accept-language": "en"},
                    timeout=30,
                )
            except Exception as e:
                meta["ui_warmup_error"] = str(e)

            # If REST host differs from UI host, a warmup on the REST host can help
            # establish cookies/headers. Also best-effort.
            try:
                ui_host = (urlparse(company.careers_url).netloc or "").lower()
                rest_host = (urlparse(rest_base).netloc or "").lower()
                if ui_host and rest_host and ui_host != rest_host:
                    session.get(rest_base, headers={"accept": "application/json"}, timeout=30)
            except Exception as e:
                meta["rest_warmup_error"] = str(e)

            # Prepare headers for API requests
            headers = dict(DEFAULT_HEADERS)
            
            # Include ORA_CX_USERID cookie if present because it's required for API access
            cx_userid = session.cookies.get("ORA_CX_USERID")
            if cx_userid:
                headers["ora-irc-cx-userid"] = cx_userid
            
            for _ in range(max_pages):
                # Build finder string for the API request
                finder = _build_finder(site, location_mode, location_value, limit, offset)
                # Make the API request to fetch one page of jobs
                # Some tenants require offset as a top-level query param, not just in finder.
                params = {"onlyData": "true", "expand": EXPAND, "finder": finder, "offset": offset}

                # Make the GET request to the REST API endpoint
                r = session.get(
                    rest_base,
                    params=params,
                    headers={**headers, "referer": company.careers_url},
                    timeout=30,
                )
                # Update meta information
                meta["status_codes"].append(r.status_code)
                # Check for successful response
                ct = (r.headers.get("content-type") or "").lower()
                # If response is not JSON or status is not 200, stop fetching further pages
                if r.status_code != 200 or "json" not in ct:
                    meta["total_raw"] = len(raw_jobs)
                    return CollectResult(
                        collector=self.name,
                        company=company.company,
                        careers_url=company.careers_url,
                        raw_jobs=raw_jobs,
                        meta=meta,
                        error=f"Non-JSON or non-200 response: status={r.status_code}, content-type={ct}",
                    )

                # load full JSON payload
                payload = r.json()

                items = payload.get("items") or []
                lens = []
                for it in items:
                    if isinstance(it, dict) and isinstance(it.get("requisitionList"), list):
                        lens.append(len(it["requisitionList"]))

                # Extract raw job requisitions from the payload
                reqs = _extract_requisitions(payload)

                # if no jobs found, stop fetching further pages
                if not reqs:
                    break
                    
                # Update meta information
                raw_jobs.extend(reqs)
                meta["pages"] += 1
                meta["offsets"].append(offset)

                # If fewer jobs than limit were returned, we've reached the last page
                if len(reqs) < limit:
                    break

                # Increment offset for the next page
                offset += limit

            # Finalize meta information
            meta["total_raw"] = len(raw_jobs)

            # Return successful CollectResult (raw collection only)
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
        """ Map raw job dicts to standardized JobRecord instances."""
        records: List[JobRecord] = []
        for raw in result.raw_jobs:
            records.append(self._map_one_raw_job(raw, result))
        return records

    def _map_one_raw_job(self, raw: Dict[str, Any], result: CollectResult) -> JobRecord:
        """Map a single raw Oracle job dict to a standardized JobRecord."""

        job_id = (
            self._pick(raw, "requisitionNumber", "RequisitionNumber")
            or self._pick(raw, "Id", "id", "requisitionId", "RequisitionId")
        )
        title = self._pick(raw, "Title", "title", "requisitionTitle")
        posted_date = self._pick(raw, "PostedDate", "postedDate", "postingDate", "PostingDate")
        location = ""
        wl = raw.get("workLocation")
        if isinstance(wl, list) and wl:
            first = wl[0]
            if isinstance(first, dict):
                location = self._pick(first, "LocationName", "locationName", "Name", "name")
        elif isinstance(wl, dict):
            # falls ein anderer Tenant doch dict liefert
            location = self._pick(wl, "LocationName", "locationName", "Name", "name")

        # Honeywell requirement: always use "Singapore" as the location label.
        ui = urlparse(result.careers_url)
        if (ui.netloc or "").lower() in HONEYWELL_UI_HOSTS:
            location = HONEYWELL_LOCATION_LABEL

        job_url = self._pick(
            raw,
            "externalUrl", "ExternalUrl",
            "applyUrl", "ApplyUrl",
            "jobDetailUrl", "JobDetailUrl"
        )
        if not job_url and job_id:
            # Default UI job URL pattern.
            u = urlparse(result.careers_url)

            # Honeywell: UI is a "preview" URL and includes location params.
            if (u.netloc or "").lower() in HONEYWELL_UI_HOSTS:
                parts = [p for p in u.path.split("/") if p]
                lang = "en"
                if parts and len(parts[0]) == 2:
                    lang = parts[0]

                site_slug = "Honeywell"
                if "sites" in parts:
                    i = parts.index("sites")
                    if i + 1 < len(parts) and parts[i + 1]:
                        site_slug = parts[i + 1]

                loc_mode, loc_value = _location_from_ui(result.careers_url)

                # Example:
                # https://careers.honeywell.com/en/sites/Honeywell/jobs/preview/133014/?location=Singapore&locationId=...
                base = f"{u.scheme}://{u.netloc}/{lang}/sites/{site_slug}/jobs/preview/{job_id}/"
                if loc_mode == "locationId" and loc_value:
                    job_url = (
                        f"{base}?location={HONEYWELL_LOCATION_LABEL}"
                        f"&locationId={loc_value}"
                        f"&locationLevel={HONEYWELL_LOCATION_LEVEL}"
                        f"&mode={HONEYWELL_MODE}"
                    )
                else:
                    job_url = base
            else:
                site = _site_number_from_ui(result.careers_url)
                job_url = f"{u.scheme}://{u.netloc}/hcmUI/CandidateExperience/en/sites/{site}/job/{job_id}"

        
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
    
        
    

def _site_number_from_ui(jobs_page_url: str) -> str:
    """ Extracts the siteNumber from the jobs UI URL."""
    u = urlparse(jobs_page_url)
    q = parse_qs(u.query)
    if "siteNumber" in q and q["siteNumber"]:
        return q["siteNumber"][0]

    # parts like: ["careers", "sites", "12345", "jobsearch"]
    parts = [p for p in u.path.split("/") if p]
    # Find "sites" and get the next part as siteNumber
    if "sites" in parts:
        # Get index of "sites"
        i = parts.index("sites")
        # Return the next part as siteNumber
        if i + 1 < len(parts):
            site = parts[i + 1]
            # Honeywell UI uses /sites/Honeywell/ but API expects CX_1.
            if (u.netloc or "").lower() in HONEYWELL_UI_HOSTS:
                if site and not site.isdigit() and not site.startswith("CX_"):
                    return HONEYWELL_DEFAULT_SITE_NUMBER
            return site  # siteNumber increment

    return "1"


def _location_from_ui(jobs_page_url: str) -> tuple[str, str]:
    """ Extracts location mode and value from the jobs UI URL."""

    # extract query parameters 
    q = parse_qs(urlparse(jobs_page_url).query)

    # Check known location parameters
    if "selectedLocationsFacet" in q:
        return "selectedLocationsFacet", q["selectedLocationsFacet"][0]
    if "locationId" in q:
        return "locationId", q["locationId"][0]

    return "", ""


def _rest_base_from_ui(jobs_page_url: str) -> str:
    """ Constructs the base REST API URL (json endpoint) from the jobs UI URL."""
    u = urlparse(jobs_page_url)
    q = parse_qs(u.query)

    # Explicit override (full URL or host).
    for key in (
        "restBase",
        "rest_base",
        "restUrl",
        "rest_url",
        "oracleHost",
        "oracle_host",
        "oracleCloudHost",
        "oracle_cloud_host",
    ):
        if key in q and q[key]:
            v = q[key][0].strip()
            if v:
                if "://" in v:
                    return v
                # treat as host
                return f"{u.scheme}://{v}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

    # Honeywell: UI host differs from OracleCloud REST host.
    if (u.netloc or "").lower() in HONEYWELL_UI_HOSTS:
        return HONEYWELL_DEFAULT_REST_BASE

    return f"{u.scheme}://{u.netloc}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

def _build_finder(
    site: str,
    location_mode: str,
    location_value: str,
    limit: int,
    offset: int,
) -> str:
    """
    Build the Oracle 'finder' query string for one page.
    """

    finder = (
        f"findReqs;"
        f"siteNumber={site},"
        f"facetsList={FACETS},"
        f"limit={limit},"
        f"offset={offset},"
    )

    if location_mode == "selectedLocationsFacet":
        finder += (
            "lastSelectedFacet=LOCATIONS,"
            f"selectedLocationsFacet={location_value},"
        )
    elif location_mode == "locationId":
        finder += f"locationId={location_value},"

    finder += "sortBy=POSTING_DATES_DESC"

    return finder

def _extract_requisitions(payload: dict) -> list[dict]:
    """ Extracts the list of requisitions=jobs from the API response payload (payload means the full JSON response)."""
    items = payload.get("items") or []
    if not isinstance(items, list):
        return []

    reqs: list[dict] = []
    for it in items:
        if isinstance(it, dict) and isinstance(it.get("requisitionList"), list):
            reqs.extend(it["requisitionList"])

    return reqs