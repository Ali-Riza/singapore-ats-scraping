from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import unquote, urlparse, urlsplit

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


APOLLO_MARKER = "window.SEEK_APOLLO_DATA ="
DEFAULT_BASE_URL = "https://sg.jobstreet.com"
DEFAULT_SITE_KEY = "SG-Main"
V5_SEARCH_PATH = "/api/jobsearch/v5/search"

# The v5 endpoint is a keyword search, not a company feed: querying "Micron
# Technology" also returns postings from contractors that merely mention Micron
# as a client. Compare the advertiser against the input company so those
# foreign listings are dropped instead of being relabelled.
_LEGAL_FORM_TOKENS = frozenset(
    {
        "pte", "pty", "ltd", "limited", "llc", "inc", "incorporated", "co",
        "company", "corp", "corporation", "gmbh", "bv", "nv", "sa", "ag",
        "plc", "lp", "llp", "holdings", "holding", "group", "international",
        "global", "asia", "pacific", "apac", "sea", "singapore", "sg",
        "regional", "and", "the", "of",
    }
)


# Words that merely qualify an entity within one company: a legal vehicle, a
# region, or the industry the company is already known for. An advertiser may
# add or drop these and still be the same employer. Words naming a distinct
# line of business ("chemicals", "elastomers") are deliberately absent, since
# those separate sibling companies in a group.
_SHARED_GROUP_QUALIFIERS = frozenset(
    {
        "semiconductors", "semiconductor", "technologies", "technology",
        "services", "service", "solutions", "systems", "energy", "industries",
        "industrial", "manufacturing", "operations", "enterprise", "enterprises",
        "offshore", "marine", "engineering", "consulting", "trading",
        "east", "west", "north", "south", "central", "far", "middle",
        "europe", "america", "americas", "africa", "china", "japan", "korea",
        "malaysia", "indonesia", "thailand", "vietnam", "philippines", "india",
        "sdn", "bhd", "kk", "gk", "srl", "spa", "as", "ab", "oy", "aps",
    }
)


def _company_tokens(name: str) -> frozenset:
    """Reduce a company name to its distinctive tokens for fuzzy comparison."""
    cleaned = re.sub(r"[^0-9a-z]+", " ", str(name or "").casefold())
    return frozenset(
        tok for tok in cleaned.split() if tok and tok not in _LEGAL_FORM_TOKENS
    )


def advertiser_matches_company(advertiser: str, company: str) -> bool:
    """True if `advertiser` plausibly denotes the same employer as `company`.

    Unknown advertisers are accepted: a missing field is not evidence of a
    mismatch, and dropping those rows would lose legitimate postings.

    Sharing one distinctive token is not enough. Sister companies in a group
    cross-list each other's postings, and "Mitsui & Co." -> {mitsui} against
    "Mitsui Chemicals" -> {chemicals, mitsui} would otherwise match in both
    directions, leaving the same job filed under both employers.
    """
    adv_tokens = _company_tokens(advertiser)
    if not adv_tokens:
        return True

    want_tokens = _company_tokens(company)
    if not want_tokens:
        return True

    if adv_tokens == want_tokens:
        return True

    # Whichever side is more specific, the extra words decide. A qualifier that
    # only narrows a region or business line ("Micron Semiconductors" vs
    # "Micron Technology") still denotes the same employer; a different line of
    # business ("Chemicals" vs bare "Mitsui") denotes a sibling company.
    if not adv_tokens & want_tokens:
        return False

    extra = adv_tokens ^ want_tokens
    return extra <= _SHARED_GROUP_QUALIFIERS


def parse_v5_job(item: Dict[str, Any], origin: str) -> Optional[Dict[str, Any]]:
    return _parse_v5_job(item, origin)


def scrape_company_page(
    url: str,
    *,
    company: Optional[str] = None,
    site_key: str = DEFAULT_SITE_KEY,
    timeout: float = 30,
    delay: float = 1,
    max_pages: int = 100,
    page_size: int = 30,
) -> List[Dict[str, Any]]:
    """Compatibility wrapper mirroring the working standalone scraper."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0 Safari/537.36"
            ),
            "Accept-Language": "en-SG,en;q=0.9",
            "Accept": "application/json",
        }
    )

    origin = _api_origin(url)
    api_url = f"{origin}{V5_SEARCH_PATH}"
    company_query = (company or "").strip() or _infer_company_query(url)
    jobs_by_id: Dict[str, Dict[str, Any]] = {}

    for page in range(1, max_pages + 1):
        response = session.get(
            api_url,
            params={
                "siteKey": site_key,
                "keywords": company_query,
                "pageSize": page_size,
                "page": page,
            },
            timeout=timeout,
        )
        response.raise_for_status()

        payload = response.json()
        raw_jobs = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(raw_jobs, list):
            raise ValueError("Unexpected response from Jobstreet v5 search API")

        new_jobs = 0
        for item in raw_jobs:
            if not isinstance(item, dict):
                continue
            job = parse_v5_job(item, origin)
            if job is None:
                continue
            if job["job_id"] not in jobs_by_id:
                new_jobs += 1
            jobs_by_id[job["job_id"]] = job

        if len(raw_jobs) < page_size or new_jobs == 0:
            break
        if delay > 0:
            import time
            time.sleep(delay)

    return list(jobs_by_id.values())


def _extract_apollo_data(html: str) -> Dict[str, Any]:
    """Extract the JSON attached to window.SEEK_APOLLO_DATA."""
    marker_position = html.find(APOLLO_MARKER)
    if marker_position < 0:
        raise ValueError("window.SEEK_APOLLO_DATA was not found in the HTML")

    json_start = marker_position + len(APOLLO_MARKER)
    json_text = html[json_start:].lstrip()
    data, _ = json.JSONDecoder().raw_decode(json_text)
    if not isinstance(data, dict):
        raise ValueError("SEEK_APOLLO_DATA is not a JSON object")
    return data


def _resolve_reference(value: Any, apollo: Dict[str, Any]) -> Any:
    if isinstance(value, dict) and isinstance(value.get("__ref"), str):
        return apollo.get(value["__ref"], {})
    return value


def _localized_value(data: Any, prefix: str) -> Any:
    if not isinstance(data, dict):
        return None
    if prefix in data:
        return data[prefix]
    for key, value in data.items():
        if key.startswith(f"{prefix}("):
            return value
    return None


def _nested(data: Any, *keys: str) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _search_responses(apollo: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    root_query = apollo.get("ROOT_QUERY")
    query_cache = root_query if isinstance(root_query, dict) else apollo

    for key, value in query_cache.items():
        if not key.startswith("jobSearchV7(") or not isinstance(value, dict):
            continue
        jobs = _nested(value, "results", "jobs")
        if isinstance(jobs, list):
            yield value


def _first_text(value: Any, *keys: str) -> Optional[str]:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    if isinstance(current, str):
        current = current.strip()
        return current or None
    return None


def _infer_company_query(url: str) -> str:
    slug = unquote(urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1])
    if slug.lower().endswith("-jobs"):
        slug = slug[:-5]
    company = " ".join(part for part in slug.replace("_", "-").split("-") if part)
    if not company:
        raise ValueError("Could not infer the company name from the URL")
    return company


def _api_origin(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.netloc:
        raise ValueError("Expected an https:// Jobstreet URL")
    return f"{parts.scheme}://{parts.netloc}"


def _parse_v5_job(item: Dict[str, Any], origin: str) -> Optional[Dict[str, Any]]:
    job_id = str(item.get("id") or "").strip()
    title = str(item.get("title") or "").strip()
    if not job_id or not title:
        return None

    locations = item.get("locations") if isinstance(item.get("locations"), list) else []
    location = locations[0] if locations and isinstance(locations[0], dict) else {}

    arrangements: List[str] = []
    arrangement_data = _nested(item, "workArrangements", "data") or []
    for arrangement in arrangement_data:
        label = _first_text(arrangement, "label", "text")
        if label and label not in arrangements:
            arrangements.append(label)

    work_types = [
        value.strip()
        for value in (item.get("workTypes") or [])
        if isinstance(value, str) and value.strip()
    ]

    bullet_values = item.get("bulletPoints") or item.get("sellingPoints") or []
    if isinstance(bullet_values, dict):
        bullet_values = bullet_values.get("data") or []
    bullets: List[str] = []
    for value in bullet_values if isinstance(bullet_values, list) else []:
        if isinstance(value, str) and value.strip():
            bullets.append(value.strip())
        elif isinstance(value, dict):
            text = value.get("text") or value.get("label")
            if isinstance(text, str) and text.strip():
                bullets.append(text.strip())

    classifications: List[str] = []
    for key in ("classification", "subClassification"):
        value = item.get(key)
        if isinstance(value, dict):
            label = value.get("description") or value.get("label")
        else:
            label = value
        if isinstance(label, str) and label.strip() and label.strip() not in classifications:
            classifications.append(label.strip())

    country_code = location.get("countryCode")
    country = "Singapore" if country_code == "SG" else country_code
    company = (
        _first_text(item, "advertiser", "description")
        or _first_text(item, "advertiser", "name")
        or (item.get("companyName") if isinstance(item.get("companyName"), str) else None)
    )

    job_url = f"{origin}/job/{job_id}"
    return {
        "job_id": job_id,
        "title": title,
        "company": company,
        "location": location.get("label"),
        "country": country or "Singapore",
        "categories": " | ".join(classifications),
        "work_type": " | ".join(work_types),
        "work_arrangement": " | ".join(arrangements),
        "salary_min": None,
        "salary_max": None,
        "salary_currency": None,
        "salary_period": None,
        "salary_display": item.get("salaryLabel") or None,
        "listed_at": item.get("listingDate"),
        "summary": item.get("teaser") or item.get("abstract"),
        "bullets": " | ".join(bullets),
        "url": job_url,
        "job_url": job_url,
        "source": "jobstreet",
        "careers_url": origin,
    }


def _parse_jobs_from_apollo_html(html: str) -> List[Dict[str, Any]]:
    apollo = _extract_apollo_data(html)
    jobs_by_id: Dict[str, Dict[str, Any]] = {}

    for response in _search_responses(apollo):
        for raw_job in _nested(response, "results", "jobs") or []:
            if not isinstance(raw_job, dict):
                continue
            job_id = str(raw_job.get("id") or "").strip()
            if not job_id:
                continue

            organisation = _resolve_reference(raw_job.get("organisation"), apollo)
            location = _resolve_reference(raw_job.get("location"), apollo)
            title = str(raw_job.get("title") or "").strip()
            if not title:
                continue

            job_url = f"{DEFAULT_BASE_URL}/job/{job_id}"
            parsed = {
                "job_id": job_id,
                "job_title": title,
                "location": _nested(location, "displayName", "text") or "",
                "company": (
                    _nested(raw_job, "advertiser", "name")
                    or (organisation.get("name") if isinstance(organisation, dict) else None)
                    or ""
                ),
                "posted_date": "",
                "job_url": job_url,
                "url": job_url,
                "careers_url": DEFAULT_BASE_URL,
            }

            listed_at = raw_job.get("listedAt") if isinstance(raw_job.get("listedAt"), dict) else {}
            if isinstance(listed_at, dict):
                dt = listed_at.get("dateTimeUtc")
                if isinstance(dt, str):
                    parsed["posted_date"] = dt[:10]

            jobs_by_id[job_id] = parsed

    return list(jobs_by_id.values())


@dataclass(frozen=True)
class _JobStreetConfig:
    page_size: int = 30
    max_pages: int = 100


class JobStreetCompanyPageCollector(BaseCollector):

    name = "jobstreet_company_page"

    def __init__(self, cfg: Optional[_JobStreetConfig] = None) -> None:
        self.cfg = cfg or _JobStreetConfig()

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        careers_url = company.careers_url or DEFAULT_BASE_URL
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0 Safari/537.36"
                ),
                "Accept-Language": "en-SG,en;q=0.9",
                "Accept": "application/json",
            }
        )

        origin = _api_origin(careers_url)
        api_url = f"{origin}{V5_SEARCH_PATH}"
        company_query = (company.company or "").strip() or _infer_company_query(careers_url)

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "status_codes": [],
            "visited_urls": [],
            "total_unique": 0,
            "source": "jobstreet_v5_api",
        }

        jobs_by_id: Dict[str, Dict[str, Any]] = {}

        try:
            for page in range(1, self.cfg.max_pages + 1):
                response = session.get(
                    api_url,
                    params={
                        "siteKey": "SG-Main",
                        "keywords": company_query,
                        "pageSize": self.cfg.page_size,
                        "page": page,
                    },
                    timeout=30,
                )
                meta["status_codes"].append(response.status_code)
                meta["visited_urls"].append(response.url)
                meta["pages"] += 1
                response.raise_for_status()

                payload = response.json()
                raw_items = payload.get("data") if isinstance(payload, dict) else None
                if not isinstance(raw_items, list):
                    break

                new_jobs = 0
                for item in raw_items:
                    if not isinstance(item, dict):
                        continue
                    job = _parse_v5_job(item, origin)
                    if job is None:
                        continue
                    if job["job_id"] not in jobs_by_id:
                        new_jobs += 1
                    jobs_by_id[job["job_id"]] = job

                if not raw_items or new_jobs == 0:
                    break

                if len(raw_items) < self.cfg.page_size:
                    break

            raw_jobs = list(jobs_by_id.values())
            meta["total_unique"] = len(raw_jobs)

        except Exception as exc:  # pragma: no cover - best-effort fallback
            meta["error"] = str(exc)
            meta["fallback_html_apollo"] = True
            try:
                html = requests.get(careers_url, timeout=30, headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                    "Accept-Language": "en-SG,en;q=0.9",
                }).text
                raw_jobs = _parse_jobs_from_apollo_html(html)
                meta["total_unique"] = len(raw_jobs)
            except Exception as fallback_exc:  # pragma: no cover
                meta["fallback_error"] = str(fallback_exc)

        return CollectResult(
            collector=self.name,
            company=company.company,
            careers_url=company.careers_url,
            raw_jobs=raw_jobs,
            meta=meta,
            error=None,
        )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        records: List[JobRecord] = []
        dropped = 0
        for raw in result.raw_jobs:
            advertiser = str(raw.get("company") or "")
            if not advertiser_matches_company(advertiser, result.company):
                dropped += 1
                continue
            records.append(self._map_one(raw, result))
        if dropped:
            result.meta["dropped_foreign_advertiser"] = dropped
        return records

    def _map_one(self, raw: Dict[str, Any], result: CollectResult) -> JobRecord:
        title = str(raw.get("job_title") or raw.get("title") or "").strip()
        location = str(raw.get("location") or "").strip()
        job_id = str(raw.get("job_id") or "").strip()
        posted_date = str(raw.get("posted_date") or raw.get("listed_at") or "").strip()
        if len(posted_date) > 10 and "T" in posted_date:
            posted_date = posted_date[:10]
        job_url = str(raw.get("job_url") or raw.get("url") or "").strip()

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
