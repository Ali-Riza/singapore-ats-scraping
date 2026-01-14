#!/usr/bin/env python3

import argparse
import sys
import time
from typing import Any, Dict, List, Optional, Set
from urllib.parse import quote

import requests


SOURCE = "umbraco-api"
COMPANY = "BMT"
BASE_URL = "https://www.bmt.org"
CAREERS_URL = f"{BASE_URL}/careers/vacancies/"
API_BASE_URL = f"{BASE_URL}/umbraco/api/v1/vacancies/"


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: float = 45.0,
    max_attempts: int = 3,
    backoff_s: float = 0.8,
) -> requests.Response:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(
                url,
                timeout=timeout,
                headers={"Accept": "application/json,text/plain,*/*"},
            )
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_s * attempt)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def _as_abs_url(path_or_url: Optional[str]) -> Optional[str]:
    if not path_or_url:
        return None
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    if path_or_url.startswith("/"):
        return f"{BASE_URL}{path_or_url}"
    return f"{BASE_URL}/{path_or_url}"


def _api_url_for_country(country: str) -> str:
    country = (country or "").strip()
    if not country:
        return API_BASE_URL

    # The site supports /vacancies/<country> as an extra path segment.
    # We URL-encode to handle spaces (e.g. "united kingdom").
    segment = quote(country.lower(), safe="")
    return API_BASE_URL.rstrip("/") + "/" + segment


def _to_canonical_job(item: Dict[str, Any]) -> Dict[str, Any]:
    countries = item.get("Countries") if isinstance(item.get("Countries"), list) else []
    country_names = [c.get("Name") for c in countries if isinstance(c, dict) and c.get("Name")]
    country = country_names[0] if country_names else None

    job_family = item.get("JobFamily") if isinstance(item.get("JobFamily"), list) else []
    job_family_names = [c.get("Name") for c in job_family if isinstance(c, dict) and c.get("Name")]

    employment_type = item.get("EmploymentType") if isinstance(item.get("EmploymentType"), list) else []
    employment_type_names = [c.get("Name") for c in employment_type if isinstance(c, dict) and c.get("Name")]

    return {
        "company": COMPANY,
        "job_title": item.get("Name"),
        "location": item.get("Location") or country,
        "job_id": item.get("Id"),
        "posted_date": item.get("DatePosted") or item.get("FormattedDatePosted"),
        "job_url": _as_abs_url(item.get("Url")),
        "source": SOURCE,
        "careers_url": CAREERS_URL,
        "country": country,
        "job_family": job_family_names[0] if job_family_names else None,
        "employment_type": employment_type_names[0] if employment_type_names else None,
    }


def _collect_countries(items: List[Dict[str, Any]]) -> List[str]:
    seen: Set[str] = set()
    for it in items:
        countries = it.get("Countries") if isinstance(it.get("Countries"), list) else []
        for c in countries:
            if not isinstance(c, dict):
                continue
            name = (c.get("Name") or "").strip()
            if name:
                seen.add(name)
    return sorted(seen)


def _filter_by_country(items: List[Dict[str, Any]], country: str) -> List[Dict[str, Any]]:
    c = (country or "").strip().lower()
    if not c:
        return items

    needles = {
        "sg": "singapore",
        "singapore": "singapore",
    }
    target = needles.get(c, c)

    out: List[Dict[str, Any]] = []
    for it in items:
        # Prefer explicit Countries[].Name
        countries = it.get("Countries") if isinstance(it.get("Countries"), list) else []
        country_names = [
            (x.get("Name") or "").strip().lower()
            for x in countries
            if isinstance(x, dict)
        ]
        if any(target == name for name in country_names if name):
            out.append(it)
            continue

        # Fallback: match within Location
        location = (it.get("Location") or "").strip().lower()
        if target and target in location:
            out.append(it)

    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "BMT vacancies scraper via Umbraco JSON API. "
            "Default is Singapore-only (even if zero); use --country '' for global/all."
        )
    )
    parser.add_argument("--api-url", default=API_BASE_URL, help="Base vacancies API URL")
    parser.add_argument(
        "--country",
        default="singapore",
        help="Country filter (default: singapore). Use --country '' to disable filtering.",
    )
    parser.add_argument("--max-jobs", type=int, default=50, help="Max jobs to print")
    parser.add_argument(
        "--list-countries",
        action="store_true",
        default=False,
        help="Print detected countries and exit",
    )

    args = parser.parse_args()

    session = requests.Session()

    # Always fetch the full list for --list-countries.
    if args.list_countries:
        resp = _get_with_retries(session, args.api_url)
        items = resp.json() if resp.content else []
        for c in _collect_countries(items):
            print(c)
        return 0

    # Try server-side country segment first for performance.
    items: List[Dict[str, Any]]
    requested_country = (args.country or "").strip()
    if requested_country:
        try:
            resp = _get_with_retries(session, _api_url_for_country(requested_country))
            items = resp.json() if resp.content else []
        except Exception:
            resp = _get_with_retries(session, args.api_url)
            items = resp.json() if resp.content else []
            items = _filter_by_country(items, requested_country)
    else:
        resp = _get_with_retries(session, args.api_url)
        items = resp.json() if resp.content else []

    out_count = 0
    for it in items:
        if out_count >= args.max_jobs:
            break
        print(_to_canonical_job(it))
        out_count += 1

    print(f"Found {out_count} jobs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
