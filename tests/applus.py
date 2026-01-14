#!/usr/bin/env python3

import argparse
import json
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://jobs.applus.com"
CAREERS_URL = f"{BASE_URL}/en"
SOURCE = "magnolia-nextjs"
COMPANY = "Applus+"


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: float = 30.0,
    max_attempts: int = 3,
    backoff_s: float = 0.8,
) -> requests.Response:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_s * attempt)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def _get_build_id(session: requests.Session) -> str:
    resp = _get_with_retries(session, CAREERS_URL)
    soup = BeautifulSoup(resp.text, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        raise RuntimeError("Could not find __NEXT_DATA__ on careers page")
    data = json.loads(script.string)
    build_id = data.get("buildId")
    if not build_id:
        raise RuntimeError("Could not extract buildId from __NEXT_DATA__")
    return str(build_id)


def _make_data_url(build_id: str, *, country_id: Optional[int], published_time_type_id: Optional[int], vacancy_type_id: Optional[int]) -> str:
    parts: List[str] = []
    if country_id is not None:
        parts.append(f"countryID={country_id}")
    if published_time_type_id is not None:
        parts.append(f"publishedTimeTypeID={published_time_type_id}")
    if vacancy_type_id is not None:
        parts.append(f"vacancyTypeID={vacancy_type_id}")

    qs = "&".join(parts)
    return f"{BASE_URL}/_next/data/{build_id}/en.json" + (f"?{qs}" if qs else "")


def _fetch_page_props(
    session: requests.Session,
    build_id: str,
    *,
    country_id: Optional[int],
    published_time_type_id: Optional[int],
    vacancy_type_id: Optional[int],
) -> Dict[str, Any]:
    url = _make_data_url(
        build_id,
        country_id=country_id,
        published_time_type_id=published_time_type_id,
        vacancy_type_id=vacancy_type_id,
    )
    js = _get_with_retries(session, url).json()
    pp = js.get("pageProps")
    if not isinstance(pp, dict):
        raise RuntimeError("Unexpected _next/data JSON: missing pageProps")
    return pp


def _extract_countries(page_props: Dict[str, Any]) -> List[Dict[str, Any]]:
    md = page_props.get("masterData")
    if not isinstance(md, dict):
        return []
    countries = md.get("countries")
    if not isinstance(countries, list):
        return []
    return [c for c in countries if isinstance(c, dict) and c.get("id") is not None]


def _extract_jobs(page_props: Dict[str, Any]) -> List[Dict[str, Any]]:
    jobs = page_props.get("jobPositionList")
    if not isinstance(jobs, list):
        return []
    return [j for j in jobs if isinstance(j, dict)]


def _find_first_country_with_jobs(
    session: requests.Session,
    build_id: str,
    *,
    countries: Iterable[Dict[str, Any]],
    skip_country_id: Optional[int],
    published_time_type_id: Optional[int],
    vacancy_type_id: Optional[int],
) -> Optional[Tuple[int, str, List[Dict[str, Any]]]]:
    for c in countries:
        cid = c.get("id")
        label = c.get("label") or ""
        try:
            cid_int = int(cid)
        except Exception:
            continue

        if skip_country_id is not None and cid_int == skip_country_id:
            continue

        pp = _fetch_page_props(
            session,
            build_id,
            country_id=cid_int,
            published_time_type_id=published_time_type_id,
            vacancy_type_id=vacancy_type_id,
        )
        jobs = _extract_jobs(pp)
        if jobs:
            return (cid_int, str(label), jobs)
    return None


def _to_canonical_job(job: Dict[str, Any], *, country_label_fallback: Optional[str]) -> Dict[str, Any]:
    job_id = job.get("id")
    job_id_str = str(job_id) if job_id is not None else None

    location = job.get("location")
    if not location and country_label_fallback:
        location = country_label_fallback

    job_url = f"{BASE_URL}/en/job-detail?id={job_id}" if job_id is not None else None

    return {
        "company": COMPANY,
        "job_title": job.get("title"),
        "location": location,
        "job_id": job_id_str,
        "posted_date": None,
        "job_url": job_url,
        "source": SOURCE,
        "careers_url": CAREERS_URL,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Applus jobs scraper (Next.js + Magnolia). Prints canonical job dicts; defaults to Singapore (countryID=202). "
            "If no Singapore jobs, you can optionally iterate all countries until it finds at least one job to validate the scraper."
        )
    )
    parser.add_argument("--country-id", type=int, default=202, help="Country ID (default: 202 = Singapore)")
    parser.add_argument("--published-time-type-id", type=int, default=None, help="Optional publishedTimeTypeID filter")
    parser.add_argument("--vacancy-type-id", type=int, default=None, help="Optional vacancyTypeID filter")
    parser.add_argument("--max-jobs", type=int, default=200, help="Max jobs to print")

    parser.add_argument(
        "--validate",
        action="store_true",
        default=False,
        help="If selected country has 0 jobs, iterate countries until a job is found (validation mode)",
    )

    args = parser.parse_args()

    session = requests.Session()

    build_id = _get_build_id(session)

    page_props = _fetch_page_props(
        session,
        build_id,
        country_id=args.country_id,
        published_time_type_id=args.published_time_type_id,
        vacancy_type_id=args.vacancy_type_id,
    )
    jobs = _extract_jobs(page_props)

    chosen_country_id = args.country_id
    chosen_country_label: Optional[str] = None

    countries = _extract_countries(page_props)
    for c in countries:
        if int(c.get("id")) == args.country_id:
            chosen_country_label = str(c.get("label") or "")
            break

    if not jobs and bool(args.validate):
        found = _find_first_country_with_jobs(
            session,
            build_id,
            countries=countries,
            skip_country_id=args.country_id,
            published_time_type_id=args.published_time_type_id,
            vacancy_type_id=args.vacancy_type_id,
        )
        if found:
            chosen_country_id, chosen_country_label, jobs = found
            print(
                f"No jobs for countryID={args.country_id}; using countryID={chosen_country_id} ({chosen_country_label}) for validation.",
                file=sys.stderr,
            )

    out_count = 0
    for job in jobs:
        if out_count >= args.max_jobs:
            break
        print(_to_canonical_job(job, country_label_fallback=chosen_country_label))
        out_count += 1

    print(f"Found {out_count} jobs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
