#!/usr/bin/env python3

import argparse
import sys
import time
from typing import Any, Dict, Iterable, List, Optional

import requests


SOURCE = "mycareersfuture"
PORTAL_BASE_URL = "https://www.mycareersfuture.gov.sg"
API_BASE_URL = "https://api.mycareersfuture.gov.sg"


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 45.0,
    max_attempts: int = 3,
    backoff_s: float = 0.8,
) -> requests.Response:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_s * attempt)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def _build_headers() -> Dict[str, str]:
    # Minimal set of headers that works for the public API.
    return {
        "accept": "application/json, text/plain, */*",
        "mcf-client": "jobseeker",
        "origin": PORTAL_BASE_URL,
        "referer": f"{PORTAL_BASE_URL}/",
        "user-agent": "Mozilla/5.0",
    }


def _extract_location(job: Dict[str, Any]) -> Optional[str]:
    addr = job.get("address")
    if not isinstance(addr, dict):
        return None

    if addr.get("isOverseas"):
        country = addr.get("overseasCountry")
        parts = [addr.get("foreignAddress1"), addr.get("foreignAddress2"), country]
        parts = [str(p).strip() for p in parts if p and str(p).strip()]
        return ", ".join(parts) if parts else None

    districts = addr.get("districts")
    if isinstance(districts, list) and districts:
        first = districts[0]
        if isinstance(first, dict):
            loc = first.get("location")
            if loc and str(loc).strip():
                return f"{str(loc).strip()}, Singapore"

    # Fallback to basic address parts
    parts: List[str] = []
    for key in ["building", "block", "street", "postalCode"]:
        val = addr.get(key)
        if val and str(val).strip():
            parts.append(str(val).strip())
    if parts:
        return ", ".join(parts) + ", Singapore"

    return "Singapore"


def _to_canonical_job(job: Dict[str, Any], *, careers_url: str) -> Dict[str, Any]:
    posted_company = job.get("postedCompany")
    company = None
    if isinstance(posted_company, dict):
        company = posted_company.get("name")

    metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
    posted_date = metadata.get("newPostingDate") or metadata.get("originalPostingDate")
    job_url = metadata.get("jobDetailsUrl")
    if not job_url:
        links = job.get("_links")
        if isinstance(links, dict):
            self_link = links.get("self")
            if isinstance(self_link, dict):
                job_url = self_link.get("href")

    return {
        "company": company,
        "job_title": job.get("title"),
        "location": _extract_location(job),
        "job_id": job.get("uuid"),
        "posted_date": posted_date,
        "job_url": job_url,
        "source": SOURCE,
        "careers_url": careers_url,
    }


def _iter_jobs(session: requests.Session, *, uen: str, page_size: int, max_items: int, debug: bool) -> Iterable[Dict[str, Any]]:
    url = f"{API_BASE_URL}/v2/jobs"
    headers = _build_headers()

    fetched = 0
    page = 0
    while fetched < max_items:
        params = {"limit": min(page_size, max_items - fetched), "page": page, "uen": uen}
        if debug:
            print(f"GET {url} params={params}", file=sys.stderr)

        js = _get_with_retries(session, url, params=params, headers=headers).json()
        results = js.get("results")
        if not isinstance(results, list) or not results:
            return

        for job in results:
            if not isinstance(job, dict):
                continue
            yield job
            fetched += 1
            if fetched >= max_items:
                return

        page += 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "MyCareersFuture (Singapore) job fetcher via public API. "
            "Fetches job postings for a specific employer UEN."
        )
    )
    parser.add_argument(
        "--uen",
        default="202321711W",
        help="Employer UEN to filter jobs (default: 202321711W from your curl)",
    )
    parser.add_argument("--max-jobs", type=int, default=50, help="Max jobs to print")
    parser.add_argument("--page-size", type=int, default=20, help="API page size (limit)")
    parser.add_argument("--debug", action="store_true", default=False)

    args = parser.parse_args()

    uen = (args.uen or "").strip()
    if not uen:
        print("Missing --uen", file=sys.stderr)
        print("Found 0 jobs")
        return 0

    careers_url = f"{PORTAL_BASE_URL}/"

    session = requests.Session()

    out_count = 0
    for raw in _iter_jobs(
        session,
        uen=uen,
        page_size=max(1, args.page_size),
        max_items=max(1, args.max_jobs),
        debug=args.debug,
    ):
        job = _to_canonical_job(raw, careers_url=careers_url)
        print(job)
        out_count += 1

    print(f"Found {out_count} jobs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
