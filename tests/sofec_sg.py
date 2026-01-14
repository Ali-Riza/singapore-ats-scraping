#!/usr/bin/env python3

import argparse
import html as html_lib
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests


SOURCE = "breezy-portal"
COMPANY = "SOFEC Singapore"
CAREERS_URL = "https://sofec-sg.us.careers.hr/"


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
            resp = session.get(url, timeout=timeout, headers={"Accept": "text/html,*/*"})
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_s * attempt)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def _strip_tags(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_position_paths(html: str) -> List[str]:
    # Breezy portals typically link positions under /p/<slug-or-id>.
    # When there are 0 jobs, no /p/ links will exist.
    patterns = [
        r'href="(/p/[^"#?\s]+)',
        r"href='(/p/[^'#?\s]+)",
        r'href="(https?://[^"\s]+/p/[^"#?\s]+)',
    ]

    found: Set[str] = set()
    for pat in patterns:
        for m in re.findall(pat, html, flags=re.IGNORECASE):
            if isinstance(m, tuple):
                m = m[0]
            found.add(m)

    return sorted(found)


def _guess_job_id_from_path(path_or_url: str) -> Optional[str]:
    m = re.search(r"/p/([^/?#]+)", path_or_url)
    return m.group(1) if m else None


def _parse_job_detail(html: str) -> Tuple[Optional[str], Optional[str]]:
    # Title: <h1> is typical on Breezy portals.
    title = None
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, flags=re.IGNORECASE | re.DOTALL)
    if m:
        title = _strip_tags(m.group(1))

    # Location: often present as an element with class="location".
    location = None
    m = re.search(
        r"class=\"[^\"]*location[^\"]*\"[^>]*>(.*?)</",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        location = _strip_tags(m.group(1))

    return title, location


def _to_canonical_job(*, job_id: Optional[str], job_title: Optional[str], location: Optional[str], job_url: str) -> Dict[str, Any]:
    return {
        "company": COMPANY,
        "job_title": job_title,
        "location": location,
        "job_id": job_id,
        "posted_date": None,
        "job_url": job_url,
        "source": SOURCE,
        "careers_url": CAREERS_URL,
    }


def _filter_by_country(jobs: List[Dict[str, Any]], *, country: str) -> List[Dict[str, Any]]:
    c = (country or "").strip().lower()
    if not c:
        return jobs

    # This portal is Singapore-only in practice, but keep the filter for consistency.
    needles = {
        "sg": ["singapore"],
        "singapore": ["singapore"],
    }.get(c, [c])

    out: List[Dict[str, Any]] = []
    for j in jobs:
        hay = " ".join([
            str(j.get("location") or ""),
            str(j.get("job_title") or ""),
            str(j.get("job_url") or ""),
        ]).lower()
        if any(n in hay for n in needles):
            out.append(j)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "SOFEC Singapore careers scraper (Breezy portal). "
            "Default is Singapore-only (even if zero); use --country '' for global/all."
        )
    )
    parser.add_argument("--careers-url", default=CAREERS_URL, help="Careers portal URL")
    parser.add_argument(
        "--country",
        default="singapore",
        help="Country filter (default: singapore). Use --country '' to disable filtering.",
    )
    parser.add_argument("--max-jobs", type=int, default=50, help="Max jobs to print")
    parser.add_argument("--debug", action="store_true", default=False)

    args = parser.parse_args()

    session = requests.Session()
    resp = _get_with_retries(session, args.careers_url)

    paths = _extract_position_paths(resp.text)
    if args.debug:
        print(f"Discovered {len(paths)} /p/ links")

    jobs: List[Dict[str, Any]] = []
    for p in paths:
        if len(jobs) >= args.max_jobs:
            break

        job_url = p if p.startswith("http") else urljoin(args.careers_url, p)
        job_id = _guess_job_id_from_path(job_url)

        # Fetch detail page to get reliable title/location.
        try:
            detail_resp = _get_with_retries(session, job_url)
            title, location = _parse_job_detail(detail_resp.text)
        except Exception:
            title, location = None, None

        jobs.append(_to_canonical_job(job_id=job_id, job_title=title, location=location, job_url=job_url))

    jobs = _filter_by_country(jobs, country=args.country)

    out_count = 0
    for j in jobs:
        if out_count >= args.max_jobs:
            break
        print(j)
        out_count += 1

    print(f"Found {out_count} jobs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
