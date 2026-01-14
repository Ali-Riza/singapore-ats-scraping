#!/usr/bin/env python3

import argparse
import re
import sys
import time
from typing import Any, Dict, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup


SOURCE = "wordpress-elementor"
COMPANY = "HBA Future Energy"
CAREERS_URL = "https://hbafutureenergy.com/contact-us/career/"


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: float = 45.0,
    max_attempts: int = 3,
    backoff_s: float = 0.8,
    headers: Optional[Dict[str, str]] = None,
) -> requests.Response:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout, headers=headers)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_s * attempt)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def _normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _extract_job_location(text: str) -> Optional[str]:
    # The page content includes a "Location:" header with bullet points.
    # We'll heuristically extract the first bullet that contains a country name.
    t = _normalize_ws(text)
    if not t:
        return None

    # Prefer an explicit Nigeria mention.
    m = re.search(r"Location:\s*(.*?)\s*Type:", t, flags=re.IGNORECASE)
    section = m.group(1) if m else t

    # Look for common separators in the list output.
    candidates = re.split(r"\s*[\u2022\-]\s*|\s*\|\s*|\s*;\s*", section)
    candidates = [_normalize_ws(c) for c in candidates if _normalize_ws(c)]

    # Choose the most informative candidate.
    for c in candidates:
        if "nigeria" in c.lower():
            return c

    # Fallback to any candidate that looks like a location.
    for c in candidates:
        if len(c) >= 4 and any(k in c.lower() for k in ["offshore", "onshore", "dubai", "nigeria", "ghana", "singapore", "malaysia", "uk", "korea"]):
            return c

    return None


def _iter_openings(html: str) -> Iterable[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    # The "Current Openings" section is rendered as nested accordion.
    for details in soup.find_all("details", class_=re.compile(r"\be-n-accordion-item\b")):
        title_el = details.select_one(".e-n-accordion-item-title-text")
        title = _normalize_ws(title_el.get_text(" ", strip=True) if title_el else "")
        if not title:
            continue

        # Content lives inside the details element.
        content_text = _normalize_ws(details.get_text(" ", strip=True))
        location = _extract_job_location(content_text)

        # Build a stable-ish job URL using the details id (anchor).
        details_id = details.get("id")
        job_url = f"{CAREERS_URL}#{details_id}" if details_id else CAREERS_URL

        yield {
            "company": COMPANY,
            "job_title": title,
            "location": location,
            "job_id": details_id or title,
            "posted_date": None,
            "job_url": job_url,
            "source": SOURCE,
            "careers_url": CAREERS_URL,
        }


def _filter_country(jobs: List[Dict[str, Any]], *, country: str) -> List[Dict[str, Any]]:
    c = (country or "").strip().lower()
    if not c:
        return jobs

    synonyms = {
        "nigeria": ["nigeria", "ng"],
        "singapore": ["singapore", "sg"],
        "united arab emirates": ["united arab emirates", "uae", "dubai"],
    }
    needles = synonyms.get(c, [c])

    out: List[Dict[str, Any]] = []
    for j in jobs:
        hay = " ".join([str(j.get("location") or ""), str(j.get("job_title") or "")]).lower()
        if any(n in hay for n in needles):
            out.append(j)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "HBA Future Energy careers scraper (WordPress/Elementor accordion). "
            "Defaults to Singapore openings via local filtering; use --validate to print a sample job if empty."
        )
    )
    parser.add_argument("--careers-url", default=CAREERS_URL, help="Careers page URL")
    parser.add_argument("--country", default="Singapore", help="Country to filter by locally (default: Singapore)")
    parser.add_argument("--max-jobs", type=int, default=50, help="Max jobs to print")
    parser.add_argument(
        "--validate",
        action="store_true",
        default=False,
        help="If filtered result is empty, print 1 job from any country (for validation)",
    )
    parser.add_argument("--debug", action="store_true", default=False)

    args = parser.parse_args()

    session = requests.Session()
    resp = _get_with_retries(session, args.careers_url, timeout=45)
    html = resp.text

    all_jobs = list(_iter_openings(html))
    filtered = _filter_country(all_jobs, country=args.country)

    to_print = filtered
    if not to_print and args.validate:
        to_print = all_jobs[:1]
        if to_print:
            print("No jobs for requested country; printing 1 job for validation.", file=sys.stderr)

    out_count = 0
    for job in to_print:
        if out_count >= args.max_jobs:
            break
        print(job)
        out_count += 1

    print(f"Found {out_count} jobs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
