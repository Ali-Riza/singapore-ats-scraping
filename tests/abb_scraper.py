#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ABB Careers (Phenom Canvas) Scraper via embedded eagerLoadRefineSearch JSON in HTML.

Why this approach:
- Some tenants (incl. ABB) don't expose a stable /jobs/search endpoint publicly.
- The search-results page embeds the job JSON in a script block:
  phApp.eagerLoadRefineSearch = {...}
  or "eagerLoadRefineSearch":{...}

We therefore:
1) GET the search-results HTML with ?qcountry=Singapore&from=<offset>
2) Extract the JSON object for eagerLoadRefineSearch via brace-matching
3) Paginate until offset >= totalHits
"""

from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlencode

import requests


CAREERS_URL = "https://careers.abb/global/en/search-results"
DEFAULT_COUNTRY = "Singapore"


# ----------------------------
# HTTP
# ----------------------------

def http_get(url: str, *, timeout: int = 30) -> str:
    headers = {
        # A normal browser UA helps avoid some bot heuristics
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        "Connection": "keep-alive",
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def build_search_url(country: str, offset: int) -> str:
    # ABB uses qcountry and supports from as offset
    qs = urlencode({"qcountry": country, "from": str(offset)})
    return f"{CAREERS_URL}?{qs}"


# ----------------------------
# Robust JSON extraction (brace matching)
# ----------------------------

def _brace_match_extract(text: str, start_idx: int) -> str:
    """
    Given text and an index pointing to a '{', returns the full substring
    representing the JSON object via brace depth matching.
    """
    if start_idx < 0 or start_idx >= len(text) or text[start_idx] != "{":
        raise ValueError("start_idx must point to '{'")

    depth = 0
    in_str = False
    esc = False

    for i in range(start_idx, len(text)):
        ch = text[i]

        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue

        # not in string
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start_idx : i + 1]

    raise RuntimeError("Unbalanced braces; could not extract JSON object.")


def extract_eagerload_refine_search(html: str) -> Dict[str, Any]:
    """
    Extracts the eagerLoadRefineSearch object from the HTML.

    Handles both patterns:
    - ..."eagerLoadRefineSearch":{"status":200,...}
    - ...eagerLoadRefineSearch = {"status":200,...}
    """
    # Try JSON-key pattern first (most common in embedded DDO structures)
    key = '"eagerLoadRefineSearch"'
    idx = html.find(key)
    if idx != -1:
        # find first '{' after the key:
        brace = html.find("{", idx)
        if brace != -1:
            obj_str = _brace_match_extract(html, brace)
            return json.loads(obj_str)

    # Try assignment-style pattern
    key2 = "eagerLoadRefineSearch"
    idx2 = html.find(key2)
    if idx2 != -1:
        brace = html.find("{", idx2)
        if brace != -1:
            obj_str = _brace_match_extract(html, brace)
            return json.loads(obj_str)

    raise RuntimeError("Konnte eagerLoadRefineSearch im HTML nicht finden/auslesen.")


# ----------------------------
# Data normalization
# ----------------------------

def normalize_job(job: Dict[str, Any], *, careers_url: str) -> Dict[str, Any]:
    return {
        "company": "ABB",
        "job_title": job.get("title") or job.get("jobTitle") or "",
        "location": job.get("location") or job.get("cityStateCountry") or job.get("cityCountry") or "",
        "job_id": job.get("jobId") or job.get("reqId") or "",
        "posted_date": job.get("postedDate") or "",
        "job_url": job.get("applyUrl") or "",
        "source": "phenom",
        "careers_url": careers_url,
    }


def job_has_singapore_location(job: Dict[str, Any]) -> bool:
    """
    Filter to keep only jobs truly available in Singapore.
    ABB has many multi_location jobs; the search facet may be Singapore,
    but the primary "location" field can be Shanghai/US/etc.
    """
    target = "Singapore, Central Singapore, Singapore"
    loc = job.get("location") or ""
    city_country = job.get("cityCountry") or ""
    multi = job.get("multi_location") or []

    if target in loc:
        return True
    if "Singapore" in city_country:
        return True
    if isinstance(multi, list) and any(target == x for x in multi):
        return True

    # Sometimes multi_location_array contains dicts
    mla = job.get("multi_location_array") or []
    if isinstance(mla, list):
        for item in mla:
            if isinstance(item, dict) and item.get("location") == target:
                return True

    return False


# ----------------------------
# Scrape loop
# ----------------------------

@dataclass
class PageResult:
    jobs: List[Dict[str, Any]]
    hits: int
    total: int


def fetch_page(country: str, offset: int, *, sleep_s: float = 0.0) -> PageResult:
    url = build_search_url(country, offset)
    html = http_get(url)
    ddo = extract_eagerload_refine_search(html)

    jobs = (ddo.get("data") or {}).get("jobs") or []
    hits = int(ddo.get("hits") or len(jobs) or 0)
    total = int(ddo.get("totalHits") or 0)

    if sleep_s > 0:
        time.sleep(sleep_s)

    return PageResult(jobs=jobs, hits=hits, total=total)


def scrape_all(country: str = DEFAULT_COUNTRY, *, only_true_singapore: bool = False) -> List[Dict[str, Any]]:
    all_jobs: List[Dict[str, Any]] = []
    offset = 0

    while True:
        page = fetch_page(country, offset, sleep_s=0.0)

        if not page.jobs:
            break

        all_jobs.extend(page.jobs)

        # If hits is 0 for some reason, fallback to len(jobs) to prevent infinite loop
        step = page.hits if page.hits > 0 else len(page.jobs)
        offset += step

        if page.total and offset >= page.total:
            break

    # Optional strict filter
    if only_true_singapore:
        all_jobs = [j for j in all_jobs if job_has_singapore_location(j)]

    return all_jobs


def main() -> int:
    country = DEFAULT_COUNTRY

    # Set True if you ONLY want jobs that truly list Singapore in locations
    ONLY_TRUE_SINGAPORE = False

    jobs_raw = scrape_all(country=country, only_true_singapore=ONLY_TRUE_SINGAPORE)

    careers_url = f"{CAREERS_URL}?qcountry={country}"
    jobs_norm = [normalize_job(j, careers_url=careers_url) for j in jobs_raw]

    out = {"jobs": jobs_norm}
    print(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n[INFO] Jobs returned: {len(jobs_norm)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())