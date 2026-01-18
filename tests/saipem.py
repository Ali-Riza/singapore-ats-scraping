#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Saipem / nCore basic test scraper.

This is a lightweight test script (in the same style as the other files
under `tests/`) that attempts to extract job listings from a Saipem job
board page. The real site injects jobs via JS in many cases; this test
tries several fallbacks:

- parse job cards inside `#positionsContainer` or `.grid`
- look for anchors/buttons with classes like `btnCandidati` or `apply`
- attempt to extract JSON embedded in <script> tags

Usage:
  python3 tests/saipem.py

Adjust `SEARCH_URL` to point to the live careers page or to a saved
HTML file (file://...) for offline testing.
"""

from __future__ import annotations

import json
import subprocess
from typing import Dict, Optional
from urllib.parse import urljoin, urlparse


import requests

API_URL = "https://jobs.saipem.com/positions_saipem.json"

def fetch_jobs_from_api(api_url: str) -> Dict[str, Dict]:
    resp = requests.get(api_url, headers={
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    })
    resp.raise_for_status()
    data = resp.json()
    jobs: Dict[str, Dict] = {}
    # The structure is data['data']['Positions']
    positions = data.get("data", {}).get("Positions", {})
    for company, joblist in positions.items():
        for item in joblist:
            title = item.get("title") or item.get("jobTitle") or item.get("name")
            jid = str(item.get("id") or item.get("jobId") or item.get("reqId") or (item.get("slug") or "")).strip()
            url_from = item.get("applyUrl") or item.get("url") or item.get("href")
            if not url_from:
                continue
            job_url = url_from if str(url_from).startswith("http") else url_from
            if not jid:
                # fallback: try to parse from url
                try:
                    parsed = urlparse(job_url)
                    jid = parsed.path.rstrip("/\n ").split("/")[-1]
                except Exception:
                    jid = job_url
            # Use composite key: company + job_id
            composite_key = f"{company}::{jid}"
            jobs[composite_key] = {
                "company": company,
                "job_title": title,
                "location": item.get("location") or item.get("countryText") or None,
                "job_id": jid,
                "posted_date": item.get("orderDate") or item.get("postedDate") or None,
                "job_url": job_url,
                "source": "saipem (api)",
                "careers_url": API_URL,
            }
    return jobs

if __name__ == "__main__":
    jobs = fetch_jobs_from_api(API_URL)
    print(json.dumps(list(jobs.values()), ensure_ascii=False, indent=2))
    print(f"\nTotal jobs extracted: {len(jobs)}")
