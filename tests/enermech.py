#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EnerMech Workable API scraper.
Scrapes all jobs from EnerMech's Workable API (paginated).
Usage:
  python3 tests/enermech.py
"""
import requests
import json

def fetch_enermech_jobs():
    url = "https://apply.workable.com/api/v3/accounts/enermech/jobs"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en",
        "content-type": "application/json",
        "origin": "https://apply.workable.com",
        "referer": "https://apply.workable.com/enermech/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    }
    # You can add cookies here if needed for access
    jobs = []
    token = None
    while True:
        data = {"query": "", "department": [], "location": [], "workplace": [], "worktype": []}
        if token:
            data["token"] = token
        resp = requests.post(url, headers=headers, json=data)
        resp.raise_for_status()
        result = resp.json()
        jobs.extend(result.get("results", []))
        token = result.get("nextPageToken") or result.get("nextPage")
        if not token:
            break
    return jobs

if __name__ == "__main__":
    jobs = fetch_enermech_jobs()
    # Filter for jobs from USA
    def is_singapore_job(job):
        raw_location = job.get('location')
        if isinstance(raw_location, str):
            location = raw_location.lower()
        elif raw_location is not None:
            location = str(raw_location).lower()
        else:
            location = ''
        title = (job.get('title') or '').lower()
        return (
            'singapore' in location or
            'sg' == location or
            'singapore' in title or
            'sg' == title
        )
    singapore_jobs = [job for job in jobs if is_singapore_job(job)]
    print(json.dumps(singapore_jobs, ensure_ascii=False, indent=2))
    print(f"\nTotal Singapore jobs extracted: {len(singapore_jobs)}")
