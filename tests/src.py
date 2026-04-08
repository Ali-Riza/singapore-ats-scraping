"""
Singapore Refining Company (SRC) Job Scraper
ATS:    RecruiterPal
API:    GET https://src.recruiterpal.com/api/v1/tms/career/jobs
Filter: Singapore (alle Jobs sind Singapore)
Output: JSON print only

Install: pip install requests
"""

import re
import time
import requests
import json
from datetime import datetime

BASE        = "https://src.recruiterpal.com"
API_URL     = f"{BASE}/api/v1/tms/career/jobs"
CAREERS_URL = f"{BASE}/career/jobs"
SOURCE_NAME = "singapore-refining-company"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CAREERS_URL,
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}


def fetch_posted_date(board_id: str) -> str:
    """Holt posted_date von der Job-Detailseite via JSON-LD."""
    try:
        url = f"{CAREERS_URL}/{board_id}"
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        match = re.search(r'"datePosted"\s*:\s*"([^"]+)"', r.text)
        if match:
            raw = match.group(1)[:10]  # nur YYYY-MM-DD
            return raw
    except Exception:
        pass
    return ""


def fetch() -> list[dict]:
    r = requests.get(API_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise Exception(f"API error: {data}")
    return data.get("rows", [])


def parse(rows: list[dict]) -> list[dict]:
    jobs = []
    for item in rows:
        title    = item.get("job_title", "")
        job_id   = item.get("id", "") or item.get("job_agg_id", "")
        location = (item.get("location") or {}).get("country", "Singapore")

        # board_identifier für Job-URL
        board_id = item.get("board_identifier", "")
        job_url  = f"{CAREERS_URL}/{board_id}" if board_id else CAREERS_URL

        jobs.append({
            "job_title":   title,
            "location":    location,
            "job_id":      job_id,
            "posted_date": "",        # wird unten befüllt
            "job_url":     job_url,
            "board_id":    board_id,  # temporär
            "source":      SOURCE_NAME,
            "careers_url": CAREERS_URL,
        })

    return jobs


def main():
    print("Singapore Refining Company  ->  Singapore", flush=True)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", flush=True)

    try:
        rows = fetch()
    except Exception as e:
        print(f"  Fehler: {e}")
        return

    jobs = parse(rows)
    print(f"  -> {len(jobs)} Jobs", flush=True)
    print("  Hole posted_date von Detailseiten ...", flush=True)
    for job in jobs:
        board_id = job.pop("board_id", "")
        if board_id:
            posted = fetch_posted_date(board_id)
            job["posted_date"] = posted
            print(f"    {job['job_title'][:40]:<40} {posted}", flush=True)
            time.sleep(0.3)
    print()
    print(f"{len(jobs)} Jobs gesamt\n")
    print(json.dumps(jobs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()