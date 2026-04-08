"""
Syngenta India Job Scraper
Site:   https://jobs.syngenta.com
API:    POST /api/jobs?page=1&country=IN
Filter: India (location_code == "IN")
Output: JSON print only

Install: pip install requests
"""

import requests
import json
import time
from datetime import datetime

BASE        = "https://jobs.syngenta.com"
API_URL     = f"{BASE}/api/jobs"
CAREERS_URL = f"{BASE}/?country=SG"
SOURCE_NAME = "syngenta"

HEADERS = {
    "Accept": "application/json",
    "Referer": BASE + "/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}


def fetch_page(page: int) -> dict:
    params = {"page": page, "country": "SG"}
    r = requests.post(API_URL, params=params, json={}, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()


def parse_date(raw: str) -> str:
    """DD.MM.YYYY -> YYYY-MM-DD"""
    try:
        return datetime.strptime(raw.strip(), "%d.%m.%Y").strftime("%Y-%m-%d")
    except Exception:
        return raw.strip()


def parse_jobs(data: dict) -> tuple[list[dict], int]:
    jobs_raw = data.get("jobs", [])
    total    = int(data.get("total", 0))

    jobs = []
    for item in jobs_raw:
        if item.get("location_code", "") != "SG":
            continue

        job_id      = str(item.get("id", ""))
        title       = item.get("title", "")
        city        = item.get("city", "")
        location    = f"{city}, Singapore" if city else "Singapore"
        raw_date    = item.get("published", "")
        posted_date = parse_date(raw_date) if raw_date else ""
        job_url     = item.get("url", "") or f"{BASE}/job/-:{job_id}"

        jobs.append({
            "job_title":   title,
            "location":    location,
            "job_id":      job_id,
            "posted_date": posted_date,
            "job_url":     job_url,
            "source":      SOURCE_NAME,
            "careers_url": CAREERS_URL,
        })

    return jobs, total


def main():
    print("Syngenta  ->  Singapore", flush=True)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", flush=True)

    all_jobs = []
    page     = 1
    total    = None

    while True:
        print(f"  Seite {page} ...", flush=True)
        try:
            data = fetch_page(page)
        except Exception as e:
            print(f"  Fehler: {e}")
            break

        jobs, page_total = parse_jobs(data)

        if total is None:
            total = page_total
            print(f"  Gesamt Jobs (alle Länder): {total}", flush=True)

        print(f"    -> {len(jobs)} India Jobs", flush=True)

        if not jobs:
            break

        all_jobs.extend(jobs)

        if len(all_jobs) >= (total or 0):
            break
        page += 1
        time.sleep(0.5)

    print(f"\n{len(all_jobs)} Jobs gesamt\n")
    print(json.dumps(all_jobs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()