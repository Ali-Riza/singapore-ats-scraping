"""
Croda Singapore Job Scraper
ATS:    Sitecore / Avanti custom API
API:    POST https://www.croda.com/api/vacancieslist/search
Filter: Singapore (client-side)
Output: JSON print only

Install: pip install requests
"""

import requests
import json
from datetime import datetime

BASE        = "https://www.croda.com"
API_URL     = f"{BASE}/api/vacancieslist/search"
CAREERS_URL = f"{BASE}/en-gb/careers/current-vacancies?country=singapore"
SOURCE_NAME = "croda"
PAGE_SIZE   = 100  # Max holen um alle Jobs in einem Call zu bekommen


HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Origin": BASE,
    "Referer": CAREERS_URL,
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}


def build_body(page: int) -> list:
    return [
        {"currentPage": str(page)},
        {"pageSize": str(PAGE_SIZE)},
        {"sortBy": "dateposted"},
        {"lang": "en-gb"},
    ]


def parse_date(raw: str) -> str:
    """03 Apr 2026 -> 2026-04-03"""
    if not raw:
        return ""
    try:
        return datetime.strptime(raw.strip(), "%d %b %Y").strftime("%Y-%m-%d")
    except Exception:
        try:
            return datetime.fromisoformat(raw.split("T")[0]).strftime("%Y-%m-%d")
        except Exception:
            return raw.strip()


def parse_jobs(results: list) -> list[dict]:
    jobs = []
    for item in results:
        location = item.get("jobLocation", "")
        if "singapore" not in location.lower():
            continue

        title    = item.get("jobTitle", "")
        job_id   = str(item.get("jobId", ""))
        job_url  = item.get("jobLink", "")
        raw_date = item.get("jobDatePosted", "")

        jobs.append({
            "job_title":   title,
            "location":    location,
            "job_id":      job_id,
            "posted_date": parse_date(raw_date),
            "job_url":     job_url,
            "source":      SOURCE_NAME,
            "careers_url": CAREERS_URL,
        })
    return jobs


def main():
    print("Croda  ->  Singapore", flush=True)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", flush=True)

    all_jobs = []
    page     = 1

    while True:
        print(f"  Seite {page} ...", flush=True)
        try:
            r = requests.post(API_URL, json=build_body(page), headers=HEADERS, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  Fehler: {e}")
            break

        results = data.get("searchResults", [])
        print(f"    -> {len(results)} Jobs auf Seite {page} (alle Laender)", flush=True)

        if not results:
            break

        sg_jobs = parse_jobs(results)
        print(f"    -> {len(sg_jobs)} Singapore Jobs", flush=True)
        all_jobs.extend(sg_jobs)

        # Pagination
        pagination = data.get("pagination", {})
        total      = pagination.get("totalItems", 0) if pagination else 0
        fetched    = page * PAGE_SIZE
        if not total or fetched >= total or len(results) < PAGE_SIZE:
            break
        page += 1

    print(f"\n{len(all_jobs)} Jobs gesamt\n")
    print(json.dumps(all_jobs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()