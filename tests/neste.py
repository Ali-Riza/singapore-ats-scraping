"""
Neste Singapore Job Scraper
ATS:    SAP SuccessFactors
Site:   https://jobs.neste.com
Filter: Singapore (location=SG)
Output: JSON print only

Install: pip install requests beautifulsoup4
"""

import requests
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup

BASE        = "https://jobs.neste.com"
SEARCH_URL  = f"{BASE}/search"
CAREERS_URL = f"{BASE}/search?q=&location=SG"
SOURCE_NAME = "neste"
PAGE_SIZE   = 25

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE,
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}


def fetch(startrow: int = 0) -> str | None:
    params = {"q": "", "location": "SG", "startrow": startrow}
    try:
        r = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  Fehler (startrow={startrow}): {e}")
        return None


def parse(html: str) -> tuple[list[dict], int]:
    soup  = BeautifulSoup(html, "html.parser")
    jobs  = []

    # Gesamtanzahl
    label = soup.select_one("span.paginationLabel")
    total = 0
    if label:
        import re
        m = re.search(r"of\s*(\d+)", label.get_text().replace("\xa0", ""))
        total = int(m.group(1)) if m else 0

    for row in soup.select("tr.data-row"):
        title_el = row.select_one("span.jobTitle.hidden-phone a.jobTitle-link")
        if not title_el:
            continue

        title   = title_el.get_text(strip=True)
        href    = title_el.get("href", "")
        job_url = f"{BASE}{href}" if href.startswith("/") else href
        job_id  = href.rstrip("/").split("/")[-1]

        loc_el   = row.select_one("td.colLocation span.jobLocation")
        location = " ".join(loc_el.get_text().split()) if loc_el else ""

        date_el     = row.select_one("td.colDate span.jobDate")
        posted_raw  = date_el.get_text(strip=True) if date_el else ""
        posted_date = ""
        if posted_raw:
            try:
                posted_date = datetime.strptime(posted_raw, "%b %d, %Y").strftime("%Y-%m-%d")
            except ValueError:
                posted_date = posted_raw

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
    print("Neste  ->  Singapore", flush=True)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", flush=True)

    all_jobs = []
    startrow = 0
    total    = None

    while True:
        print(f"  startrow={startrow} ...", flush=True)
        html = fetch(startrow)
        if not html:
            break

        jobs, page_total = parse(html)
        if total is None:
            total = page_total
            print(f"  Gesamt Singapore Jobs: {total}", flush=True)

        print(f"    -> {len(jobs)} Jobs", flush=True)
        if not jobs:
            break

        all_jobs.extend(jobs)
        startrow += PAGE_SIZE

        if startrow >= (total or 0):
            break
        time.sleep(0.8)

    print(f"\n{len(all_jobs)} Jobs gesamt\n")
    print(json.dumps(all_jobs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()