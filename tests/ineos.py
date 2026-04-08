"""
INEOS Job Scraper
Site:   https://careers.ineos.com
Filter: Alle Jobs (kein Country-Filter, da Server-Filter nicht greift)
Tech:   requests + BeautifulSoup
Output: JSON print only

Install: pip install requests beautifulsoup4
"""

import re
import requests
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup

BASE        = "https://careers.ineos.com"
SEARCH_URL  = BASE   # kein /jobs — Paginierung via ?pageNo=N&pageSize=N
CAREERS_URL = f"{BASE}?countryid=9"
SOURCE_NAME = "ineos"
PAGE_SIZE   = 50

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}


def fetch(page: int) -> str | None:
    # Server ignoriert countryid -- alle Jobs holen, client-seitig filtern
    params = {
        "pageNo":   page,
        "pageSize": PAGE_SIZE,
    }
    try:
        r = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  Fehler Seite {page}: {e}")
        return None


def parse(html: str) -> tuple[list[dict], int]:
    soup  = BeautifulSoup(html, "html.parser")
    jobs  = []

    total_el = soup.select_one("p.u-text-medium")
    total    = 0
    if total_el:
        m = re.search(r"(\d+)", total_el.get_text())
        total = int(m.group(1)) if m else 0

    for card in soup.select("div.c-card--job"):
        title_el = card.select_one("a.c-card__title-link")
        if not title_el:
            continue

        meta_items = card.select("li.c-meta__item")
        location   = meta_items[0].get_text(strip=True) if meta_items else ""

        # Client-seitiger Singapore-Filter
        if "singapore" not in location.lower():
            continue

        title = title_el.get_text(strip=True)
        href        = title_el.get("href", "")
        job_url     = f"{BASE}{href}" if href.startswith("/") else href
        job_id      = href.split("_")[-1].split("/")[0] if "_" in href else ""
        date_el     = card.select_one("time[datetime]")
        posted_date = date_el["datetime"] if date_el else ""

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
    print("INEOS  ->  Singapore", flush=True)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", flush=True)

    all_jobs = []
    page     = 1
    total    = None

    while True:
        print(f"  Seite {page} ...", flush=True)
        html = fetch(page)
        if not html:
            break

        jobs, page_total = parse(html)
        if total is None:
            total = page_total
            print(f"  Gesamt Jobs: {total}", flush=True)

        print(f"    -> {len(jobs)} Jobs", flush=True)
        if not jobs:
            break

        all_jobs.extend(jobs)

        if page * PAGE_SIZE >= (total or 0):
            break
        page += 1
        time.sleep(0.5)

    print(f"\n{len(all_jobs)} Jobs gesamt\n")
    print(json.dumps(all_jobs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()