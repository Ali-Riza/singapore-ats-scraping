"""
Mitsui Chemicals Group Singapore Job Scraper
Source: sg.jobstreet.com (company page)
Tech:   requests + JSON aus eingebettetem window.SEEK_APOLLO_DATA
Output: JSON print only

Install: pip install requests
"""

import re
import json
import requests
from datetime import datetime
from playwright.sync_api import sync_playwright

COMPANY_URL = "https://sg.jobstreet.com/Yeakin-Plastic-Industry-Pte-Ltd-jobs/at-this-company"
CAREERS_URL = COMPANY_URL
SOURCE_NAME = "yeakin-plastic-industry"
BASE        = "https://sg.jobstreet.com"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-SG,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}


def fetch_all_pages() -> list[str]:
    """Holt alle Seiten via Playwright und gibt HTML-Liste zurück."""
    pages_html = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
            locale="en-SG",
        )
        page = context.new_page()
        page_num = 1

        while True:
            url = COMPANY_URL if page_num == 1 else f"{COMPANY_URL}?page={page_num}"
            print(f"  Seite {page_num}: {url}", flush=True)
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(1500)
            html = page.content()
            pages_html.append(html)

            # Prüfen ob weitere Seiten existieren
            _, total = parse(html)
            fetched = page_num * 32
            if fetched >= total or total == 0:
                break
            page_num += 1

        browser.close()
    return pages_html


def parse(html: str) -> tuple[list[dict], int]:
    # Jobs sind vollständig in window.SEEK_APOLLO_DATA eingebettet
    match = re.search(r'window\.SEEK_APOLLO_DATA\s*=\s*(\{.*?\});\s*\n', html, re.DOTALL)
    if not match:
        return [], 0

    data = json.loads(match.group(1))

    # Gesamtanzahl aus ROOT_QUERY holen
    total = 0
    for key, val in data.items():
        if "jobSearchV6" in key and isinstance(val, dict):
            total = val.get("totalCount", 0)
            break

    jobs = []
    for key, val in data.items():
        if not isinstance(val, dict) or val.get("__typename") != "JobSearchV6Data":
            continue

        job_id    = val.get("id", "")
        title     = val.get("title", "")
        locations = val.get("locations", [])
        location  = locations[0].get("label", "") if locations else ""
        listing   = val.get("listingDate", {})
        posted_date = listing.get("dateTimeUtc", "")[:10] if listing else ""
        job_url   = f"{BASE}/job/{job_id}"

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
    print("Yeakin Plastic Industry  ->  Singapore (JobStreet)", flush=True)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", flush=True)

    all_jobs = []
    page     = 1
    page_size = 32  # JobStreet default

    pages = fetch_all_pages()
    for html in pages:
        jobs, total = parse(html)
        print(f"    -> {len(jobs)} Jobs  (gesamt: {total})", flush=True)
        all_jobs.extend(jobs)

    print(f"\n{len(all_jobs)} Jobs gesamt\n")
    print(json.dumps(all_jobs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()