import re
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# =========================
# Konfiguration
# =========================
CAREERS_URL = (
    "https://careers.macgregor.com/search/"
    "?createNewAlert=false&q=&locationsearch=singapore"
    "&optionsFacetsDD_facility=&optionsFacetsDD_country="
)
BASE_URL = "https://careers.macgregor.com"

COMPANY = "MacGregor"
SOURCE = "successfactors"


# =========================
# HTTP Session
# =========================
session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
})


# =========================
# Helper
# =========================
def extract_job_id(href: str) -> Optional[str]:
    """
    /job/Singapore-Account-Representative/1161773355/
    -> 1161773355
    """
    m = re.search(r"/job/.+/(\d+)/", href)
    return m.group(1) if m else None


def extract_posted_date_from_detail(html: str) -> Optional[str]:
    """
    Liest:
    <meta itemprop="datePosted" content="Wed Dec 17 02:00:00 UTC 2025">
    -> 2025-12-17
    """
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", attrs={"itemprop": "datePosted"})
    if not meta or not meta.get("content"):
        return None

    raw = meta["content"].strip()
    try:
        dt = datetime.strptime(raw, "%a %b %d %H:%M:%S UTC %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


# =========================
# Core Scraping
# =========================
def scrape_macgregor_jobs() -> List[Dict]:
    resp = session.get(CAREERS_URL, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.select_one("table#searchresults")
    if not table:
        return []

    jobs: List[Dict] = []

    for row in table.select("tbody tr.data-row"):
        link = row.select_one("a.jobTitle-link[href]")
        if not link:
            continue

        job_title = link.get_text(strip=True)
        href = link["href"]
        job_url = urljoin(BASE_URL, href)
        job_id = extract_job_id(href)

        loc_el = row.select_one("td.colLocation span.jobLocation")
        location = loc_el.get_text(" ", strip=True) if loc_el else None

        # Detailseite laden (für posted_date)
        posted_date = None
        try:
            detail_resp = session.get(job_url, timeout=30)
            detail_resp.raise_for_status()
            posted_date = extract_posted_date_from_detail(detail_resp.text)
        except requests.RequestException:
            posted_date = None

        jobs.append({
            "company": COMPANY,
            "job_title": job_title,
            "location": location,
            "job_id": job_id,
            "posted_date": posted_date,          # YYYY-MM-DD
            "job_url": job_url,
            "source": SOURCE,
            "careers_url": CAREERS_URL,
        })

    return jobs


# =========================
# Run
# =========================
if __name__ == "__main__":
    results = scrape_macgregor_jobs()
    for r in results:
        print(r)