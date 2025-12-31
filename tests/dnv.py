import json
import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup


# =========================
# Konfiguration
# =========================

CAREERS_URL = "https://jobs.dnv.com/job-search?country%5B0%5D=Singapore"
SOURCE = "dnv_algolia"

ALGOLIA_APP_ID = "RVMOB42DFH"
ALGOLIA_API_KEY = "fd9e8d499b1d7ede4cd848b00aef0c65"
ALGOLIA_HOST = f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes/*/queries"
INDEX_NAME = "production__dnvvcare2301__sort-rank"

BASE_SITE = "https://jobs.dnv.com"

# Performance-Tuning
HITS_PER_PAGE = 50
MAX_PAGES = 200

DETAIL_WORKERS = 12          # typischer sweet spot: 8–16
DETAIL_TIMEOUT = 20
DETAIL_JITTER_SEC = (0.00, 0.10)  # klein halten; 0 deaktiviert "Höflichkeitsbremse"

SESSION_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# =========================
# Algolia Fetch
# =========================

def build_params(country: str, page: int, hits_per_page: int) -> str:
    params = {
        "facetFilters": f'[[\"country:{country}\"]]',
        "facets": '["business_unit","contract_type","country","position_type"]',
        "getRankingInfo": "true",
        "highlightPostTag": "__/ais-highlight__",
        "highlightPreTag": "__ais-highlight__",
        "hitsPerPage": str(hits_per_page),
        "maxValuesPerFacet": "999",
        "page": str(page),
        "query": "",
    }
    return urlencode(params)


def fetch_page(session: requests.Session, country: str, page: int, hits_per_page: int = 50) -> dict:
    headers = {
        "Accept": "*/*",
        "Origin": "https://jobs.dnv.com",
        "Referer": "https://jobs.dnv.com/",
        "User-Agent": "Mozilla/5.0",
        "x-algolia-application-id": ALGOLIA_APP_ID,
        "x-algolia-api-key": ALGOLIA_API_KEY,
    }

    payload = {
        "requests": [
            {
                "indexName": INDEX_NAME,
                "params": build_params(country, page, hits_per_page),
            }
        ]
    }

    r = session.post(ALGOLIA_HOST, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


# =========================
# Posted-Date Extraktion
# =========================

def normalize_date(raw: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip()

    # häufig: ISO oder ISO mit Zeit
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%b %d, %Y", "%B %d, %Y"):
        try:
            # defensive: nur die ersten ~19 chars (YYYY-MM-DDTHH:MM:SS)
            return datetime.strptime(raw[:19], fmt).date().isoformat()
        except Exception:
            continue

    # notfalls roh zurückgeben
    return raw


def extract_posted_date_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    # 1) JSON-LD (bevorzugt)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except Exception:
            continue

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "datePosted" in item:
                    return normalize_date(item["datePosted"])
        elif isinstance(data, dict):
            if "datePosted" in data:
                return normalize_date(data["datePosted"])

    # 2) HTML-Text Fallback
    text = soup.get_text(separator=" ", strip=True)
    patterns = [
        r"Posted[:\s]+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"Date posted[:\s]+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"Posting date[:\s]+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return normalize_date(m.group(1))

    return None


def fetch_posted_date(session: requests.Session, job_url: str) -> Optional[str]:
    # mini-jitter, damit parallele Requests nicht wie ein "Burst" wirken
    if DETAIL_JITTER_SEC and (DETAIL_JITTER_SEC[0] > 0 or DETAIL_JITTER_SEC[1] > 0):
        import time
        time.sleep(random.uniform(*DETAIL_JITTER_SEC))

    try:
        r = session.get(job_url, timeout=DETAIL_TIMEOUT)
        r.raise_for_status()
        return extract_posted_date_from_html(r.text)
    except Exception:
        return None


# =========================
# Mapping
# =========================

def build_job_url(hit: dict) -> Optional[str]:
    jd_url = hit.get("jd_url")
    if isinstance(jd_url, str) and jd_url.startswith("/"):
        return BASE_SITE + jd_url
    if isinstance(jd_url, str) and jd_url.startswith("http"):
        return jd_url
    return None


def build_location(hit: dict) -> Optional[str]:
    loc = hit.get("display_location")
    if loc:
        return loc
    tc = hit.get("town_city")
    if isinstance(tc, list):
        return ", ".join(map(str, tc))
    if isinstance(tc, str):
        return tc
    return None


def hit_to_base_record(hit: dict) -> dict:
    job_url = build_job_url(hit)
    return {
        "company": "DNV",
        "job_title": hit.get("title"),
        "location": build_location(hit),
        "job_id": str(hit.get("objectID")) if hit.get("objectID") is not None else None,
        "posted_date": None,  # wird parallel gefüllt
        "job_url": job_url,
        "source": SOURCE,
        "careers_url": CAREERS_URL,
    }


# =========================
# Scraper: schnell + parallel
# =========================

def scrape_dnv_jobs_fast(
    country: str = "Singapore",
    hits_per_page: int = HITS_PER_PAGE,
    max_pages: int = MAX_PAGES,
    detail_workers: int = DETAIL_WORKERS,
) -> list[dict]:

    session = requests.Session()
    session.headers.update(SESSION_HEADERS)

    # 1) Alle Hits (Algolia) sammeln
    base_records: list[dict] = []
    page = 0
    while page < max_pages:
        data = fetch_page(session, country=country, page=page, hits_per_page=hits_per_page)
        results0 = data["results"][0]
        hits = results0.get("hits", [])
        if not hits:
            break

        for hit in hits:
            base_records.append(hit_to_base_record(hit))

        page += 1

    # 2) Dedupe früh (spart Detail-Requests)
    seen = set()
    deduped: list[dict] = []
    for r in base_records:
        key = (r["company"], r["job_id"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    # 3) posted_date parallel holen (nur wenn job_url existiert)
    url_to_date: dict[str, Optional[str]] = {}
    urls = [r["job_url"] for r in deduped if r.get("job_url")]

    # nochmals unique, falls gleiche URL mehrfach vorkommt
    urls_unique = list(dict.fromkeys(urls))

    if urls_unique:
        with ThreadPoolExecutor(max_workers=detail_workers) as ex:
            futures = {ex.submit(fetch_posted_date, session, url): url for url in urls_unique}
            for fut in as_completed(futures):
                url = futures[fut]
                try:
                    url_to_date[url] = fut.result()
                except Exception:
                    url_to_date[url] = None

    # 4) Dates einfüllen
    for r in deduped:
        url = r.get("job_url")
        if url:
            r["posted_date"] = url_to_date.get(url)

    return deduped


if __name__ == "__main__":
    records = scrape_dnv_jobs_fast(country="Singapore")
    print("Total:", len(records))
    print("Sample:", records[:2])