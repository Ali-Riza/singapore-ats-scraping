import requests
import re
import math
import json

BASE = "https://jobs.tuvsud.com"
SEARCH_URL = (
    "https://jobs.tuvsud.com/search?"
    "facetFilters=%7B%22cust_brand%22%3A%5B%22T%C3%9CV+S%C3%9CD%22%5D%2C"
    "%22jobLocationCountry%22%3A%5B%22Singapore%22%5D%7D"
    "&pageNumber=0&locale=en_US&searchResultView=LIST"
)

API_URL = f"{BASE}/services/recruiting/v1/jobs"
PAGE_SIZE = 10  # kommt aus deinem Ergebnis (11 Jobs → 2 Seiten)

def get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": BASE,
        "Referer": SEARCH_URL,
    })

    # Search-Seite laden → Cookies + CSRF
    html = s.get(SEARCH_URL, timeout=30).text
    m = re.search(r'var\s+CSRFToken\s*=\s*"([^"]+)"', html)
    if not m:
        raise RuntimeError("CSRF Token nicht gefunden")

    s.headers["X-CSRF-Token"] = m.group(1)
    return s

def fetch_page(session, page_number: int):
    payload = {
        "locale": "en_US",
        "pageNumber": page_number,
        "sortBy": "",
        "keywords": "",
        "location": "",
        "facetFilters": {
            "cust_brand": ["TÜV SÜD"],
            "jobLocationCountry": ["Singapore"]
        },
        "brand": "",
        "skills": [],
        "categoryId": 0,
        "alertId": "",
        "rcmCandidateId": ""
    }

    r = session.post(API_URL, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    session = get_session()

    page = 0
    total_jobs = None
    max_pages = None

    while True:
        data = fetch_page(session, page)

        if total_jobs is None:
            total_jobs = int(data.get("totalJobs", 0))
            max_pages = math.ceil(total_jobs / PAGE_SIZE)
            print(f"\nTotal jobs: {total_jobs} → pages: {max_pages}\n")

        results = data.get("jobSearchResult", [])
        if not results:
            break

        print(f"=== PAGE {page + 1} ===")

        for idx, item in enumerate(results, 1):
            job_dict = item.get("response", {})
            print(f"\nJob {idx} (Page {page + 1}):")
            print(json.dumps(job_dict, indent=2, ensure_ascii=False))

        page += 1
        if page >= max_pages:
            break

if __name__ == "__main__":
    main()