import json
import re
from urllib.parse import urlencode

import requests


BASE_URL = "https://careers.gevernova.com/jobs"
COUNTRY = "Singapore"


def _extract_preload_state(html: str) -> dict:
    m = re.search(
        r"window\.__PRELOAD_STATE__\s*=\s*({.*?})\s*;\s*window\.__BUILD__",
        html,
        flags=re.DOTALL,
    )
    if not m:
        m = re.search(
            r"window\.__PRELOAD_STATE__\s*=\s*({.*?})\s*;",
            html,
            flags=re.DOTALL,
        )
    if not m:
        raise RuntimeError("Could not find window.__PRELOAD_STATE__ in HTML.")
    return json.loads(m.group(1))


def _careers_url(page_number: int) -> str:
    # Keep the same filter param style you used.
    params = {
        "filter[country][0]": COUNTRY,
        "page_number": page_number,
    }
    return f"{BASE_URL}?{urlencode(params)}"


def _map_job(j: dict, careers_url: str) -> dict:
    job_id = j.get("requisitionID") or j.get("reference") or ""

    posted_date = ""
    for cf in j.get("customFields", []) or []:
        if cf.get("cfKey") == "cf_posting_start_date" and cf.get("value"):
            posted_date = cf["value"]
            break
    if not posted_date:
        posted_date = (j.get("createDate") or j.get("updatedDate") or "")[:10]

    location = extract_location(j)

    # This is a relative path like: "lead-project-manager/job/R5028671"
    original = (j.get("originalURL") or "").lstrip("/")
    job_url = f"https://careers.gevernova.com/{original}" if original else careers_url

    company = j.get("companyName") or "GE Vernova"

    return {
        "company": company,
        "job_title": j.get("title", ""),
        "location": location,
        "job_id": job_id,
        "posted_date": posted_date,
        "job_url": job_url,
        "source": "careers.gevernova.com",
        "careers_url": careers_url,
    }


def scrape_gevernova_country_all_pages(max_pages: int = 50) -> list[dict]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    all_rows: list[dict] = []
    seen_ids: set[str] = set()

    for page in range(1, max_pages + 1):
        url = _careers_url(page)

        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()

        state = _extract_preload_state(r.text)
        jobs = (state.get("jobSearch") or {}).get("jobs") or []

        # Stop if a page returns nothing.
        if not jobs:
            break

        new_on_this_page = 0
        for j in jobs:
            if not is_singapore_job(j):
                continue
            row = _map_job(j, careers_url=url)
            jid = row["job_id"] or row["job_url"]

            if jid in seen_ids:
                continue

            seen_ids.add(jid)
            all_rows.append(row)
            new_on_this_page += 1

        # If we got a full page but zero new jobs, we’re likely looping / last page.
        if new_on_this_page == 0:
            break

    return all_rows

def is_singapore_job(j: dict) -> bool:
    # 1) Check locations[]
    for loc in (j.get("locations") or []):
        if (loc.get("country") or "").strip().lower() == "singapore":
            return True

    # 2) Fallback: custom field
    for cf in (j.get("customFields") or []):
        if cf.get("cfKey") == "cf_primary_location_country":
            if (cf.get("value") or "").strip().lower() == "singapore":
                return True

    return False
def extract_location(j: dict) -> str:
    locs = j.get("locations") or []
    texts = []
    for loc in locs:
        t = (
            loc.get("locationName")
            or loc.get("locationParsedText")
            or loc.get("locationText")
            or loc.get("cityState")
            or ""
        ).strip()
        if t:
            texts.append(t)
    # dedupe, keep order
    seen = set()
    uniq = []
    for t in texts:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return " | ".join(uniq)

if __name__ == "__main__":
    rows = scrape_gevernova_country_all_pages(max_pages=20)
    print({"count": len(rows), "records": rows})  # full output as dict