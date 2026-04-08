"""
Chevron Singapore Job Scraper
ATS:    Radancy (frontend) + Workday (backend)
Site:   https://careers.chevron.com
Filter: Singapore
Output: JSON print only

posted_date wird direkt von der Workday-API geholt.

Install: pip install requests beautifulsoup4
"""

import re
import time
import json
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# ── Konfiguration ──────────────────────────────────────────────────────────────

BASE        = "https://careers.chevron.com"
SEARCH_URL  = f"{BASE}/search-jobs/Singapore/35016/2/1880251/1x36667/103x8/50/2"
CAREERS_URL = SEARCH_URL
SOURCE_NAME = "chevron"

# Workday-Instanz für Chevron
WD_BASE     = "https://chevron.wd5.myworkdayjobs.com"
WD_API      = f"{WD_BASE}/wday/cxs/chevron/jobs/jobs"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}

WD_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Workday-Client": "wd5",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Referer": "https://chevron.wd5.myworkdayjobs.com/",
    "Origin": "https://chevron.wd5.myworkdayjobs.com",
}

# ── Workday posted_date ────────────────────────────────────────────────────────

def get_posted_date(req_id: str) -> str:
    """
    Scrapt posted_date von der Workday Job-Suchseite.
    Workday gibt bei Suche nach der Req-ID die postedOn Info zurück.
    """
    from datetime import timedelta
    try:
        # Workday CXS Search API
        payload = {"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": req_id}
        r = requests.post(WD_API, json=payload, headers=WD_HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()

        postings = data.get("jobPostings", [])
        if not postings:
            return ""

        posting    = postings[0]
        posted_on  = posting.get("postedOn", "")
        start_date = posting.get("startDate", "")

        if start_date:
            try:
                dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        if posted_on:
            # "Posted 7 Days Ago" / "Vor 7 Tagen"
            match = re.search(r"(\d+)\s+day", posted_on, re.IGNORECASE)
            if match:
                days_ago = int(match.group(1))
                dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                dt -= timedelta(days=days_ago)
                return dt.strftime("%Y-%m-%d")
            if "today" in posted_on.lower():
                return datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return posted_on

    except Exception:
        # Fallback: Workday Detailseite direkt scrapen
        try:
            wd_search = f"{WD_BASE}/wday/cxs/chevron/jobs/jobs"
            payload2  = {"appliedFacets": {"Job_Requisition_ID": [req_id]}, "limit": 1, "offset": 0, "searchText": ""}
            r2 = requests.post(wd_search, json=payload2, headers=WD_HEADERS, timeout=15)
            if r2.ok:
                data2    = r2.json()
                postings = data2.get("jobPostings", [])
                if postings:
                    return postings[0].get("postedOn", "")
        except Exception:
            pass
        return ""


# ── Radancy Parsing ────────────────────────────────────────────────────────────

def parse(html: str) -> tuple[list[dict], int, int]:
    soup         = BeautifulSoup(html, "html.parser")
    section      = soup.select_one("section#search-results")
    total_pages  = int(section.get("data-total-pages",  1)) if section else 1
    current_page = int(section.get("data-current-page", 1)) if section else 1

    jobs = []
    for li in soup.select("section#search-results-list li"):
        a = li.select_one("a[data-job-id]")
        if not a:
            continue

        title_el = li.select_one("h2")
        loc_el   = li.select_one("span.job-location")

        title    = title_el.get_text(strip=True) if title_el else ""
        location = loc_el.get_text(strip=True)   if loc_el   else ""
        job_id   = a.get("data-job-id", "")
        href     = a.get("href", "")
        job_url  = f"{BASE}{href}" if href.startswith("/") else href

        # Requisition-ID aus URL extrahieren (letztes Segment)
        # z.B. /job/singapore/process-engineer/38138/93368524464
        # Die Req-ID (R000070183) steckt in der Workday-URL, nicht hier direkt.
        # Wir suchen sie im HTML der Detailseite oder nutzen job_id als Fallback.
        req_id = job_id  # wird unten ggf. überschrieben

        jobs.append({
            "job_title":   title,
            "location":    location,
            "job_id":      job_id,
            "req_id":      req_id,   # temporär, wird nach Workday-Lookup entfernt
            "posted_date": "",
            "job_url":     job_url,
            "source":      SOURCE_NAME,
            "careers_url": CAREERS_URL,
        })

    return jobs, current_page, total_pages


def get_req_id_from_detail(job_url: str) -> str:
    """
    Holt die Workday Requisition-ID von der Chevron-Detailseite.
    Die Seite enthält z.B. "R000070183" als Text oder in einem data-Attribut.
    """
    try:
        r = requests.get(job_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        # Suche nach Muster R\d{9}
        match = re.search(r"\bR\d{6,10}\b", r.text)
        return match.group(0) if match else ""
    except Exception:
        return ""


def build_page_url(page: int) -> str:
    parts = SEARCH_URL.rstrip("/").rsplit("/", 1)
    return f"{parts[0]}/{page}"


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("Chevron  ->  Singapore", flush=True)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", flush=True)

    all_jobs = []
    page     = 1

    # 1. Alle Jobs von Radancy holen
    while True:
        url = build_page_url(page)
        print(f"  Seite {page}: {url}", flush=True)
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"  Fehler: {e}")
            break

        jobs, current, total = parse(r.text)
        print(f"    -> {len(jobs)} Jobs  (Seite {current}/{total})", flush=True)
        all_jobs.extend(jobs)

        if page >= total:
            break
        page += 1

    # 2. Für jeden Job: Req-ID von Detailseite holen, dann posted_date von Workday
    print(f"\n  Hole posted_date von Workday ({len(all_jobs)} Jobs) ...", flush=True)
    for job in all_jobs:
        # Req-ID von der Detailseite holen
        req_id = get_req_id_from_detail(job["job_url"])
        if req_id:
            print(f"    {job['job_title'][:40]:<40} req_id={req_id}", flush=True)
            posted = get_posted_date(req_id)
            job["posted_date"] = posted
            print(f"      posted_date: {posted}", flush=True)
        else:
            print(f"    {job['job_title'][:40]:<40} req_id nicht gefunden", flush=True)

        # Temporäres req_id-Feld entfernen
        job.pop("req_id", None)
        time.sleep(0.5)

    print(f"\n{len(all_jobs)} Jobs gesamt\n")
    print(json.dumps(all_jobs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()