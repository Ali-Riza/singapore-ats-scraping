"""
PPG Singapore Job Scraper
ATS:    Phenom People
Tech:   requests + eingebettetes phApp.ddo JSON (kein Playwright nötig)
Filter: Singapore (client-side, alle Jobs laden und filtern)
Output: JSON print only

Install: pip install requests
"""

import re
import json
import requests
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

BASE        = "https://careers.ppg.com"
CAREERS_URL = f"{BASE}/us/en/search-results"
SOURCE_NAME = "ppg"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
})


def set_param(url: str, **params) -> str:
    u = urlparse(url)
    q = parse_qs(u.query, keep_blank_values=True)
    for k, v in params.items():
        q[k] = [str(v)]
    new_q = urlencode({k: vs[0] for k, vs in q.items()}, doseq=False)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))


def brace_match(text: str, start: int) -> str:
    depth, in_str, esc, quote = 0, False, False, ""
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc: esc = False; continue
            if ch == "\\": esc = True; continue
            if ch == quote: in_str = False; quote = ""
            continue
        if ch in ('"', "'"):
            in_str = True; quote = ch; continue
        if ch == "{": depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i+1]
    raise RuntimeError("Brace matching failed")


def extract_eager(html: str) -> dict:
    for anchor in ("phApp.ddo =", "phApp.ddo="):
        idx = html.find(anchor)
        if idx != -1:
            start = html.find("{", idx)
            ddo = json.loads(brace_match(html, start))
            eager = ddo.get("eagerLoadRefineSearch")
            if isinstance(eager, dict):
                return eager
    raise RuntimeError("eagerLoadRefineSearch nicht gefunden")


def fetch_page(offset: int) -> dict:
    url = set_param(CAREERS_URL, **{"from": offset})
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return extract_eager(r.text)


def parse_jobs(eager: dict, filter_country: str = "China") -> tuple[list[dict], int, int]:
    jobs_raw = (eager.get("data") or {}).get("jobs") or []
    hits     = int(eager.get("hits") or len(jobs_raw))
    total    = int(eager.get("totalHits") or 0)

    jobs = []
    for item in jobs_raw:
        country  = str(item.get("country") or "")
        location = str(item.get("location") or "")
        multi    = item.get("multi_location") or []

        is_match = (
            country == filter_country
            or filter_country in location
            or any(filter_country in str(x) for x in (multi if isinstance(multi, list) else []))
        )
        if not is_match:
            continue

        job_id      = str(item.get("jobId") or item.get("reqId") or "")
        title       = item.get("title") or ""
        apply_url   = item.get("applyUrl") or ""
        job_url     = apply_url.replace("/apply", "") if apply_url else ""
        raw_date    = item.get("postedDate") or ""
        posted_date = raw_date[:10] if raw_date else ""

        jobs.append({
            "job_title":   title,
            "location":    location,
            "job_id":      job_id,
            "posted_date": posted_date,
            "job_url":     job_url,
            "source":      SOURCE_NAME,
            "careers_url": CAREERS_URL,
        })

    return jobs, hits, total


def main():
    print("PPG  ->  Singapore", flush=True)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", flush=True)

    all_jobs = []
    offset   = 0
    total    = None
    page_num = 1

    while True:
        print(f"  Seite {page_num} (offset={offset}) ...", flush=True)
        try:
            eager = fetch_page(offset)
        except Exception as e:
            print(f"  Fehler: {e}")
            break

        jobs, hits, page_total = parse_jobs(eager, filter_country="Singapore")

        if total is None:
            total = page_total
            # Singapore count from aggregation
            for agg in (eager.get("data") or {}).get("aggregations") or []:
                if agg.get("field") == "country":
                    singapore_count = agg.get("value", {}).get("Singapore", 0)
                    print(f"  Singapore Jobs laut Aggregation: {singapore_count} / {total} gesamt", flush=True)
                    break

        print(f"    -> {len(jobs)} Singapore Jobs (hits={hits})", flush=True)
        all_jobs.extend(jobs)

        offset += hits if hits > 0 else 10
        page_num += 1

        if not hits or offset >= (total or 0):
            break

    print(f"\n{len(all_jobs)} Jobs gesamt\n")
    print(json.dumps(all_jobs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()