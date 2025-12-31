import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, parse_qs, urlencode, urlunsplit

import requests


SEARCH_URL = "https://careers.kbr.com/us/en/search-results?keywords=&p=ChIJdZOLiiMR2jERxPWrUs9peIg&location=Singapore"


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def extract_balanced_object(text: str, key: str) -> Optional[str]:
    """
    Finds `"key": { ... }` in a big text blob and returns the balanced JSON object string `{ ... }`.
    Works even with nested braces.
    """
    key_pos = text.find(f'"{key}"')
    if key_pos == -1:
        return None

    brace_start = text.find("{", key_pos)
    if brace_start == -1:
        return None

    depth = 0
    in_str = False
    escape = False

    for i in range(brace_start, len(text)):
        ch = text[i]

        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            continue
        else:
            if ch == '"':
                in_str = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[brace_start : i + 1]

    return None


def set_query_param(url: str, key: str, value: str) -> str:
    parts = urlsplit(url)
    qs = parse_qs(parts.query, keep_blank_values=True)
    qs[key] = [value]
    new_query = urlencode(qs, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def fetch_eager_block(search_url: str, headers: Dict[str, str]) -> Tuple[Dict[str, Any], str]:
    resp = requests.get(search_url, headers=headers, timeout=30)
    resp.raise_for_status()
    html = resp.text

    raw_obj = extract_balanced_object(html, "eagerLoadRefineSearch")
    if not raw_obj:
        raise RuntimeError("Could not find eagerLoadRefineSearch JSON block in HTML.")

    eager = json.loads(raw_obj)
    return eager, html


def fetch_kbr_jobs_all(search_url: str) -> Dict[str, Any]:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; JobScraper/1.0; +https://example.com/bot)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    base_job_url = "https://careers.kbr.com/us/en/job"

    # --- First page: discover totalHits + page size ---
    eager0, _html0 = fetch_eager_block(search_url, headers=headers)
    total_hits = eager0.get("totalHits")
    jobs0 = eager0.get("data", {}).get("jobs", []) or []

    if total_hits is None:
        # Fallback: at least return what we see on page 1
        total_hits = len(jobs0)

    # Page size: usually 10, but we infer from the first response
    page_size = len(jobs0) if len(jobs0) > 0 else 10

    # --- Loop pages using "from" ---
    all_jobs: List[Dict[str, Any]] = []
    seen_keys = set()

    def add_jobs(jobs: List[Dict[str, Any]], careers_url: str) -> None:
        for j in jobs:
            title = j.get("title") or ""
            job_seq = j.get("jobSeqNo")
            job_id = j.get("jobId") or j.get("reqId") or job_seq

            # dedupe key
            key = job_seq or job_id or (title, j.get("location"))
            if key in seen_keys:
                continue
            seen_keys.add(key)

            job_url = None
            if job_seq and title:
                job_url = f"{base_job_url}/{job_seq}/{slugify(title)}"
            elif job_seq:
                job_url = f"{base_job_url}/{job_seq}"

            all_jobs.append(
                {
                    "company": "KBR",
                    "job_title": title,
                    "location": j.get("location") or j.get("cityStateCountry") or j.get("city"),
                    "job_id": job_id,
                    "posted_date": j.get("postedDate"),
                    "job_url": job_url,
                    "source": "phenompeople",
                    "careers_url": careers_url,
                }
            )

    # Add page 1
    add_jobs(jobs0, search_url)

    # If we already got everything (e.g., totalHits <= page_size), stop
    # Otherwise: page through using from=page_size, 2*page_size, ...
    next_from = page_size
    safety_pages = 0

    while len(all_jobs) < int(total_hits) and safety_pages < 50:
        safety_pages += 1

        paged_url = set_query_param(search_url, "from", str(next_from))
        eager, _html = fetch_eager_block(paged_url, headers=headers)

        jobs = eager.get("data", {}).get("jobs", []) or []
        if not jobs:
            # No more results → stop
            break

        add_jobs(jobs, paged_url)
        next_from += page_size

        # Optional: if the last page returned fewer than page_size, we can stop early
        if len(jobs) < page_size:
            break

    return {
        "meta": {
            "search_url": search_url,
            "hits_returned": len(all_jobs),
            "total_hits": int(total_hits),
            "page_size_inferred": page_size,
        },
        "records": all_jobs,
    }


if __name__ == "__main__":
    out = fetch_kbr_jobs_all(SEARCH_URL)
    print(json.dumps(out, ensure_ascii=False, indent=2))