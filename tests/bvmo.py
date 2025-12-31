# tests/bvmo.py
from __future__ import annotations

import csv
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse

import requests


# =========================
# CONFIG
# =========================

BV_SEARCH_URL = "https://jobs.bureauveritas.com/gb/en/search-results?m=3&location=Singapore%2C%20Other%2FNot%20Applicable%2C%20Singapore"
PAGE_SIZE_GUESS = 20  # wird dynamisch über "hits" geprüft
TIMEOUT = 30

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "out")
OUT_JSON = os.path.join(OUT_DIR, "bv_jobs.json")
OUT_CSV = os.path.join(OUT_DIR, "bv_jobs.csv")


# =========================
# HTTP
# =========================

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9,de-DE;q=0.8,de;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return s


def fetch_html(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=TIMEOUT, allow_redirects=True)
    ct = (r.headers.get("Content-Type") or "").lower()

    # Debug falls wir nicht HTML bekommen oder Bot/403 etc.
    if r.status_code >= 400:
        snippet = (r.text or "")[:500].replace("\n", " ")
        raise RuntimeError(
            f"HTTP {r.status_code} for {url}\n"
            f"Content-Type: {ct}\n"
            f"Body snippet: {snippet}"
        )

    # Manche Seiten liefern HTML aber mit komischem CT, das ist ok – wir parsen trotzdem
    return r.text


# =========================
# JSON EXTRACTION (robust brace matching)
# =========================

def _brace_match_object(text: str, start_idx: int) -> str:
    """
    Given text and index of '{', returns substring containing the full JSON object
    using brace matching while respecting strings and escapes.
    """
    if start_idx < 0 or start_idx >= len(text) or text[start_idx] != "{":
        raise ValueError("start_idx must point to '{'")

    depth = 0
    in_str = False
    esc = False

    for i in range(start_idx, len(text)):
        ch = text[i]

        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue

        # not in string
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start_idx : i + 1]

    raise RuntimeError("Brace matching failed (unterminated object).")


def extract_eager_load_refine_search(html: str) -> Dict[str, Any]:
    """
    Extract the JSON object for "eagerLoadRefineSearch": {...}
    from the HTML (Phenom embeds this inside phApp.ddo).
    """
    key = '"eagerLoadRefineSearch"'
    idx = html.find(key)
    if idx == -1:
        # hilfreiches Debug
        raise RuntimeError(
            "Could not find eagerLoadRefineSearch in HTML. "
            "Maybe the page layout changed or you received a bot-check page."
        )

    # Find the ':' after the key, then the first '{'
    colon = html.find(":", idx + len(key))
    if colon == -1:
        raise RuntimeError("Malformed eagerLoadRefineSearch (missing ':').")

    obj_start = html.find("{", colon)
    if obj_start == -1:
        raise RuntimeError("Malformed eagerLoadRefineSearch (missing '{').")

    raw_obj = _brace_match_object(html, obj_start)

    try:
        return json.loads(raw_obj)
    except json.JSONDecodeError as e:
        # show small context to debug
        context = raw_obj[:400].replace("\n", " ")
        raise RuntimeError(f"JSON decode failed: {e}\nObject head: {context}")


# =========================
# PAGINATION
# =========================

def set_query_param(url: str, **params: Any) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            q.pop(k, None)
        else:
            q[k] = str(v)
    new_q = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))


def scrape_bv_search_results(base_url: str) -> Dict[str, Any]:
    session = make_session()

    all_jobs: List[Dict[str, Any]] = []
    seen = set()

    offset = 0
    total_hits: Optional[int] = None
    page_size = PAGE_SIZE_GUESS

    while True:
        url = set_query_param(base_url, **{"from": offset})
        html = fetch_html(session, url)

        blob = extract_eager_load_refine_search(html)

        # Struktur: {"status":200,"hits":X,"totalHits":Y,"data":{"jobs":[...] ...}}
        hits = int(blob.get("hits") or 0)
        total = int(blob.get("totalHits") or 0)
        data = blob.get("data") or {}
        jobs = data.get("jobs") or []

        if total_hits is None:
            total_hits = total

        # Wenn page_size_guess falsch war: update
        if hits > 0:
            page_size = hits

        # Debug info
        print(f"[INFO] from={offset} -> hits={hits}, totalHits={total} (page_size={page_size})")

        if not jobs:
            # keine Ergebnisse mehr (oder blocked/changed) -> Ende
            break

        new_count = 0
        for j in jobs:
            key = j.get("jobSeqNo") or j.get("jobId") or j.get("reqId") or json.dumps(j, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            all_jobs.append(j)
            new_count += 1

        print(f"[INFO] collected +{new_count}, total_collected={len(all_jobs)}")

        # Pagination Ende?
        if total_hits is not None and (offset + page_size) >= total_hits:
            break

        offset += page_size

        # kleine Pause (freundlicher / weniger Bot-Trigger)
        time.sleep(0.7 + random.random() * 0.6)

    return {
        "source": "Bureau Veritas (Phenom search-results HTML embedded JSON)",
        "base_url": base_url,
        "total_collected": len(all_jobs),
        "jobs": all_jobs,
    }


# =========================
# OUTPUT
# =========================

def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def flatten_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep it simple & stable for CSV:
    pick common useful fields that usually exist.
    """
    return {
        "jobSeqNo": job.get("jobSeqNo"),
        "jobId": job.get("jobId"),
        "reqId": job.get("reqId"),
        "title": job.get("title"),
        "category": job.get("category"),
        "location": job.get("location") or job.get("cityStateCountry") or job.get("cityState"),
        "country": job.get("country"),
        "city": job.get("city"),
        "state": job.get("state"),
        "workingPattern": job.get("workingPattern"),
        "educationLevel": job.get("educationLevel"),
        "postedDate": job.get("postedDate"),
        "externalApply": job.get("externalApply"),
        "visibilityType": job.get("visibilityType"),
        "descriptionTeaser": job.get("descriptionTeaser"),
    }


def write_csv(path: str, jobs: List[Dict[str, Any]]) -> None:
    rows = [flatten_job(j) for j in jobs]
    # union of keys (stable order: based on first row, then extras)
    fieldnames: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


# =========================
# MAIN
# =========================

def main() -> None:
    try:
        result = scrape_bv_search_results(BV_SEARCH_URL)
    except Exception as e:
        print("\n[ERROR]", str(e), file=sys.stderr)
        sys.exit(1)

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()