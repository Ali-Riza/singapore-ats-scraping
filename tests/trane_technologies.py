import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import requests


CAREERS_URL = "https://careers.tranetechnologies.com/global/en/search-results"
BASE_URL = "https://careers.tranetechnologies.com/global/en/"


def slugify_title(title: str) -> str:
    s = (title or "").strip().lower()
    # keep ascii letters/digits, turn everything else into hyphens
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "job"


def extract_js_object_by_anchor(html: str, anchor: str) -> dict:
    """
    Finds e.g. "phApp.ddo = { ... };" and extracts the {...} using brace matching.
    Handles strings and escapes inside the JS object.
    """
    idx = html.find(anchor)
    if idx == -1:
        raise RuntimeError(f"Anchor not found: {anchor}")

    # find first '{' after the anchor
    start = html.find("{", idx)
    if start == -1:
        raise RuntimeError("Could not find opening '{' after anchor.")

    depth = 0
    in_str = False
    esc = False
    quote_char = ""

    for i in range(start, len(html)):
        ch = html[i]

        if in_str:
            if esc:
                esc = False
                continue
            if ch == "\\":
                esc = True
                continue
            if ch == quote_char:
                in_str = False
                quote_char = ""
            continue

        # not in string
        if ch == '"' or ch == "'":
            in_str = True
            quote_char = ch
            continue

        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                obj_text = html[start : i + 1]
                try:
                    return json.loads(obj_text)
                except json.JSONDecodeError as e:
                    # Helpful debug snippet
                    snippet = obj_text[:2000]
                    raise RuntimeError(
                        f"JSON decode failed near: {e.msg}\n"
                        f"First 2000 chars of extracted object:\n{snippet}"
                    ) from e

    raise RuntimeError("Did not find matching closing '}' for the JS object.")


def make_session() -> requests.Session:
    """Creates a session with connection pooling for faster requests."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    })
    return session


def fetch_page(session: requests.Session, from_offset: int) -> str:
    """
    Loads the search results HTML. Pagination is controlled by ?from=<offset>.
    """
    params = {"from": str(from_offset)}
    r = session.get(CAREERS_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.text


def extract_jobs_from_html(html: str) -> tuple[list[dict], int, int]:
    """
    Returns (jobs, hits, totalHits) from eagerLoadRefineSearch.
    """
    ddo = extract_js_object_by_anchor(html, "phApp.ddo =")
    refine = ddo.get("eagerLoadRefineSearch") or {}
    hits = int(refine.get("hits") or 0)
    total = int(refine.get("totalHits") or 0)
    jobs = ((refine.get("data") or {}).get("jobs") or [])
    return jobs, hits, total


def build_row(job: dict) -> dict:
    company_val = job.get("jobCompany") or "Trane Technologies"
    title = job.get("title") or ""
    location = job.get("location") or (job.get("multi_location") or [None])[0]
    job_id = job.get("jobId") or job.get("reqId") or ""
    posted_date = job.get("postedDate") or ""

    job_seq = job.get("jobSeqNo") or ""
    job_url = ""
    if job_seq:
        job_path = f"job/{job_seq}/{slugify_title(title)}"
        job_url = urljoin(BASE_URL, job_path)

    return {
        "company": company_val,
        "job_title": title,
        "location": location or "",
        "job_id": str(job_id),
        "posted_date": posted_date,
        "job_url": job_url,
        "source": "phenom_eagerLoadRefineSearch",
        "careers_url": CAREERS_URL,
    }


def is_singapore(job: dict) -> bool:
    # Primary: exact country field
    if job.get("country") == "Singapore":
        return True

    # Fallbacks:
    loc = (job.get("location") or "")
    if "Singapore" in loc:
        return True

    multi_locs = job.get("multi_location") or []
    if any("Singapore" in (x or "") for x in multi_locs):
        return True

    return False


def main() -> None:
    session = make_session()
    results: list[dict] = []
    seen_ids: set[str] = set()

    # First request to get total hits and page size
    html = fetch_page(session, 0)
    jobs, hits, total_hits = extract_jobs_from_html(html)

    # Process first page
    for job in jobs:
        if not is_singapore(job):
            continue
        jid = str(job.get("jobId") or job.get("jobSeqNo") or job.get("reqId") or "")
        if jid and jid not in seen_ids:
            seen_ids.add(jid)
            results.append(build_row(job))

    # Calculate remaining pages
    if hits <= 0:
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return

    offsets = list(range(hits, total_hits, hits))

    # Fetch all remaining pages in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_offset = {
            executor.submit(fetch_page, session, offset): offset
            for offset in offsets
        }

        for future in as_completed(future_to_offset):
            try:
                html = future.result()
                jobs, _, _ = extract_jobs_from_html(html)

                for job in jobs:
                    if not is_singapore(job):
                        continue
                    jid = str(job.get("jobId") or job.get("jobSeqNo") or job.get("reqId") or "")
                    if jid and jid not in seen_ids:
                        seen_ids.add(jid)
                        results.append(build_row(job))
            except Exception as e:
                offset = future_to_offset[future]
                print(f"Warning: Failed to fetch offset {offset}: {e}", file=__import__("sys").stderr)

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()