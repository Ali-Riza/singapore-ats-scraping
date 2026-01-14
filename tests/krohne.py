#!/usr/bin/env python3

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from datetime import date
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.krohne.com"
CAREERS_URL = f"{BASE_URL}/en/company/career"
COMPANY = "KROHNE"
SOURCE = "krohne-careers-nextjs"
CONTENTSET_API_URL = f"{BASE_URL}/api/contentset"


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: float = 30.0,
    max_attempts: int = 3,
    backoff_s: float = 0.8,
) -> requests.Response:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(
                url,
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; ATS-Scraper/1.0)",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_s * attempt)

    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def _fetch_html(session: requests.Session, url: str) -> str:
    """Fetch HTML.

    krohne.com is sometimes protected by Cloudflare bot checks that may return 403
    for Python TLS fingerprints. We try requests first; if blocked, fall back to
    system curl (usually allowed).
    """

    # Try requests first (a few times for transient issues). If we hit 403, fall back to curl.
    last_exc: Optional[BaseException] = None
    for attempt in range(1, 4):
        try:
            resp = session.get(
                url,
                timeout=30.0,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; ATS-Scraper/1.0)",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            if resp.status_code == 403:
                break
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except Exception as exc:
            last_exc = exc
            if attempt < 3:
                time.sleep(0.8 * attempt)

    # Fallback: curl
    try:
        proc = subprocess.run(
            [
                "curl",
                "-sSL",
                "--compressed",
                "-H",
                "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "-H",
                "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                url,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except FileNotFoundError:
        raise RuntimeError(
            f"Blocked by Cloudflare (403) and 'curl' is not available to fall back to: {url}"
        ) from last_exc

    if proc.returncode != 0:
        raise RuntimeError(
            f"Blocked by Cloudflare and curl failed (code {proc.returncode}): {url}\n{proc.stderr.decode('utf-8', errors='replace')}"
        ) from last_exc

    return proc.stdout.decode("utf-8", errors="replace")


def _clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = re.sub(r"\s+", " ", s).strip()
    return s2 or None


def _stable_job_id(*parts: Optional[str]) -> str:
    normalized = "|".join([(p or "").strip().lower() for p in parts])
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:16]


def _try_extract_job_id_from_url(job_url: str) -> Optional[str]:
    # Example: https://recruiting.krohne.com/de/Vacancies/1304/Description/1
    m = re.search(r"/Vacancies/(\d+)(?:/|$)", job_url)
    if m:
        return m.group(1)

    # Example: https://www.werkenbijkrohne.nl/vacaturebeschrijving/hr-adviseur
    m = re.search(r"/vacaturebeschrijving/([^/?#]+)", job_url)
    if m:
        return m.group(1)

    # PDFs: use basename
    try:
        path = urlparse(job_url).path
        if path.lower().endswith(".pdf"):
            base = path.rsplit("/", 1)[-1]
            return base or None
    except Exception:
        pass

    return None


def _try_parse_date_posted(iso_dt: Optional[str]) -> Optional[str]:
    if not iso_dt:
        return None
    # Typical: 2026-01-06T00:00:00.000Z
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", iso_dt)
    if not m:
        return None
    try:
        y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return str(date(y, mth, d))
    except Exception:
        return None


def _extract_next_data(html: str) -> Optional[dict]:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _find_job_contentset_block(next_data: dict) -> Optional[dict]:
    """Locate the content-set node that powers the career job list.

    On the career page, Next.js embeds a structure like:
      { contentset, filters, attributes, items, pagination }
    where contentset.entity_type == "job".
    """

    def walk(node: Any) -> Optional[dict]:
        if isinstance(node, dict):
            if {"contentset", "filters", "attributes", "pagination"}.issubset(node.keys()):
                cs = node.get("contentset")
                if isinstance(cs, dict) and cs.get("entity_type") == "job":
                    return node

            for v in node.values():
                found = walk(v)
                if found is not None:
                    return found

        elif isinstance(node, list):
            for v in node:
                found = walk(v)
                if found is not None:
                    return found

        return None

    return walk(next_data)


def _post_contentset(session: requests.Session, *, body_obj: dict, referer: str) -> dict:
    """Call the internal contentset endpoint.

    The site requires `X-Body-Hash`, which is sha256 of the exact request body string.
    Important: use `ensure_ascii=False` so unicode is not escaped, matching the site's
    Node/JSON parsing + (re)stringifying behavior.
    """

    body = json.dumps(body_obj, ensure_ascii=False, separators=(",", ":"))
    body_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Body-Hash": body_hash,
        "User-Agent": "Mozilla/5.0 (compatible; ATS-Scraper/1.0)",
        "Referer": referer,
    }

    resp = session.post(
        CONTENTSET_API_URL,
        headers=headers,
        data=body.encode("utf-8"),
        timeout=60.0,
    )

    # If the TLS fingerprint is blocked, retry with curl.
    if resp.status_code in (403, 429):
        proc = subprocess.run(
            [
                "curl",
                "-sSL",
                "--compressed",
                "-X",
                "POST",
                CONTENTSET_API_URL,
                "-H",
                "Accept: application/json",
                "-H",
                "Content-Type: application/json",
                "-H",
                f"X-Body-Hash: {body_hash}",
                "-H",
                "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "-H",
                f"Referer: {referer}",
                "--data-binary",
                body,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"POST {CONTENTSET_API_URL} failed (curl rc={proc.returncode})\n{proc.stderr.decode('utf-8', errors='replace')}"
            )
        try:
            return json.loads(proc.stdout.decode("utf-8", errors="replace"))
        except Exception as exc:
            raise RuntimeError("Failed to parse /api/contentset response as JSON") from exc

    resp.raise_for_status()
    return resp.json()


def _country_labels(country_field: Any) -> List[str]:
    labels: List[str] = []
    if isinstance(country_field, list):
        for c in country_field:
            if isinstance(c, dict):
                lbl = _clean_text(c.get("label"))
                if lbl:
                    labels.append(lbl)
    elif isinstance(country_field, dict):
        lbl = _clean_text(country_field.get("label"))
        if lbl:
            labels.append(lbl)
    return labels


def _jobs_from_entities(entities: List[dict], *, careers_url: str) -> List[dict]:
    out: List[dict] = []
    for e in entities:
        if not isinstance(e, dict):
            continue

        job_id = _clean_text(e.get("id"))
        job_title = _clean_text(e.get("title"))

        job_url = _clean_text(e.get("joblink_pdf"))
        if job_url and job_url.startswith("/"):
            job_url = urljoin(BASE_URL, job_url)

        city = _clean_text(e.get("location_city"))
        countries = _country_labels(e.get("country"))
        location_parts = [p for p in [city, ", ".join(countries) if countries else None] if p]
        location = ", ".join(location_parts) if location_parts else None

        posted_date = _try_parse_date_posted(_clean_text(e.get("publishing_start_date")))

        if not job_id:
            job_id = _try_extract_job_id_from_url(job_url) if job_url else None
        if not job_id:
            job_id = _stable_job_id(COMPANY, job_title, location, job_url)

        if not job_title and not job_url:
            continue

        out.append(
            {
                "company": COMPANY,
                "job_title": job_title,
                "location": location,
                "job_id": job_id,
                "posted_date": posted_date,
                "job_url": job_url,
                "source": SOURCE,
                "careers_url": careers_url,
            }
        )

    return out


def _is_singapore_job(job: dict) -> bool:
    needle = "singapore"
    loc = (job.get("location") or "").lower()
    url = (job.get("job_url") or "").lower()

    if needle in loc:
        return True
    if needle in url:
        return True

    # Heuristic fallbacks for file paths / identifiers.
    if "/sg/" in url or "-sg" in url or "_sg" in url:
        return True

    return False


def _parse_jobs_from_html(html: str, *, page_url: str) -> Tuple[List[dict], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")

    jobs: List[dict] = []

    cards = soup.find_all(attrs={"itemtype": re.compile(r"JobPosting$", re.I)})
    for card in cards:
        if not getattr(card, "get", None):
            continue

        # Title
        title_el = card.find(attrs={"itemprop": "title"})
        job_title = _clean_text(title_el.get_text(" ")) if title_el else None

        # URL
        link_el = card.find("a", attrs={"itemprop": "url"})
        href = link_el.get("href") if link_el else None
        job_url = urljoin(page_url, href) if href else None

        # Location
        loc_el = card.find(attrs={"itemprop": "jobLocation"})
        loc_p = loc_el.find("p") if loc_el else None
        location = _clean_text(loc_p.get_text(" ")) if loc_p else None

        # Posted date
        meta_date = card.find("meta", attrs={"itemprop": "datePosted"})
        posted_date = _try_parse_date_posted(meta_date.get("content") if meta_date else None)

        if not job_title and not job_url:
            continue

        job_id = _try_extract_job_id_from_url(job_url) if job_url else None
        if not job_id:
            # Fall back to stable hash
            job_id = _stable_job_id(COMPANY, job_title, location, job_url)

        jobs.append(
            {
                "company": COMPANY,
                "job_title": job_title,
                "location": location,
                "job_id": job_id,
                "posted_date": posted_date,
                "job_url": job_url,
                "source": SOURCE,
                "careers_url": page_url,
            }
        )

    # Pagination: prefer <link rel="next" href="..."> (BeautifulSoup usually parses rel as a list)
    next_url: Optional[str] = None
    for link in soup.find_all("link"):
        rel = link.get("rel")
        rel_values: List[str] = []
        if isinstance(rel, list):
            rel_values = [str(r).lower() for r in rel]
        elif isinstance(rel, str):
            rel_values = [rel.lower()]
        if "next" in rel_values:
            href = link.get("href")
            if href:
                next_url = urljoin(page_url, href)
            break

    return jobs, next_url


def _iter_all_jobs(
    session: requests.Session,
    start_url: str,
    *,
    max_pages: int,
    max_jobs: int,
) -> List[dict]:
    # Preferred path: use Next.js embedded data + internal /api/contentset,
    # which returns the full set (47) even though the server-rendered HTML
    # only contains the first 20.
    html = _fetch_html(session, start_url)
    next_data = _extract_next_data(html)
    if next_data:
        block = _find_job_contentset_block(next_data)
        if block:
            body_obj: Dict[str, Any] = {
                "contentset": block.get("contentset"),
                "filters": block.get("filters"),
                "attributes": block.get("attributes"),
                "pagination": block.get("pagination"),
            }
            try:
                api_json = _post_contentset(session, body_obj=body_obj, referer=start_url)
                entities = api_json.get("entities")
                if isinstance(entities, list) and entities:
                    jobs = _jobs_from_entities(entities, careers_url=start_url)
                    if max_jobs:
                        return jobs[:max_jobs]
                    return jobs
            except Exception:
                # Fall back to HTML-only scraping.
                pass

    # Fallback: HTML-only scraping (usually limited to the first 20 items).
    out: List[dict] = []
    seen_page_urls: Set[str] = set()
    seen_job_keys: Set[Tuple[Optional[str], Optional[str]]] = set()

    url: Optional[str] = start_url
    page_no = 0

    while url:
        if url in seen_page_urls:
            break
        seen_page_urls.add(url)

        page_no += 1
        if max_pages and page_no > max_pages:
            break

        html = _fetch_html(session, url)
        jobs, next_url = _parse_jobs_from_html(html, page_url=url)

        for j in jobs:
            key = (j.get("job_id"), j.get("job_url"))
            if key in seen_job_keys:
                continue
            seen_job_keys.add(key)

            out.append(j)
            if max_jobs and len(out) >= max_jobs:
                return out

        url = next_url

    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "KROHNE Careers scraper. Uses the page's embedded Next.js data + internal /api/contentset to fetch all jobs. "
            "Prints canonical job dicts."
        )
    )
    parser.add_argument("--url", default=CAREERS_URL, help="Start URL (default: KROHNE career page)")
    parser.add_argument("--max-pages", type=int, default=50, help="Safety limit for pagination (default: 50)")
    parser.add_argument("--max-jobs", type=int, default=500, help="Max jobs to print (default: 500)")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Disable Singapore-only filter and print all jobs",
    )

    args = parser.parse_args()

    session = requests.Session()

    jobs = _iter_all_jobs(session, args.url, max_pages=args.max_pages, max_jobs=args.max_jobs)

    # Default: only Singapore jobs (use --all to disable)
    if not args.all:
        jobs = [j for j in jobs if _is_singapore_job(j)]

    for j in jobs:
        print(j)

    print(f"Found {len(jobs)} jobs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
