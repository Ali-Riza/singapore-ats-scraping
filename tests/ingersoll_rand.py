from __future__ import annotations

import argparse
import re
import time
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (INGERSOLL RAND / SAP SuccessFactors (Jobs2Web / RMK))
# =========================

COMPANY = "Ingersoll Rand"
SOURCE = "successfactors"

BASE_URL = "https://careers.irco.com"
DEFAULT_CAREERS_URL = f"{BASE_URL}/go/Asia-Pacific/9515400/?q=&location=singapore"

DEFAULT_TIMEOUT_S = 30
DEFAULT_SLEEP_S = 0.0

JOB_ID_RE = re.compile(r"/(\d{6,})/?$")


# =========================
# HELPERS
# =========================

def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _make_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return s


def _fetch_html(session: requests.Session, url: str, timeout_s: int) -> str:
    r = session.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.text


def _extract_job_id_from_url(job_url: str) -> str:
    m = JOB_ID_RE.search((job_url or "").strip())
    return m.group(1) if m else ""


def _parse_listing_jobs(html: str, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    table = soup.select_one("table#searchresults")
    if not table:
        return []

    out: list[dict[str, Any]] = []
    for row in table.select("tbody tr.data-row"):
        a = row.select_one("a.jobTitle-link[href]")
        if not a:
            continue

        title = _clean_text(a.get_text(" ", strip=True))
        href = (a.get("href") or "").strip()
        if not href:
            continue

        job_url = urljoin(page_url, href)
        job_url, _frag = urldefrag(job_url)

        loc_el = row.select_one("td.colLocation span.jobLocation")
        location = _clean_text(loc_el.get_text(" ", strip=True)) if loc_el else ""

        dept_el = row.select_one("td.colDepartment span.jobDepartment")
        department = _clean_text(dept_el.get_text(" ", strip=True)) if dept_el else ""

        job_id = _extract_job_id_from_url(job_url) or _extract_job_id_from_url(href)

        out.append(
            {
                "job_title": title,
                "job_url": job_url,
                "job_id": job_id,
                "location": location,
                "department": department or None,
            }
        )

    # De-dupe by URL, preserve order
    seen: set[str] = set()
    uniq: list[dict[str, Any]] = []
    for it in out:
        u = it.get("job_url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(it)

    return uniq


def _discover_pagination_urls(html: str, page_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: set[str] = set()

    for container in soup.select(".paginationShell, .pagination-top, .pagination-bottom"):
        for a in container.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href)
            full, _frag = urldefrag(full)
            urls.add(full)

    return sorted(urls)


def _parse_posted_date(raw: str) -> Optional[str]:
    raw = (raw or "").strip()
    if not raw:
        return None

    for fmt in (
        "%a %b %d %H:%M:%S UTC %Y",
        "%a %b %d %H:%M:%S %Z %Y",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.date().isoformat()
        except Exception:
            continue

    return None


def _extract_posted_date_from_detail(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    meta = soup.find("meta", attrs={"itemprop": "datePosted"})
    if meta and meta.get("content"):
        return _parse_posted_date(str(meta.get("content")))

    el = soup.select_one('[data-careersite-propertyid="date"]')
    if el:
        txt = _clean_text(el.get_text(" ", strip=True))
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                dt = datetime.strptime(txt, fmt)
                return dt.date().isoformat()
            except Exception:
                continue

    return None


def _extract_description(detail_soup: BeautifulSoup) -> str:
    main = detail_soup.select_one("main")
    if not main:
        # fallback to content wrapper
        main = detail_soup.select_one("#content")
    if not main:
        return ""
    return _clean_text(main.get_text("\n", strip=True))


# =========================
# SCRAPER
# =========================

def scrape(
    *,
    careers_url: str = DEFAULT_CAREERS_URL,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    sleep_s: float = DEFAULT_SLEEP_S,
    max_jobs: Optional[int] = None,
    include_description: bool = False,
    debug: bool = True,
) -> list[dict]:
    with _make_session() as session:
        visited_pages: set[str] = set()
        to_visit: list[str] = [careers_url]

        listing_items: list[dict[str, Any]] = []

        while to_visit:
            page_url = to_visit.pop(0)
            page_url, _frag = urldefrag(page_url)
            if page_url in visited_pages:
                continue
            visited_pages.add(page_url)

            html = _fetch_html(session, page_url, timeout_s)
            items = _parse_listing_jobs(html, page_url)
            listing_items.extend(items)

            if debug:
                print(f"page={page_url} items={len(items)}")

            for next_url in _discover_pagination_urls(html, page_url):
                if next_url not in visited_pages:
                    to_visit.append(next_url)

            if sleep_s:
                time.sleep(sleep_s)

        # Optional: keep only singapore if the URL filter gets ignored
        listing_items = [
            it for it in listing_items if "singapore" in (it.get("location") or "").lower()
        ]

        if max_jobs is not None:
            listing_items = listing_items[:max_jobs]

        jobs: list[dict] = []
        for idx, it in enumerate(listing_items, start=1):
            job_url = it.get("job_url") or ""
            if not job_url:
                continue

            try:
                detail_html = _fetch_html(session, job_url, timeout_s)
                detail_soup = BeautifulSoup(detail_html, "html.parser")

                rec: dict[str, Any] = {
                    "company": COMPANY,
                    "job_title": it.get("job_title") or "",
                    "location": it.get("location") or "",
                    "job_url": job_url,
                    "job_id": it.get("job_id") or _extract_job_id_from_url(job_url) or None,
                    "posted_date": _extract_posted_date_from_detail(detail_html),
                    "department": it.get("department"),
                    "apply_url": job_url,
                    "source": SOURCE,
                    "careers_url": careers_url,
                }

                if include_description:
                    rec["description"] = _extract_description(detail_soup)

                jobs.append(rec)

            except Exception as e:
                if debug:
                    print(
                        f"detail_fetch_failed idx={idx} url={job_url} err={type(e).__name__}: {e}"
                    )

            if sleep_s:
                time.sleep(sleep_s)

        return jobs


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Ingersoll Rand (careers.irco.com) scraper — SAP SuccessFactors (Jobs2Web). Defaults to Singapore jobs."
    )
    ap.add_argument("--url", default=DEFAULT_CAREERS_URL, help="Listing/category URL")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="HTTP timeout seconds")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S, help="Sleep seconds between requests")
    ap.add_argument("--max-jobs", type=int, default=None, help="Stop after N jobs")
    ap.add_argument("--include-description", action="store_true", help="Include long description text")
    ap.add_argument("--no-debug", action="store_true", help="Disable debug prints")
    ap.add_argument(
        "--print-limit",
        type=int,
        default=10,
        help="How many job dicts to print at the end (default: 10). Use 0 to print none.",
    )
    ap.add_argument("--print-all", action="store_true", help="Print all job dicts")
    args = ap.parse_args()

    jobs = scrape(
        careers_url=args.url,
        timeout_s=args.timeout,
        sleep_s=args.sleep,
        max_jobs=args.max_jobs,
        include_description=args.include_description,
        debug=not args.no_debug,
    )

    print(f"Found {len(jobs)} jobs")
    if args.print_all:
        for j in jobs:
            print(j)
    elif args.print_limit and args.print_limit > 0:
        for j in jobs[: args.print_limit]:
            print(j)


if __name__ == "__main__":
    main()
