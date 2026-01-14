from __future__ import annotations

import argparse
import re
import time
from typing import Any, Optional
from urllib.parse import urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (ALFA LAVAL / Clinch career site; hints of Greenhouse)
# =========================

COMPANY = "Alfa Laval"
SOURCE = "clinch_careers_site"

BASE_URL = "https://career.alfalaval.com"
SEARCH_PATH = "/jobs/search"

DEFAULT_TIMEOUT_S = 30
DEFAULT_SLEEP_S = 0.0

DEFAULT_COUNTRY_CODE = "SG"
DEFAULT_QUERY = "singapore"
DEFAULT_MAX_PAGES = 1


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


def _build_search_url(*, query: str, country_code: str, page: int) -> str:
    # Example:
    # https://career.alfalaval.com/jobs/search?page=1&query=singapore&country_codes%5B%5D=SG
    params = [
        ("page", str(page)),
        ("query", query),
        ("country_codes[]", country_code),
    ]
    return f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"


def _normalize_url(href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""

    abs_url = urljoin(BASE_URL + "/", href)
    parsed = urlparse(abs_url)
    if parsed.scheme not in ("http", "https"):
        return ""
    if parsed.netloc and parsed.netloc not in ("career.alfalaval.com",):
        return ""

    return parsed._replace(fragment="").geturl()


def _extract_search_rows(search_soup: BeautifulSoup) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for tr in search_soup.select("table.table tbody tr"):
        a = tr.select_one("td.job-search-results-title a[href]")
        if not a:
            continue

        job_url = _normalize_url(a.get("href") or "")
        title = _clean_text(a.get_text(" ", strip=True))

        dept = [
            _clean_text(li.get_text(" ", strip=True))
            for li in tr.select("td.job-search-results-department li")
            if _clean_text(li.get_text(" ", strip=True))
        ]
        emp = [
            _clean_text(li.get_text(" ", strip=True))
            for li in tr.select("td.job-search-results-employment-type li")
            if _clean_text(li.get_text(" ", strip=True))
        ]
        locs = [
            _clean_text(li.get_text(" ", strip=True))
            for li in tr.select("td.job-search-results-location li")
            if _clean_text(li.get_text(" ", strip=True))
        ]

        if not job_url:
            continue

        out.append(
            {
                "job_url": job_url,
                "job_title": title,
                "departments": dept,
                "employment_types": emp,
                "locations": locs,
            }
        )

    # de-dupe by URL, preserve order
    seen: set[str] = set()
    uniq: list[dict[str, Any]] = []
    for it in out:
        u = it.get("job_url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(it)

    return uniq


def _job_uid_from_detail(detail_html: str) -> str:
    # Clinch uses an internal job uid in turbo-frame URLs and /me/jobs/<uid>/favourites
    m = re.search(r"job_uid=([0-9a-f]{32})", detail_html, flags=re.I)
    if m:
        return m.group(1)

    m = re.search(r"/me/jobs/([0-9a-f]{32})/favourites", detail_html, flags=re.I)
    if m:
        return m.group(1)

    return ""


def _public_uuid_from_url(job_url: str) -> str:
    # URLs often end with a public UUID: ...-4f0bbe66-1077-4776-aab7-508a6340b943
    m = re.search(
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
        urlparse(job_url).path,
        flags=re.I,
    )
    return m.group(1) if m else ""


def _extract_apply_url(detail_soup: BeautifulSoup, job_url: str) -> Optional[str]:
    # In practice this Clinch site uses an on-page "#apply" anchor.
    # Provide a browser-openable URL (detail page + #apply).
    if detail_soup.select_one("a#apply"):
        return f"{job_url.rstrip('/') }#apply"

    for a in detail_soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if href == "#apply":
            return f"{job_url.rstrip('/') }#apply"

    return None


def _extract_description(detail_soup: BeautifulSoup) -> str:
    # Best-effort: take main content
    main = detail_soup.select_one("main")
    if not main:
        return ""
    return _clean_text(main.get_text("\n", strip=True))


# =========================
# SCRAPER
# =========================

def scrape(
    *,
    query: str = DEFAULT_QUERY,
    country_code: str = DEFAULT_COUNTRY_CODE,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    sleep_s: float = DEFAULT_SLEEP_S,
    max_jobs: Optional[int] = None,
    include_description: bool = False,
    debug: bool = True,
) -> list[dict]:
    with _make_session() as session:
        listing_items: list[dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            search_url = _build_search_url(query=query, country_code=country_code, page=page)
            html = _fetch_html(session, search_url, timeout_s)
            soup = BeautifulSoup(html, "html.parser")

            page_items = _extract_search_rows(soup)
            if debug:
                print(f"page={page} items={len(page_items)} url={search_url}")

            if not page_items:
                break

            listing_items.extend(page_items)

            if sleep_s:
                time.sleep(sleep_s)

        jobs: list[dict] = []
        for idx, item in enumerate(listing_items, start=1):
            if max_jobs is not None and len(jobs) >= max_jobs:
                break

            job_url = item.get("job_url") or ""
            if not job_url:
                continue

            try:
                detail_html = _fetch_html(session, job_url, timeout_s)
                detail_soup = BeautifulSoup(detail_html, "html.parser")

                job_uid = _job_uid_from_detail(detail_html)
                public_id = _public_uuid_from_url(job_url)

                rec: dict[str, Any] = {
                    "company": COMPANY,
                    "job_title": item.get("job_title") or "",
                    "location": "; ".join(item.get("locations") or []),
                    "job_url": job_url,
                    "job_id": job_uid or public_id,
                    "posted_date": None,
                    "deadline": None,
                    "job_type": (item.get("employment_types") or [None])[0],
                    "department": "; ".join(item.get("departments") or []),
                    "apply_url": _extract_apply_url(detail_soup, job_url),
                    "source": SOURCE,
                    "careers_url": _build_search_url(query=query, country_code=country_code, page=1),
                }

                if public_id and public_id != rec["job_id"]:
                    rec["public_id"] = public_id

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
        description="Alfa Laval jobs scraper (Clinch career site search; Singapore-focused defaults)"
    )
    ap.add_argument("--query", default=DEFAULT_QUERY, help="Search query (default: singapore)")
    ap.add_argument("--country", default=DEFAULT_COUNTRY_CODE, help="Country code filter (default: SG)")
    ap.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES, help="How many search pages to fetch")
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
        query=args.query,
        country_code=args.country,
        max_pages=args.max_pages,
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
