from __future__ import annotations

import argparse
import re
import time
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (KONGSBERG / Optimizely(EPiServer) CMS + EasyCruit apply)
# =========================

COMPANY = "Kongsberg Maritime"
SOURCE = "kongsberg_optimizely_easycruit"

BASE_URL = "https://www.kongsberg.com"
CAREERS_URL = f"{BASE_URL}/careers/vacancies/"

DEFAULT_TIMEOUT_S = 30
DEFAULT_SLEEP_S = 0.0
DEFAULT_LOCATION_CONTAINS = "Singapore"


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


def _normalize_kongsberg_url(href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""

    abs_url = urljoin(BASE_URL + "/", href)
    parsed = urlparse(abs_url)
    if parsed.scheme not in ("http", "https"):
        return ""
    if parsed.netloc and parsed.netloc not in ("www.kongsberg.com", "kongsberg.com"):
        return ""

    # Strip fragments; keep query (some sites use it for tracking)
    cleaned = parsed._replace(fragment="").geturl()
    return cleaned


def _extract_listing_job_urls(listing_soup: BeautifulSoup) -> list[str]:
    # Best-effort: collect anything that looks like a vacancy detail link.
    # Detail pages follow /careers/vacancies/<slug>/
    urls: list[str] = []

    for a in listing_soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        if "/careers/vacancies/" not in href:
            continue

        abs_url = _normalize_kongsberg_url(href)
        if not abs_url:
            continue

        # Avoid re-adding the listing page itself
        if abs_url.rstrip("/") == CAREERS_URL.rstrip("/"):
            continue

        # Heuristic: require path depth beyond /careers/vacancies/
        path = urlparse(abs_url).path.rstrip("/")
        if path.count("/") < 3:
            continue

        urls.append(abs_url)

    # De-dupe while preserving order
    seen: set[str] = set()
    uniq: list[str] = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)

    return uniq


def _extract_listing_items_from_html(listing_html: str) -> list[dict[str, str]]:
    """Extract vacancy listing items from embedded JSON-like data.

    The Kongsberg vacancies listing page includes structured entries like:
    {"id":"3574671","title":"...","location":"Kongsberg Maritime Singapore, Singapore","detailsUrl":"/careers/vacancies/.../", ...}

    Parsing this is much faster than fetching all detail pages just to filter by location.
    """

    items: list[dict[str, str]] = []

    # Keep the regex intentionally narrow to avoid accidental cross-matching.
    pattern = re.compile(
        r'{"id":"(?P<id>\d+)",' 
        r'"title":"(?P<title>[^"]*)",'
        r'"location":"(?P<location>[^"]*)",'
        r'"detailsUrl":"(?P<detailsUrl>[^"]+)"'
    )

    for m in pattern.finditer(listing_html or ""):
        details_url = (m.group("detailsUrl") or "").strip()
        if "/careers/vacancies/" not in details_url:
            continue

        job_url = _normalize_kongsberg_url(details_url)
        if not job_url:
            continue
        if job_url.rstrip("/") == CAREERS_URL.rstrip("/"):
            continue

        items.append(
            {
                "job_id": (m.group("id") or "").strip(),
                "job_title": _clean_text(m.group("title")),
                "location": _clean_text(m.group("location")),
                "job_url": job_url,
            }
        )

    # De-dupe by URL (preserve order)
    seen: set[str] = set()
    uniq: list[dict[str, str]] = []
    for it in items:
        u = it.get("job_url", "")
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(it)

    return uniq


def _extract_info_panel(detail_soup: BeautifulSoup) -> dict[str, str]:
    out: dict[str, str] = {}

    # Expected structure:
    # <div class="InfoPanel"><dl> <div><dt>Location</dt><dd>Singapore</dd></div> ...
    for row in detail_soup.select(".InfoPanel dl div"):
        dt = row.select_one("dt")
        dd = row.select_one("dd")
        if not dt or not dd:
            continue

        key = _clean_text(dt.get_text(" ", strip=True)).lower()
        val = _clean_text(dd.get_text(" ", strip=True))
        if key and val:
            out[key] = val

    return out


def _extract_job_title(detail_soup: BeautifulSoup) -> str:
    h1 = detail_soup.select_one("h1.VacancyPage__heading")
    if h1:
        return _clean_text(h1.get_text(" ", strip=True))

    h1 = detail_soup.select_one("h1")
    return _clean_text(h1.get_text(" ", strip=True) if h1 else "")


def _extract_apply_url(detail_soup: BeautifulSoup) -> str:
    a = detail_soup.select_one("a.VacancyPage__applyNowBtn[href]")
    if a:
        href = (a.get("href") or "").strip()
        return href

    for a in detail_soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if "easycruit.com/vacancy" in href:
            return href

    return ""


def _job_id_from_detail(detail_soup: BeautifulSoup, job_url: str, apply_url: str) -> str:
    # Prefer EasyCruit vacancy id
    m = re.search(r"/vacancy/application/(\d+)/", apply_url or "")
    if m:
        return m.group(1)

    # Sometimes appears in hero image filename: vacancy-3367411-YYYY-MM-DD.jpg
    srcset = " ".join(
        [
            (s.get("srcset") or "")
            for s in detail_soup.select(".VacancyPage__picture source[srcset]")
        ]
    )
    m = re.search(r"vacancy-(\d+)-", srcset)
    if m:
        return m.group(1)

    # Fallback: slug
    try:
        path = urlparse(job_url).path.rstrip("/")
        return path.split("/")[-1] if path else ""
    except Exception:
        return ""


def _extract_description(detail_soup: BeautifulSoup) -> str:
    content = detail_soup.select_one(".VacancyPage__bodyContent")
    if not content:
        content = detail_soup.select_one("main")
    if not content:
        return ""

    return _clean_text(content.get_text("\n", strip=True))


# =========================
# SCRAPER
# =========================

def scrape(
    *,
    careers_url: str = CAREERS_URL,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    sleep_s: float = DEFAULT_SLEEP_S,
    max_jobs: Optional[int] = None,
    location_contains: Optional[str] = None,
    include_description: bool = False,
    debug: bool = True,
) -> list[dict]:
    with _make_session() as session:
        listing_html = _fetch_html(session, careers_url, timeout_s)
        listing_soup = BeautifulSoup(listing_html, "html.parser")

        listing_items = _extract_listing_items_from_html(listing_html)
        if listing_items:
            if debug:
                print(f"listing_items={len(listing_items)}")
        else:
            job_urls = _extract_listing_job_urls(listing_soup)
            listing_items = [{"job_url": u, "job_id": "", "job_title": "", "location": ""} for u in job_urls]
            if debug:
                print(f"listing_job_urls={len(job_urls)}")

        if location_contains is not None:
            needle = location_contains.lower()
            listing_items = [
                it for it in listing_items if needle in (it.get("location") or "").lower()
            ]
            if debug:
                print(f"listing_items_filtered={len(listing_items)}")

        jobs: list[dict] = []
        for idx, item in enumerate(listing_items, start=1):
            if max_jobs is not None and len(jobs) >= max_jobs:
                break

            job_url = item.get("job_url", "")
            if not job_url:
                continue

            try:
                detail_html = _fetch_html(session, job_url, timeout_s)
                detail_soup = BeautifulSoup(detail_html, "html.parser")

                title = _extract_job_title(detail_soup) or item.get("job_title", "")
                info = _extract_info_panel(detail_soup)
                apply_url = _extract_apply_url(detail_soup)
                job_id = _job_id_from_detail(detail_soup, job_url, apply_url) or item.get("job_id", "")

                rec: dict[str, Any] = {
                    "company": COMPANY,
                    "job_title": title,
                    "location": info.get("location") or item.get("location", ""),
                    "job_url": job_url,
                    "job_id": job_id,
                    "posted_date": None,
                    "deadline": info.get("application deadline") or None,
                    "job_type": info.get("job type") or None,
                    "working_hours": info.get("working hours") or None,
                    "working_days": info.get("working days") or None,
                    "apply_url": apply_url or None,
                    "source": SOURCE,
                    "careers_url": careers_url,
                }

                if include_description:
                    rec["description"] = _extract_description(detail_soup)

                jobs.append(rec)

            except Exception as e:
                if debug:
                    print(f"detail_fetch_failed idx={idx} url={job_url} err={type(e).__name__}: {e}")

            if sleep_s:
                time.sleep(sleep_s)

        return jobs


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Kongsberg vacancies scraper (Optimizely CMS pages; apply via EasyCruit)"
    )
    ap.add_argument("--url", default=CAREERS_URL, help="Vacancies listing URL")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="HTTP timeout seconds")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S, help="Sleep seconds between requests")
    ap.add_argument("--max-jobs", type=int, default=None, help="Stop after N jobs")
    ap.add_argument(
        "--location-contains",
        default=DEFAULT_LOCATION_CONTAINS,
        help=(
            "Only keep jobs whose location contains this substring (case-insensitive). "
            "Default: Singapore. Use an empty string to disable filtering."
        ),
    )
    ap.add_argument("--include-description", action="store_true", help="Include long description text")
    ap.add_argument("--no-debug", action="store_true", help="Disable debug prints")
    ap.add_argument(
        "--print-limit",
        type=int,
        default=3,
        help="How many job dicts to print at the end (default: 3). Use 0 to print none.",
    )
    ap.add_argument("--print-all", action="store_true", help="Print all job dicts")
    args = ap.parse_args()

    jobs = scrape(
        careers_url=args.url,
        timeout_s=args.timeout,
        sleep_s=args.sleep,
        max_jobs=args.max_jobs,
        location_contains=args.location_contains,
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
