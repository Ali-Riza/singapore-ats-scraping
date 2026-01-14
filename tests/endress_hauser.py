from __future__ import annotations

import argparse
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional
from urllib.parse import urljoin, urldefrag

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (Endress+Hauser / SAP SuccessFactors (Jobs2Web / RMK))
# =========================

COMPANY = "Endress+Hauser"
SOURCE = "successfactors"

BASE_URL = "https://careers.endress.com"
DEFAULT_CAREERS_URL = (
    f"{BASE_URL}/other-countries/search/?q=&sortColumn=referencedate&sortDirection=desc"
)

DEFAULT_TIMEOUT_S = 30

JOB_ID_RE = re.compile(r"/(\d{6,})/?$")
COUNTRY_CODE_RE = re.compile(r"\b([A-Z]{2})\b")

COUNTRY_ALIASES: dict[str, str] = {
    "singapore": "SG",
    "germany": "DE",
    "switzerland": "CH",
    "united states": "US",
    "usa": "US",
    "australia": "AU",
}


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
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
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


def _extract_country_code(location: str) -> str:
    parts = [p.strip() for p in (location or "").split(",") if p.strip()]
    # typical: "City, State, AU, 2064" → want "AU"
    for part in reversed(parts):
        if len(part) == 2 and part.isalpha() and part.upper() == part:
            return part

    # fallback: any standalone 2-letter token
    m = COUNTRY_CODE_RE.search(location or "")
    return m.group(1) if m else ""


def _parse_total_results(soup: BeautifulSoup) -> Optional[int]:
    label = soup.select_one("span.paginationLabel")
    if not label:
        return None

    bs = label.find_all("b")
    if not bs:
        return None

    last = _clean_text(bs[-1].get_text(" ", strip=True))
    try:
        return int(last.replace(",", ""))
    except Exception:
        return None


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

        date_el = row.select_one("span.jobDate")
        posted_date = _clean_text(date_el.get_text(" ", strip=True)) if date_el else ""

        job_id = _extract_job_id_from_url(job_url) or _extract_job_id_from_url(href)

        out.append(
            {
                "job_title": title,
                "job_url": job_url,
                "job_id": job_id,
                "location": location,
                "country_code": _extract_country_code(location) or None,
                "posted_date": posted_date or None,
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


def _resolve_country_code(country: str, country_code: str | None) -> tuple[str, Optional[str]]:
    if country_code:
        cc = country_code.strip().upper()
        return country, cc or None

    c = (country or "").strip()
    if len(c) == 2 and c.isalpha():
        return country, c.upper()

    alias = COUNTRY_ALIASES.get(c.lower().strip())
    return country, alias


def _filter_jobs(
    jobs: list[dict[str, Any]],
    *,
    country: str,
    country_code: Optional[str],
) -> list[dict[str, Any]]:
    if not country and not country_code:
        return jobs

    cc = (country_code or "").strip().upper()
    c = (country or "").strip().lower()

    out: list[dict[str, Any]] = []
    for j in jobs:
        loc = str(j.get("location") or "")
        j_cc = str(j.get("country_code") or "").strip().upper()

        if cc and j_cc == cc:
            out.append(j)
            continue

        if c and c in loc.lower():
            out.append(j)
            continue

    return out


def scrape(
    *,
    careers_url: str,
    country: str,
    country_code: Optional[str],
    timeout_s: int,
    max_pages: int,
    max_jobs: int,
    workers: int,
) -> list[dict[str, Any]]:
    with _make_session() as session:
        first_url = careers_url
        if "startrow=" not in first_url:
            first_url = (
                f"{first_url}&startrow=0" if "?" in first_url else f"{first_url}?startrow=0"
            )

        first_html = _fetch_html(session, first_url, timeout_s)
        first_soup = BeautifulSoup(first_html, "html.parser")

        first_jobs = _parse_listing_jobs(first_html, first_url)
        page_size = max(1, len(first_jobs))
        total = _parse_total_results(first_soup)

        startrows: list[int]
        if total is None:
            startrows = [0]
        else:
            startrows = list(range(0, total, page_size))

        if max_pages > 0:
            startrows = startrows[:max_pages]

        html_by_startrow: dict[int, str] = {0: first_html}

        def page_url_for(startrow: int) -> str:
            if startrow == 0:
                return first_url
            if "startrow=" in careers_url:
                # Keep it simple: append and let server pick last value.
                return f"{careers_url}&startrow={startrow}"
            return (
                f"{careers_url}&startrow={startrow}"
                if "?" in careers_url
                else f"{careers_url}?startrow={startrow}"
            )

        remaining = [sr for sr in startrows if sr != 0]

        def fetch_one(startrow: int) -> tuple[int, str]:
            url = page_url_for(startrow)
            with _make_session() as s:
                return startrow, _fetch_html(s, url, timeout_s)

        if remaining and workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                for sr, html in ex.map(fetch_one, remaining):
                    html_by_startrow[sr] = html
        else:
            for sr in remaining:
                try:
                    html_by_startrow[sr] = _fetch_html(session, page_url_for(sr), timeout_s)
                except Exception:
                    html_by_startrow[sr] = ""

        all_jobs: list[dict[str, Any]] = []
        for sr in startrows:
            html = html_by_startrow.get(sr) or ""
            if not html:
                continue
            page_jobs = _parse_listing_jobs(html, page_url_for(sr))
            all_jobs.extend(page_jobs)
            if max_jobs > 0 and len(all_jobs) >= max_jobs:
                all_jobs = all_jobs[:max_jobs]
                break

    filtered = _filter_jobs(all_jobs, country=country, country_code=country_code)

    # Final de-dupe by URL + add consistent fields
    seen: set[str] = set()
    uniq: list[dict[str, Any]] = []
    for j in filtered:
        u = str(j.get("job_url") or "")
        if not u or u in seen:
            continue
        seen.add(u)
        j.setdefault("company", COMPANY)
        j.setdefault("source", SOURCE)
        j.setdefault("careers_url", careers_url)
        uniq.append(j)

    return uniq


def main() -> None:
    ap = argparse.ArgumentParser(description=f"{COMPANY} careers scraper ({SOURCE})")
    ap.add_argument("--url", default=DEFAULT_CAREERS_URL, help="Search/listing URL")
    ap.add_argument("--country", default="Singapore", help="Country name (fallback match)")
    ap.add_argument("--country-code", default=None, help="2-letter country code (preferred)")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S)
    ap.add_argument("--max-pages", type=int, default=0, help="0 = no limit")
    ap.add_argument("--max-jobs", type=int, default=0, help="0 = no limit")
    ap.add_argument("--workers", type=int, default=4, help="Parallel page fetch workers")

    args = ap.parse_args()

    country, cc = _resolve_country_code(args.country, args.country_code)

    jobs = scrape(
        careers_url=args.url,
        country=country,
        country_code=cc,
        timeout_s=args.timeout,
        max_pages=args.max_pages,
        max_jobs=args.max_jobs,
        workers=max(1, args.workers),
    )

    for j in jobs:
        print(j)

    print(f"Found {len(jobs)} jobs")


if __name__ == "__main__":
    main()
