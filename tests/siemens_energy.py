from __future__ import annotations

import argparse
import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional
from urllib.parse import urljoin, urldefrag

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (Siemens Energy / Avature Careers Marketplace)
# =========================

COMPANY = "Siemens Energy"
SOURCE = "avature"

BASE_URL = "https://jobs.siemens-energy.com"
DEFAULT_LISTING_URL = (
    f"{BASE_URL}/en_US/jobs/Jobs/"
    "?29454=964610&29454_format=11381&listFilterMode=1&folderRecordsPerPage=20"
)
JOBINFO_URL = f"{BASE_URL}/en_US/jobs/JobInfo"

DEFAULT_TIMEOUT_S = 30
DEFAULT_SLEEP_S = 0.0

FOLDER_ID_RE = re.compile(r"/(\d{3,})/?$")


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


def _extract_folder_id(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    url, _frag = urldefrag(url)
    m = FOLDER_ID_RE.search(url)
    return m.group(1) if m else ""


def _parse_listing(html: str, page_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")

    items: list[dict[str, str]] = []
    for a in soup.select("a.article__header__focusable[href]"):
        title = _clean_text(a.get_text(" ", strip=True))
        href = (a.get("href") or "").strip()
        if not href:
            continue

        folder_url = urljoin(page_url, href)
        folder_url, _frag = urldefrag(folder_url)

        folder_id = _extract_folder_id(folder_url) or _extract_folder_id(href)

        if not folder_id or not title:
            continue

        items.append({"folder_id": folder_id, "job_id": folder_id, "job_title": title, "job_url": folder_url})

    # de-dupe by folder_id
    seen: set[str] = set()
    uniq: list[dict[str, str]] = []
    for it in items:
        fid = it["folder_id"]
        if fid in seen:
            continue
        seen.add(fid)
        uniq.append(it)

    return uniq


def _fetch_jobinfo_fields(session: requests.Session, *, folder_id: str, timeout_s: int) -> dict[str, str]:
    html = _fetch_html(session, f"{JOBINFO_URL}?jobId={folder_id}", timeout_s)
    soup = BeautifulSoup(html, "html.parser")

    out: dict[str, str] = {}
    for field in soup.select(".article__content__view__field"):
        lab = field.select_one(".article__content__view__field__label")
        val = field.select_one(".article__content__view__field__value")
        label = _clean_text(lab.get_text(" ", strip=True)) if lab else ""
        value = _clean_text(val.get_text(" ", strip=True)) if val else ""
        if label and value:
            out[label] = value

    return out


def _make_threadlocal_session_factory() -> tuple[threading.local, Any]:
    threadlocal = threading.local()

    def get_session() -> requests.Session:
        sess = getattr(threadlocal, "session", None)
        if sess is None:
            sess = _make_session()
            threadlocal.session = sess
        return sess

    return threadlocal, get_session


def _build_location(fields: dict[str, str]) -> str:
    city = fields.get("City") or ""
    state = fields.get("State/Prov/County") or ""
    country = fields.get("Country / Region") or ""

    parts = [p for p in [city, state, country] if p]
    return ", ".join(parts)


def _extract_additional_posting_location_fields(folderdetail_html: str) -> dict[str, str]:
    soup = BeautifulSoup(folderdetail_html, "html.parser")
    out: dict[str, str] = {}

    # Avature uses this block for extra posting locations on FolderDetail pages
    container = soup.select_one(".article__content__view__field.additional-posting-locations")
    if not container:
        return out

    for field in container.select(".MultipleDataSetField"):
        lab = field.select_one(".MultipleDataSetFieldLabel")
        val = field.select_one(".MultipleDataSetFieldValue")
        label = _clean_text(lab.get_text(" ", strip=True)) if lab else ""
        value = _clean_text(val.get_text(" ", strip=True)) if val else ""
        if label and value:
            # Keep first occurrence; these can repeat across multiple postings.
            out.setdefault(label, value)

    return out


def _extract_posted_date_from_folderdetail(folderdetail_html: str) -> Optional[str]:
    # Avature FolderDetail includes JSON-LD with datePosted (ISO-8601).
    m = re.search(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        folderdetail_html,
        flags=re.S | re.I,
    )
    if not m:
        return None
    try:
        data = json.loads(m.group(1).strip())
    except Exception:
        return None
    v = data.get("datePosted")
    if isinstance(v, str) and v:
        return v.strip()
    return None


# =========================
# SCRAPER
# =========================

def scrape(
    *,
    listing_url: str = DEFAULT_LISTING_URL,
    country_contains: str = "Singapore",
    timeout_s: int = DEFAULT_TIMEOUT_S,
    sleep_s: float = DEFAULT_SLEEP_S,
    workers: int = 8,
    max_jobs: Optional[int] = None,
    debug: bool = True,
) -> list[dict[str, Any]]:
    with _make_session() as session:
        listing_html = _fetch_html(session, listing_url, timeout_s)
        listing_items = _parse_listing(listing_html, listing_url)

        if debug:
            print(f"listing_items={len(listing_items)}")

        if max_jobs is not None:
            listing_items = listing_items[:max_jobs]

        jobs: list[dict[str, Any]] = []
        _, get_thread_session = _make_threadlocal_session_factory()

        def fetch_fields(folder_id: str) -> dict[str, str]:
            sess = get_thread_session()
            return _fetch_jobinfo_fields(sess, folder_id=folder_id, timeout_s=timeout_s)

        max_workers = max(1, int(workers or 1))
        if debug:
            print(f"jobinfo_workers={max_workers}")

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures_by_id: dict[str, Any] = {}
            for it in listing_items:
                folder_id = it.get("folder_id") or ""
                if not folder_id:
                    continue
                futures_by_id[folder_id] = ex.submit(fetch_fields, folder_id)

            # Consume in original order for deterministic output
            for idx, it in enumerate(listing_items, start=1):
                folder_id = it.get("folder_id") or ""
                if not folder_id:
                    continue

                fut = futures_by_id.get(folder_id)
                if fut is None:
                    continue

                try:
                    fields = fut.result()
                except Exception as e:
                    if debug:
                        print(f"jobinfo_failed idx={idx} id={folder_id} err={type(e).__name__}: {e}")
                    continue

                if sleep_s:
                    time.sleep(sleep_s)

                country = fields.get("Country / Region") or ""
                folderdetail_html: Optional[str] = None
                if country_contains and country_contains.lower() not in country.lower():
                    # Fallback: some postings are primarily elsewhere but have additional
                    # posting locations (e.g., Singapore) only visible on FolderDetail.
                    try:
                        folderdetail_html = _fetch_html(session, it.get("job_url") or "", timeout_s)
                    except Exception as e:
                        if debug:
                            print(
                                f"skip idx={idx} id={folder_id} country={country} folderdetail_err={type(e).__name__}: {e}"
                            )
                        continue

                    extra = _extract_additional_posting_location_fields(folderdetail_html)
                    extra_country = extra.get("Country:") or ""
                    if country_contains.lower() not in extra_country.lower():
                        if debug:
                            print(f"skip idx={idx} id={folder_id} country={country}")
                        continue

                    # Override to represent the Singapore posting.
                    fields = dict(fields)
                    fields["Country / Region"] = country_contains
                    extra_state = extra.get("State/Province/County:") or ""
                    if extra_state:
                        fields["State/Prov/County"] = extra_state
                    fields["City"] = ""
                    country = fields.get("Country / Region") or country

                # posted_date: read from FolderDetail JSON-LD
                if folderdetail_html is None:
                    try:
                        folderdetail_html = _fetch_html(session, it.get("job_url") or "", timeout_s)
                    except Exception:
                        folderdetail_html = None
                posted_date = (
                    _extract_posted_date_from_folderdetail(folderdetail_html)
                    if folderdetail_html
                    else None
                )

                job_url = it.get("job_url") or ""
                apply_url = f"{BASE_URL}/en_US/jobs/Login?folderId={folder_id}"

                rec: dict[str, Any] = {
                    "company": COMPANY,
                    "job_title": it.get("job_title") or "",
                    "job_url": job_url,
                    "apply_url": apply_url,
                    "job_id": folder_id,
                    "location": _build_location(fields),
                    "country": country or None,
                    "city": fields.get("City") or None,
                    "state": fields.get("State/Prov/County") or None,
                    "business_unit": fields.get("Business Unit") or None,
                    "full_part_time": fields.get("Full / Part time") or None,
                    "experience_level": fields.get("Experience Level") or None,
                    "remote_vs_office": fields.get("Remote vs. Office") or None,
                    "global_job_family": fields.get("Global Job Family") or None,
                    "posted_date": posted_date,
                    "source": SOURCE,
                    "listing_url": listing_url,
                }

                jobs.append(rec)

        return jobs


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Siemens Energy (jobs.siemens-energy.com) scraper — Avature Careers Marketplace. Defaults to Singapore via JobInfo country filter."
    )
    ap.add_argument("--url", default=DEFAULT_LISTING_URL, help="Listing URL (Jobs page)")
    ap.add_argument(
        "--country-contains",
        default="Singapore",
        help="Country/Region substring filter (default: Singapore). Use empty string to disable.",
    )
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="HTTP timeout seconds")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S, help="Sleep seconds between requests")
    ap.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Parallel workers for JobInfo requests (default: 8).",
    )
    ap.add_argument("--max-jobs", type=int, default=None, help="Only process first N listing items")
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
        listing_url=args.url,
        country_contains=args.country_contains,
        timeout_s=args.timeout,
        sleep_s=args.sleep,
        workers=args.workers,
        max_jobs=args.max_jobs,
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
