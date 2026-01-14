from __future__ import annotations

import argparse
import time
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (AIBEL / CUSTOM SITE HTML + HR-MANAGER APPLY)
# =========================

COMPANY = "Aibel"
SOURCE = "aibel_html_hr_manager"

BASE_URL = "https://aibel.com"
CAREERS_URL = f"{BASE_URL}/careers/vacant-positions"

DEFAULT_TIMEOUT_S = 30
DEFAULT_SLEEP_S = 0.0


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


def _job_id_from_url(job_url: str) -> str:
    try:
        path = urlparse(job_url).path
    except Exception:
        path = job_url

    path = (path or "").rstrip("/")
    if not path:
        return ""

    parts = [p for p in path.split("/") if p]
    # expected: /jobs/<slug>
    if len(parts) >= 2 and parts[-2] == "jobs":
        return parts[-1]
    return parts[-1]


def _extract_listing_jobs(soup: BeautifulSoup) -> list[dict]:
    out: list[dict] = []

    for teaser in soup.select("div.c-job-teaser"):
        a = teaser.select_one(".c-job-list__td--position a[href]")
        if not a:
            continue

        job_url = urljoin(BASE_URL + "/", (a.get("href") or "").strip())
        title = _clean_text(a.get_text(" ", strip=True))
        if not title or not job_url:
            continue

        loc_el = teaser.select_one(".c-job-list__td--location .c-job-teaser__text")
        location = _clean_text(loc_el.get_text(" ", strip=True) if loc_el else "")

        dl_el = teaser.select_one(".c-job-list__td--deadline .c-job-teaser__text")
        deadline = _clean_text(dl_el.get_text(" ", strip=True) if dl_el else "")

        category = _clean_text(teaser.get("data-category") or "")

        out.append(
            {
                "job_title": title,
                "job_url": job_url,
                "location": location,
                "deadline": deadline or None,
                "category": category or None,
                "job_id": _job_id_from_url(job_url),
            }
        )

    # de-dupe by job_url
    seen: set[str] = set()
    uniq: list[dict] = []
    for j in out:
        if j["job_url"] in seen:
            continue
        seen.add(j["job_url"])
        uniq.append(j)

    return uniq


def _extract_apply_url(detail_soup: BeautifulSoup, base_url: str) -> str:
    # Prefer explicit ATS links
    for a in detail_soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if "hr-manager.net" in href:
            return href

    # Fallback: any external-ish link that looks like an apply CTA
    for a in detail_soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        txt = _clean_text(a.get_text(" ", strip=True)).lower()
        if not href:
            continue
        if any(k in txt for k in ("apply", "apply now", "søk", "søknad", "application")):
            return urljoin(base_url, href)

    return ""


def _extract_description(detail_soup: BeautifulSoup) -> str:
    # Best-effort: grab main content and strip boilerplate.
    main = detail_soup.select_one("main")
    container = main if main is not None else detail_soup.body
    if container is None:
        return ""

    # remove nav/footer-ish noise if present
    for sel in ("header", "nav", "footer", ".c-main-menu", ".c-footer"):
        for node in container.select(sel):
            node.extract()

    text = _clean_text(container.get_text("\n", strip=True))
    return text


# =========================
# SCRAPER
# =========================

def scrape(
    *,
    careers_url: str = CAREERS_URL,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    sleep_s: float = DEFAULT_SLEEP_S,
    max_jobs: Optional[int] = None,
    include_apply_url: bool = False,
    include_description: bool = False,
    debug: bool = True,
) -> list[dict]:
    with _make_session() as session:
        html = _fetch_html(session, careers_url, timeout_s)
        soup = BeautifulSoup(html, "html.parser")

        listing = _extract_listing_jobs(soup)
        if debug:
            print(f"listing_jobs={len(listing)}")

        jobs: list[dict] = []
        for idx, item in enumerate(listing, start=1):
            if max_jobs is not None and len(jobs) >= max_jobs:
                break

            rec = {
                "company": COMPANY,
                "job_title": item["job_title"],
                "location": item.get("location") or "",
                "job_url": item["job_url"],
                "job_id": item.get("job_id") or "",
                "posted_date": None,
                "deadline": item.get("deadline"),
                "category": item.get("category"),
                "source": SOURCE,
                "careers_url": careers_url,
            }

            if include_apply_url or include_description:
                try:
                    detail_html = _fetch_html(session, item["job_url"], timeout_s)
                    detail_soup = BeautifulSoup(detail_html, "html.parser")

                    if include_apply_url:
                        apply_url = _extract_apply_url(detail_soup, item["job_url"])
                        if apply_url:
                            rec["apply_url"] = apply_url

                    if include_description:
                        desc = _extract_description(detail_soup)
                        if desc:
                            rec["description"] = desc
                except Exception as e:
                    if debug:
                        print(
                            f"detail_fetch_failed idx={idx} url={item['job_url']} err={type(e).__name__}: {e}"
                        )

            jobs.append(rec)

            if sleep_s:
                time.sleep(sleep_s)

        return jobs


def main() -> None:
    ap = argparse.ArgumentParser(description="Aibel job opportunities scraper (HTML listing; apply via HR-Manager)")
    ap.add_argument("--url", default=CAREERS_URL, help="Careers page URL")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="HTTP timeout seconds")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S, help="Sleep seconds between processing")
    ap.add_argument("--max-jobs", type=int, default=None, help="Stop after N jobs")
    ap.add_argument("--no-debug", action="store_true", help="Disable debug prints")
    ap.add_argument(
        "--include-apply-url",
        action="store_true",
        help="Fetch detail pages and include apply_url if present (often hr-manager.net).",
    )
    ap.add_argument(
        "--include-description",
        action="store_true",
        help="Fetch detail pages and include description (can be large).",
    )
    ap.add_argument(
        "--print-limit",
        type=int,
        default=3,
        help="How many job dicts to print at the end (default: 3). Use 0 to print none.",
    )
    ap.add_argument(
        "--print-all",
        action="store_true",
        help="Print all job dicts (overrides --print-limit).",
    )
    args = ap.parse_args()

    jobs = scrape(
        careers_url=args.url,
        timeout_s=args.timeout,
        sleep_s=args.sleep,
        max_jobs=args.max_jobs,
        include_apply_url=args.include_apply_url,
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
