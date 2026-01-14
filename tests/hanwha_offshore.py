from __future__ import annotations

import argparse
import time
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (HANWHA OFFSHORE SINGAPORE / WORDPRESS + INLINE MODALS)
# =========================

COMPANY = "Hanwha Offshore Singapore"
SOURCE = "wordpress_inline_modals"

CAREERS_URL = "https://www.hanwhaoffshoresingapore.com/career/"

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


def _extract_cards(soup: BeautifulSoup) -> list[dict]:
    """Extract job cards: title + modal target id."""
    out: list[dict] = []

    for a in soup.select(".career-box a[data-bs-target]"):
        target = (a.get("data-bs-target") or "").strip()
        if not target.startswith("#"):
            continue
        modal_id = target[1:]

        h6 = a.select_one("h6")
        title = _clean_text(h6.get_text(" ", strip=True) if h6 else "")
        if not title or not modal_id:
            continue

        out.append({"title": title, "modal_id": modal_id})

    # de-dupe by modal_id
    seen: set[str] = set()
    uniq: list[dict] = []
    for j in out:
        if j["modal_id"] in seen:
            continue
        seen.add(j["modal_id"])
        uniq.append(j)
    return uniq


def _extract_modal_details(soup: BeautifulSoup, modal_id: str) -> dict:
    modal = soup.find(id=modal_id)
    if modal is None:
        return {}

    title_el = modal.select_one("h3")
    title = _clean_text(title_el.get_text(" ", strip=True) if title_el else "")

    apply_a = modal.select_one('a[href][class*="custom-button"]')
    apply_url = (apply_a.get("href") or "").strip() if apply_a else ""
    if apply_url:
        apply_url = urljoin(CAREERS_URL, apply_url)

    content_el = modal.select_one(".cereer-modal-content")
    description = _clean_text(content_el.get_text("\n", strip=True) if content_el else "")

    return {
        "title": title,
        "apply_url": apply_url,
        "description": description,
    }


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

        cards = _extract_cards(soup)
        if debug:
            print(f"cards={len(cards)}")

        jobs: list[dict] = []
        for idx, card in enumerate(cards, start=1):
            if max_jobs is not None and len(jobs) >= max_jobs:
                break

            modal_id = card["modal_id"]
            details = _extract_modal_details(soup, modal_id)

            job_title = details.get("title") or card["title"]
            # There is no per-job detail page; use a stable fragment URL.
            job_url = f"{careers_url.rstrip('/')}/#{modal_id}"

            # Prefer numeric id from careerModal####; otherwise keep modal_id.
            job_id = modal_id.replace("careerModal", "") if modal_id.startswith("careerModal") else modal_id

            rec = {
                "company": COMPANY,
                "job_title": job_title,
                "location": "Singapore",
                "job_url": job_url,
                "job_id": job_id,
                "posted_date": None,
                "source": SOURCE,
                "careers_url": careers_url,
            }
            if include_apply_url and details.get("apply_url"):
                rec["apply_url"] = details["apply_url"]
            if include_description and details.get("description"):
                rec["description"] = details["description"]

            jobs.append(rec)

            if sleep_s:
                time.sleep(sleep_s)

        return jobs


def main() -> None:
    ap = argparse.ArgumentParser(description="Hanwha Offshore Singapore (WordPress inline modals) scraper")
    ap.add_argument("--url", default=CAREERS_URL, help="Careers page URL")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="HTTP timeout seconds")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S, help="Sleep seconds between processing")
    ap.add_argument("--max-jobs", type=int, default=None, help="Stop after N jobs")
    ap.add_argument("--no-debug", action="store_true", help="Disable debug prints")
    ap.add_argument(
        "--include-apply-url",
        action="store_true",
        help="Include apply_url in printed records.",
    )
    ap.add_argument(
        "--include-description",
        action="store_true",
        help="Include description in printed records (can be large).",
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
