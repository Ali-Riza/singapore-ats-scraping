from __future__ import annotations

import argparse
import re
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (Everllence / Sitefinity CMS page with embedded job accordions)
# =========================

COMPANY = "Everllence"
SOURCE = "sitefinity"

DEFAULT_URL = "https://www.everllence.com/career/international-jobs/singapore"
DEFAULT_TIMEOUT_S = 30

ID_RE = re.compile(r"accordion-[^-]+-([a-f0-9\-]{36})", re.I)


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


def _scrape_jobs_from_page(html: str, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    # This page contains multiple accordions (e.g., benefits). We only want the
    # job-offerings accordion which is rendered after the "Job offerings" block.
    job_accordion = None
    marker = soup.find(string=re.compile(r"Job offerings", re.I))
    if marker:
        # Find the next accordion after the marker.
        cur = marker.parent
        while cur:
            cur = cur.find_next()
            if not cur:
                break
            if getattr(cur, "name", None) and "accordion" in (cur.get("class") or []):
                job_accordion = cur
                break
            acc = cur.select_one(".accordion") if getattr(cur, "select_one", None) else None
            if acc is not None:
                job_accordion = acc
                break

    if job_accordion is None:
        job_accordion = soup.select_one(".accordion")

    out: list[dict[str, Any]] = []
    for item in (job_accordion.select(".accordion-item") if job_accordion else []):
        title_el = item.select_one("h5")
        title = _clean_text(title_el.get_text(" ", strip=True)) if title_el else ""
        if not title:
            continue

        # Try to extract a stable ID from the collapse target or element ids
        job_id = None
        collapse = item.select_one(".accordion-collapse[id]")
        if collapse and collapse.get("id"):
            m = ID_RE.search(str(collapse.get("id")))
            if m:
                job_id = m.group(1)
            else:
                job_id = str(collapse.get("id"))

        desc_el = item.select_one(".accordion-body .card-text")
        description_html = str(desc_el) if desc_el else ""

        # This page is already Singapore-specific
        job_url = page_url
        if collapse and collapse.get("id"):
            job_url = page_url.split("#")[0] + "#" + str(collapse.get("id"))

        rec: dict[str, Any] = {
            "company": COMPANY,
            "source": SOURCE,
            "careers_url": page_url.split("#")[0],
            "country": "Singapore",
            "location": "Singapore",
            "job_title": title,
            "job_id": job_id,
            "posted_date": None,
            "job_url": job_url,
            "apply_url": None,  # no ATS/apply link present on this page
        }

        if description_html:
            rec["description_html"] = description_html

        out.append(rec)

    return out


def scrape(*, url: str, timeout_s: int, max_jobs: int) -> list[dict[str, Any]]:
    with _make_session() as session:
        r = session.get(url, timeout=timeout_s)
        r.raise_for_status()
        html = r.text

    jobs = _scrape_jobs_from_page(html, url)

    if max_jobs > 0:
        jobs = jobs[:max_jobs]

    return jobs


def main() -> None:
    ap = argparse.ArgumentParser(description=f"{COMPANY} Singapore jobs (CMS page)")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S)
    ap.add_argument("--max-jobs", type=int, default=0, help="0 = no limit")

    args = ap.parse_args()

    jobs = scrape(url=args.url, timeout_s=args.timeout, max_jobs=args.max_jobs)

    for j in jobs:
        print(j)

    print(f"Found {len(jobs)} jobs")


if __name__ == "__main__":
    main()
