#!/usr/bin/env python3

import argparse
import hashlib
import re
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.classnk.com"
CAREERS_URL = f"{BASE_URL}/hp/en/about/recruitment/"
SOURCE = "classnk-static-html"
COMPANY = "ClassNK"


_WS_RE = re.compile(r"\s+")


def _clean_text(text: str) -> str:
    text = text.replace("\u00a0", " ")
    return _WS_RE.sub(" ", text).strip()


def _stable_job_id(*parts: str) -> str:
    material = "|".join([p.strip() for p in parts if p is not None]).strip()
    digest = hashlib.sha1(material.encode("utf-8"), usedforsecurity=False).hexdigest()
    return digest


def _get(session: requests.Session, url: str, *, timeout: float = 30.0) -> str:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    # Page declares UTF-8 via <meta charset="UTF-8"> but headers may be incomplete.
    resp.encoding = "utf-8"
    return resp.text


def _parse_listings(html: str, *, careers_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    # The page contains a single recruitment table with 4 columns.
    table = soup.find("table")
    if not table:
        return []

    tbody = table.find("tbody")
    if not tbody:
        return []

    jobs: List[Dict[str, Any]] = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        location = _clean_text(tds[0].get_text(" ", strip=True))
        job_title = _clean_text(tds[1].get_text(" ", strip=True))

        designated_office: Optional[str] = None
        requirements: Optional[str] = None

        if len(tds) >= 3:
            # Preserve line breaks for readability.
            designated_office = _clean_text(tds[2].get_text("\n", strip=True))
        if len(tds) >= 4:
            requirements = _clean_text(tds[3].get_text("\n", strip=True))

        job_id = _stable_job_id(COMPANY, job_title, location)

        jobs.append(
            {
                "company": COMPANY,
                "job_title": job_title or None,
                "location": location or None,
                "job_id": job_id,
                "posted_date": None,
                "job_url": careers_url,
                "source": SOURCE,
                "careers_url": careers_url,
                # Extra fields (useful but not required)
                "designated_office": designated_office,
                "requirements": requirements,
            }
        )

    return jobs


def _filter_by_country(jobs: List[Dict[str, Any]], country: str) -> List[Dict[str, Any]]:
    country = (country or "").strip()
    if not country:
        return jobs
    needle = country.casefold()
    out: List[Dict[str, Any]] = []
    for j in jobs:
        loc = str(j.get("location") or "")
        if needle in loc.casefold():
            out.append(j)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "ClassNK recruitment table scraper (static HTML). Prints all job listings from the recruitment page."
        )
    )
    parser.add_argument("--url", default=CAREERS_URL, help=f"Recruitment page URL (default: {CAREERS_URL})")
    parser.add_argument(
        "--country",
        default="singapore",
        help="Filter by country substring match in the Location column (default: singapore). Use --country '' for all.",
    )
    parser.add_argument("--max-jobs", type=int, default=200, help="Max listings to print")
    args = parser.parse_args()

    session = requests.Session()
    html = _get(session, args.url)
    jobs = _parse_listings(html, careers_url=args.url)
    jobs = _filter_by_country(jobs, args.country)

    out = 0
    for job in jobs:
        if out >= args.max_jobs:
            break
        print(job)
        out += 1

    print(f"Found {out} jobs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
