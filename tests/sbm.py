from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urldefrag

import requests
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class ScrapeConfig:
    company: str = "SBM Offshore"
    source: str = "successfactors"  # Site ist SAP SuccessFactors (j2w)
    timeout_s: int = 30


JOB_ID_RE = re.compile(r"/(\d+)/?$")


def _clean_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


def _extract_job_id(href: str) -> Optional[str]:
    m = JOB_ID_RE.search(href or "")
    return m.group(1) if m else None


def _parse_jobs_from_html(html: str, page_url: str, cfg: ScrapeConfig) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table", id="searchresults")
    if not table:
        return []

    results: List[Dict] = []
    rows = table.select("tbody tr.data-row")

    for tr in rows:
        a = tr.select_one("a.jobTitle-link")
        if not a:
            continue

        title = _clean_text(a.get_text())
        href = a.get("href", "")
        job_url = urljoin(page_url, href)

        loc_el = tr.select_one("td.colLocation span.jobLocation")
        location = _clean_text(loc_el.get_text()) if loc_el else ""

        date_el = tr.select_one("td.colDate span.jobDate")
        posted_date = _clean_text(date_el.get_text()) if date_el else ""

        job_id = _extract_job_id(href) or _extract_job_id(job_url)

        results.append(
            {
                "company": cfg.company,
                "job_title": title,
                "location": location,
                "job_id": job_id,
                "posted_date": posted_date,
                "job_url": job_url,
                "source": cfg.source,
                "careers_url": page_url,
            }
        )

    return results


def _discover_pagination_urls(html: str, page_url: str) -> List[str]:
    """
    SuccessFactors/J2W Seiten haben meist eine Pagination mit Links (1,2,3,»,…).
    Wir sammeln alle unterschiedlichen Page-URLs ein.
    """
    soup = BeautifulSoup(html, "lxml")

    urls: Set[str] = set()

    # Kandidaten: Pagination-Container (kommt je nach Template 1x oder 2x vor)
    for container in soup.select(".paginationShell, .pagination-top, .pagination-bottom"):
        for a in container.select("a[href]"):
            href = a.get("href", "")
            if not href:
                continue
            full = urljoin(page_url, href)
            full, _ = urldefrag(full)
            urls.add(full)

    # Wenn gar nichts gefunden wurde, leere Liste zurück
    # Falls gefunden: sortieren für Stabilität (Reihenfolge ist nicht kritisch, wir deduplizieren eh)
    return sorted(urls)


def scrape_sbm_search(careers_url: str, cfg: ScrapeConfig = ScrapeConfig()) -> List[Dict]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    visited: Set[str] = set()
    to_visit: List[str] = [careers_url]
    all_jobs: List[Dict] = []

    while to_visit:
        url = to_visit.pop(0)
        url, _ = urldefrag(url)
        if url in visited:
            continue
        visited.add(url)

        r = session.get(url, timeout=cfg.timeout_s)
        r.raise_for_status()
        html = r.text

        all_jobs.extend(_parse_jobs_from_html(html, url, cfg))

        # Pagination-Links einsammeln (falls es mehrere Seiten gibt)
        for next_url in _discover_pagination_urls(html, url):
            if next_url not in visited:
                to_visit.append(next_url)

    # Optional: dedupe per job_id+company (falls Pagination Links doppelt liefern)
    deduped: Dict[tuple, Dict] = {}
    for job in all_jobs:
        key = (job.get("company"), job.get("job_id"), job.get("job_url"))
        deduped[key] = job

    return list(deduped.values())


if __name__ == "__main__":
    url = "https://careers.sbmoffshore.com/search/?createNewAlert=false&q=&locationsearch=singapore&optionsFacetsDD_department=&optionsFacetsDD_facility="
    jobs = scrape_sbm_search(url)
    # Beispiel: erstes Ergebnis ausgeben
    print(f"Found {len(jobs)} jobs")
    for j in jobs[:3]:
        print(j)