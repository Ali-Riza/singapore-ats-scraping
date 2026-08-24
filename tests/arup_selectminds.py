#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Arup SelectMinds/Taleo test scraper.

Source page type: Oracle Taleo / SelectMinds (legacy)
Output: Prints parsed jobs as JSON

Install:
  pip install requests beautifulsoup4
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urljoin
import re

import requests
from bs4 import BeautifulSoup, Tag


BASE_URL = "https://jobs.arup.com"
START_URL = "https://jobs.arup.com/page/data-centre-roles-with-arup-512"
SOURCE_NAME = "arup_selectminds"
LOCATION_FILTER = "Singapore, -, Singapore"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


class ArupSelectMindsScraper:
    """Simple test scraper for Arup's SelectMinds/Taleo listing pages."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.jobs: List[Dict[str, Any]] = []
        self.total_rows_found: int = 0
        self.tss_token: str = ""

    def fetch_jobs(self, url: str = START_URL) -> List[Dict[str, Any]]:
        """Fetch and parse jobs from all listing pages for this landing page."""
        self.jobs = []
        all_rows: List[Tag] = []

        print(f"[Arup] Fetching: {url}", flush=True)
        response = self.session.get(url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        self.tss_token = self._extract_tss_token(soup)
        rows = soup.select("div.job_list_row")
        all_rows.extend(rows)

        num_pages = self._extract_num_pages(soup)
        search_id = self._extract_search_id(soup)

        if num_pages > 1 and search_id:
            for page_index in range(2, num_pages + 1):
                page_rows = self._fetch_page_rows(search_id, page_index)
                all_rows.extend(page_rows)

        self.total_rows_found = len(all_rows)

        print(f"[Arup] Found {len(all_rows)} job rows", flush=True)

        for row in all_rows:
            job = self._parse_job_row(row)
            if job and self._is_singapore_job(job):
                self.jobs.append(job)

        return self.jobs

    def _parse_job_row(self, row: Tag) -> Dict[str, Any] | None:
        """Parse one SelectMinds job card from `div.job_list_row`."""
        link = row.select_one("a.job_link")
        if not isinstance(link, Tag):
            return None

        title = link.get_text(" ", strip=True)
        rel_url = (link.get("href") or "").strip()
        job_url = urljoin(BASE_URL, rel_url)

        job_id = ""
        row_id = (row.get("id") or "").strip()
        if row_id.startswith("job_list_"):
            job_id = row_id.replace("job_list_", "", 1)

        location = ""
        location_link = row.select_one("p.jlr_location a.location")
        if isinstance(location_link, Tag):
            location = self._clean_location_text(location_link.get_text(" ", strip=True))

        requisition = ""
        req_node = row.select_one(".job_external_id .field_value")
        if isinstance(req_node, Tag):
            requisition = req_node.get_text(" ", strip=True)

        total_views = ""
        views_node = row.select_one(".job_total_views .field_value")
        if isinstance(views_node, Tag):
            total_views = views_node.get_text(" ", strip=True)

        summary = ""
        summary_node = row.select_one("p.jlr_description")
        if isinstance(summary_node, Tag):
            summary = summary_node.get_text(" ", strip=True)

        if not title:
            return None

        return {
            "job_id": job_id,
            "job_title": title,
            "location": location,
            "requisition_number": requisition,
            "total_views": total_views,
            "summary": summary,
            "job_url": job_url,
            "source": SOURCE_NAME,
            "careers_url": START_URL,
            "scraped_at": datetime.now().isoformat(),
        }

    def _extract_num_pages(self, soup: BeautifulSoup) -> int:
        """Read the total number of pages from hidden field `#jNumPagesInit`."""
        node = soup.select_one("#jNumPagesInit")
        if not isinstance(node, Tag):
            return 1
        raw = (node.get("value") or "1").strip()
        try:
            return max(1, int(float(raw)))
        except ValueError:
            return 1

    def _extract_search_id(self, soup: BeautifulSoup) -> str:
        """Read Search ID from hidden field `#jSearchId`."""
        node = soup.select_one("#jSearchId")
        if not isinstance(node, Tag):
            return ""
        return (node.get("value") or "").strip()

    def _extract_tss_token(self, soup: BeautifulSoup) -> str:
        """Read anti-CSRF token from hidden field `#tsstoken`."""
        node = soup.select_one("#tsstoken")
        if not isinstance(node, Tag):
            return ""
        return (node.get("value") or "").strip()

    def _fetch_page_rows(self, search_id: str, page_index: int) -> List[Tag]:
        """Fetch one extra page using SelectMinds ajax endpoint."""
        uid = int(datetime.now().timestamp() * 1000) % 1000
        ajax_url = (
            f"{BASE_URL}/ajax/content/landingpage_job_results"
            f"?JobSearch.id={search_id}&page_index={page_index}&uid={uid}"
        )
        print(f"[Arup] Fetching page {page_index}: {ajax_url}", flush=True)

        ajax_headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Origin": BASE_URL,
            "Referer": START_URL,
            "X-Requested-With": "XMLHttpRequest",
        }
        if self.tss_token:
            ajax_headers["tss-token"] = self.tss_token

        response = self.session.post(ajax_url, headers=ajax_headers, timeout=30)
        response.raise_for_status()
        payload = response.json()
        html = payload.get("Result") if isinstance(payload, dict) else ""

        if not isinstance(html, str) or not html.strip():
            return []

        page_soup = BeautifulSoup(html, "html.parser")
        return list(page_soup.select("div.job_list_row"))

    def _clean_location_text(self, raw_location: str) -> str:
        """Remove leading icon chars and normalize whitespace."""
        text = raw_location.strip()
        text = re.sub(r"^[^A-Za-z0-9]+", "", text)
        return " ".join(text.split())

    def _is_singapore_job(self, job: Dict[str, Any]) -> bool:
        """Hard filter to keep only Singapore jobs."""
        location = (job.get("location") or "").strip().lower()
        return LOCATION_FILTER.lower() in location

    def print_jobs_json(self) -> None:
        """Print parsed jobs as formatted JSON."""
        payload = {
            "source": SOURCE_NAME,
            "careers_url": START_URL,
            "location_filter": LOCATION_FILTER,
            "total_rows_found": self.total_rows_found,
            "total_jobs": len(self.jobs),
            "jobs": self.jobs,
        }
        print("\n" + "=" * 80)
        print("[ARUP_SELECTMINDS] Job Listings - JSON Output")
        print("=" * 80)
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        print("=" * 80 + "\n")


def main() -> None:
    scraper = ArupSelectMindsScraper()
    print(f"[Arup] Start: {datetime.now().isoformat()}", flush=True)

    try:
        jobs = scraper.fetch_jobs()
        print(f"[Arup] Parsed {len(jobs)} jobs", flush=True)
        scraper.print_jobs_json()
    except Exception as exc:
        print(f"[Arup] Error: {exc}", flush=True)


if __name__ == "__main__":
    main()
