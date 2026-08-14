#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Amgen Careers Site Scraper (TalentBrew)

ATS:    TalentBrew (proprietary)
URL:    https://careers.amgen.com/
Filter: Singapore location
Output: All jobs as JSON print

Install: pip install requests beautifulsoup4
"""

import json
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime


CAREERS_URL = "https://careers.amgen.com"
SEARCH_URL = f"{CAREERS_URL}/search-jobs"
SOURCE_NAME = "amgen"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


class AmgenScraper:
    """Scraper for Amgen Careers site using TalentBrew."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.jobs: List[Dict[str, Any]] = []

    def fetch_jobs(self, location: str = "Singapore", max_pages: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch all jobs from Amgen careers site for given location.
        
        Args:
            location: Job location filter (default: Singapore)
            max_pages: Maximum pages to scrape
            
        Returns:
            List of job dictionaries with standardized fields
        """
        self.jobs = []
        current_page = 1
        
        while current_page <= max_pages:
            print(f"[Amgen] Fetching page {current_page}...", flush=True)
            
            params = {
                "location": location,
                "page": current_page,
                "sort": "date",
            }
            
            try:
                response = self.session.get(SEARCH_URL, params=params, timeout=30)
                response.raise_for_status()
            except requests.RequestException as e:
                print(f"[Amgen] Error fetching page {current_page}: {e}", flush=True)
                break
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find job listing container
            job_list = soup.find("ul", {"id": "search-results-jobs"})
            if not job_list:
                print(f"[Amgen] No jobs found on page {current_page}", flush=True)
                break
            
            job_items = job_list.find_all("li", recursive=False)
            if not job_items:
                print(f"[Amgen] No job items on page {current_page}", flush=True)
                break
            
            for item in job_items:
                job = self._parse_job_item(item)
                if job:
                    self.jobs.append(job)
            
            # Check for next page button
            next_button = soup.find("a", {"aria-label": "Next"})
            if not next_button or next_button.get("disabled"):
                print(f"[Amgen] No next page found, stopping at page {current_page}", flush=True)
                break
            
            current_page += 1
        
        return self.jobs

    def _parse_job_item(self, item) -> Dict[str, Any] | None:
        """
        Parse a single job item from HTML li element.
        
        Structure:
        <li>
          <a href="/job/...">
            <h3>Job Title</h3>
            <div>Location</div>
            <div>Posted Date</div>
            ...
          </a>
        </li>
        """
        try:
            link = item.find("a", href=True)
            if not link:
                return None
            
            job_url = link.get("href", "")
            if job_url and not job_url.startswith("http"):
                job_url = CAREERS_URL + job_url
            
            # Extract job ID from URL
            job_id = job_url.split("/")[-1] if job_url else ""
            
            # Job title (usually in h3)
            title_elem = link.find("h3")
            job_title = title_elem.get_text(strip=True) if title_elem else ""
            
            # Location (usually in a div)
            location_elem = link.find("div", class_=lambda x: x and "location" in x.lower())
            location = location_elem.get_text(strip=True) if location_elem else ""
            
            # Posted date (usually in a div)
            date_elem = link.find("div", class_=lambda x: x and "date" in x.lower())
            posted_date = date_elem.get_text(strip=True) if date_elem else ""
            
            if not job_title:
                return None
            
            return {
                "job_id": job_id,
                "job_title": job_title,
                "location": location,
                "posted_date": posted_date,
                "job_url": job_url,
                "source": SOURCE_NAME,
                "careers_url": CAREERS_URL,
                "scraped_at": datetime.now().isoformat(),
            }
        
        except Exception as e:
            print(f"[Amgen] Error parsing job item: {e}", flush=True)
            return None

    def print_jobs_json(self):
        """Print all fetched jobs as formatted JSON."""
        output = {
            "source": SOURCE_NAME,
            "careers_url": CAREERS_URL,
            "total_jobs": len(self.jobs),
            "jobs": self.jobs,
        }
        print("\n" + "=" * 80)
        print(f"[{SOURCE_NAME.upper()}] Job Listings - JSON Output")
        print("=" * 80)
        print(json.dumps(output, indent=2, ensure_ascii=False))
        print("=" * 80 + "\n")


def main():
    """Main entry point for testing."""
    scraper = AmgenScraper()
    
    print(f"[Amgen] Starting scraper at {datetime.now().isoformat()}", flush=True)
    
    # Fetch jobs for Singapore
    jobs = scraper.fetch_jobs(location="Singapore", max_pages=5)
    
    print(f"[Amgen] Fetched {len(jobs)} jobs", flush=True)
    
    # Print all jobs as JSON
    scraper.print_jobs_json()


if __name__ == "__main__":
    main()
