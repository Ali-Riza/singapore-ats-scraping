import subprocess
import json
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://jobs.carrier.com"
SEARCH_URL = "https://jobs.carrier.com/en/search-jobs/singapore"


def fetch_html_with_curl(url: str) -> str:
    cmd = ["curl", "-L", "-sS", "--compressed", url]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"curl failed ({res.returncode}):\n{res.stderr}")
    return res.stdout


def normalize_date(date_str: str | None) -> str | None:
    if not date_str:
        return None
    try:
        # z.B. "2025-10-9" oder "2025-12-29" -> ISO
        return datetime.fromisoformat(date_str).date().isoformat()
    except Exception:
        return date_str


def get_posted_date(job_url: str) -> str | None:
    html = fetch_html_with_curl(job_url)
    soup = BeautifulSoup(html, "html.parser")

    for script in soup.select('script[type="application/ld+json"]'):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except Exception:
            continue

        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            return normalize_date(data.get("datePosted"))

    return None


def scrape_search(url: str) -> dict:
    html = fetch_html_with_curl(url)
    soup = BeautifulSoup(html, "html.parser")

    jobs = {}

    for a in soup.select("#search-results-list ul li a[data-job-id]"):
        job_id = (a.get("data-job-id") or "").strip()
        title_el = a.select_one("h2")
        loc_el = a.select_one(".job-location")
        rel = (a.get("href") or "").strip()

        job_url = urljoin(BASE_URL, rel)

        jobs[job_id] = {
            "company": "Carrier",
            "job_title": title_el.get_text(strip=True) if title_el else None,
            "location": loc_el.get_text(strip=True) if loc_el else None,
            "job_id": job_id,
            "posted_date": get_posted_date(job_url),
            "job_url": job_url,
            "source": "jobs.carrier.com",
            "careers_url": "https://jobs.carrier.com/en/search-jobs",
        }

    return jobs


if __name__ == "__main__":
    jobs = scrape_search(SEARCH_URL)
    print(jobs)