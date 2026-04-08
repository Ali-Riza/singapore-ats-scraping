import json
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

CAREERS_URL = (
    "https://jobs.totalenergies.com/en_US/careers/SearchJobs/"
    "?707=%5B42257%2C42253%5D&707_format=1393&3834=%5B41686%5D"
    "&3834_format=3639&listFilterMode=1&jobRecordsPerPage=20"
)
SOURCE = "totalenergies"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def extract_job_id(job_url: str | None, apply_url: str | None) -> str | None:
    # 1) bevorzugt aus Apply-URL: ...ApplicationMethods?jobId=77933
    if apply_url:
        parsed = urlparse(apply_url)
        qs = parse_qs(parsed.query)
        if "jobId" in qs and qs["jobId"]:
            return qs["jobId"][0]

    # 2) fallback aus Job-Detail-URL: .../JobDetail/.../78708
    if job_url:
        m = re.search(r"/JobDetail/.+/(\d+)$", job_url)
        if m:
            return m.group(1)

    return None


def scrape_totalenergies_jobs():
    response = requests.get(CAREERS_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    results = []

    # jeder Job steckt in einem article--result Block
    for article in soup.select("div.article.article--result"):
        title_link = article.select_one("h3.article__header__text__title a.link")
        if not title_link:
            continue

        job_title = title_link.get_text(strip=True)
        job_url = title_link.get("href")

        posted_date_el = article.select_one(".list-item-jobCreationDate")
        location_el = article.select_one(".list-item-jobCountry")

        apply_link = article.select_one("a.button.button--secondary[href*='ApplicationMethods']")
        apply_url = apply_link.get("href") if apply_link else None

        job_id = extract_job_id(job_url, apply_url)

        results.append({
            "job_title": job_title,
            "location": location_el.get_text(strip=True) if location_el else None,
            "job_id": job_id,
            "posted_date": posted_date_el.get_text(strip=True) if posted_date_el else None,
            "job_url": job_url,
            "source": SOURCE,
            "careers_url": CAREERS_URL,
            "status": "active"
        })

    return results


if __name__ == "__main__":
    jobs = scrape_totalenergies_jobs()
    print(json.dumps(jobs, ensure_ascii=False, indent=2))