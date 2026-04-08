import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://www.teknorapexcareers.com"
CAREERS_URL = "https://www.teknorapexcareers.com/jobs/sg-sgp-singapore/"
SOURCE = "teknorapexcareers"


def scrape_teknor_apex_jobs():
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(CAREERS_URL, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    results = []

    job_list_section = soup.select_one("#job-list-section")
    if not job_list_section:
        return results

    for job_card in job_list_section.find_all("div", recursive=False):
        link = job_card.find("a", href=True)
        if not link:
            continue

        title_el = link.find("h3")
        job_title = title_el.get_text(strip=True) if title_el else None
        job_url = urljoin(BASE_URL, link["href"])

        job_id = None
        location = None

        for span in job_card.select("dl span"):
            dt = span.find("dt")
            dd = span.find("dd")
            if not dt or not dd:
                continue

            label = dt.get_text(" ", strip=True).lower()
            value = " ".join(dd.stripped_strings)

            if "job ref" in label:
                job_id = value
            elif "location" in label:
                location = value

        results.append({
            "job_title": job_title,
            "location": location,
            "job_id": job_id,
            "posted_date": None,     # auf der Listing-Seite nicht sichtbar
            "job_url": job_url,
            "source": SOURCE,
            "careers_url": CAREERS_URL,
            "status": "active"
        })

    return results


if __name__ == "__main__":
    jobs = scrape_teknor_apex_jobs()
    print(json.dumps(jobs, ensure_ascii=False, indent=2))