from __future__ import annotations

import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE = "https://careers.wartsila.com"
COMPANY = "Wärtsilä"
SOURCE = "wartsila"


def extract_job_id_from_href(href: str) -> str:
    # Beispiel: /job/Singapore-Service-Engineer%2C-Propulsion/1251238801/
    m = re.search(r"/(\d{6,})/?$", (href or "").strip())
    return m.group(1) if m else ""


def scrape_wartsila_search_page(careers_url: str) -> list[dict]:
    """
    Finale Version:
    - Holt genau DIESE Search-Seite (keine Pagination, kein startrow)
    - Parsed die Tabelle #searchresults (alle <tr class="data-row">)
    - Liefert Dictionaries mit:
      company, job_title, location, job_id, posted_date, job_url, source, careers_url
    """
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; JobScraper/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
        }
    )

    r = s.get(careers_url, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    table = soup.select_one("table#searchresults")
    if not table:
        return []

    jobs: list[dict] = []

    for row in table.select("tbody tr.data-row"):
        a = row.select_one("td.colTitle a.jobTitle-link")
        if not a:
            continue

        href = (a.get("href") or "").strip()
        job_title = a.get_text(strip=True)
        job_url = urljoin(BASE, href)

        loc_el = row.select_one("td.colLocation span.jobLocation")
        location = loc_el.get_text(" ", strip=True) if loc_el else ""

        date_el = row.select_one("td.colDate span.jobDate")
        posted_date = date_el.get_text(" ", strip=True) if date_el else ""

        job_id = extract_job_id_from_href(href)

        jobs.append(
            {
                "company": COMPANY,
                "job_title": job_title,
                "location": location,
                "job_id": job_id,
                "posted_date": posted_date,
                "job_url": job_url,
                "source": SOURCE,
                "careers_url": careers_url,
            }
        )

    return jobs


if __name__ == "__main__":
    careers_url = "https://careers.wartsila.com/search/?createNewAlert=false&q=singapore&optionsFacetsDD_title=&optionsFacetsDD_country=&optionsFacetsDD_customfield2=&optionsFacetsDD_customfield1=&optionsFacetsDD_customfield3="
    jobs = scrape_wartsila_search_page(careers_url)

    print(f"Found {len(jobs)} jobs")
    for j in jobs:
        print(j)