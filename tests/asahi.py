"""
Asahi Kasei Plastics Singapore Job Scraper
Site:   https://www.asahi-kasei.com.sg/APS/careers/
ATS:    WordPress + Simple Job Board Plugin
Tech:   requests + BeautifulSoup (kein Playwright noetig)
Output: job_title | location | job_id | posted_date | job_url | source | careers_url | status

Install: pip install requests beautifulsoup4
"""

import requests
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# ── Konfiguration ──────────────────────────────────────────────────────────────

CAREERS_URL = "https://www.asahi-kasei.com.sg/APS/careers/"
BASE_URL    = "https://www.asahi-kasei.com.sg"
SOURCE_NAME = "asahi-kasei-singapore"
STATUS      = "active"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}

OUTPUT_COLUMNS = [
    "job_title", "location", "job_id", "posted_date",
    "job_url", "source", "careers_url", "status",
]

OUTPUT_DIR = Path(".")

# ── Scraping ───────────────────────────────────────────────────────────────────

def fetch() -> str | None:
    try:
        r = requests.get(CAREERS_URL, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  Fehler: {e}")
        return None


def parse(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    # Alle Datenzeilen in der Job-Tabelle (erste Zeile = Header, überspringen)
    rows = soup.select("table.job_aval tr")[1:]

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue

        title_a  = cols[1].find("a")
        if not title_a:
            continue

        title    = title_a.get_text(strip=True)
        job_url  = title_a.get("href", "")
        if job_url.startswith("/"):
            job_url = BASE_URL + job_url

        # Job-ID aus URL-Slug
        job_id   = job_url.rstrip("/").split("/")[-1]

        job_type = cols[2].get_text(strip=True)   # z.B. Permanent
        dept     = cols[3].get_text(strip=True)   # z.B. Production
        location = cols[4].get_text(strip=True)   # z.B. Jurong Island

        # Kein Datum in der Tabelle vorhanden — leer lassen
        posted_date = ""

        jobs.append({
            "job_title":   title,
            "location":    location,
            "job_id":      job_id,
            "posted_date": posted_date,
            "job_url":     job_url,
            "source":      SOURCE_NAME,
            "careers_url": CAREERS_URL,
            "status":      STATUS,
        })

    return jobs


# ── Output ─────────────────────────────────────────────────────────────────────

def save_csv(rows: list[dict], path: Path):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  CSV:  {path}")


def save_json(rows: list[dict], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"  JSON: {path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"\nAsahi Kasei Singapore Scraper")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    print(f"  Lade {CAREERS_URL} ...")
    html = fetch()
    if not html:
        print("  Seite nicht erreichbar.")
        return

    jobs = parse(html)
    print(f"  {len(jobs)} Job(s) gefunden.\n")

    if not jobs:
        print("  Keine Jobs auf der Seite.")
        return

    for j in jobs:
        print(f"  {j['job_id']:<30} {j['job_title']:<35} {j['location']}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"\nSpeichere ...")
    save_csv(jobs,  OUTPUT_DIR / f"asahi_kasei_sg_{ts}.csv")
    save_json(jobs, OUTPUT_DIR / f"asahi_kasei_sg_{ts}.json")
    print(f"\nFertig  --  {len(jobs)} Job(s) exportiert.")


if __name__ == "__main__":
    main()