"""
Celanese Asia Careers Scraper
ATS:    iCIMS
Filter: Singapore
Output: JSON print only
"""

import json
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

CAREERS_URL = "https://asiacareers-celanese.icims.com/jobs/search?ss=1&searchLocation=13542--Singapore"
BASE        = "https://asiacareers-celanese.icims.com"
SOURCE_NAME = "celanese"


def parse(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    # Jobs sind in div.iCIMS_JobsTable, jede Zeile hat einen iCIMS_Anchor Link
    table = soup.select_one("div.iCIMS_JobsTable")
    if not table:
        return jobs

    for anchor in table.select("a.iCIMS_Anchor"):
        raw_title = anchor.get_text(strip=True)
        if not raw_title:
            continue
        # iCIMS fügt den Spalten-Header in den Link-Text ein — entfernen
        # Format: "Job Title 职位名称Actual Job Name" -> "Actual Job Name"
        for prefix in ["Job Title 职位名称", "Job Title", "职位名称"]:
            if raw_title.startswith(prefix):
                raw_title = raw_title[len(prefix):]
                break
        title = raw_title.strip()
        if not title:
            continue

        href    = anchor.get("href", "")
        job_url = href if href.startswith("http") else f"{BASE}{href}"
        # in_iframe Parameter entfernen
        job_url = job_url.replace("?in_iframe=1&", "?").replace("&in_iframe=1", "").replace("?in_iframe=1", "")

        # Job-ID aus URL: /jobs/22695/advanced-engineer -> 22695
        parts  = [p for p in href.replace(BASE, "").split("/") if p]
        job_id = parts[1] if len(parts) > 1 else parts[0] if parts else ""

        # Location + Date aus den Job-Header-Feldern der Zeile
        row = anchor.find_parent("div", class_="iCIMS_JobHeaderGroup") or anchor.find_parent("div")
        location    = ""
        posted_date = ""
        if row:
            fields = row.select("div.iCIMS_JobHeaderField")
            for field in fields:
                tag  = field.select_one("div.iCIMS_JobHeaderTag")
                data = field.select_one("div.iCIMS_JobHeaderData")
                if not tag or not data:
                    continue
                tag_text = tag.get_text(strip=True).lower()
                if "location" in tag_text:
                    location = data.get_text(strip=True)
                elif "date" in tag_text or "posted" in tag_text:
                    posted_date = data.get_text(strip=True)

        jobs.append({
            "job_title":   title,
            "location":    location,
            "job_id":      job_id,
            "posted_date": posted_date,
            "job_url":     job_url,
            "source":      SOURCE_NAME,
            "careers_url": CAREERS_URL,
        })

    return jobs


def get_next_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    # iCIMS Paginator
    for a in soup.select("a"):
        if a.get_text(strip=True).lower() in ("next", ">", "»") or "next" in (a.get("class") or []):
            href = a.get("href", "")
            if href and "javascript" not in href:
                return href if href.startswith("http") else f"{BASE}{href}"
    return None


def main():
    print("Celanese Asia  ->  Singapore", flush=True)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", flush=True)

    all_jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()

        page.goto(CAREERS_URL, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)

        iframe_frame = None
        for frame in page.frames:
            if "in_iframe=1" in frame.url:
                iframe_frame = frame
                break

        if not iframe_frame:
            print("  Kein iframe gefunden.")
            browser.close()
            return

        # Warten bis Jobs geladen
        try:
            iframe_frame.wait_for_selector("div.iCIMS_JobsTable a.iCIMS_Anchor", timeout=15000)
        except PlaywrightTimeout:
            print("  Timeout -- keine Jobs im iframe.")
            browser.close()
            return

        page_n = 1
        while True:
            print(f"  Seite {page_n} ...", flush=True)
            html  = iframe_frame.content()
            jobs  = parse(html)
            print(f"    -> {len(jobs)} Jobs", flush=True)
            all_jobs.extend(jobs)

            next_url = get_next_url(html)
            if not next_url:
                break

            page.goto(next_url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(1500)

            iframe_frame = None
            for frame in page.frames:
                if "in_iframe=1" in frame.url:
                    iframe_frame = frame
                    break
            if not iframe_frame:
                break

            try:
                iframe_frame.wait_for_selector("div.iCIMS_JobsTable a.iCIMS_Anchor", timeout=10000)
            except PlaywrightTimeout:
                break

            page_n += 1

        browser.close()

    print(f"\n{len(all_jobs)} Jobs gesamt\n")
    print(json.dumps(all_jobs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()