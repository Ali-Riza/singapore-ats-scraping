import re
from playwright.sync_api import sync_playwright

CAREERS_URL = "https://www.trelleborg.com/en/career/vacancies"
SOURCE = "trelleborg"


def clean(text):
    return re.sub(r"\s+", " ", text).strip() if text else None


def extract_job_id(url):
    if not url:
        return None
    m = re.search(r"/jobs/(\d+)-", url)
    return m.group(1) if m else None


def parse_card_text(text):
    if not text:
        return None, None, None

    full = clean(text)

    # 1) Job title sauber extrahieren
    title_match = re.search(
        r"Job title:\s*(.+?)(?=\s+(?:On-site|Remote|Hybrid)\b|\s+Job category:|\s+Location:|\s+Apply by:|$)",
        full,
        re.IGNORECASE
    )
    job_title = clean(title_match.group(1)) if title_match else None

    # 2) Location extrahieren
    location_match = re.search(
        r"Location:\s*(.+?)(?=\s+Apply by:|\s+Read more|$)",
        full,
        re.IGNORECASE
    )
    location = clean(location_match.group(1)) if location_match else None

    # 3) Apply date extrahieren
    date_match = re.search(r"Apply by:\s*(\d{4}-\d{2}-\d{2})", full, re.IGNORECASE)
    posted_date = date_match.group(1) if date_match else None

    return job_title, location, posted_date

def scrape_trelleborg_singapore_jobs():
    jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(CAREERS_URL, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(5000)

        onecruiter_frame = None
        for f in page.frames:
            if "trelleborg.onecruiter.com" in f.url:
                onecruiter_frame = f
                break

        if not onecruiter_frame:
            browser.close()
            return jobs

        # Alle potentiellen Karten/Container sammeln, die Singapore enthalten
        cards = onecruiter_frame.locator("div, article, li, section").evaluate_all("""
            els => els.map(el => ({
                text: (el.innerText || '').trim(),
                html: el.innerHTML || ''
            }))
        """)

        seen_urls = set()

        for card in cards:
            text = clean(card.get("text"))
            html = card.get("html") or ""

            if not text or "singapore" not in text.lower():
                continue

            # Job-URL aus dem Karten-HTML ziehen
            hrefs = re.findall(r'href=["\\\']([^"\\\']+)["\\\']', html, re.IGNORECASE)
            job_url = None

            for href in hrefs:
                if "/jobs/" in href:
                    job_url = href
                    break

            if not job_url:
                continue

            if job_url.startswith("/"):
                job_url = "https://trelleborg.onecruiter.com" + job_url
            elif job_url.startswith("https://trelleborg.onecruiter.com") is False and job_url.startswith("http"):
                pass

            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)

            job_title, location, posted_date = parse_card_text(text)

            jobs.append({
                "job_title": job_title,
                "location": location,
                "job_id": extract_job_id(job_url),
                "posted_date": posted_date,
                "job_url": job_url,
                "source": SOURCE,
                "careers_url": CAREERS_URL,
            })

        browser.close()

    return jobs


if __name__ == "__main__":
    jobs = scrape_trelleborg_singapore_jobs()
    print(jobs)
    print(f"{len(jobs)} Singapore jobs found")