import re
import time
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup


# -----------------------------
# Konfiguration
# -----------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; JobScraper/1.0; +https://example.com/bot)"
}

JOB_REF_RE = re.compile(r"\b[\w-]+/TP/\d+/\d+\b", re.IGNORECASE)
DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{2,4})\b")  # 03/12/25 oder 02/01/2026
RECORD_RE = re.compile(r"[?&]record=(\d+)\b")


# -----------------------------
# URL-Helpers
# -----------------------------

def with_query(url: str, **params) -> str:
    """Fügt Query-Parameter hinzu/überschreibt sie."""
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in params.items():
        if v is None:
            continue
        q[k] = [str(v)]
    new_query = urlencode({k: v[0] for k, v in q.items() if v and v[0] != ""}, doseq=False)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


def extract_filters(seed_url: str) -> dict:
    """
    Übernimmt Filter aus der Seed-URL, z.B. location_country=200, job_type=..., category=...
    Ignoriert leere Werte.
    """
    u = urlparse(seed_url)
    q = parse_qs(u.query)
    return {k: v[0] for k, v in q.items() if v and v[0] not in ("", "-1")}


def discover_listing_url(seed_url: str) -> str:
    """
    Tribepad hat meist einen "View Jobs" Listing-Pfad.
    """
    u = urlparse(seed_url)
    base = f"{u.scheme}://{u.netloc}"
    return urljoin(base, "/v2/view%20jobs")


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


# -----------------------------
# Parsing: Listing & Detail
# -----------------------------

@dataclass
class ListingHit:
    job_url: str
    record_id: str
    job_reference: Optional[str]
    posted_date: Optional[str]


def parse_listing_page(html: str, base_url: str) -> List[ListingHit]:
    """
    Extrahiert Job-Links aus einer Tribepad Listing-Seite.
    Die Links gehen i.d.R. auf /members/modules/job/detail.php?record=XYZ
    """
    soup = BeautifulSoup(html, "html.parser")

    hits: List[ListingHit] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "members/modules/job/detail.php" not in href:
            continue

        abs_url = urljoin(base_url, href)
        m = RECORD_RE.search(abs_url)
        if not m:
            continue
        record_id = m.group(1)

        text = " ".join(a.get_text(" ", strip=True).split())

        job_ref = None
        mref = JOB_REF_RE.search(text)
        if mref:
            job_ref = mref.group(0)

        dates = DATE_RE.findall(text)
        posted = dates[-1] if dates else None

        hits.append(ListingHit(
            job_url=abs_url,
            record_id=record_id,
            job_reference=job_ref,
            posted_date=posted
        ))

    # Dedup nach record_id
    uniq: Dict[str, ListingHit] = {}
    for h in hits:
        uniq[h.record_id] = h
    return list(uniq.values())


def parse_detail_page(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extrahiert (job_title, location, job_reference) aus der Detailseite.
    """
    soup = BeautifulSoup(html, "html.parser")

    title = None
    h1 = soup.find(["h1", "h2"])
    if h1:
        title = h1.get_text(" ", strip=True)

    location = None
    text = soup.get_text("\n", strip=True)
    mloc = re.search(r"\bLocation:\s*\n\s*([^\n]+)", text)
    if mloc:
        location = mloc.group(1).strip()

    job_ref = None
    mref = re.search(r"\bJob Reference\s+([^\s]+/TP/\d+/\d+)\b", text, flags=re.IGNORECASE)
    if mref:
        job_ref = mref.group(1).strip()

    return title, location, job_ref


# -----------------------------
# Scraper
# -----------------------------

def scrape_tribepad_company(
    company: str,
    careers_seed_url: str,
    max_pages: int = 20,
    sleep_s: float = 0.2
) -> List[Dict]:
    """
    1) Versucht zuerst die Seed-URL (z.B. /v2/abljobs?location_country=200).
       Wenn daraus 0 Treffer kommen, nutzt es /v2/view%20jobs, übernimmt aber die Filter aus der Seed-URL.
    2) Pagination über ?page=1..N
    3) Pro Job: Detailseite holen -> Title/Location
    """
    session = get_session()

    # Seed abrufen
    seed_resp = session.get(careers_seed_url, timeout=30)
    seed_resp.raise_for_status()
    seed_hits = parse_listing_page(seed_resp.text, base_url=careers_seed_url)

    # Filter aus Seed übernehmen (z.B. location_country=200)
    filters = extract_filters(careers_seed_url)

    # listing_base bestimmen
    if seed_hits:
        # Seed hat direkt Treffer -> wir können auf Seed-Basis paginieren
        listing_base = urlunparse(urlparse(careers_seed_url)._replace(query=""))  # ohne query
        listing_base = with_query(listing_base, **filters)
    else:
        # Fallback auf /v2/view%20jobs, aber MIT den Filtern
        listing_base = discover_listing_url(careers_seed_url)
        listing_base = with_query(listing_base, **filters)

    results: List[Dict] = []
    seen_records = set()

    # Pagination: hier ist page DEFINIERT
    for page in range(1, max_pages + 1):
        page_url = with_query(listing_base, page=page)
        resp = session.get(page_url, timeout=30)
        resp.raise_for_status()

        page_hits = parse_listing_page(resp.text, base_url=page_url)
        if not page_hits:
            break

        for h in page_hits:
            if h.record_id in seen_records:
                continue
            seen_records.add(h.record_id)

            d = session.get(h.job_url, timeout=30)
            d.raise_for_status()
            job_title, location, job_ref_detail = parse_detail_page(d.text)

            job_reference = h.job_reference or job_ref_detail
            posted_date = h.posted_date

            results.append({
                "company": company,
                "job_title": job_title,
                "location": location,
                "job_id": job_reference or h.record_id,  # fallback: record_id
                "posted_date": posted_date,
                "job_url": h.job_url,
                "source": "tribepad",
                "careers_url": page_url,
            })

            time.sleep(sleep_s)

    return results


# -----------------------------
# CLI / Test
# -----------------------------

if __name__ == "__main__":
    seeds = [
        ("ABL Group", "https://abl-group.tribepad.com/v2/abljobs?keywords=&location_country=200&job_type=&category="),
        ("Longitude", "https://abl-group.tribepad.com/v2/longitudejobs?keywords=&location_country=200&job_type=&category="),
    ]

    all_jobs: List[Dict] = []
    for company, url in seeds:
        jobs = scrape_tribepad_company(company=company, careers_seed_url=url, max_pages=20)
        print(f"{company}: {len(jobs)} jobs")
        all_jobs.extend(jobs)

    if all_jobs:
        print(all_jobs[0])