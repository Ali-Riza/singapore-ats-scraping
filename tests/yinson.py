#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


# -----------------------------
# Config
# -----------------------------

BASE = "https://careers.yinson.com"
SEARCH_URL = f"{BASE}/search/"
DEFAULT_QUERY = "singapore"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9,de;q=0.8",
}


# -----------------------------
# Helpers
# -----------------------------

def safe_get(session: requests.Session, url: str, *, timeout: int = 30, max_retries: int = 3) -> str:
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_exc = e
            # small backoff
            time.sleep(0.6 * attempt)
    raise RuntimeError(f"GET failed after {max_retries} retries: {url}\nLast error: {last_exc}")


def normalize_url(url: str) -> str:
    """
    Normalize to absolute + remove fragments; keep trailing slash as in site.
    """
    abs_url = url if url.startswith("http") else urljoin(BASE, url)
    parsed = urlparse(abs_url)
    cleaned = parsed._replace(fragment="").geturl()
    return cleaned


def extract_job_id_from_url(job_url: str) -> Optional[str]:
    """
    Yinson detail URLs look like:
      https://careers.yinson.com/job/Singapore-Senior-Contract-Specialist/1332939755/
    """
    m = re.search(r"/(\d{6,})/?$", job_url)
    return m.group(1) if m else None


def parse_posted_date_from_detail_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    # 1) Best: schema.org meta itemprop
    meta = soup.select_one('meta[itemprop="datePosted"]')
    if meta and meta.get("content"):
        raw = meta["content"].strip()
        # Example: "Fri Dec 12 00:00:00 UTC 2025"
        for fmt in ("%a %b %d %H:%M:%S %Z %Y", "%a %b %d %H:%M:%S %z %Y"):
            try:
                dt = datetime.strptime(raw, fmt)
                return dt.date().isoformat()
            except ValueError:
                pass

    # 2) Fallback: visible date text
    # Example: <span data-careersite-propertyid="date">12 Dec 2025</span>
    el = soup.select_one('[data-careersite-propertyid="date"]')
    if el:
        txt = " ".join(el.get_text(" ", strip=True).split())
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                dt = datetime.strptime(txt, fmt)
                return dt.date().isoformat()
            except ValueError:
                pass

    return None


def parse_title_from_detail_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    # <span itemprop="title" data-careersite-propertyid="title">Senior Contract Specialist</span>
    el = soup.select_one('[data-careersite-propertyid="title"]')
    if el:
        return el.get_text(" ", strip=True) or None
    # fallback: <title>Senior Contract Specialist Job Details | Yinson</title>
    t = soup.title.get_text(strip=True) if soup.title else ""
    if t:
        return t.replace(" Job Details | Yinson", "").strip()
    return None


def parse_location_from_detail_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    # In schema.org: <meta itemprop="streetAddress" content="Singapore, SG">
    loc_meta = soup.select_one('meta[itemprop="streetAddress"]')
    if loc_meta and loc_meta.get("content"):
        return loc_meta["content"].strip()
    return None


def extract_job_urls_from_search_html(html: str) -> list[str]:
    """
    Robust extraction:
    - Find all anchor hrefs containing /job/
    - Also handle canonical links etc.
    - Deduplicate
    """
    soup = BeautifulSoup(html, "html.parser")

    urls: set[str] = set()

    # 1) anchors
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if "/job/" in href:
            urls.add(normalize_url(href))

    # 2) regex fallback over raw HTML (sometimes links are embedded in scripts)
    for m in re.finditer(r'href="([^"]*?/job/[^"]+)"', html):
        urls.add(normalize_url(m.group(1)))

    # Filter to only real detail-like urls that end with digits (job id)
    cleaned: list[str] = []
    for u in sorted(urls):
        if re.search(r"/job/.*?/\d{6,}/?$", u):
            # ensure trailing slash (nice consistency)
            if not u.endswith("/"):
                u += "/"
            cleaned.append(u)

    # Final dedupe in list order
    seen = set()
    out = []
    for u in cleaned:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# -----------------------------
# Main scraping logic
# -----------------------------

def build_search_url(query: str) -> str:
    # Simple query usage: /search/?q=singapore
    return f"{SEARCH_URL}?q={requests.utils.quote(query)}"


def scrape_yinson(query: str, *, max_jobs: Optional[int] = None, delay: float = 0.0) -> list[dict]:
    with requests.Session() as session:
        careers_url = build_search_url(query)
        search_html = safe_get(session, careers_url)

        job_urls = extract_job_urls_from_search_html(search_html)
        if max_jobs is not None:
            job_urls = job_urls[:max_jobs]

        results: list[dict] = []
        for idx, job_url in enumerate(job_urls, start=1):
            if delay:
                time.sleep(delay)

            detail_html = safe_get(session, job_url)

            job_id = extract_job_id_from_url(job_url)
            title = parse_title_from_detail_html(detail_html)
            location = parse_location_from_detail_html(detail_html)
            posted_date = parse_posted_date_from_detail_html(detail_html)

            results.append({
                "company": "Yinson",
                "job_title": title,
                "location": location,
                "job_id": job_id,
                "posted_date": posted_date,
                "job_url": job_url,
                "source": "successfactors",
                "careers_url": careers_url,
            })

        return results


# -----------------------------
# CLI
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Scrape Yinson (SuccessFactors/jobs2web) jobs and include posted_date from detail pages.")
    ap.add_argument("--q", default=DEFAULT_QUERY, help="Search keyword, e.g. singapore")
    ap.add_argument("--max", type=int, default=None, help="Max number of jobs to fetch (debug)")
    ap.add_argument("--delay", type=float, default=0.0, help="Delay seconds between detail requests (politeness/avoid rate limit)")
    args = ap.parse_args()

    jobs = scrape_yinson(args.q, max_jobs=args.max, delay=args.delay)

    for j in jobs:
        print(j)

    print(f"\nTOTAL_RESULTS={len(jobs)}")


if __name__ == "__main__":
    main()