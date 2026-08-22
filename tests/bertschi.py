#!/usr/bin/env python3
"""Scrape public Bertschi job information for one configured location."""

from __future__ import annotations

import csv
import json
import re
import sys
import time
import unicodedata
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Change this value to the location you want to scrape.
LOCATION = "Singapore"

BASE_URL = "https://www.bertschi.com"
JOB_FINDER_URL = f"{BASE_URL}/en/career/job-finder"
OUTPUT_DIR = Path(__file__).resolve().parent / "data"
REQUEST_DELAY_SECONDS = 0.5
REQUEST_TIMEOUT = (10, 30)
USER_AGENT = "BertschiJobInfoScraper/1.0"


class ScraperError(RuntimeError):
    """Raised when the site structure does not match the scraper's assumptions."""


def clean_text(value: Tag | str | None) -> str:
    """Return whitespace-normalized text for a Beautiful Soup node or string."""
    if value is None:
        return ""
    text = value.get_text(" ", strip=True) if isinstance(value, Tag) else value
    return re.sub(r"\s+", " ", text).strip()


def normalized(value: str) -> str:
    """Normalize human-readable values for case-insensitive comparisons."""
    return unicodedata.normalize("NFKC", value).casefold().strip()


def filename_slug(value: str) -> str:
    """Create a portable filename component from a location name."""
    ascii_value = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    slug = re.sub(r"[^a-z0-9]+", "_", ascii_value).strip("_")
    return slug or "location"


def create_session() -> requests.Session:
    """Create an HTTP session with conservative retries."""
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
        }
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def get_soup(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, str] | None = None,
) -> tuple[BeautifulSoup, str]:
    """Fetch a page and return its parsed HTML plus the final URL."""
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser"), response.url


def resolve_location_id(soup: BeautifulSoup, location: str) -> str:
    """Resolve a location name to the numeric value used by the site filter."""
    select = soup.select_one("select#edit-city")
    if select is None:
        raise ScraperError("Der Standortfilter '#edit-city' wurde nicht gefunden.")

    options = {
        normalized(clean_text(option)): option.get("value", "")
        for option in select.select("option[value]")
        if clean_text(option)
    }
    location_id = options.get(normalized(location))
    if location_id:
        return location_id

    available = ", ".join(
        sorted(
            clean_text(option)
            for option in select.select("option")
            if clean_text(option)
        )
    )
    raise ScraperError(
        f"Standort {location!r} wurde nicht gefunden. Verfügbare Standorte: {available}"
    )


def ensure_location_filter(url: str, location_id: str) -> str:
    """Keep the selected city filter on pagination links."""
    parts = urlsplit(url)
    query = parse_qsl(parts.query, keep_blank_values=True)
    if not any(key == "city[]" for key, _ in query):
        query.append(("city[]", location_id))
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query, doseq=True), parts.fragment)
    )


def parse_job_cards(soup: BeautifulSoup, location: str) -> list[dict[str, str]]:
    """Extract matching job summaries from one result page."""
    cards: list[dict[str, str]] = []
    for card in soup.select("a.job-position.teaser[href]"):
        locality = clean_text(card.select_one(".locality"))
        city, separator, country = locality.partition("|")
        city = city.strip()
        country = country.strip() if separator else ""

        # The server-side filter should already do this; the check prevents
        # unrelated results if the site's query handling changes.
        if city and normalized(city) != normalized(location):
            continue

        href = card.get("href")
        if not href:
            continue
        cards.append(
            {
                "title": clean_text(card.select_one(".job-title")),
                "city": city,
                "country": country,
                "url": urljoin(BASE_URL, href),
            }
        )
    return cards


def crawl_result_pages(
    session: requests.Session,
    location: str,
    location_id: str,
) -> list[dict[str, str]]:
    """Collect job cards from all result pages for the selected location."""
    first_soup, first_url = get_soup(
        session,
        JOB_FINDER_URL,
        params={"city[]": location_id},
    )
    queued: deque[str] = deque([first_url])
    cached_pages = {first_url: first_soup}
    visited: set[str] = set()
    jobs_by_url: dict[str, dict[str, str]] = {}
    expected_path = urlsplit(JOB_FINDER_URL).path

    while queued:
        page_url = queued.popleft()
        if page_url in visited:
            continue
        visited.add(page_url)

        if page_url in cached_pages:
            soup = cached_pages.pop(page_url)
        else:
            time.sleep(REQUEST_DELAY_SECONDS)
            soup, page_url = get_soup(session, page_url)

        for card in parse_job_cards(soup, location):
            jobs_by_url[card["url"]] = card

        for link in soup.select("nav.pager a[href], .pager a[href]"):
            next_url = ensure_location_filter(
                urljoin(page_url, link.get("href", "")), location_id
            )
            parts = urlsplit(next_url)
            if (
                parts.netloc == urlsplit(BASE_URL).netloc
                and parts.path == expected_path
                and next_url not in visited
            ):
                queued.append(next_url)

    return list(jobs_by_url.values())


def field_text(soup: BeautifulSoup, selector: str) -> str:
    """Read a Drupal field's value without its visible label."""
    field = soup.select_one(selector)
    if field is None:
        return ""
    item = field.select_one(":scope > .field--item")
    return clean_text(item or field)


def field_items(soup: BeautifulSoup, selector: str) -> list[str]:
    """Read list items from a Drupal text field, with a text fallback."""
    field = soup.select_one(selector)
    if field is None:
        return []
    items = [clean_text(item) for item in field.select("li") if clean_text(item)]
    if items:
        return items
    text = field_text(soup, selector)
    return [text] if text else []


def updated_value(soup: BeautifulSoup) -> str:
    """Find the human-readable 'last updated' value in the job metadata."""
    for item in soup.select(".job-meta-row .item"):
        label = clean_text(item.select_one(".desc"))
        if "aktualisierung" in normalized(label):
            return clean_text(item.select_one(".value"))
    return ""


def parse_job_detail(
    soup: BeautifulSoup,
    card: dict[str, str],
    scraped_at: str,
) -> dict[str, Any]:
    """Extract the public information from a Bertschi job detail page."""
    article = soup.select_one("article.job-position.full")
    if article is None:
        raise ScraperError(f"Detailbereich fehlt: {card['url']}")

    title = clean_text(article.select_one("h1.job-title")) or card["title"]
    city = field_text(soup, ".field--name-field-job-city") or card["city"]
    country = field_text(soup, ".field--name-field-job-country") or card["country"]

    return {
        "title": title,
        "city": city,
        "country": country,
        "url": card["url"],
        "last_updated": updated_value(soup),
        "heading": field_text(soup, ".field--name-field-job-heading"),
        "description": field_text(soup, ".field--name-field-job-description"),
        "company_summary": clean_text(soup.select_one(".job-future-hint p")),
        "responsibilities": field_items(soup, ".field--name-field-job-role"),
        "qualifications": field_items(
            soup, ".field--name-field-job-qualifications"
        ),
        "offer": field_items(soup, ".field--name-field-job-offer"),
        "final_note": clean_text(soup.select_one(".job-final-hint")),
        "contact_name": clean_text(soup.select_one(".contact-person .name")),
        "contact_role": clean_text(soup.select_one(".contact-person .function")),
        "scraped_at_utc": scraped_at,
    }


def scrape_jobs(session: requests.Session, location: str) -> list[dict[str, Any]]:
    """Scrape all job details for one location."""
    finder_soup, _ = get_soup(session, JOB_FINDER_URL)
    location_id = resolve_location_id(finder_soup, location)
    print(f"Standort: {location} (interne ID: {location_id})")

    cards = crawl_result_pages(session, location, location_id)
    print(f"Gefundene Stellen: {len(cards)}")
    scraped_at = datetime.now(timezone.utc).isoformat()

    jobs: list[dict[str, Any]] = []
    for index, card in enumerate(cards, start=1):
        if index > 1:
            time.sleep(REQUEST_DELAY_SECONDS)
        print(f"[{index}/{len(cards)}] {card['title']}")
        soup, _ = get_soup(session, card["url"])
        job = parse_job_detail(soup, card, scraped_at)

        if normalized(job["city"]) != normalized(location):
            raise ScraperError(
                f"Unerwarteter Standort {job['city']!r} bei {card['url']}"
            )
        jobs.append(job)

    return jobs


def write_outputs(jobs: list[dict[str, Any]], location: str) -> tuple[Path, Path]:
    """Write the scraped jobs to JSON and CSV files."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    slug = filename_slug(location)
    json_path = OUTPUT_DIR / f"bertschi_jobs_{slug}.json"
    csv_path = OUTPUT_DIR / f"bertschi_jobs_{slug}.csv"

    json_path.write_text(
        json.dumps(jobs, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    columns = [
        "title",
        "city",
        "country",
        "url",
        "last_updated",
        "heading",
        "description",
        "company_summary",
        "responsibilities",
        "qualifications",
        "offer",
        "final_note",
        "contact_name",
        "contact_role",
        "scraped_at_utc",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for job in jobs:
            writer.writerow(
                {
                    key: " | ".join(value) if isinstance(value, list) else value
                    for key, value in job.items()
                }
            )

    return json_path, csv_path


def main() -> int:
    try:
        with create_session() as session:
            jobs = scrape_jobs(session, LOCATION)
        json_path, csv_path = write_outputs(jobs, LOCATION)
    except (requests.RequestException, ScraperError) as error:
        print(f"Fehler: {error}", file=sys.stderr)
        return 1

    print(f"JSON: {json_path}")
    print(f"CSV:  {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
