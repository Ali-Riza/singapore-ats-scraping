from __future__ import annotations

import re
import unicodedata
from collections import deque
from typing import Any, Dict, List
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag

from src.collectors.base import BaseCollector
from src.core.models import CollectResult, CompanyItem, JobRecord

BASE_URL = "https://www.bertschi.com"
JOB_FINDER_PATH = "/en/career/job-finder"
TARGET_LOCATION = "Singapore"
REQUEST_TIMEOUT = 30
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def _clean(value: Tag | str | None) -> str:
    if value is None:
        return ""
    text = value.get_text(" ", strip=True) if isinstance(value, Tag) else value
    return re.sub(r"\s+", " ", text).strip()


def _normalized(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold().strip()


def _resolve_location_id(soup: BeautifulSoup, location: str) -> str:
    """Map a location name to the numeric value the site's city filter expects."""
    select = soup.select_one("select#edit-city")
    if select is None:
        raise ValueError("city filter '#edit-city' not found on the job finder page")

    options = {
        _normalized(_clean(option)): option.get("value", "")
        for option in select.select("option[value]")
        if _clean(option)
    }
    location_id = options.get(_normalized(location))
    if not location_id:
        raise ValueError(f"location {location!r} is not offered by the city filter")
    return location_id


def _with_city_filter(url: str, location_id: str) -> str:
    """Keep the selected city on pagination links, which drop the query string."""
    parts = urlsplit(url)
    query = parse_qsl(parts.query, keep_blank_values=True)
    if not any(key == "city[]" for key, _ in query):
        query.append(("city[]", location_id))
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query, doseq=True), parts.fragment)
    )


def _job_id_from_url(url: str) -> str:
    """The URL slug is the only stable identifier; the pages carry no node id."""
    return urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1]


def _parse_job_cards(soup: BeautifulSoup, location: str) -> List[Dict[str, Any]]:
    """Extract job summaries for the requested location from one result page."""
    cards: List[Dict[str, Any]] = []
    for card in soup.select("a.job-position.teaser[href]"):
        href = card.get("href")
        if not href:
            continue

        locality = _clean(card.select_one(".locality"))
        city, separator, country = locality.partition("|")
        city = city.strip()
        country = country.strip() if separator else ""

        # The server-side filter already narrows this down; the check keeps
        # unrelated rows out if the site's query handling ever changes.
        if city and _normalized(city) != _normalized(location):
            continue

        url = urljoin(BASE_URL, href)
        cards.append(
            {
                "job_id": _job_id_from_url(url),
                "title": _clean(card.select_one(".job-title")),
                "city": city,
                "country": country,
                "job_url": url,
            }
        )
    return cards


def _field_text(soup: BeautifulSoup, selector: str) -> str:
    """Read a Drupal field's value without its visible label."""
    field = soup.select_one(selector)
    if field is None:
        return ""
    item = field.select_one(":scope > .field--item")
    return _clean(item or field)


def _field_items(soup: BeautifulSoup, selector: str) -> List[str]:
    field = soup.select_one(selector)
    if field is None:
        return []
    items = [_clean(item) for item in field.select("li") if _clean(item)]
    if items:
        return items
    text = _field_text(soup, selector)
    return [text] if text else []


def _last_updated(soup: BeautifulSoup) -> str:
    """Read the relative 'last updated' value from the job metadata row."""
    for item in soup.select(".job-meta-row .item"):
        label = _clean(item.select_one(".desc"))
        if "aktualisierung" in _normalized(label) or "update" in _normalized(label):
            return _clean(item.select_one(".value"))
    return ""


class BertschiCollector(BaseCollector):
    """Collector for the Drupal-based Bertschi job finder.

    The finder renders every posting server-side and filters by a numeric city
    id, so one filtered request plus its pagination covers all Singapore jobs.
    Detail pages are fetched to enrich each card with description fields.
    """

    name = "bertschi"

    max_pages = 20

    def _get_soup(
        self,
        session: requests.Session,
        url: str,
        meta: Dict[str, Any],
        *,
        params: Dict[str, str] | None = None,
    ) -> tuple[BeautifulSoup, str]:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        meta["status"].append(response.status_code)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser"), response.url

    def _crawl_result_pages(
        self,
        session: requests.Session,
        finder_url: str,
        location_id: str,
        meta: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        first_soup, first_url = self._get_soup(
            session, finder_url, meta, params={"city[]": location_id}
        )
        cached = {first_url: first_soup}
        queued: deque[str] = deque([first_url])
        visited: set[str] = set()
        jobs_by_url: Dict[str, Dict[str, Any]] = {}
        expected_path = urlsplit(finder_url).path

        while queued and len(visited) < self.max_pages:
            page_url = queued.popleft()
            if page_url in visited:
                continue
            visited.add(page_url)

            if page_url in cached:
                soup = cached.pop(page_url)
            else:
                soup, page_url = self._get_soup(session, page_url, meta)

            for card in _parse_job_cards(soup, TARGET_LOCATION):
                jobs_by_url[card["job_url"]] = card

            for link in soup.select("nav.pager a[href], .pager a[href]"):
                next_url = _with_city_filter(
                    urljoin(page_url, link.get("href", "")), location_id
                )
                parts = urlsplit(next_url)
                if (
                    parts.netloc == urlsplit(BASE_URL).netloc
                    and parts.path == expected_path
                    and next_url not in visited
                ):
                    queued.append(next_url)

        meta["pages"] = len(visited)
        return list(jobs_by_url.values())

    def _enrich(
        self,
        session: requests.Session,
        card: Dict[str, Any],
        meta: Dict[str, Any],
    ) -> Dict[str, Any]:
        soup, _ = self._get_soup(session, card["job_url"], meta)
        article = soup.select_one("article.job-position.full")
        if article is None:
            return card

        enriched = dict(card)
        enriched.update(
            {
                "title": _clean(article.select_one("h1.job-title")) or card["title"],
                "city": _field_text(soup, ".field--name-field-job-city") or card["city"],
                "country": _field_text(soup, ".field--name-field-job-country")
                or card["country"],
                "last_updated": _last_updated(soup),
                "heading": _field_text(soup, ".field--name-field-job-heading"),
                "description": _field_text(soup, ".field--name-field-job-description"),
                "responsibilities": _field_items(soup, ".field--name-field-job-role"),
                "qualifications": _field_items(
                    soup, ".field--name-field-job-qualifications"
                ),
                "offer": _field_items(soup, ".field--name-field-job-offer"),
                "contact_name": _clean(soup.select_one(".contact-person .name")),
                "contact_role": _clean(soup.select_one(".contact-person .function")),
            }
        )
        return enriched

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "status": [], "total_raw": 0}
        try:
            finder_url = urljoin(company.careers_url or BASE_URL, JOB_FINDER_PATH)
            with requests.Session() as session:
                session.headers.update({"User-Agent": USER_AGENT})

                finder_soup, _ = self._get_soup(session, finder_url, meta)
                location_id = _resolve_location_id(finder_soup, TARGET_LOCATION)
                meta["location_id"] = location_id

                cards = self._crawl_result_pages(session, finder_url, location_id, meta)

                for card in cards:
                    # A detail-page hiccup must not drop the card itself.
                    try:
                        raw_jobs.append(self._enrich(session, card, meta))
                    except requests.RequestException:
                        raw_jobs.append(card)

            meta["total_raw"] = len(raw_jobs)
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )
        except Exception as exc:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(exc),
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        records: List[JobRecord] = []
        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue
            title = str(raw.get("title") or "").strip()
            job_url = str(raw.get("job_url") or "").strip()
            if not title or not job_url:
                continue

            city = str(raw.get("city") or "").strip()
            country = str(raw.get("country") or "").strip()
            location = ", ".join(part for part in (city, country) if part) or TARGET_LOCATION

            records.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=location,
                    job_id=str(raw.get("job_id") or "").strip(),
                    # The site only exposes a relative age ("4 Wochen"), not a date.
                    posted_date="",
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return records
