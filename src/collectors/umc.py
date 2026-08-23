from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CollectResult, CompanyItem, JobRecord

# The board's certificate chain omits a Subject Key Identifier, which certifi's
# stricter OpenSSL path rejects; the OS trust store accepts it (as curl does).
try:
    import truststore

    truststore.inject_into_ssl()
except Exception:  # pragma: no cover - falls back to certifi
    pass

DEFAULT_CAREERS_URL = "https://sgcareers.umc.com/jobsearch.php"
DETAIL_PATH = "jobin.php"
TIMEOUT = 30
DETAIL_WORKERS = 6

# The board renders the whole result set in one page (no pager), so the listing
# rows are simply every <li id="myli...">. Columns are positional.
LISTING_COLUMNS = ("job_title", "job_function", "experience", "education", "location", "updated")

# Detail pages label every value with a fullwidth colon, e.g. "Job Function：".
# Map the labels we care about onto snake_case keys; anything else is kept as-is.
DETAIL_FIELDS: Dict[str, str] = {
    "Type of employment": "employment_type",
    "Job Title": "job_title",
    "Number of Vacancies": "vacancies",
    "Job Function": "job_function",
    "Description": "description",
    "Job Description": "job_description",
    "Location": "location",
    "Site": "site",
    "Management Responsibility": "management_responsibility",
    "Business travel": "business_travel",
    "Working Hours": "working_hours",
    "Shift Pattern": "shift_pattern",
    "Leave entitlement": "leave_entitlement",
    "Date available": "date_available",
    "Years of Experience": "experience",
    "Education": "education",
    "Major": "major",
    "Other requirement": "other_requirement",
    "Updating Date": "updated",
    "Contact Person": "contact_person",
    "How to Apply": "how_to_apply",
}

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\xa0", " ").replace("　", " ").split()).strip()


def _clean_block(value: Any) -> str:
    """Collapse horizontal whitespace but keep the paragraph breaks of the description."""
    text = str(value or "").replace("\xa0", " ").replace("　", " ")
    lines: List[str] = []
    for line in text.split("\n"):
        line = " ".join(line.split()).strip()
        if line and (not lines or lines[-1] != line):
            lines.append(line)
    return "\n".join(lines)


def _mid(url: str) -> str:
    return _clean(parse_qs(urlparse(url).query).get("mid", [""])[0])


def _iso_date(value: Any) -> str:
    """The board already prints ISO dates ('2026-02-25'); anything else is dropped."""
    match = _DATE_RE.search(_clean(value))
    return match.group(0) if match else ""


class UmcCollector(BaseCollector):
    """Collector for UMC's Singapore PHP job board (sgcareers.umc.com).

    The listing page renders every vacancy at once, so one GET yields all rows.
    Each row links to a jobin.php detail page whose 'About the Role' block is a
    flat list of label/value pairs in English; those are fetched in parallel and
    merged over the listing row, which stays even if its detail fetch fails.
    """

    name = "umc"

    def _session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": "Mozilla/5.0",
            }
        )
        return session

    def _parse_listing(self, html: str, base_url: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        listings: List[Dict[str, Any]] = []

        for row in soup.select('li[id^="myli"]'):
            link = row.find("a", href=True)
            if not link:
                continue
            job_url = urljoin(base_url, link["href"])
            if DETAIL_PATH not in job_url:
                continue
            job_id = _mid(job_url)
            if not job_id:
                continue

            container = link.find_parent("div").parent if link.find_parent("div") else row
            cells = [c for c in container.find_all("div", recursive=False)]
            values = [_clean(cell.get_text(" ", strip=True)) for cell in cells]

            listing: Dict[str, Any] = {"job_id": job_id, "job_url": job_url}
            for key, value in zip(LISTING_COLUMNS, values):
                listing[key] = value
            listing["updated"] = _iso_date(listing.get("updated"))

            if listing.get("job_title"):
                listings.append(listing)

        deduped: Dict[str, Dict[str, Any]] = {}
        for listing in listings:
            deduped.setdefault(listing["job_id"], listing)
        return list(deduped.values())

    def _parse_detail(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        detail: Dict[str, Any] = {}

        # Both the 'About the Role' block and the 'How to Apply' box use the same
        # <span>Label：</span><value> shape, so one sweep covers them.
        for span in soup.select("#jobin span, #wayjob span"):
            label = _clean(span.get_text(" ", strip=True)).rstrip("：:").strip()
            key = DETAIL_FIELDS.get(label)
            if not key:
                continue
            holder = span.parent
            if holder is None:
                continue
            # The value is everything in the wrapper except the label span itself.
            value_nodes = [node for node in holder.find_all(recursive=False) if node is not span]
            if value_nodes:
                text = "\n".join(node.get_text("\n", strip=True) for node in value_nodes)
            else:
                text = holder.get_text("\n", strip=True)
                text = text.replace(span.get_text(" ", strip=True), "", 1)
            value = _clean_block(text)
            if value and not detail.get(key):
                detail[key] = value

        if detail.get("updated"):
            detail["updated"] = _iso_date(detail["updated"])
        return detail

    def _enrich(self, session: requests.Session, listing: Dict[str, Any]) -> Dict[str, Any]:
        response = session.get(listing["job_url"], timeout=TIMEOUT)
        response.raise_for_status()
        response.encoding = response.encoding or "utf-8"
        detail = self._parse_detail(response.text)

        enriched = dict(listing)
        # Detail values win, but never let a blank detail field erase a listing value.
        for key, value in detail.items():
            if value:
                enriched[key] = value
        enriched["detail_available"] = bool(detail)
        return enriched

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"total_raw": 0, "detail_errors": 0}

        try:
            careers_url = (company.careers_url or "").strip() or DEFAULT_CAREERS_URL
            session = self._session()

            response = session.get(careers_url, timeout=TIMEOUT)
            meta["status"] = response.status_code
            response.raise_for_status()
            response.encoding = response.encoding or "utf-8"

            listings = self._parse_listing(response.text, response.url)
            meta["listing_count"] = len(listings)

            if listings:
                with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
                    futures = {
                        executor.submit(self._enrich, session, listing): listing
                        for listing in listings
                    }
                    for future in as_completed(futures):
                        listing = futures[future]
                        try:
                            raw_jobs.append(future.result())
                        except Exception:
                            # A detail hiccup must not drop the listing row.
                            meta["detail_errors"] += 1
                            raw_jobs.append({**listing, "detail_available": False})

            raw_jobs.sort(key=lambda job: job.get("job_title", ""))
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
            title = _clean(raw.get("job_title"))
            job_url = _clean(raw.get("job_url"))
            if not title or not job_url:
                continue

            # 'Site' is the concrete campus (Pasir Ris); 'Location' the country.
            location = _clean(raw.get("location")) or _clean(raw.get("site"))
            site = _clean(raw.get("site"))
            if site and site.casefold() not in location.casefold():
                location = f"{location}, {site}" if location else site

            records.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=location or "Singapore",
                    job_id=_clean(raw.get("job_id")),
                    posted_date=_iso_date(raw.get("updated")),
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return records
