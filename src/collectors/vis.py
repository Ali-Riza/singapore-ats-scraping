from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CollectResult, CompanyItem, JobRecord

DEFAULT_CAREERS_URL = "https://vis.applyourjobs.com/"
JOB_LIST_ENDPOINT = "default.aspx/GetJobList"
DETAIL_PATH = "jobdetails.aspx"
TIMEOUT = 30
DETAIL_WORKERS = 10

# The board pages at 20 rows in the browser, but the endpoint honours any window,
# so one oversized request returns the whole set. PagerLength reports the total.
PAGE_SIZE = 500

# The ASP.NET ScriptService wraps its payload in {"d": {...}} and hands back the
# actual rows as a JSON *string* under JsonData.
LISTING_FIELDS: Dict[str, str] = {
    "JOBID": "job_id",
    "REFERENCENUMBER": "reference_number",
    "JOBTITLE": "job_title",
    "JOBCONTAINER": "department",
    "JOBSPECIALIZATION": "job_function",
    "JOBSUBSPECIALIZATION": "job_sub_function",
    "JOBTYPE": "employment_type",
    "JOBLOCATION": "location",
    "JOBSUBLOCATION": "sub_location",
    "JOBCATEGORY": "category",
    "JOBQUALIFICATIONS": "qualification",
    "MINYEARSOFEXPERIENCE": "minimum_experience",
    "VACANCY": "vacancies",
    "OPENDATE": "open_date",
    "POSTINGDATE": "posting_date",
    "EXPIRYDATE": "expiry_date",
    "TIMEAGO": "time_ago",
    "MINSALARYRANGE": "salary_min",
    "MAXSALARYRANGE": "salary_max",
    "SALARYLEVEL": "salary_level",
    "SALARYDESCRIPTION": "salary_description",
    "JOBCODE": "job_code",
}

_ISO_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\xa0", " ").split()).strip()


def _clean_block(value: Any) -> str:
    """Collapse horizontal whitespace but keep the paragraph breaks of the description."""
    text = str(value or "").replace("\xa0", " ")
    lines: List[str] = []
    for line in text.split("\n"):
        line = " ".join(line.split()).strip()
        if line and (not lines or lines[-1] != line):
            lines.append(line)
    return "\n".join(lines)


def _iso_date(value: Any) -> str:
    """The API returns '2026-08-18T11:40:00'; keep just the date part."""
    text = _clean(value)
    if not text:
        return ""
    match = _ISO_RE.match(text)
    if match:
        return match.group(1)
    try:
        return datetime.strptime(text, "%d %b %Y").date().isoformat()
    except ValueError:
        return ""


class VisCollector(BaseCollector):
    """Collector for VIS Singapore on the applyourjobs.com ASP.NET portal.

    Unlike the sibling Jurong board, this tenant exposes the underlying
    ScriptService directly, so the listing needs no browser: one POST to
    GetJobList returns every vacancy with full metadata. Only the job
    description lives on the detail page, which is fetched over plain HTTP
    via ?ID=<JOBCODE> and merged in; a failing detail never drops its row.
    """

    name = "vis"

    def _session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "Mozilla/5.0",
                "X-Requested-With": "XMLHttpRequest",
            }
        )
        return session

    def _fetch_listings(
        self, session: requests.Session, base_url: str, meta: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        payload = {
            "srcArr": [],
            "sortingColumn": "",
            "sortDirection": "",
            "startCount": "0",
            "endCount": PAGE_SIZE,
        }
        response = session.post(
            urljoin(base_url, JOB_LIST_ENDPOINT),
            data=json.dumps(payload),
            timeout=TIMEOUT,
            headers={"Referer": base_url},
        )
        meta["status"] = response.status_code
        response.raise_for_status()

        envelope = response.json().get("d") or {}
        meta["reported_total"] = envelope.get("PagerLength")

        rows = json.loads(envelope.get("JsonData") or "[]")
        if not isinstance(rows, list):
            return []

        listings: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            listing: Dict[str, Any] = {}
            for source_key, target_key in LISTING_FIELDS.items():
                listing[target_key] = _clean(row.get(source_key))
            for key in ("open_date", "posting_date", "expiry_date"):
                listing[key] = _iso_date(listing.get(key))

            job_code = listing.get("job_code") or ""
            if not job_code:
                continue
            # JOBCODE arrives percent-encoded and is used verbatim as the ?ID= value.
            listing["job_url"] = urljoin(base_url, f"{DETAIL_PATH}?ID={job_code}")
            listings.append(listing)

        deduped: Dict[str, Dict[str, Any]] = {}
        for listing in listings:
            deduped.setdefault(listing["job_code"], listing)
        return list(deduped.values())

    def _parse_detail(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        detail: Dict[str, Any] = {}

        description = soup.select_one("#ctl00_body_tbJobDesc")
        if description:
            detail["description"] = _clean_block(description.get_text("\n", strip=True))

        # The <title> is "<job title> | <company>"; useful as a title cross-check.
        if soup.title:
            title = _clean(soup.title.get_text(" ", strip=True))
            detail["detail_title"] = title.split("|")[0].strip() if "|" in title else title

        return detail

    def _enrich(self, listing: Dict[str, Any]) -> Dict[str, Any]:
        # A fresh session per detail page on purpose: ASP.NET serialises requests
        # that share a session cookie, so reusing one here costs ~2.3x wall clock.
        response = requests.get(
            listing["job_url"],
            timeout=TIMEOUT,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": "Mozilla/5.0",
            },
        )
        response.raise_for_status()
        response.encoding = response.encoding or "utf-8"

        # A stale JOBCODE bounces to InvalidRequest.aspx instead of 404-ing.
        if "invalidrequest" in urlparse(response.url).path.lower():
            return {**listing, "detail_available": False}

        detail = self._parse_detail(response.text)
        enriched = dict(listing)
        for key, value in detail.items():
            if value:
                enriched[key] = value
        enriched["detail_available"] = bool(detail.get("description"))
        return enriched

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"total_raw": 0, "detail_errors": 0}

        try:
            careers_url = (company.careers_url or "").strip() or DEFAULT_CAREERS_URL
            if not careers_url.endswith("/"):
                careers_url += "/"
            session = self._session()

            listings = self._fetch_listings(session, careers_url, meta)
            meta["listing_count"] = len(listings)
            if meta.get("reported_total") not in (None, len(listings)):
                # Worth surfacing: the endpoint advertises a count we did not reach.
                meta["count_mismatch"] = True

            if listings:
                with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
                    futures = {
                        executor.submit(self._enrich, listing): listing
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

            location = _clean(raw.get("location"))
            sub_location = _clean(raw.get("sub_location"))
            # "Any" is the board's placeholder for "no sub-location", not a place.
            if sub_location.casefold() == "any":
                sub_location = ""
            if sub_location and sub_location.casefold() not in location.casefold():
                location = f"{location}, {sub_location}" if location else sub_location

            records.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=location or "Singapore",
                    # REFERENCENUMBER is the public-facing id; JOBID is the internal key.
                    job_id=_clean(raw.get("reference_number")) or _clean(raw.get("job_id")),
                    posted_date=_iso_date(raw.get("posting_date"))
                    or _iso_date(raw.get("open_date")),
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return records
