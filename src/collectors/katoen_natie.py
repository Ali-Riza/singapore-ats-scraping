from __future__ import annotations

import json
import re
from typing import Any, Dict, List
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CollectResult, CompanyItem, JobRecord

BASE_URL = "https://www.careers-page.com"
EXPECTED_HOST = "www.careers-page.com"
DEFAULT_SLUG = "katoennatiesingapore"
PAGE_SIZE = 50
MAX_PAGES = 20
REQUEST_TIMEOUT = 30
ORDERING = "-is_pinned_in_career_page,-last_published_at"

# The portal hosts many employers; only these names belong to this client.
ACCEPTED_HIRING_NAMES = {
    "katoen natie singapore",
    "katoen natie singapore (jurong) pte ltd",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0 Safari/537.36"
)


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\xa0", " ").split()).strip()


def _looks_singapore(location: str) -> bool:
    return "singapore" in (location or "").casefold()


def _iso_date(value: Any) -> str:
    """The schema carries ISO timestamps; keep only the date part."""
    match = re.match(r"^(\d{4}-\d{2}-\d{2})", _clean(value))
    return match.group(1) if match else ""


def _html_to_text(html: Any) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(str(html), "html.parser")
    for item in soup.select("li"):
        item.insert(0, "· ")
    lines = [_clean(line) for line in soup.get_text("\n").splitlines()]
    return "\n".join(line for line in lines if line)


def _slug_from_url(careers_url: str) -> str:
    """The API is keyed by the client slug, which is the careers URL's first path part."""
    parts = [p for p in urlsplit(careers_url).path.split("/") if p]
    return parts[0] if parts else DEFAULT_SLUG


def _find_job_postings(value: Any) -> List[Dict[str, Any]]:
    """Walk a JSON-LD document and collect every JobPosting node."""
    postings: List[Dict[str, Any]] = []
    if isinstance(value, list):
        for item in value:
            postings.extend(_find_job_postings(item))
        return postings
    if not isinstance(value, dict):
        return postings

    schema_type = value.get("@type")
    if schema_type == "JobPosting" or (
        isinstance(schema_type, list) and "JobPosting" in schema_type
    ):
        postings.append(value)

    for child in value.values():
        if isinstance(child, (dict, list)):
            postings.extend(_find_job_postings(child))
    return postings


def _extract_job_schema(html: str) -> Dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.select('script[type="application/ld+json"]'):
        content = script.string or script.get_text()
        if not content or not content.strip():
            continue
        try:
            document = json.loads(content)
        except json.JSONDecodeError:
            continue
        postings = _find_job_postings(document)
        if postings:
            return postings[0]
    return None


def _extract_from_html(html: str) -> Dict[str, str]:
    """Read title/location/department from the page banner.

    Not every posting ships a JSON-LD block, but all of them render the banner,
    so this keeps schema-less jobs in the result set.
    """
    soup = BeautifulSoup(html, "html.parser")
    banner = soup.select_one(".banner")
    if banner is None:
        return {}

    title = _clean(banner.select_one("h1").get_text(" ")) if banner.select_one("h1") else ""
    if not title:
        return {}

    location = ""
    department = ""
    for span in banner.select("span"):
        text = _clean(span.get_text(" "))
        if not text:
            continue
        # The banner marks location and department only by their icon class.
        if span.select_one("i.fa-map-marker-alt") and not location:
            location = text
        elif span.select_one("i.fa-building") and not department:
            department = text

    description = ""
    body = soup.select_one("#job-description") or soup.select_one("main")
    if body is not None:
        for tag in body(["script", "style"]):
            tag.decompose()
        description = _html_to_text(str(body))

    return {
        "title": title,
        "location": location,
        "department": department,
        "description": description,
    }


def _normalize_employment_type(value: Any) -> str:
    if isinstance(value, list):
        parts = [_normalize_employment_type(item) for item in value]
        return ", ".join(part for part in parts if part)
    text = _clean(value)
    return text.replace("_", " ").capitalize() if text else ""


def _extract_experience(value: Any) -> str:
    if isinstance(value, list):
        parts = [_extract_experience(item) for item in value]
        return ", ".join(part for part in parts if part)
    if isinstance(value, dict):
        for key in ("name", "value", "description", "monthsOfExperience"):
            result = _clean(value.get(key))
            if result:
                return result
        return ""
    return _clean(value)


def _extract_location(job_schema: Dict[str, Any]) -> str:
    locations = job_schema.get("jobLocation")
    if not locations:
        return ""
    if not isinstance(locations, list):
        locations = [locations]

    parts: List[str] = []
    seen: set[str] = set()
    for location in locations:
        if not isinstance(location, dict):
            continue
        address = location.get("address")
        if not isinstance(address, dict):
            continue
        for key in ("addressLocality", "addressRegion", "addressCountry"):
            value = address.get(key)
            if isinstance(value, dict):
                value = value.get("name") or value.get("value")
            value = _clean(value)
            if value and value.casefold() not in seen:
                seen.add(value.casefold())
                parts.append(value)
    return ", ".join(parts)


class KatoenNatieCollector(BaseCollector):
    """Collector for the careers-page.com portal used by Katoen Natie Singapore.

    The listing comes from the portal's paginated JSON API (with a fallback to
    the job links rendered into the career page), and each posting's fields are
    read from the JSON-LD block on its detail page.
    """

    name = "katoen_natie"

    def _check_host(self, url: str) -> None:
        parts = urlsplit(url)
        if parts.scheme != "https" or parts.hostname != EXPECTED_HOST:
            raise ValueError(f"Unexpected URL: {url}")

    def _get(self, session: requests.Session, url: str, meta: Dict[str, Any], **kwargs):
        self._check_host(url)
        response = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
        meta["status"].append(response.status_code)
        response.raise_for_status()
        return response

    def _api_jobs(
        self, session: requests.Session, slug: str, meta: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        jobs: List[Dict[str, Any]] = []
        seen: set[str] = set()
        api_url = f"{BASE_URL}/api/v1.0/c/{slug}/jobs/"

        for page in range(1, MAX_PAGES + 1):
            response = self._get(
                session,
                api_url,
                meta,
                params={"page_size": PAGE_SIZE, "page": page, "ordering": ORDERING},
                headers={"Accept": "application/json"},
            )
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError("Unexpected API response")

            for job in data.get("results") or []:
                if not isinstance(job, dict):
                    continue
                job_hash = _clean(job.get("hash"))
                if job_hash and job_hash not in seen:
                    seen.add(job_hash)
                    jobs.append(job)

            meta["pages"] = page
            if not data.get("next"):
                break

        return jobs

    def _hashes_from_html(self, html: str, slug: str) -> List[Dict[str, Any]]:
        pattern = rf'href=["\']/{re.escape(slug)}/job/([^/"\'?#]+)'
        jobs: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for job_hash in re.findall(pattern, html, flags=re.IGNORECASE):
            if job_hash not in seen:
                seen.add(job_hash)
                jobs.append({"hash": job_hash})
        return jobs

    def _build_raw_job(
        self, api_job: Dict[str, Any], job_schema: Dict[str, Any], detail_url: str
    ) -> Dict[str, Any] | None:
        hiring = job_schema.get("hiringOrganization")
        if not isinstance(hiring, dict):
            return None

        # The portal is multi-tenant, so a foreign employer means a foreign job.
        hiring_name = _clean(hiring.get("name"))
        if hiring_name.casefold() not in ACCEPTED_HIRING_NAMES:
            return None

        title = _clean(job_schema.get("title"))
        if not title:
            return None

        return self._raw_from_schema(api_job, job_schema, detail_url, title)

    def _raw_from_schema(
        self,
        api_job: Dict[str, Any],
        job_schema: Dict[str, Any],
        detail_url: str,
        title: str,
    ) -> Dict[str, Any]:

        department = _clean(api_job.get("organization_name")) or _clean(
            job_schema.get("occupationalCategory")
        )

        return {
            "job_id": _clean(api_job.get("hash")),
            "job_title": title,
            "job_url": detail_url,
            "posted_date": _iso_date(job_schema.get("datePosted")),
            "closing_date": _iso_date(job_schema.get("validThrough")),
            "specialization": department,
            "employment_type": _normalize_employment_type(
                job_schema.get("employmentType")
            ),
            "minimum_experience": _extract_experience(
                job_schema.get("experienceRequirements")
            ),
            "location": _extract_location(job_schema),
            "description": _html_to_text(job_schema.get("description")),
        }

    def _raw_from_html(
        self, api_job: Dict[str, Any], html: str, detail_url: str
    ) -> Dict[str, Any] | None:
        parsed = _extract_from_html(html)
        title = _clean(api_job.get("position_name")) or parsed.get("title", "")
        if not title:
            return None

        return {
            "job_id": _clean(api_job.get("hash")),
            "job_title": title,
            "job_url": detail_url,
            # Neither the API nor these pages publish a posting date.
            "posted_date": "",
            "closing_date": "",
            "specialization": _clean(api_job.get("organization_name"))
            or parsed.get("department", ""),
            "employment_type": "",
            "minimum_experience": "",
            "location": _clean(api_job.get("location_display"))
            or parsed.get("location", ""),
            "description": _html_to_text(api_job.get("description"))
            or parsed.get("description", ""),
        }

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "status": [],
            "total_raw": 0,
            "skipped": 0,
            "html_fallback": 0,
            "non_singapore": 0,
        }

        try:
            careers_url = (company.careers_url or "").strip() or (
                f"{BASE_URL}/{DEFAULT_SLUG}"
            )
            slug = _slug_from_url(careers_url)
            meta["slug"] = slug

            with requests.Session() as session:
                session.headers.update(
                    {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
                )

                listing_html = self._get(
                    session,
                    careers_url,
                    meta,
                    headers={"Accept": "text/html,*/*;q=0.8"},
                ).text

                try:
                    api_jobs = self._api_jobs(session, slug, meta)
                except (requests.RequestException, ValueError):
                    # Fall back to the links the career page renders server-side.
                    meta["api_fallback"] = True
                    api_jobs = self._hashes_from_html(listing_html, slug)

                for api_job in api_jobs:
                    job_hash = _clean(api_job.get("hash"))
                    if not job_hash:
                        continue
                    detail_url = f"{BASE_URL}/{slug}/job/{job_hash}"

                    try:
                        detail_html = self._get(
                            session,
                            detail_url,
                            meta,
                            headers={"Accept": "text/html,*/*;q=0.8"},
                        ).text
                    except requests.HTTPError as exc:
                        # Postings closed between listing and fetch are dropped.
                        if exc.response is not None and exc.response.status_code in {
                            404,
                            410,
                        }:
                            meta["skipped"] += 1
                            continue
                        raise

                    job_schema = _extract_job_schema(detail_html)
                    if job_schema:
                        raw = self._build_raw_job(api_job, job_schema, detail_url)
                    else:
                        # The portal omits JSON-LD on some postings; the URL slug
                        # already scopes these to this client's career page.
                        meta["html_fallback"] += 1
                        raw = self._raw_from_html(api_job, detail_html, detail_url)

                    if raw is None:
                        meta["skipped"] += 1
                        continue

                    # The client's portal also advertises regional roles.
                    if not _looks_singapore(raw["location"]):
                        meta["non_singapore"] += 1
                        continue

                    raw_jobs.append(raw)

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

            records.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=_clean(raw.get("location")) or "Singapore",
                    job_id=_clean(raw.get("job_id")),
                    posted_date=_clean(raw.get("posted_date")),
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return records
