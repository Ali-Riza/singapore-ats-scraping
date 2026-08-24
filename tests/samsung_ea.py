#!/usr/bin/env python3
"""Collect public Samsung E&A jobs from the SmartRecruiters Posting API."""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


API_ROOT = "https://api.smartrecruiters.com/v1"
COMPANY_IDENTIFIER = "SamsungEnA"
CAREERS_URL = "https://www.samsungena-global.com/careers"
USER_AGENT = "SamsungEAJobCollector/1.0 (+public SmartRecruiters postings)"


class CollectorError(RuntimeError):
    """Raised when the collector cannot retrieve or validate API data."""


class _TextExtractor(HTMLParser):
    BLOCK_TAGS = {
        "address",
        "article",
        "aside",
        "blockquote",
        "br",
        "div",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "li",
        "ol",
        "p",
        "pre",
        "section",
        "table",
        "tr",
        "ul",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def html_to_text(value: str | None) -> str:
    if not value:
        return ""
    parser = _TextExtractor()
    parser.feed(value)
    parser.close()
    text = html.unescape("".join(parser.parts)).replace("\xa0", " ")
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "job"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def nested_label(record: dict[str, Any], key: str) -> str:
    value = record.get(key)
    return value.get("label", "") if isinstance(value, dict) else ""


class SmartRecruitersClient:
    def __init__(self, *, timeout: float = 30.0, retries: int = 4) -> None:
        self.timeout = timeout
        self.retries = retries

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{API_ROOT}/{path.lstrip('/')}"
        if params:
            url = f"{url}?{urlencode(params)}"

        for attempt in range(self.retries + 1):
            request = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": USER_AGENT,
                },
            )
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    payload = json.load(response)
                if not isinstance(payload, dict):
                    raise CollectorError(f"Unexpected API response at {url}")
                return payload
            except HTTPError as exc:
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if not retryable or attempt >= self.retries:
                    body = exc.read(500).decode("utf-8", "replace")
                    raise CollectorError(f"HTTP {exc.code} for {url}: {body}") from exc
                retry_after = exc.headers.get("Retry-After", "")
                delay = float(retry_after) if retry_after.isdigit() else min(2**attempt, 20)
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                if attempt >= self.retries:
                    raise CollectorError(f"Request failed for {url}: {exc}") from exc
                delay = min(2**attempt, 20)
            time.sleep(delay)

        raise CollectorError(f"Request failed for {url}")


def collect_listing(
    client: SmartRecruitersClient,
    *,
    company_identifier: str,
    page_size: int,
    max_jobs: int | None,
) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    offset = 0
    total_found: int | None = None

    while total_found is None or offset < total_found:
        payload = client.get_json(
            f"companies/{quote(company_identifier)}/postings",
            {"limit": page_size, "offset": offset},
        )
        content = payload.get("content", [])
        if not isinstance(content, list):
            raise CollectorError("SmartRecruiters response has no valid content list")

        if total_found is None:
            total_found = int(payload.get("totalFound", len(content)))
        if not content:
            break

        jobs.extend(job for job in content if isinstance(job, dict))
        offset += len(content)
        if max_jobs is not None and len(jobs) >= max_jobs:
            jobs = jobs[:max_jobs]
            break

    deduplicated: dict[str, dict[str, Any]] = {}
    for job in jobs:
        job_id = str(job.get("id", ""))
        if job_id:
            deduplicated[job_id] = job
    return list(deduplicated.values())


def collect_details(
    client: SmartRecruitersClient,
    jobs: list[dict[str, Any]],
    *,
    company_identifier: str,
    workers: int,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, str]]]:
    details: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []

    def fetch(job_id: str) -> dict[str, Any]:
        return client.get_json(
            f"companies/{quote(company_identifier)}/postings/{quote(job_id)}"
        )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch, str(job["id"])): str(job["id"])
            for job in jobs
            if job.get("id")
        }
        for future in as_completed(futures):
            job_id = futures[future]
            try:
                details[job_id] = future.result()
            except Exception as exc:  # Preserve the listing even if one detail request fails.
                errors.append({"job_id": job_id, "error": str(exc)})

    errors.sort(key=lambda item: item["job_id"])
    return details, errors


def normalize_job(
    listing: dict[str, Any],
    detail: dict[str, Any] | None,
    *,
    company_identifier: str,
) -> dict[str, Any]:
    source = detail or listing
    job_id = str(source.get("id") or listing.get("id") or "")
    title = str(source.get("name") or listing.get("name") or "")
    location = source.get("location") if isinstance(source.get("location"), dict) else {}
    company = source.get("company") if isinstance(source.get("company"), dict) else {}

    custom_fields: dict[str, str] = {}
    custom_field_ids: dict[str, dict[str, str]] = {}
    for field in source.get("customField", []) or []:
        if not isinstance(field, dict):
            continue
        field_id = str(field.get("fieldId", ""))
        label = str(field.get("fieldLabel", ""))
        value_label = str(field.get("valueLabel", ""))
        if label:
            custom_fields[label] = value_label
        if field_id:
            custom_field_ids[field_id] = {
                "label": label,
                "value_id": str(field.get("valueId", "")),
                "value": value_label,
            }

    sections: dict[str, dict[str, str]] = {}
    job_ad = source.get("jobAd") if isinstance(source.get("jobAd"), dict) else {}
    raw_sections = job_ad.get("sections") if isinstance(job_ad.get("sections"), dict) else {}
    for key, section in raw_sections.items():
        if not isinstance(section, dict):
            continue
        section_html = str(section.get("text", ""))
        sections[str(key)] = {
            "title": str(section.get("title", "")),
            "html": section_html,
            "text": html_to_text(section_html),
        }

    generated_posting_url = (
        f"https://jobs.smartrecruiters.com/{company_identifier}/{job_id}-{slugify(title)}"
        if job_id
        else ""
    )
    posting_url = str(source.get("postingUrl") or generated_posting_url)
    apply_url = str(source.get("applyUrl") or (f"{posting_url}?oga=true" if posting_url else ""))

    return {
        "id": job_id,
        "uuid": str(source.get("uuid") or listing.get("uuid") or ""),
        "job_id": str(source.get("jobId", "")),
        "job_ad_id": str(source.get("jobAdId") or listing.get("jobAdId") or ""),
        "reference_number": str(source.get("refNumber") or listing.get("refNumber") or ""),
        "title": title,
        "company": str(company.get("name", "Samsung E&A")),
        "company_identifier": str(company.get("identifier", company_identifier)),
        "released_at": str(source.get("releasedDate") or listing.get("releasedDate") or ""),
        "active": source.get("active"),
        "visibility": str(source.get("visibility") or listing.get("visibility") or ""),
        "posting_url": posting_url,
        "apply_url": apply_url,
        "referral_url": str(source.get("referralUrl", "")),
        "location": {
            "full": str(location.get("fullLocation", "")),
            "city": str(location.get("city", "")),
            "region": str(location.get("region", "")),
            "country_code": str(location.get("country", "")),
            "remote": bool(location.get("remote", False)),
            "hybrid": bool(location.get("hybrid", False)),
            "latitude": str(location.get("latitude", "")),
            "longitude": str(location.get("longitude", "")),
        },
        "industry": nested_label(source, "industry") or nested_label(listing, "industry"),
        "department": nested_label(source, "department") or nested_label(listing, "department"),
        "function": nested_label(source, "function") or nested_label(listing, "function"),
        "employment_type": nested_label(source, "typeOfEmployment")
        or nested_label(listing, "typeOfEmployment"),
        "experience_level": nested_label(source, "experienceLevel")
        or nested_label(listing, "experienceLevel"),
        "language": nested_label(source, "language") or nested_label(listing, "language"),
        "custom_fields": custom_fields,
        "custom_field_ids": custom_field_ids,
        "sections": sections,
        "source_api_url": str(
            listing.get("ref")
            or f"{API_ROOT}/companies/{company_identifier}/postings/{job_id}"
        ),
    }


def matches_filters(
    job: dict[str, Any],
    *,
    countries: list[str],
    departments: list[str],
) -> bool:
    if countries:
        location = job["location"]
        haystack = " ".join(
            [location.get("country_code", ""), location.get("full", "")]
        ).casefold()
        if not any(country.casefold() in haystack for country in countries):
            return False
    if departments:
        department = str(job.get("department", "")).casefold()
        if not any(value.casefold() in department for value in departments):
            return False
    return True


def csv_row(job: dict[str, Any]) -> dict[str, Any]:
    location = job["location"]
    sections = job.get("sections", {})

    def section_text(key: str) -> str:
        section = sections.get(key, {})
        return str(section.get("text", "")) if isinstance(section, dict) else ""

    return {
        "id": job["id"],
        "uuid": job["uuid"],
        "reference_number": job["reference_number"],
        "title": job["title"],
        "company": job["company"],
        "released_at": job["released_at"],
        "active": job["active"],
        "posting_url": job["posting_url"],
        "apply_url": job["apply_url"],
        "location": location["full"],
        "city": location["city"],
        "region": location["region"],
        "country_code": location["country_code"],
        "remote": location["remote"],
        "hybrid": location["hybrid"],
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "industry": job["industry"],
        "department": job["department"],
        "function": job["function"],
        "employment_type": job["employment_type"],
        "experience_level": job["experience_level"],
        "language": job["language"],
        "company_description": section_text("companyDescription"),
        "job_description": section_text("jobDescription"),
        "qualifications": section_text("qualifications"),
        "additional_information": section_text("additionalInformation"),
        "custom_fields_json": json.dumps(job["custom_fields"], ensure_ascii=False, sort_keys=True),
        "other_sections_json": json.dumps(
            {
                key: value
                for key, value in sections.items()
                if key
                not in {
                    "companyDescription",
                    "jobDescription",
                    "qualifications",
                    "additionalInformation",
                }
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        "source_api_url": job["source_api_url"],
    }


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def write_csv(path: Path, jobs: Iterable[dict[str, Any]]) -> None:
    rows = [csv_row(job) for job in jobs]
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    fieldnames = list(rows[0].keys()) if rows else list(csv_row(empty_job()).keys())
    with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def empty_job() -> dict[str, Any]:
    return {
        "id": "",
        "uuid": "",
        "reference_number": "",
        "title": "",
        "company": "",
        "released_at": "",
        "active": None,
        "posting_url": "",
        "apply_url": "",
        "location": {
            "full": "",
            "city": "",
            "region": "",
            "country_code": "",
            "remote": False,
            "hybrid": False,
            "latitude": "",
            "longitude": "",
        },
        "industry": "",
        "department": "",
        "function": "",
        "employment_type": "",
        "experience_level": "",
        "language": "",
        "sections": {},
        "custom_fields": {},
        "source_api_url": "",
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Collect public Samsung E&A jobs from SmartRecruiters."
    )
    parser.add_argument("--output-dir", type=Path, default=script_dir / "data")
    parser.add_argument("--prefix", default="samsung_ea_jobs")
    parser.add_argument("--format", choices=("json", "csv", "both"), default="both")
    parser.add_argument("--country", action="append", default=[], help="Repeatable ISO code or country-name filter")
    parser.add_argument("--department", action="append", default=[], help="Repeatable department substring filter")
    parser.add_argument("--max-jobs", type=int, default=None, help="Limit jobs, useful for tests")
    parser.add_argument("--page-size", type=int, default=100, choices=range(1, 101), metavar="1-100")
    parser.add_argument("--workers", type=int, default=8, choices=range(1, 33), metavar="1-32")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument(
        "--no-details",
        dest="include_details",
        action="store_false",
        help="Skip per-job detail requests (descriptions will be empty)",
    )
    parser.set_defaults(include_details=True)
    args = parser.parse_args(argv)
    if args.max_jobs is not None and args.max_jobs < 1:
        parser.error("--max-jobs must be at least 1")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than 0")
    if args.retries < 0:
        parser.error("--retries must not be negative")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    client = SmartRecruitersClient(timeout=args.timeout, retries=args.retries)
    collected_at = utc_now()

    try:
        listings = collect_listing(
            client,
            company_identifier=COMPANY_IDENTIFIER,
            page_size=args.page_size,
            max_jobs=args.max_jobs,
        )
        details: dict[str, dict[str, Any]] = {}
        detail_errors: list[dict[str, str]] = []
        if args.include_details and listings:
            details, detail_errors = collect_details(
                client,
                listings,
                company_identifier=COMPANY_IDENTIFIER,
                workers=args.workers,
            )
    except CollectorError as exc:
        print(f"Collector failed: {exc}", file=sys.stderr)
        return 1

    jobs = [
        normalize_job(
            listing,
            details.get(str(listing.get("id", ""))),
            company_identifier=COMPANY_IDENTIFIER,
        )
        for listing in listings
    ]
    jobs = [
        job
        for job in jobs
        if matches_filters(
            job,
            countries=args.country,
            departments=args.department,
        )
    ]
    jobs.sort(key=lambda job: (job.get("released_at", ""), job.get("id", "")), reverse=True)

    payload = {
        "metadata": {
            "collector": "samsung-ea-smartrecruiters",
            "collector_version": "1.0.0",
            "collected_at": collected_at,
            "company": "Samsung E&A",
            "company_identifier": COMPANY_IDENTIFIER,
            "ats": "SmartRecruiters",
            "careers_url": CAREERS_URL,
            "api_endpoint": f"{API_ROOT}/companies/{COMPANY_IDENTIFIER}/postings",
            "details_included": args.include_details,
            "job_count": len(jobs),
            "detail_error_count": len(detail_errors),
            "detail_errors": detail_errors,
            "filters": {
                "countries": args.country,
                "departments": args.department,
                "max_jobs": args.max_jobs,
            },
        },
        "jobs": jobs,
    }

    output_dir: Path = args.output_dir.expanduser().resolve()
    output_paths: list[Path] = []
    if args.format in {"json", "both"}:
        json_path = output_dir / f"{args.prefix}.json"
        write_json(json_path, payload)
        output_paths.append(json_path)
    if args.format in {"csv", "both"}:
        csv_path = output_dir / f"{args.prefix}.csv"
        write_csv(csv_path, jobs)
        output_paths.append(csv_path)

    print(f"Collected {len(jobs)} Samsung E&A jobs.")
    if detail_errors:
        print(f"Warning: {len(detail_errors)} detail request(s) failed; listings were retained.")
    for path in output_paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
