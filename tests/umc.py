#!/usr/bin/env python3
"""Collect public job postings from the UMC Singapore career portal."""

from __future__ import annotations

import argparse
import csv
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
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen


BASE_URL = "https://sgcareers.umc.com/"
CAREERS_URL = urljoin(BASE_URL, "jobsearch.php")
USER_AGENT = "UMCSingaporeJobCollector/1.0 (+public career postings)"


class CollectorError(RuntimeError):
    """Raised when public career data cannot be retrieved or validated."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def clean_text(parts: Iterable[str]) -> str:
    text = "".join(parts).replace("\xa0", " ").replace("\u3000", " ")
    lines = [re.sub(r"[ \t\r\f\v]+", " ", line).strip() for line in text.split("\n")]
    result: list[str] = []
    for line in lines:
        if line and (not result or result[-1] != line):
            result.append(line)
    return "\n".join(result)


def normalized_label(value: str) -> str:
    return re.sub(r"\s*[：:]\s*$", "", value).strip()


class JobListParser(HTMLParser):
    """Parse all server-rendered rows, including rows hidden behind “more”."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.jobs: list[dict[str, str]] = []
        self.last_row_index: int | None = None
        self._current: dict[str, Any] | None = None
        self._link_capture = False
        self._link_parts: list[str] = []
        self._link_href = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "input" and attributes.get("id") == "tpage":
            value = attributes.get("value") or ""
            if value.isdigit():
                self.last_row_index = int(value)

        row_id = attributes.get("id") or ""
        if tag == "li" and re.fullmatch(r"myli\d+", row_id):
            self._current = {"values": [], "href": "", "id": ""}
            return

        if self._current is None:
            return

        if tag == "a":
            href = attributes.get("href") or ""
            parsed = urlparse(urljoin(BASE_URL, href))
            if parsed.path.endswith("/jobin.php") and parse_qs(parsed.query).get("mid"):
                self._link_capture = True
                self._link_parts = []
                self._link_href = urljoin(BASE_URL, href)

        if self._link_capture and tag == "br":
            self._link_parts.append("\n")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._link_capture and tag == "br":
            self._link_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._link_capture:
            assert self._current is not None
            parsed = urlparse(self._link_href)
            job_id = parse_qs(parsed.query).get("mid", [""])[0]
            self._current["values"].append(clean_text(self._link_parts))
            self._current["href"] = self._link_href
            self._current["id"] = job_id
            self._link_capture = False
            self._link_parts = []
            self._link_href = ""

        if tag == "li" and self._current is not None:
            values = self._current["values"]
            if self._current["id"] and len(values) >= 6:
                self.jobs.append(
                    {
                        "id": self._current["id"],
                        "title": values[0],
                        "job_function": values[1],
                        "experience": values[2],
                        "education": values[3],
                        "location_summary": values[4],
                        "updated_date": values[5],
                        "posting_url": self._current["href"],
                    }
                )
            self._current = None
            self._link_capture = False
            self._link_parts = []

    def handle_data(self, data: str) -> None:
        if self._link_capture:
            self._link_parts.append(data)


class JobDetailParser(HTMLParser):
    """Extract labeled fields from the UMC job and application sections."""

    BLOCK_TAGS = {"br", "div", "li", "p", "tr"}
    CONTAINER_IDS = {"jobin", "wayjob"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.fields: dict[str, str] = {}
        self.apply_control_present = False
        self._depth = 0
        self._container_depth: int | None = None
        self._label_depth: int | None = None
        self._label_parts: list[str] = []
        self._pending_label = ""
        self._value_depth: int | None = None
        self._value_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        next_depth = self._depth + 1

        if tag == "div" and attributes.get("id") in self.CONTAINER_IDS:
            self._container_depth = next_depth
        elif self._container_depth is not None:
            if self._pending_label and self._value_depth is None and tag in {"div", "span"}:
                self._value_depth = next_depth
                self._value_parts = []
            elif (
                tag == "span"
                and self._label_depth is None
                and self._value_depth is None
                and not self._pending_label
            ):
                self._label_depth = next_depth
                self._label_parts = []

            if self._value_depth is not None and tag in self.BLOCK_TAGS:
                self._value_parts.append("\n")

        onclick = attributes.get("onclick") or ""
        if tag == "a" and re.search(r"showlogin\s*\(\s*['\"]Y['\"]\s*\)", onclick):
            self.apply_control_present = True

        self._depth = next_depth

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._container_depth is not None and self._value_depth is not None:
            if tag in self.BLOCK_TAGS:
                self._value_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self._label_depth is not None and self._depth == self._label_depth and tag == "span":
            label = clean_text(self._label_parts)
            self._label_depth = None
            self._label_parts = []
            if label.rstrip().endswith(("：", ":")):
                self._pending_label = normalized_label(label)

        if self._value_depth is not None and self._depth == self._value_depth:
            value = clean_text(self._value_parts)
            if self._pending_label:
                self.fields[self._pending_label] = value
            self._pending_label = ""
            self._value_depth = None
            self._value_parts = []
        elif self._value_depth is not None and tag in self.BLOCK_TAGS:
            self._value_parts.append("\n")

        if self._container_depth is not None and self._depth == self._container_depth and tag == "div":
            self._container_depth = None
            self._label_depth = None
            self._pending_label = ""
            self._value_depth = None

        self._depth -= 1

    def handle_data(self, data: str) -> None:
        if self._label_depth is not None:
            self._label_parts.append(data)
        elif self._value_depth is not None:
            self._value_parts.append(data)


class HttpClient:
    def __init__(self, *, timeout: float = 30.0, retries: int = 4) -> None:
        self.timeout = timeout
        self.retries = retries

    def get_text(self, url: str) -> str:
        for attempt in range(self.retries + 1):
            request = Request(
                url,
                headers={
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "en-SG,en;q=0.9",
                    "User-Agent": USER_AGENT,
                },
            )
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    raw = response.read()
                    encoding = response.headers.get_content_charset() or "utf-8"
                return raw.decode(encoding, "replace")
            except HTTPError as exc:
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if not retryable or attempt >= self.retries:
                    raise CollectorError(f"HTTP {exc.code} for {url}") from exc
                retry_after = exc.headers.get("Retry-After", "")
                delay = float(retry_after) if retry_after.isdigit() else min(2**attempt, 20)
            except (URLError, TimeoutError) as exc:
                if attempt >= self.retries:
                    raise CollectorError(f"Request failed for {url}: {exc}") from exc
                delay = min(2**attempt, 20)
            time.sleep(delay)
        raise CollectorError(f"Request failed for {url}")


def parse_listing(html: str) -> JobListParser:
    parser = JobListParser()
    parser.feed(html)
    parser.close()
    return parser


def parse_detail(html: str) -> JobDetailParser:
    parser = JobDetailParser()
    parser.feed(html)
    parser.close()
    return parser


def collect_listing(client: HttpClient, *, max_jobs: int | None) -> list[dict[str, str]]:
    parsed = parse_listing(client.get_text(CAREERS_URL))
    if not parsed.jobs:
        raise CollectorError("No UMC Singapore jobs found on the career page")

    expected_count = parsed.last_row_index + 1 if parsed.last_row_index is not None else None
    if expected_count is not None and len(parsed.jobs) != expected_count:
        raise CollectorError(
            f"Career page indicates {expected_count} jobs, but {len(parsed.jobs)} were parsed"
        )

    unique: dict[str, dict[str, str]] = {}
    for job in parsed.jobs:
        unique[job["id"]] = job
    jobs = list(unique.values())
    return jobs[:max_jobs] if max_jobs is not None else jobs


def collect_details(
    client: HttpClient,
    jobs: list[dict[str, str]],
    *,
    workers: int,
) -> tuple[dict[str, JobDetailParser], list[dict[str, str]]]:
    details: dict[str, JobDetailParser] = {}
    errors: list[dict[str, str]] = []

    def fetch(job: dict[str, str]) -> JobDetailParser:
        return parse_detail(client.get_text(job["posting_url"]))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch, job): job for job in jobs}
        for future in as_completed(futures):
            job = futures[future]
            try:
                detail = future.result()
                details[job["id"]] = detail
                if not detail.fields.get("Job Title"):
                    errors.append({"job_id": job["id"], "error": "Detail page has no job title"})
            except Exception as exc:
                errors.append({"job_id": job["id"], "error": str(exc)})

    errors.sort(key=lambda item: item["job_id"])
    return details, errors


def field(fields: dict[str, str], *labels: str) -> str:
    for label in labels:
        if fields.get(label):
            return fields[label]
    return ""


def normalize_job(listing: dict[str, str], detail: JobDetailParser | None) -> dict[str, Any]:
    detail_available = bool(detail and detail.fields.get("Job Title"))
    fields = detail.fields if detail_available and detail else {}
    detail_error = "" if detail_available else "Detail not requested, failed, or has no job title"
    apply_available = bool(detail_available and detail and detail.apply_control_present)

    return {
        "id": listing["id"],
        "title": field(fields, "Job Title") or listing["title"],
        "company": "United Microelectronics Corporation (UMC)",
        "company_local": "United Microelectronics Corporation (Singapore Branch)",
        "ats": "Custom UMC PHP career portal",
        "employment_type": field(fields, "Type of employment"),
        "vacancies": field(fields, "Number of Vacancies"),
        "job_function": field(fields, "Job Function") or listing["job_function"],
        "description": field(fields, "Description"),
        "job_description": field(fields, "Job Description"),
        "location": field(fields, "Location"),
        "site": field(fields, "Site") or listing["location_summary"],
        "location_summary": listing["location_summary"],
        "management_responsibility": field(fields, "Management Responsibility"),
        "business_travel": field(fields, "Business travel"),
        "working_hours": field(fields, "Working Hours"),
        "shift_pattern": field(fields, "Shift Pattern"),
        "leave_entitlement": field(fields, "Leave entitlement"),
        "date_available": field(fields, "Date available"),
        "experience": field(fields, "Years of Experience") or listing["experience"],
        "education": field(fields, "Education") or listing["education"],
        "major": field(fields, "Major"),
        "requirements": field(fields, "Other requirement"),
        "updated_date": field(fields, "Updating Date") or listing["updated_date"],
        "contact_person": field(fields, "Contact Person"),
        "how_to_apply": field(fields, "How to Apply"),
        "posting_url": listing["posting_url"],
        "apply_url": (
            urljoin(BASE_URL, f"login.php?act=b&jobmid={listing['id']}") if apply_available else ""
        ),
        "application_requires_login": apply_available,
        "detail_available": detail_available,
        "detail_error": detail_error,
        "fields": fields,
        "source_careers_url": CAREERS_URL,
    }


def contains_any(value: str, needles: list[str]) -> bool:
    folded = value.casefold()
    return any(needle.casefold() in folded for needle in needles)


def matches_filters(
    job: dict[str, Any],
    *,
    keywords: list[str],
    job_functions: list[str],
    locations: list[str],
    experiences: list[str],
    educations: list[str],
) -> bool:
    if keywords:
        searchable = "\n".join(
            [
                str(job.get("title", "")),
                str(job.get("description", "")),
                str(job.get("job_description", "")),
                str(job.get("requirements", "")),
            ]
        )
        if not contains_any(searchable, keywords):
            return False
    if job_functions and not contains_any(str(job.get("job_function", "")), job_functions):
        return False
    if locations:
        location = " ".join(
            [
                str(job.get("location", "")),
                str(job.get("site", "")),
                str(job.get("location_summary", "")),
            ]
        )
        if not contains_any(location, locations):
            return False
    if experiences and not contains_any(str(job.get("experience", "")), experiences):
        return False
    if educations and not contains_any(str(job.get("education", "")), educations):
        return False
    return True


def csv_row(job: dict[str, Any]) -> dict[str, Any]:
    row = {key: value for key, value in job.items() if key != "fields"}
    row["fields_json"] = json.dumps(job["fields"], ensure_ascii=False, sort_keys=True)
    return row


def empty_job() -> dict[str, Any]:
    return normalize_job(
        {
            "id": "",
            "title": "",
            "job_function": "",
            "experience": "",
            "education": "",
            "location_summary": "",
            "updated_date": "",
            "posting_url": "",
        },
        None,
    )


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def write_csv(path: Path, jobs: Iterable[dict[str, Any]]) -> None:
    rows = [csv_row(job) for job in jobs]
    fieldnames = list(rows[0].keys()) if rows else list(csv_row(empty_job()).keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Collect public jobs from the UMC Singapore career portal."
    )
    parser.add_argument("--output-dir", type=Path, default=script_dir / "data")
    parser.add_argument("--prefix", default="umc_singapore_jobs")
    parser.add_argument("--format", choices=("json", "csv", "both"), default="both")
    parser.add_argument("--keyword", action="append", default=[], help="Repeatable text filter")
    parser.add_argument("--job-function", action="append", default=[], help="Repeatable function filter")
    parser.add_argument("--location", action="append", default=[], help="Repeatable location filter")
    parser.add_argument("--experience", action="append", default=[], help="Repeatable experience filter")
    parser.add_argument("--education", action="append", default=[], help="Repeatable education filter")
    parser.add_argument("--max-jobs", type=int, default=None, help="Limit jobs, useful for tests")
    parser.add_argument("--workers", type=int, default=6, choices=range(1, 13), metavar="1-12")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument(
        "--no-details",
        dest="include_details",
        action="store_false",
        help="Skip individual detail pages",
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
    client = HttpClient(timeout=args.timeout, retries=args.retries)

    try:
        listings = collect_listing(client, max_jobs=args.max_jobs)
        details: dict[str, JobDetailParser] = {}
        detail_errors: list[dict[str, str]] = []
        if args.include_details:
            details, detail_errors = collect_details(client, listings, workers=args.workers)
    except CollectorError as exc:
        print(f"Collector failed: {exc}", file=sys.stderr)
        return 1

    jobs = [normalize_job(listing, details.get(listing["id"])) for listing in listings]
    jobs = [
        job
        for job in jobs
        if matches_filters(
            job,
            keywords=args.keyword,
            job_functions=args.job_function,
            locations=args.location,
            experiences=args.experience,
            educations=args.education,
        )
    ]

    payload = {
        "metadata": {
            "collector": "umc-singapore-custom-php",
            "collector_version": "1.0.0",
            "collected_at": utc_now(),
            "company": "United Microelectronics Corporation (UMC)",
            "company_local": "United Microelectronics Corporation (Singapore Branch)",
            "ats": "Custom UMC PHP career portal",
            "careers_url": CAREERS_URL,
            "details_included": args.include_details,
            "job_count": len(jobs),
            "available_detail_count": sum(job["detail_available"] for job in jobs),
            "detail_error_count": len(detail_errors),
            "detail_errors": detail_errors,
            "filters": {
                "keywords": args.keyword,
                "job_functions": args.job_function,
                "locations": args.location,
                "experiences": args.experience,
                "educations": args.education,
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

    print(f"Collected {len(jobs)} UMC Singapore jobs.")
    if detail_errors:
        print(f"Notice: {len(detail_errors)} detail page(s) failed; listings were retained.")
    for path in output_paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
