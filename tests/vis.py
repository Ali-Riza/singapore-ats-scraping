#!/usr/bin/env python3
"""Collect public jobs from the VIS applyourjobs.com career portal."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URL = "https://vis.applyourjobs.com/"
CAREERS_URL = BASE_URL
JOBS_API_URL = BASE_URL + "default.aspx/GetJobList"
USER_AGENT = "VISJobCollector/1.0 (+public career postings)"
ATS_NAME = "applyourjobs.com white-label ASP.NET recruiting portal"


class CollectorError(RuntimeError):
    """Raised when public career data cannot be retrieved or validated."""


@dataclass(frozen=True)
class TextResponse:
    text: str
    final_url: str


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


class JobDetailParser(HTMLParser):
    """Extract the visible description and application state from a detail page."""

    BLOCK_TAGS = {"br", "div", "li", "p", "tr", "ul", "ol", "h1", "h2", "h3", "h4"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.description = ""
        self.page_title = ""
        self.apply_control_present = False
        self._depth = 0
        self._description_depth: int | None = None
        self._description_parts: list[str] = []
        self._title_depth: int | None = None
        self._title_parts: list[str] = []
        self._page_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        next_depth = self._depth + 1
        classes = set((attributes.get("class") or "").split())

        if tag == "title":
            self._title_depth = next_depth
            self._title_parts = []
        if tag == "div" and "htmlJDstyleParent" in classes:
            self._description_depth = next_depth
            self._description_parts = []
        elif self._description_depth is not None and tag in self.BLOCK_TAGS:
            self._description_parts.append("\n")

        element_id = (attributes.get("id") or "").casefold()
        value = (attributes.get("value") or "").casefold()
        if tag in {"input", "button"} and ("btnapply" in element_id or value == "apply"):
            self.apply_control_present = True

        self._depth = next_depth

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._description_depth is not None and tag in self.BLOCK_TAGS:
            self._description_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self._title_depth is not None and self._depth == self._title_depth and tag == "title":
            self.page_title = clean_text(self._title_parts)
            self._title_depth = None
            self._title_parts = []

        if self._description_depth is not None:
            if self._depth == self._description_depth and tag == "div":
                self.description = clean_text(self._description_parts)
                self._description_depth = None
                self._description_parts = []
            elif tag in self.BLOCK_TAGS:
                self._description_parts.append("\n")

        self._depth -= 1

    def handle_data(self, data: str) -> None:
        self._page_parts.append(data)
        if self._title_depth is not None:
            self._title_parts.append(data)
        if self._description_depth is not None:
            self._description_parts.append(data)

    @property
    def closed(self) -> bool:
        text = clean_text(self._page_parts).casefold()
        return "withdrawn/closed" in text or self.page_title.casefold().startswith("no jobs")


class FragmentTextParser(HTMLParser):
    """Convert an HTML fragment into readable text, tolerating legacy markup."""

    BLOCK_TAGS = {"br", "div", "li", "p", "tr", "ul", "ol", "h1", "h2", "h3", "h4"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


class HttpClient:
    def __init__(self, *, timeout: float = 30.0, retries: int = 4) -> None:
        self.timeout = timeout
        self.retries = retries

    def get_text(self, url: str) -> TextResponse:
        return self._request(url)

    def post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._request(
            url,
            data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            content_type="application/json; charset=utf-8",
        )
        try:
            parsed = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise CollectorError(f"Invalid JSON returned by {url}: {exc}") from exc
        if not isinstance(parsed, dict):
            raise CollectorError(f"Unexpected JSON structure returned by {url}")
        return parsed

    def _request(
        self,
        url: str,
        *,
        data: bytes | None = None,
        content_type: str | None = None,
    ) -> TextResponse:
        for attempt in range(self.retries + 1):
            headers = {
                "Accept": "application/json,text/html,application/xhtml+xml",
                "Accept-Language": "en-SG,en;q=0.9",
                "Referer": CAREERS_URL,
                "User-Agent": USER_AGENT,
            }
            if content_type:
                headers["Content-Type"] = content_type
                headers["X-Requested-With"] = "XMLHttpRequest"
            request = Request(url, data=data, headers=headers)
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    raw = response.read()
                    encoding = response.headers.get_content_charset() or "utf-8"
                    final_url = response.url
                return TextResponse(raw.decode(encoding, "replace"), final_url)
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


def parse_api_response(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    try:
        data = payload["d"]
        raw_jobs = data.get("JsonData") or "[]"
        jobs = json.loads(raw_jobs) if isinstance(raw_jobs, str) else raw_jobs
        total = int(data.get("PagerLength", len(jobs)))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CollectorError(f"Unexpected jobs API response: {exc}") from exc
    if not isinstance(jobs, list) or not all(isinstance(job, dict) for job in jobs):
        raise CollectorError("Jobs API did not return a list of objects")
    return jobs, total


def fetch_job_page(
    client: HttpClient,
    *,
    start: int,
    end: int,
) -> tuple[list[dict[str, Any]], int]:
    payload = {
        "srcArr": [],
        "sortingColumn": "POSTINGDATE",
        "sortDirection": "desc",
        "startCount": str(start),
        "endCount": end,
    }
    return parse_api_response(client.post_json(JOBS_API_URL, payload))


def collect_listing(
    client: HttpClient,
    *,
    max_jobs: int | None,
    page_size: int = 100,
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    expected_total: int | None = None
    start = 0

    while expected_total is None or start < expected_total:
        rows, total = fetch_job_page(client, start=start, end=start + page_size)
        expected_total = total
        if not rows:
            break
        collected.extend(rows)
        start += len(rows)
        if max_jobs is not None and len(collected) >= max_jobs:
            break

    unique: dict[str, dict[str, Any]] = {}
    for row in collected:
        job_code = str(row.get("JOBCODE") or "")
        if job_code:
            unique[job_code] = row
    jobs = list(unique.values())

    if not jobs:
        raise CollectorError("No active VIS jobs returned by the careers API")
    if max_jobs is None and expected_total is not None and len(jobs) != expected_total:
        raise CollectorError(
            f"Jobs API reports {expected_total} jobs, but {len(jobs)} unique jobs were collected"
        )
    return jobs[:max_jobs] if max_jobs is not None else jobs


def posting_url(row: dict[str, Any]) -> str:
    return BASE_URL + "jobdetails.aspx?ID=" + str(row.get("JOBCODE") or "")


def parse_detail(html: str) -> JobDetailParser:
    parser = JobDetailParser()
    parser.feed(html)
    parser.close()

    # Several postings contain malformed Word-generated HTML. Extracting the
    # known server-rendered description region separately prevents unbalanced
    # tags from swallowing the remainder of the document.
    start_match = re.search(
        r"<div\b[^>]*class\s*=\s*(['\"])[^'\"]*\bhtmlJDstyleParent\b[^'\"]*\1[^>]*>",
        html,
        flags=re.IGNORECASE,
    )
    if start_match:
        end_match = re.search(
            r"<div\b[^>]*id\s*=\s*(['\"])ctl00_body_updatepanel1\1",
            html[start_match.end() :],
            flags=re.IGNORECASE,
        )
        end = (
            start_match.end() + end_match.start()
            if end_match
            else len(html)
        )
        fragment_parser = FragmentTextParser()
        fragment_parser.feed(html[start_match.end() : end])
        fragment_parser.close()
        description = clean_text(fragment_parser.parts)
        if description:
            parser.description = description

    apply_tags = re.findall(r"<(?:input|button)\b[^>]*>", html, flags=re.IGNORECASE)
    parser.apply_control_present = any(
        re.search(r"\bbtnapply\b", tag, flags=re.IGNORECASE)
        or re.search(r"\bvalue\s*=\s*(['\"])apply\1", tag, flags=re.IGNORECASE)
        for tag in apply_tags
    )
    return parser


def collect_details(
    client: HttpClient,
    rows: list[dict[str, Any]],
    *,
    workers: int,
) -> tuple[dict[str, JobDetailParser], list[dict[str, str]]]:
    details: dict[str, JobDetailParser] = {}
    errors: list[dict[str, str]] = []

    def fetch(row: dict[str, Any]) -> JobDetailParser:
        response = client.get_text(posting_url(row))
        parsed = parse_detail(response.text)
        if response.final_url.casefold().endswith("nojobs.aspx"):
            parsed.description = ""
        return parsed

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch, row): row for row in rows}
        for future in as_completed(futures):
            row = futures[future]
            job_id = str(row.get("JOBID") or row.get("REFERENCENUMBER") or "")
            try:
                detail = future.result()
                details[str(row["JOBCODE"])] = detail
                if detail.closed:
                    errors.append({"job_id": job_id, "error": "Posting is withdrawn or closed"})
                elif not detail.description:
                    errors.append({"job_id": job_id, "error": "Detail page has no description"})
            except Exception as exc:
                errors.append({"job_id": job_id, "error": str(exc)})

    errors.sort(key=lambda item: item["job_id"])
    return details, errors


def source_value(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    return "" if value is None else str(value).strip()


def normalize_job(
    row: dict[str, Any],
    detail: JobDetailParser | None,
    *,
    details_requested: bool,
) -> dict[str, Any]:
    detail_available = bool(detail and detail.description and not detail.closed)
    if detail_available:
        detail_error = ""
    elif not details_requested:
        detail_error = "Detail pages were not requested"
    elif detail and detail.closed:
        detail_error = "Posting is withdrawn or closed"
    else:
        detail_error = "Detail request failed or page has no description"

    url = posting_url(row)
    apply_available = bool(detail_available and detail and detail.apply_control_present)
    return {
        "id": source_value(row, "JOBID"),
        "reference_number": source_value(row, "REFERENCENUMBER"),
        "title": source_value(row, "JOBTITLE"),
        "company": "Vanguard International Semiconductor Singapore Pte Ltd",
        "company_short": "VIS Singapore",
        "ats": ATS_NAME,
        "department": source_value(row, "JOBCONTAINER"),
        "job_function": source_value(row, "JOBSPECIALIZATION"),
        "job_sub_function": source_value(row, "JOBSUBSPECIALIZATION"),
        "job_type": source_value(row, "JOBTYPE"),
        "location": source_value(row, "JOBLOCATION"),
        "sub_location": source_value(row, "JOBSUBLOCATION"),
        "category": source_value(row, "JOBCATEGORY"),
        "qualification": source_value(row, "JOBQUALIFICATIONS"),
        "minimum_experience": source_value(row, "MINYEARSOFEXPERIENCE"),
        "vacancies": source_value(row, "VACANCY"),
        "open_date": source_value(row, "OPENDATE"),
        "posting_date": source_value(row, "POSTINGDATE"),
        "expiry_date": source_value(row, "EXPIRYDATE"),
        "time_ago": source_value(row, "TIMEAGO"),
        "description": detail.description if detail_available and detail else "",
        "posting_url": url,
        "apply_url": url if apply_available else "",
        "application_requires_login": apply_available,
        "application_note": (
            "Open the posting URL, select Apply, then log in or register."
            if apply_available
            else ""
        ),
        "detail_available": detail_available,
        "detail_error": detail_error,
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
    job_types: list[str],
    locations: list[str],
    qualifications: list[str],
) -> bool:
    if keywords:
        searchable = "\n".join(
            [
                str(job.get("title", "")),
                str(job.get("department", "")),
                str(job.get("job_function", "")),
                str(job.get("description", "")),
            ]
        )
        if not contains_any(searchable, keywords):
            return False
    if job_functions and not contains_any(str(job.get("job_function", "")), job_functions):
        return False
    if job_types and not contains_any(str(job.get("job_type", "")), job_types):
        return False
    if locations:
        location = " ".join(
            [str(job.get("location", "")), str(job.get("sub_location", ""))]
        )
        if not contains_any(location, locations):
            return False
    if qualifications and not contains_any(
        str(job.get("qualification", "")), qualifications
    ):
        return False
    return True


CSV_FIELDS = [
    "id",
    "reference_number",
    "title",
    "company",
    "company_short",
    "ats",
    "department",
    "job_function",
    "job_sub_function",
    "job_type",
    "location",
    "sub_location",
    "category",
    "qualification",
    "minimum_experience",
    "vacancies",
    "open_date",
    "posting_date",
    "expiry_date",
    "time_ago",
    "description",
    "posting_url",
    "apply_url",
    "application_requires_login",
    "application_note",
    "detail_available",
    "detail_error",
    "source_careers_url",
]


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def write_csv(path: Path, jobs: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(jobs)
    os.replace(temporary, path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Collect public jobs from the VIS applyourjobs.com career portal."
    )
    parser.add_argument("--output-dir", type=Path, default=script_dir / "data")
    parser.add_argument("--prefix", default="vis_jobs")
    parser.add_argument("--format", choices=("json", "csv", "both"), default="both")
    parser.add_argument("--keyword", action="append", default=[], help="Repeatable text filter")
    parser.add_argument("--job-function", action="append", default=[], help="Repeatable function filter")
    parser.add_argument("--job-type", action="append", default=[], help="Repeatable type filter")
    parser.add_argument("--location", action="append", default=[], help="Repeatable location filter")
    parser.add_argument("--qualification", action="append", default=[], help="Repeatable qualification filter")
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
        rows = collect_listing(client, max_jobs=args.max_jobs)
        details: dict[str, JobDetailParser] = {}
        detail_errors: list[dict[str, str]] = []
        if args.include_details:
            details, detail_errors = collect_details(client, rows, workers=args.workers)
    except CollectorError as exc:
        print(f"Collector failed: {exc}", file=sys.stderr)
        return 1

    jobs = [
        normalize_job(
            row,
            details.get(str(row.get("JOBCODE") or "")),
            details_requested=args.include_details,
        )
        for row in rows
    ]
    jobs = [
        job
        for job in jobs
        if matches_filters(
            job,
            keywords=args.keyword,
            job_functions=args.job_function,
            job_types=args.job_type,
            locations=args.location,
            qualifications=args.qualification,
        )
    ]

    payload = {
        "metadata": {
            "collector": "vis-applyourjobs",
            "collector_version": "1.0.0",
            "collected_at": utc_now(),
            "company": "Vanguard International Semiconductor Singapore Pte Ltd",
            "company_short": "VIS Singapore",
            "ats": ATS_NAME,
            "ats_vendor_identified": False,
            "careers_url": CAREERS_URL,
            "jobs_api_url": JOBS_API_URL,
            "details_included": args.include_details,
            "job_count": len(jobs),
            "available_detail_count": sum(job["detail_available"] for job in jobs),
            "detail_error_count": len(detail_errors),
            "detail_errors": detail_errors,
            "filters": {
                "keywords": args.keyword,
                "job_functions": args.job_function,
                "job_types": args.job_type,
                "locations": args.location,
                "qualifications": args.qualification,
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

    print(f"Collected {len(jobs)} VIS jobs.")
    if detail_errors:
        print(f"Notice: {len(detail_errors)} detail page(s) failed; listings were retained.")
    for path in output_paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
