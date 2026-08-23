#!/usr/bin/env python3
"""Collect Toyo Engineering career jobs and their SONAR ATS details."""

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
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen


CAREERS_URL = "https://career.toyo-eng.com/"
ATS_HOST = "toyo-eng.snar.jp"
USER_AGENT = "ToyoEngineeringJobCollector/1.0 (+public career postings)"
UNAVAILABLE_TEXT = "現在こちらのページはご利用になれません"


class CollectorError(RuntimeError):
    """Raised when the collector cannot retrieve or validate public job data."""


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


def class_names(attrs: list[tuple[str, str | None]]) -> set[str]:
    attributes = dict(attrs)
    return set((attributes.get("class") or "").split())


class CareersListParser(HTMLParser):
    """Extract job cards from the Toyo Engineering WordPress career page."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.jobs: list[dict[str, str]] = []
        self.expected_count: int | None = None
        self.current: dict[str, Any] | None = None
        self.capture_field: str | None = None
        self.capture_tag: str | None = None
        self.capture_parts: list[str] = []
        self.capture_count = False
        self.count_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        classes = class_names(attrs)
        attributes = dict(attrs)

        if tag == "span" and "post__count-num" in classes:
            self.capture_count = True
            self.count_parts = []

        if tag == "li" and "post__item" in classes:
            self.current = {
                "id": "",
                "title": "",
                "location_summary": "",
                "summary": "",
                "posting_url": "",
            }
            return

        if self.current is None:
            return

        if tag == "h3" and "post__item-ttl-sub" in classes:
            self._start_capture("title", tag)
        elif tag == "p" and "post__item-ttl-area" in classes:
            self._start_capture("location_summary", tag)
        elif tag == "p" and "post__item-content-text" in classes:
            self._start_capture("summary", tag)
        elif tag == "a":
            href = attributes.get("href") or ""
            parsed = urlparse(href)
            if parsed.netloc.lower() == ATS_HOST and parsed.path.endswith("/jobboard/detail.aspx"):
                self.current["posting_url"] = href
                self.current["id"] = parse_qs(parsed.query).get("id", [""])[0]

        if self.capture_field and tag == "br":
            self.capture_parts.append("\n")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.capture_field and tag == "br":
            self.capture_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self.capture_count and tag == "span":
            value = clean_text(self.count_parts)
            if value.isdigit():
                self.expected_count = int(value)
            self.capture_count = False

        if self.capture_field and tag == self.capture_tag:
            assert self.current is not None
            self.current[self.capture_field] = clean_text(self.capture_parts)
            self.capture_field = None
            self.capture_tag = None
            self.capture_parts = []

        if tag == "li" and self.current is not None:
            if self.current.get("id") and self.current.get("posting_url"):
                self.jobs.append({key: str(value) for key, value in self.current.items()})
            self.current = None
            self.capture_field = None
            self.capture_tag = None
            self.capture_parts = []

    def handle_data(self, data: str) -> None:
        if self.capture_count:
            self.count_parts.append(data)
        if self.capture_field:
            self.capture_parts.append(data)

    def _start_capture(self, field: str, tag: str) -> None:
        self.capture_field = field
        self.capture_tag = tag
        self.capture_parts = []


class SonarDetailParser(HTMLParser):
    """Extract structured sections from a SONAR ATS job detail page."""

    BLOCK_TAGS = {"br", "div", "li", "p", "tr"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title = ""
        self.sections: dict[str, str] = {}
        self.apply_control_present = False
        self.page_text: list[str] = []
        self._title_capture = False
        self._title_parts: list[str] = []
        self._section_depth = 0
        self._section_title_capture = False
        self._section_title_parts: list[str] = []
        self._section_body_started = False
        self._section_body_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        classes = class_names(attrs)
        attributes = dict(attrs)

        if tag == "h3" and "tit" in classes:
            self._title_capture = True
            self._title_parts = []

        if self._section_depth == 0 and tag == "div" and "box" in classes:
            self._section_depth = 1
            self._section_title_capture = False
            self._section_title_parts = []
            self._section_body_started = False
            self._section_body_parts = []
        elif self._section_depth > 0 and tag == "div":
            self._section_depth += 1

        if self._section_depth > 0:
            if tag == "h4" and "tit3" in classes:
                self._section_title_capture = True
                self._section_title_parts = []
            elif self._section_body_started and tag in self.BLOCK_TAGS:
                self._section_body_parts.append("\n")

        if tag == "a" and attributes.get("id") == "lkb_Apply":
            self.apply_control_present = True

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._section_depth > 0 and self._section_body_started and tag in self.BLOCK_TAGS:
            self._section_body_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self._title_capture and tag == "h3":
            self.title = clean_text(self._title_parts)
            self._title_capture = False

        if self._section_depth > 0 and self._section_title_capture and tag == "h4":
            self._section_title_capture = False
            self._section_body_started = True

        if self._section_depth > 0 and self._section_body_started and tag in self.BLOCK_TAGS:
            self._section_body_parts.append("\n")

        if self._section_depth > 0 and tag == "div":
            self._section_depth -= 1
            if self._section_depth == 0:
                title = clean_text(self._section_title_parts)
                body = clean_text(self._section_body_parts)
                if title:
                    self.sections[title] = body
                self._section_title_capture = False
                self._section_body_started = False

    def handle_data(self, data: str) -> None:
        self.page_text.append(data)
        if self._title_capture:
            self._title_parts.append(data)
        if self._section_depth > 0:
            if self._section_title_capture:
                self._section_title_parts.append(data)
            elif self._section_body_started:
                self._section_body_parts.append(data)

    @property
    def unavailable(self) -> bool:
        return UNAVAILABLE_TEXT in clean_text(self.page_text)


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
                    "Accept-Language": "ja,en;q=0.8",
                    "User-Agent": USER_AGENT,
                },
            )
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    raw = response.read()
                    content_type = response.headers.get_content_charset() or "utf-8"
                return raw.decode(content_type, "replace")
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


def collect_listing(client: HttpClient, *, max_jobs: int | None) -> list[dict[str, str]]:
    parser = CareersListParser()
    parser.feed(client.get_text(CAREERS_URL))
    parser.close()

    if parser.expected_count is not None and len(parser.jobs) != parser.expected_count:
        raise CollectorError(
            f"Career page advertises {parser.expected_count} jobs, but {len(parser.jobs)} were parsed"
        )
    if not parser.jobs:
        raise CollectorError("No Toyo Engineering jobs found on the career page")

    deduplicated: dict[str, dict[str, str]] = {}
    for job in parser.jobs:
        deduplicated[job["id"]] = job
    jobs = list(deduplicated.values())
    return jobs[:max_jobs] if max_jobs is not None else jobs


def parse_detail(html: str) -> SonarDetailParser:
    parser = SonarDetailParser()
    parser.feed(html)
    parser.close()
    return parser


def collect_details(
    client: HttpClient,
    jobs: list[dict[str, str]],
    *,
    workers: int,
) -> tuple[dict[str, SonarDetailParser], list[dict[str, str]]]:
    details: dict[str, SonarDetailParser] = {}
    errors: list[dict[str, str]] = []

    def fetch(job: dict[str, str]) -> SonarDetailParser:
        return parse_detail(client.get_text(job["posting_url"]))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch, job): job for job in jobs}
        for future in as_completed(futures):
            job = futures[future]
            try:
                detail = future.result()
                details[job["id"]] = detail
                if detail.unavailable:
                    errors.append({"job_id": job["id"], "error": UNAVAILABLE_TEXT})
                elif not detail.title:
                    errors.append({"job_id": job["id"], "error": "Detail page has no job title"})
            except Exception as exc:
                errors.append({"job_id": job["id"], "error": str(exc)})

    errors.sort(key=lambda item: item["job_id"])
    return details, errors


def section_value(sections: dict[str, str], *labels: str) -> str:
    for label in labels:
        if label in sections:
            return sections[label]
    return ""


def normalize_job(
    listing: dict[str, str],
    detail: SonarDetailParser | None,
) -> dict[str, Any]:
    detail_available = bool(detail and detail.title and not detail.unavailable)
    detail_error = ""
    if detail is None:
        detail_error = "Detail not requested or request failed"
    elif detail.unavailable:
        detail_error = UNAVAILABLE_TEXT
    elif not detail.title:
        detail_error = "Detail page has no job title"

    sections = detail.sections if detail_available and detail else {}
    location_summary = re.sub(r"^勤務地[：:]\s*", "", listing["location_summary"]).strip()
    location = section_value(sections, "勤務地") or location_summary
    title = detail.title if detail_available and detail else listing["title"]

    return {
        "id": listing["id"],
        "title": title,
        "company": "Toyo Engineering Corporation",
        "company_japanese": "東洋エンジニアリング株式会社",
        "ats": "SONAR ATS",
        "summary": listing["summary"],
        "location_summary": location_summary,
        "location": location,
        "employment_type": section_value(sections, "勤務形態"),
        "headcount": section_value(sections, "募集人員"),
        "department": section_value(sections, "募集部門"),
        "working_conditions": section_value(sections, "勤務時間／諸条件", "勤務時間・諸条件"),
        "responsibilities": section_value(sections, "具体的な業務例（下記に限りません）"),
        "career_path": section_value(sections, "中長期的なキャリアプラン"),
        "requirements": section_value(sections, "募集要件", "応募要件"),
        "candidate_profile": section_value(sections, "求める人物像"),
        "travel": section_value(sections, "出張"),
        "compensation": section_value(sections, "給与・昇給"),
        "sections": sections,
        "posting_url": listing["posting_url"],
        "apply_url": listing["posting_url"] if detail_available and detail and detail.apply_control_present else "",
        "detail_available": detail_available,
        "detail_error": detail_error,
        "source_careers_url": CAREERS_URL,
    }


def matches_filters(job: dict[str, Any], *, keywords: list[str], locations: list[str]) -> bool:
    if keywords:
        haystack = "\n".join(
            [
                str(job.get("title", "")),
                str(job.get("summary", "")),
                "\n".join(str(value) for value in job.get("sections", {}).values()),
            ]
        ).casefold()
        if not any(keyword.casefold() in haystack for keyword in keywords):
            return False
    if locations:
        location = " ".join(
            [str(job.get("location_summary", "")), str(job.get("location", ""))]
        ).casefold()
        if not any(value.casefold() in location for value in locations):
            return False
    return True


def csv_row(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": job["id"],
        "title": job["title"],
        "company": job["company"],
        "company_japanese": job["company_japanese"],
        "summary": job["summary"],
        "location_summary": job["location_summary"],
        "location": job["location"],
        "employment_type": job["employment_type"],
        "headcount": job["headcount"],
        "department": job["department"],
        "working_conditions": job["working_conditions"],
        "responsibilities": job["responsibilities"],
        "career_path": job["career_path"],
        "requirements": job["requirements"],
        "candidate_profile": job["candidate_profile"],
        "travel": job["travel"],
        "compensation": job["compensation"],
        "posting_url": job["posting_url"],
        "apply_url": job["apply_url"],
        "detail_available": job["detail_available"],
        "detail_error": job["detail_error"],
        "sections_json": json.dumps(job["sections"], ensure_ascii=False, sort_keys=True),
        "source_careers_url": job["source_careers_url"],
    }


def empty_job() -> dict[str, Any]:
    return {
        "id": "",
        "title": "",
        "company": "",
        "company_japanese": "",
        "summary": "",
        "location_summary": "",
        "location": "",
        "employment_type": "",
        "headcount": "",
        "department": "",
        "working_conditions": "",
        "responsibilities": "",
        "career_path": "",
        "requirements": "",
        "candidate_profile": "",
        "travel": "",
        "compensation": "",
        "posting_url": "",
        "apply_url": "",
        "detail_available": False,
        "detail_error": "",
        "sections": {},
        "source_careers_url": "",
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
        description="Collect Toyo Engineering career jobs and SONAR ATS details."
    )
    parser.add_argument("--output-dir", type=Path, default=script_dir / "data")
    parser.add_argument("--prefix", default="toyo_engineering_jobs")
    parser.add_argument("--format", choices=("json", "csv", "both"), default="both")
    parser.add_argument("--keyword", action="append", default=[], help="Repeatable job-text filter")
    parser.add_argument("--location", action="append", default=[], help="Repeatable location filter")
    parser.add_argument("--max-jobs", type=int, default=None, help="Limit jobs, useful for tests")
    parser.add_argument("--workers", type=int, default=8, choices=range(1, 17), metavar="1-16")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument(
        "--no-details",
        dest="include_details",
        action="store_false",
        help="Skip SONAR detail requests",
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
    collected_at = utc_now()

    try:
        listings = collect_listing(client, max_jobs=args.max_jobs)
        details: dict[str, SonarDetailParser] = {}
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
        if matches_filters(job, keywords=args.keyword, locations=args.location)
    ]

    payload = {
        "metadata": {
            "collector": "toyo-engineering-sonar",
            "collector_version": "1.0.0",
            "collected_at": collected_at,
            "company": "Toyo Engineering Corporation",
            "company_japanese": "東洋エンジニアリング株式会社",
            "ats": "SONAR ATS",
            "careers_url": CAREERS_URL,
            "ats_host": ATS_HOST,
            "details_included": args.include_details,
            "job_count": len(jobs),
            "available_detail_count": sum(job["detail_available"] for job in jobs),
            "detail_error_count": len(detail_errors),
            "detail_errors": detail_errors,
            "filters": {
                "keywords": args.keyword,
                "locations": args.location,
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

    print(f"Collected {len(jobs)} Toyo Engineering jobs.")
    if detail_errors:
        print(f"Notice: {len(detail_errors)} unavailable or failed detail page(s); listings were retained.")
    for path in output_paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
