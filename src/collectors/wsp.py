from __future__ import annotations

import re
import time
from typing import Any, Dict, List
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

import requests

from src.collectors.base import BaseCollector
from src.core.models import CollectResult, CompanyItem, JobRecord

# Each job card renders an <a> whose data-value holds a dataLayer JSON blob with
# link_url / link_text / job_city. Quotes inside it are single quotes.
_JOB_LINK_RE = re.compile(
    r'<a\s+href="(?P<url>https://applr\.io/l/(?P<job_id>[A-Za-z0-9]+)[^"]*)"'
    r'\s+data-value="(?P<data>\{.*?\})"',
    re.DOTALL,
)
_FIELD_RE = {
    "title": re.compile(r"'link_text':\s*'(?P<v>[^']*)'"),
    "city": re.compile(r"'job_city':\s*'(?P<v>[^']*)'"),
    "country": re.compile(r"'job_country':\s*'(?P<v>[^']*)'"),
}


def _unescape(value: str) -> str:
    return (
        value.replace("&amp;", "&")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
    )


def _parse_wsp_markdown(html: str) -> List[Dict[str, Any]]:
    """Parse job cards out of the WSP careers page HTML."""
    jobs: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for match in _JOB_LINK_RE.finditer(html):
        job_id = match.group("job_id")
        if job_id in seen:
            continue

        blob = match.group("data")
        fields = {
            key: (pattern.search(blob).group("v").strip() if pattern.search(blob) else "")
            for key, pattern in _FIELD_RE.items()
        }
        title = _unescape(fields["title"])
        if not title:
            continue

        location = _unescape(fields["city"]) or _unescape(fields["country"]) or "Singapore"
        seen.add(job_id)
        jobs.append(
            {
                "job_id": job_id,
                "title": title,
                "location": location,
                "posted_date": "",
                "job_url": _unescape(match.group("url")),
            }
        )
    return jobs


def _reported_total(html: str) -> int:
    match = re.search(r"Showing\s+(\d+)\s+jobs", html, re.IGNORECASE)
    return int(match.group(1)) if match else 0


def _last_page(html: str) -> int:
    """Highest page number offered by the pagination widget (1 when absent)."""
    values = [int(v) for v in re.findall(r'name="page"[^>]*data-value="(\d+)"', html)]
    values += [int(v) for v in re.findall(r'data-value="(\d+)"[^>]*name="page"', html)]
    return max(values) if values else 1


def _with_params(url: str, **params: Any) -> str:
    parts = urlparse(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({key: str(value) for key, value in params.items()})
    return urlunparse(parts._replace(query=urlencode(query)))


class WspCollector(BaseCollector):
    name = "wsp"

    # WSP renders 20 job cards per page; the rest live behind ?page=N.
    page_size = 20
    max_pages = 100

    def _fetch(self, url: str, meta: Dict[str, Any]) -> str:
        # Jina's markdown mode drops the job-card list entirely, so ask for HTML.
        response = requests.get(
            f"https://r.jina.ai/{url}",
            headers={"x-respond-with": "html"},
            timeout=60,
        )
        meta["status"].append(response.status_code)
        response.raise_for_status()
        return response.text

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "status": [], "total_raw": 0, "fetch_mode": "jina_html"}
        try:
            scrape_timestamp = int(time.time())
            first_url = _with_params(company.careers_url, _scrape_ts=scrape_timestamp)
            html = self._fetch(first_url, meta)

            raw_jobs = _parse_wsp_markdown(html)
            total_jobs = _reported_total(html)
            last_page = _last_page(html)
            if total_jobs:
                # Trust whichever signal implies more pages.
                last_page = max(last_page, -(-total_jobs // self.page_size))

            seen = {job["job_id"] for job in raw_jobs}
            for page_number in range(2, min(last_page, self.max_pages) + 1):
                page_url = _with_params(
                    company.careers_url, page=page_number, _scrape_ts=scrape_timestamp
                )
                page_html = self._fetch(page_url, meta)
                new_jobs = [j for j in _parse_wsp_markdown(page_html) if j["job_id"] not in seen]
                if not new_jobs:
                    break
                raw_jobs.extend(new_jobs)
                seen.update(job["job_id"] for job in new_jobs)

            meta["pages"] = len(meta["status"])
            meta["reported_total"] = total_jobs
            meta["last_page"] = last_page
            meta["total_raw"] = len(raw_jobs)
            if total_jobs and len(raw_jobs) < total_jobs:
                meta["incomplete"] = True
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
        usable = [
            raw
            for raw in result.raw_jobs
            if isinstance(raw, dict) and raw.get("job_id") and raw.get("title") and raw.get("job_url")
        ]

        # WSP posts several distinct requisitions that share both title and
        # location (verified: different job_ids resolve to different postings).
        # The shared dedupe key is (company, job_title), so a bare title would
        # collapse them into one row and silently drop real jobs. Suffix the
        # job_id only on the titles that actually repeat, keeping unique titles
        # untouched.
        title_counts: Dict[str, int] = {}
        for raw in usable:
            title = str(raw.get("title") or "").strip()
            title_counts[title] = title_counts.get(title, 0) + 1

        records: List[JobRecord] = []
        for raw in usable:
            title = str(raw.get("title") or "").strip()
            job_id = str(raw.get("job_id") or "").strip()
            records.append(
                JobRecord(
                    company=result.company,
                    job_title=f"{title} ({job_id})" if title_counts[title] > 1 else title,
                    location=str(raw.get("location") or "Singapore").strip(),
                    job_id=job_id,
                    posted_date=str(raw.get("posted_date") or "").strip(),
                    job_url=str(raw.get("job_url") or "").strip(),
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return records
