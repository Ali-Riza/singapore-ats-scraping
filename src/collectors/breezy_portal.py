from __future__ import annotations

import html as html_lib
import re
import time
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _strip_tags(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: float = 45.0,
    max_attempts: int = 3,
    backoff_s: float = 0.8,
) -> requests.Response:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout, headers={"Accept": "text/html,*/*"})
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_s * attempt)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def _extract_position_paths(html: str) -> List[str]:
    patterns = [
        r'href="(/p/[^"#?\s]+)',
        r"href='(/p/[^'#?\s]+)",
        r'href="(https?://[^"\s]+/p/[^"#?\s]+)',
    ]

    found: Set[str] = set()
    for pat in patterns:
        for m in re.findall(pat, html, flags=re.IGNORECASE):
            if isinstance(m, tuple):
                m = m[0]
            found.add(m)

    return sorted(found)


def _guess_job_id_from_path(path_or_url: str) -> str:
    m = re.search(r"/p/([^/?#]+)", path_or_url)
    return m.group(1) if m else ""


def _parse_job_detail(html: str) -> tuple[str, str]:
    title = ""
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, flags=re.IGNORECASE | re.DOTALL)
    if m:
        title = _strip_tags(m.group(1))

    location = ""
    m = re.search(
        r'class=\"[^\"]*location[^\"]*\"[^>]*>(.*?)</',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        location = _strip_tags(m.group(1))

    return title, location


def _looks_singapore(job_title: str, location: str, job_url: str) -> bool:
    hay = " ".join([job_title or "", location or "", job_url or ""]).lower()
    return "singapore" in hay or "\bsg\b" in hay


class BreezyPortalCollector(BaseCollector):
    name = "breezy_portal"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"discovered_links": 0}

        try:
            careers_url = company.careers_url
            if not careers_url:
                raise ValueError("Missing careers_url")

            session = requests.Session()
            resp = _get_with_retries(session, careers_url)

            paths = _extract_position_paths(resp.text)
            meta["discovered_links"] = len(paths)

            for p in paths:
                job_url = p if p.startswith("http") else urljoin(careers_url, p)
                job_id = _guess_job_id_from_path(job_url)

                try:
                    detail_resp = _get_with_retries(session, job_url)
                    title, location = _parse_job_detail(detail_resp.text)
                except Exception:
                    title, location = "", ""

                if not _looks_singapore(title, location, job_url):
                    continue

                raw_jobs.append(
                    {
                        "job_id": job_id,
                        "title": title,
                        "location": location,
                        "posted_date": "",
                        "job_url": job_url,
                    }
                )

            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )
        except Exception as e:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(e),
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        out: List[JobRecord] = []
        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue
            out.append(
                JobRecord(
                    company=result.company,
                    job_title=_clean_text(raw.get("title")),
                    location=_clean_text(raw.get("location")),
                    job_id=_clean_text(raw.get("job_id")),
                    posted_date=_clean_text(raw.get("posted_date")),
                    job_url=_clean_text(raw.get("job_url")),
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return out
