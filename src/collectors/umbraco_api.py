from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import quote, urlparse

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _base_from_url(url: str) -> str:
    u = urlparse(url)
    if not u.scheme or not u.netloc:
        return url.rstrip("/")
    return f"{u.scheme}://{u.netloc}".rstrip("/")


def _looks_singapore(*vals: str) -> bool:
    hay = " ".join([v or "" for v in vals]).lower()
    return "singapore" in hay or "\bsg\b" in hay


def _api_url_for_country(base: str, country: str) -> str:
    country = (country or "").strip()
    if not country:
        return base

    segment = quote(country.lower(), safe="")
    return base.rstrip("/") + "/" + segment


class UmbracoApiCollector(BaseCollector):
    name = "umbraco_api"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None, "api_url": None}

        try:
            base = _base_from_url(company.careers_url)
            api_base_url = base + "/umbraco/api/v1/vacancies/"
            api_url = _api_url_for_country(api_base_url, "singapore")
            meta["api_url"] = api_url

            r = requests.get(api_url, timeout=30, headers={"Accept": "application/json,text/plain,*/*"})
            meta["status"] = r.status_code
            r.raise_for_status()
            data = r.json()

            items = data if isinstance(data, list) else []

            for j in items:
                if not isinstance(j, dict):
                    continue
                title = _clean_text(j.get("Name") or j.get("title"))
                location = _clean_text(j.get("Location") or j.get("location"))
                url = _clean_text(j.get("Url") or j.get("url") or j.get("jobUrl") or "")
                if url and url.startswith("/"):
                    url = base + url

                # The endpoint is already country-filtered; keep a defensive filter anyway.
                if not _looks_singapore(title, location, url):
                    continue
                raw_jobs.append(j)

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
        base = _base_from_url(result.careers_url)

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue
            title = _clean_text(raw.get("Name") or raw.get("title"))
            location = _clean_text(raw.get("Location") or raw.get("location"))
            job_id = _clean_text(raw.get("Id") or raw.get("id") or raw.get("vacancyId") or raw.get("jobId"))
            posted_date = _clean_text(
                raw.get("DatePosted")
                or raw.get("FormattedDatePosted")
                or raw.get("postedDate")
                or raw.get("date")
                or raw.get("posted_date")
            )
            job_url = _clean_text(raw.get("Url") or raw.get("url") or raw.get("jobUrl") or "")
            if job_url and job_url.startswith("/"):
                job_url = base + job_url

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=location,
                    job_id=job_id,
                    posted_date=posted_date,
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return out
