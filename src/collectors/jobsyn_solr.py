from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


API_URL = "https://prod-search-api.jobsyn.org/api/v1/solr/search"


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _pick(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, "", [], {}):
            return d[k]
    return default


def _first_list_of_dicts(obj: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            return obj
        for x in obj:
            res = _first_list_of_dicts(x)
            if res is not None:
                return res
        return None

    if isinstance(obj, dict):
        for k in ("docs", "jobs", "results", "items", "data", "response", "payload"):
            if k in obj:
                res = _first_list_of_dicts(obj[k])
                if res is not None:
                    return res
        for v in obj.values():
            res = _first_list_of_dicts(v)
            if res is not None:
                return res

    return None


def _extract_docs(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        jobs = payload.get("jobs")
        if isinstance(jobs, list) and all(isinstance(x, dict) for x in jobs):
            return jobs

        resp = payload.get("response")
        if isinstance(resp, dict):
            docs = resp.get("docs")
            if isinstance(docs, list) and all(isinstance(x, dict) for x in docs):
                return docs

        data = payload.get("data")
        if isinstance(data, dict):
            resp2 = data.get("response")
            if isinstance(resp2, dict):
                docs2 = resp2.get("docs")
                if isinstance(docs2, list) and all(isinstance(x, dict) for x in docs2):
                    return docs2

        docs3 = payload.get("docs")
        if isinstance(docs3, list) and all(isinstance(x, dict) for x in docs3):
            return docs3

    return _first_list_of_dicts(payload) or []


def _normalize_date(v: Any) -> str:
    if v is None:
        return ""

    if isinstance(v, (int, float)):
        ts = float(v)
        if ts > 10_000_000_000:  # likely ms
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.date().isoformat()

    s = _clean_text(v)
    if not s:
        return ""

    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            dt2 = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt2.date().isoformat()
        except Exception:
            return s[:10]

    try:
        dt3 = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt3.date().isoformat()
    except Exception:
        return ""


def _base_from_careers_url(careers_url: str) -> str:
    u = urlparse(careers_url)
    if not u.scheme or not u.netloc:
        return careers_url.rstrip("/")
    return f"{u.scheme}://{u.netloc}".rstrip("/")


def _headers_for_site(careers_url: str) -> Dict[str, str]:
    base = _base_from_careers_url(careers_url)
    u = urlparse(careers_url)
    netloc = u.netloc
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "origin": base,
        "referer": base + "/",
        "x-origin": netloc,
        "user-agent": "Mozilla/5.0",
    }


def _extract_job_id(job: Dict[str, Any]) -> str:
    jid = _pick(
        job,
        [
            "job_id",
            "jobId",
            "jobID",
            "req_id",
            "reqId",
            "requisitionId",
            "requisition_id",
            "reqid",
            "id",
            "uuid",
        ],
        "",
    )
    return _clean_text(jid)


def _extract_title(job: Dict[str, Any]) -> str:
    return _clean_text(
        _pick(
            job,
            [
                "job_title",
                "jobTitle",
                "title",
                "positionTitle",
                "position_title",
                "job_title_s",
                "job_title_t",
                "title_s",
                "title_t",
                "title_exact",
                "title_slab_exact",
                "posting_title",
                "seo_job_title",
                "seo_title",
                "name",
            ],
            "",
        )
    )


def _format_location(job: Dict[str, Any]) -> str:
    loc = _pick(
        job,
        [
            "location",
            "location_name",
            "jobLocation",
            "job_location",
            "location_s",
            "location_display",
            "location_text",
            "job_location_s",
            "location_exact",
        ],
        "",
    )
    if isinstance(loc, str) and loc.strip():
        return _clean_text(loc)

    all_locations = _pick(job, ["all_locations", "allLocations"], None)
    if isinstance(all_locations, list):
        parts = [_clean_text(x) for x in all_locations if _clean_text(x)]
        if parts:
            return ", ".join(parts)

    city = _pick(job, ["city", "jobCity", "city_exact"], "")
    state = _pick(job, ["state", "region", "jobState"], "")
    country = _pick(job, ["country", "jobCountry", "country_exact", "country_short_exact"], "")

    parts2 = [p for p in [_clean_text(city), _clean_text(state), _clean_text(country)] if p]
    return ", ".join(parts2).strip()


def _extract_posted_date(job: Dict[str, Any]) -> str:
    return _normalize_date(
        _pick(
            job,
            [
                "posted_date",
                "postedDate",
                "datePosted",
                "date_posted",
                "postedAt",
                "createdAt",
                "createDate",
                "created_date",
                "date_added",
                "date_new",
                "date_updated",
                "salted_date",
            ],
            None,
        )
    )


def _extract_job_url(job: Dict[str, Any], careers_url: str) -> str:
    url = _pick(
        job,
        [
            "job_url",
            "jobUrl",
            "url",
            "applyUrl",
            "apply_url",
            "canonicalUrl",
            "canonical_url",
            "detailUrl",
            "detail_url",
            "job_url_s",
            "apply_url_s",
            "canonical_url_s",
            "seo_url",
            "seoUrl",
        ],
        "",
    )
    if isinstance(url, str) and url.strip():
        u = url.strip()
        if u.startswith("/"):
            return _base_from_careers_url(careers_url) + u
        return u
    return ""


class JobsynSolrCollector(BaseCollector):
    name = "jobsyn_solr"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "status_codes": []}

        location = "singapore"
        num_items = 15
        max_pages = 200

        try:
            with requests.Session() as session:
                session.headers.update(_headers_for_site(company.careers_url))

                seen: set[str] = set()
                for page in range(1, max_pages + 1):
                    params = {"location": location, "page": str(page), "num_items": str(num_items)}
                    r = session.get(API_URL, params=params, timeout=30)
                    meta["pages"] += 1
                    meta["status_codes"].append(r.status_code)
                    r.raise_for_status()

                    payload = r.json()
                    docs = _extract_docs(payload)
                    if not docs:
                        break

                    new_on_page = 0
                    for d in docs:
                        if not isinstance(d, dict):
                            continue
                        key = _extract_job_id(d) or _extract_job_url(d, company.careers_url)
                        if not key or key in seen:
                            continue
                        seen.add(key)
                        raw_jobs.append(d)
                        new_on_page += 1

                    if new_on_page == 0:
                        break

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
            job_id = _extract_job_id(raw)
            title = _extract_title(raw)
            location = _format_location(raw)
            posted_date = _extract_posted_date(raw)
            job_url = _extract_job_url(raw, result.careers_url)

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
