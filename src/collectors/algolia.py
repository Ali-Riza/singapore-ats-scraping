from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


def _pick(d: Dict[str, Any], keys: Sequence[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return default


def _normalize_date(raw: Any) -> str:
    if raw is None:
        return ""

    if isinstance(raw, (int, float)):
        try:
            return datetime.utcfromtimestamp(float(raw)).date().isoformat()
        except Exception:
            return str(raw)

    s = str(raw).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue

    # Some pages expose e.g. "Dec 22, 2025"
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue

    return s


def _extract_posted_date_from_html(html: str) -> str:
    """Prefer JSON-LD datePosted if present; else try text patterns."""
    soup = BeautifulSoup(html, "html.parser")

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "datePosted" in item:
                    return _normalize_date(item.get("datePosted"))
        elif isinstance(data, dict):
            if "datePosted" in data:
                return _normalize_date(data.get("datePosted"))

    text = soup.get_text(separator=" ", strip=True)
    patterns = [
        r"Posted[:\s]+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"Date posted[:\s]+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"Posting date[:\s]+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return _normalize_date(m.group(1))

    return ""


@dataclass(frozen=True)
class AlgoliaConfig:
    app_id: str
    api_key: str
    index_name: str
    origin: str
    base_site: str

    facet_filters_singapore: str
    hits_per_page: int = 50
    max_pages: int = 200

    # Optional additional queries used by some implementations
    extra_queries: Tuple[Dict[str, Any], ...] = ()

    # Some tenants require a detail fetch to get posted_date.
    needs_detail_posted_date: bool = False


def _config_for_company(company: CompanyItem) -> AlgoliaConfig:
    name = (company.company or "").strip().lower()

    # DNV
    if name == "dnv":
        return AlgoliaConfig(
            app_id="RVMOB42DFH",
            api_key="fd9e8d499b1d7ede4cd848b00aef0c65",
            index_name="production__dnvvcare2301__sort-rank",
            origin="https://jobs.dnv.com",
            base_site="https://jobs.dnv.com",
            facet_filters_singapore='[["country:Singapore"]]',
            hits_per_page=50,
            max_pages=200,
            extra_queries=(),
            needs_detail_posted_date=True,
        )

    # Johnson Controls
    if name == "johnson controls" or name == "johnsoncontrols":
        # Locations are in facet locations_list:Singapore
        extra2_params = {
            "analytics": "false",
            "clickAnalytics": "false",
            "facets": "locations_list",
            "highlightPostTag": "__/ais-highlight__",
            "highlightPreTag": "__ais-highlight__",
            "hitsPerPage": "0",
            "maxValuesPerFacet": "50",
            "page": "0",
            "query": "",
        }
        return AlgoliaConfig(
            app_id="UM59DWRPA1",
            api_key="33719eb8d9f28725f375583b7e78dbab",
            index_name="production_JCI_jobs",
            origin="https://jobs.johnsoncontrols.com",
            base_site="https://jobs.johnsoncontrols.com",
            facet_filters_singapore='[["locations_list:Singapore"]]',
            hits_per_page=50,
            max_pages=100,
            extra_queries=(
                {
                    "indexName": "production_JCI_jobs",
                    "params": urlencode(extra2_params),
                },
            ),
            needs_detail_posted_date=False,
        )

    raise RuntimeError(f"No Algolia config for company: {company.company}")


def _algolia_endpoint(app_id: str) -> str:
    return f"https://{app_id.lower()}-dsn.algolia.net/1/indexes/*/queries"


def _build_primary_params(cfg: AlgoliaConfig, page: int) -> str:
    params: Dict[str, str] = {
        "facetFilters": cfg.facet_filters_singapore,
        "facets": '["employee_type","job_family_group","locations_list","parent_category"]',
        "getRankingInfo": "true",
        "highlightPostTag": "__/ais-highlight__",
        "highlightPreTag": "__ais-highlight__",
        "hitsPerPage": str(cfg.hits_per_page),
        "maxValuesPerFacet": "999",
        "page": str(page),
        "query": "",
    }

    # DNV uses different facet list in the test; keep it closer by switching.
    if cfg.base_site == "https://jobs.dnv.com":
        params["facets"] = '["business_unit","contract_type","country","position_type"]'
        params["maxValuesPerFacet"] = "999"

    return urlencode(params)


def _job_url_from_hit(cfg: AlgoliaConfig, hit: Dict[str, Any]) -> str:
    # Prefer a job *detail* URL on the same site (base_site/origin) if available.
    # Some tenants (e.g., DNV) provide external apply links (OracleCloud) that do not
    # contain datePosted; using the on-site detail URL enables detail scraping.
    def _collect_url_candidates(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            v = value.strip()
            return [v] if v else []
        if isinstance(value, (list, tuple, set)):
            out: List[str] = []
            for item in value:
                out.extend(_collect_url_candidates(item))
            return out
        if isinstance(value, dict):
            # Common patterns: {"url": "..."} or {"href": "..."}
            return _collect_url_candidates(value.get("url")) + _collect_url_candidates(value.get("href"))
        return []

    candidates: List[str] = []
    for k in [
        "job_url",
        "jobUrl",
        "url",
        "job_detail_url",
        "jobDetailUrl",
        "detail_url",
        "detailUrl",
        "jd_url",
        "apply_url",
        "applyUrl",
        "application_url",  # Johnson Controls: list of apply links
        "external_url",
        "externalUrl",
    ]:
        candidates.extend(_collect_url_candidates(hit.get(k)))

    def _to_abs(u: str) -> str:
        if u.startswith("/"):
            return cfg.base_site.rstrip("/") + u
        return u

    abs_candidates = [_to_abs(u) for u in candidates]

    preferred_prefixes = [cfg.base_site.rstrip("/"), cfg.origin.rstrip("/")]
    for u in abs_candidates:
        for pfx in preferred_prefixes:
            if pfx and u.startswith(pfx):
                return u

    for u in abs_candidates:
        if u.startswith("http"):
            return u

    jd_url = hit.get("jd_url")
    if isinstance(jd_url, str) and jd_url:
        if jd_url.startswith("/"):
            return cfg.base_site.rstrip("/") + jd_url
        return jd_url

    slug = _pick(hit, ["slug", "job_slug", "jobSlug", "url_slug"], default=None)
    for s in _collect_url_candidates(slug):
        if s.startswith("http"):
            return s
        return f"{cfg.base_site.rstrip('/')}/job/{s.lstrip('/')}"

    return ""


class AlgoliaCollector(BaseCollector):
    """Collector for Algolia-backed career sites.

    Currently supports the two known tenants from tests: DNV + Johnson Controls.
    """

    name = "algolia"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "status_codes": [],
            "total_raw": 0,
            "detail_fetches": 0,
            "app_id": "",
            "index": "",
        }

        try:
            cfg = _config_for_company(company)
            meta["app_id"] = cfg.app_id
            meta["index"] = cfg.index_name

            session = requests.Session()
            headers = {
                "Accept": "*/*",
                "Origin": cfg.origin,
                "Referer": cfg.origin + "/",
                "User-Agent": "Mozilla/5.0",
                "x-algolia-application-id": cfg.app_id,
                "x-algolia-api-key": cfg.api_key,
            }

            seen_ids: Set[str] = set()

            for page in range(cfg.max_pages):
                primary = {
                    "indexName": cfg.index_name,
                    "params": _build_primary_params(cfg, page),
                }

                payload = {"requests": [primary, *cfg.extra_queries]}
                r = session.post(_algolia_endpoint(cfg.app_id), headers=headers, json=payload, timeout=30)
                meta["status_codes"].append(r.status_code)
                r.raise_for_status()

                data = r.json()
                results = data.get("results") or []
                if not results:
                    break

                hits = results[0].get("hits") or []
                if not hits:
                    break

                added = 0
                for hit in hits:
                    job_id = _pick(
                        hit,
                        ["job_id", "jobId", "requisition_id", "requisitionId", "req_id", "id", "objectID"],
                        default=None,
                    )
                    job_id_s = str(job_id) if job_id is not None else ""
                    if not job_id_s or job_id_s in seen_ids:
                        continue
                    seen_ids.add(job_id_s)

                    title = _pick(hit, ["job_title", "jobTitle", "title", "name", "position_title"], default="")
                    location = _pick(
                        hit,
                        ["location", "locations", "locations_list", "locationsList", "city", "location_name", "display_location"],
                        default="",
                    )
                    if isinstance(location, list):
                        location = ", ".join([str(x) for x in location if x])

                    posted = _pick(
                        hit,
                        ["posted_date", "postedDate", "date", "created_at", "createdAt", "first_seen", "published_at"],
                        default="",
                    )

                    job_url = _job_url_from_hit(cfg, hit)

                    raw_jobs.append(
                        {
                            "job_id": job_id_s,
                            "title": _clean_text(str(title or "")),
                            "location": _clean_text(str(location or "")),
                            "posted_date": _normalize_date(posted) if posted else "",
                            "job_url": job_url,
                            "_hit": hit,
                            "_page": page,
                        }
                    )
                    added += 1

                meta["pages"] += 1
                if added == 0:
                    break

            # Optional detail fetch for posted_date
            if cfg.needs_detail_posted_date:
                need = [r for r in raw_jobs if not r.get("posted_date") and r.get("job_url")]
                budget = min(len(need), 400)
                need = need[:budget]

                def _fetch(one_url: str) -> Tuple[str, str]:
                    dr = session.get(one_url, timeout=20)
                    meta["status_codes"].append(dr.status_code)
                    dr.raise_for_status()
                    return one_url, _extract_posted_date_from_html(dr.text)

                url_to_date: Dict[str, str] = {}
                if need:
                    with ThreadPoolExecutor(max_workers=12) as ex:
                        futures = [ex.submit(_fetch, str(item["job_url"])) for item in need]
                        for fut in as_completed(futures):
                            try:
                                url, date = fut.result()
                                meta["detail_fetches"] += 1
                                if date:
                                    url_to_date[url] = date
                            except Exception:
                                continue

                for item in raw_jobs:
                    u = str(item.get("job_url") or "")
                    if u and not item.get("posted_date") and u in url_to_date:
                        item["posted_date"] = url_to_date[u]

            meta["total_raw"] = len(raw_jobs)
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )

        except Exception as e:
            meta["total_raw"] = len(raw_jobs)
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(e),
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        return [self._map_one(raw, result) for raw in result.raw_jobs]

    def _map_one(self, raw: Dict[str, Any], result: CollectResult) -> JobRecord:
        job_id = _clean_text(str(raw.get("job_id") or ""))
        title = _clean_text(str(raw.get("title") or ""))
        location = _clean_text(str(raw.get("location") or ""))
        posted_date = _clean_text(str(raw.get("posted_date") or ""))
        job_url = str(raw.get("job_url") or "")

        return JobRecord(
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
