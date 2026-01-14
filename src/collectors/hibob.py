from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _make_session(companyidentifier: str) -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "companyidentifier": companyidentifier,
        }
    )
    return s


def _base_from_url(url: str) -> str:
    u = urlparse(url)
    if not u.scheme or not u.netloc:
        return url.rstrip("/")
    return f"{u.scheme}://{u.netloc}".rstrip("/")


def _companyidentifier_from_url(url: str) -> str:
    # HiBob expects a `companyidentifier` header. For most tenants, this is the first label.
    u = urlparse(url)
    host = (u.netloc or "").split(":")[0]
    return (host.split(".")[0] if host else "").strip()


def _extract_filters(careers_url: str, raw_data_row: Dict[str, Any]) -> tuple[str, Optional[str]]:
    """Return (country_filter, site_filter). Defaults to Singapore-only to match test script behavior."""
    # Default behavior: keep only Singapore jobs unless caller overrides via query or row fields.
    country = "singapore"
    site: Optional[str] = None

    try:
        qs = parse_qs(urlparse(careers_url).query)
        if qs.get("country") and qs["country"][0].strip():
            country = qs["country"][0].strip()
        if qs.get("site") and qs["site"][0].strip():
            site = qs["site"][0].strip()
    except Exception:
        pass

    # Excel optional columns
    for key in ("country", "Country"):
        v = raw_data_row.get(key)
        if v and str(v).strip():
            country = str(v).strip()
            break
    for key in ("site", "Site", "city", "City"):
        v = raw_data_row.get(key)
        if v and str(v).strip():
            site = str(v).strip()
            break

    return country, site


# Known fallbacks for broken/blank careers_url values in Excel.
HIBOB_FALLBACKS: Dict[str, Dict[str, str]] = {
    "global maritime": {
        "base_url": "https://globalmaritime.careers.hibob.com",
        "companyidentifier": "globalmaritime",
    }
}


class HibobCollector(BaseCollector):
    name = "hibob"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None, "companyidentifier": None, "base_url": None, "country_filter": None, "site_filter": None}

        try:
            base = _base_from_url(company.careers_url)
            companyidentifier = _companyidentifier_from_url(company.careers_url)

            # Fallback: Excel row has garbage URL (e.g. '/.'). Use known tenant defaults.
            fallback = HIBOB_FALLBACKS.get(company.company.strip().lower())
            if (not base or "://" not in base or not companyidentifier) and fallback:
                base = fallback["base_url"].rstrip("/")
                companyidentifier = fallback["companyidentifier"].strip()

            if not companyidentifier or "://" not in base:
                raise ValueError("Could not derive companyidentifier/base_url from careers_url")

            country_filter, site_filter = _extract_filters(company.careers_url, company.raw_data_row)

            meta["companyidentifier"] = companyidentifier
            meta["base_url"] = base
            meta["country_filter"] = country_filter
            meta["site_filter"] = site_filter

            with _make_session(companyidentifier) as session:
                url = base + "/api/job-ad"
                r = session.get(url, timeout=30)
                meta["status"] = r.status_code
                r.raise_for_status()
                data = r.json()

            job_ads = data.get("jobAdDetails") or []
            if isinstance(job_ads, list):
                raw_jobs = [j for j in job_ads if isinstance(j, dict)]

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

        base = _clean_text(result.meta.get("base_url")) or _base_from_url(result.careers_url)
        country_filter = _clean_text(result.meta.get("country_filter") or "").lower() or "singapore"
        site_filter = _clean_text(result.meta.get("site_filter") or "") or None

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            job_id = _clean_text(raw.get("id"))
            title = _clean_text(raw.get("title"))
            country = _clean_text(raw.get("country"))
            site = _clean_text(raw.get("site"))
            posted_date = _clean_text(raw.get("publishedAt"))
            if country_filter:
                if country.lower() != country_filter and (not site_filter or site.lower() != site_filter.lower()):
                    continue

            if site_filter and site.lower() != site_filter.lower():
                continue

            location = site or country
            job_url = base + f"/jobs/{job_id}" if job_id else ""

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
