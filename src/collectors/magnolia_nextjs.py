from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _make_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; ATS-Scraper/1.0)",
            "Accept": "application/json,text/html,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def _base_from_url(url: str) -> str:
    u = urlparse(url)
    if not u.scheme or not u.netloc:
        return url.rstrip("/")
    return f"{u.scheme}://{u.netloc}".rstrip("/")


def _extract_build_id(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        raise RuntimeError("Could not find __NEXT_DATA__")
    data = json.loads(script.string)
    build_id = data.get("buildId")
    if not build_id:
        raise RuntimeError("Could not extract buildId")
    return str(build_id)


def _pick_locale_path(path: str) -> str:
    # Applus pattern: /en -> data path /en.json
    parts = [p for p in (path or "").split("/") if p]
    if not parts:
        return "/en"
    return "/" + parts[0]


def _make_data_url(base: str, build_id: str, locale_path: str, params: Dict[str, Any]) -> str:
    qs_parts: List[str] = []
    for k, v in params.items():
        if v is None or v == "":
            continue
        qs_parts.append(f"{k}={v}")
    qs = "&".join(qs_parts)
    return f"{base}/_next/data/{build_id}{locale_path}.json" + (f"?{qs}" if qs else "")


class MagnoliaNextJsCollector(BaseCollector):
    name = "magnolia_nextjs"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": [], "countryID": None}

        try:
            careers_url = company.careers_url
            base = _base_from_url(careers_url)
            parsed = urlparse(careers_url)
            locale_path = _pick_locale_path(parsed.path)

            qs = parse_qs(parsed.query)
            country_id = (qs.get("countryID") or qs.get("countryId") or qs.get("country") or [None])[0]
            published_time_type_id = (qs.get("publishedTimeTypeID") or [None])[0]
            vacancy_type_id = (qs.get("vacancyTypeID") or [None])[0]

            if country_id is None:
                country_id = 202  # Singapore default from test script
            meta["countryID"] = country_id

            with _make_session() as session:
                r = session.get(careers_url, timeout=30)
                meta["status"].append(r.status_code)
                r.raise_for_status()

                build_id = _extract_build_id(r.text)

                data_url = _make_data_url(
                    base,
                    build_id,
                    locale_path,
                    {
                        "countryID": country_id,
                        "publishedTimeTypeID": published_time_type_id,
                        "vacancyTypeID": vacancy_type_id,
                    },
                )

                rr = session.get(data_url, timeout=30)
                meta["status"].append(rr.status_code)
                rr.raise_for_status()

                js = rr.json() if isinstance(rr.json(), dict) else {}
                pp = js.get("pageProps") if isinstance(js.get("pageProps"), dict) else {}

                jobs = pp.get("jobPositionList")
                if isinstance(jobs, list):
                    raw_jobs = [j for j in jobs if isinstance(j, dict)]

                # also keep country labels in meta if present
                md = pp.get("masterData") if isinstance(pp.get("masterData"), dict) else {}
                countries = md.get("countries") if isinstance(md.get("countries"), list) else []
                for c in countries:
                    if isinstance(c, dict) and str(c.get("id")) == str(country_id):
                        meta["countryLabel"] = c.get("label")
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

        base = _base_from_url(result.careers_url)
        parsed = urlparse(result.careers_url)
        locale_path = _pick_locale_path(parsed.path)
        locale = locale_path.strip("/")
        country_label = _clean_text(result.meta.get("countryLabel"))

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            job_id = raw.get("id")
            job_id_str = str(job_id) if job_id is not None else ""

            title = _clean_text(raw.get("title"))
            location = _clean_text(raw.get("location")) or country_label

            job_url = f"{base}/{locale}/job-detail?id={job_id_str}" if job_id_str else ""

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=location,
                    job_id=job_id_str,
                    posted_date="",
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return out
