from __future__ import annotations

import json
import re
from html import unescape as html_unescape
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


DEFAULT_CHANNEL_ID = 12


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _abs(base_url: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def _looks_like_muz_jobboard(html: str) -> bool:
    h = (html or "").lower()
    return (
        "milchundzucker" in h
        or "global-jobboard-client" in h
        or "gjb_scripts.js" in h
        or "jquery.jobboard.datatable" in h
    )


def _extract_dom_id_text(html: str, element_id: str) -> Optional[str]:
    m = re.search(rf"\bid=\"{re.escape(element_id)}\"[^>]*>(.*?)</", html or "", flags=re.DOTALL)
    if not m:
        return None
    return html_unescape(m.group(1)).strip()


def _get_gjb_address(session: requests.Session, base_url: str) -> Optional[str]:
    js_url = _abs(base_url, "/script/gjb_scripts.js")
    r = session.get(js_url, timeout=30)
    if r.status_code >= 400:
        return None

    m = re.search(r"\bgjbAddress\s*=\s*\"([^\"]+)\"", r.text or "")
    return m.group(1).strip() if m else None


def _get_matched_object_descriptor(session: requests.Session, base_url: str) -> Optional[List[str]]:
    cfg_url = _abs(base_url, "/assets/js/jobboard.config.json")
    r = session.get(cfg_url, timeout=30)
    if r.status_code >= 400:
        return None

    try:
        cfg = r.json()
        mod = cfg["configWidgetContainer"]["search"]["parameter"]["matchedObjectDescriptor"]["search"]
    except Exception:
        return None

    if not isinstance(mod, list) or not all(isinstance(x, str) for x in mod):
        return None
    return mod


def _extract_location(item: Dict[str, Any]) -> str:
    loc_obj = item.get("PositionLocation")
    if isinstance(loc_obj, list) and loc_obj:
        first = loc_obj[0]
        if isinstance(first, dict):
            city = first.get("CityName") or first.get("LocationName") or first.get("City")
            country = first.get("CountryName") or first.get("Country")
            parts = [_clean_text(x) for x in [city, country] if _clean_text(x)]
            return ", ".join(parts) if parts else ""
    if isinstance(loc_obj, dict):
        city = loc_obj.get("City") or loc_obj.get("LocationName")
        country = loc_obj.get("Country") or loc_obj.get("CountryName")
        parts = [_clean_text(x) for x in [city, country] if _clean_text(x)]
        return ", ".join(parts) if parts else ""

    city = item.get("PositionLocation.City") or item.get("PositionLocation.LocationName")
    country = item.get("PositionLocation.Country") or item.get("PositionLocation.CountryName")
    parts = [_clean_text(x) for x in [city, country] if _clean_text(x)]
    return ", ".join(parts) if parts else ""


def _extract_first_url_field(item: Dict[str, Any]) -> Optional[str]:
    for _, v in item.items():
        if not isinstance(v, str):
            continue
        if v.startswith("http://") or v.startswith("https://"):
            return v
        if v.startswith("/index.php"):
            return v
    return None


def _guess_job_id(item: Dict[str, Any]) -> Optional[str]:
    for k in ["id", "jobId", "jobID", "JobId", "JobID", "positionId", "PositionID", "jobadId", "JobAdId"]:
        if k in item and item[k] is not None:
            return str(item[k])

    for k, v in item.items():
        if v is None:
            continue
        if "id" in str(k).lower() and isinstance(v, (int, str)):
            return str(v)

    return None


def _looks_singapore(loc: str) -> bool:
    t = (loc or "").casefold()
    return "singapore" in t or "singapur" in t or t.strip() in ("sg",)


class MilchUndZuckerGjbCollector(BaseCollector):
    name = "milchundzucker_gjb"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": [], "base_url": None, "channel_id": None}

        try:
            careers_url = (company.careers_url or "").strip()
            if not careers_url:
                raise ValueError("Missing careers_url")

            u = urlparse(careers_url)
            if not u.scheme or not u.netloc:
                raise ValueError("Invalid careers_url")

            base_url = f"{u.scheme}://{u.netloc}".rstrip("/")
            meta["base_url"] = base_url

            qs = parse_qs(u.query)
            channel_id = None
            for key in ("search_criterion_channel[]", "search_criterion_channel%5B%5D"):
                if key in qs and qs[key]:
                    try:
                        channel_id = int(str(qs[key][0]))
                    except Exception:
                        channel_id = None
            if channel_id is None:
                # Support already-decoded key
                if "search_criterion_channel[]" in qs and qs["search_criterion_channel[]"]:
                    try:
                        channel_id = int(str(qs["search_criterion_channel[]"][0]))
                    except Exception:
                        channel_id = None

            if channel_id is None:
                channel_id = DEFAULT_CHANNEL_ID
            meta["channel_id"] = channel_id

            # Ensure we have a search_result page URL
            if "ac=search_result" not in careers_url:
                careers_url = (
                    f"{base_url}/index.php?ac=search_result&search_criterion_channel%5B%5D={channel_id}&btn_dosearch="
                )

            with requests.Session() as session:
                session.headers.update(
                    {
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Connection": "keep-alive",
                    }
                )

                html = session.get(careers_url, timeout=30)
                meta["status"].append(html.status_code)
                html.raise_for_status()

                if not _looks_like_muz_jobboard(html.text or ""):
                    raise RuntimeError("Careers page does not look like milchundzucker Global Jobboard")

                gjb_address = _get_gjb_address(session, base_url)
                if not gjb_address:
                    raise RuntimeError("Could not determine gjbAddress")

                mod = _get_matched_object_descriptor(session, base_url)
                if not mod:
                    raise RuntimeError("Could not load matchedObjectDescriptor")

                sort_text = _extract_dom_id_text(html.text, "escapedGjbPrepareSearchSort")
                if not sort_text:
                    raise RuntimeError("Missing escapedGjbPrepareSearchSort")

                sort_obj = json.loads(sort_text)

                payload: Dict[str, Any] = {
                    "LanguageCode": "en",
                    "SearchParameters": {
                        "FirstItem": 1,
                        "CountItem": 2000,
                        "Sort": [sort_obj],
                        "MatchedObjectDescriptor": mod,
                    },
                    "SearchCriteria": [{"Criterion": "ChannelIDs", "Value": [str(channel_id)]}],
                }

                search_url = gjb_address.rstrip("/") + "/search/"
                params = {"data": json.dumps(payload, separators=(",", ":"), ensure_ascii=False)}
                r = session.get(search_url, params=params, headers={"Accept": "application/json, text/plain, */*"}, timeout=45)
                meta["status"].append(r.status_code)
                r.raise_for_status()

                js = r.json() if isinstance(r.json(), dict) else {}
                sr = js.get("SearchResult") if isinstance(js.get("SearchResult"), dict) else {}
                items = sr.get("SearchResultItems") if isinstance(sr.get("SearchResultItems"), list) else []
                raw_jobs = [x for x in items if isinstance(x, dict)]

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

        base_url = str(result.meta.get("base_url") or "")

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            src = raw.get("MatchedObjectDescriptor") if isinstance(raw.get("MatchedObjectDescriptor"), dict) else raw

            title = _clean_text(src.get("title") or src.get("Title") or src.get("PositionTitle") or src.get("jobTitle"))
            location = _clean_text(src.get("location") or src.get("Location") or _extract_location(src))
            job_id = _clean_text(src.get("ID") or _guess_job_id(src))
            posted_date = _clean_text(src.get("PublicationStartDate") or src.get("publicationStartDate"))

            url_field = _extract_first_url_field(src)
            job_url = _abs(base_url, url_field) if url_field and base_url else (url_field or "")
            if not job_url and job_id and base_url:
                job_url = f"{base_url.rstrip('/')}/index.php?ac=jobad&id={job_id}"

            if not _looks_singapore(location):
                continue

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
