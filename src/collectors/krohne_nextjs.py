from __future__ import annotations

import hashlib
import json
import re
import subprocess
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(s: Any) -> str:
    return " ".join(str(s or "").split()).strip()


def _stable_job_id(*parts: Optional[str]) -> str:
    normalized = "|".join([(p or "").strip().lower() for p in parts])
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:16]


def _try_parse_date_posted(iso_dt: Optional[str]) -> str:
    if not iso_dt:
        return ""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", iso_dt)
    if not m:
        return ""
    try:
        y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return str(date(y, mth, d))
    except Exception:
        return ""


def _fetch_html(url: str) -> str:
    # Try requests first; if blocked (403/429), fall back to curl.
    s = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ATS-Scraper/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    for attempt in range(1, 4):
        try:
            r = s.get(url, timeout=30.0, headers=headers)
            if r.status_code in (403, 429):
                break
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception:
            if attempt < 3:
                continue

    proc = subprocess.run(
        [
            "curl",
            "-sSL",
            "--compressed",
            "-H",
            "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "-H",
            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            url,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode("utf-8", errors="replace"))

    return proc.stdout.decode("utf-8", errors="replace")


def _extract_next_data(html: str) -> Optional[dict]:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html or "", re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _find_job_contentset_block(next_data: dict) -> Optional[dict]:
    def walk(node: Any) -> Optional[dict]:
        if isinstance(node, dict):
            if {"contentset", "filters", "attributes", "pagination"}.issubset(node.keys()):
                cs = node.get("contentset")
                if isinstance(cs, dict) and cs.get("entity_type") == "job":
                    return node
            for v in node.values():
                found = walk(v)
                if found is not None:
                    return found
        elif isinstance(node, list):
            for v in node:
                found = walk(v)
                if found is not None:
                    return found
        return None

    return walk(next_data)


def _post_contentset(session: requests.Session, api_url: str, *, body_obj: dict, referer: str) -> dict:
    body = json.dumps(body_obj, ensure_ascii=False, separators=(",", ":"))
    body_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Body-Hash": body_hash,
        "User-Agent": "Mozilla/5.0 (compatible; ATS-Scraper/1.0)",
        "Referer": referer,
    }

    resp = session.post(api_url, headers=headers, data=body.encode("utf-8"), timeout=60.0)

    if resp.status_code in (403, 429):
        proc = subprocess.run(
            [
                "curl",
                "-sSL",
                "--compressed",
                "-X",
                "POST",
                api_url,
                "-H",
                "Accept: application/json",
                "-H",
                "Content-Type: application/json",
                "-H",
                f"X-Body-Hash: {body_hash}",
                "-H",
                "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "-H",
                f"Referer: {referer}",
                "--data-binary",
                body,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode("utf-8", errors="replace"))
        return json.loads(proc.stdout.decode("utf-8", errors="replace"))

    resp.raise_for_status()
    return resp.json()


def _country_labels(country_field: Any) -> List[str]:
    labels: List[str] = []
    if isinstance(country_field, list):
        for c in country_field:
            if isinstance(c, dict):
                lbl = _clean_text(c.get("label"))
                if lbl:
                    labels.append(lbl)
    elif isinstance(country_field, dict):
        lbl = _clean_text(country_field.get("label"))
        if lbl:
            labels.append(lbl)
    return labels


def _looks_singapore(location: str, job_url: str) -> bool:
    needle = "singapore"
    loc = (location or "").lower()
    url = (job_url or "").lower()
    if needle in loc or needle in url:
        return True
    return "/sg/" in url or "-sg" in url or "_sg" in url


class KrohneNextJsCollector(BaseCollector):
    name = "krohne_nextjs"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": [], "used_contentset": False}

        try:
            start_url = company.careers_url
            base = _clean_text(f"{urlparse(start_url).scheme}://{urlparse(start_url).netloc}")
            api_url = base.rstrip("/") + "/api/contentset"

            html = _fetch_html(start_url)
            meta["status"].append(200)

            next_data = _extract_next_data(html)
            if next_data:
                block = _find_job_contentset_block(next_data)
                if block:
                    body_obj: Dict[str, Any] = {
                        "contentset": block.get("contentset"),
                        "filters": block.get("filters"),
                        "attributes": block.get("attributes"),
                        "pagination": block.get("pagination"),
                    }
                    with requests.Session() as session:
                        api_json = _post_contentset(session, api_url, body_obj=body_obj, referer=start_url)
                    entities = api_json.get("entities")
                    if isinstance(entities, list):
                        meta["used_contentset"] = True
                        raw_jobs = [e for e in entities if isinstance(e, dict)]

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

        start_url = result.careers_url
        base = _clean_text(f"{urlparse(start_url).scheme}://{urlparse(start_url).netloc}")

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            job_id = _clean_text(raw.get("id"))
            title = _clean_text(raw.get("title"))

            job_url = _clean_text(raw.get("joblink_pdf"))
            if job_url.startswith("/"):
                job_url = urljoin(base, job_url)

            city = _clean_text(raw.get("location_city"))
            countries = _country_labels(raw.get("country"))
            loc_parts = [p for p in [city, ", ".join(countries) if countries else ""] if p]
            location = ", ".join(loc_parts)

            posted_date = _try_parse_date_posted(_clean_text(raw.get("publishing_start_date")))

            if not job_id:
                # Try extracting from URL patterns
                m = re.search(r"/Vacancies/(\d+)(?:/|$)", job_url or "")
                if m:
                    job_id = m.group(1)
            if not job_id:
                job_id = _stable_job_id(result.company, title, location, job_url)

            if not _looks_singapore(location, job_url):
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
