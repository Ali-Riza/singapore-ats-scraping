#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from datetime import datetime
from html import unescape
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup


CAREERS_URL = "https://bw-group.com/about-us/careers/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


def _fetch_html(url: str, *, timeout: int = 30) -> str:
    return _fetch_html_with_session(None, url, timeout=timeout)


def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _fetch_html_with_session(
    session: Optional[requests.Session],
    url: str,
    *,
    timeout: int = 30,
    allow_curl_fallback: bool = True,
) -> str:
    sess = session or _make_session()
    try:
        resp = sess.get(url, timeout=timeout, allow_redirects=True)
        if resp.status_code == 403 and allow_curl_fallback:
            # Some WP/Cloudflare setups can be fussy; curl often works.
            try:
                out = subprocess.check_output(
                    ["curl", "-sSL", "--compressed", url, "-H", f"User-Agent: {HEADERS['User-Agent']}"]
                )
                return out.decode("utf-8", errors="replace")
            except Exception:
                resp.raise_for_status()
        resp.raise_for_status()
        return resp.text
    finally:
        if session is None:
            sess.close()


def _parse_date_to_iso(raw: Optional[str]) -> Optional[str]:
    s = (raw or "").strip()
    if not s:
        return None

    # ISO prefix
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)

    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
    ):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue

    # Best-effort: if it contains an ISO date anywhere
    m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", s)
    if m:
        return m.group(1)

    return None


def _extract_posted_date_from_cornerstone(detail_html: str) -> Optional[str]:
    # Deprecated: Cornerstone requisition pages are SPA shells and usually don't include
    # posted date in HTML. We now fetch the JSON jobDetails API instead.
    return None


def _extract_csod_token(html: str) -> Optional[str]:
    m = re.search(r'"token"\s*:\s*"([^"]+)"', html or "")
    return m.group(1) if m else None


def _extract_csod_culture_id(html: str) -> Optional[str]:
    m = re.search(r'"cultureID"\s*:\s*(\d+)', html or "")
    return m.group(1) if m else None


def _fetch_cornerstone_job_details(
    session: requests.Session,
    *,
    req_id: str,
    token: str,
    culture_id: str,
    referer: str,
    timeout: int,
) -> dict[str, Any]:
    api_url = (
        f"https://bwoffshore.csod.com/services/x/job-requisition/v2/requisitions/{req_id}"
        f"/jobDetails?cultureId={culture_id}"
    )

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "Referer": referer,
    }

    r = session.get(api_url, headers=headers, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _extract_posted_date_from_cornerstone_api(data: dict[str, Any]) -> Optional[str]:
    # Observed field: openDate (e.g. 2025-10-15T08:56:49)
    for key in ("openDate", "postingStartDate", "datePosted", "postedDate"):
        v = data.get(key)
        if isinstance(v, str) and v.strip():
            return _parse_date_to_iso(v)
    return None


def _extract_data_options_json(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    el = soup.select_one(".c-careers-table[data-options]")
    if not el:
        raise RuntimeError("Could not find careers table element with data-options")

    raw = el.get("data-options")
    if not raw:
        raise RuntimeError("data-options attribute missing")

    # Depending on parser, entities might still be present.
    raw = unescape(raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Extremely defensive fallback: try to locate the JSON object in the attribute text.
        m = re.search(r"\{.*\}\s*$", raw, flags=re.DOTALL)
        if not m:
            raise
        return json.loads(m.group(0))


_HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)


def _extract_href(actions_html: str) -> Optional[str]:
    if not actions_html:
        return None
    m = _HREF_RE.search(actions_html)
    return m.group(1) if m else None


def _infer_source(job_url: str) -> str:
    u = (job_url or "").lower()
    if "csod.com" in u:
        return "cornerstone"
    if "recruiterpal.com" in u:
        return "recruiterpal"
    if "teamtailor.com" in u:
        return "teamtailor"
    if "apply.workable.com" in u:
        return "workable"
    if "varbi.com" in u:
        return "varbi"
    return "bw-group"


def _extract_job_id(job_url: str) -> Optional[str]:
    if not job_url:
        return None
    m = re.search(r"/requisition/(\d+)", job_url)
    if m:
        return m.group(1)
    m = re.search(r"/jobs/([a-z0-9]+)", job_url, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def scrape_bw_offshore_singapore(
    careers_url: str = CAREERS_URL,
    *,
    fetch_posted_date: bool = True,
    sleep_s: float = 0.0,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    session = _make_session()
    try:
        html = _fetch_html_with_session(session, careers_url, timeout=timeout)
        data = _extract_data_options_json(html)
        items = data.get("items") or []

        results: list[dict[str, Any]] = []

        for it in items:
            company = (it.get("company") or "").strip()
            location_country = (it.get("location_country") or "").strip()
            location = (it.get("location") or "").strip()

            if company.lower() != "bw offshore":
                continue

            # The page provides both `location` and `location_country`.
            # We require Singapore.
            if (location_country or location).lower() != "singapore":
                continue

            title = (it.get("name") or "").strip() or None
            job_url = _extract_href(it.get("actions") or "")

            rec = {
                "company": "BW Offshore",
                "job_title": title,
                "location": "Singapore",
                "job_id": _extract_job_id(job_url or ""),
                "posted_date": None,
                "job_url": job_url,
                "source": _infer_source(job_url or ""),
                "careers_url": careers_url,
            }
            results.append(rec)

        # Enrich posted_date from Cornerstone job detail API.
        if fetch_posted_date:
            for rec in results:
                if rec.get("source") != "cornerstone":
                    continue

                job_url = rec.get("job_url") or ""
                req_id = str(rec.get("job_id") or "").strip()
                if not job_url or not req_id:
                    continue

                try:
                    # Must fetch the requisition page first to get cookies (e.g. cscx)
                    # and extract token/cultureId.
                    page_html = _fetch_html_with_session(
                        session,
                        job_url,
                        timeout=timeout,
                        allow_curl_fallback=False,
                    )
                    token = _extract_csod_token(page_html)
                    culture_id = _extract_csod_culture_id(page_html)
                    if not token or not culture_id:
                        rec["posted_date"] = None
                    else:
                        details = _fetch_cornerstone_job_details(
                            session,
                            req_id=req_id,
                            token=token,
                            culture_id=culture_id,
                            referer=job_url,
                            timeout=timeout,
                        )
                        rec["posted_date"] = _extract_posted_date_from_cornerstone_api(details)
                except Exception:
                    rec["posted_date"] = None

                if sleep_s:
                    time.sleep(sleep_s)

        return results
    finally:
        session.close()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="BW Offshore (Singapore) scraper via BW Group careers page embedded DataTables JSON.",
    )
    ap.add_argument("--url", default=CAREERS_URL, help="BW careers page URL")
    ap.add_argument(
        "--no-detail",
        action="store_true",
        help="Skip fetching job detail pages (posted_date will be None).",
    )
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between detail requests")
    ap.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    args = ap.parse_args()

    jobs = scrape_bw_offshore_singapore(
        args.url,
        fetch_posted_date=not args.no_detail,
        sleep_s=max(0.0, float(args.sleep)),
        timeout=max(5, int(args.timeout)),
    )

    for j in jobs:
        print(j)

    print(f"Found {len(jobs)} jobs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
