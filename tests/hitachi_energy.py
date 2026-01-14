from __future__ import annotations

import argparse
import re
import time
from datetime import datetime
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (HITACHI ENERGY)
# =========================

COMPANY = "Hitachi Energy"
SOURCE = "aem_workday_json"

LIST_URL = (
    "https://www.hitachienergy.com/careers/open-jobs/"
    "_jcr_content/root/container/content_1/content/grid_0/joblist.listsearchresults.json"
)

DEFAULT_TIMEOUT_S = 30
DEFAULT_SLEEP_S = 0.0
DEFAULT_LOCATION = "Singapore"


_JOB_ID_RE = re.compile(r"/details/([^/?#]+)")
_WORKDAY_REQ_ID_RE = re.compile(r"_(R\d+)")


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
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return s


def _parse_iso_date(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None

    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date().isoformat()
        except Exception:
            continue

    if len(s) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", s[:10]):
        return s[:10]
    return None


def _extract_job_id(job_url: str) -> str:
    s = (job_url or "").strip()
    m = _JOB_ID_RE.search(s)
    return m.group(1) if m else ""


def _extract_workday_req_id(apply_url: str) -> Optional[str]:
    s = (apply_url or "").strip()
    m = _WORKDAY_REQ_ID_RE.search(s)
    return m.group(1) if m else None


def _extract_country_from_location(location: str) -> Optional[str]:
    parts = [p.strip() for p in (location or "").split(",") if p.strip()]
    if not parts:
        return None
    return parts[-1]


def _get_page(
    session: requests.Session,
    *,
    location: str,
    offset: int,
    timeout_s: int,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if location:
        params["location"] = location
    if offset:
        params["offset"] = offset

    r = session.get(LIST_URL, params=params, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def scrape(
    *,
    location: str = DEFAULT_LOCATION,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    sleep_s: float = DEFAULT_SLEEP_S,
    max_jobs: Optional[int] = None,
    debug: bool = True,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    with _make_session() as session:
        offset = 0
        while True:
            data = _get_page(session, location=location, offset=offset, timeout_s=timeout_s)

            items = data.get("items") or []
            total = int(data.get("totalNumber") or 0)
            load_more = bool(data.get("loadMore"))

            if debug:
                print(f"offset={offset} items={len(items)} total={total or None} loadMore={load_more}")

            for it in items:
                job_url = _clean_text(it.get("url") or "")
                apply_url = _clean_text(it.get("applyNowUrl") or "") or job_url
                loc = _clean_text(it.get("location") or it.get("primaryLocation") or "")

                normalized_country = _extract_country_from_location(loc)
                if location.casefold() == "singapore" and "singapore" in (loc or "").casefold():
                    normalized_country = "Singapore"

                rec: dict[str, Any] = {
                    "company": COMPANY,
                    "job_title": _clean_text(it.get("title") or ""),
                    "job_url": job_url,
                    "apply_url": apply_url,
                    "job_id": _extract_job_id(job_url) or None,
                    "workday_req_id": _extract_workday_req_id(apply_url),
                    "posted_date": _parse_iso_date(_clean_text(it.get("publicationDate") or "")),
                    "location": loc or None,
                    "country": normalized_country,
                    "employment_type": _clean_text(it.get("jobType") or "") or None,
                    "contract_type": _clean_text(it.get("contractType") or "") or None,
                    "category": _clean_text(it.get("jobFunction") or "") or None,
                    "source": SOURCE,
                    "listing_url": "https://www.hitachienergy.com/careers/open-jobs",
                }

                # Safety: keep only matches for the requested location.
                if location and location.casefold() not in (loc or "").casefold():
                    continue

                out.append(rec)
                if max_jobs is not None and len(out) >= max_jobs:
                    return out

            if not items:
                break

            offset += len(items)
            if not load_more:
                break
            if total and offset >= total:
                break

            if sleep_s:
                time.sleep(sleep_s)

    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Hitachi Energy open jobs (Singapore-only) runner")
    p.add_argument("--location", default=DEFAULT_LOCATION, help="Location filter (default: Singapore)")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S)
    p.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S)
    p.add_argument("--max-jobs", type=int, default=None)
    p.add_argument("--no-debug", action="store_true")
    args = p.parse_args()

    jobs = scrape(
        location=args.location,
        timeout_s=args.timeout,
        sleep_s=args.sleep,
        max_jobs=args.max_jobs,
        debug=not args.no_debug,
    )

    for j in jobs:
        print(j)
    print(f"Found {len(jobs)} jobs")


if __name__ == "__main__":
    main()
