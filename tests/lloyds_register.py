from __future__ import annotations

import argparse
import re
import time
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (LLOYD'S REGISTER / LR careers search)
# =========================

COMPANY = "Lloyd's Register"
SOURCE = "lr_episerver_api"

BASE_URL = "https://www.lr.org"
SEARCH_API_URL = f"{BASE_URL}/api/search/careers/"

# Comes from `data-initial-state` on the careers search page.
DEFAULT_ROOT_ID = 25796
DEFAULT_LANGUAGE = "en"

DEFAULT_TIMEOUT_S = 30
DEFAULT_SLEEP_S = 0.0

DEFAULT_PAGE_SIZE = 50


_JOB_ID_FROM_PATH_RE = re.compile(r"-(\d+)/?$")


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
            "Content-Type": "application/json",
            "Connection": "keep-alive",
        }
    )
    return s


def _parse_iso_date(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date().isoformat()
        except Exception:
            continue

    # Best-effort: take YYYY-MM-DD prefix if present.
    if len(s) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", s[:10]):
        return s[:10]
    return None


def _extract_job_id_from_lr_url(path_or_url: str) -> str:
    s = (path_or_url or "").strip()
    m = _JOB_ID_FROM_PATH_RE.search(s)
    return m.group(1) if m else ""


def _build_successfactors_apply_url(job_id: str) -> Optional[str]:
    job_id = (job_id or "").strip()
    if not job_id:
        return None
    # Observed on LR job pages: career_job_req_id=<job_id>
    return (
        "https://career5.successfactors.eu/careers"
        f"?company=lloydsregiP&career_job_req_id={job_id}"
        "&clientId=jobs2web&socialApply=false"
        "&career_ns=job_application"
        "&jobPipeline=www.lr.org&isInternalUser=false"
    )


def _post_search(
    session: requests.Session,
    *,
    page: int,
    page_size: int,
    language: str,
    root_id: int,
    query: str,
    filters: dict[str, Any],
    timeout_s: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "page": page,
        "pageSize": page_size,
        "language": language,
        "rootId": root_id,
        "query": query,
    }
    if filters:
        payload["filters"] = filters

    r = session.post(SEARCH_API_URL, json=payload, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def scrape(
    *,
    language: str = DEFAULT_LANGUAGE,
    root_id: int = DEFAULT_ROOT_ID,
    country: str = "Singapore",
    query: str = "",
    page_size: int = DEFAULT_PAGE_SIZE,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    sleep_s: float = DEFAULT_SLEEP_S,
    max_jobs: Optional[int] = None,
    debug: bool = True,
) -> list[dict[str, Any]]:
    filters = {"JobCountry": [country]}

    with _make_session() as session:
        items_out: list[dict[str, Any]] = []

        page = 1
        while True:
            data = _post_search(
                session,
                page=page,
                page_size=page_size,
                language=language,
                root_id=root_id,
                query=query,
                filters=filters,
                timeout_s=timeout_s,
            )

            page_items = data.get("items") or []
            if debug:
                print(
                    f"page={page} items={len(page_items)} hasMore={data.get('hasMore')} pages={data.get('numberOfPages')}"
                )

            for it in page_items:
                lr_path = str(it.get("url") or it.get("pagePath") or "").strip()
                job_url = urljoin(BASE_URL, lr_path) if lr_path else ""
                job_id = _extract_job_id_from_lr_url(lr_path) or _extract_job_id_from_lr_url(job_url)

                posted_date = (
                    _parse_iso_date(str(it.get("published") or ""))
                    or _parse_iso_date(str(it.get("postingStartDate") or ""))
                    or None
                )

                location_parts = [
                    _clean_text(it.get("jobLocation")),
                    _clean_text(it.get("city")),
                    _clean_text(it.get("jobCountry")),
                ]
                location = ", ".join([p for p in location_parts if p])

                rec: dict[str, Any] = {
                    "company": COMPANY,
                    "job_title": _clean_text(it.get("jobTitle") or it.get("heading") or ""),
                    "job_url": job_url,
                    "apply_url": _build_successfactors_apply_url(job_id) or job_url,
                    "job_id": job_id or None,
                    "posted_date": posted_date,
                    "location": location,
                    "country": _clean_text(it.get("jobCountry") or "") or None,
                    "city": _clean_text(it.get("city") or "") or None,
                    "employment_type": _clean_text(it.get("employmentType") or "") or None,
                    "category": _clean_text(it.get("positionCategory") or "") or None,
                    "source": SOURCE,
                    "listing_url": f"{BASE_URL}/en/careers/careers-search-page/",
                }

                # Safety: keep only exact country match in case API filtering changes.
                if country.casefold() != (rec.get("country") or "").casefold():
                    continue

                items_out.append(rec)

                if max_jobs is not None and len(items_out) >= max_jobs:
                    return items_out

            has_more = bool(data.get("hasMore"))
            num_pages = int(data.get("numberOfPages") or 0)

            if not has_more:
                break
            if num_pages and page >= num_pages:
                break

            page += 1
            if sleep_s:
                time.sleep(sleep_s)

        return items_out


def main() -> None:
    ap = argparse.ArgumentParser(description="Lloyd's Register (LR) Singapore-only runner")
    ap.add_argument("--country", default="Singapore")
    ap.add_argument("--query", default="")
    ap.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    ap.add_argument("--max-jobs", type=int, default=None)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S)
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S)
    ap.add_argument("--no-debug", action="store_true")
    args = ap.parse_args()

    jobs = scrape(
        language=DEFAULT_LANGUAGE,
        root_id=DEFAULT_ROOT_ID,
        country=args.country,
        query=args.query,
        page_size=args.page_size,
        timeout_s=args.timeout,
        sleep_s=args.sleep,
        max_jobs=args.max_jobs,
        debug=not args.no_debug,
    )

    print(f"Found {len(jobs)} jobs")
    for j in jobs:
        print(j)


if __name__ == "__main__":
    main()
