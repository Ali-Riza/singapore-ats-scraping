from __future__ import annotations

import argparse
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG (Global Maritime / HiBob Careers)
# =========================

COMPANY = "Global Maritime"
SOURCE = "hibob"

DEFAULT_BASE_URL = "https://globalmaritime.careers.hibob.com"
DEFAULT_COMPANYIDENTIFIER = "globalmaritime"

DEFAULT_TIMEOUT_S = 30


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


def _get_jobs(
    session: requests.Session,
    *,
    base_url: str,
    timeout_s: int,
) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/api/job-ad"
    r = session.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def _filter_jobs(
    jobs: list[dict[str, Any]],
    *,
    country: str,
    site: Optional[str],
) -> list[dict[str, Any]]:
    c = (country or "").strip().lower()
    s = (site or "").strip().lower()

    if not c and not s:
        return jobs

    out: list[dict[str, Any]] = []
    for j in jobs:
        j_country = _clean_text(j.get("country")).lower()
        j_site = _clean_text(j.get("site")).lower()

        if c and j_country == c:
            out.append(j)
            continue

        if s and j_site == s:
            out.append(j)
            continue

    return out


def scrape(
    *,
    base_url: str,
    companyidentifier: str,
    country: str,
    site: Optional[str],
    timeout_s: int,
    include_description: bool,
    max_jobs: int,
) -> list[dict[str, Any]]:
    with _make_session(companyidentifier) as session:
        data = _get_jobs(session, base_url=base_url, timeout_s=timeout_s)

    job_ads = data.get("jobAdDetails") or []
    if not isinstance(job_ads, list):
        return []

    filtered = _filter_jobs(job_ads, country=country, site=site)

    out: list[dict[str, Any]] = []
    for j in filtered:
        job_id = _clean_text(j.get("id"))
        title = _clean_text(j.get("title"))
        country_v = _clean_text(j.get("country"))
        site_v = _clean_text(j.get("site"))
        posted_date = _clean_text(j.get("publishedAt"))

        job_url = base_url.rstrip("/") + f"/jobs/{job_id}" if job_id else ""

        rec: dict[str, Any] = {
            # Canonical fields (used across runners)
            "company": COMPANY,
            "job_title": title,
            "location": site_v or country_v or None,
            "job_id": job_id or None,
            "posted_date": posted_date or None,
            "job_url": job_url or None,
            "source": SOURCE,
            "careers_url": base_url.rstrip("/") + "/jobs",

            # Extra fields (kept for convenience)
            "apply_url": job_url or None,
            "country": country_v or None,
            "site": site_v or None,
            "department": _clean_text(j.get("department")) or None,
            "employment_type": _clean_text(j.get("employmentType")) or None,
            "workspace_type": _clean_text(j.get("workspaceType")) or None,
        }

        if include_description:
            rec["description_html"] = j.get("description")

        out.append(rec)
        if max_jobs > 0 and len(out) >= max_jobs:
            break

    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=f"{COMPANY} careers scraper ({SOURCE})")
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL, help="HiBob careers base URL")
    ap.add_argument(
        "--companyidentifier",
        default=DEFAULT_COMPANYIDENTIFIER,
        help="HiBob companyidentifier header value",
    )
    ap.add_argument("--country", default="Singapore", help="Exact match on job country")
    ap.add_argument("--site", default=None, help="Exact match on job site (city/office)")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S)
    ap.add_argument("--include-description", action="store_true")
    ap.add_argument("--max-jobs", type=int, default=0, help="0 = no limit")

    args = ap.parse_args()

    jobs = scrape(
        base_url=args.base_url,
        companyidentifier=args.companyidentifier,
        country=args.country,
        site=args.site,
        timeout_s=args.timeout,
        include_description=bool(args.include_description),
        max_jobs=args.max_jobs,
    )

    for j in jobs:
        print(j)

    print(f"Found {len(jobs)} jobs")


if __name__ == "__main__":
    main()
