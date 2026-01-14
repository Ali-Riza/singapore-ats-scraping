#!/usr/bin/env python3

import argparse
import json
import re
import sys
import time
import os
from html import unescape as html_unescape
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from bs4 import BeautifulSoup


SOURCE = "milchundzucker-global-jobboard"
COMPANY = "Dräger"

DEFAULT_BASE_URL_CANDIDATES = [
    # We try a few plausible hosts and validate by content markers.
    "https://erecruitment.draeger.com",
    "https://jobs.draeger.com",
    "https://career.draeger.com",
    "https://karriere.draeger.com",
]


def _js_string_unescape(s: str) -> str:
    # muZ embeds JSON inside a JS string literal (escaped quotes, unicode escapes).
    # Example: "[{\"Criterion\":\"ChannelIDs\",...}]"
    return bytes(s, "utf-8").decode("unicode_escape")


def _extract_js_var_string(html: str, var_name: str) -> Optional[str]:
    m = re.search(rf"\b{re.escape(var_name)}\s*=\s*(['\"])(.*?)\1", html, flags=re.DOTALL)
    if not m:
        return None
    return m.group(2)


def _extract_dom_id_text(html: str, element_id: str) -> Optional[str]:
    m = re.search(rf"\bid=\"{re.escape(element_id)}\"[^>]*>(.*?)</", html, flags=re.DOTALL)
    if not m:
        return None
    return html_unescape(m.group(1)).strip()


def _get_gjb_address(session: requests.Session, base_url: str, *, debug: bool) -> Optional[str]:
    js_url = _abs(base_url, "/script/gjb_scripts.js")
    try:
        js = _get_with_retries(session, js_url, timeout=30).text
    except Exception as exc:
        if debug:
            print(f"Failed to fetch gjb_scripts.js: {exc}", file=sys.stderr)
        return None

    m = re.search(r"\bgjbAddress\s*=\s*\"([^\"]+)\"", js)
    if not m:
        return None
    return m.group(1).strip()


def _get_matched_object_descriptor(session: requests.Session, base_url: str, *, debug: bool) -> Optional[List[str]]:
    cfg_url = _abs(base_url, "/assets/js/jobboard.config.json")
    try:
        cfg = _get_with_retries(session, cfg_url, timeout=30).json()
    except Exception as exc:
        if debug:
            print(f"Failed to fetch jobboard.config.json: {exc}", file=sys.stderr)
        return None

    try:
        mod = cfg["configWidgetContainer"]["search"]["parameter"]["matchedObjectDescriptor"]["search"]
    except Exception:
        return None

    if not isinstance(mod, list) or not all(isinstance(x, str) for x in mod):
        return None
    return mod


def _extract_location(item: Dict[str, Any]) -> Optional[str]:
    # muZ may return nested dicts (preferred) or flattened keys.
    loc_obj = item.get("PositionLocation")
    if isinstance(loc_obj, list) and loc_obj:
        first = loc_obj[0]
        if isinstance(first, dict):
            city = first.get("CityName") or first.get("LocationName") or first.get("City")
            country = first.get("CountryName") or first.get("Country")
            parts = [str(x).strip() for x in [city, country] if x and str(x).strip()]
            return ", ".join(parts) if parts else None
    if isinstance(loc_obj, dict):
        city = loc_obj.get("City") or loc_obj.get("LocationName")
        country = loc_obj.get("Country") or loc_obj.get("CountryName")
        parts = [str(x).strip() for x in [city, country] if x and str(x).strip()]
        return ", ".join(parts) if parts else None

    # Flattened keys fallback.
    city = item.get("PositionLocation.City") or item.get("PositionLocation.LocationName")
    country = item.get("PositionLocation.Country") or item.get("PositionLocation.CountryName")
    parts = [str(x).strip() for x in [city, country] if x and str(x).strip()]
    return ", ".join(parts) if parts else None


def _fetch_jobs_via_gjb_api(
    session: requests.Session,
    base_url: str,
    careers_url: str,
    *,
    channel_id: int,
    max_items: int,
    debug: bool,
) -> Optional[List[Dict[str, Any]]]:
    """Fetch jobs through the muZ Global Jobboard backend API (beesite)."""

    html = _get_with_retries(session, careers_url, timeout=30).text
    if not _looks_like_muz_jobboard(html):
        if debug:
            print("Careers page does not look like muZ jobboard.", file=sys.stderr)
        return None

    gjb_address = _get_gjb_address(session, base_url, debug=debug)
    if not gjb_address:
        if debug:
            print("Could not determine gjbAddress from gjb_scripts.js", file=sys.stderr)
        return None

    matched_object_descriptor = _get_matched_object_descriptor(session, base_url, debug=debug)
    if not matched_object_descriptor:
        if debug:
            print("Could not load matchedObjectDescriptor.search", file=sys.stderr)
        return None

    # Extract request parameters embedded by the frontend.
    # On Dräger these are stored in hidden DOM nodes rather than JS variables.
    sort_text = (
        _extract_dom_id_text(html, "escapedGjbPrepareSearchSort")
        or _extract_js_var_string(html, "escapedGjbPrepareSearchSort")
    )
    hits_text = (
        _extract_dom_id_text(html, "escapedGjbHitsPerPage")
        or _extract_js_var_string(html, "escapedGjbPrepareSearchHitsPerPage")
    )

    if not sort_text:
        if debug:
            print("Missing escapedGjbPrepareSearchSort on page", file=sys.stderr)
        return None

    try:
        sort_obj = json.loads(sort_text)
    except Exception as exc:
        if debug:
            print(f"Failed to parse sort JSON: {exc}", file=sys.stderr)
        return None

    # Build criteria ourselves (more stable than scraping it from the page).
    # Note: Dräger's numeric country IDs are not obvious; we fetch the channel and apply a local filter.
    criteria: List[Dict[str, Any]] = [{"Criterion": "ChannelIDs", "Value": [str(channel_id)]}]

    language_code = "en"

    # The UI uses a small page size (often 10), but the backend accepts larger CountItem.
    count_item = max(1, min(max_items, 10000))

    payload = {
        "LanguageCode": language_code,
        "SearchParameters": {
            "FirstItem": 1,
            "CountItem": int(count_item),
            "Sort": [sort_obj],
            "MatchedObjectDescriptor": matched_object_descriptor,
        },
        "SearchCriteria": criteria,
    }

    search_url = gjb_address.rstrip("/") + "/search/"
    params = {"data": json.dumps(payload, separators=(",", ":"), ensure_ascii=False)}
    headers = {"Accept": "application/json, text/plain, */*"}

    try:
        resp = session.get(search_url, params=params, headers=headers, timeout=45)
        resp.raise_for_status()
        js = resp.json()
    except Exception as exc:
        if debug:
            print(f"GJB search request failed: {exc}", file=sys.stderr)
        return None

    search_result = js.get("SearchResult")
    if not isinstance(search_result, dict):
        if debug:
            print(f"Unexpected GJB response keys: {list(js.keys())}", file=sys.stderr)
        return None
    items = search_result.get("SearchResultItems")
    if not isinstance(items, list):
        if debug:
            print(f"Unexpected GJB SearchResult keys: {list(search_result.keys())}", file=sys.stderr)
        return None
    return [x for x in items if isinstance(x, dict)]


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: float = 30.0,
    max_attempts: int = 3,
    backoff_s: float = 0.8,
    headers: Optional[Dict[str, str]] = None,
) -> requests.Response:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout, headers=headers)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_s * attempt)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def _abs(base_url: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def _looks_like_muz_jobboard(html: str) -> bool:
    h = html.lower()
    return (
        "milchundzucker" in h
        or "global-jobboard-client" in h
        or "gjb_scripts.js" in h
        or "jquery.jobboard.datatable" in h
    )


def _auto_detect_base_url(session: requests.Session, *, debug: bool) -> Optional[str]:
    for base_url in DEFAULT_BASE_URL_CANDIDATES:
        try:
            probe = f"{base_url.rstrip('/')}/index.php?ac=search_result"
            resp = session.get(probe, timeout=20)
            if resp.status_code != 200:
                if debug:
                    print(f"Base URL probe failed {resp.status_code}: {probe}", file=sys.stderr)
                continue
            if _looks_like_muz_jobboard(resp.text):
                if debug:
                    print(f"Auto-detected base URL: {base_url}", file=sys.stderr)
                return base_url
        except Exception as exc:
            if debug:
                print(f"Base URL probe error for {base_url}: {exc}", file=sys.stderr)
            continue
    return None


def _discover_ac_candidates(session: requests.Session, base_url: str, search_url: str, *, debug: bool) -> List[str]:
    """Try to discover `ac=...` endpoints from known muZ jobboard JS assets."""

    html = _get_with_retries(session, search_url).text
    soup = BeautifulSoup(html, "html.parser")

    script_srcs: List[str] = []
    for s in soup.find_all("script"):
        src = s.get("src")
        if not src:
            continue
        if any(
            token in src
            for token in [
                "gjb_scripts.js",
                "jquery.jobboard.datatable.js",
                "jquery.jobboard.container.js",
                "global-jobboard-client",
            ]
        ):
            script_srcs.append(_abs(base_url, src))

    # Add the assets that appear in the pasted HTML as a fallback.
    fallback_assets = [
        _abs(base_url, "/script/gjb_scripts.js"),
        _abs(base_url, "/assets/vendor/muz/global-jobboard-client/js/jquery.jobboard.datatable.js"),
        _abs(base_url, "/assets/vendor/muz/global-jobboard-client/js/jquery.jobboard.container.js"),
        _abs(base_url, "/assets/js/general/functionsGlobalJobboardClient.js"),
    ]

    to_fetch = list(dict.fromkeys(script_srcs + fallback_assets))

    ac_values: List[str] = []
    for u in to_fetch:
        try:
            js = _get_with_retries(session, u).text
        except Exception:
            continue

        for m in re.finditer(r"\bac\s*=\s*['\"]([a-zA-Z0-9_\-]+)['\"]", js):
            ac_values.append(m.group(1))
        for m in re.finditer(r"index\.php\?ac=([a-zA-Z0-9_\-]+)", js):
            ac_values.append(m.group(1))

    # Add common muZ/GJB candidates (order matters: more likely first).
    common = [
        "gjb_jobads",
        "gjb_search",
        "gjb_search_result",
        "gjb_search_jobads",
        "jobboard_search",
        "jobboard_datatable",
        "jobads",
        "jobad",
        "search",
        "search_result",
        "search_jobads",
    ]

    # Deduplicate while preserving order.
    seen = set()
    out: List[str] = []
    for ac in ac_values + common:
        if not ac or ac in seen:
            continue
        seen.add(ac)
        out.append(ac)

    if debug:
        print(f"Discovered {len(out)} ac candidates", file=sys.stderr)
        for ac in out[:40]:
            print("  ac=", ac, file=sys.stderr)

    return out


def _looks_like_job(item: Dict[str, Any]) -> bool:
    keys = {k.lower() for k in item.keys()}
    return any(k in keys for k in ["title", "positiontitle", "jobtitle"]) and any(
        k in keys for k in ["id", "jobid", "positionid", "jobadid", "vacancyid"]
    )


def _find_job_list(obj: Any) -> Optional[List[Dict[str, Any]]]:
    """Recursively locate a list of dicts that looks like job ads."""
    if isinstance(obj, list):
        dicts = [x for x in obj if isinstance(x, dict)]
        if dicts and sum(1 for d in dicts if _looks_like_job(d)) >= max(1, len(dicts) // 3):
            return dicts
        for x in obj:
            found = _find_job_list(x)
            if found:
                return found
    elif isinstance(obj, dict):
        for v in obj.values():
            found = _find_job_list(v)
            if found:
                return found
    return None


def _extract_first_url_field(item: Dict[str, Any]) -> Optional[str]:
    for k, v in item.items():
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
    # fallback: any *id* key
    for k, v in item.items():
        if v is None:
            continue
        if "id" in str(k).lower() and isinstance(v, (int, str)):
            return str(v)
    return None


def _to_canonical_job(item: Dict[str, Any], *, base_url: str, careers_url: str) -> Dict[str, Any]:
    src = item.get("MatchedObjectDescriptor") if isinstance(item.get("MatchedObjectDescriptor"), dict) else item

    title = src.get("title") or src.get("Title") or src.get("PositionTitle") or src.get("jobTitle")
    location = src.get("location") or src.get("Location") or _extract_location(src)
    job_id = src.get("ID") or _guess_job_id(src)

    url_field = _extract_first_url_field(src)
    job_url = _abs(base_url, url_field) if url_field else None

    # If we have no URL, try a common pattern (best-effort only).
    if not job_url and job_id:
        job_url = f"{base_url.rstrip('/')}/index.php?ac=jobad&id={job_id}"

    return {
        "company": COMPANY,
        "job_title": title,
        "location": location,
        "job_id": job_id,
        "posted_date": src.get("PublicationStartDate") or src.get("publicationStartDate"),
        "job_url": job_url,
        "source": SOURCE,
        "careers_url": careers_url,
    }


def _filter_singapore(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for j in jobs:
        loc = (j.get("location") or "")
        if "singapore" in loc.lower() or "singapur" in loc.lower() or loc.strip().lower() == "sg":
            out.append(j)
    return out


def _filter_country(jobs: List[Dict[str, Any]], *, country: str) -> List[Dict[str, Any]]:
    c = country.strip().lower()
    if not c:
        return jobs

    # Common local spellings / abbreviations.
    synonyms = {
        "denmark": ["denmark", "dänemark", "daenemark", "dk"],
        "singapore": ["singapore", "singapur", "sg"],
    }
    needles = synonyms.get(c, [c])

    out: List[Dict[str, Any]] = []
    for j in jobs:
        # We only have canonical fields here; location is usually enough.
        loc = str(j.get("location") or "").lower()
        if any(n in loc for n in needles):
            out.append(j)
    return out


def _try_fetch_jobs_via_ac(
    session: requests.Session,
    base_url: str,
    ac: str,
    *,
    channel_id: Optional[int],
    language_id: Optional[int],
    debug: bool,
) -> Optional[List[Dict[str, Any]]]:
    """Best-effort probing of likely JSON endpoints.

    muZ jobboards vary a bit; we try both GET and XHR-ish GET.
    """

    params: List[Tuple[str, str]] = [("ac", ac)]
    if channel_id is not None:
        params.append(("search_criterion_channel[]", str(channel_id)))
    if language_id is not None:
        params.append(("language", str(language_id)))

    # Standard GET.
    url = f"{base_url.rstrip('/')}/index.php"

    headers = {"Accept": "application/json, text/plain, */*", "X-Requested-With": "XMLHttpRequest"}

    try:
        resp = session.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code >= 400:
            return None

        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "json" in ctype:
            js = resp.json()
            lst = _find_job_list(js)
            if lst:
                if debug:
                    print(f"ac={ac} -> JSON job list len={len(lst)}", file=sys.stderr)
                return lst
        # Some endpoints return JSON with wrong content-type.
        try:
            js = resp.json()
            lst = _find_job_list(js)
            if lst:
                if debug:
                    print(f"ac={ac} -> (forced) JSON job list len={len(lst)}", file=sys.stderr)
                return lst
        except Exception:
            pass

        # Some endpoints return HTML snippets. Try to find jobad links.
        if "<html" in resp.text.lower() or "jobad" in resp.text.lower():
            soup = BeautifulSoup(resp.text, "html.parser")
            anchors = soup.find_all("a", href=True)
            hits: List[Dict[str, Any]] = []
            for a in anchors:
                href = a.get("href")
                if not href:
                    continue
                if "ac=jobad" in href and "id=" in href:
                    txt = a.get_text(" ", strip=True)
                    m = re.search(r"[?&]id=(\d+)", href)
                    hits.append({"id": m.group(1) if m else None, "title": txt or None, "url": href})
            if hits:
                if debug:
                    print(f"ac={ac} -> HTML jobad hits len={len(hits)}", file=sys.stderr)
                return hits

    except Exception:
        return None

    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Dräger careers scraper. ATS family: milch & zucker (muZ) Global Jobboard. "
            "By default it prints Denmark jobs (local filter). Use --validate to print a sample job from any country when the filter returns empty."
        )
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Portal base URL, e.g. https://<host> (optional: if omitted, common Dräger portal hosts are tried)",
    )
    parser.add_argument(
        "--country",
        default="Singapore",
        help="Country name to filter by locally (default: Singapore). Use --validate if this yields 0 jobs.",
    )
    parser.add_argument(
        "--channel-id",
        type=int,
        default=12,
        help="Publication channel ID (default: 12, as seen in the HTML)",
    )
    parser.add_argument(
        "--language-id",
        type=int,
        default=None,
        help="Optional language id from the portal links (e.g. 1=DE, 2=EN)",
    )
    parser.add_argument("--max-jobs", type=int, default=200, help="Max jobs to print")
    parser.add_argument(
        "--validate",
        action="store_true",
        default=False,
        help="If filtered result is empty, print 1 job from any country (for validation)",
    )
    parser.add_argument("--debug", action="store_true", default=False, help="Print endpoint discovery details to stderr")

    args = parser.parse_args()

    session = requests.Session()

    base_url = (args.base_url or os.environ.get("DRAEGER_BASE_URL") or "").rstrip("/")
    if not base_url:
        detected = _auto_detect_base_url(session, debug=args.debug)
        if not detected:
            print(
                "Could not auto-detect the Dräger portal base URL. Set DRAEGER_BASE_URL or run with --base-url https://<host>.",
                file=sys.stderr,
            )
            print("Found 0 jobs")
            return 0
        base_url = detected.rstrip("/")
    careers_url = f"{base_url}/index.php?ac=search_result&search_criterion_channel%5B%5D={args.channel_id}&btn_dosearch="

    raw_jobs = _fetch_jobs_via_gjb_api(
        session,
        base_url,
        careers_url,
        channel_id=args.channel_id,
        max_items=min(max(args.max_jobs * 10, 500), 2000),
        debug=args.debug,
    )

    if not raw_jobs:
        # Fallback to legacy probing (kept for safety), but the GJB API should be stable for Dräger.
        ac_candidates = _discover_ac_candidates(session, base_url, careers_url, debug=args.debug)
        for ac in ac_candidates:
            raw_jobs = _try_fetch_jobs_via_ac(
                session,
                base_url,
                ac,
                channel_id=args.channel_id,
                language_id=args.language_id,
                debug=args.debug,
            )
            if raw_jobs:
                break

    if not raw_jobs:
        print("Found 0 jobs")
        return 0

    canonical_all = [_to_canonical_job(j, base_url=base_url, careers_url=careers_url) for j in raw_jobs]
    canonical_filtered = _filter_country(canonical_all, country=args.country)

    to_print = canonical_filtered
    if not to_print and args.validate:
        # Validation mode: show at least one job from any location.
        to_print = canonical_all[:1]
        if to_print:
            print("No jobs for requested country; printing 1 job for validation.", file=sys.stderr)

    out_count = 0
    for job in to_print:
        if out_count >= args.max_jobs:
            break
        print(job)
        out_count += 1

    print(f"Found {out_count} jobs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
