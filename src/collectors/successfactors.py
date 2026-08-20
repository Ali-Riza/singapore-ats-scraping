from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urldefrag, urljoin, urlparse
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup
from bs4 import FeatureNotFound

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


_JOB_ID_RE = re.compile(r"/(\d+)(?:-[A-Za-z]{2}_[A-Za-z]{2})?/?$")


def _clean_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


def _extract_job_id_from_url(job_url: str) -> str:
    path = urlparse((job_url or "").strip()).path
    m = _JOB_ID_RE.search(path)
    return m.group(1) if m else ""


def _normalize_url(base_url: str, url: str) -> str:
    abs_url = url if (url or "").startswith("http") else urljoin(base_url, url)
    abs_url, _frag = urldefrag(abs_url)
    return abs_url


def _parse_dateposted_meta(content: str) -> Optional[str]:
    """SuccessFactors detail pages often have meta[itemprop=datePosted].

    Example: "Wed Dec 17 02:00:00 UTC 2025" -> "2025-12-17"
    If parsing fails, return None (normalization happens later in pipeline).
    """
    raw = (content or "").strip()
    if not raw:
        return None

    # Common SF pattern
    for fmt in (
        "%a %b %d %H:%M:%S UTC %Y",
        "%a %b %d %H:%M:%S %Z %Y",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.date().isoformat()
        except Exception:
            continue

    return None


def _extract_posted_date_from_detail(html: str) -> Optional[str]:
    soup = _soup(html)
    meta = soup.find("meta", attrs={"itemprop": "datePosted"})
    if meta and meta.get("content"):
        return _parse_dateposted_meta(str(meta.get("content")))

    # Some templates expose a visible date element
    el = soup.select_one('[data-careersite-propertyid="date"]')
    if el:
        txt = _clean_text(el.get_text(" ", strip=True))
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                dt = datetime.strptime(txt, fmt)
                return dt.date().isoformat()
            except Exception:
                continue

    return None


def _extract_title_from_detail(html: str) -> Optional[str]:
    soup = _soup(html)
    el = soup.select_one('[data-careersite-propertyid="title"]')
    if el:
        t = _clean_text(el.get_text(" ", strip=True))
        return t or None

    # fallback: <title>Senior Contract Specialist Job Details | Yinson</title>
    if soup.title and soup.title.string:
        t = _clean_text(str(soup.title.string))
        t = t.replace(" Job Details", "").strip()
        return t or None

    return None


def _extract_location_from_detail(html: str) -> Optional[str]:
    soup = _soup(html)
    # Many SF/J2W detail pages contain multiple job locations as repeated
    # meta[itemprop=streetAddress] tags. Collect all so downstream normalization
    # can prefer Singapore.
    metas = soup.select('meta[itemprop="streetAddress"][content]')
    if metas:
        parts: List[str] = []
        seen: Set[str] = set()
        for m in metas:
            txt = _clean_text(str(m.get("content") or ""))
            if not txt:
                continue
            if txt in seen:
                continue
            seen.add(txt)
            parts.append(txt)
        if parts:
            return "\n".join(parts)

    # Newer SF templates expose location text via careersite property id
    # Sometimes there are multiple locations listed; return a joined string so
    # downstream normalization can prefer Singapore.
    els = soup.select('[data-careersite-propertyid="location"], [data-careersite-propertyid="locations"]')
    if els:
        parts = []
        for el in els:
            txt = _clean_text(el.get_text(" ", strip=True))
            if txt:
                parts.append(txt)
        if parts:
            return "\n".join(parts)

    # Fallback: many SF tenants embed locations in JSON/script blocks or render a subset.
    # Extract location-like chunks from the raw HTML to recover the full list.
    # Examples: "Singapore, SG, 629350", "Tianjin, CN, 300450", "Georgetown, GY".
    loc_re = re.compile(
        r"([A-Za-z0-9][A-Za-z0-9\-\s'\.]*?,\s*[A-Z]{2,3}(?:,\s*[A-Z]{2,3}){0,2}(?:,\s*\d{4,6})?)"
    )
    found = [m.group(1).strip() for m in loc_re.finditer(html)]
    if found:
        seen: Set[str] = set()
        uniq: List[str] = []
        for s in found:
            s2 = _clean_text(s)
            if not s2 or s2 in seen:
                continue
            seen.add(s2)
            uniq.append(s2)
        # Prefer returning multiple lines if we found several distinct locations.
        if uniq:
            return "\n".join(uniq)

    return None


def _looks_truncated_location(loc: str) -> bool:
    s = (loc or "").strip().casefold()
    if not s:
        return False
    # SF UI often shows: "+10 more…" or mojibake variants when copied.
    if "+" in s and "more" in s:
        return True
    if "‚ä¶" in s or "…" in s:
        return True
    return False


def _looks_multi_location_job(title: str, job_url: str) -> bool:
    t = (title or "").casefold()
    u = (job_url or "").casefold()
    if "multiple" in t and "location" in t:
        return True
    if "multiple-location" in t or "multiple locations" in t:
        return True
    if "multiple" in u and "location" in u:
        return True
    return False


def _should_keep_company_job(company_name: str, job_url: str, location: str) -> bool:
    """Apply company-specific keep/drop rules.

    Important: keep this narrowly scoped. Do NOT apply global filtering here.
    """
    c = (company_name or "").strip().casefold()
    u = (job_url or "").casefold()
    loc = (location or "").casefold()

    # Yinson: the search page can return non-Singapore roles even when using q=singapore.
    # Keep only roles that clearly indicate Singapore via location or URL.
    if c in {"yinson production"}:
        return ("singapore" in loc) or ("/singapore" in u) or ("singapore-" in u)

    # These tenants sometimes ignore or mis-handle URL filters. Keep SG only.
    if c in {"sulzer", "endress+hauser", "rina", "paxocean"}:
        return ("singapore" in loc) or ("/singapore" in u) or ("singapore-" in u)

    # Neste: observed to return global roles even when using location=SG.
    # Restrict to jobs that clearly indicate Singapore via location or URL.
    if "neste" in c:
        return ("singapore" in loc) or ("/singapore" in u) or ("singapore-" in u)

    return True


def _canonical_listing_url(company: CompanyItem) -> Optional[str]:
    """Return a stable server-rendered listing URL for tenants where the
    Excel-provided URL can hide the results table (common when adding location=singapore).

    This keeps changes narrowly scoped to the companies we have working test scripts for.
    """
    c = (company.company or "").strip().casefold()
    u = urlparse(company.careers_url)
    if not u.scheme or not u.netloc:
        return None
    base = f"{u.scheme}://{u.netloc}"

    if c == "sulzer":
        return f"{base}/search/?q=&sortColumn=referencedate&sortDirection=desc"

    if c == "rina":
        return f"{base}/search/?q=&sortColumn=referencedate&sortDirection=desc"

    if c == "endress+hauser":
        return f"{base}/other-countries/search/?q=&sortColumn=referencedate&sortDirection=desc"

    return None


def _category_id_from_url(careers_url: str) -> Optional[str]:
    parsed = urlparse(careers_url)
    match = re.search(r"/go/[^/]+/(\d+)/?", parsed.path, re.IGNORECASE)
    return match.group(1) if match else None


def _category_rss_url(careers_url: str) -> Optional[str]:
    parsed = urlparse(careers_url)
    category_id = _category_id_from_url(careers_url)
    if not category_id or not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/services/rss/category/?catid={category_id}"


def _parse_recruiting_api_job(response: Dict[str, Any], base_url: str, locale: str) -> Optional[Dict[str, Any]]:
    job_id = _clean_text(str(response.get("id") or ""))
    title = _clean_text(str(response.get("unifiedStandardTitle") or ""))
    url_title = _clean_text(str(response.get("unifiedUrlTitle") or response.get("urlTitle") or ""))
    if not job_id or not title or not url_title:
        return None

    locations = response.get("jobLocationShort")
    location = ""
    if isinstance(locations, list) and locations:
        location = _clean_text(re.sub(r"<br\s*/?>", "", str(locations[0]), flags=re.IGNORECASE))

    posted_date = ""
    date_text = _clean_text(str(response.get("unifiedStandardStart") or ""))
    if date_text:
        try:
            posted_date = datetime.strptime(date_text, "%d/%m/%Y").date().isoformat()
        except ValueError:
            posted_date = date_text

    return {
        "title": title,
        "job_url": f"{base_url}/job/{url_title}/{job_id}-{locale}",
        "location": location,
        "posted_date": posted_date,
    }


def _fetch_category_api(
    session: requests.Session, careers_url: str, timeout: int = 30
) -> Tuple[List[Dict[str, Any]], List[int], int]:
    parsed = urlparse(careers_url)
    category_id = _category_id_from_url(careers_url)
    if not category_id or not parsed.scheme or not parsed.netloc:
        return [], [], 0

    page_response = session.get(careers_url, timeout=timeout)
    page_response.raise_for_status()
    token_match = re.search(r'var CSRFToken = "([^"]+)"', page_response.text)
    if not token_match:
        return [], [page_response.status_code], 0

    base_url = f"{parsed.scheme}://{parsed.netloc}"
    endpoint = f"{base_url}/services/recruiting/v1/jobs"
    locale = "en_GB"
    jobs: List[Dict[str, Any]] = []
    statuses = [page_response.status_code]
    total_jobs = 0

    for page_number in range(200):
        payload = {
            "locale": locale,
            "pageNumber": page_number,
            "sortBy": "",
            "keywords": "",
            "location": "",
            "facetFilters": {},
            "brand": "",
            "skills": [],
            "categoryId": int(category_id),
            "alertId": "",
            "rcmCandidateId": "",
        }
        response = session.post(
            endpoint,
            json=payload,
            headers={"X-CSRF-Token": token_match.group(1), "Origin": base_url, "Referer": careers_url},
            timeout=timeout,
        )
        statuses.append(response.status_code)
        response.raise_for_status()
        data = response.json()
        total_jobs = int(data.get("totalJobs") or 0)
        page_results = data.get("jobSearchResult")
        if not isinstance(page_results, list) or not page_results:
            break
        for item in page_results:
            raw_response = item.get("response") if isinstance(item, dict) else None
            if not isinstance(raw_response, dict):
                continue
            job = _parse_recruiting_api_job(raw_response, base_url, locale)
            if job:
                jobs.append(job)
        if len(jobs) >= total_jobs:
            break

    return jobs, statuses, total_jobs


def _parse_category_rss(xml: bytes) -> Tuple[List[Dict[str, Any]], bool]:
    root = ElementTree.fromstring(xml)
    raw_jobs: List[Dict[str, Any]] = []
    no_jobs_available = False

    for item in root.findall(".//item"):
        title = _clean_text(item.findtext("title") or "")
        job_url = _clean_text(item.findtext("link") or "")
        if title.casefold().startswith("no jobs currently available"):
            no_jobs_available = True
            continue
        if not title or not job_url or not _extract_job_id_from_url(job_url):
            continue

        location = ""
        title_match = re.match(r"^(?P<title>.+) \((?P<location>[^()]*,[^()]*)\)$", title)
        if title_match:
            title = _clean_text(title_match.group("title"))
            location = _clean_text(title_match.group("location"))

        posted_date = ""
        date_text = item.findtext("pubDate")
        if date_text:
            try:
                posted_date = parsedate_to_datetime(date_text.strip()).date().isoformat()
            except (TypeError, ValueError):
                posted_date = ""

        raw_jobs.append(
            {
                "title": title,
                "job_url": job_url,
                "location": location,
                "posted_date": posted_date,
            }
        )

    return raw_jobs, no_jobs_available


@dataclass(frozen=True)
class _ListingJob:
    title: str
    job_url: str
    location: str
    posted_date: str


def _parse_listing_page(html: str, page_url: str) -> List[_ListingJob]:
    """Parse a SuccessFactors/J2W listing page.

    Typical structure:
    - table#searchresults
    - rows: tr.data-row
    - title link: a.jobTitle-link
    - location: td.colLocation span.jobLocation
    - date: td.colDate span.jobDate (may be missing)
    """
    soup = _soup(html)
    table = soup.select_one("table#searchresults")
    if not table:
        return []

    out: List[_ListingJob] = []
    for row in table.select("tbody tr.data-row"):
        a = row.select_one("a.jobTitle-link[href]")
        if not a:
            continue

        title = _clean_text(a.get_text(" ", strip=True))
        href = (a.get("href") or "").strip()
        if not href:
            continue

        job_url = urljoin(page_url, href)

        loc_el = row.select_one("td.colLocation span.jobLocation")
        location = _clean_text(loc_el.get_text(" ", strip=True)) if loc_el else ""

        date_el = row.select_one("td.colDate span.jobDate")
        posted_date = _clean_text(date_el.get_text(" ", strip=True)) if date_el else ""

        out.append(_ListingJob(title=title, job_url=job_url, location=location, posted_date=posted_date))

    return out


def _discover_pagination_urls(html: str, page_url: str) -> List[str]:
    """Discover pagination URLs from common SuccessFactors templates."""
    soup = _soup(html)

    urls: Set[str] = set()
    for container in soup.select(".paginationShell, .pagination-top, .pagination-bottom"):
        for a in container.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href)
            full, _frag = urldefrag(full)
            urls.add(full)

    return sorted(urls)


def _extract_job_urls_from_search_html(html: str, page_url: str) -> List[str]:
    """Fallback for SF templates that don't have table#searchresults.

    Example: Yinson search page lists jobs as links containing /job/.
    We collect candidate URLs, normalize to absolute, and keep only URLs that
    look like detail pages ending with a numeric job id.
    """
    soup = _soup(html)
    base = f"{urlparse(page_url).scheme}://{urlparse(page_url).netloc}"

    urls: Set[str] = set()

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if "/job/" in href:
            urls.add(_normalize_url(base, href))

    for m in re.finditer(r'href="([^"]*?/job/[^"]+)"', html):
        urls.add(_normalize_url(base, m.group(1)))

    cleaned: List[str] = []
    for u in sorted(urls):
        if re.search(r"/job/.*?/\d{6,}/?$", u):
            cleaned.append(u)

    # stable dedupe
    seen: Set[str] = set()
    out: List[str] = []
    for u in cleaned:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _soup(html: str) -> BeautifulSoup:
    """Create BeautifulSoup with a robust parser choice.

    Many SuccessFactors/J2W pages parse more reliably with lxml.
    If lxml isn't installed, fall back to the stdlib html.parser.
    """
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


class SuccessFactorsCollector(BaseCollector):
    """Collector for SAP SuccessFactors / Jobs2Web career sites.

    SRP: only (Z3) fetch raw jobs and (Z4) map to JobRecord.
    """

    name = "successfactors"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        max_pages = 200

        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "visited_urls": [],
            "status_codes": [],
            "total_raw": 0,
            "pagination_urls_found": 0,
            "detail_date_fetches": 0,
            "filtered_out": 0,
        }

        try:
            session = requests.Session()
            session.headers.update(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-GB,en;q=0.9",
                    "Connection": "keep-alive",
                }
            )

            attempted_starts: List[str] = []

            def _crawl_from(start_url: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
                local_raw: List[Dict[str, Any]] = []
                local_meta: Dict[str, Any] = {
                    "pages": 0,
                    "visited_urls": [],
                    "status_codes": [],
                    "pagination_urls_found": 0,
                }

                visited: Set[str] = set()
                to_visit: List[str] = [start_url]
                seen_job_urls: Set[str] = set()

                pages = 0
                while to_visit and pages < max_pages:
                    url = to_visit.pop(0)
                    url, _frag = urldefrag(url)
                    if url in visited:
                        continue
                    visited.add(url)

                    r = session.get(url, timeout=30)
                    local_meta["status_codes"].append(r.status_code)
                    r.raise_for_status()

                    html = r.text
                    local_meta["visited_urls"].append(url)
                    local_meta["pages"] += 1
                    pages += 1

                    listing_jobs = _parse_listing_page(html, url)

                    # Fallback for non-table templates: extract job detail URLs.
                    if not listing_jobs:
                        for job_url in _extract_job_urls_from_search_html(html, url):
                            if job_url in seen_job_urls:
                                continue
                            seen_job_urls.add(job_url)
                            local_raw.append(
                                {
                                    "title": "",
                                    "job_url": job_url,
                                    "location": "",
                                    "posted_date": "",
                                    "_page_url": url,
                                }
                            )

                    # Add newly discovered pagination links
                    for next_url in _discover_pagination_urls(html, url):
                        if next_url not in visited:
                            to_visit.append(next_url)

                    # Parse jobs and store as raw dicts
                    for j in listing_jobs:
                        if j.job_url in seen_job_urls:
                            continue
                        seen_job_urls.add(j.job_url)
                        local_raw.append(
                            {
                                "title": j.title,
                                "job_url": j.job_url,
                                "location": j.location,
                                "posted_date": j.posted_date,
                                "_page_url": url,
                            }
                        )

                local_meta["pagination_urls_found"] = max(0, len(visited) - 1)
                return local_raw, local_meta

            starts: List[str] = [company.careers_url]
            canon = _canonical_listing_url(company)
            if canon and canon not in starts:
                starts.append(canon)

            # Crawl from the Excel URL first; if it yields 0 jobs, retry from canonical listing URL.
            combined_status: List[int] = []
            combined_visited: List[str] = []
            combined_pages = 0
            combined_pagination_found = 0

            for start in starts:
                attempted_starts.append(start)
                local_raw, local_meta = _crawl_from(start)
                combined_status.extend(list(local_meta.get("status_codes") or []))
                combined_visited.extend(list(local_meta.get("visited_urls") or []))
                combined_pages += int(local_meta.get("pages") or 0)
                combined_pagination_found += int(local_meta.get("pagination_urls_found") or 0)

                if local_raw:
                    raw_jobs = local_raw
                    break

            meta["status_codes"] = combined_status
            meta["visited_urls"] = combined_visited
            meta["pages"] = combined_pages
            meta["pagination_urls_found"] = combined_pagination_found
            meta["attempted_start_urls"] = attempted_starts

            if not raw_jobs and _category_id_from_url(company.careers_url):
                try:
                    api_jobs, api_statuses, api_total = _fetch_category_api(session, company.careers_url)
                    meta["status_codes"].extend(api_statuses)
                    meta["category_api_total"] = api_total
                    raw_jobs = api_jobs
                except Exception as exc:
                    meta["category_api_error"] = str(exc)

            rss_url = _category_rss_url(company.careers_url)
            if not raw_jobs and rss_url:
                rss_response = session.get(rss_url, timeout=30)
                meta["status_codes"].append(rss_response.status_code)
                rss_response.raise_for_status()
                raw_jobs, no_jobs_available = _parse_category_rss(rss_response.content)
                meta["rss_category_url"] = rss_url
                meta["rss_no_jobs_available"] = no_jobs_available

            # Optional: for rows with missing posted_date, fetch detail pages best-effort.
            # Keep this bounded to avoid too many requests.
            # If a site requires detail parsing for title/location (Yinson-style), we must
            # allow fetching all details; otherwise keep a conservative cap.
            needs_full_detail = any(
                (not _clean_text(str(r.get("title") or ""))) or (not _clean_text(str(r.get("location") or "")))
                for r in raw_jobs
            )
            detail_budget = len(raw_jobs) if needs_full_detail else 80
            for raw in raw_jobs:
                if meta["detail_date_fetches"] >= detail_budget:
                    break

                loc_val = str(raw.get("location") or "")
                title_val = str(raw.get("title") or "")
                url_val = str(raw.get("job_url") or "")
                is_multi_loc = _looks_multi_location_job(title_val, url_val)
                has_sg = "singapore" in loc_val.casefold()
                needs_loc_detail = (not raw.get("location")) or _looks_truncated_location(loc_val) or (is_multi_loc and not has_sg)

                if raw.get("posted_date") and raw.get("title") and (not needs_loc_detail):
                    continue

                job_url = str(raw.get("job_url") or "")
                if not job_url:
                    continue

                try:
                    dr = session.get(job_url, timeout=30)
                    dr.raise_for_status()
                    if not raw.get("title"):
                        t = _extract_title_from_detail(dr.text)
                        if t:
                            raw["title"] = t
                    if not raw.get("location"):
                        loc = _extract_location_from_detail(dr.text)
                        if loc:
                            raw["location"] = loc
                    elif _looks_truncated_location(str(raw.get("location") or "")):
                        loc = _extract_location_from_detail(dr.text)
                        if loc:
                            raw["location"] = loc
                    elif needs_loc_detail:
                        loc = _extract_location_from_detail(dr.text)
                        if loc:
                            raw["location"] = loc
                    if not raw.get("posted_date"):
                        pd = _extract_posted_date_from_detail(dr.text)
                        if pd:
                            raw["posted_date"] = pd
                    meta["detail_date_fetches"] += 1
                except Exception:
                    # ignore per-job detail failures
                    continue

            meta["total_raw"] = len(raw_jobs)

            # Company-specific filtering (keep this narrow).
            kept: List[Dict[str, Any]] = []
            filtered_out = 0
            for rj in raw_jobs:
                if _should_keep_company_job(
                    company.company,
                    str(rj.get("job_url") or ""),
                    str(rj.get("location") or ""),
                ):
                    kept.append(rj)
                else:
                    filtered_out += 1
            if filtered_out:
                raw_jobs = kept
                meta["filtered_out"] = filtered_out
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
        records: List[JobRecord] = []
        for raw in result.raw_jobs:
            records.append(self._map_one(raw, result))
        return records

    def _map_one(self, raw: Dict[str, Any], result: CollectResult) -> JobRecord:
        title = _clean_text(str(raw.get("title") or ""))
        job_url = str(raw.get("job_url") or "")
        location = _clean_text(str(raw.get("location") or ""))
        posted_date = _clean_text(str(raw.get("posted_date") or ""))

        job_id = _extract_job_id_from_url(job_url)

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
