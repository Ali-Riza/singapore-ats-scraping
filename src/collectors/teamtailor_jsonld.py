from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterator, List, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _is_singapore(text: str) -> bool:
    return bool(re.search(r"\bsingapore\b|\bSG\b", str(text or ""), re.I))


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    )
    return s


def _get_soup(session: requests.Session, url: str, timeout: float = 25.0) -> BeautifulSoup:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    r.encoding = "utf-8"
    return BeautifulSoup(r.text, "html.parser")


def _job_postings(soup: BeautifulSoup) -> Iterator[Dict[str, Any]]:
    def walk(x: Any) -> Iterator[Dict[str, Any]]:
        if isinstance(x, list):
            for y in x:
                yield from walk(y)
        elif isinstance(x, dict):
            t = x.get("@type", [])
            if t == "JobPosting" or "JobPosting" in (t if isinstance(t, list) else [t]):
                yield x
            if "@graph" in x:
                yield from walk(x["@graph"])

    for z in soup.select('script[type="application/ld+json"]'):
        try:
            yield from walk(json.loads(z.string or z.get_text()))
        except (ValueError, TypeError):
            continue


def _jobposting_location(d: Dict[str, Any]) -> str:
    locs = d.get("jobLocation", [])
    locs = locs if isinstance(locs, list) else [locs]
    out: List[str] = []
    for loc in locs:
        a = (loc or {}).get("address", {})
        if isinstance(a, str):
            out.append(a)
            continue
        country = a.get("addressCountry", "")
        country = country.get("name", "") if isinstance(country, dict) else country
        parts = list(dict.fromkeys(str(a.get(k)) for k in ("addressLocality", "addressRegion") if a.get(k)))
        out.append(", ".join(parts + ([country] if country else [])))
    return "; ".join(out)


class TeamtailorJsonLdCollector(BaseCollector):
    """Teamtailor career site: listing pages -> /jobs/<id>-slug detail pages with JobPosting JSON-LD."""

    name = "teamtailor_jsonld"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"pages": 0, "listing_unique": 0}
        try:
            session = _make_session()
            host = urlparse(company.careers_url)
            job_re = re.compile(r"^%s://%s/jobs/\d+" % (host.scheme, re.escape(host.netloc)))
            pending, visited, urls = [company.careers_url], set(), set()
            while pending:
                if len(visited) >= 50:
                    raise RuntimeError("Teamtailor pagination limit exceeded")
                u = pending.pop()
                if u in visited:
                    continue
                visited.add(u)
                soup = _get_soup(session, u)
                for a in soup.select("a[href]"):
                    link = urljoin(u, a["href"]).split("?")[0]
                    if job_re.match(link):
                        urls.add(link)
                pending.extend(urljoin(u, a["href"]) for a in soup.select('a[rel="next"]'))
            if not urls:
                raise RuntimeError("No job links found on Teamtailor listing")
            meta.update(pages=len(visited), listing_unique=len(urls))

            for u in sorted(urls):
                d = next(_job_postings(_get_soup(session, u)), None)
                if not d:
                    raise RuntimeError("Missing JobPosting JSON-LD: " + u)
                loc = _jobposting_location(d)
                if not _is_singapore(loc):
                    continue
                raw_jobs.append(
                    {
                        "job_id": re.search(r"/jobs/(\d+)", u).group(1),
                        "title": d.get("title"),
                        "location": loc,
                        "posted_date": d.get("datePosted") or "",
                        "job_url": u,
                    }
                )
            return CollectResult(self.name, company.company, company.careers_url, raw_jobs, meta, None)
        except Exception as e:
            return CollectResult(self.name, company.company, company.careers_url, raw_jobs, meta, str(e))

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        return [
            JobRecord(
                company=result.company,
                job_title=_clean(r.get("title")),
                location=_clean(r.get("location")),
                job_id=_clean(r.get("job_id")),
                posted_date=_clean(r.get("posted_date")),
                job_url=_clean(r.get("job_url")),
                source=self.name,
                careers_url=result.careers_url,
                raw=r,
            )
            for r in result.raw_jobs
            if isinstance(r, dict)
        ]
