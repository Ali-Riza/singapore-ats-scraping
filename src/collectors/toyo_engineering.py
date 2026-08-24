from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.core.models import CollectResult, CompanyItem, JobRecord

DEFAULT_CAREERS_URL = "https://career.toyo-eng.com/"
ATS_HOST = "toyo-eng.snar.jp"
DETAIL_PATH = "/jobboard/detail.aspx"
TIMEOUT = 30
DETAIL_WORKERS = 8

# SONAR renders this instead of a posting once the vacancy is withdrawn.
UNAVAILABLE_TEXT = "現在こちらのページはご利用になれません"

# Toyo publishes only in Japanese (career.toyo-eng.com has no /en/ variant and the
# SONAR board ignores lang/culture params), so the section headings we key on are
# Japanese while every field we emit carries an English name.
SECTION_LABELS: Dict[str, tuple[str, ...]] = {
    "employment_type": ("勤務形態",),
    "headcount": ("募集人員",),
    "department": ("募集部門",),
    "location": ("勤務地",),
    "working_conditions": ("勤務時間／諸条件", "勤務時間・諸条件"),
    "responsibilities": ("具体的な業務例（下記に限りません）",),
    "career_path": ("中長期的なキャリアプラン",),
    "requirements": ("募集要件", "応募要件"),
    "candidate_profile": ("求める人物像",),
    "travel": ("出張",),
    "compensation": ("給与・昇給",),
}

_WS_RE = re.compile(r"[ \t\r\f\v]+")


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\xa0", " ").replace("　", " ").split()).strip()


def _clean_block(value: Any) -> str:
    """Collapse horizontal whitespace but keep the line breaks SONAR uses in sections."""
    text = str(value or "").replace("\xa0", " ").replace("　", " ")
    lines: List[str] = []
    for line in text.split("\n"):
        line = _WS_RE.sub(" ", line).strip()
        # SONAR pads sections with repeated blank/duplicate lines.
        if line and (not lines or lines[-1] != line):
            lines.append(line)
    return "\n".join(lines)


def _is_detail_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc.lower() == ATS_HOST and parsed.path.lower().endswith(DETAIL_PATH)


def _job_id_from_url(url: str) -> str:
    return _clean(parse_qs(urlparse(url).query).get("id", [""])[0])


def _section_value(sections: Dict[str, str], field: str) -> str:
    for label in SECTION_LABELS[field]:
        if sections.get(label):
            return sections[label]
    return ""


class ToyoEngineeringCollector(BaseCollector):
    """Collector for Toyo Engineering's WordPress career page + SONAR ATS details.

    The listing at career.toyo-eng.com carries title, area and teaser for every
    posting and links straight to the SONAR detail page, which supplies the
    structured sections (location, requirements, compensation, ...). Detail pages
    are fetched in parallel and a failing one never drops its listing row.
    """

    name = "toyo_engineering"

    def _session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "ja,en;q=0.8",
                "User-Agent": "Mozilla/5.0",
            }
        )
        return session

    def _parse_listing(self, html: str, base_url: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        listings: List[Dict[str, Any]] = []

        for item in soup.select("li.post__item"):
            link = next(
                (
                    a
                    for a in item.find_all("a", href=True)
                    if _is_detail_url(urljoin(base_url, a["href"]))
                ),
                None,
            )
            if not link:
                continue

            job_url = urljoin(base_url, link["href"])
            job_id = _job_id_from_url(job_url)
            if not job_id:
                continue

            title = item.select_one("h3.post__item-ttl-sub")
            area = item.select_one("p.post__item-ttl-area")
            summary = item.select_one("p.post__item-content-text")

            listings.append(
                {
                    "job_id": job_id,
                    "job_title": _clean(title.get_text(" ", strip=True)) if title else "",
                    # The card prints the area as "勤務地：千葉県"; keep only the value.
                    "location_summary": re.sub(
                        r"^勤務地[：:]\s*", "", _clean(area.get_text(" ", strip=True)) if area else ""
                    ).strip(),
                    "summary": _clean(summary.get_text(" ", strip=True)) if summary else "",
                    "job_url": job_url,
                }
            )

        # The same posting can be listed under several categories.
        deduped: Dict[str, Dict[str, Any]] = {}
        for listing in listings:
            deduped.setdefault(listing["job_id"], listing)
        return list(deduped.values())

    def _expected_count(self, html: str) -> Optional[int]:
        soup = BeautifulSoup(html, "html.parser")
        node = soup.select_one("span.post__count-num")
        text = _clean(node.get_text(strip=True)) if node else ""
        return int(text) if text.isdigit() else None

    def _parse_detail(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")

        if UNAVAILABLE_TEXT in soup.get_text(" ", strip=True):
            return {"unavailable": True, "detail_title": "", "sections": {}, "apply_url": ""}

        title = soup.select_one("h3.tit")
        sections: Dict[str, str] = {}
        for box in soup.select("div.box"):
            heading = box.select_one("h4.tit3")
            if not heading:
                continue
            label = _clean(heading.get_text(" ", strip=True))
            if not label:
                continue
            # Everything in the box except the heading is the section body.
            heading.extract()
            body = _clean_block(box.get_text("\n", strip=True))
            if label not in sections:
                sections[label] = body

        return {
            "unavailable": False,
            "detail_title": _clean(title.get_text(" ", strip=True)) if title else "",
            "sections": sections,
            "apply_url": "" if soup.find("a", id="lkb_Apply") is None else "present",
        }

    def _enrich(
        self, session: requests.Session, listing: Dict[str, Any]
    ) -> Dict[str, Any]:
        response = session.get(listing["job_url"], timeout=TIMEOUT)
        response.raise_for_status()
        response.encoding = response.encoding or "utf-8"
        detail = self._parse_detail(response.text)

        sections: Dict[str, str] = detail["sections"]
        enriched = dict(listing)
        enriched.update(
            {
                "job_title": detail["detail_title"] or listing["job_title"],
                "sections": sections,
                "detail_available": not detail["unavailable"] and bool(detail["detail_title"]),
                "apply_url": listing["job_url"] if detail["apply_url"] else "",
            }
        )
        for field in SECTION_LABELS:
            enriched[field] = _section_value(sections, field)
        enriched["location"] = enriched["location"] or listing["location_summary"]
        return enriched

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"total_raw": 0, "detail_errors": 0}

        try:
            careers_url = (company.careers_url or "").strip() or DEFAULT_CAREERS_URL
            session = self._session()

            response = session.get(careers_url, timeout=TIMEOUT)
            meta["status"] = response.status_code
            response.raise_for_status()
            response.encoding = response.encoding or "utf-8"

            listings = self._parse_listing(response.text, response.url)
            expected = self._expected_count(response.text)
            if expected is not None:
                meta["expected_count"] = expected
                if expected != len(listings):
                    # Worth surfacing: the page advertises a count we did not reach.
                    meta["count_mismatch"] = True

            if listings:
                with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
                    futures = {
                        executor.submit(self._enrich, session, listing): listing
                        for listing in listings
                    }
                    for future in as_completed(futures):
                        listing = futures[future]
                        try:
                            raw_jobs.append(future.result())
                        except Exception:
                            # A detail hiccup must not drop the listing row.
                            meta["detail_errors"] += 1
                            raw_jobs.append({**listing, "detail_available": False})

            raw_jobs.sort(key=lambda job: job.get("job_title", ""))
            meta["total_raw"] = len(raw_jobs)
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )
        except Exception as exc:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=company.careers_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(exc),
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        records: List[JobRecord] = []
        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue
            title = _clean(raw.get("job_title"))
            job_url = _clean(raw.get("job_url"))
            if not title or not job_url:
                continue

            records.append(
                JobRecord(
                    company=result.company,
                    job_title=title,
                    location=_clean(raw.get("location")) or _clean(raw.get("location_summary")),
                    job_id=_clean(raw.get("job_id")),
                    # SONAR exposes no posting date on either the card or the detail page.
                    posted_date="",
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return records
