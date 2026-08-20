from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse
import re

import requests
from bs4 import BeautifulSoup, Tag

from src.collectors.base import BaseCollector
from src.core.models import CollectResult, CompanyItem, JobRecord


BASE_URL = "https://jobs.arup.com"
DEFAULT_START_URL = f"{BASE_URL}/page/data-centre-roles-with-arup-512"
SOURCE_NAME = "arup_selectminds"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _clean_location_text(raw_location: str) -> str:
    text = raw_location.strip()
    text = re.sub(r"^[^A-Za-z0-9]+", "", text)
    return " ".join(text.split())


class ArupSelectMindsCollector(BaseCollector):
    """Collector for Arup's SelectMinds/Taleo pages (Singapore hard-filtered)."""

    name = SOURCE_NAME

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {
            "pages": 0,
            "status": [],
            "total_rows_found": 0,
            "location_filter": "Singapore",
        }

        start_url = (company.careers_url or "").strip() or DEFAULT_START_URL
        parsed_start = urlparse(start_url)
        base_url = f"{parsed_start.scheme}://{parsed_start.netloc}"
        if parsed_start.netloc.endswith("selectminds.com") and parsed_start.path.rstrip("/") == "":
            start_url = f"{base_url}/latest-jobs"

        try:
            with requests.Session() as session:
                session.headers.update(HEADERS)

                response = session.get(start_url, timeout=45)
                meta["status"].append(response.status_code)
                response.raise_for_status()
                meta["pages"] += 1

                soup = BeautifulSoup(response.text, "lxml")
                tss_token = self._extract_tss_token(soup)
                search_id = self._extract_search_id(soup)
                num_pages = self._extract_num_pages(soup)

                all_rows: List[Tag] = list(soup.select("div.job_list_row"))

                if search_id and num_pages > 1:
                    for page_index in range(2, num_pages + 1):
                        page_rows, status_code = self._fetch_page_rows(
                            session=session,
                            start_url=start_url,
                            search_id=search_id,
                            page_index=page_index,
                            tss_token=tss_token,
                            base_url=base_url,
                        )
                        meta["status"].append(status_code)
                        if status_code != 200:
                            break
                        meta["pages"] += 1
                        all_rows.extend(page_rows)

                meta["total_rows_found"] = len(all_rows)

                seen: set[str] = set()
                for row in all_rows:
                    parsed = self._parse_job_row(row, start_url, base_url)
                    if not parsed:
                        continue

                    location = (parsed.get("location") or "").lower()
                    if "singapore" not in location:
                        continue

                    job_id = str(parsed.get("job_id") or "")
                    if not job_id or job_id in seen:
                        continue

                    seen.add(job_id)
                    raw_jobs.append(parsed)

            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=start_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=None,
            )
        except Exception as exc:
            return CollectResult(
                collector=self.name,
                company=company.company,
                careers_url=start_url,
                raw_jobs=raw_jobs,
                meta=meta,
                error=str(exc),
            )

    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        records: List[JobRecord] = []

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            job_title = str(raw.get("job_title") or "").strip()
            job_id = str(raw.get("job_id") or "").strip()
            job_url = str(raw.get("job_url") or "").strip()
            posted_date = str(raw.get("posted_date") or "").strip()

            if not (job_title and job_id and job_url):
                continue

            records.append(
                JobRecord(
                    company=result.company,
                    job_title=job_title,
                    location="Singapore",
                    job_id=job_id,
                    posted_date=posted_date,
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return records

    def _extract_num_pages(self, soup: BeautifulSoup) -> int:
        node = soup.select_one("#jNumPagesInit")
        if not isinstance(node, Tag):
            return 1
        raw = (node.get("value") or "1").strip()
        try:
            return max(1, int(float(raw)))
        except ValueError:
            return 1

    def _extract_search_id(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("#jSearchId")
        if not isinstance(node, Tag):
            return ""
        return (node.get("value") or "").strip()

    def _extract_tss_token(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("#tsstoken")
        if not isinstance(node, Tag):
            return ""
        return (node.get("value") or "").strip()

    def _fetch_page_rows(
        self,
        *,
        session: requests.Session,
        start_url: str,
        search_id: str,
        page_index: int,
        tss_token: str,
        base_url: str,
    ) -> tuple[List[Tag], int]:
        uid = int(datetime.now().timestamp() * 1000) % 1000
        ajax_url = (
            f"{base_url}/ajax/content/landingpage_job_results"
            f"?JobSearch.id={search_id}&page_index={page_index}&uid={uid}"
        )

        ajax_headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Origin": base_url,
            "Referer": start_url,
            "X-Requested-With": "XMLHttpRequest",
        }
        if tss_token:
            ajax_headers["tss-token"] = tss_token

        response = session.post(ajax_url, headers=ajax_headers, timeout=45)
        status_code = response.status_code
        if status_code != 200:
            return [], status_code

        payload = response.json()
        html = payload.get("Result") if isinstance(payload, dict) else ""
        if not isinstance(html, str) or not html.strip():
            return [], status_code

        page_soup = BeautifulSoup(html, "lxml")
        return list(page_soup.select("div.job_list_row")), status_code

    def _parse_job_row(self, row: Tag, careers_url: str, base_url: str = BASE_URL) -> Dict[str, Any] | None:
        link = row.select_one("a.job_link")
        if not isinstance(link, Tag):
            return None

        title = link.get_text(" ", strip=True)
        rel_url = (link.get("href") or "").strip()
        if not title or not rel_url:
            return None

        job_url = urljoin(base_url, rel_url)

        job_id = ""
        row_id = (row.get("id") or "").strip()
        if row_id.startswith("job_list_"):
            job_id = row_id.replace("job_list_", "", 1)

        location = ""
        location_link = row.select_one("p.jlr_location a.location")
        if isinstance(location_link, Tag):
            location = _clean_location_text(location_link.get_text(" ", strip=True))

        requisition = ""
        req_node = row.select_one(".job_external_id .field_value")
        if isinstance(req_node, Tag):
            requisition = req_node.get_text(" ", strip=True)

        total_views = ""
        views_node = row.select_one(".job_total_views .field_value")
        if isinstance(views_node, Tag):
            total_views = views_node.get_text(" ", strip=True)

        summary = ""
        summary_node = row.select_one("p.jlr_description")
        if isinstance(summary_node, Tag):
            summary = summary_node.get_text(" ", strip=True)

        return {
            "job_id": job_id,
            "job_title": title,
            "location": location,
            "requisition_number": requisition,
            "total_views": total_views,
            "summary": summary,
            "job_url": job_url,
            "source": SOURCE_NAME,
            "careers_url": careers_url,
            "posted_date": "",
        }
