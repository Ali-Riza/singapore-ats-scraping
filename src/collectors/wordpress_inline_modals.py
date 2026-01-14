from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


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
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return s


def _extract_cards(soup: BeautifulSoup) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []

    for a in soup.select(".career-box a[data-bs-target]"):
        target = (a.get("data-bs-target") or "").strip()
        if not target.startswith("#"):
            continue
        modal_id = target[1:]

        h6 = a.select_one("h6")
        title = _clean_text(h6.get_text(" ", strip=True) if h6 else "")
        if title and modal_id:
            out.append({"title": title, "modal_id": modal_id})

    # de-dupe by modal_id
    seen: set[str] = set()
    uniq: List[Dict[str, str]] = []
    for j in out:
        if j["modal_id"] in seen:
            continue
        seen.add(j["modal_id"])
        uniq.append(j)

    return uniq


def _extract_modal_details(soup: BeautifulSoup, modal_id: str, careers_url: str) -> Dict[str, str]:
    modal = soup.find(id=modal_id)
    if modal is None:
        return {}

    title_el = modal.select_one("h3")
    title = _clean_text(title_el.get_text(" ", strip=True) if title_el else "")

    apply_a = modal.select_one('a[href][class*="custom-button"]')
    apply_url = (apply_a.get("href") or "").strip() if apply_a else ""
    if apply_url:
        apply_url = urljoin(careers_url, apply_url)

    content_el = modal.select_one(".cereer-modal-content")
    description = _clean_text(content_el.get_text("\n", strip=True) if content_el else "")

    return {"title": title, "apply_url": apply_url, "description": description}


class WordpressInlineModalsCollector(BaseCollector):
    name = "wordpress_inline_modals"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None}

        try:
            with _make_session() as session:
                r = session.get(company.careers_url, timeout=30)
                meta["status"] = r.status_code
                r.raise_for_status()

                soup = BeautifulSoup(r.text, "html.parser")
                cards = _extract_cards(soup)

                for card in cards:
                    modal_id = card["modal_id"]
                    details = _extract_modal_details(soup, modal_id, company.careers_url)
                    raw_jobs.append({**card, **details})

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

        for raw in result.raw_jobs:
            if not isinstance(raw, dict):
                continue

            modal_id = _clean_text(raw.get("modal_id"))
            job_title = _clean_text(raw.get("title"))
            job_url = f"{result.careers_url.rstrip('/')}/#{modal_id}" if modal_id else result.careers_url
            job_id = modal_id.replace("careerModal", "") if modal_id.startswith("careerModal") else modal_id

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=job_title,
                    location="Singapore",
                    job_id=job_id,
                    posted_date="",
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return out
