from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

from src.collectors.base import BaseCollector
from src.core.models import CollectResult, CompanyItem, JobRecord

DEFAULT_CAREERS_URL = "https://jel.applyourjobs.com/"
EXPECTED_HOST = "jel.applyourjobs.com"
NAV_TIMEOUT = 60_000
WAIT_TIMEOUT = 30_000

# The listing rows only appear after the site's own JS fills #divDynamicList,
# so we wait for either a row or the explicit empty-state text.
LISTING_READY_JS = """
() =>
    document.querySelector("#divDynamicList .job-listing-details") !== null ||
    /No job\\(s\\) found/i.test(
        document.querySelector("#divDynamicList")?.innerText || ""
    )
"""

LISTING_ROWS_JS = """
rows => {
    const clean = value =>
        String(value ?? "").replace(/\\u00a0/g, " ").replace(/\\s+/g, " ").trim();

    const cleanValue = value => {
        const result = clean(value).replace(/^:\\s*/, "");
        if (!result || result.startsWith("!") || result === "cls_remove_empty") {
            return null;
        }
        return result;
    };

    return rows
        .map(row => {
            const fields = {};
            for (const group of Array.from(row.children)) {
                const cells = Array.from(group.children);
                if (cells.length < 2) {
                    continue;
                }
                const label = clean(cells[0].textContent);
                if (label) {
                    fields[label] = cleanValue(cells[1].textContent);
                }
            }

            const href = row
                .querySelector(".sendto-detail-page")
                ?.getAttribute("href");
            if (!href) {
                return null;
            }

            return {
                job_id: fields["Job ID"],
                job_title: fields["Job Title"],
                job_url: new URL(href, location.href).href,
                posted_date: fields["Posting Date"],
                specialization: fields["Job Specialization"],
                sub_specializations: fields["Job Sub Specialization"]
                    ? fields["Job Sub Specialization"]
                          .split(";")
                          .map(clean)
                          .filter(Boolean)
                    : [],
                employment_type: fields["Job Type"],
                location: fields["Work Location"]
            };
        })
        .filter(Boolean);
}
"""

# Detail values are injected after the title renders; until then the fields
# still carry the site's "!placeholder" markers.
DETAIL_READY_JS = """
() => [...document.querySelectorAll("[removeifnodata]")].some(row => {
    const fields = row.querySelectorAll(".form-label");
    return (
        fields.length >= 2 &&
        fields[0].textContent.trim() === "Work Location" &&
        !fields[fields.length - 1].textContent.includes("!")
    );
})
"""

DETAIL_JS = """
() => {
    const clean = value =>
        String(value ?? "")
            .replace(/\\u00a0/g, " ")
            .replace(/[ \\t]+/g, " ")
            .replace(/\\s*\\n\\s*/g, "\\n")
            .trim();

    const cleanValue = value => {
        const result = clean(value).replace(/^:\\s*/, "");
        if (!result || result.startsWith("!") || result === "cls_remove_empty") {
            return null;
        }
        return result;
    };

    const header = clean(
        document.querySelector("#ctl00_body_dvJobHeader")?.textContent
    );
    const dates = header.match(
        /Advertised on\\s*:\\s*(.*?)\\s*\\|\\s*Closing Date\\s*:\\s*(.*)$/i
    );

    const fields = {};
    for (const row of document.querySelectorAll("[removeifnodata]")) {
        const labels = row.querySelectorAll(".form-label");
        if (labels.length < 2) {
            continue;
        }
        const label = clean(labels[0].textContent);
        const value = cleanValue(labels[labels.length - 1].textContent);
        if (label && value !== null) {
            fields[label] = value;
        }
    }

    const description = document.querySelector("#ctl00_body_tbJobDesc");

    return {
        posted_date: dates?.[1] || null,
        closing_date: dates?.[2] || null,
        specialization: fields["Specialization"] || null,
        employment_type: fields["Type of Employment"] || null,
        minimum_experience: fields["Minimum Experience"] || null,
        location: fields["Work Location"] || null,
        description: clean(
            description?.innerText || description?.textContent
        ) || null
    };
}
"""


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\xa0", " ").split()).strip()


def _iso_date(value: Any) -> str:
    """The site prints dates as '05 Aug 2026'; anything else is dropped."""
    text = _clean(value)
    if not text:
        return ""
    try:
        return datetime.strptime(text, "%d %b %Y").date().isoformat()
    except ValueError:
        return ""


class JurongEngineeringCollector(BaseCollector):
    """Collector for the ASP.NET 'applyourjobs' portal used by Jurong Engineering.

    The listing and the detail pages are both rendered client-side, so the whole
    run happens in one browser: one listing page for the job rows plus a reused
    detail page that enriches every row with description and closing date.
    """

    name = "jurong_engineering"

    def _collect_listings(self, page) -> List[Dict[str, Any]]:
        page.wait_for_function(LISTING_READY_JS, timeout=WAIT_TIMEOUT)
        return page.locator("#divDynamicList .job-listing-details").evaluate_all(
            LISTING_ROWS_JS
        )

    def _enrich(self, page, listing: Dict[str, Any]) -> Dict[str, Any]:
        page.goto(listing["job_url"], wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        page.wait_for_selector("#ctl00_body_lbljobtitle", timeout=WAIT_TIMEOUT)
        page.wait_for_function(DETAIL_READY_JS, timeout=WAIT_TIMEOUT)
        detail = page.evaluate(DETAIL_JS)

        enriched = dict(listing)
        enriched.update(
            {
                "posted_date": detail["posted_date"] or listing.get("posted_date"),
                "closing_date": detail["closing_date"],
                "specialization": detail["specialization"]
                or listing.get("specialization"),
                "employment_type": detail["employment_type"]
                or listing.get("employment_type"),
                "minimum_experience": detail["minimum_experience"],
                "location": detail["location"] or listing.get("location"),
                "description": detail["description"],
            }
        )
        return enriched

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"total_raw": 0, "detail_errors": 0}

        try:
            careers_url = (company.careers_url or "").strip() or DEFAULT_CAREERS_URL
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                try:
                    listing_page = browser.new_page()
                    listing_page.goto(
                        careers_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT
                    )

                    host = urlparse(listing_page.url).hostname
                    if host != EXPECTED_HOST:
                        raise RuntimeError(f"Unexpected host: {host}")
                    meta["host"] = host

                    listings = self._collect_listings(listing_page)
                    listing_page.close()

                    if listings:
                        detail_page = browser.new_page()
                        for listing in listings:
                            # A detail-page hiccup must not drop the listing row.
                            try:
                                raw_jobs.append(self._enrich(detail_page, listing))
                            except Exception:
                                meta["detail_errors"] += 1
                                raw_jobs.append(listing)
                        detail_page.close()
                finally:
                    browser.close()

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
                    location=_clean(raw.get("location")) or "Singapore",
                    job_id=_clean(raw.get("job_id")),
                    posted_date=_iso_date(raw.get("posted_date")),
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )
        return records
