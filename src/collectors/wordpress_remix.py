from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


def _clean_text(v: Any) -> str:
    return " ".join(str(v or "").split()).strip()


def _extract_js_object_by_brace_match(text: str, start_index: int) -> str:
    if start_index < 0 or start_index >= len(text) or text[start_index] != "{":
        raise ValueError("start_index must point to '{'")

    i = start_index
    depth = 0
    in_string = False
    escape = False

    while i < len(text):
        ch = text[i]

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
        else:
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start_index : i + 1]

        i += 1

    raise ValueError("Unterminated object")


def _extract_remix_context(html: str) -> Dict[str, Any]:
    marker = "window.__remixContext ="
    idx = (html or "").find(marker)
    if idx == -1:
        raise RuntimeError("Could not find window.__remixContext")

    after = html[idx + len(marker) :]
    brace_idx = after.find("{")
    if brace_idx == -1:
        raise RuntimeError("Could not find '{' after window.__remixContext")

    obj_start = idx + len(marker) + brace_idx
    obj_str = _extract_js_object_by_brace_match(html, obj_start)
    return json.loads(obj_str)


def _iter_vacancy_nodes_from_remix_context(ctx: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    state = ctx.get("state")
    if not isinstance(state, dict):
        return

    loader_data = state.get("loaderData")
    if not isinstance(loader_data, dict):
        return

    page_blob = loader_data.get("./templates/page")
    if not isinstance(page_blob, dict):
        return

    page = page_blob.get("page")
    if not isinstance(page, dict):
        return

    flex_content = page.get("flexContent")
    if not isinstance(flex_content, dict):
        return

    flex_items = flex_content.get("flex")
    if not isinstance(flex_items, list):
        return

    for item in flex_items:
        if not isinstance(item, dict):
            continue
        vacancies = item.get("vacancies")
        if not isinstance(vacancies, dict):
            continue
        edges = vacancies.get("edges")
        if not isinstance(edges, list):
            continue

        for edge in edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node")
            if isinstance(node, dict):
                yield node


def _as_abs_url(portal_base: str, uri_or_url: Optional[str]) -> str:
    if not uri_or_url:
        return ""
    if uri_or_url.startswith("http://") or uri_or_url.startswith("https://"):
        return uri_or_url
    if uri_or_url.startswith("/"):
        return f"{portal_base}{uri_or_url}"
    return f"{portal_base}/{uri_or_url}"


def _looks_singapore(node: Dict[str, Any]) -> bool:
    recap = node.get("recap") if isinstance(node.get("recap"), dict) else {}
    hay = " ".join(
        [
            str(recap.get("country") or ""),
            str(recap.get("location") or ""),
            str(recap.get("city") or ""),
            str(node.get("title") or ""),
        ]
    ).casefold()
    return "singapore" in hay


class WordpressRemixCollector(BaseCollector):
    name = "wordpress_remix"

    def collect_raw(self, company: CompanyItem) -> CollectResult:
        raw_jobs: List[Dict[str, Any]] = []
        meta: Dict[str, Any] = {"status": None}

        try:
            r = requests.get(company.careers_url, timeout=45, headers={"User-Agent": "Mozilla/5.0"})
            meta["status"] = r.status_code
            r.raise_for_status()

            ctx = _extract_remix_context(r.text)
            nodes = list(_iter_vacancy_nodes_from_remix_context(ctx))
            raw_jobs = [n for n in nodes if isinstance(n, dict)]

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

        base = ""
        # Best effort portal base from careers_url
        if result.careers_url.startswith("http://") or result.careers_url.startswith("https://"):
            parts = result.careers_url.split("//", 1)[1].split("/", 1)[0]
            scheme = result.careers_url.split("://", 1)[0]
            base = f"{scheme}://{parts}"

        for raw in result.raw_jobs:
            if not isinstance(raw, dict) or not _looks_singapore(raw):
                continue

            recap = raw.get("recap") if isinstance(raw.get("recap"), dict) else {}
            location = _clean_text(recap.get("location") or recap.get("city") or recap.get("country"))
            job_id = _clean_text(raw.get("databaseId") or raw.get("id") or raw.get("uri"))
            job_url = _as_abs_url(base, raw.get("uri"))

            out.append(
                JobRecord(
                    company=result.company,
                    job_title=_clean_text(raw.get("title")),
                    location=location,
                    job_id=job_id,
                    posted_date="",
                    job_url=job_url,
                    source=self.name,
                    careers_url=result.careers_url,
                    raw=raw,
                )
            )

        return out
