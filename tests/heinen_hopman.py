#!/usr/bin/env python3

import argparse
import json
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Set

import requests


SOURCE = "wordpress-remix"
COMPANY = "Heinen & Hopman"
PORTAL_BASE_URL = "https://www.werkenbijheinenhopman.nl"
VACANCIES_URL = f"{PORTAL_BASE_URL}/vacatures/"


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: float = 45.0,
    max_attempts: int = 3,
    backoff_s: float = 0.8,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> requests.Response:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout, headers=headers, params=params)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_s * attempt)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def _extract_js_object_by_brace_match(text: str, start_index: int) -> str:
    """Return a JS object string that starts with '{' at start_index.

    This is a simple brace-matching scanner that is safe for JSON-like objects.
    It assumes braces are balanced and strings use double quotes.
    """
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

    raise ValueError("Unterminated object: could not find matching '}'")


def _extract_remix_context(html: str) -> Dict[str, Any]:
    marker = "window.__remixContext ="
    idx = html.find(marker)
    if idx == -1:
        raise RuntimeError("Could not find window.__remixContext in HTML")

    after = html[idx + len(marker) :]
    brace_idx = after.find("{")
    if brace_idx == -1:
        raise RuntimeError("Could not find '{' after window.__remixContext =")

    obj_start = idx + len(marker) + brace_idx
    obj_str = _extract_js_object_by_brace_match(html, obj_start)

    try:
        return json.loads(obj_str)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse __remixContext JSON: {exc}") from exc


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


def _as_abs_url(uri_or_url: Optional[str]) -> Optional[str]:
    if not uri_or_url:
        return None
    if uri_or_url.startswith("http://") or uri_or_url.startswith("https://"):
        return uri_or_url
    if uri_or_url.startswith("/"):
        return f"{PORTAL_BASE_URL}{uri_or_url}"
    return f"{PORTAL_BASE_URL}/{uri_or_url}"


def _to_canonical_job(node: Dict[str, Any]) -> Dict[str, Any]:
    recap = node.get("recap") if isinstance(node.get("recap"), dict) else {}

    return {
        "company": COMPANY,
        "job_title": node.get("title"),
        "location": recap.get("location") or recap.get("city") or recap.get("country"),
        "job_id": node.get("databaseId") or node.get("id") or node.get("uri"),
        "posted_date": None,
        "job_url": _as_abs_url(node.get("uri")),
        "source": SOURCE,
        "careers_url": VACANCIES_URL,
        "country": recap.get("country"),
        "city": recap.get("city"),
        "hours": recap.get("hours"),
        "salary": recap.get("salary"),
    }


def _filter_by_country(jobs: List[Dict[str, Any]], *, country: Optional[str]) -> List[Dict[str, Any]]:
    c = (country or "").strip().lower()
    if not c:
        return jobs

    # Simple matching. You can pass either English or Dutch.
    synonyms = {
        "netherlands": ["netherlands", "nederland"],
        "nederland": ["netherlands", "nederland"],
        "singapore": ["singapore"],
    }
    needles = synonyms.get(c, [c])

    out: List[Dict[str, Any]] = []
    for j in jobs:
        hay = " ".join([
            str(j.get("country") or ""),
            str(j.get("location") or ""),
            str(j.get("job_title") or ""),
        ]).lower()
        if any(n in hay for n in needles):
            out.append(j)
    return out


def _collect_countries(jobs: List[Dict[str, Any]]) -> List[str]:
    seen: Set[str] = set()
    for j in jobs:
        c = (j.get("country") or "").strip()
        if c:
            seen.add(c)
    return sorted(seen)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Heinen & Hopman vacancies scraper via Remix __remixContext embedded JSON. "
            "By default prints all jobs; optionally filter by country."
        )
    )
    parser.add_argument("--vacancies-url", default=VACANCIES_URL, help="Vacancies listing URL")
    parser.add_argument(
        "--country",
        default="singapore",
        help="Country filter (default: singapore). Use --country '' to disable filtering.",
    )
    parser.add_argument("--max-jobs", type=int, default=50, help="Max jobs to print")
    parser.add_argument(
        "--list-countries",
        action="store_true",
        default=False,
        help="Print detected countries and exit",
    )
    parser.add_argument("--debug", action="store_true", default=False)

    args = parser.parse_args()

    session = requests.Session()
    resp = _get_with_retries(session, args.vacancies_url)
    ctx = _extract_remix_context(resp.text)

    all_nodes = list(_iter_vacancy_nodes_from_remix_context(ctx))
    all_jobs = [_to_canonical_job(n) for n in all_nodes]

    if args.list_countries:
        for c in _collect_countries(all_jobs):
            print(c)
        return 0

    jobs = _filter_by_country(all_jobs, country=args.country)

    out_count = 0
    for job in jobs:
        if out_count >= args.max_jobs:
            break
        print(job)
        out_count += 1

    print(f"Found {out_count} jobs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
