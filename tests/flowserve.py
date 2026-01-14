from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests


# =========================
# CONFIG (FLOWSERVE / JOBSYN SOLR)
# =========================

COMPANY = "Flowserve"
SOURCE = "jobsyn_solr"

CAREERS_URL = "https://flowservecareers.com/"
API_URL = "https://prod-search-api.jobsyn.org/api/v1/solr/search"

DEFAULT_LOCATION = "singapore"
DEFAULT_NUM_ITEMS = 15
DEFAULT_MAX_PAGES = 200
DEFAULT_TIMEOUT_S = 30
DEFAULT_SLEEP_S = 0.1

# Based on Flowserve HTML config: public.x-origin = "flowservecareers.com"
HEADERS = {
	"accept": "application/json, text/plain, */*",
	"accept-language": "en-US,en;q=0.9",
	"origin": "https://flowservecareers.com",
	"referer": "https://flowservecareers.com/",
	"x-origin": "flowservecareers.com",
	"user-agent": "Mozilla/5.0",
}


# =========================
# HELPERS
# =========================


def _clean_text(v: Any) -> str:
	return " ".join(str(v or "").split()).strip()


def _pick(d: dict, keys: list[str], default: Any = None) -> Any:
	for k in keys:
		if k in d and d[k] not in (None, "", [], {}):
			return d[k]
	return default


def _first_list_of_dicts(obj: Any) -> Optional[list[dict]]:
	if isinstance(obj, list):
		if obj and all(isinstance(x, dict) for x in obj):
			return obj
		for x in obj:
			res = _first_list_of_dicts(x)
			if res is not None:
				return res
		return None

	if isinstance(obj, dict):
		for k in ("docs", "jobs", "results", "items", "data", "response", "payload"):
			if k in obj:
				res = _first_list_of_dicts(obj[k])
				if res is not None:
					return res
		for v in obj.values():
			res = _first_list_of_dicts(v)
			if res is not None:
				return res

	return None


def _extract_docs(payload: Any) -> list[dict]:
	"""Extract job documents from common Solr-ish response shapes."""
	if isinstance(payload, dict):
		# Jobsyn/NLX wrapper: {"jobs": [...]}
		jobs = payload.get("jobs")
		if isinstance(jobs, list) and all(isinstance(x, dict) for x in jobs):
			return jobs

		# Classic Solr: {"response": {"docs": [...]}}
		resp = payload.get("response")
		if isinstance(resp, dict):
			docs = resp.get("docs")
			if isinstance(docs, list) and all(isinstance(x, dict) for x in docs):
				return docs

		# Some wrappers: {"data": {"response": {"docs": [...]}}}
		data = payload.get("data")
		if isinstance(data, dict):
			resp2 = data.get("response")
			if isinstance(resp2, dict):
				docs2 = resp2.get("docs")
				if isinstance(docs2, list) and all(isinstance(x, dict) for x in docs2):
					return docs2

		# Direct docs
		docs3 = payload.get("docs")
		if isinstance(docs3, list) and all(isinstance(x, dict) for x in docs3):
			return docs3

	return _first_list_of_dicts(payload) or []


def _normalize_date(v: Any) -> Optional[str]:
	if v is None:
		return None

	# epoch seconds/ms
	if isinstance(v, (int, float)):
		ts = float(v)
		if ts > 10_000_000_000:  # likely ms
			ts = ts / 1000.0
		dt = datetime.fromtimestamp(ts, tz=timezone.utc)
		return dt.date().isoformat()

	s = _clean_text(v)
	if not s:
		return None

	# ISO date already
	if len(s) >= 10 and s[4] == "-" and s[7] == "-":
		try:
			dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
			return dt.date().isoformat()
		except Exception:
			return s[:10]

	try:
		dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
		return dt.date().isoformat()
	except Exception:
		return None


def _format_location(job: dict) -> str:
	loc = _pick(
		job,
		[
			"location",
			"location_name",
			"jobLocation",
			"job_location",
			"location_s",
			"location_display",
			"location_text",
			"job_location_s",
			"location_exact",
		],
	)
	if isinstance(loc, str) and loc.strip():
		return _clean_text(loc)

	all_locations = _pick(job, ["all_locations", "allLocations"], None)
	if isinstance(all_locations, list):
		parts = [_clean_text(x) for x in all_locations if _clean_text(x)]
		if parts:
			return ", ".join(parts)

	city = _pick(job, ["city", "jobCity", "city_exact"])
	state = _pick(job, ["state", "region", "jobState"])
	country = _pick(job, ["country", "jobCountry", "country_exact", "country_short_exact"])

	parts2 = [p for p in [_clean_text(city), _clean_text(state), _clean_text(country)] if p]
	return ", ".join(parts2).strip()


def _extract_job_id(job: dict) -> str:
	jid = _pick(
		job,
		[
			"job_id",
			"jobId",
			"jobID",
			"req_id",
			"reqId",
			"requisitionId",
			"requisition_id",
			"reqid",
			"id",
			"uuid",
		],
		"",
	)
	return _clean_text(jid)


def _extract_title(job: dict) -> str:
	return _clean_text(
		_pick(
			job,
			[
				"job_title",
				"jobTitle",
				"title",
				"positionTitle",
				"position_title",
				"job_title_s",
				"job_title_t",
				"title_s",
				"title_t",
				"title_exact",
				"title_slab_exact",
				"posting_title",
				"seo_job_title",
				"seo_title",
				"name",
			],
			"",
		)
	)


def _extract_posted_date(job: dict) -> Optional[str]:
	return _normalize_date(
		_pick(
			job,
			[
				"posted_date",
				"postedDate",
				"datePosted",
				"date_posted",
				"postedAt",
				"createdAt",
				"createDate",
				"created_date",
				"date_added",
				"date_new",
				"date_updated",
				"salted_date",
			],
			None,
		)
	)


def _extract_job_url(job: dict) -> str:
	url = _pick(
		job,
		[
			"job_url",
			"jobUrl",
			"url",
			"applyUrl",
			"apply_url",
			"canonicalUrl",
			"canonical_url",
			"detailUrl",
			"detail_url",
			"job_url_s",
			"apply_url_s",
			"canonical_url_s",
			"seo_url",
			"seoUrl",
		],
	)
	if isinstance(url, str) and url.strip():
		u = url.strip()
		if u.startswith("/"):
			return f"{CAREERS_URL.rstrip('/')}{u}"
		return u

	# Best-effort URL construction
	slug = _pick(job, ["title_slug", "titleSlug"], "")
	guid = _pick(job, ["guid"], "")
	if not guid:
		job_id = _extract_job_id(job)
		if job_id and "." in job_id:
			guid = job_id.split(".")[-1]

	slug = _clean_text(slug)
	guid = _clean_text(guid)
	base = CAREERS_URL.rstrip("/")
	if slug and guid:
		return f"{base}/job/{slug}/{guid}"
	if guid:
		return f"{base}/job/{guid}"
	return ""


# =========================
# SCRAPER
# =========================


def fetch_page(
	session: requests.Session,
	*,
	location: str,
	page: int,
	num_items: int,
	timeout_s: int,
) -> dict:
	params = {
		"location": location,
		"page": str(page),
		"num_items": str(num_items),
	}
	r = session.get(API_URL, params=params, timeout=timeout_s)
	r.raise_for_status()
	return r.json()


def scrape(
	*,
	location: str = DEFAULT_LOCATION,
	num_items: int = DEFAULT_NUM_ITEMS,
	max_pages: int = DEFAULT_MAX_PAGES,
	max_jobs: Optional[int] = None,
	sleep_s: float = DEFAULT_SLEEP_S,
	timeout_s: int = DEFAULT_TIMEOUT_S,
	debug: bool = True,
) -> list[dict]:
	results: list[dict] = []
	seen: set[str] = set()

	with requests.Session() as session:
		session.headers.update(HEADERS)

		for page in range(1, max_pages + 1):
			payload = fetch_page(
				session, location=location, page=page, num_items=num_items, timeout_s=timeout_s
			)

			jobs_raw = _extract_docs(payload)
			if debug:
				print(f"page={page} jobs_raw={len(jobs_raw)}")

			if debug and page == 1:
				if isinstance(payload, dict):
					print("payload_keys(sample):", sorted(list(payload.keys()))[:40])
				if jobs_raw:
					first = jobs_raw[0]
					if isinstance(first, dict):
						print("first_doc_keys(sample):", sorted(list(first.keys()))[:80])

			if not jobs_raw:
				break

			added_this_page = 0
			for job in jobs_raw:
				if not isinstance(job, dict):
					continue

				job_id = _extract_job_id(job)
				url = _extract_job_url(job)
				title = _extract_title(job)
				dedupe_key = job_id or (url or "") + "|" + (title or "")

				if not dedupe_key or dedupe_key in seen:
					continue
				seen.add(dedupe_key)

				rec = {
					"company": COMPANY,
					"job_title": title,
					"location": _format_location(job),
					"job_url": url,
					"job_id": job_id,
					"posted_date": _extract_posted_date(job),
					"source": SOURCE,
					"careers_url": CAREERS_URL,
				}
				results.append(rec)
				added_this_page += 1

				if max_jobs is not None and len(results) >= max_jobs:
					return results[:max_jobs]

			if debug:
				print(f"page={page} added_unique={added_this_page} total_unique={len(results)}")

			if added_this_page == 0:
				break

			if sleep_s:
				time.sleep(sleep_s)

	return results


def main() -> None:
	ap = argparse.ArgumentParser(description="Flowserve (jobsyn Solr) scraper for Singapore.")
	ap.add_argument("--location", default=DEFAULT_LOCATION, help="Location query (default: singapore)")
	ap.add_argument("--num-items", type=int, default=DEFAULT_NUM_ITEMS, help="Items per page")
	ap.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES, help="Max pages to scan")
	ap.add_argument("--max-jobs", type=int, default=None, help="Stop after N unique jobs (debug)")
	ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S, help="Sleep seconds between pages")
	ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="HTTP timeout seconds")
	ap.add_argument("--no-debug", action="store_true", help="Disable debug prints")
	ap.add_argument(
		"--print-limit",
		type=int,
		default=3,
		help="How many job dicts to print at the end (default: 3). Use 0 to print none.",
	)
	ap.add_argument(
		"--print-all",
		action="store_true",
		help="Print all job dicts (overrides --print-limit).",
	)
	args = ap.parse_args()

	jobs = scrape(
		location=args.location,
		num_items=args.num_items,
		max_pages=args.max_pages,
		max_jobs=args.max_jobs,
		sleep_s=args.sleep,
		timeout_s=args.timeout,
		debug=not args.no_debug,
	)

	print(f"Found {len(jobs)} jobs")

	if args.print_all:
		for j in jobs:
			print(j)
	elif args.print_limit and args.print_limit > 0:
		for j in jobs[: args.print_limit]:
			print(j)


if __name__ == "__main__":
	main()

