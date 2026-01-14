#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Schneider Electric (careers.se.com) scraper.

Goal (like tests/yinson.py): print one dict per job:
{
	'company', 'job_title', 'location', 'job_id', 'posted_date', 'job_url', 'source', 'careers_url'
}

Implementation note:
The listings UI is a Jibe search app and exposes a JSON endpoint at:
	https://careers.se.com/api/jobs

Using the API avoids flaky Playwright/WAF issues and is fast/reliable.
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import requests


COMPANY = "Schneider Electric"
SOURCE = "icims"

START_URL = (
	"https://careers.se.com/jobs?page=1&location=Singapore,%20Singapore"
	"&woe=7&regionCode=SG&stretchUnit=MILES&stretch=10"
)

USER_AGENT = (
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
	"AppleWebKit/537.36 (KHTML, like Gecko) "
	"Chrome/122.0.0.0 Safari/537.36"
)

HEADERS = {
	"User-Agent": USER_AGENT,
	"Accept": "application/json,text/plain,*/*",
	"Accept-Language": "en-GB,en;q=0.9,de;q=0.8",
}


def _listing_url_to_api_params(careers_url: str) -> dict[str, str]:
	"""Translate the listing URL query string into /api/jobs query params."""
	u = urlparse(careers_url)
	qs = parse_qs(u.query, keep_blank_values=True)
	params: dict[str, str] = {}
	for k, vs in qs.items():
		if not vs:
			continue
		params[k] = vs[0]
	# default to external search if not explicitly internal
	params.setdefault("internal", "false")
	return params


def _normalize_date(raw: Any) -> Optional[str]:
	if raw is None:
		return None
	s = str(raw).strip()
	if not s:
		return None

	# ISO-ish strings from the API can look like:
	#  - 2025-11-25T06:13:00+0000
	#  - 2025-11-25T06:13:00Z
	try:
		s2 = s.replace("Z", "+00:00")
		# Handle +0000 without colon
		if len(s2) >= 5 and (s2[-5] in {"+", "-"}) and s2[-2:] != ":00" and s2[-3] != ":":
			# e.g. ...+0000 -> ...+00:00
			s2 = s2[:-5] + s2[-5:-2] + ":" + s2[-2:]
		dt = datetime.fromisoformat(s2)
		return dt.date().isoformat()
	except Exception:
		pass

	for fmt in ("%Y-%m-%d", "%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y"):
		try:
			return datetime.strptime(s[:30], fmt).date().isoformat()
		except Exception:
			continue

	return s

def _job_url_from_api(job: dict) -> Optional[str]:
	md = job.get("meta_data") or {}
	url = md.get("canonical_url")
	if isinstance(url, str) and url.strip():
		return url.strip()
	slug = job.get("slug") or job.get("req_id")
	if slug:
		return f"https://careers.se.com/jobs/{slug}?lang={job.get('language','en-us')}"
	return None


def _posted_date_from_api(job: dict) -> Optional[str]:
	md = job.get("meta_data") or {}
	icims = md.get("icims") or {}
	primary = icims.get("primary_posted_site_object") if isinstance(icims, dict) else None
	if isinstance(primary, dict) and primary.get("datePosted"):
		return _normalize_date(primary.get("datePosted"))
	return _normalize_date(job.get("posted_date"))


def _location_from_api(job: dict) -> Optional[str]:
	for k in ("full_location", "short_location", "location_name"):
		v = job.get(k)
		if isinstance(v, str) and v.strip():
			return v.strip()
	country = job.get("country")
	return country.strip() if isinstance(country, str) and country.strip() else None


def scrape_schneider(
	careers_url: str,
	*,
	max_pages: int = 50,
	max_jobs: Optional[int] = None,
	delay: float = 0.0,
) -> list[dict]:
	params = _listing_url_to_api_params(careers_url)
	results: list[dict] = []
	seen_ids: set[str] = set()

	with requests.Session() as session:
		session.headers.update(HEADERS)

		for pnum in range(1, max_pages + 1):
			if delay:
				time.sleep(delay)
			params["page"] = str(pnum)
			r = session.get("https://careers.se.com/api/jobs", params=params, timeout=30)
			r.raise_for_status()
			payload = r.json()
			jobs = payload.get("jobs") or []
			if not jobs:
				break

			for item in jobs:
				job = item.get("data") if isinstance(item, dict) else None
				if not isinstance(job, dict):
					continue

				job_id = str(job.get("req_id") or job.get("slug") or "").strip() or None
				if job_id and job_id in seen_ids:
					continue
				if job_id:
					seen_ids.add(job_id)

				job_url = _job_url_from_api(job)
				rec = {
					"company": COMPANY,
					"job_title": (str(job.get("title")).strip() if job.get("title") else None),
					"location": _location_from_api(job),
					"job_id": job_id,
					"posted_date": _posted_date_from_api(job),
					"job_url": job_url,
					"source": SOURCE,
					"careers_url": careers_url,
				}
				results.append(rec)

				if max_jobs is not None and len(results) >= max_jobs:
					return results[:max_jobs]

			total = payload.get("totalCount")
			if isinstance(total, int) and len(results) >= total:
				break

	return results


def main() -> None:
	ap = argparse.ArgumentParser(description="Scrape Schneider Electric jobs and print Yinson-style dicts.")
	ap.add_argument("--url", default=START_URL, help="Listing URL")
	ap.add_argument("--max-pages", type=int, default=50, help="Max listing pages to scan")
	ap.add_argument("--max", type=int, default=None, help="Max number of jobs (debug)")
	ap.add_argument("--delay", type=float, default=0.0, help="Delay seconds between detail requests")
	args = ap.parse_args()

	jobs = scrape_schneider(
		args.url,
		max_pages=args.max_pages,
		max_jobs=args.max,
		delay=args.delay,
	)

	for j in jobs:
		print(j)
	print(f"\nTOTAL_RESULTS={len(jobs)}")


if __name__ == "__main__":
	main()
