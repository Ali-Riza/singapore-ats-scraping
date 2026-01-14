from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


COMPANY = "Mitsubishi Electric Asia"
SOURCE = "kentico_html"

BASE_URL = "https://www.mitsubishielectric.com.sg"
LISTING_URL = f"{BASE_URL}/careers/job-listings/"

DEFAULT_TIMEOUT_S = 30
DEFAULT_SLEEP_S = 0.15


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
			"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
			"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
			"Accept-Language": "en-US,en;q=0.9",
			"Connection": "keep-alive",
		}
	)
	return s


def _normalize_url(href: str) -> str:
	href = (href or "").strip()
	if not href:
		return ""
	return urljoin(BASE_URL + "/", href)


def _slug_from_url(url: str) -> str:
	try:
		path = urlparse(url).path
	except Exception:
		path = url
	path = (path or "").rstrip("/")
	if not path:
		return ""
	return path.split("/")[-1]


def _extract_posted_date_from_text(text: str) -> Optional[str]:
	"""Best-effort; Kentico page may not expose posted date."""
	t = _clean_text(text)
	if not t:
		return None

	# YYYY-MM-DD
	m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", t)
	if m:
		return m.group(1)

	# 02 Jan 2026 / 2 January 2026
	m = re.search(
		r"\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|January|February|March|April|June|July|August|September|October|November|December)\s+(20\d{2})\b",
		t,
		re.IGNORECASE,
	)
	if m:
		# Keep as ISO-ish when possible (don’t over-parse; tests runners accept None)
		return None

	return None


def _extract_location_from_detail(soup: BeautifulSoup) -> str:
	"""Try to find a location field in the detail page text."""
	text = soup.get_text("\n", strip=True)
	# Common label patterns
	for pat in (
		r"\bLocation\b\s*[:\-]\s*(.+)",
		r"\bJob\s*Location\b\s*[:\-]\s*(.+)",
	):
		m = re.search(pat, text, re.IGNORECASE)
		if m:
			val = _clean_text(m.group(1))
			# cut off at next label-ish line break
			val = val.split("\n")[0].strip()
			# avoid giant captures
			if 1 <= len(val) <= 80:
				return val

	# Fallback: this is the Singapore site; keep it simple
	return "Singapore"


@dataclass(frozen=True)
class Job:
	company: str
	job_title: str
	location: str
	job_url: str
	job_id: str
	posted_date: Optional[str]
	source: str
	careers_url: str


def _parse_listing_jobs(html: str) -> list[dict]:
	soup = BeautifulSoup(html, "html.parser")

	out: list[dict] = []
	for card in soup.select("section.imagecta"):
		h2 = card.select_one("h2")
		a = card.select_one("a[href]")
		if not h2 or not a:
			continue
		title = _clean_text(h2.get_text(" ", strip=True))
		href = (a.get("href") or "").strip()
		url = _normalize_url(href)
		if not title or not url:
			continue
		out.append({"title": title, "url": url})

	# De-dupe by URL (Kentico sometimes repeats)
	seen: set[str] = set()
	uniq: list[dict] = []
	for j in out:
		if j["url"] in seen:
			continue
		seen.add(j["url"])
		uniq.append(j)
	return uniq


def _fetch_html(session: requests.Session, url: str, timeout_s: int) -> str:
	r = session.get(url, timeout=timeout_s)
	r.raise_for_status()
	return r.text


def scrape(
	*,
	listing_url: str = LISTING_URL,
	timeout_s: int = DEFAULT_TIMEOUT_S,
	sleep_s: float = DEFAULT_SLEEP_S,
	max_jobs: Optional[int] = None,
	debug: bool = True,
) -> list[dict]:
	with _make_session() as session:
		listing_html = _fetch_html(session, listing_url, timeout_s)
		listing_jobs = _parse_listing_jobs(listing_html)
		if debug:
			print(f"listing_jobs={len(listing_jobs)}")

		results: list[dict] = []
		for idx, item in enumerate(listing_jobs, start=1):
			if max_jobs is not None and len(results) >= max_jobs:
				break

			job_url = item["url"]
			job_title = item["title"]
			job_id = _slug_from_url(job_url)

			location = ""
			posted_date: Optional[str] = None

			try:
				detail_html = _fetch_html(session, job_url, timeout_s)
				soup = BeautifulSoup(detail_html, "html.parser")

				# If detail has a better title, use it
				h1 = soup.select_one("h1")
				if h1:
					t = _clean_text(h1.get_text(" ", strip=True))
					if t:
						job_title = t

				location = _extract_location_from_detail(soup)
				posted_date = _extract_posted_date_from_text(soup.get_text("\n", strip=True))
			except Exception as e:
				if debug:
					print(f"detail_fetch_failed idx={idx} url={job_url} err={type(e).__name__}: {e}")

			rec = {
				"company": COMPANY,
				"job_title": job_title,
				"location": location or "Singapore",
				"job_url": job_url,
				"job_id": job_id,
				"posted_date": posted_date,
				"source": SOURCE,
				"careers_url": listing_url,
			}
			results.append(rec)

			if sleep_s:
				time.sleep(sleep_s)

		return results


def main() -> None:
	ap = argparse.ArgumentParser(description="Mitsubishi Electric Asia (Kentico HTML) scraper")
	ap.add_argument("--url", default=LISTING_URL, help="Listing URL")
	ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="HTTP timeout seconds")
	ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S, help="Sleep seconds between detail requests")
	ap.add_argument("--max-jobs", type=int, default=None, help="Stop after N jobs")
	ap.add_argument("--no-debug", action="store_true", help="Disable debug prints")
	ap.add_argument(
		"--print-limit",
		type=int,
		default=10,
		help="How many job dicts to print at the end (default: 10). Use 0 to print none.",
	)
	ap.add_argument(
		"--print-all",
		action="store_true",
		help="Print all job dicts (overrides --print-limit).",
	)
	args = ap.parse_args()

	jobs = scrape(
		listing_url=args.url,
		timeout_s=args.timeout,
		sleep_s=args.sleep,
		max_jobs=args.max_jobs,
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

