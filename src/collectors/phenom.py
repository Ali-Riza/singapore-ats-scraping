from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse, parse_qs, urlunparse

import requests

from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord


SG_COUNTRY = "Singapore"


def _slugify(text: str) -> str:
	s = (text or "").strip().lower()
	s = re.sub(r"[^a-z0-9]+", "-", s)
	s = re.sub(r"-{2,}", "-", s).strip("-")
	return s or "job"


def _set_query_params(url: str, params: Dict[str, str]) -> str:
	"""Set/override query params on url; keeps other params."""
	u = urlparse(url)
	q = parse_qs(u.query, keep_blank_values=True)
	for k, v in params.items():
		if v is None:
			continue
		q[k] = [str(v)]
	new_q = urlencode({k: vs[0] for k, vs in q.items() if vs and vs[0] is not None}, doseq=False)
	return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))


def _brace_match_object(text: str, start_idx: int) -> str:
	"""Return balanced JSON object substring starting at '{' (string/escape aware)."""
	if start_idx < 0 or start_idx >= len(text) or text[start_idx] != "{":
		raise ValueError("start_idx must point to '{'")

	depth = 0
	in_str = False
	esc = False
	quote = ""

	for i in range(start_idx, len(text)):
		ch = text[i]

		if in_str:
			if esc:
				esc = False
				continue
			if ch == "\\":
				esc = True
				continue
			if ch == quote:
				in_str = False
				quote = ""
			continue

		if ch in ('"', "'"):
			in_str = True
			quote = ch
			continue

		if ch == "{":
			depth += 1
		elif ch == "}":
			depth -= 1
			if depth == 0:
				return text[start_idx : i + 1]

	raise RuntimeError("Brace matching failed (unterminated object).")


def _extract_json_object_by_anchor(html: str, anchor: str) -> Dict[str, Any]:
	"""Extract JSON object assigned after an anchor like 'phApp.ddo =' using brace matching."""
	idx = html.find(anchor)
	if idx == -1:
		raise RuntimeError(f"Anchor not found: {anchor}")
	start = html.find("{", idx)
	if start == -1:
		raise RuntimeError("Could not find opening '{' after anchor")
	obj_text = _brace_match_object(html, start)
	return json.loads(obj_text)


def _extract_eager_block(html: str) -> Dict[str, Any]:
	"""Return the eagerLoadRefineSearch object as dict.

	Supports these patterns observed in tests:
	- phApp.ddo = { eagerLoadRefineSearch: {...} }
	- ... "eagerLoadRefineSearch": { ... } ...  (embedded JSON)
	- ... eagerLoadRefineSearch = { ... } ...
	"""

	# Pattern 1: full DDO object
	for anchor in ("phApp.ddo =", "phApp.ddo="):
		if anchor in html:
			ddo = _extract_json_object_by_anchor(html, anchor)
			eager = ddo.get("eagerLoadRefineSearch")
			if isinstance(eager, dict):
				return eager

	# Pattern 2: JSON key
	key = '"eagerLoadRefineSearch"'
	idx = html.find(key)
	if idx != -1:
		colon = html.find(":", idx + len(key))
		if colon != -1:
			brace = html.find("{", colon)
			if brace != -1:
				obj_text = _brace_match_object(html, brace)
				return json.loads(obj_text)

	# Pattern 3: assignment-style
	key2 = "eagerLoadRefineSearch"
	idx2 = html.find(key2)
	if idx2 != -1:
		brace = html.find("{", idx2)
		if brace != -1:
			obj_text = _brace_match_object(html, brace)
			return json.loads(obj_text)

	raise RuntimeError("Could not extract eagerLoadRefineSearch from HTML")


def _jobs_from_eager(eager: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int, int]:
	jobs = ((eager.get("data") or {}).get("jobs") or [])
	if not isinstance(jobs, list):
		jobs = []
	hits = int(eager.get("hits") or 0)
	total = int(eager.get("totalHits") or 0)
	# fallback (avoid infinite loop when hits missing)
	if hits <= 0:
		hits = len(jobs)
	return [j for j in jobs if isinstance(j, dict)], hits, total


def _is_singapore_job(job: Dict[str, Any], *, company_name: str = "") -> bool:
	"""Best-effort Singapore filter; keeps SRP by staying in collector fetch/mapping layer."""
	# ABB needs stricter check because search facets can still include other locations.
	if (company_name or "").strip().lower() == "abb":
		target = "Singapore, Central Singapore, Singapore"
		loc = str(job.get("location") or "")
		city_country = str(job.get("cityCountry") or "")
		# Keep Singapore if present in any of the known location fields.
		# Important: some ABB postings are multi-location and the primary "location" can be non-SG.
		if target in loc:
			return True
		if "Singapore" in city_country:
			return True
		multi = job.get("multi_location") or []
		if isinstance(multi, list) and any(target == (x or "") for x in multi):
			return True
		mla = job.get("multi_location_array") or []
		if isinstance(mla, list):
			for item in mla:
				if isinstance(item, dict) and item.get("location") == target:
					return True
		return False

	# Generic (Trane-like)
	if str(job.get("country") or "") == SG_COUNTRY:
		return True
	loc = str(job.get("location") or "")
	if SG_COUNTRY in loc:
		return True
	multi_locs = job.get("multi_location") or []
	if isinstance(multi_locs, list) and any(SG_COUNTRY in str(x or "") for x in multi_locs):
		return True
	return False


def _pick_preferred_location(raw: Dict[str, Any]) -> str:
	"""Pick a human-friendly location, preferring Singapore when available.

	Phenom jobs can be multi-location. When any Singapore location exists, we prefer
	showing that in the output to avoid confusing rows like "Shanghai" in a SG-only run.
	"""
	primary = _pick_first(raw, "location", "cityStateCountry", "cityCountry")
	if primary and SG_COUNTRY in primary:
		return primary

	# Try multi-location string list
	multi = raw.get("multi_location")
	if isinstance(multi, list):
		for item in multi:
			s = str(item or "")
			if SG_COUNTRY in s:
				return s

	# Try multi-location array of dicts
	mla = raw.get("multi_location_array")
	if isinstance(mla, list):
		for item in mla:
			if isinstance(item, dict):
				s = str(item.get("location") or "")
				if SG_COUNTRY in s:
					return s

	# Fall back to primary; if missing, fall back to first multi-location
	if primary:
		return primary
	if isinstance(multi, list) and multi:
		return str(multi[0] or "")
	return ""


def _base_public_from_search_url(careers_url: str) -> str:
	base = careers_url.split("?", 1)[0]
	base = base.rstrip("/")
	if base.endswith("/search-results"):
		base = base[: -len("/search-results")]
	return base.rstrip("/") + "/"


def _pick_first(raw: Dict[str, Any], *keys: str) -> str:
	for k in keys:
		v = raw.get(k)
		# Some tenants return JSON booleans for missing values (e.g. applyUrl=false).
		# Treat booleans as missing to avoid exporting literal "False" strings.
		if v is None or v == "" or isinstance(v, bool):
			continue
		if v not in (None, "", [], {}):
			return str(v)
	return ""


def _derive_job_url(careers_url: str, raw: Dict[str, Any]) -> str:
	# Prefer explicit URLs if present
	url = _pick_first(
		raw,
		"applyUrl",
		"externalApplyUrl",
		"externalApply",
		"jobUrl",
		"jobURL",
		"jobDetailUrl",
		"detailUrl",
	)
	if url:
		return url

	# Otherwise derive from jobSeqNo and title (common Phenom pattern)
	job_seq = _pick_first(raw, "jobSeqNo")
	if job_seq:
		title = _pick_first(raw, "title")
		base = _base_public_from_search_url(careers_url)
		path = f"job/{job_seq}/{_slugify(title)}" if title else f"job/{job_seq}"
		return urljoin(base, path)

	return ""


@dataclass(frozen=True)
class PhenomQueryConfig:
	extra_params: Dict[str, str]


def _config_for_company(company: CompanyItem) -> PhenomQueryConfig:
	"""Company-specific search parameter defaults.

	Important: this stays minimal and only covers the 4 known scripts.
	Most Phenom tenants already embed filters inside the provided careers_url.
	"""
	name = (company.company or "").strip().lower()
	ats = (company.ats_type or "").strip().lower()

	# ABB: tests expect qcountry=Singapore
	if name == "abb" or "abb" in ats:
		return PhenomQueryConfig(extra_params={"qcountry": SG_COUNTRY})

	# Others: rely on their careers_url params (location=Singapore, etc.)
	return PhenomQueryConfig(extra_params={})


class PhenomCollector(BaseCollector):
	"""Collector for Phenom-hosted career sites.

	SRP: This collector only (1) fetches raw jobs and (2) maps them to JobRecord.
	Normalize/validate/dedupe happen in pipeline modules.
	"""

	name = "phenom"

	def _choose_effective_query_params(
		self,
		*,
		session: requests.Session,
		careers_url: str,
		base_params: Dict[str, str],
	) -> Tuple[Dict[str, str], Dict[str, Any]]:
		"""Pick query params that likely enforce server-side Singapore filtering.

		Why: Some Phenom tenants ignore/omit Singapore filters in the provided UI URL,
		so we end up paging through global listings (very slow) and filtering client-side.
		We probe once (offset=0) and choose the param set that yields the smallest
		positive totalHits.
		"""
		meta_hint: Dict[str, Any] = {
			"probe_ran": False,
			"picked": dict(base_params),
			"candidates": [],
		}

		# If the URL already appears to be Singapore-scoped, don't add extra probing.
		u = urlparse(careers_url)
		q = parse_qs(u.query, keep_blank_values=True)
		q_flat = {k: (v[0] if v else "") for k, v in q.items()}
		already_scoped = False
		for k, v in q_flat.items():
			vv = str(v or "").lower()
			if k.lower() in ("qcountry", "country", "qlocation", "location") and "singapore" in vv:
				already_scoped = True
				break
		if "qcountry" in (k.lower() for k in q_flat.keys()):
			already_scoped = True

		if already_scoped:
			meta_hint["picked"] = dict(base_params)
			return dict(base_params), meta_hint

		# Candidates: as-is vs. forced qcountry=Singapore.
		candidates: List[Dict[str, str]] = [dict(base_params), {**base_params, "qcountry": SG_COUNTRY}]
		meta_hint["probe_ran"] = True

		best_params = dict(base_params)
		best_total: Optional[int] = None

		for cand in candidates:
			try:
				probe_url = _set_query_params(careers_url, {"from": "0", **cand})
				r = session.get(probe_url, timeout=30)
				r.raise_for_status()
				eager = _extract_eager_block(r.text)
				_jobs, _hits, total = _jobs_from_eager(eager)
				meta_hint["candidates"].append({"params": dict(cand), "totalHits": int(total or 0)})

				# Pick the smallest positive totalHits; 0 likely means "filter not supported".
				total_i = int(total or 0)
				if total_i <= 0:
					continue
				if best_total is None or total_i < best_total:
					best_total = total_i
					best_params = dict(cand)
			except Exception:
				meta_hint["candidates"].append({"params": dict(cand), "totalHits": None})
				continue

		meta_hint["picked"] = dict(best_params)
		return best_params, meta_hint

	def collect_raw(self, company: CompanyItem) -> CollectResult:
		limit_guess = 20
		max_pages = 200

		raw_jobs: List[Dict[str, Any]] = []
		meta: Dict[str, Any] = {
			"pages": 0,
			"offsets": [],
			"status_codes": [],
			"hits": [],
			"total_hits": None,
			"total_raw": 0,
		}

		try:
			session = requests.Session()
			session.headers.update(
				{
					"User-Agent": (
						"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
						"AppleWebKit/537.36 (KHTML, like Gecko) "
						"Chrome/120.0.0.0 Safari/537.36"
					),
					"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
					"Accept-Encoding": "gzip, deflate, br",
					"Accept-Language": "en-US,en;q=0.9",
					"Connection": "keep-alive",
				}
			)

			cfg = _config_for_company(company)
			effective_params, probe_meta = self._choose_effective_query_params(
				session=session,
				careers_url=company.careers_url,
				base_params=dict(cfg.extra_params),
			)
			meta["query_params"] = probe_meta

			offset = 0
			total_hits: Optional[int] = None

			for _ in range(max_pages):
				url = _set_query_params(company.careers_url, {"from": str(offset), **effective_params})
				r = session.get(url, timeout=30)
				meta["status_codes"].append(r.status_code)
				r.raise_for_status()

				eager = _extract_eager_block(r.text)
				jobs, hits, total = _jobs_from_eager(eager)

				if total_hits is None and total > 0:
					total_hits = total
					meta["total_hits"] = total_hits

				# Filter to Singapore (best-effort)
				jobs = [j for j in jobs if _is_singapore_job(j, company_name=company.company)]

				raw_jobs.extend(jobs)
				meta["pages"] += 1
				meta["offsets"].append(offset)
				meta["hits"].append(hits)

				if not hits or hits <= 0:
					break
				offset += hits if hits > 0 else limit_guess

				if total_hits is not None and offset >= total_hits:
					break

				# If server returns fewer jobs than hits (rare, but possible with filtering),
				# keep pagination driven by hits/total to avoid skipping.

			meta["total_raw"] = len(raw_jobs)
			return CollectResult(
				collector=self.name,
				company=company.company,
				careers_url=company.careers_url,
				raw_jobs=raw_jobs,
				meta=meta,
				error=None,
			)

		except Exception as e:
			meta["total_raw"] = len(raw_jobs)
			return CollectResult(
				collector=self.name,
				company=company.company,
				careers_url=company.careers_url,
				raw_jobs=raw_jobs,
				meta=meta,
				error=str(e),
			)

	def map_to_records(self, result: CollectResult) -> List[JobRecord]:
		records: List[JobRecord] = []
		for raw in result.raw_jobs:
			records.append(self._map_one(raw, result))
		return records

	def _map_one(self, raw: Dict[str, Any], result: CollectResult) -> JobRecord:
		title = _pick_first(raw, "title", "jobTitle")
		location = _pick_preferred_location(raw)

		job_id = _pick_first(raw, "jobId", "reqId") or _pick_first(raw, "jobSeqNo")
		posted_date = _pick_first(raw, "postedDate")
		job_url = _derive_job_url(result.careers_url, raw)

		company_val = _pick_first(raw, "jobCompany") or result.company

		return JobRecord(
			company=company_val,
			job_title=title,
			location=location,
			job_id=str(job_id),
			posted_date=posted_date,
			job_url=job_url,
			source=self.name,
			careers_url=result.careers_url,
			raw=raw,
		)

