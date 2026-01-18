from __future__ import annotations

import requests
from urllib.parse import urlparse
from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord
from typing import Any, Dict, List

API_URL = "https://jobs.saipem.com/positions_saipem.json"

class SaipemNcoreCollector(BaseCollector):
	name = "saipem_ncore"

	def collect_raw(self, company: CompanyItem) -> CollectResult:
		raw_jobs: List[Dict[str, Any]] = []
		meta: Dict[str, Any] = {"status": None}
		try:
			resp = requests.get(API_URL, headers={
				"Accept": "application/json, text/javascript, */*; q=0.01",
				"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
				"X-Requested-With": "XMLHttpRequest",
			})
			meta["status"] = resp.status_code
			resp.raise_for_status()
			data = resp.json()
			positions = data.get("data", {}).get("Positions", {})
			for company_name, joblist in positions.items():
				for item in joblist:
					location = item.get("location") or item.get("countryText") or ""
					if "singapore" not in str(location).lower():
						continue
					title = item.get("title") or item.get("jobTitle") or item.get("name")
					jid = str(item.get("id") or item.get("jobId") or item.get("reqId") or (item.get("slug") or "")).strip()
					url_from = item.get("applyUrl") or item.get("url") or item.get("href")
					if not url_from:
						continue
					job_url = url_from if str(url_from).startswith("http") else url_from
					if not jid:
						# fallback: try to parse from url
						try:
							parsed = urlparse(job_url)
							jid = parsed.path.rstrip("/\n ").split("/")[-1]
						except Exception:
							jid = job_url
					raw_jobs.append({
						"company": company_name,
						"job_title": title,
						"location": location,
						"job_id": jid,
						"posted_date": item.get("orderDate") or item.get("postedDate") or None,
						"job_url": job_url,
						"source": self.name,
						"careers_url": API_URL,
					})
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
		return [
			JobRecord(
				company=result.company,
				job_title=str(raw.get("job_title") or ""),
				location=str(raw.get("location") or ""),
				job_id=str(raw.get("job_id") or ""),
				posted_date=str(raw.get("posted_date") or ""),
				job_url=str(raw.get("job_url") or ""),
				source=self.name,
				careers_url=result.careers_url,
				raw=raw,
			)
			for raw in result.raw_jobs
			if isinstance(raw, dict)
		]
