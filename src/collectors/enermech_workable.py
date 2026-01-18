from __future__ import annotations

import requests
from src.collectors.base import BaseCollector
from src.core.models import CompanyItem, CollectResult, JobRecord
from typing import Any, Dict, List

API_URL = "https://apply.workable.com/api/v3/accounts/enermech/jobs"

class EnermechWorkableCollector(BaseCollector):
	name = "enermech_workable"

	def collect_raw(self, company: CompanyItem) -> CollectResult:
		raw_jobs: List[Dict[str, Any]] = []
		meta: Dict[str, Any] = {"status": None}
		try:
			headers = {
				"accept": "application/json, text/plain, */*",
				"accept-language": "en",
				"content-type": "application/json",
				"origin": "https://apply.workable.com",
				"referer": "https://apply.workable.com/enermech/",
				"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
			}
			jobs = []
			token = None
			while True:
				data = {"query": "", "department": [], "location": [], "workplace": [], "worktype": []}
				if token:
					data["token"] = token
				resp = requests.post(API_URL, headers=headers, json=data)
				meta["status"] = resp.status_code
				resp.raise_for_status()
				result = resp.json()
				jobs.extend(result.get("results", []))
				token = result.get("nextPageToken") or result.get("nextPage")
				if not token:
					break
			raw_jobs = jobs
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
		def is_singapore_job(job):
			raw_location = job.get('location')
			if isinstance(raw_location, str):
				location = raw_location.lower()
			elif raw_location is not None:
				location = str(raw_location).lower()
			else:
				location = ''
			title = (job.get('title') or '').lower()
			return (
				'singapore' in location or
				'sg' == location or
				'singapore' in title or
				'sg' == title
			)
		return [
			JobRecord(
				company=result.company,
				job_title=str(raw.get("title") or ""),
				location=str(raw.get("location") or ""),
				job_id=str(raw.get("id") or ""),
				posted_date=str(raw.get("createdAt") or ""),
				job_url=str(raw.get("url") or ""),
				source=self.name,
				careers_url=result.careers_url,
				raw=raw,
			)
			for raw in result.raw_jobs
			if isinstance(raw, dict) and is_singapore_job(raw)
		]
