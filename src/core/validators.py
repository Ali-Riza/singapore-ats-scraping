from __future__ import annotations # For forward compatibility with future Python versions
from typing import List, Dict, Tuple
import re # re is used for regex matching (date validation)

from src.core.models import JobRecord # JobRecord dataclass

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$") # convert YYYY-MM-DD

def validate_records(records: List[JobRecord]) -> Dict[str, object]:
    """ Validate JobRecord fields and return statistics."""

    # Initialize statistics dictionary
    stats: Dict[str, object] = {}

    # Total records
    n = len(records)

    missing = {
        "company": 0,
        "job_title": 0,
        "job_id": 0,
        "job_url": 0,
        "posted_date": 0,
        "location": 0,
    }

    bad_date = 0 # count of records with bad date format
    seen: set[Tuple[str, str]] = set() # to track duplicates based on (company, job_id)
    dup = 0 # count of duplicate records

    # Check each record for missing fields and bad date formats
    for r in records:
        if not r.company: missing["company"] += 1
        if not r.job_title: missing["job_title"] += 1
        if not r.job_id: missing["job_id"] += 1
        if not r.job_url: missing["job_url"] += 1
        if not r.posted_date: missing["posted_date"] += 1
        if not r.location: missing["location"] += 1
        # Validate posted_date format
        if r.posted_date and not DATE_RE.fullmatch(r.posted_date):
            bad_date += 1
        
        if r.job_id:
            key = (r.company, r.job_id)
            if key in seen:
                dup += 1
            else:
                seen.add(key)

    return {
        "total": n,
        "missing": missing,
        "bad_date_format": bad_date,
        "duplicates_company_jobid": dup,
    }