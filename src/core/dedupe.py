from __future__ import annotations # For forward compatibility with future Python versions

from typing import List, Tuple, Set

from src.core.models import JobRecord


def dedupe_records(records: List[JobRecord]) -> List[JobRecord]:
    """ Deduplicate JobRecords based on (company, job_id)"""
    
    deduplicated_records: List[JobRecord] = []
    seen_keys: Set[Tuple[str, str]] = set()

    for r in records:
        if not r.job_id:
            deduplicated_records.append(r)
            continue

        key = (r.company, r.job_id)
        if key in seen_keys:
            continue

        seen_keys.add(key)
        deduplicated_records.append(r)

    return deduplicated_records