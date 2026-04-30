from __future__ import annotations # For forward compatibility with future Python versions

from typing import List, Tuple, Set

from src.core.models import JobRecord


def dedupe_records(records: List[JobRecord]) -> List[JobRecord]:
    """Deduplicate JobRecords.

    Prefer a stable key based on (company, job_title). Some sources generate
    unstable job_id values across runs, which would prevent effective dedupe.
    """
    
    def _norm(s: str) -> str:
        return " ".join((s or "").split()).strip().casefold()

    deduplicated_records: List[JobRecord] = []
    seen_keys: Set[Tuple[str, str]] = set()

    for r in records:
        company = _norm(getattr(r, "company", "") or "")
        title = _norm(getattr(r, "job_title", "") or "")

        # Strict dedupe key: company + title.
        # If either is missing, do not attempt to dedupe.
        if not company or not title:
            deduplicated_records.append(r)
            continue

        key = (company, title)

        if key in seen_keys:
            continue

        seen_keys.add(key)
        deduplicated_records.append(r)

    return deduplicated_records