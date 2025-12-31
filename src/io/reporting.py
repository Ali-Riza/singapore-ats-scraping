from __future__ import annotations # For forward compatibility with future Python versions

import json # For JSON export
from datetime import datetime # For timestamping the report
from pathlib import Path    # For file path manipulations
from typing import Any, Dict, List

from src.core.models import JobRecord


def build_report(
    *,
    records_before_dedupe: List[JobRecord],
    records_after_dedupe: List[JobRecord],
    validation_stats: Dict[str, Any],
    per_company_counts: Dict[str, int],
    input_total_companies: int,
    selected_companies: int,
    ats_name: str,
) -> Dict[str, Any]:
    """ Build a report dictionary summarizing the scraping session."""
    return {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "ats": ats_name,
        "companies": {
            "total_loaded": input_total_companies,
            "selected": selected_companies,
            "per_company_job_counts_after_dedupe": per_company_counts,
        },
        "records": {
            "before_dedupe": len(records_before_dedupe),
            "after_dedupe": len(records_after_dedupe),
        },
        "validation": validation_stats,
    }


def export_report_json(report: Dict[str, Any], out_path: str) -> None:
    """ Export the report dictionary as a JSON file at out_path."""
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)