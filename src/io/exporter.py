from __future__ import annotations # For forward compatibility with future Python versions

import csv  # For CSV export
from dataclasses import asdict # asdict to convert dataclass to dict
from pathlib import Path
from typing import Iterable, List

from src.core.models import JobRecord


CSV_FIELDS: List[str] = [
    "company",
    "job_title",
    "location",
    "job_id",
    "posted_date",
    "job_url",
    "source",
    "careers_url",
]


def export_records_csv(records: Iterable[JobRecord], out_path: str) -> None:
    """ Export JobRecords to a CSV file at out_path."""
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Use UTF-8 with BOM to help Excel (especially on macOS) detect encoding correctly.
    # This avoids mojibake like "W√§rtsil√§" when opening the CSV directly in Excel.
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in records:
            d = asdict(r)
            row = {k: d.get(k, "") for k in CSV_FIELDS}
            w.writerow(row)