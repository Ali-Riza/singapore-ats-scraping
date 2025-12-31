from __future__ import annotations # For forward compatibility with future Python versions

from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


def _mojibake_score(s: str) -> int:
    suspects = ("√", "Ã", "Â", "�")
    return sum(s.count(ch) for ch in suspects)


def _fix_mojibake(s: str) -> str:
    """Best-effort fix for common mis-decoded UTF-8 strings.

    Example: "W√§rtsil√§" -> "Wärtsilä".
    """
    if not s:
        return s

    base = s
    base_score = _mojibake_score(base)
    if base_score == 0:
        return base

    best = base
    best_score = base_score

    for wrong_enc in ("mac_roman", "latin-1", "cp1252"):
        try:
            b = base.encode(wrong_enc)
            cand = b.decode("utf-8")
        except Exception:
            continue

        score = _mojibake_score(cand)
        if score < best_score:
            best = cand
            best_score = score

    return best

from src.core.models import CompanyItem

# Column names used in the input CSV file master_companies.csv
COL_COMPANY = "Company"
COL_JOBS_SG = "Jobs Page (Singapore)"
COL_ATS = "ats_new_norm"
COL_CATEGORY = "Category"
COL_WEBSITE = "Website"

REQUIRED_COLS = [COL_COMPANY, COL_JOBS_SG]

def _clean_str(x: Any) -> str | None:
    """Cleans a string by stripping whitespace and converting empty or 'nan' strings to None."""
    if x is None:
        return None
    s = _fix_mojibake(str(x)).strip()
    if s == "" or s.lower() == "nan":
        return None
    return s

def load_companies(path: str) -> List[CompanyItem]:
    """
    Load companies from the master Excel file.
    Returns only valid CompanyItem entries (rows missing required fields are skipped).
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")

    df = pd.read_excel(p)

    # Validate required columns
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}. Found columns: {list(df.columns)}"
        )
    # Process each row and create CompanyItem instances
    items: List[CompanyItem] = []

    for idx, row in df.iterrows():
        row_number = int(idx) + 2  # skip header row
        company = _clean_str(row.get(COL_COMPANY))
        jobs_url = _clean_str(row.get(COL_JOBS_SG))

        # Validate required fields
        if not company or not jobs_url:
            continue
        
        # Optional fields
        ats_type = _clean_str(row.get(COL_ATS))
        category = _clean_str(row.get(COL_CATEGORY))
        website = _clean_str(row.get(COL_WEBSITE))

        item = CompanyItem(
            raw_data_row=row.to_dict(),
            company=company,
            careers_url=jobs_url,
            ats_type=ats_type,
            category=category,
            website=website,
            row_number=row_number,
        )
        items.append(item)

    return items

    