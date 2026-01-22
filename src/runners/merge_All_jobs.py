from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.io.exporter import CSV_FIELDS


DEFAULT_INPUT_DIR = Path("data/output/ats_runs")
DEFAULT_GLOB = "*_jobs_batch3.csv"
DEFAULT_OUT = Path("data/output/all_jobs_batch3.xlsx")
DEFAULT_SHEET = "jobs"


def _read_one_csv(path: Path) -> pd.DataFrame:
    # Keep everything as strings; don't convert empties to NaN.
    import pandas as pd
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        print(f"Warnung: Leere Datei übersprungen: {path}")
        return None

    # Ensure schema is consistent even if older CSVs miss fields.
    for col in CSV_FIELDS:
        if col not in df.columns:
            df[col] = ""

    df = df[list(CSV_FIELDS)].copy()

    # Normalize whitespace (especially important for company/company overrides).
    for col in df.columns:
        df[col] = df[col].astype(str).fillna("").map(lambda s: s.strip())

    return df


def _iter_input_csvs(input_dir: Path, pattern: str) -> Iterable[Path]:
    # Deterministic order.
    return sorted([p for p in input_dir.glob(pattern) if p.is_file()])


def _dedupe(df: pd.DataFrame) -> pd.DataFrame:
    # Conservative dedupe key:
    # - Prefer company|job_id if job_id exists
    # - Else company|job_url if job_url exists
    # - Else company|job_title|location
    company = df["company"].astype(str).fillna("").str.strip()
    job_id = df["job_id"].astype(str).fillna("").str.strip()
    job_url = df["job_url"].astype(str).fillna("").str.strip()
    job_title = df["job_title"].astype(str).fillna("").str.strip()
    location = df["location"].astype(str).fillna("").str.strip()

    key = company + "|" + job_id
    key = key.where(job_id != "", company + "|" + job_url)
    key = key.where(job_url != "", company + "|" + job_title + "|" + location)

    out = df.copy()
    out["__dedupe_key"] = key
    out = out.drop_duplicates(subset=["__dedupe_key"], keep="first")
    out = out.drop(columns=["__dedupe_key"])
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Merge all *_jobs_batch2.csv into one XLSX workbook (data/output/all_jobs_batch2.xlsx by default)."
    )
    parser.add_argument(
        "--input-dir",
        default=str(DEFAULT_INPUT_DIR),
        help=f"Directory containing batch2 CSV outputs (default: {DEFAULT_INPUT_DIR}).",
    )
    parser.add_argument(
        "--pattern",
        default=DEFAULT_GLOB,
        help=f"Glob pattern for batch2 CSVs (default: {DEFAULT_GLOB}).",
    )
    parser.add_argument(
        "--out",
        default=str(DEFAULT_OUT),
        help=f"Output XLSX path (default: {DEFAULT_OUT}).",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Do not deduplicate rows while merging.",
    )
    parser.add_argument(
        "--sheet",
        default=DEFAULT_SHEET,
        help=f"Excel sheet name (default: {DEFAULT_SHEET}).",
    )

    args = parser.parse_args(argv)

    input_dir = Path(args.input_dir)
    out_path = Path(args.out)

    csv_paths = list(_iter_input_csvs(input_dir, args.pattern))
    if not csv_paths:
        raise RuntimeError(f"No CSV files found: {input_dir}/{args.pattern}")

    dfs: list[pd.DataFrame] = []
    for p in csv_paths:
        df = _read_one_csv(p)
        if df is not None and not df.empty:
            dfs.append(df)
        else:
            print(f"Datei übersprungen (leer oder None): {p}")

    merged = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=CSV_FIELDS)
    before = len(merged)

    if not args.no_dedupe and before:
        merged = _dedupe(merged)

    after = len(merged)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        merged.to_excel(writer, index=False, sheet_name=str(args.sheet))

    print(f"Merged CSV files: {len(csv_paths)}")
    print(f"Rows: {before} -> {after}")
    print(f"XLSX: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
