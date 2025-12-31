from __future__ import annotations # For forward compatibility with future Python versions

from collections import Counter # For counting per-company jobs
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait # For parallel collection of companies

from src.io.loaders import load_companies # Load companies from Excel
from src.collectors.registry import pick_collector # Pick collector based on company item
from src.collectors.oracle import OracleCollector # Oracle collector
from src.collectors.phenom import PhenomCollector # Phenom collector
from src.collectors.workday import WorkdayCollector # Workday collector

from src.core.normalize import normalize_records # Normalize JobRecord fields
from src.core.validators import validate_records # Validate JobRecord fields
from src.core.dedupe import dedupe_records # Dedupe JobRecord list

from src.io.exporter import export_records_csv # Export JobRecord list to CSV
from src.io.reporting import build_report, export_report_json # Build and export report

from src.utils.cli import hr


# Constants for input/output paths
MASTER_INPUT = "data/input/master_companies_with_fingerprint.xlsx"

OUT_ORACLE_CSV = "data/output/oracle_jobs_batch1.csv"
OUT_ORACLE_REPORT = "data/output/oracle_report_batch1.json"

OUT_WORKDAY_CSV = "data/output/workday_jobs_batch1.csv"
OUT_WORKDAY_REPORT = "data/output/workday_report_batch1.json"

OUT_PHENOM_CSV = "data/output/phenom_jobs_batch1.csv"
OUT_PHENOM_REPORT = "data/output/phenom_report_batch1.json"


def main() -> None:
    """ Main function to run batch collection for Oracle and Workday ATS."""

    # 1) Load companies
    items = load_companies(MASTER_INPUT)

    # 2) Filter companies by ATS type
    oracle_items = [it for it in items if pick_collector(it) == "oracle"]
    workday_items = [it for it in items if pick_collector(it) == "workday"]
    phenom_items = [it for it in items if pick_collector(it) == "phenom"]

    # Print loaded and selected counts
    print(f"Loaded total: {len(items)}")
    print(f"Oracle selected: {len(oracle_items)}")
    print(f"Workday selected: {len(workday_items)}")
    print(f"Phenom selected: {len(phenom_items)}")

    # Show which Oracle companies will be processed (or provide a helpful hint)
    if oracle_items:
        print("Oracle companies:", [c.company for c in oracle_items])
    else:
        print("No oracle companies selected. Check ATS_Type in Excel + loader mapping.")
    
    if workday_items:
        print("Workday companies:", [c.company for c in workday_items])
    else:
        print("No workday companies selected. Check ATS_Type in Excel + loader mapping.")

    if phenom_items:
        print("Phenom companies:", [c.company for c in phenom_items])
    else:
        print("No phenom companies selected. Check ATS_Type in Excel + loader mapping.")

    print(hr())

    if oracle_items:
        run_one_ats(
            ats_name="oracle",
            companies=oracle_items,
            collector=OracleCollector(),
            items_total=len(items),
            out_csv=OUT_ORACLE_CSV,
            out_report=OUT_ORACLE_REPORT,
        )

    if workday_items:
        run_one_ats(
            ats_name="workday",
            companies=workday_items,
            collector=WorkdayCollector(),
            items_total=len(items),
            out_csv=OUT_WORKDAY_CSV,
            out_report=OUT_WORKDAY_REPORT,
        )

    if phenom_items:
        run_one_ats(
            ats_name="phenom",
            companies=phenom_items,
            collector=PhenomCollector(),
            items_total=len(items),
            out_csv=OUT_PHENOM_CSV,
            out_report=OUT_PHENOM_REPORT,
        )

    if not oracle_items and not workday_items and not phenom_items:
        raise RuntimeError("No Oracle, Workday, or Phenom companies found")

def run_one_ats(
    *,
    ats_name: str,
    companies: list,
    collector,
    items_total: int,
    out_csv: str,
    out_report: str,
) -> None:
    """ Run collection, mapping, normalization, validation, dedupe, export for one ATS."""
    
    # Collect and normalize job records from all companies
    normalized_job_records = []
    
    # Use ThreadPoolExecutor for parallel HTTP requests
    with ThreadPoolExecutor(max_workers=20) as executor:

        def _collect_one(c):
            print(f"{ats_name}: start {c.company}")
            return _collect_and_map(c, collector)

        future_to_company = {executor.submit(_collect_one, c): c.company for c in companies}
        pending = set(future_to_company.keys())
        total = len(pending)
        done_n = 0

        while pending:
            done, pending = wait(pending, timeout=15, return_when=FIRST_COMPLETED)

            for fut in done:
                done_n += 1
                company_name = future_to_company.get(fut, "<unknown>")
                try:
                    mapped = fut.result()
                    if mapped:
                        normalized_job_records.extend(normalize_records(mapped))
                    print(f"{ats_name}: done {company_name} ({done_n}/{total})")
                except Exception as e:
                    print(f"Warning: {ats_name} failed for {company_name}: {e}")

            if pending:
                names = [future_to_company.get(f, "<unknown>") for f in pending]
                names.sort()
                print(f"{ats_name}: waiting on {len(pending)}/{total} companies: {names}")

    # 3) Validate
    validation_stats = validate_records(normalized_job_records)

    # 4) Dedupe
    records_after_dedupe = dedupe_records(normalized_job_records)

    # 5) Per-company counts (after dedupe)
    per_company_counts = dict(Counter(r.company for r in records_after_dedupe))

    # 6) Export CSV
    export_records_csv(records_after_dedupe, out_csv)

    # 7) Build + export report
    report = build_report(
        records_before_dedupe=normalized_job_records,
        records_after_dedupe=records_after_dedupe,
        validation_stats=validation_stats,
        per_company_counts=per_company_counts,
        input_total_companies=items_total,
        selected_companies=len(companies),
        ats_name=ats_name,
    )
    export_report_json(report, out_report)

    print("DONE:", ats_name)
    print(f"CSV:    {out_csv}")
    print(f"REPORT: {out_report}")
    print(f"Records after dedupe: {len(records_after_dedupe)}")
    print(hr())


def _collect_and_map(company, collector):
    """ Helper to collect and map for one company."""
    res = collector.collect_raw(company)
    if res.error:
        return None
    return collector.map_to_records(res)

if __name__ == "__main__":
    main()