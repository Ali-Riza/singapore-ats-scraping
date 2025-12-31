from __future__ import annotations # For forward compatibility with future Python versions

from collections import Counter # For counting per-company jobs
from concurrent.futures import ThreadPoolExecutor # For parallel collection of companies

from src.io.loaders import load_companies # Load companies from Excel
from src.collectors.registry import pick_collector # Pick collector based on company item
from src.collectors.oracle import OracleCollector # Oracle collector
from src.collectors.eightfold import EightfoldCollector # Eightfold collector
from src.collectors.algolia import AlgoliaCollector # Algolia collector
from src.collectors.cornerstone import CornerstoneCollector # Cornerstone collector
from src.collectors.embeddedstate import EmbeddedStateCollector # Embedded State collector
from src.collectors.html_paged_search import HtmlPagedSearchCollector # HTML-Paged-Search (RSS + HTML fallback)
from src.collectors.phenom import PhenomCollector # Phenom collector
from src.collectors.successfactors import SuccessFactorsCollector # SuccessFactors collector
from src.collectors.tribepad import TribepadCollector # Tribepad collector
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

OUT_PHENOM_CSV = "data/output/phenom_jobs_batch2.csv"
OUT_PHENOM_REPORT = "data/output/phenom_report_batch2.json"

OUT_SUCCESSFACTORS_CSV = "data/output/successfactors_jobs_batch2.csv"
OUT_SUCCESSFACTORS_REPORT = "data/output/successfactors_report_batch2.json"

OUT_TRIBEPAD_CSV = "data/output/tribepad_jobs_batch2.csv"
OUT_TRIBEPAD_REPORT = "data/output/tribepad_report_batch2.json"

OUT_EIGHTFOLD_CSV = "data/output/eightfold_jobs_batch2.csv"
OUT_EIGHTFOLD_REPORT = "data/output/eightfold_report_batch2.json"

OUT_ALGOLIA_CSV = "data/output/algolia_jobs_batch2.csv"
OUT_ALGOLIA_REPORT = "data/output/algolia_report_batch2.json"

OUT_CORNERSTONE_CSV = "data/output/cornerstone_jobs_batch2.csv"
OUT_CORNERSTONE_REPORT = "data/output/cornerstone_report_batch2.json"

OUT_EMBEDDEDSTATE_CSV = "data/output/embeddedstate_jobs_batch2.csv"
OUT_EMBEDDEDSTATE_REPORT = "data/output/embeddedstate_report_batch2.json"

OUT_HTMLPAGEDSEARCH_CSV = "data/output/htmlpagedsearch_jobs_batch2.csv"
OUT_HTMLPAGEDSEARCH_REPORT = "data/output/htmlpagedsearch_report_batch2.json"


def main() -> None:
    """ Main function to run batch collection for Oracle and Workday ATS."""

    # 1) Load companies
    items = load_companies(MASTER_INPUT)

    # 2) Filter companies by ATS type
    oracle_items = [it for it in items if pick_collector(it) == "oracle"]
    workday_items = [it for it in items if pick_collector(it) == "workday"]
    phenom_items = [it for it in items if pick_collector(it) == "phenom"]
    successfactors_items = [it for it in items if pick_collector(it) == "successfactors"]
    tribepad_items = [it for it in items if pick_collector(it) == "tribepad"]
    eightfold_items = [it for it in items if pick_collector(it) == "eightfold"]
    algolia_items = [it for it in items if pick_collector(it) == "algolia"]
    cornerstone_items = [it for it in items if pick_collector(it) == "cornerstone"]
    embeddedstate_items = [it for it in items if pick_collector(it) == "embeddedstate"]
    htmlpagedsearch_items = [it for it in items if pick_collector(it) == "htmlpagedsearch"]

    # Print loaded and selected counts
    print(hr())
    print(f"Loaded total: {len(items)}")
    print(hr())
    print(f"Oracle selected: {len(oracle_items)}")
    print(f"Workday selected: {len(workday_items)}")
    print(f"Phenom selected: {len(phenom_items)}")
    print(f"SuccessFactors selected: {len(successfactors_items)}")
    print(f"Tribepad selected: {len(tribepad_items)}")
    print(f"Eightfold selected: {len(eightfold_items)}")
    print(f"Algolia selected: {len(algolia_items)}")
    print(f"Cornerstone selected: {len(cornerstone_items)}")
    print(f"EmbeddedState selected: {len(embeddedstate_items)}")
    print(f"HtmlPagedSearch selected: {len(htmlpagedsearch_items)}")
    print(hr())

  
    if oracle_items:
        print("Oracle companies:", [c.company for c in oracle_items])
        print(hr())
    else:
        print("No oracle companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())
    
    if workday_items:
        print("Workday companies:", [c.company for c in workday_items])
        print(hr())
    else:
        print("No workday companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if phenom_items:
        print("Phenom companies:", [c.company for c in phenom_items])
        print(hr())
    else:
        print("No phenom companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if successfactors_items:
        print("SuccessFactors companies:", [c.company for c in successfactors_items])
        print(hr())
    else:
        print("No successfactors companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if tribepad_items:
        print("Tribepad companies:", [c.company for c in tribepad_items])
        print(hr())
    else:
        print("No tribepad companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if eightfold_items:
        print("Eightfold companies:", [c.company for c in eightfold_items])
        print(hr())
    else:
        print("No eightfold companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if algolia_items:
        print("Algolia companies:", [c.company for c in algolia_items])
        print(hr())
    else:
        print("No algolia companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if cornerstone_items:
        print("Cornerstone companies:", [c.company for c in cornerstone_items])
        print(hr())
    else:
        print("No cornerstone companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if embeddedstate_items:
        print("EmbeddedState companies:", [c.company for c in embeddedstate_items])
        print(hr())
    else:
        print("No embeddedstate companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if htmlpagedsearch_items:
        print("HtmlPagedSearch companies:", [c.company for c in htmlpagedsearch_items])
        print(hr())
    else:
        print("No htmlpagedsearch companies selected. Check ATS_Type in Excel + loader mapping.")
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

    if successfactors_items:
        run_one_ats(
            ats_name="successfactors",
            companies=successfactors_items,
            collector=SuccessFactorsCollector(),
            items_total=len(items),
            out_csv=OUT_SUCCESSFACTORS_CSV,
            out_report=OUT_SUCCESSFACTORS_REPORT,
        )

    if tribepad_items:
        run_one_ats(
            ats_name="tribepad",
            companies=tribepad_items,
            collector=TribepadCollector(),
            items_total=len(items),
            out_csv=OUT_TRIBEPAD_CSV,
            out_report=OUT_TRIBEPAD_REPORT,
        )

    if eightfold_items:
        run_one_ats(
            ats_name="eightfold",
            companies=eightfold_items,
            collector=EightfoldCollector(),
            items_total=len(items),
            out_csv=OUT_EIGHTFOLD_CSV,
            out_report=OUT_EIGHTFOLD_REPORT,
        )

    if algolia_items:
        run_one_ats(
            ats_name="algolia",
            companies=algolia_items,
            collector=AlgoliaCollector(),
            items_total=len(items),
            out_csv=OUT_ALGOLIA_CSV,
            out_report=OUT_ALGOLIA_REPORT,
        )

    if cornerstone_items:
        run_one_ats(
            ats_name="cornerstone",
            companies=cornerstone_items,
            collector=CornerstoneCollector(),
            items_total=len(items),
            out_csv=OUT_CORNERSTONE_CSV,
            out_report=OUT_CORNERSTONE_REPORT,
        )

    if embeddedstate_items:
        run_one_ats(
            ats_name="embeddedstate",
            companies=embeddedstate_items,
            collector=EmbeddedStateCollector(),
            items_total=len(items),
            out_csv=OUT_EMBEDDEDSTATE_CSV,
            out_report=OUT_EMBEDDEDSTATE_REPORT,
        )

    if htmlpagedsearch_items:
        run_one_ats(
            ats_name="htmlpagedsearch",
            companies=htmlpagedsearch_items,
            collector=HtmlPagedSearchCollector(),
            items_total=len(items),
            out_csv=OUT_HTMLPAGEDSEARCH_CSV,
            out_report=OUT_HTMLPAGEDSEARCH_REPORT,
        )

    if (
        not oracle_items
        and not workday_items
        and not phenom_items
        and not successfactors_items
        and not tribepad_items
        and not eightfold_items
        and not algolia_items
        and not cornerstone_items
        and not embeddedstate_items
        and not htmlpagedsearch_items
    ):
        raise RuntimeError("No Oracle, Workday, Phenom, SuccessFactors, Tribepad, Eightfold, Algolia, Cornerstone, EmbeddedState, or HtmlPagedSearch companies found")

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
        
        # Submit collection tasks for each company (runs in background)
        collection_tasks = []
        for company in companies:
            # Submit collection task for this company in ThreadPool for collection + mapping
            task = executor.submit(_collect_and_map, company, collector)
            collection_tasks.append(task)
        
        # After mapping is done, normalize and aggregate results
        for task in collection_tasks:
            try:
                mapped = task.result()
                if mapped:
                    normalized = normalize_records(mapped)
                    normalized_job_records.extend(normalized)
            except Exception as e:
                print(f"Warning: collection failed: {e}")
                continue

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