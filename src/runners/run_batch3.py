from __future__ import annotations # For forward compatibility with future Python versions
import csv
import subprocess
from typing import Dict, Tuple, Set
def get_job_id(record: dict) -> str:
    # Versuche, eine stabile Job-ID zu nehmen, sonst Fallback auf Company+Title+Location+URL
    for key in ("job_id", "id", "JobID", "JobId", "Job_ID"):  # mögliche Varianten
        if key in record and record[key]:
            return str(record[key]).strip()
    # Fallback: Company, Title, Location, URL
    return "|".join([
        str(record.get("company", "")).strip(),
        str(record.get("title", "")).strip(),
        str(record.get("location", "")).strip(),
        str(record.get("url", "")).strip(),
    ])

def read_jobs_csv(path: str) -> Dict[str, dict]:
    jobs = {}
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                jobid = get_job_id(row)
                jobs[jobid] = row
    except FileNotFoundError:
        pass
    return jobs

def compare_job_status(previous_csv: str, current_csv: str) -> Dict[str, Tuple[str, dict]]:
    """
    Vergleicht previous.csv und current.csv und gibt ein Dict mit Job-ID -> (Status, Datensatz) zurück.
    Status: 'New', 'Closed', 'Open'
    """
    prev_jobs = read_jobs_csv(previous_csv)
    curr_jobs = read_jobs_csv(current_csv)
    prev_ids = set(prev_jobs.keys())
    curr_ids = set(curr_jobs.keys())

    status_dict = {}
    for jobid in curr_ids - prev_ids:
        status_dict[jobid] = ("New", curr_jobs[jobid])
    for jobid in prev_ids - curr_ids:
        status_dict[jobid] = ("Closed", prev_jobs[jobid])
    for jobid in prev_ids & curr_ids:
        status_dict[jobid] = ("Open", curr_jobs[jobid])
    return status_dict

def export_status_csv(status_dict: Dict[str, Tuple[str, dict]], out_path: str):
    if not status_dict:
        return
    # Nimm alle Felder aus einem beliebigen Datensatz plus 'status'
    sample = next(iter(status_dict.values()))[1]
    fieldnames = list(sample.keys()) + ["status"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for status, row in status_dict.values():
            row_out = dict(row)
            row_out["status"] = status
            writer.writerow(row_out)


import argparse
import os
from collections import Counter # For counting per-company jobs
from concurrent.futures import ThreadPoolExecutor # For parallel collection of companies
from dataclasses import dataclass, replace

from src.collectors import enermech_workable
from src.collectors import saipem_ncore
from src.io.loaders import load_companies # Load companies from Excel
from src.collectors.registry import pick_collector # Pick collector based on company item
from src.collectors.oracle import OracleCollector # Oracle collector
from src.collectors.eightfold import EightfoldCollector # Eightfold collector
from src.collectors.algolia import AlgoliaCollector # Algolia collector
from src.collectors.cornerstone import CornerstoneCollector # Cornerstone collector
from src.collectors.embeddedstate import EmbeddedStateCollector # Embedded State collector
from src.collectors.jibe_api_jobs import JibeApiJobsCollector # Jibe (/api/jobs) collector
from src.collectors.html_paged_search import HtmlPagedSearchCollector # HTML-Paged-Search (RSS + HTML fallback)
from src.collectors.phenom import PhenomCollector # Phenom collector
from src.collectors.successfactors import SuccessFactorsCollector # SuccessFactors collector
from src.collectors.tribepad import TribepadCollector # Tribepad collector
from src.collectors.workday import WorkdayCollector # Workday collector

# Batch2 expansion collectors
from src.collectors.hibob import HibobCollector
from src.collectors.jobsyn_solr import JobsynSolrCollector
from src.collectors.avature import AvatureCollector
from src.collectors.breezy_portal import BreezyPortalCollector
from src.collectors.umbraco_api import UmbracoApiCollector
from src.collectors.mycareersfuture import MyCareersFutureCollector
from src.collectors.tuvsud_recruiting_api import TuvSudRecruitingApiCollector
from src.collectors.milchundzucker_gjb import MilchUndZuckerGjbCollector
from src.collectors.clinch_careers_site import ClinchCareersSiteCollector
from src.collectors.kentico_html import KenticoHtmlCollector
from src.collectors.wordpress_inline_modals import WordpressInlineModalsCollector
from src.collectors.wordpress_elementor import WordpressElementorCollector
from src.collectors.wordpress_remix import WordpressRemixCollector
from src.collectors.magnolia_nextjs import MagnoliaNextJsCollector
from src.collectors.krohne_nextjs import KrohneNextJsCollector
from src.collectors.kongsberg_optimizely_easycruit import KongsbergOptimizelyEasycruitCollector
from src.collectors.lr_episerver_api import LrEpiserverApiCollector
from src.collectors.aem_workday_json import AemWorkdayJsonCollector
from src.collectors.carrier_html import CarrierHtmlCollector
from src.collectors.classnk_static_html import ClassNkStaticHtmlCollector
from src.collectors.aibel_html_hr_manager import AibelHtmlHrManagerCollector
from src.collectors.sitefinity import SitefinityCollector
# Add EnerMech and Saipem collectors
from src.collectors.enermech_workable import EnermechWorkableCollector
from src.collectors.saipem_ncore import SaipemNcoreCollector

from src.core.normalize import normalize_records # Normalize JobRecord fields
from src.core.validators import validate_records # Validate JobRecord fields
from src.core.dedupe import dedupe_records # Dedupe JobRecord list

from src.io.exporter import export_records_csv # Export JobRecord list to CSV
from src.io.reporting import build_report, export_report_json # Build and export report

from src.utils.cli import hr


# Constants for input/output paths

MASTER_INPUT = "data/input/master_companies_with_fingerprint.xlsx"


_ATS_OUTDIR = "data/output/ats_runs/"

OUT_ORACLE_CSV = _ATS_OUTDIR + "oracle_jobs_batch3.csv"
OUT_ORACLE_REPORT = _ATS_OUTDIR + "oracle_report_batch3.json"

OUT_WORKDAY_CSV = _ATS_OUTDIR + "workday_jobs_batch3.csv"
OUT_WORKDAY_REPORT = _ATS_OUTDIR + "workday_report_batch3.json"

OUT_PHENOM_CSV = _ATS_OUTDIR + "phenom_jobs_batch3.csv"
OUT_PHENOM_REPORT = _ATS_OUTDIR + "phenom_report_batch3.json"

OUT_SUCCESSFACTORS_CSV = _ATS_OUTDIR + "successfactors_jobs_batch3.csv"
OUT_SUCCESSFACTORS_REPORT = _ATS_OUTDIR + "successfactors_report_batch3.json"

OUT_TRIBEPAD_CSV = _ATS_OUTDIR + "tribepad_jobs_batch3.csv"
OUT_TRIBEPAD_REPORT = _ATS_OUTDIR + "tribepad_report_batch3.json"

OUT_EIGHTFOLD_CSV = _ATS_OUTDIR + "eightfold_jobs_batch3.csv"
OUT_EIGHTFOLD_REPORT = _ATS_OUTDIR + "eightfold_report_batch3.json"

OUT_ALGOLIA_CSV = _ATS_OUTDIR + "algolia_jobs_batch3.csv"
OUT_ALGOLIA_REPORT = _ATS_OUTDIR + "algolia_report_batch3.json"

OUT_CORNERSTONE_CSV = _ATS_OUTDIR + "cornerstone_jobs_batch3.csv"
OUT_CORNERSTONE_REPORT = _ATS_OUTDIR + "cornerstone_report_batch3.json"

OUT_EMBEDDEDSTATE_CSV = _ATS_OUTDIR + "embeddedstate_jobs_batch3.csv"
OUT_EMBEDDEDSTATE_REPORT = _ATS_OUTDIR + "embeddedstate_report_batch3.json"

OUT_HTMLPAGEDSEARCH_CSV = _ATS_OUTDIR + "htmlpagedsearch_jobs_batch3.csv"
OUT_HTMLPAGEDSEARCH_REPORT = _ATS_OUTDIR + "htmlpagedsearch_report_batch3.json"

OUT_JIBE_API_JOBS_CSV = _ATS_OUTDIR + "jibe_api_jobs_batch3.csv"
OUT_JIBE_API_JOBS_REPORT = _ATS_OUTDIR + "jibe_api_jobs_report_batch3.json"

OUT_HIBOB_CSV = _ATS_OUTDIR + "hibob_jobs_batch3.csv"
OUT_HIBOB_REPORT = _ATS_OUTDIR + "hibob_report_batch3.json"

OUT_JOBSYNC_SOLR_CSV = _ATS_OUTDIR + "jobsyn_solr_jobs_batch3.csv"
OUT_JOBSYNC_SOLR_REPORT = _ATS_OUTDIR + "jobsyn_solr_report_batch3.json"

OUT_AVATURE_CSV = _ATS_OUTDIR + "avature_jobs_batch3.csv"
OUT_AVATURE_REPORT = _ATS_OUTDIR + "avature_report_batch3.json"

OUT_BREEZY_PORTAL_CSV = _ATS_OUTDIR + "breezy_portal_jobs_batch3.csv"
OUT_BREEZY_PORTAL_REPORT = _ATS_OUTDIR + "breezy_portal_report_batch3.json"

OUT_UMBRACO_API_CSV = _ATS_OUTDIR + "umbraco_api_jobs_batch3.csv"
OUT_UMBRACO_API_REPORT = _ATS_OUTDIR + "umbraco_api_report_batch3.json"

OUT_MYCAREERSFUTURE_CSV = _ATS_OUTDIR + "mycareersfuture_jobs_batch3.csv"
OUT_MYCAREERSFUTURE_REPORT = _ATS_OUTDIR + "mycareersfuture_report_batch3.json"

OUT_TUVSUD_RECRUITING_API_CSV = _ATS_OUTDIR + "tuvsud_recruiting_api_jobs_batch3.csv"
OUT_TUVSUD_RECRUITING_API_REPORT = _ATS_OUTDIR + "tuvsud_recruiting_api_report_batch3.json"

OUT_MILCHUNDZUCKER_GJB_CSV = _ATS_OUTDIR + "milchundzucker_gjb_jobs_batch3.csv"
OUT_MILCHUNDZUCKER_GJB_REPORT = _ATS_OUTDIR + "milchundzucker_gjb_report_batch3.json"

OUT_CLINCH_CAREERS_SITE_CSV = _ATS_OUTDIR + "clinch_careers_site_jobs_batch3.csv"
OUT_CLINCH_CAREERS_SITE_REPORT = _ATS_OUTDIR + "clinch_careers_site_report_batch3.json"

OUT_KENTICO_HTML_CSV = _ATS_OUTDIR + "kentico_html_jobs_batch3.csv"
OUT_KENTICO_HTML_REPORT = _ATS_OUTDIR + "kentico_html_report_batch3.json"

OUT_WORDPRESS_INLINE_MODALS_CSV = _ATS_OUTDIR + "wordpress_inline_modals_jobs_batch3.csv"
OUT_WORDPRESS_INLINE_MODALS_REPORT = _ATS_OUTDIR + "wordpress_inline_modals_report_batch3.json"

OUT_WORDPRESS_ELEMENTOR_CSV = _ATS_OUTDIR + "wordpress_elementor_jobs_batch3.csv"
OUT_WORDPRESS_ELEMENTOR_REPORT = _ATS_OUTDIR + "wordpress_elementor_report_batch3.json"

OUT_WORDPRESS_REMIX_CSV = _ATS_OUTDIR + "wordpress_remix_jobs_batch3.csv"
OUT_WORDPRESS_REMIX_REPORT = _ATS_OUTDIR + "wordpress_remix_report_batch3.json"

OUT_MAGNOLIA_NEXTJS_CSV = _ATS_OUTDIR + "magnolia_nextjs_jobs_batch3.csv"
OUT_MAGNOLIA_NEXTJS_REPORT = _ATS_OUTDIR + "magnolia_nextjs_report_batch3.json"

OUT_KROHNE_NEXTJS_CSV = _ATS_OUTDIR + "krohne_nextjs_jobs_batch3.csv"
OUT_KROHNE_NEXTJS_REPORT = _ATS_OUTDIR + "krohne_nextjs_report_batch3.json"

OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_CSV = _ATS_OUTDIR + "kongsberg_optimizely_easycruit_jobs_batch3.csv"
OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_REPORT = _ATS_OUTDIR + "kongsberg_optimizely_easycruit_report_batch3.json"

OUT_LR_EPISERVER_API_CSV = _ATS_OUTDIR + "lr_episerver_api_jobs_batch3.csv"
OUT_LR_EPISERVER_API_REPORT = _ATS_OUTDIR + "lr_episerver_api_report_batch3.json"

OUT_AEM_WORKDAY_JSON_CSV = _ATS_OUTDIR + "aem_workday_json_jobs_batch3.csv"
OUT_AEM_WORKDAY_JSON_REPORT = _ATS_OUTDIR + "aem_workday_json_report_batch3.json"

OUT_CARRIER_HTML_CSV = _ATS_OUTDIR + "carrier_html_jobs_batch3.csv"
OUT_CARRIER_HTML_REPORT = _ATS_OUTDIR + "carrier_html_report_batch3.json"

OUT_CLASSNK_STATIC_HTML_CSV = _ATS_OUTDIR + "classnk_static_html_jobs_batch3.csv"
OUT_CLASSNK_STATIC_HTML_REPORT = _ATS_OUTDIR + "classnk_static_html_report_batch3.json"

OUT_AIBEL_HTML_HR_MANAGER_CSV = _ATS_OUTDIR + "aibel_html_hr_manager_jobs_batch3.csv"
OUT_AIBEL_HTML_HR_MANAGER_REPORT = _ATS_OUTDIR + "aibel_html_hr_manager_report_batch3.json"

OUT_SITEFINITY_CSV = _ATS_OUTDIR + "sitefinity_jobs_batch3.csv"
OUT_SITEFINITY_REPORT = _ATS_OUTDIR + "sitefinity_report_batch3.json"

# Run these ATS groups first (so you can validate new collectors quickly).
# You can override via CLI: `--priority ats1,ats2`.
DEFAULT_PRIORITY_ATS = [
    "clinch_careers_site"
]


@dataclass(frozen=True)
class AtsGroup:
    ats_name: str
    companies: list
    collector: object
    out_csv: str
    out_report: str


def _parse_csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [p.strip() for p in value.split(",") if p.strip()]


def _priority_order_key(ats_name: str, priority: list[str]) -> tuple[int, int]:
    try:
        return (0, priority.index(ats_name))
    except ValueError:
        return (1, 10_000)


def _build_groups(
    *,
    items: list,
) -> list[AtsGroup]:
    """Build ATS groups (items + collector + output paths) in a stable default order."""

    oracle_items = [it for it in items if pick_collector(it) == "oracle"]
    workday_items = [it for it in items if pick_collector(it) == "workday"]
    phenom_items = [it for it in items if pick_collector(it) == "phenom"]
    successfactors_items = [it for it in items if pick_collector(it) == "successfactors"]
    tribepad_items = [it for it in items if pick_collector(it) == "tribepad"]
    eightfold_items = [it for it in items if pick_collector(it) == "eightfold"]
    algolia_items = [it for it in items if pick_collector(it) == "algolia"]
    cornerstone_items = [it for it in items if pick_collector(it) == "cornerstone"]
    embeddedstate_items = [it for it in items if pick_collector(it) == "embeddedstate"]
    jibe_api_jobs_items = [it for it in items if pick_collector(it) == "jibe_api_jobs"]
    htmlpagedsearch_items = [it for it in items if pick_collector(it) == "htmlpagedsearch"]

    hibob_items = [it for it in items if pick_collector(it) == "hibob"]
    jobsyn_solr_items = [it for it in items if pick_collector(it) == "jobsyn_solr"]
    avature_items = [it for it in items if pick_collector(it) == "avature"]
    breezy_portal_items = [it for it in items if pick_collector(it) == "breezy_portal"]
    umbraco_api_items = [it for it in items if pick_collector(it) == "umbraco_api"]
    mycareersfuture_items = [it for it in items if pick_collector(it) == "mycareersfuture"]
    tuvsud_recruiting_api_items = [it for it in items if pick_collector(it) == "tuvsud_recruiting_api"]
    milchundzucker_gjb_items = [it for it in items if pick_collector(it) == "milchundzucker_gjb"]
    clinch_careers_site_items = [it for it in items if pick_collector(it) == "clinch_careers_site"]
    kentico_html_items = [it for it in items if pick_collector(it) == "kentico_html"]
    wordpress_inline_modals_items = [it for it in items if pick_collector(it) == "wordpress_inline_modals"]
    wordpress_elementor_items = [it for it in items if pick_collector(it) == "wordpress_elementor"]
    wordpress_remix_items = [it for it in items if pick_collector(it) == "wordpress_remix"]
    magnolia_nextjs_items = [it for it in items if pick_collector(it) == "magnolia_nextjs"]
    krohne_nextjs_items = [it for it in items if pick_collector(it) == "krohne_nextjs"]
    kongsberg_optimizely_easycruit_items = [it for it in items if pick_collector(it) == "kongsberg_optimizely_easycruit"]
    lr_episerver_api_items = [it for it in items if pick_collector(it) == "lr_episerver_api"]
    aem_workday_json_items = [it for it in items if pick_collector(it) == "aem_workday_json"]
    carrier_html_items = [it for it in items if pick_collector(it) == "carrier_html"]
    classnk_static_html_items = [it for it in items if pick_collector(it) == "classnk_static_html"]
    aibel_html_hr_manager_items = [it for it in items if pick_collector(it) == "aibel_html_hr_manager"]
    sitefinity_items = [it for it in items if pick_collector(it) == "sitefinity"]
    enermech_workable_items = [it for it in items if pick_collector(it) == "enermech_workable"]
    saipem_ncore_items = [it for it in items if pick_collector(it) == "saipem_ncore"]

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
    print(f"JibeApiJobs selected: {len(jibe_api_jobs_items)}")
    print(f"HtmlPagedSearch selected: {len(htmlpagedsearch_items)}")
    print(f"HiBob selected: {len(hibob_items)}")
    print(f"JobsynSolr selected: {len(jobsyn_solr_items)}")
    print(f"Avature selected: {len(avature_items)}")
    print(f"BreezyPortal selected: {len(breezy_portal_items)}")
    print(f"UmbracoApi selected: {len(umbraco_api_items)}")
    print(f"MyCareersFuture selected: {len(mycareersfuture_items)}")
    print(f"TuvSudRecruitingApi selected: {len(tuvsud_recruiting_api_items)}")
    print(f"MilchUndZuckerGjb selected: {len(milchundzucker_gjb_items)}")
    print(f"ClinchCareersSite selected: {len(clinch_careers_site_items)}")
    print(f"KenticoHtml selected: {len(kentico_html_items)}")
    print(f"WordpressInlineModals selected: {len(wordpress_inline_modals_items)}")
    print(f"WordpressElementor selected: {len(wordpress_elementor_items)}")
    print(f"WordpressRemix selected: {len(wordpress_remix_items)}")
    print(f"MagnoliaNextJs selected: {len(magnolia_nextjs_items)}")
    print(f"KrohneNextJs selected: {len(krohne_nextjs_items)}")
    print(f"KongsbergOptimizelyEasycruit selected: {len(kongsberg_optimizely_easycruit_items)}")
    print(f"LrEpiserverApi selected: {len(lr_episerver_api_items)}")
    print(f"AemWorkdayJson selected: {len(aem_workday_json_items)}")
    print(f"CarrierHtml selected: {len(carrier_html_items)}")
    print(f"ClassNkStaticHtml selected: {len(classnk_static_html_items)}")
    print(f"AibelHtmlHrManager selected: {len(aibel_html_hr_manager_items)}")
    print(f"Sitefinity selected: {len(sitefinity_items)}")
    print(f"EnermechWorkable selected: {len(enermech_workable_items)}")
    print(f"SaipemNcore selected: {len(saipem_ncore_items)}")
    print(hr())

    if enermech_workable_items:
        print("EnermechWorkable companies:", [c.company for c in enermech_workable_items])
        print(hr())
    else:
        print("No EnermechWorkable companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if saipem_ncore_items:
        print("SaipemNcore companies:", [c.company for c in saipem_ncore_items])
        print(hr())
    else:
        print("No SaipemNcore companies selected. Check ATS_Type in Excel + loader mapping.")
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

    if jibe_api_jobs_items:
        print("JibeApiJobs companies:", [c.company for c in jibe_api_jobs_items])
        print(hr())
    else:
        print("No jibe_api_jobs companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if htmlpagedsearch_items:
        print("HtmlPagedSearch companies:", [c.company for c in htmlpagedsearch_items])
        print(hr())
    else:
        print("No htmlpagedsearch companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if jobsyn_solr_items:
        print("JobsynSolr companies:", [c.company for c in jobsyn_solr_items])
        print(hr())
    else:
        print("No jobsyn_solr companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    if avature_items:
        print("Avature companies:", [c.company for c in avature_items])
        print(hr())
    else:
        print("No avature companies selected. Check ATS_Type in Excel + loader mapping.")
        print(hr())

    groups: list[AtsGroup] = [
        AtsGroup(
            ats_name="oracle",
            companies=oracle_items,
            collector=OracleCollector(),
            out_csv=OUT_ORACLE_CSV,
            out_report=OUT_ORACLE_REPORT,
        ),
        AtsGroup(
            ats_name="workday",
            companies=workday_items,
            collector=WorkdayCollector(),
            out_csv=OUT_WORKDAY_CSV,
            out_report=OUT_WORKDAY_REPORT,
        ),
        AtsGroup(
            ats_name="phenom",
            companies=phenom_items,
            collector=PhenomCollector(),
            out_csv=OUT_PHENOM_CSV,
            out_report=OUT_PHENOM_REPORT,
        ),
        AtsGroup(
            ats_name="successfactors",
            companies=successfactors_items,
            collector=SuccessFactorsCollector(),
            out_csv=OUT_SUCCESSFACTORS_CSV,
            out_report=OUT_SUCCESSFACTORS_REPORT,
        ),
        AtsGroup(
            ats_name="tribepad",
            companies=tribepad_items,
            collector=TribepadCollector(),
            out_csv=OUT_TRIBEPAD_CSV,
            out_report=OUT_TRIBEPAD_REPORT,
        ),
        AtsGroup(
            ats_name="eightfold",
            companies=eightfold_items,
            collector=EightfoldCollector(),
            out_csv=OUT_EIGHTFOLD_CSV,
            out_report=OUT_EIGHTFOLD_REPORT,
        ),
        AtsGroup(
            ats_name="algolia",
            companies=algolia_items,
            collector=AlgoliaCollector(),
            out_csv=OUT_ALGOLIA_CSV,
            out_report=OUT_ALGOLIA_REPORT,
        ),
        AtsGroup(
            ats_name="cornerstone",
            companies=cornerstone_items,
            collector=CornerstoneCollector(),
            out_csv=OUT_CORNERSTONE_CSV,
            out_report=OUT_CORNERSTONE_REPORT,
        ),
        AtsGroup(
            ats_name="embeddedstate",
            companies=embeddedstate_items,
            collector=EmbeddedStateCollector(),
            out_csv=OUT_EMBEDDEDSTATE_CSV,
            out_report=OUT_EMBEDDEDSTATE_REPORT,
        ),
        AtsGroup(
            ats_name="jibe_api_jobs",
            companies=jibe_api_jobs_items,
            collector=JibeApiJobsCollector(),
            out_csv=OUT_JIBE_API_JOBS_CSV,
            out_report=OUT_JIBE_API_JOBS_REPORT,
        ),
        AtsGroup(
            ats_name="htmlpagedsearch",
            companies=htmlpagedsearch_items,
            collector=HtmlPagedSearchCollector(),
            out_csv=OUT_HTMLPAGEDSEARCH_CSV,
            out_report=OUT_HTMLPAGEDSEARCH_REPORT,
        ),
        # Batch2 expansion collectors
        AtsGroup(
            ats_name="hibob",
            companies=hibob_items,
            collector=HibobCollector(),
            out_csv=OUT_HIBOB_CSV,
            out_report=OUT_HIBOB_REPORT,
        ),
        AtsGroup(
            ats_name="jobsyn_solr",
            companies=jobsyn_solr_items,
            collector=JobsynSolrCollector(),
            out_csv=OUT_JOBSYNC_SOLR_CSV,
            out_report=OUT_JOBSYNC_SOLR_REPORT,
        ),
        AtsGroup(
            ats_name="avature",
            companies=avature_items,
            collector=AvatureCollector(),
            out_csv=OUT_AVATURE_CSV,
            out_report=OUT_AVATURE_REPORT,
        ),
        AtsGroup(
            ats_name="breezy_portal",
            companies=breezy_portal_items,
            collector=BreezyPortalCollector(),
            out_csv=OUT_BREEZY_PORTAL_CSV,
            out_report=OUT_BREEZY_PORTAL_REPORT,
        ),
        AtsGroup(
            ats_name="umbraco_api",
            companies=umbraco_api_items,
            collector=UmbracoApiCollector(),
            out_csv=OUT_UMBRACO_API_CSV,
            out_report=OUT_UMBRACO_API_REPORT,
        ),
        AtsGroup(
            ats_name="mycareersfuture",
            companies=mycareersfuture_items,
            collector=MyCareersFutureCollector(),
            out_csv=OUT_MYCAREERSFUTURE_CSV,
            out_report=OUT_MYCAREERSFUTURE_REPORT,
        ),
        AtsGroup(
            ats_name="tuvsud_recruiting_api",
            companies=tuvsud_recruiting_api_items,
            collector=TuvSudRecruitingApiCollector(),
            out_csv=OUT_TUVSUD_RECRUITING_API_CSV,
            out_report=OUT_TUVSUD_RECRUITING_API_REPORT,
        ),
        AtsGroup(
            ats_name="milchundzucker_gjb",
            companies=milchundzucker_gjb_items,
            collector=MilchUndZuckerGjbCollector(),
            out_csv=OUT_MILCHUNDZUCKER_GJB_CSV,
            out_report=OUT_MILCHUNDZUCKER_GJB_REPORT,
        ),
        AtsGroup(
            ats_name="clinch_careers_site",
            companies=clinch_careers_site_items,
            collector=ClinchCareersSiteCollector(),
            out_csv=OUT_CLINCH_CAREERS_SITE_CSV,
            out_report=OUT_CLINCH_CAREERS_SITE_REPORT,
        ),
        AtsGroup(
            ats_name="kentico_html",
            companies=kentico_html_items,
            collector=KenticoHtmlCollector(),
            out_csv=OUT_KENTICO_HTML_CSV,
            out_report=OUT_KENTICO_HTML_REPORT,
        ),
        AtsGroup(
            ats_name="wordpress_inline_modals",
            companies=wordpress_inline_modals_items,
            collector=WordpressInlineModalsCollector(),
            out_csv=OUT_WORDPRESS_INLINE_MODALS_CSV,
            out_report=OUT_WORDPRESS_INLINE_MODALS_REPORT,
        ),
        AtsGroup(
            ats_name="wordpress_elementor",
            companies=wordpress_elementor_items,
            collector=WordpressElementorCollector(),
            out_csv=OUT_WORDPRESS_ELEMENTOR_CSV,
            out_report=OUT_WORDPRESS_ELEMENTOR_REPORT,
        ),
        AtsGroup(
            ats_name="wordpress_remix",
            companies=wordpress_remix_items,
            collector=WordpressRemixCollector(),
            out_csv=OUT_WORDPRESS_REMIX_CSV,
            out_report=OUT_WORDPRESS_REMIX_REPORT,
        ),
        AtsGroup(
            ats_name="magnolia_nextjs",
            companies=magnolia_nextjs_items,
            collector=MagnoliaNextJsCollector(),
            out_csv=OUT_MAGNOLIA_NEXTJS_CSV,
            out_report=OUT_MAGNOLIA_NEXTJS_REPORT,
        ),
        AtsGroup(
            ats_name="krohne_nextjs",
            companies=krohne_nextjs_items,
            collector=KrohneNextJsCollector(),
            out_csv=OUT_KROHNE_NEXTJS_CSV,
            out_report=OUT_KROHNE_NEXTJS_REPORT,
        ),
        AtsGroup(
            ats_name="kongsberg_optimizely_easycruit",
            companies=kongsberg_optimizely_easycruit_items,
            collector=KongsbergOptimizelyEasycruitCollector(),
            out_csv=OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_CSV,
            out_report=OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_REPORT,
        ),
        AtsGroup(
            ats_name="lr_episerver_api",
            companies=lr_episerver_api_items,
            collector=LrEpiserverApiCollector(),
            out_csv=OUT_LR_EPISERVER_API_CSV,
            out_report=OUT_LR_EPISERVER_API_REPORT,
        ),
        AtsGroup(
            ats_name="aem_workday_json",
            companies=aem_workday_json_items,
            collector=AemWorkdayJsonCollector(),
            out_csv=OUT_AEM_WORKDAY_JSON_CSV,
            out_report=OUT_AEM_WORKDAY_JSON_REPORT,
        ),
        AtsGroup(
            ats_name="carrier_html",
            companies=carrier_html_items,
            collector=CarrierHtmlCollector(),
            out_csv=OUT_CARRIER_HTML_CSV,
            out_report=OUT_CARRIER_HTML_REPORT,
        ),
        AtsGroup(
            ats_name="classnk_static_html",
            companies=classnk_static_html_items,
            collector=ClassNkStaticHtmlCollector(),
            out_csv=OUT_CLASSNK_STATIC_HTML_CSV,
            out_report=OUT_CLASSNK_STATIC_HTML_REPORT,
        ),
        AtsGroup(
            ats_name="aibel_html_hr_manager",
            companies=aibel_html_hr_manager_items,
            collector=AibelHtmlHrManagerCollector(),
            out_csv=OUT_AIBEL_HTML_HR_MANAGER_CSV,
            out_report=OUT_AIBEL_HTML_HR_MANAGER_REPORT,
        ),
        AtsGroup(
            ats_name="sitefinity",
            companies=sitefinity_items,
            collector=SitefinityCollector(),
            out_csv=OUT_SITEFINITY_CSV,
            out_report=OUT_SITEFINITY_REPORT,
        ),
        AtsGroup(
            ats_name="enermech_workable",
            companies=enermech_workable_items,
            collector=EnermechWorkableCollector(),
            out_csv=_ATS_OUTDIR + "enermech_workable_jobs_batch2.csv",
            out_report=_ATS_OUTDIR + "enermech_workable_report_batch2.json",
        ),
        AtsGroup(
            ats_name="saipem_ncore",
            companies=saipem_ncore_items,
            collector=SaipemNcoreCollector(),
            out_csv=_ATS_OUTDIR + "saipem_ncore_jobs_batch2.csv",
            out_report=_ATS_OUTDIR + "saipem_ncore_report_batch2.json",
        ),
    ]

    return groups


def main(argv: list[str] | None = None) -> None:
    """Main function to run batch collection for all supported ATS groups."""

    parser = argparse.ArgumentParser(description="Run ATS batch collection (batch2).")
    parser.add_argument(
        "--only",
        default=os.environ.get("ATS_ONLY"),
        help="Comma-separated ATS names to run (e.g. jobsyn_solr,avature)",
    )
    parser.add_argument(
        "--priority",
        default=os.environ.get("ATS_PRIORITY"),
        help="Comma-separated ATS names to run first (overrides default priority list)",
    )
    parser.add_argument(
        "--stop-after-priority",
        action="store_true",
        default=(os.environ.get("ATS_STOP_AFTER_PRIORITY", "").strip().lower() in {"1", "true", "yes"}),
        help="Run priority ATS groups first, then exit (skip the rest)",
    )
    parser.add_argument(
        "--only-priority",
        action="store_true",
        default=(os.environ.get("ATS_ONLY_PRIORITY", "").strip().lower() in {"1", "true", "yes"}),
        help="Run only priority ATS groups (skip everything else)",
    )
    args = parser.parse_args(argv)

    only_set = set(_parse_csv_list(args.only)) if args.only else None

    priority = _parse_csv_list(args.priority)
    if not priority:
        priority = DEFAULT_PRIORITY_ATS

    # 1) Load companies
    items = load_companies(MASTER_INPUT)

    # 2) Build groups + print selection counts
    groups = _build_groups(items=items)

    # 3) Filter and order
    if args.only_priority:
        groups = [g for g in groups if g.ats_name in set(priority)]
    elif only_set is not None:
        groups = [g for g in groups if g.ats_name in only_set]

    groups = [g for g in groups if g.companies]
    if not groups:
        raise RuntimeError(
            "No supported ATS companies found (check ats_new_norm in Excel + registry mappings)"
        )

    # Stable order, but priority ATS groups first.
    groups_sorted = sorted(groups, key=lambda g: _priority_order_key(g.ats_name, priority))


    # 4) Parallelisiere die Ausführung der ATS-Gruppen
    from concurrent.futures import ThreadPoolExecutor, as_completed
    ran_any = False
    all_zero_vacancy_companies = []
    all_zero_vacancy_companies_set = set()
    futures = []
    group_names = []
    # Nutze ThreadPoolExecutor für parallele Gruppen (CPU-lastige Teile könnten auch ProcessPoolExecutor nutzen)
    with ThreadPoolExecutor(max_workers=min(8, len(groups_sorted))) as executor:
        for group in groups_sorted:
            # Führe run_one_ats für jede Gruppe parallel aus
            future = executor.submit(
                run_one_ats,
                ats_name=group.ats_name,
                companies=group.companies,
                collector=group.collector,
                items_total=len(items),
                out_csv=group.out_csv,
                out_report=group.out_report,
                return_per_company_counts=True
            )
            futures.append((group, future))
            group_names.append(group.ats_name)

        for group, future in futures:
            try:
                per_company_counts = future.result()
                ran_any = True
                # Unternehmen mit zero vacancies in dieser Gruppe sammeln
                zero_vacancy_companies = [c for c in group.companies if per_company_counts.get(c.company, 0) == 0]
                for c in zero_vacancy_companies:
                    if c.company not in all_zero_vacancy_companies_set:
                        all_zero_vacancy_companies.append((c.company, c.careers_url))
                        all_zero_vacancy_companies_set.add(c.company)
            except Exception as e:
                print(f"Fehler in Gruppe {group.ats_name}: {e}")

    if not ran_any:
        raise RuntimeError(
            "No supported ATS companies found (check ats_new_norm in Excel + registry mappings)"
        )

    # --- Schreibe alle Unternehmen mit zero vacancies in eine CSV ---
    out_zero_vacancies = "data/output/companies_with_zero_vacancies.csv"
    import csv
    with open(out_zero_vacancies, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["company", "careers_url"])
        for name, url in all_zero_vacancy_companies:
            writer.writerow([name, url])

def run_one_ats(
    *,
    ats_name: str,
    companies: list,
    collector,
    items_total: int,
    out_csv: str,
    out_report: str,
    return_per_company_counts: bool = False,
) -> None:
    """ Run collection, mapping, normalization, validation, dedupe, export for one ATS.
        Wenn return_per_company_counts=True, wird das per_company_counts-Dict zurückgegeben.
    """
    
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

    # --- Statuslogik: previous.csv vs current.csv ---
    previous_csv = out_csv.replace("_jobs_batch2.csv", "_jobs_previous.csv")
    current_csv = out_csv
    import os
    if not os.path.exists(previous_csv):
        # Erster Lauf: Alle als New markieren und current als previous speichern
        import shutil
        # Lese aktuelle CSV
        import csv
        with open(current_csv, newline="", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
        for row in reader:
            row["status"] = "New"
        fieldnames = list(reader[0].keys()) if reader else []
        with open(current_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(reader)
        shutil.copy(current_csv, previous_csv)
        print(f"Erster Lauf: Status=New und {previous_csv} angelegt.\n")
    else:
        status_dict = compare_job_status(previous_csv, current_csv)
        # Schreibe Status-Spalte direkt in current.csv
        if status_dict:
            import csv
            with open(current_csv, newline="", encoding="utf-8") as f:
                reader = list(csv.DictReader(f))
            for row in reader:
                jobid = get_job_id(row)
                status = status_dict.get(jobid, ("New",))[0]  # Default: New
                row["status"] = status
            fieldnames = list(reader[0].keys()) if reader else []
            with open(current_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(reader)
            print(f"Status-Spalte in {current_csv} ergänzt (New/Closed/Open)\n")



    print("DONE:", ats_name)
    print(f"CSV:    {out_csv}")
    print(f"REPORT: {out_report}")
    print(f"Records after dedupe: {len(records_after_dedupe)}")
    print(hr())

    # Für die Gesamtausgabe: per_company_counts zurückgeben, falls gewünscht
    # (default: None, für Kompatibilität mit bestehendem Aufruf)
    if return_per_company_counts:
        return per_company_counts


def _collect_and_map(company, collector):
    """ Helper to collect and map for one company."""
    res = collector.collect_raw(company)
    if res.error:
        return None
    records = collector.map_to_records(res)
    # Always take company name from input (Excel) instead of any scraped/ATS-provided company field.
    input_company = (getattr(company, "company", None) or "").strip()
    if not input_company:
        return records
    return [replace(r, company=input_company) for r in records]

if __name__ == "__main__":
    main()
    subprocess.run(["python", "-m", "src.runners.merge_All_jobs"])

