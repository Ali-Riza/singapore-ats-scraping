from __future__ import annotations  # For forward compatibility with future Python versions

import argparse
import csv
import logging
import os
import shutil
import subprocess
import sys
import time
from collections import Counter  # For counting per-company jobs
from concurrent.futures import ThreadPoolExecutor, as_completed  # For parallel collection of companies
from datetime import date, datetime
from typing import Dict, Tuple


def get_job_id(record: dict) -> str:
    """Return a stable identifier for status comparisons.

    We intentionally prefer a deterministic key based on company + job title.
    Some sources generate unstable job_id values across runs (hashes, transient
    IDs), which would create false duplicates and 'Closed' misclassifications.

    Note: our exported CSV schema uses job_title/job_url (not title/url).
    """

    company = str(record.get("company", "") or "").strip()
    title = str(record.get("job_title") or record.get("title") or "").strip()
    if company and title:
        return f"{company}|{title}"

    # Strict mode: we do not fall back to URL or vendor IDs, because that would
    # reintroduce instability across runs and cause false Closed/Open flips.
    raise ValueError(
        "Missing required fields for stable status key (need company + job_title). "
        f"Present keys: {sorted(record.keys())}"
    )


def read_jobs_csv(path: str) -> Dict[str, dict]:
    jobs = {}
    try:
        # CSVs are written as utf-8-sig to help Excel. Using utf-8-sig here
        # avoids a BOM leaking into the first header (e.g. '\ufeffcompany').
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    jobid = get_job_id(row)
                except ValueError as exc:
                    logging.getLogger(__name__).warning(
                        "Skipping row without stable status key in %s (%s)",
                        path,
                        exc,
                    )
                    continue
                jobs[jobid] = row
    except FileNotFoundError:
        pass
    return jobs


def _parse_posted_date_to_date(raw: object) -> date | None:
    """Parses a raw posted date string into a date object."""
    s = str(raw or "").strip()
    if not s or s.upper() == "NONE":
        return None

    # Prefer normalized format (YYYY-MM-DD).
    if len(s) >= 10:
        head = s[:10]
        try:
            return date.fromisoformat(head)
        except ValueError:
            pass

    # Fallback for alternate formats seen in some feeds.
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    return None


def _is_new_by_posted_date(row: dict, last_run_date: date, current_run_date: date) -> bool:
    """ Determines if a job is 'New' based on its posted_date field compared to last and current run dates."""
    posted = _parse_posted_date_to_date(row.get("posted_date"))
    if posted is None:
        return False
    return last_run_date <= posted <= current_run_date


def compare_job_status(
    previous_csv: str,
    current_csv: str,
    *,
    last_run_date: date,
    current_run_date: date,
) -> Dict[str, Tuple[str, dict]]:
    """
    Vergleicht previous.csv und current.csv und gibt ein Dict mit Job-ID -> (Status, Datensatz) zurück.
    Status: 'New', 'Closed', 'Open'
    New ist datumsbasiert:
      1) posted_date existiert
      2) posted_date >= last_run_date
      3) posted_date <= current_run_date
    """
    prev_jobs = read_jobs_csv(previous_csv)
    curr_jobs = read_jobs_csv(current_csv)
    prev_ids = set(prev_jobs.keys())
    curr_ids = set(curr_jobs.keys())

    # Vergleiche die beiden Sätze und bestimme den Status jedes Jobs
    status_dict = {}
    for jobid in prev_ids - curr_ids:
        status_dict[jobid] = ("Closed", prev_jobs[jobid])

    for jobid in curr_ids:
        row = curr_jobs[jobid]
        if _is_new_by_posted_date(row, last_run_date, current_run_date):
            status_dict[jobid] = ("New", row)
        else:
            status_dict[jobid] = ("Open", row)

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


try:
    import dataclasses as _dataclasses

    dataclass = _dataclasses.dataclass  # type: ignore[attr-defined]
    replace = _dataclasses.replace  # type: ignore[attr-defined]
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Your Python stdlib module 'dataclasses' is missing/broken (no 'dataclass' / 'replace'). "
        "In this environment, that usually means a corrupted Homebrew Python 3.13 install (empty dataclasses.py). "
        "Fix: reinstall/upgrade Python (e.g. `brew reinstall python@3.13` or use Python 3.12), "
        "then recreate the virtualenv (.venv)."
    ) from exc

from tqdm import tqdm

from src.io.loaders import load_companies  # Load companies from Excel
from src.collectors.registry import pick_collector  # Pick collector based on company item
from src.collectors.oracle import OracleCollector  # Oracle collector
from src.collectors.wsp import WspCollector
from src.collectors.bertschi import BertschiCollector
from src.collectors.vis import VisCollector
from src.collectors.umc import UmcCollector
from src.collectors.katoen_natie import KatoenNatieCollector
from src.collectors.jurong_engineering import JurongEngineeringCollector
from src.collectors.toyo_engineering import ToyoEngineeringCollector
from src.collectors.eightfold import EightfoldCollector  # Eightfold collector
from src.collectors.algolia import AlgoliaCollector  # Algolia collector
from src.collectors.cornerstone import CornerstoneCollector  # Cornerstone collector
from src.collectors.embeddedstate import EmbeddedStateCollector  # Embedded State collector
from src.collectors.jibe_api_jobs import JibeApiJobsCollector  # Jibe (/api/jobs) collector
from src.collectors.html_paged_search import HtmlPagedSearchCollector  # HTML-Paged-Search (RSS + HTML fallback)
from src.collectors.phenom import PhenomCollector  # Phenom collector
from src.collectors.successfactors import SuccessFactorsCollector  # SuccessFactors collector
from src.collectors.tribepad import TribepadCollector  # Tribepad collector
from src.collectors.workday import WorkdayCollector  # Workday collector

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
from src.collectors.jobstreet_company_page import JobStreetCompanyPageCollector
from src.collectors.icims import IcimsCollector
from src.collectors.recruiterpal_api import RecruiterpalApiCollector
from src.collectors.smartrecruiters_api import SmartRecruitersApiCollector
from src.collectors.syngenta_api import SyngentaApiCollector
from src.collectors.wordpress_simple_job_board import WordpressSimpleJobBoardCollector
from src.collectors.ineos_html import IneosHtmlCollector
from src.collectors.croda_api import CrodaApiCollector
from src.collectors.teknorapex_html import TeknorApexHtmlCollector
from src.collectors.onecruiter_iframe import OnecruiterIframeCollector
from src.collectors.amgen import AmgenCollector
from src.collectors.arup_selectminds import ArupSelectMindsCollector
# Add EnerMech and Saipem collectors
from src.collectors.enermech_workable import EnermechWorkableCollector
from src.collectors.saipem_ncore import SaipemNcoreCollector

from src.core.normalize import normalize_records  # Normalize JobRecord fields
from src.core.validators import validate_records  # Validate JobRecord fields
from src.core.dedupe import dedupe_records  # Dedupe JobRecord list

from src.io.exporter import export_records_csv, CSV_FIELDS  # Export JobRecord list to CSV and use consistent CSV schema
from src.io.reporting import build_report, export_report_json  # Build and export report

logger = logging.getLogger(__name__)


# Constants for input/output paths

MASTER_INPUT = "data/input/master_companies_with_fingerprint.xlsx"


_ATS_OUTDIR = "data/output/ats_runs/"
MERGED_XLSX = "data/output/all_jobs.xlsx"


def _previous_csv_path(out_csv: str) -> str:
    """Compute a sibling CSV path used to store previous run data."""
    candidates = []
    if "_jobs_.csv" in out_csv:
        candidates.append(out_csv.replace("_jobs_.csv", "_jobs_previous.csv"))
    if "_jobs_batch2.csv" in out_csv:
        candidates.append(out_csv.replace("_jobs_batch2.csv", "_jobs_previous.csv"))
    base, ext = os.path.splitext(out_csv)
    candidates.append(f"{base}_previous{ext}")
    candidates.append(f"{out_csv}.previous")
    for candidate in candidates:
        if candidate and candidate != out_csv:
            return candidate
    return out_csv + ".previous"


def _read_last_run_date_from_csv(csv_path: str) -> date | None:
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                parsed = _parse_posted_date_to_date(row.get("run_date"))
                if parsed is not None:
                    return parsed
    except (FileNotFoundError, OSError, TypeError, ValueError):
        return None

    return None

OUT_ORACLE_CSV = _ATS_OUTDIR + "oracle_jobs_.csv"
OUT_ORACLE_REPORT = _ATS_OUTDIR + "oracle_report_.json"

OUT_WSP_CSV = _ATS_OUTDIR + "wsp_jobs_.csv"
OUT_WSP_REPORT = _ATS_OUTDIR + "wsp_report_.json"

OUT_BERTSCHI_CSV = _ATS_OUTDIR + "bertschi_jobs_.csv"
OUT_BERTSCHI_REPORT = _ATS_OUTDIR + "bertschi_report_.json"

OUT_VIS_CSV = _ATS_OUTDIR + "vis_jobs_.csv"
OUT_VIS_REPORT = _ATS_OUTDIR + "vis_report_.json"

OUT_UMC_CSV = _ATS_OUTDIR + "umc_jobs_.csv"
OUT_UMC_REPORT = _ATS_OUTDIR + "umc_report_.json"

OUT_KATOEN_NATIE_CSV = _ATS_OUTDIR + "katoen_natie_jobs_.csv"
OUT_KATOEN_NATIE_REPORT = _ATS_OUTDIR + "katoen_natie_report_.json"

OUT_JURONG_ENGINEERING_CSV = _ATS_OUTDIR + "jurong_engineering_jobs_.csv"
OUT_JURONG_ENGINEERING_REPORT = _ATS_OUTDIR + "jurong_engineering_report_.json"

OUT_TOYO_ENGINEERING_CSV = _ATS_OUTDIR + "toyo_engineering_jobs_.csv"
OUT_TOYO_ENGINEERING_REPORT = _ATS_OUTDIR + "toyo_engineering_report_.json"

OUT_WORKDAY_CSV = _ATS_OUTDIR + "workday_jobs_.csv"
OUT_WORKDAY_REPORT = _ATS_OUTDIR + "workday_report_.json"

OUT_PHENOM_CSV = _ATS_OUTDIR + "phenom_jobs_.csv"
OUT_PHENOM_REPORT = _ATS_OUTDIR + "phenom_report_.json"

OUT_SUCCESSFACTORS_CSV = _ATS_OUTDIR + "successfactors_jobs_.csv"
OUT_SUCCESSFACTORS_REPORT = _ATS_OUTDIR + "successfactors_report_.json"

OUT_TRIBEPAD_CSV = _ATS_OUTDIR + "tribepad_jobs_.csv"
OUT_TRIBEPAD_REPORT = _ATS_OUTDIR + "tribepad_report_.json"

OUT_EIGHTFOLD_CSV = _ATS_OUTDIR + "eightfold_jobs_.csv"
OUT_EIGHTFOLD_REPORT = _ATS_OUTDIR + "eightfold_report_.json"

OUT_ALGOLIA_CSV = _ATS_OUTDIR + "algolia_jobs_.csv"
OUT_ALGOLIA_REPORT = _ATS_OUTDIR + "algolia_report_.json"

OUT_CORNERSTONE_CSV = _ATS_OUTDIR + "cornerstone_jobs_.csv"
OUT_CORNERSTONE_REPORT = _ATS_OUTDIR + "cornerstone_report_.json"

OUT_EMBEDDEDSTATE_CSV = _ATS_OUTDIR + "embeddedstate_jobs_.csv"
OUT_EMBEDDEDSTATE_REPORT = _ATS_OUTDIR + "embeddedstate_report_.json"

OUT_HTMLPAGEDSEARCH_CSV = _ATS_OUTDIR + "htmlpagedsearch_jobs_.csv"
OUT_HTMLPAGEDSEARCH_REPORT = _ATS_OUTDIR + "htmlpagedsearch_report_.json"

OUT_JIBE_API_JOBS_CSV = _ATS_OUTDIR + "jibe_api_jobs_.csv"
OUT_JIBE_API_JOBS_REPORT = _ATS_OUTDIR + "jibe_api_jobs_report_.json"

OUT_HIBOB_CSV = _ATS_OUTDIR + "hibob_jobs_.csv"
OUT_HIBOB_REPORT = _ATS_OUTDIR + "hibob_report_.json"

OUT_JOBSYNC_SOLR_CSV = _ATS_OUTDIR + "jobsyn_solr_jobs_.csv"
OUT_JOBSYNC_SOLR_REPORT = _ATS_OUTDIR + "jobsyn_solr_report_.json"

OUT_AVATURE_CSV = _ATS_OUTDIR + "avature_jobs_.csv"
OUT_AVATURE_REPORT = _ATS_OUTDIR + "avature_report_.json"

OUT_BREEZY_PORTAL_CSV = _ATS_OUTDIR + "breezy_portal_jobs_.csv"
OUT_BREEZY_PORTAL_REPORT = _ATS_OUTDIR + "breezy_portal_report_.json"

OUT_UMBRACO_API_CSV = _ATS_OUTDIR + "umbraco_api_jobs_.csv"
OUT_UMBRACO_API_REPORT = _ATS_OUTDIR + "umbraco_api_report_.json"

OUT_MYCAREERSFUTURE_CSV = _ATS_OUTDIR + "mycareersfuture_jobs_.csv"
OUT_MYCAREERSFUTURE_REPORT = _ATS_OUTDIR + "mycareersfuture_report_.json"

OUT_TUVSUD_RECRUITING_API_CSV = _ATS_OUTDIR + "tuvsud_recruiting_api_jobs_.csv"
OUT_TUVSUD_RECRUITING_API_REPORT = _ATS_OUTDIR + "tuvsud_recruiting_api_report_.json"

OUT_MILCHUNDZUCKER_GJB_CSV = _ATS_OUTDIR + "milchundzucker_gjb_jobs_.csv"
OUT_MILCHUNDZUCKER_GJB_REPORT = _ATS_OUTDIR + "milchundzucker_gjb_report_.json"

OUT_CLINCH_CAREERS_SITE_CSV = _ATS_OUTDIR + "clinch_careers_site_jobs_.csv"
OUT_CLINCH_CAREERS_SITE_REPORT = _ATS_OUTDIR + "clinch_careers_site_report_.json"

OUT_KENTICO_HTML_CSV = _ATS_OUTDIR + "kentico_html_jobs_.csv"
OUT_KENTICO_HTML_REPORT = _ATS_OUTDIR + "kentico_html_report_.json"

OUT_WORDPRESS_INLINE_MODALS_CSV = _ATS_OUTDIR + "wordpress_inline_modals_jobs_.csv"
OUT_WORDPRESS_INLINE_MODALS_REPORT = _ATS_OUTDIR + "wordpress_inline_modals_report_.json"

OUT_WORDPRESS_ELEMENTOR_CSV = _ATS_OUTDIR + "wordpress_elementor_jobs_.csv"
OUT_WORDPRESS_ELEMENTOR_REPORT = _ATS_OUTDIR + "wordpress_elementor_report_.json"

OUT_WORDPRESS_REMIX_CSV = _ATS_OUTDIR + "wordpress_remix_jobs_.csv"
OUT_WORDPRESS_REMIX_REPORT = _ATS_OUTDIR + "wordpress_remix_report_.json"

OUT_MAGNOLIA_NEXTJS_CSV = _ATS_OUTDIR + "magnolia_nextjs_jobs_.csv"
OUT_MAGNOLIA_NEXTJS_REPORT = _ATS_OUTDIR + "magnolia_nextjs_report_.json"

OUT_KROHNE_NEXTJS_CSV = _ATS_OUTDIR + "krohne_nextjs_jobs_.csv"
OUT_KROHNE_NEXTJS_REPORT = _ATS_OUTDIR + "krohne_nextjs_report_.json"

OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_CSV = _ATS_OUTDIR + "kongsberg_optimizely_easycruit_jobs_.csv"
OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_REPORT = _ATS_OUTDIR + "kongsberg_optimizely_easycruit_report_.json"

OUT_LR_EPISERVER_API_CSV = _ATS_OUTDIR + "lr_episerver_api_jobs_.csv"
OUT_LR_EPISERVER_API_REPORT = _ATS_OUTDIR + "lr_episerver_api_report_.json"

OUT_AEM_WORKDAY_JSON_CSV = _ATS_OUTDIR + "aem_workday_json_jobs_.csv"
OUT_AEM_WORKDAY_JSON_REPORT = _ATS_OUTDIR + "aem_workday_json_report_.json"

OUT_CARRIER_HTML_CSV = _ATS_OUTDIR + "carrier_html_jobs_.csv"
OUT_CARRIER_HTML_REPORT = _ATS_OUTDIR + "carrier_html_report_.json"

OUT_CLASSNK_STATIC_HTML_CSV = _ATS_OUTDIR + "classnk_static_html_jobs_.csv"
OUT_CLASSNK_STATIC_HTML_REPORT = _ATS_OUTDIR + "classnk_static_html_report_.json"

OUT_AIBEL_HTML_HR_MANAGER_CSV = _ATS_OUTDIR + "aibel_html_hr_manager_jobs_.csv"
OUT_AIBEL_HTML_HR_MANAGER_REPORT = _ATS_OUTDIR + "aibel_html_hr_manager_report_.json"

OUT_SITEFINITY_CSV = _ATS_OUTDIR + "sitefinity_jobs_.csv"
OUT_SITEFINITY_REPORT = _ATS_OUTDIR + "sitefinity_report_.json"

OUT_JOBSTREET_COMPANY_PAGE_CSV = _ATS_OUTDIR + "jobstreet_company_page_jobs_.csv"
OUT_JOBSTREET_COMPANY_PAGE_REPORT = _ATS_OUTDIR + "jobstreet_company_page_report_.json"

OUT_ICIMS_CSV = _ATS_OUTDIR + "icims_jobs_.csv"
OUT_ICIMS_REPORT = _ATS_OUTDIR + "icims_report_.json"

OUT_RECRUITERPAL_API_CSV = _ATS_OUTDIR + "recruiterpal_api_jobs_.csv"
OUT_RECRUITERPAL_API_REPORT = _ATS_OUTDIR + "recruiterpal_api_report_.json"

OUT_SYNGENTA_API_CSV = _ATS_OUTDIR + "syngenta_api_jobs_.csv"
OUT_SYNGENTA_API_REPORT = _ATS_OUTDIR + "syngenta_api_report_.json"

OUT_WORDPRESS_SIMPLE_JOB_BOARD_CSV = _ATS_OUTDIR + "wordpress_simple_job_board_jobs_.csv"
OUT_WORDPRESS_SIMPLE_JOB_BOARD_REPORT = _ATS_OUTDIR + "wordpress_simple_job_board_report_.json"

OUT_INEOS_HTML_CSV = _ATS_OUTDIR + "ineos_html_jobs_.csv"
OUT_INEOS_HTML_REPORT = _ATS_OUTDIR + "ineos_html_report_.json"

OUT_CRODA_API_CSV = _ATS_OUTDIR + "croda_api_jobs_.csv"
OUT_CRODA_API_REPORT = _ATS_OUTDIR + "croda_api_report_.json"

OUT_TEKNORAPEX_HTML_CSV = _ATS_OUTDIR + "teknorapex_html_jobs_.csv"
OUT_TEKNORAPEX_HTML_REPORT = _ATS_OUTDIR + "teknorapex_html_report_.json"

OUT_ONECRUITER_IFRAME_CSV = _ATS_OUTDIR + "onecruiter_iframe_jobs_.csv"
OUT_ONECRUITER_IFRAME_REPORT = _ATS_OUTDIR + "onecruiter_iframe_report_.json"

OUT_AMGEN_CSV = _ATS_OUTDIR + "amgen_jobs_.csv"
OUT_AMGEN_REPORT = _ATS_OUTDIR + "amgen_report_.json"

OUT_ARUP_SELECTMINDS_CSV = _ATS_OUTDIR + "arup_selectminds_jobs_.csv"
OUT_ARUP_SELECTMINDS_REPORT = _ATS_OUTDIR + "arup_selectminds_report_.json"

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


@dataclass(frozen=True)
class AtsRunSummary:
    per_company_counts: dict[str, int]
    total_records: int
    status_counts: Counter
    duration_seconds: float


def _parse_csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [p.strip() for p in value.split(",") if p.strip()]


def _env_int(name: str) -> int | None:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _env_bool(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _priority_order_key(ats_name: str, priority: list[str]) -> tuple[int, int]:
    try:
        return (0, priority.index(ats_name))
    except ValueError:
        return (1, 10_000)


def _build_groups(
    *,
    items: list,
    ats_outdir: str | None = None,
) -> list[AtsGroup]:
    """Build ATS groups (items + collector + output paths) in a stable default order."""

    def _out(path: str) -> str:
        if not ats_outdir:
            return path
        return os.path.join(ats_outdir, os.path.basename(path))

    oracle_items = [it for it in items if pick_collector(it) == "oracle"]
    wsp_items = [it for it in items if pick_collector(it) == "wsp"]
    bertschi_items = [it for it in items if pick_collector(it) == "bertschi"]
    vis_items = [it for it in items if pick_collector(it) == "vis"]
    umc_items = [it for it in items if pick_collector(it) == "umc"]
    katoen_natie_items = [it for it in items if pick_collector(it) == "katoen_natie"]
    jurong_engineering_items = [it for it in items if pick_collector(it) == "jurong_engineering"]
    toyo_engineering_items = [it for it in items if pick_collector(it) == "toyo_engineering"]
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

    jobstreet_company_page_items = [
        it for it in items if pick_collector(it) == "jobstreet_company_page"
    ]
    icims_items = [it for it in items if pick_collector(it) == "icims"]
    recruiterpal_api_items = [it for it in items if pick_collector(it) == "recruiterpal_api"]
    smartrecruiters_api_items = [it for it in items if pick_collector(it) == "smartrecruiters_api"]
    syngenta_api_items = [it for it in items if pick_collector(it) == "syngenta_api"]
    wordpress_simple_job_board_items = [
        it for it in items if pick_collector(it) == "wordpress_simple_job_board"
    ]
    ineos_html_items = [it for it in items if pick_collector(it) == "ineos_html"]
    croda_api_items = [it for it in items if pick_collector(it) == "croda_api"]
    teknorapex_html_items = [it for it in items if pick_collector(it) == "teknorapex_html"]
    onecruiter_iframe_items = [it for it in items if pick_collector(it) == "onecruiter_iframe"]
    amgen_items = [it for it in items if pick_collector(it) == "amgen"]
    arup_selectminds_items = [it for it in items if pick_collector(it) == "arup_selectminds"]

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

    groups: list[AtsGroup] = [
        AtsGroup(
            ats_name="oracle",
            companies=oracle_items,
            collector=OracleCollector(),
            out_csv=_out(OUT_ORACLE_CSV),
            out_report=_out(OUT_ORACLE_REPORT),
        ),
        AtsGroup(
            ats_name="wsp",
            companies=wsp_items,
            collector=WspCollector(),
            out_csv=_out(OUT_WSP_CSV),
            out_report=_out(OUT_WSP_REPORT),
        ),
        AtsGroup(
            ats_name="bertschi",
            companies=bertschi_items,
            collector=BertschiCollector(),
            out_csv=_out(OUT_BERTSCHI_CSV),
            out_report=_out(OUT_BERTSCHI_REPORT),
        ),
        AtsGroup(
            ats_name="vis",
            companies=vis_items,
            collector=VisCollector(),
            out_csv=_out(OUT_VIS_CSV),
            out_report=_out(OUT_VIS_REPORT),
        ),
        AtsGroup(
            ats_name="umc",
            companies=umc_items,
            collector=UmcCollector(),
            out_csv=_out(OUT_UMC_CSV),
            out_report=_out(OUT_UMC_REPORT),
        ),
        AtsGroup(
            ats_name="katoen_natie",
            companies=katoen_natie_items,
            collector=KatoenNatieCollector(),
            out_csv=_out(OUT_KATOEN_NATIE_CSV),
            out_report=_out(OUT_KATOEN_NATIE_REPORT),
        ),
        AtsGroup(
            ats_name="jurong_engineering",
            companies=jurong_engineering_items,
            collector=JurongEngineeringCollector(),
            out_csv=_out(OUT_JURONG_ENGINEERING_CSV),
            out_report=_out(OUT_JURONG_ENGINEERING_REPORT),
        ),
        AtsGroup(
            ats_name="toyo_engineering",
            companies=toyo_engineering_items,
            collector=ToyoEngineeringCollector(),
            out_csv=_out(OUT_TOYO_ENGINEERING_CSV),
            out_report=_out(OUT_TOYO_ENGINEERING_REPORT),
        ),
        AtsGroup(
            ats_name="workday",
            companies=workday_items,
            collector=WorkdayCollector(),
            out_csv=_out(OUT_WORKDAY_CSV),
            out_report=_out(OUT_WORKDAY_REPORT),
        ),
        AtsGroup(
            ats_name="phenom",
            companies=phenom_items,
            collector=PhenomCollector(),
            out_csv=_out(OUT_PHENOM_CSV),
            out_report=_out(OUT_PHENOM_REPORT),
        ),
        AtsGroup(
            ats_name="successfactors",
            companies=successfactors_items,
            collector=SuccessFactorsCollector(),
            out_csv=_out(OUT_SUCCESSFACTORS_CSV),
            out_report=_out(OUT_SUCCESSFACTORS_REPORT),
        ),
        AtsGroup(
            ats_name="tribepad",
            companies=tribepad_items,
            collector=TribepadCollector(),
            out_csv=_out(OUT_TRIBEPAD_CSV),
            out_report=_out(OUT_TRIBEPAD_REPORT),
        ),
        AtsGroup(
            ats_name="eightfold",
            companies=eightfold_items,
            collector=EightfoldCollector(),
            out_csv=_out(OUT_EIGHTFOLD_CSV),
            out_report=_out(OUT_EIGHTFOLD_REPORT),
        ),
        AtsGroup(
            ats_name="algolia",
            companies=algolia_items,
            collector=AlgoliaCollector(),
            out_csv=_out(OUT_ALGOLIA_CSV),
            out_report=_out(OUT_ALGOLIA_REPORT),
        ),
        AtsGroup(
            ats_name="cornerstone",
            companies=cornerstone_items,
            collector=CornerstoneCollector(),
            out_csv=_out(OUT_CORNERSTONE_CSV),
            out_report=_out(OUT_CORNERSTONE_REPORT),
        ),
        AtsGroup(
            ats_name="embeddedstate",
            companies=embeddedstate_items,
            collector=EmbeddedStateCollector(),
            out_csv=_out(OUT_EMBEDDEDSTATE_CSV),
            out_report=_out(OUT_EMBEDDEDSTATE_REPORT),
        ),
        AtsGroup(
            ats_name="jibe_api_jobs",
            companies=jibe_api_jobs_items,
            collector=JibeApiJobsCollector(),
            out_csv=_out(OUT_JIBE_API_JOBS_CSV),
            out_report=_out(OUT_JIBE_API_JOBS_REPORT),
        ),
        AtsGroup(
            ats_name="htmlpagedsearch",
            companies=htmlpagedsearch_items,
            collector=HtmlPagedSearchCollector(),
            out_csv=_out(OUT_HTMLPAGEDSEARCH_CSV),
            out_report=_out(OUT_HTMLPAGEDSEARCH_REPORT),
        ),
        # Batch2 expansion collectors
        AtsGroup(
            ats_name="hibob",
            companies=hibob_items,
            collector=HibobCollector(),
            out_csv=_out(OUT_HIBOB_CSV),
            out_report=_out(OUT_HIBOB_REPORT),
        ),
        AtsGroup(
            ats_name="jobsyn_solr",
            companies=jobsyn_solr_items,
            collector=JobsynSolrCollector(),
            out_csv=_out(OUT_JOBSYNC_SOLR_CSV),
            out_report=_out(OUT_JOBSYNC_SOLR_REPORT),
        ),
        AtsGroup(
            ats_name="avature",
            companies=avature_items,
            collector=AvatureCollector(),
            out_csv=_out(OUT_AVATURE_CSV),
            out_report=_out(OUT_AVATURE_REPORT),
        ),
        AtsGroup(
            ats_name="breezy_portal",
            companies=breezy_portal_items,
            collector=BreezyPortalCollector(),
            out_csv=_out(OUT_BREEZY_PORTAL_CSV),
            out_report=_out(OUT_BREEZY_PORTAL_REPORT),
        ),
        AtsGroup(
            ats_name="umbraco_api",
            companies=umbraco_api_items,
            collector=UmbracoApiCollector(),
            out_csv=_out(OUT_UMBRACO_API_CSV),
            out_report=_out(OUT_UMBRACO_API_REPORT),
        ),
        AtsGroup(
            ats_name="mycareersfuture",
            companies=mycareersfuture_items,
            collector=MyCareersFutureCollector(),
            out_csv=_out(OUT_MYCAREERSFUTURE_CSV),
            out_report=_out(OUT_MYCAREERSFUTURE_REPORT),
        ),
        AtsGroup(
            ats_name="tuvsud_recruiting_api",
            companies=tuvsud_recruiting_api_items,
            collector=TuvSudRecruitingApiCollector(),
            out_csv=_out(OUT_TUVSUD_RECRUITING_API_CSV),
            out_report=_out(OUT_TUVSUD_RECRUITING_API_REPORT),
        ),
        AtsGroup(
            ats_name="milchundzucker_gjb",
            companies=milchundzucker_gjb_items,
            collector=MilchUndZuckerGjbCollector(),
            out_csv=_out(OUT_MILCHUNDZUCKER_GJB_CSV),
            out_report=_out(OUT_MILCHUNDZUCKER_GJB_REPORT),
        ),
        AtsGroup(
            ats_name="clinch_careers_site",
            companies=clinch_careers_site_items,
            collector=ClinchCareersSiteCollector(),
            out_csv=_out(OUT_CLINCH_CAREERS_SITE_CSV),
            out_report=_out(OUT_CLINCH_CAREERS_SITE_REPORT),
        ),
        AtsGroup(
            ats_name="kentico_html",
            companies=kentico_html_items,
            collector=KenticoHtmlCollector(),
            out_csv=_out(OUT_KENTICO_HTML_CSV),
            out_report=_out(OUT_KENTICO_HTML_REPORT),
        ),
        AtsGroup(
            ats_name="wordpress_inline_modals",
            companies=wordpress_inline_modals_items,
            collector=WordpressInlineModalsCollector(),
            out_csv=_out(OUT_WORDPRESS_INLINE_MODALS_CSV),
            out_report=_out(OUT_WORDPRESS_INLINE_MODALS_REPORT),
        ),
        AtsGroup(
            ats_name="wordpress_elementor",
            companies=wordpress_elementor_items,
            collector=WordpressElementorCollector(),
            out_csv=_out(OUT_WORDPRESS_ELEMENTOR_CSV),
            out_report=_out(OUT_WORDPRESS_ELEMENTOR_REPORT),
        ),
        AtsGroup(
            ats_name="wordpress_remix",
            companies=wordpress_remix_items,
            collector=WordpressRemixCollector(),
            out_csv=_out(OUT_WORDPRESS_REMIX_CSV),
            out_report=_out(OUT_WORDPRESS_REMIX_REPORT),
        ),
        AtsGroup(
            ats_name="magnolia_nextjs",
            companies=magnolia_nextjs_items,
            collector=MagnoliaNextJsCollector(),
            out_csv=_out(OUT_MAGNOLIA_NEXTJS_CSV),
            out_report=_out(OUT_MAGNOLIA_NEXTJS_REPORT),
        ),
        AtsGroup(
            ats_name="krohne_nextjs",
            companies=krohne_nextjs_items,
            collector=KrohneNextJsCollector(),
            out_csv=_out(OUT_KROHNE_NEXTJS_CSV),
            out_report=_out(OUT_KROHNE_NEXTJS_REPORT),
        ),
        AtsGroup(
            ats_name="kongsberg_optimizely_easycruit",
            companies=kongsberg_optimizely_easycruit_items,
            collector=KongsbergOptimizelyEasycruitCollector(),
            out_csv=_out(OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_CSV),
            out_report=_out(OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_REPORT),
        ),
        AtsGroup(
            ats_name="lr_episerver_api",
            companies=lr_episerver_api_items,
            collector=LrEpiserverApiCollector(),
            out_csv=_out(OUT_LR_EPISERVER_API_CSV),
            out_report=_out(OUT_LR_EPISERVER_API_REPORT),
        ),
        AtsGroup(
            ats_name="aem_workday_json",
            companies=aem_workday_json_items,
            collector=AemWorkdayJsonCollector(),
            out_csv=_out(OUT_AEM_WORKDAY_JSON_CSV),
            out_report=_out(OUT_AEM_WORKDAY_JSON_REPORT),
        ),
        AtsGroup(
            ats_name="carrier_html",
            companies=carrier_html_items,
            collector=CarrierHtmlCollector(),
            out_csv=_out(OUT_CARRIER_HTML_CSV),
            out_report=_out(OUT_CARRIER_HTML_REPORT),
        ),
        AtsGroup(
            ats_name="classnk_static_html",
            companies=classnk_static_html_items,
            collector=ClassNkStaticHtmlCollector(),
            out_csv=_out(OUT_CLASSNK_STATIC_HTML_CSV),
            out_report=_out(OUT_CLASSNK_STATIC_HTML_REPORT),
        ),
        AtsGroup(
            ats_name="aibel_html_hr_manager",
            companies=aibel_html_hr_manager_items,
            collector=AibelHtmlHrManagerCollector(),
            out_csv=_out(OUT_AIBEL_HTML_HR_MANAGER_CSV),
            out_report=_out(OUT_AIBEL_HTML_HR_MANAGER_REPORT),
        ),
        AtsGroup(
            ats_name="sitefinity",
            companies=sitefinity_items,
            collector=SitefinityCollector(),
            out_csv=_out(OUT_SITEFINITY_CSV),
            out_report=_out(OUT_SITEFINITY_REPORT),
        ),
        AtsGroup(
            ats_name="jobstreet_company_page",
            companies=jobstreet_company_page_items,
            collector=JobStreetCompanyPageCollector(),
            out_csv=_out(OUT_JOBSTREET_COMPANY_PAGE_CSV),
            out_report=_out(OUT_JOBSTREET_COMPANY_PAGE_REPORT),
        ),
        AtsGroup(
            ats_name="icims",
            companies=icims_items,
            collector=IcimsCollector(),
            out_csv=_out(OUT_ICIMS_CSV),
            out_report=_out(OUT_ICIMS_REPORT),
        ),
        AtsGroup(
            ats_name="recruiterpal_api",
            companies=recruiterpal_api_items,
            collector=RecruiterpalApiCollector(),
            out_csv=_out(OUT_RECRUITERPAL_API_CSV),
            out_report=_out(OUT_RECRUITERPAL_API_REPORT),
        ),
        AtsGroup(
            ats_name="smartrecruiters_api",
            companies=smartrecruiters_api_items,
            collector=SmartRecruitersApiCollector(),
            out_csv=_out(_ATS_OUTDIR + "smartrecruiters_api_jobs_.csv"),
            out_report=_out(_ATS_OUTDIR + "smartrecruiters_api_report_.json"),
        ),
        AtsGroup(
            ats_name="syngenta_api",
            companies=syngenta_api_items,
            collector=SyngentaApiCollector(),
            out_csv=_out(OUT_SYNGENTA_API_CSV),
            out_report=_out(OUT_SYNGENTA_API_REPORT),
        ),
        AtsGroup(
            ats_name="wordpress_simple_job_board",
            companies=wordpress_simple_job_board_items,
            collector=WordpressSimpleJobBoardCollector(),
            out_csv=_out(OUT_WORDPRESS_SIMPLE_JOB_BOARD_CSV),
            out_report=_out(OUT_WORDPRESS_SIMPLE_JOB_BOARD_REPORT),
        ),
        AtsGroup(
            ats_name="ineos_html",
            companies=ineos_html_items,
            collector=IneosHtmlCollector(),
            out_csv=_out(OUT_INEOS_HTML_CSV),
            out_report=_out(OUT_INEOS_HTML_REPORT),
        ),
        AtsGroup(
            ats_name="croda_api",
            companies=croda_api_items,
            collector=CrodaApiCollector(),
            out_csv=_out(OUT_CRODA_API_CSV),
            out_report=_out(OUT_CRODA_API_REPORT),
        ),
        AtsGroup(
            ats_name="teknorapex_html",
            companies=teknorapex_html_items,
            collector=TeknorApexHtmlCollector(),
            out_csv=_out(OUT_TEKNORAPEX_HTML_CSV),
            out_report=_out(OUT_TEKNORAPEX_HTML_REPORT),
        ),
        AtsGroup(
            ats_name="onecruiter_iframe",
            companies=onecruiter_iframe_items,
            collector=OnecruiterIframeCollector(),
            out_csv=_out(OUT_ONECRUITER_IFRAME_CSV),
            out_report=_out(OUT_ONECRUITER_IFRAME_REPORT),
        ),
        AtsGroup(
            ats_name="amgen",
            companies=amgen_items,
            collector=AmgenCollector(),
            out_csv=_out(OUT_AMGEN_CSV),
            out_report=_out(OUT_AMGEN_REPORT),
        ),
        AtsGroup(
            ats_name="arup_selectminds",
            companies=arup_selectminds_items,
            collector=ArupSelectMindsCollector(),
            out_csv=_out(OUT_ARUP_SELECTMINDS_CSV),
            out_report=_out(OUT_ARUP_SELECTMINDS_REPORT),
        ),
        AtsGroup(
            ats_name="enermech_workable",
            companies=enermech_workable_items,
            collector=EnermechWorkableCollector(),
            out_csv=_out(_ATS_OUTDIR + "enermech_workable_jobs_batch2.csv"),
            out_report=_out(_ATS_OUTDIR + "enermech_workable_report_batch2.json"),
        ),
        AtsGroup(
            ats_name="saipem_ncore",
            companies=saipem_ncore_items,
            collector=SaipemNcoreCollector(),
            out_csv=_out(_ATS_OUTDIR + "saipem_ncore_jobs_batch2.csv"),
            out_report=_out(_ATS_OUTDIR + "saipem_ncore_report_batch2.json"),
        ),
    ]

    return groups


def main(argv: list[str] | None = None) -> None:
    """Main function to run ATS collection for a custom Excel input."""
    parser = argparse.ArgumentParser(
        prog="python -m src.runners.run_pipeline",
        description="Run ATS pipeline for a custom Excel input.",
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Pfad zur Kunden-Excel (z.B. data/input/client_x.xlsx)",
    )
    parser.add_argument(
        "--only",
        default=os.environ.get("ATS_ONLY"),
        help="Comma-separated ATS names to run (e.g. jobsyn_solr,avature)",
    )

    args = parser.parse_args(argv)

    only_set = set(_parse_csv_list(args.only)) if args.only else None

    # Fixed defaults for simplified CLI
    group_workers = None
    company_workers = None
    use_cache = True
    cache_dir = "data/cache"
    cache_ttl = 900
    passes = 3

    priority = DEFAULT_PRIORITY_ATS

    # Create run-scoped output paths derived from input filename,
    # so each input writes to its own folder.
    run_name = os.path.splitext(os.path.basename(args.input))[0].strip() or "run"
    run_root = os.path.join("data", "output", run_name)
    ats_outdir = os.path.join(run_root, "ats_runs")
    merged_xlsx = os.path.join(run_root, "all_jobs.xlsx")
    zero_vacancies_csv = os.path.join(run_root, "companies_with_zero_vacancies.csv")
    os.makedirs(ats_outdir, exist_ok=True)

    # 1) Load companies from provided Excel
    items = load_companies(args.input)

    # 2) Build groups
    groups = _build_groups(items=items, ats_outdir=ats_outdir)

    # 3) Filter and order
    if only_set is not None:
        groups = [g for g in groups if g.ats_name in only_set]

    groups = [g for g in groups if g.companies]
    if not groups:
        raise RuntimeError(
            "No supported ATS companies found (check ats_new_norm in Excel + registry mappings)"
        )

    # Stable order, but priority ATS groups first.
    groups_sorted = sorted(groups, key=lambda g: _priority_order_key(g.ats_name, priority))

    overall_start = time.perf_counter()


    # 4) Parallelisiere die Ausführung der ATS-Gruppen
    ran_any = False
    all_zero_vacancy_companies = []
    all_zero_vacancy_companies_set = set()
    total_companies = sum(len(group.companies) for group in groups_sorted)

    collectors_completed = 0
    total_jobs = 0
    aggregate_status = Counter()
    ats_durations: list[tuple[str, float, int]] = []

    default_group_workers = max(4, (os.cpu_count() or 4) * 2)
    effective_group_workers = min(len(groups_sorted), group_workers or default_group_workers)
    if effective_group_workers < 1:
        effective_group_workers = 1

    progress_total = max(1, total_companies * passes)
    with tqdm(
        total=progress_total,
        desc="ATS collection",
        unit="company",
        dynamic_ncols=True,
        colour="green",
    ) as progress:
        for pass_index in range(passes):
            pass_label = f"pass {pass_index + 1}/{passes}"
            progress.set_description(f"ATS collection ({pass_label})")
            tqdm.write(f"Starting {pass_label}")
            pass_start = time.perf_counter()

            with ThreadPoolExecutor(max_workers=effective_group_workers) as executor:
                futures = {
                    executor.submit(
                        run_one_ats,
                        ats_name=group.ats_name,
                        companies=group.companies,
                        collector=group.collector,
                        items_total=len(items),
                        out_csv=group.out_csv,
                        out_report=group.out_report,
                        progress=progress,
                        company_workers=company_workers,
                        cache_dir=cache_dir,
                        use_cache=use_cache,
                        cache_ttl=cache_ttl,
                        update_status=(pass_index == passes - 1),
                    ): group
                    for group in groups_sorted
                }

                for future in as_completed(futures):
                    group = futures[future]
                    try:
                        summary = future.result()
                        if pass_index == passes - 1:
                            ran_any = True
                            collectors_completed += 1
                            total_jobs += summary.total_records
                            aggregate_status.update(summary.status_counts)
                            per_company_counts = summary.per_company_counts
                            ats_durations.append((group.ats_name, summary.duration_seconds, summary.total_records))
                            zero_vacancy_companies = [
                                c for c in group.companies if per_company_counts.get(c.company, 0) == 0
                            ]
                            for c in zero_vacancy_companies:
                                if c.company not in all_zero_vacancy_companies_set:
                                    all_zero_vacancy_companies.append((c.company, c.careers_url))
                                    all_zero_vacancy_companies_set.add(c.company)
                    except Exception:
                        logger.exception("ATS group failed (%s) during %s", group.ats_name, pass_label)

            pass_elapsed = time.perf_counter() - pass_start
            tqdm.write(f"Completed {pass_label} in {pass_elapsed:.1f}s")

    if not ran_any:
        raise RuntimeError(
            "No supported ATS companies found (check ats_new_norm in Excel + registry mappings)"
        )

    # --- Schreibe alle Unternehmen mit zero vacancies in eine CSV ---
    out_zero_vacancies = zero_vacancies_csv
    import csv
    with open(out_zero_vacancies, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["company", "careers_url"])
        for name, url in all_zero_vacancy_companies:
            writer.writerow([name, url])

    width = shutil.get_terminal_size(fallback=(80, 20)).columns
    summary_line = "-" * max(20, width)
    tqdm.write(summary_line)
    tqdm.write("Pipeline finished")
    tqdm.write(summary_line)
    tqdm.write(f"Collectors run: {collectors_completed}")
    tqdm.write(f"Total jobs: {total_jobs}")
    tqdm.write(
        "New: {new} | Open: {open_} | Closed: {closed}".format(
            new=aggregate_status.get("New", 0),
            open_=aggregate_status.get("Open", 0),
            closed=aggregate_status.get("Closed", 0),
        )
    )
    tqdm.write(summary_line)
    tqdm.write(f"CSV folder: {ats_outdir}")
    tqdm.write(f"XLSX: {merged_xlsx}")
    if ats_durations:
        tqdm.write(summary_line)
        tqdm.write("Slowest ATS groups (top 5):")
        for name, duration, job_count in sorted(ats_durations, key=lambda item: item[1], reverse=True)[:5]:
            tqdm.write(f"- {name}: {duration:.1f}s ({job_count} jobs)")
    total_elapsed = time.perf_counter() - overall_start
    tqdm.write(summary_line)
    tqdm.write(f"Elapsed: {total_elapsed:.1f}s")
    tqdm.write(summary_line)
    if (cache_dir is not None and use_cache) or passes != 1:
        cache_state = "on" if cache_dir is not None and use_cache else "off"
        tqdm.write(
            f"Options: fast_mode=off | cache={cache_state} (ttl={cache_ttl}s) | passes={passes}"
        )
        tqdm.write(summary_line)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "src.runners.merge_All_jobs",
            "--input-dir",
            ats_outdir,
            "--out",
            merged_xlsx,
        ],
        check=True,
    )


def _backfill_company_column_in_csv(csv_path: str, companies: list) -> None:
    """Ensure the 'company' column in a finished CSV is filled.

    Uses the careers_url -> company mapping from the CompanyItem list for this
    ATS group. This is a defensive post-processing step so that downstream
    users always see the company name in the CSV, even if earlier stages left
    the field empty.
    """

    if not companies or not os.path.exists(csv_path):
        return

    url_to_company: Dict[str, str] = {}
    for it in companies:
        try:
            cu = (getattr(it, "careers_url", "") or "").strip()
            name = (getattr(it, "company", "") or "").strip()
        except Exception:
            continue
        if cu and name and cu not in url_to_company:
            url_to_company[cu] = name

    if not url_to_company:
        return

    import csv as _csv

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = _csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return

    for row in rows:
        comp = (row.get("company") or "").strip()
        if comp:
            continue
        cu = (row.get("careers_url") or "").strip()
        mapped = url_to_company.get(cu, "")
        if mapped:
            row["company"] = mapped

    # Rewrite CSV with the same schema (CSV_FIELDS) and BOM for Excel.
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = _csv.DictWriter(f, fieldnames=list(CSV_FIELDS))
        writer.writeheader()
        for row in rows:
            out_row = {k: (row.get(k, "") or "") for k in CSV_FIELDS}
            writer.writerow(out_row)


def run_one_ats(
    *,
    ats_name: str,
    companies: list,
    collector,
    items_total: int,
    out_csv: str,
    out_report: str,
    progress: tqdm | None = None,
    company_workers: int | None = None,
    cache_dir: str | None = None,
    use_cache: bool = True,
    cache_ttl: int = 900,
    update_status: bool = True,
) -> AtsRunSummary:
    """Run collection, mapping, normalization, validation, dedupe, export for one ATS."""
    start_time = time.perf_counter()

    cache_enabled = bool(use_cache and cache_dir)
    effective_cache_dir = cache_dir if cache_enabled else None
    if cache_enabled and effective_cache_dir:
        os.makedirs(effective_cache_dir, exist_ok=True)

    try:
        setattr(collector, "cache_enabled", cache_enabled)
        setattr(collector, "cache_dir", effective_cache_dir)
        setattr(collector, "cache_ttl", cache_ttl)
    except Exception:
        pass

    # Collect and normalize job records from all companies
    normalized_job_records = []

    default_company_workers = max(4, (os.cpu_count() or 4) * 2)
    pool_size = company_workers if company_workers and company_workers > 0 else default_company_workers
    if companies:
        pool_size = max(1, min(pool_size, len(companies)))
    else:
        pool_size = 1

    # Use ThreadPoolExecutor for parallel HTTP requests
    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        futures = {
            executor.submit(_collect_and_map, company, collector): company for company in companies
        }

        for future in as_completed(futures):
            company = futures[future]
            try:
                mapped = future.result()
                if mapped:
                    normalized = normalize_records(mapped)
                    normalized_job_records.extend(normalized)
            except Exception:
                logger.warning("Collection failed for %s", getattr(company, "company", "unknown"))
            finally:
                if progress is not None:
                    progress.update(1)

    # 3) Validate
    validation_stats = validate_records(normalized_job_records)

    # 4) Dedupe
    records_after_dedupe = dedupe_records(normalized_job_records)

    # 4b) Ensure company is populated from input Excel companies if missing.
    # In some CSVs the company column ended up empty even though the
    # JobRecords and reports clearly tracked per-company counts. As an
    # additional safety net, we re-derive the company name from the
    # CompanyItem list (company + careers_url) for any records where
    # company is blank before exporting.
    if companies:
        url_to_company: dict[str, str] = {}
        for it in companies:
            try:
                cu = (getattr(it, "careers_url", "") or "").strip()
                name = (getattr(it, "company", "") or "").strip()
            except Exception:
                continue
            if cu and name and cu not in url_to_company:
                url_to_company[cu] = name

        if url_to_company:
            fixed: list = []
            for r in records_after_dedupe:
                comp = (getattr(r, "company", "") or "").strip()
                if not comp:
                    cu = (getattr(r, "careers_url", "") or "").strip()
                    mapped = url_to_company.get(cu, "")
                    if mapped:
                        r = replace(r, company=mapped)
                fixed.append(r)
            records_after_dedupe = fixed

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

    status_counts = Counter()
    if not update_status:
        return AtsRunSummary(
            per_company_counts=per_company_counts,
            total_records=len(records_after_dedupe),
            status_counts=status_counts,
            duration_seconds=time.perf_counter() - start_time,
        )

    # --- Statuslogik: previous.csv vs current.csv ---
    previous_csv = _previous_csv_path(out_csv)
    current_csv = out_csv
    status_counts = Counter()

    def _write_current_with_status(rows: list[dict[str, str]]) -> None:
        """Write rows to current_csv using the global CSV_FIELDS schema (incl. status).

        This ensures that we keep a stable column order and always include
        historical/closed jobs as long as they appear in our aggregated dataset.
        """
        fieldnames = list(CSV_FIELDS)
        with open(current_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                out_row = {k: row.get(k, "") for k in fieldnames}
                writer.writerow(out_row)

    # Load current CSV rows
    # Wichtig: ursprüngliche CSVs werden mit "utf-8-sig" geschrieben (BOM),
    # damit Excel die Kodierung korrekt erkennt. Wenn wir hier nur "utf-8"
    # verwenden, landet das erste Spalten-Label als "\ufeffcompany" und
    # beim Zurückschreiben verlieren wir den Wert in "company".
    # Mit "utf-8-sig" wird der BOM sauber entfernt und die Spalten heißen
    # wieder exakt wie in CSV_FIELDS.
    with open(current_csv, newline="", encoding="utf-8-sig") as f:
        current_rows = list(csv.DictReader(f))

    current_run_date = date.today()
    current_run_date_str = current_run_date.isoformat()

    if not os.path.exists(previous_csv):
        # First run (or after a reset): write a consistent status column.
        first_status = "New"
        for row in current_rows:
            row["status"] = first_status
            row["run_date"] = current_run_date_str
        _write_current_with_status(current_rows)
        if previous_csv != current_csv:
            prev_dir = os.path.dirname(previous_csv) or "."
            os.makedirs(prev_dir, exist_ok=True)
            shutil.copy(current_csv, previous_csv)
        status_counts[first_status] = len(current_rows)
    else:
        # Vergleiche previous.csv und current.csv und baue ein vereinheitlichtes
        # Dataset aus New/Open/Closed-Jobs. Dabei werden auch bereits geschlossene
        # Listings aus previous.csv weiterhin mit Status=Closed in die aktuelle
        # CSV übernommen (historische Speicherung).
        last_run_date = _read_last_run_date_from_csv(previous_csv)
        if last_run_date is None:
            # Backward compatibility for older CSVs without run_date.
            last_run_date = date.fromtimestamp(os.path.getmtime(previous_csv))
        status_dict = compare_job_status(
            previous_csv,
            current_csv,
            last_run_date=last_run_date,
            current_run_date=current_run_date,
        )

        if not status_dict:
            # Keine Jobs insgesamt – leere Struktur, aber previous.csv in Sync halten
            _write_current_with_status([])
            if previous_csv != current_csv:
                prev_dir = os.path.dirname(previous_csv) or "."
                os.makedirs(prev_dir, exist_ok=True)
                shutil.copy(current_csv, previous_csv)
        else:
            rows_with_status: list[dict[str, str]] = []
            for jobid, (status, row) in status_dict.items():
                row_out = dict(row)
                row_out["status"] = status
                row_out["run_date"] = current_run_date_str
                rows_with_status.append(row_out)

            _write_current_with_status(rows_with_status)
            if previous_csv != current_csv:
                prev_dir = os.path.dirname(previous_csv) or "."
                os.makedirs(prev_dir, exist_ok=True)
                shutil.copy(current_csv, previous_csv)

            # Status-Zählung direkt aus status_dict ableiten
            status_counts.update(status for status, _ in status_dict.values())

    # Final safety: ensure the on-disk CSV has company filled.
    try:
        _backfill_company_column_in_csv(current_csv, companies)
        if os.path.exists(previous_csv):
            _backfill_company_column_in_csv(previous_csv, companies)
    except Exception:
        # Do not fail the whole run just because of a backfill issue.
        logger.warning("company backfill failed for %s", current_csv)

    return AtsRunSummary(
        per_company_counts=per_company_counts,
        total_records=len(records_after_dedupe),
        status_counts=status_counts,
        duration_seconds=time.perf_counter() - start_time,
    )


def _collect_and_map(company, collector):
    """Helper to collect and map for one company."""
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

