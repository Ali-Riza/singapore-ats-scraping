from __future__ import annotations # For forward compatibility with future Python versions

from collections import Counter # For counting per-company jobs
from concurrent.futures import ThreadPoolExecutor # For parallel collection of companies
from dataclasses import replace

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

from src.core.normalize import normalize_records # Normalize JobRecord fields
from src.core.validators import validate_records # Validate JobRecord fields
from src.core.dedupe import dedupe_records # Dedupe JobRecord list

from src.io.exporter import export_records_csv # Export JobRecord list to CSV
from src.io.reporting import build_report, export_report_json # Build and export report

from src.utils.cli import hr


# Constants for input/output paths
MASTER_INPUT = "data/input/master_companies_with_fingerprint.xlsx"

OUT_ORACLE_CSV = "data/output/oracle_jobs_batch2.csv"
OUT_ORACLE_REPORT = "data/output/oracle_report_batch2.json"

OUT_WORKDAY_CSV = "data/output/workday_jobs_batch2.csv"
OUT_WORKDAY_REPORT = "data/output/workday_report_batch2.json"

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

OUT_JIBE_API_JOBS_CSV = "data/output/jibe_api_jobs_batch2.csv"
OUT_JIBE_API_JOBS_REPORT = "data/output/jibe_api_jobs_report_batch2.json"

OUT_HIBOB_CSV = "data/output/hibob_jobs_batch2.csv"
OUT_HIBOB_REPORT = "data/output/hibob_report_batch2.json"

OUT_JOBSYNC_SOLR_CSV = "data/output/jobsyn_solr_jobs_batch2.csv"
OUT_JOBSYNC_SOLR_REPORT = "data/output/jobsyn_solr_report_batch2.json"

OUT_AVATURE_CSV = "data/output/avature_jobs_batch2.csv"
OUT_AVATURE_REPORT = "data/output/avature_report_batch2.json"

OUT_BREEZY_PORTAL_CSV = "data/output/breezy_portal_jobs_batch2.csv"
OUT_BREEZY_PORTAL_REPORT = "data/output/breezy_portal_report_batch2.json"

OUT_UMBRACO_API_CSV = "data/output/umbraco_api_jobs_batch2.csv"
OUT_UMBRACO_API_REPORT = "data/output/umbraco_api_report_batch2.json"

OUT_MYCAREERSFUTURE_CSV = "data/output/mycareersfuture_jobs_batch2.csv"
OUT_MYCAREERSFUTURE_REPORT = "data/output/mycareersfuture_report_batch2.json"

OUT_TUVSUD_RECRUITING_API_CSV = "data/output/tuvsud_recruiting_api_jobs_batch2.csv"
OUT_TUVSUD_RECRUITING_API_REPORT = "data/output/tuvsud_recruiting_api_report_batch2.json"

OUT_MILCHUNDZUCKER_GJB_CSV = "data/output/milchundzucker_gjb_jobs_batch2.csv"
OUT_MILCHUNDZUCKER_GJB_REPORT = "data/output/milchundzucker_gjb_report_batch2.json"

OUT_CLINCH_CAREERS_SITE_CSV = "data/output/clinch_careers_site_jobs_batch2.csv"
OUT_CLINCH_CAREERS_SITE_REPORT = "data/output/clinch_careers_site_report_batch2.json"

OUT_KENTICO_HTML_CSV = "data/output/kentico_html_jobs_batch2.csv"
OUT_KENTICO_HTML_REPORT = "data/output/kentico_html_report_batch2.json"

OUT_WORDPRESS_INLINE_MODALS_CSV = "data/output/wordpress_inline_modals_jobs_batch2.csv"
OUT_WORDPRESS_INLINE_MODALS_REPORT = "data/output/wordpress_inline_modals_report_batch2.json"

OUT_WORDPRESS_ELEMENTOR_CSV = "data/output/wordpress_elementor_jobs_batch2.csv"
OUT_WORDPRESS_ELEMENTOR_REPORT = "data/output/wordpress_elementor_report_batch2.json"

OUT_WORDPRESS_REMIX_CSV = "data/output/wordpress_remix_jobs_batch2.csv"
OUT_WORDPRESS_REMIX_REPORT = "data/output/wordpress_remix_report_batch2.json"

OUT_MAGNOLIA_NEXTJS_CSV = "data/output/magnolia_nextjs_jobs_batch2.csv"
OUT_MAGNOLIA_NEXTJS_REPORT = "data/output/magnolia_nextjs_report_batch2.json"

OUT_KROHNE_NEXTJS_CSV = "data/output/krohne_nextjs_jobs_batch2.csv"
OUT_KROHNE_NEXTJS_REPORT = "data/output/krohne_nextjs_report_batch2.json"

OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_CSV = "data/output/kongsberg_optimizely_easycruit_jobs_batch2.csv"
OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_REPORT = "data/output/kongsberg_optimizely_easycruit_report_batch2.json"

OUT_LR_EPISERVER_API_CSV = "data/output/lr_episerver_api_jobs_batch2.csv"
OUT_LR_EPISERVER_API_REPORT = "data/output/lr_episerver_api_report_batch2.json"

OUT_AEM_WORKDAY_JSON_CSV = "data/output/aem_workday_json_jobs_batch2.csv"
OUT_AEM_WORKDAY_JSON_REPORT = "data/output/aem_workday_json_report_batch2.json"

OUT_CARRIER_HTML_CSV = "data/output/carrier_html_jobs_batch2.csv"
OUT_CARRIER_HTML_REPORT = "data/output/carrier_html_report_batch2.json"

OUT_CLASSNK_STATIC_HTML_CSV = "data/output/classnk_static_html_jobs_batch2.csv"
OUT_CLASSNK_STATIC_HTML_REPORT = "data/output/classnk_static_html_report_batch2.json"

OUT_AIBEL_HTML_HR_MANAGER_CSV = "data/output/aibel_html_hr_manager_jobs_batch2.csv"
OUT_AIBEL_HTML_HR_MANAGER_REPORT = "data/output/aibel_html_hr_manager_report_batch2.json"

OUT_SITEFINITY_CSV = "data/output/sitefinity_jobs_batch2.csv"
OUT_SITEFINITY_REPORT = "data/output/sitefinity_report_batch2.json"


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

    if jibe_api_jobs_items:
        run_one_ats(
            ats_name="jibe_api_jobs",
            companies=jibe_api_jobs_items,
            collector=JibeApiJobsCollector(),
            items_total=len(items),
            out_csv=OUT_JIBE_API_JOBS_CSV,
            out_report=OUT_JIBE_API_JOBS_REPORT,
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

    if hibob_items:
        run_one_ats(
            ats_name="hibob",
            companies=hibob_items,
            collector=HibobCollector(),
            items_total=len(items),
            out_csv=OUT_HIBOB_CSV,
            out_report=OUT_HIBOB_REPORT,
        )

    if jobsyn_solr_items:
        run_one_ats(
            ats_name="jobsyn_solr",
            companies=jobsyn_solr_items,
            collector=JobsynSolrCollector(),
            items_total=len(items),
            out_csv=OUT_JOBSYNC_SOLR_CSV,
            out_report=OUT_JOBSYNC_SOLR_REPORT,
        )

    if avature_items:
        run_one_ats(
            ats_name="avature",
            companies=avature_items,
            collector=AvatureCollector(),
            items_total=len(items),
            out_csv=OUT_AVATURE_CSV,
            out_report=OUT_AVATURE_REPORT,
        )

    if breezy_portal_items:
        run_one_ats(
            ats_name="breezy_portal",
            companies=breezy_portal_items,
            collector=BreezyPortalCollector(),
            items_total=len(items),
            out_csv=OUT_BREEZY_PORTAL_CSV,
            out_report=OUT_BREEZY_PORTAL_REPORT,
        )

    if umbraco_api_items:
        run_one_ats(
            ats_name="umbraco_api",
            companies=umbraco_api_items,
            collector=UmbracoApiCollector(),
            items_total=len(items),
            out_csv=OUT_UMBRACO_API_CSV,
            out_report=OUT_UMBRACO_API_REPORT,
        )

    if mycareersfuture_items:
        run_one_ats(
            ats_name="mycareersfuture",
            companies=mycareersfuture_items,
            collector=MyCareersFutureCollector(),
            items_total=len(items),
            out_csv=OUT_MYCAREERSFUTURE_CSV,
            out_report=OUT_MYCAREERSFUTURE_REPORT,
        )

    if tuvsud_recruiting_api_items:
        run_one_ats(
            ats_name="tuvsud_recruiting_api",
            companies=tuvsud_recruiting_api_items,
            collector=TuvSudRecruitingApiCollector(),
            items_total=len(items),
            out_csv=OUT_TUVSUD_RECRUITING_API_CSV,
            out_report=OUT_TUVSUD_RECRUITING_API_REPORT,
        )

    if milchundzucker_gjb_items:
        run_one_ats(
            ats_name="milchundzucker_gjb",
            companies=milchundzucker_gjb_items,
            collector=MilchUndZuckerGjbCollector(),
            items_total=len(items),
            out_csv=OUT_MILCHUNDZUCKER_GJB_CSV,
            out_report=OUT_MILCHUNDZUCKER_GJB_REPORT,
        )

    if clinch_careers_site_items:
        run_one_ats(
            ats_name="clinch_careers_site",
            companies=clinch_careers_site_items,
            collector=ClinchCareersSiteCollector(),
            items_total=len(items),
            out_csv=OUT_CLINCH_CAREERS_SITE_CSV,
            out_report=OUT_CLINCH_CAREERS_SITE_REPORT,
        )

    if kentico_html_items:
        run_one_ats(
            ats_name="kentico_html",
            companies=kentico_html_items,
            collector=KenticoHtmlCollector(),
            items_total=len(items),
            out_csv=OUT_KENTICO_HTML_CSV,
            out_report=OUT_KENTICO_HTML_REPORT,
        )

    if wordpress_inline_modals_items:
        run_one_ats(
            ats_name="wordpress_inline_modals",
            companies=wordpress_inline_modals_items,
            collector=WordpressInlineModalsCollector(),
            items_total=len(items),
            out_csv=OUT_WORDPRESS_INLINE_MODALS_CSV,
            out_report=OUT_WORDPRESS_INLINE_MODALS_REPORT,
        )

    if wordpress_elementor_items:
        run_one_ats(
            ats_name="wordpress_elementor",
            companies=wordpress_elementor_items,
            collector=WordpressElementorCollector(),
            items_total=len(items),
            out_csv=OUT_WORDPRESS_ELEMENTOR_CSV,
            out_report=OUT_WORDPRESS_ELEMENTOR_REPORT,
        )

    if wordpress_remix_items:
        run_one_ats(
            ats_name="wordpress_remix",
            companies=wordpress_remix_items,
            collector=WordpressRemixCollector(),
            items_total=len(items),
            out_csv=OUT_WORDPRESS_REMIX_CSV,
            out_report=OUT_WORDPRESS_REMIX_REPORT,
        )

    if magnolia_nextjs_items:
        run_one_ats(
            ats_name="magnolia_nextjs",
            companies=magnolia_nextjs_items,
            collector=MagnoliaNextJsCollector(),
            items_total=len(items),
            out_csv=OUT_MAGNOLIA_NEXTJS_CSV,
            out_report=OUT_MAGNOLIA_NEXTJS_REPORT,
        )

    if krohne_nextjs_items:
        run_one_ats(
            ats_name="krohne_nextjs",
            companies=krohne_nextjs_items,
            collector=KrohneNextJsCollector(),
            items_total=len(items),
            out_csv=OUT_KROHNE_NEXTJS_CSV,
            out_report=OUT_KROHNE_NEXTJS_REPORT,
        )

    if kongsberg_optimizely_easycruit_items:
        run_one_ats(
            ats_name="kongsberg_optimizely_easycruit",
            companies=kongsberg_optimizely_easycruit_items,
            collector=KongsbergOptimizelyEasycruitCollector(),
            items_total=len(items),
            out_csv=OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_CSV,
            out_report=OUT_KONGSBERG_OPTIMIZELY_EASYCRUIT_REPORT,
        )

    if lr_episerver_api_items:
        run_one_ats(
            ats_name="lr_episerver_api",
            companies=lr_episerver_api_items,
            collector=LrEpiserverApiCollector(),
            items_total=len(items),
            out_csv=OUT_LR_EPISERVER_API_CSV,
            out_report=OUT_LR_EPISERVER_API_REPORT,
        )

    if aem_workday_json_items:
        run_one_ats(
            ats_name="aem_workday_json",
            companies=aem_workday_json_items,
            collector=AemWorkdayJsonCollector(),
            items_total=len(items),
            out_csv=OUT_AEM_WORKDAY_JSON_CSV,
            out_report=OUT_AEM_WORKDAY_JSON_REPORT,
        )

    if carrier_html_items:
        run_one_ats(
            ats_name="carrier_html",
            companies=carrier_html_items,
            collector=CarrierHtmlCollector(),
            items_total=len(items),
            out_csv=OUT_CARRIER_HTML_CSV,
            out_report=OUT_CARRIER_HTML_REPORT,
        )

    if classnk_static_html_items:
        run_one_ats(
            ats_name="classnk_static_html",
            companies=classnk_static_html_items,
            collector=ClassNkStaticHtmlCollector(),
            items_total=len(items),
            out_csv=OUT_CLASSNK_STATIC_HTML_CSV,
            out_report=OUT_CLASSNK_STATIC_HTML_REPORT,
        )

    if aibel_html_hr_manager_items:
        run_one_ats(
            ats_name="aibel_html_hr_manager",
            companies=aibel_html_hr_manager_items,
            collector=AibelHtmlHrManagerCollector(),
            items_total=len(items),
            out_csv=OUT_AIBEL_HTML_HR_MANAGER_CSV,
            out_report=OUT_AIBEL_HTML_HR_MANAGER_REPORT,
        )

    if sitefinity_items:
        run_one_ats(
            ats_name="sitefinity",
            companies=sitefinity_items,
            collector=SitefinityCollector(),
            items_total=len(items),
            out_csv=OUT_SITEFINITY_CSV,
            out_report=OUT_SITEFINITY_REPORT,
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
        and not jibe_api_jobs_items
        and not htmlpagedsearch_items
        and not hibob_items
        and not jobsyn_solr_items
        and not avature_items
        and not breezy_portal_items
        and not umbraco_api_items
        and not mycareersfuture_items
        and not tuvsud_recruiting_api_items
        and not milchundzucker_gjb_items
        and not clinch_careers_site_items
        and not kentico_html_items
        and not wordpress_inline_modals_items
        and not wordpress_elementor_items
        and not wordpress_remix_items
        and not magnolia_nextjs_items
        and not krohne_nextjs_items
        and not kongsberg_optimizely_easycruit_items
        and not lr_episerver_api_items
        and not aem_workday_json_items
        and not carrier_html_items
        and not classnk_static_html_items
        and not aibel_html_hr_manager_items
        and not sitefinity_items
    ):
        raise RuntimeError(
            "No supported ATS companies found (check ats_new_norm in Excel + registry mappings)"
        )

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
    records = collector.map_to_records(res)
    # Always take company name from input (Excel) instead of any scraped/ATS-provided company field.
    input_company = (getattr(company, "company", None) or "").strip()
    if not input_company:
        return records
    return [replace(r, company=input_company) for r in records]

if __name__ == "__main__":
    main()

