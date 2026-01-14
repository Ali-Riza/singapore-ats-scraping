from __future__ import annotations # For forward compatibility with future Python versions
from typing import Dict, List

from src.core.models import CompanyItem

def pick_collector(item: CompanyItem) -> str:
    """ Pick collector type based on Company's ATS type."""
    ats = (item.ats_type or "").strip().lower()
    # Strict matching: do not use substring heuristics.
    # Keep the input data (ats_type) canonicalized to one of these values.
    if ats == "oracle":
        return "oracle"
    if ats == "workday":
        return "workday"
    if ats == "phenom":
        return "phenom"
    if ats == "successfactors":
        return "successfactors"
    if ats == "tribepad":
        return "tribepad"
    if ats == "eightfold":
        return "eightfold"
    if ats == "algolia":
        return "algolia"
    if ats == "cornerstone":
        return "cornerstone"
    if ats == "embeddedstate":
        return "embeddedstate"
    if ats == "jibe_api_jobs":
        return "jibe_api_jobs"
    if ats == "htmlpagedsearch":
        return "htmlpagedsearch"
    if ats == "siemens_searchjobs":
        # Legacy alias (kept for backwards compatibility with existing Excel values)
        return "htmlpagedsearch"

    # Additional collectors (batch2 expansion)
    if ats == "hibob":
        return "hibob"
    if ats == "successfactors2":
        # Legacy alias seen in the current Excel input.
        # Keep routing strict: treat it as SuccessFactors unless the Excel is updated.
        return "successfactors"
    if ats == "jobsyn_solr":
        return "jobsyn_solr"
    if ats == "avature":
        return "avature"
    if ats == "breezy_portal":
        return "breezy_portal"
    if ats == "umbraco_api":
        return "umbraco_api"
    if ats == "mycareersfuture":
        return "mycareersfuture"
    if ats == "tuvsud_recruiting_api":
        return "tuvsud_recruiting_api"
    if ats == "milchundzucker_gjb":
        return "milchundzucker_gjb"
    if ats == "clinch_careers_site":
        return "clinch_careers_site"
    if ats == "kentico_html":
        return "kentico_html"
    if ats == "wordpress_inline_modals":
        return "wordpress_inline_modals"
    if ats == "wordpress_elementor":
        return "wordpress_elementor"
    if ats == "wordpress_remix":
        return "wordpress_remix"
    if ats == "magnolia_nextjs":
        return "magnolia_nextjs"
    if ats == "krohne_nextjs":
        return "krohne_nextjs"
    if ats == "kongsberg_optimizely_easycruit":
        return "kongsberg_optimizely_easycruit"
    if ats == "lr_episerver_api":
        return "lr_episerver_api"
    if ats == "aem_workday_json":
        return "aem_workday_json"
    if ats == "carrier_html":
        return "carrier_html"
    if ats == "classnk_static_html":
        return "classnk_static_html"
    if ats == "aibel_html_hr_manager":
        return "aibel_html_hr_manager"
    if ats == "sitefinity":
        return "sitefinity"

    return "skip"