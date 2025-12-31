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
    if ats == "htmlpagedsearch":
        return "htmlpagedsearch"
    if ats == "siemens_searchjobs":
        # Legacy alias (kept for backwards compatibility with existing Excel values)
        return "htmlpagedsearch"

    return "skip"