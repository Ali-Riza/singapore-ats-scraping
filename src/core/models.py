from dataclasses import dataclass
from typing import Optional, Dict, Any, List

# Shared data models used across the pipeline (Z1-Z8)

@dataclass
class CompanyItem:
    raw_data_row:Dict[str, Any] # The raw data row from which this item was created
    company:str
    careers_url:str
    ats_type:Optional[str]
    category:Optional[str]
    website: Optional[str]
    row_number:int

@dataclass
class CollectResult:
    """CollectResult = unfiltered raw data collected from the ATS (Z3 output)."""
    collector: str
    company: str
    careers_url: str
    raw_jobs: List[Dict[str, Any]] # ATS-specific raw job data
    meta: Dict[str, Any] # e.g., pages fetched, status codes, timings
    error: Optional[str] = None # error message if collection failed

@dataclass
class JobRecord:
    """ Standardized job record after parsing/mapping/translating (Z4 output). 
        Problem e.g.:
        
        Oracle raw
        {
        "requisitionNumber": "JR-123",
        "postingDate": "2025-12-01T10:23:00Z",
        "requisitionTitle": "Data Engineer"
        ...
        }

        Workday raw
        {
        "jobReqId": "98765",
        "postedOn": "12/01/2025",
        "title": "Data Engineer"
        ...
        }

        After parsing/mapping/translating to JobRecord:

        Oracle standardized
        {
        company="ACME Corp",
        job_title="Senior Data Engineer",
        location="Singapore, Singapore",
        job_id="JR-2025-0421",
        posted_date="2025-12-01T10:23:00Z",
        job_url="https://careers.company.com/job/JR-2025-0421",
        source="oracle",
        careers_url="https://careers.company.com/hcmUI/CandidateExperience/en/sites/CX_2001/jobs",
        raw={...}  # original raw job dict
        }
    """
    company: str
    job_title: str
    location: str
    job_id: str
    posted_date: str
    job_url: str

    source: str = ""          
    careers_url: str = ""     
    raw: Optional[Dict[str, Any]] = None 