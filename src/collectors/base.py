from __future__ import annotations # For forward compatibility with future Python versions

from abc import ABC, abstractmethod # For defining abstract base classes
from typing import Any, Dict, List

from src.core.models import CompanyItem, CollectResult, JobRecord


class BaseCollector(ABC):
    """ Abstract base class for all collectors."""
    name: str

    @abstractmethod
    def collect_raw(self, company: CompanyItem) -> CollectResult:
        """Z3: fetch raw jobs + meta. No parsing/mapping."""
        raise NotImplementedError
    @abstractmethod
    def map_to_records(self, result: CollectResult) -> List[JobRecord]:
        """Z4: raw -> JobRecord list (ATS-specific mapping)."""
        raise NotImplementedError

    # no abstractmethod because this has a default implementation
    def collect(self, company: CompanyItem) -> List[JobRecord]:
        """ Complete collection: raw fetch + mapping to JobRecord."""
        res = self.collect_raw(company)
        if res.error:
            return []
        return self.map_to_records(res)
    
    def _pick(self, raw: Dict[str, Any], *keys: str, default: str = "") -> str:
        """
        raw = {
            "Title": "Software Engineer",
            "Id": 123
            ...
        }

        Pick the first non-empty value from raw for the given keys.

        raw.get("Title") -> "Software Engineer"
        """
        for k in keys:
            v = raw.get(k)
            # Some ATS payloads use JSON booleans for missing fields (e.g. "externalUrl": false).
            # Treat booleans as missing to avoid exporting literal "False"/"True" strings.
            if v is None or v == "" or isinstance(v, bool):
                continue
            if v not in (None, ""):
                return str(v)
        return default