from __future__ import annotations # For forward compatibility with future Python versions

from dataclasses import replace # replace is a method to create a copy of a dataclass with some fields changed
from datetime import date, datetime # for date parsing
import re
from typing import List, Optional

from src.core.models import JobRecord # JobRecord dataclass


def normalize_records(records: List[JobRecord]) -> List[JobRecord]:
    """ Z5: Normalize JobRecord fields (trim strings, normalize dates, locations, etc.) """
    
    normalized_records: List[JobRecord] = []
    
    # Process each record
    for r in records:
        normalized_records.append(
            replace(
                r,
                company=_s(r.company),
                job_title=_s(r.job_title),
                location=_norm_location(r.location),
                job_id=_s(r.job_id),
                posted_date=_norm_date(r.posted_date),
                job_url=_s(r.job_url),
                source=_s(r.source),
                careers_url=_s(r.careers_url),
            )
        )
    return normalized_records

def _s(x: Optional[object]) -> str:
    """Convert to trimmed string. None -> ''."""
    if x is None:
        return ""
    return _fix_mojibake(str(x)).strip()


def _mojibake_score(s: str) -> int:
    # Characters that commonly indicate mis-decoded UTF-8 (MacRoman/Latin-1/CP1252)
    # Example: "W√§rtsil√§" should become "Wärtsilä".
    suspects = ("√", "Ã", "Â", "�")
    return sum(s.count(ch) for ch in suspects)


def _fix_mojibake(s: str) -> str:
    """Best-effort fix for common mojibake caused by wrong decoding.

    We only apply a fix if it reduces the mojibake score.
    """
    if not s:
        return s

    base = s
    base_score = _mojibake_score(base)
    if base_score == 0:
        return base

    best = base
    best_score = base_score

    # Try common mistaken decodes (MacRoman is common on macOS, Latin-1/CP1252 common elsewhere)
    for wrong_enc in ("mac_roman", "latin-1", "cp1252"):
        try:
            b = base.encode(wrong_enc)
        except Exception:
            continue
        try:
            cand = b.decode("utf-8")
        except Exception:
            continue

        score = _mojibake_score(cand)
        if score < best_score:
            best = cand
            best_score = score

    return best

def _norm_date(x: Optional[object]) -> str:
    """Normalize date to YYYY-MM-DD if possible; else keep original string."""
    s = _s(x)
    if not s:
        return ""

    # Handle list-like string representations, e.g. "['2025-11-27']" or "[\"2025-11-27\"]"
    m_list = re.fullmatch(r"\s*\[\s*['\"]?(\d{4}-\d{2}-\d{2})['\"]?\s*\]\s*", s)
    if m_list:
        s = m_list.group(1)

    # Fast-path
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s

    s = s.strip()

    def _month_name_to_english(token: str) -> str:
        t = (token or "").strip().strip(".")
        if not t:
            return ""
        k = t.casefold()
        # German month abbreviations / names -> English (for strptime %b/%B)
        mapping = {
            "jan": "Jan",
            "januar": "January",
            "feb": "Feb",
            "februar": "February",
            "mär": "Mar",
            "maerz": "Mar",
            "märz": "March",
            "mar": "Mar",
            "april": "April",
            "apr": "Apr",
            "mai": "May",
            "may": "May",
            "jun": "Jun",
            "juni": "June",
            "jul": "Jul",
            "juli": "July",
            "aug": "Aug",
            "august": "August",
            "sep": "Sep",
            "sept": "Sep",
            "september": "September",
            "okt": "Oct",
            "oktober": "October",
            "oct": "Oct",
            "nov": "Nov",
            "november": "November",
            "dez": "Dec",
            "dezember": "December",
            "dec": "Dec",
            "december": "December",
        }
        return mapping.get(k, t)

    # Handle patterns like: "20. Nov 25" or "20. Nov. 25" (assume 2000-2099 for 2-digit years)
    m = re.fullmatch(r"\s*(\d{1,2})\.\s*([A-Za-zÄÖÜäöüß]+)\.?\s*(\d{2})\s*", s)
    if m:
        day = int(m.group(1))
        mon_token = _month_name_to_english(m.group(2))
        yy = int(m.group(3))
        year = 2000 + yy
        try:
            # Convert month token to month number via strptime
            mon = datetime.strptime(mon_token[:3], "%b").month
            return date(year, mon, day).isoformat()
        except ValueError:
            pass

    # Replace German month tokens inside the string (best-effort)
    def _replace_month_tokens(text: str) -> str:
        def repl(match: re.Match[str]) -> str:
            return _month_name_to_english(match.group(0))

        # Replace words that look like month names/abbrevs
        return re.sub(r"\b[A-Za-zÄÖÜäöüß]{3,9}\.?(?=\b)", repl, text)

    s_norm = _replace_month_tokens(s)

    # Common Oracle / ISO variants
    fmts = (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        # Human-readable (SuccessFactors / various sites)
        "%b %d, %Y",   # Dec 10, 2025
        "%B %d, %Y",   # December 10, 2025
        "%d %b %Y",    # 10 Dec 2025
        "%d %B %Y",    # 10 December 2025
        "%d.%m.%Y",    # 20.11.2025
        "%d.%m.%y",    # 20.11.25
        "%d. %b %Y",   # 20. Nov 2025
        "%d. %b %y",   # 20. Nov 25
        "%d. %B %Y",   # 20. November 2025
        "%d. %B %y",   # 20. November 25
    )
    for fmt in fmts:
        try:
            return datetime.strptime(s_norm, fmt).date().isoformat()
        except ValueError:
            pass
    try:
        s2 = s_norm.replace("Z", "+00:00")
        return datetime.fromisoformat(s2).date().isoformat()
    except ValueError:
        return s


def _norm_location(x: Optional[object]) -> str:
    """Trim + collapse whitespace."""
    s = _s(x)
    if not s:
        return ""

    raw = s.strip()
    if not raw:
        return ""

    # Clean common ATS UI artifacts / encoding issues
    raw = raw.replace("‚Ä¶", "...")
    raw = raw.replace("…", "...")
    raw = raw.replace("‚", " ")
    # Remove patterns like: "Tianjin, CN, 300450 +10 more..." / "MC +4 more..."
    raw = re.sub(r"\s*\+\s*\d+\s+more\b.*$", "", raw, flags=re.IGNORECASE).strip()

    # 1) If the source provided multiple lines, prefer the Singapore line.
    lines = [ln.strip() for ln in re.split(r"\r?\n+", raw) if ln.strip()]
    if len(lines) > 1:
        singapore_lines = [" ".join(ln.split()) for ln in lines if "singapore" in ln.casefold()]
        # If multiple Singapore entries exist (e.g., different postal codes), preserve all.
        if singapore_lines:
            seen_sg = set()
            uniq_sg: list[str] = []
            for ln in singapore_lines:
                if ln not in seen_sg:
                    seen_sg.add(ln)
                    uniq_sg.append(ln)
            return " | ".join(uniq_sg)
        return " ".join(lines[0].split())

    # 2) If multiple locations are embedded in one string (common in ATS exports),
    # try to extract location-like chunks and prefer the Singapore chunk.
    collapsed = " ".join(raw.split())
    collapsed = re.sub(r"^location\s*:\s*", "", collapsed, flags=re.IGNORECASE)

    # Match patterns like:
    # - "Singapore, SG, 629350"
    # - "Fos-sur-mer, FR, 13270"
    # - "Zhuhai, GD, CN, 519050" (region + country)
    # - "Georgetown, GY" (no postal)
    loc_re = re.compile(
        r"([A-Za-z0-9][A-Za-z0-9\-\s'\.]*?,\s*[A-Z]{2,3}(?:,\s*[A-Z]{2,3}){0,2}(?:,\s*\d{4,6})?)"
    )
    chunks = [m.group(1).strip() for m in loc_re.finditer(collapsed)]

    # Deduplicate while preserving order
    seen = set()
    uniq: list[str] = []
    for c in chunks:
        if c not in seen:
            seen.add(c)
            uniq.append(c)

    if len(uniq) > 1:
        singapore_chunks = [" ".join(c.split()) for c in uniq if "singapore" in c.casefold()]
        if singapore_chunks:
            seen_sg = set()
            uniq_sg: list[str] = []
            for c in singapore_chunks:
                if c not in seen_sg:
                    seen_sg.add(c)
                    uniq_sg.append(c)
            return " | ".join(uniq_sg)
        return " ".join(uniq[0].split())

    # 3) Default: collapse whitespace.
    return " ".join(collapsed.split())
