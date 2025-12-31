import re
import requests
import pandas as pd

ATS_SIGNATURES = {
    "workday": [
        r"myworkdayjobs\.com",
        r"wd[0-9]+\.myworkdayjobs",
        r"/wday/cxs/",
        r"myworkdayjobs\.com",
    r"myworkdaysite\.com",
    r"/wday/cxs/",
    ],
    "oracle_hcm": [
        r"oraclecloud\.com",
        r"hcmUI/CandidateExperience",
        r"hcmRestApi",
    ],
    "successfactors": [
        r"jobs\.siemens\.com",
        r"successfactors",
        r"SearchJobs",
        r"successfactors",
    r"/SearchJobs",
    r"/jobs/search",
    r"/careers/search",
    ],
    "eightfold": [r"eightfold\.ai"],
    "cornerstone": [r"csod\.com", r"cornerstoneondemand"],
    "tribepad": [r"tribepad\.com"],
    "greenhouse": [r"greenhouse\.io", r"boards\.greenhouse\.io"],
    "lever": [r"lever\.co"],
    "smartrecruiters": [r"smartrecruiters\.com"],
    "icims": [r"icims\.com"],
    "avature": [r"avature\.net"],
    "phenom": [
    r"phenompeople\.com",
    r"phenom",
    r"/jobs/search",
],
    "taleo": [r"taleo\.net", r"/careersection/"],
    "jobvite": [r"jobvite\.com", r"jobs\.jobvite\.com"],
    "bamboohr": [r"bamboohr\.com"],
    "breezy": [r"breezy\.hr"],
    "recruitee": [r"recruitee\.com"],
    "teamtailor": [r"teamtailor\.com"],
    "workable": [r"workable\.com", r"apply\.workable\.com"],
    "ashby": [r"ashbyhq\.com"],
    "fountain": [r"fountain\.com"],
    "jazzhr": [r"jazz\.co", r"jazzhr\.com"],
    "mysap": [r"mysap\.com", r"job\.sap\.com"],
    "bullhorn": [r"bullhornstaffing\.com"],
    "peoplehr": [r"peoplehr\.com"],
    "personio": [r"personio\.de", r"personio\.com"],
    "hibob": [r"hibob\.com",]
}

HEADERS = {"User-Agent": "Mozilla/5.0 ATS-Fingerprint/1.0"}

def classify_by_url(url: str):
    for ats, patterns in ATS_SIGNATURES.items():
        for p in patterns:
            if re.search(p, url, re.IGNORECASE):
                return ats, "high", f"url-regex:{p}"
    return None

def classify_by_html(url: str, timeout=10):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)

        if r.status_code in (401, 403):
            return "blocked", "low", f"http:{r.status_code}"
        if r.status_code == 429:
            return "rate_limited", "low", "http:429"
        if r.status_code >= 400:
            return "unreachable", "low", f"http:{r.status_code}"

        ctype = (r.headers.get("content-type") or "").lower()
        if "text/html" not in ctype and "application/xhtml+xml" not in ctype:
            return "non_html", "low", f"content-type:{ctype[:80]}"

        html = (r.text or "")[:300_000]
        for ats, patterns in ATS_SIGNATURES.items():
            for p in patterns:
                if re.search(p, html, re.IGNORECASE):
                    return ats, "medium", f"html-regex:{p}"

    except requests.RequestException as e:
        return "unreachable", "low", f"requests:{type(e).__name__}"

    return None

def classify_url(url):
    if not isinstance(url, str) or not url.strip():
        return "empty", "low", "empty-cell"
    url = url.strip().strip('"')

    hit = classify_by_url(url)
    if hit:
        return hit

    hit = classify_by_html(url)
    if hit:
        return hit

    return "unknown", "low", "no-match"

def normalize_ats(x: object) -> str:
    """Normalize old/new ATS labels so comparisons are stable."""
    if x is None:
        return ""
    s = str(x).strip().lower()
    if s in ("", "nan", "none", "null"):
        return ""

    # Optional: wenn master_companies andere Namen nutzt als deine ATS-Families
    aliases = {
        "oracle": "oracle",
        "oracle hcm": "oracle",
        "oracle_hcm": "oracle",
        "sf": "successfactors",
        "success factors": "successfactors",
        "successfactors": "successfactors",
        "workday": "workday",
        "green house": "greenhouse",
        "greenhouse.io": "greenhouse",
    }
    return aliases.get(s, s)

def main():
    from pathlib import Path
    import sys
    import pandas as pd

    BASE_DIR = Path(__file__).resolve().parent  # tests/
    # Prefer project-level data/output if tests/data/output is missing
    candidate_paths = [
        BASE_DIR / "data" / "input" / "master_companies.xlsx",
        BASE_DIR.parent / "data" / "input" / "master_companies.xlsx",
    ]
    EXCEL_PATH = next((p for p in candidate_paths if p.exists()), candidate_paths[0])

    if not EXCEL_PATH.exists():
        print(f"Input Excel not found: {EXCEL_PATH}")
        print("Checked also:", candidate_paths)
        sys.exit(1)

    URL_COL = "Jobs Page (Singapore)"
    OLD_ATS_COL = "ATS_Type"

    df = pd.read_excel(EXCEL_PATH)

    # Schritt 2: neu klassifizieren und ins Master-DF schreiben
    new_ats, new_conf, new_evid = [], [], []
    for url in df[URL_COL].tolist():
        ats, confidence, evidence = classify_url(url)
        new_ats.append(ats)
        new_conf.append(confidence)
        new_evid.append(evidence)

    df["ats_family_new"] = new_ats
    df["confidence_new"] = new_conf
    df["evidence_new"] = new_evid

    # Schritt 3: Normalisieren + Mismatch berechnen
    df["ats_family_old"] = df[OLD_ATS_COL]
    df["ats_old_norm"] = df["ats_family_old"].apply(normalize_ats)
    df["ats_new_norm"] = df["ats_family_new"].apply(normalize_ats)

    df["is_mismatch"] = df["ats_old_norm"] != df["ats_new_norm"]

    # Mismatch-Liste (nur Konflikte)
    mismatches = df[df["is_mismatch"]].copy()

    # Needs-action-Liste (unsichere/technische Fälle)
    needs_action_statuses = {"unknown", "blocked", "non_html", "unreachable", "rate_limited", "empty"}
    needs_action = df[df["ats_family_new"].isin(needs_action_statuses)].copy()

    # Optional: "echte" Mismatches (ignoriert unknown etc.)
    true_mismatches = mismatches[~mismatches["ats_family_new"].isin(needs_action_statuses)].copy()

    # Summary
    summary_counts = (
        df["ats_family_new"]
        .value_counts(dropna=False)
        .rename_axis("ats_family_new")
        .reset_index(name="count")
    )

    top_changes = (
        mismatches.groupby(["ats_old_norm", "ats_new_norm"])
        .size()
        .sort_values(ascending=False)
        .head(50)
        .reset_index(name="count")
    )

    # Outputs
    out_dir = (BASE_DIR.parent / "data" / "output") if (BASE_DIR.parent / "data" / "output").exists() else (BASE_DIR / "data" / "output")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_excel = out_dir / "master_companies_with_fingerprint.xlsx"
    with pd.ExcelWriter(out_excel, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="master_with_fingerprint")
        mismatches.to_excel(writer, index=False, sheet_name="mismatches_all")
        true_mismatches.to_excel(writer, index=False, sheet_name="mismatches_true")
        needs_action.to_excel(writer, index=False, sheet_name="needs_action")
        summary_counts.to_excel(writer, index=False, sheet_name="summary_counts")
        top_changes.to_excel(writer, index=False, sheet_name="top_old_to_new")

    print("Written:", out_excel)
    print("\nNew ATS counts:\n", df["ats_family_new"].value_counts(dropna=False))
    print("\nMismatch rows:", len(mismatches))
    print("True mismatches (excluding unknown/blocked/etc.):", len(true_mismatches))
    print("Needs action:", len(needs_action))

if __name__ == "__main__":
    main()