import csv
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests


# =========================
# CONFIG (WORLEY / EIGHTFOLD PCS)
# =========================

COMPANY = "Worley"
SOURCE = "eightfold"

CAREERS_URL = (
    "https://jobs.worley.com/careers"
    "?start=0&location=Singapore&pid=1133910430521&sort_by=distance&filter_include_remote=1"
)

DOMAIN = "worley.com"          # important for PCS APIs
HL = "de"
QUERIED_LOCATION = "Singapore"

MAX_PAGES = 200
SLEEP_S = 0.05

DETAIL_WORKERS = 10
DETAIL_TIMEOUT_S = 30
SEARCH_TIMEOUT_S = 30


# =========================
# HELPERS
# =========================

def extract_pid_from_careers_url(careers_url: str) -> str | None:
    q = parse_qs(urlparse(careers_url).query)
    v = q.get("pid", [None])[0]
    return str(v) if v not in (None, "") else None


def extract_location_from_careers_url(careers_url: str) -> str:
    q = parse_qs(urlparse(careers_url).query)
    v = q.get("location", [None])[0]
    v = str(v) if v not in (None, "") else ""
    return v or QUERIED_LOCATION


def api_base(careers_url: str) -> str:
    u = urlparse(careers_url)
    return f"{u.scheme}://{u.netloc}"


PID = extract_pid_from_careers_url(CAREERS_URL)
LOCATION = extract_location_from_careers_url(CAREERS_URL)

SEARCH_API_BASE = f"{api_base(CAREERS_URL)}/api/pcsx/search"
DETAIL_API_BASE = f"{api_base(CAREERS_URL)}/api/pcsx/position_details"


def build_search_api_url(start: int) -> str:
    params = {
        "domain": DOMAIN,
        "query": "",
        "location": LOCATION,
        "start": start,
        "sort_by": "distance",
        "filter_include_remote": "1",
    }
    if PID:
        params["pid"] = PID
    return f"{SEARCH_API_BASE}?{urlencode(params)}"


def build_detail_api_url(position_id: str) -> str:
    params = {
        "position_id": position_id,
        "domain": DOMAIN,
        "hl": HL,
        "queried_location": LOCATION,
    }
    return f"{DETAIL_API_BASE}?{urlencode(params)}"


def find_first_list_of_dicts(obj):
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            return obj
        for x in obj:
            res = find_first_list_of_dicts(x)
            if res is not None:
                return res
        return None

    if isinstance(obj, dict):
        for k in ("positions", "jobs", "results", "items", "data", "searchResults", "search_results"):
            if k in obj:
                res = find_first_list_of_dicts(obj[k])
                if res is not None:
                    return res
        for v in obj.values():
            res = find_first_list_of_dicts(v)
            if res is not None:
                return res

    return None


def pick(d: dict, keys: list[str], default=None):
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return default


def clean_text(s: str | None) -> str:
    return " ".join((s or "").split()).strip()


def normalize_location(job: dict) -> str:
    loc = pick(job, ["location", "locations", "jobLocation", "job_location"])
    if isinstance(loc, str):
        return clean_text(loc)
    if isinstance(loc, list) and loc:
        first = loc[0]
        if isinstance(first, str):
            return clean_text(first)
        if isinstance(first, dict):
            return clean_text(str(pick(first, ["name", "displayName", "label"], "")))
    if isinstance(loc, dict):
        return clean_text(str(pick(loc, ["name", "displayName", "label"], "")))
    return ""


def job_url_from_id(job_id: str) -> str:
    u = urlparse(CAREERS_URL)
    return f"{u.scheme}://{u.netloc}/careers/job/{job_id}"


def posted_date_from_posted_ts(posted_ts) -> str | None:
    if posted_ts is None:
        return None
    try:
        v = float(posted_ts)
        if v > 10_000_000_000:  # likely ms
            v = v / 1000.0
        dt = datetime.fromtimestamp(v, tz=timezone.utc)
        if dt.year == 1970:
            return None
        return dt.date().isoformat()
    except Exception:
        return None


def fetch_posted_date(session: requests.Session, job_id: str) -> tuple[str, str | None]:
    url = build_detail_api_url(job_id)
    r = session.get(url, timeout=DETAIL_TIMEOUT_S)
    r.raise_for_status()
    details = r.json()
    posted_ts = None
    if isinstance(details, dict):
        data = details.get("data")
        if isinstance(data, dict):
            posted_ts = data.get("postedTs")
    return job_id, posted_date_from_posted_ts(posted_ts)


# =========================
# SCRAPER
# =========================

def scrape() -> list[dict]:
    session = requests.Session()
    session.headers.update(
        {
            "accept": "application/json, text/plain, */*",
            "user-agent": "Mozilla/5.0",
            "referer": CAREERS_URL,
        }
    )

    # 1) Search pages
    results: list[dict] = []
    start = 0

    for _ in range(MAX_PAGES):
        url = build_search_api_url(start)
        r = session.get(url, timeout=SEARCH_TIMEOUT_S)
        r.raise_for_status()
        data = r.json()

        jobs_raw = find_first_list_of_dicts(data) or []
        if not jobs_raw:
            break

        for j in jobs_raw:
            job_id = pick(j, ["id", "jobId", "job_id", "reqId", "requisitionId", "requisition_id"])
            job_id = str(job_id) if job_id not in (None, "") else ""

            results.append(
                {
                    "company": COMPANY,
                    "job_title": clean_text(
                        str(
                            pick(
                                j,
                                ["title", "jobTitle", "job_title", "name", "positionTitle", "position_title"],
                                "",
                            )
                        )
                    ),
                    "location": normalize_location(j),
                    "job_id": job_id,
                    "posted_date": None,
                    "job_url": job_url_from_id(job_id) if job_id else "",
                    "source": SOURCE,
                    "careers_url": CAREERS_URL,
                }
            )

        # heuristic: stop when last page
        if len(jobs_raw) < 10:
            break

        start += len(jobs_raw)
        time.sleep(SLEEP_S)

    # Dedupe by job_id
    deduped: list[dict] = []
    seen: set[str] = set()
    for item in results:
        jid = item.get("job_id") or ""
        if not jid or jid in seen:
            continue
        seen.add(jid)
        deduped.append(item)

    # 2) Enrich posted_date (parallel)
    id_to_date: dict[str, str | None] = {}
    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
        futures = [ex.submit(fetch_posted_date, session, item["job_id"]) for item in deduped if item.get("job_id")]
        for fut in as_completed(futures):
            try:
                job_id, posted_date = fut.result()
                id_to_date[job_id] = posted_date
            except Exception:
                continue

    for item in deduped:
        jid = item.get("job_id") or ""
        item["posted_date"] = id_to_date.get(jid)

    return deduped


# =========================
# OUTPUT
# =========================

def out_dir() -> Path:
    base = Path(__file__).resolve()
    project_out = base.parent.parent / "data" / "output"
    test_out = base.parent / "data" / "output"
    d = project_out if project_out.exists() else test_out
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_csv(path: Path, rows: list[dict]) -> None:
    fieldnames = ["company", "job_title", "location", "job_url", "job_id", "posted_date", "source", "careers_url"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    jobs = scrape()
    print(f"Found {len(jobs)} jobs")

    d = out_dir()
    csv_path = d / "worley_jobs.csv"
    json_path = d / "worley_jobs.json"

    write_csv(csv_path, jobs)
    write_json(json_path, {"count": len(jobs), "jobs": jobs})

    print(f"Wrote: {csv_path}")
    print(f"Wrote: {json_path}")

    for j in jobs[:3]:
        print(j)


if __name__ == "__main__":
    main()