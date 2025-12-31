import time
from datetime import datetime, timezone
from urllib.parse import urlencode, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


CAREERS_URL = "https://eaton.eightfold.ai/careers?start=0&location=Singapore&pid=687233727270&sort_by=distance&filter_include_remote=1"

SEARCH_API_BASE = "https://eaton.eightfold.ai/api/pcsx/search"
DETAIL_API_BASE = "https://eaton.eightfold.ai/api/pcsx/position_details"

COMPANY = "Eaton"
SOURCE = "eightfold"

DOMAIN = "eaton.com"
HL = "de"
QUERIED_LOCATION = "Singapore"

MAX_PAGES = 200
SLEEP_S = 0.05

# Speed knobs
DETAIL_WORKERS = 10          # 5-15 ist meist gut
DETAIL_TIMEOUT_S = 30
SEARCH_TIMEOUT_S = 30


def extract_pid_from_careers_url(careers_url: str) -> str | None:
    q = parse_qs(urlparse(careers_url).query)
    return q.get("pid", [None])[0]


PID = extract_pid_from_careers_url(CAREERS_URL)


def build_search_api_url(start: int) -> str:
    params = {
        "domain": DOMAIN,
        "query": "",
        "location": QUERIED_LOCATION,
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
        "queried_location": QUERIED_LOCATION,
    }
    return f"{DETAIL_API_BASE}?{urlencode(params)}"


def find_first_list_of_dicts(obj):
    # minimaler, aber ausreichend toleranter Finder
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


def normalize_location(job: dict) -> str | None:
    loc = pick(job, ["location", "locations", "jobLocation", "job_location"])
    if isinstance(loc, str):
        return loc.strip()
    if isinstance(loc, list) and loc:
        first = loc[0]
        if isinstance(first, str):
            return first.strip()
        if isinstance(first, dict):
            return pick(first, ["name", "displayName", "label"])
    if isinstance(loc, dict):
        return pick(loc, ["name", "displayName", "label"])
    return None


def job_url_from_id(job_id: str) -> str:
    return f"https://eaton.eightfold.ai/careers/job/{job_id}"


def posted_date_from_postedTs(posted_ts) -> str | None:
    if posted_ts is None:
        return None
    # postedTs ist sehr wahrscheinlich epoch ms
    try:
        v = float(posted_ts)
        if v > 10_000_000_000:  # ms
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
    # Wir wissen jetzt: data.postedTs
    posted_ts = None
    if isinstance(details, dict):
        data = details.get("data")
        if isinstance(data, dict):
            posted_ts = data.get("postedTs")
    return job_id, posted_date_from_postedTs(posted_ts)


def scrape() -> list[dict]:
    session = requests.Session()
    session.headers.update({
        "accept": "application/json, text/plain, */*",
        "user-agent": "Mozilla/5.0",
        "referer": CAREERS_URL,
    })

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
            job_id = str(job_id) if job_id is not None else None

            results.append({
                "company": COMPANY,
                "job_title": pick(j, ["title", "jobTitle", "job_title", "name", "positionTitle", "position_title"]),
                "location": normalize_location(j),
                "job_id": job_id,
                "posted_date": None,  # kommt gleich
                "job_url": job_url_from_id(job_id) if job_id else None,
                "source": SOURCE,
                "careers_url": CAREERS_URL,
            })

        # stop if last page
        if len(jobs_raw) < 10:
            break

        start += len(jobs_raw)
        time.sleep(SLEEP_S)

    # Dedupe by job_id
    deduped = []
    seen = set()
    for item in results:
        jid = item.get("job_id")
        if not jid:
            continue
        if jid in seen:
            continue
        seen.add(jid)
        deduped.append(item)

    # 2) Enrich posted_date (parallel)
    id_to_date: dict[str, str | None] = {}
    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
        futures = [ex.submit(fetch_posted_date, session, item["job_id"]) for item in deduped if item.get("job_id")]
        for fut in as_completed(futures):
            job_id, posted_date = fut.result()
            id_to_date[job_id] = posted_date

    for item in deduped:
        jid = item.get("job_id")
        item["posted_date"] = id_to_date.get(jid)

    return deduped


if __name__ == "__main__":
    jobs = scrape()
    print(f"Found {len(jobs)} jobs")
    for j in jobs[:3]:
        print(j)