import json
import urllib.parse
from datetime import datetime
import requests

ALGOLIA_APP_ID = "UM59DWRPA1"
ALGOLIA_API_KEY = "33719eb8d9f28725f375583b7e78dbab"
INDEX_NAME = "production_JCI_jobs"

CAREERS_URL = "https://jobs.johnsoncontrols.com"
SOURCE = "johnsoncontrols"
COMPANY = "Johnson Controls"

ALGOLIA_ENDPOINT = (
    f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes/*/queries"
    f"?x-algolia-application-id={ALGOLIA_APP_ID}"
    f"&x-algolia-api-key={ALGOLIA_API_KEY}"
)

HEADERS = {
    "Accept": "*/*",
    "Origin": CAREERS_URL,
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0",
}

# Das ist dein params-String aus der curl — wir ersetzen nur page=...
BASE_PARAMS = (
    "facetFilters=%5B%5B%22locations_list%3ASingapore%22%5D%5D"
    "&facets=%5B%22employee_type%22%2C%22job_family_group%22%2C%22locations_list%22%2C%22parent_category%22%5D"
    "&highlightPostTag=__%2Fais-highlight__"
    "&highlightPreTag=__ais-highlight__"
    "&maxValuesPerFacet=50"
    "&page={page}"
    "&query="
)

SECOND_QUERY_PARAMS = (
    "analytics=false&clickAnalytics=false"
    "&facets=locations_list"
    "&highlightPostTag=__%2Fais-highlight__"
    "&highlightPreTag=__ais-highlight__"
    "&hitsPerPage=0"
    "&maxValuesPerFacet=50"
    "&page=0"
    "&query="
)

def _pick(d: dict, keys: list[str]):
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return None

def _parse_posted_date(hit: dict):
    # Algolia / SmartDreamers kann unterschiedliche Felder liefern – wir probieren mehrere.
    raw = _pick(hit, ["posted_date", "postedDate", "date", "created_at", "createdAt", "first_seen", "published_at"])
    if raw is None:
        return None

    # Manche liefern ISO-Strings, manche Unix timestamps, manche "YYYY-MM-DD"
    if isinstance(raw, (int, float)):
        try:
            return datetime.utcfromtimestamp(raw).date().isoformat()
        except Exception:
            return str(raw)

    s = str(raw).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return s  # fallback unverändert

def _build_job_url(hit: dict) -> str | None:
    # Häufige Kandidaten:
    url = _pick(hit, ["job_url", "jobUrl", "url", "apply_url", "applyUrl", "external_url", "externalUrl"])
    if url:
        # manchmal kommt relativ
        if isinstance(url, str) and url.startswith("/"):
            return CAREERS_URL.rstrip("/") + url
        return str(url)

    slug = _pick(hit, ["slug", "job_slug", "jobSlug"])
    if slug:
        # Manche Systeme nutzen /job/<slug> – wenn’s bei dir anders ist, passt du das gleich an.
        if str(slug).startswith("http"):
            return str(slug)
        return f"{CAREERS_URL.rstrip('/')}/job/{str(slug).lstrip('/')}"

    return None

def _build_payload(page: int) -> str:
    data = {
        "requests": [
            {
                "indexName": INDEX_NAME,
                "params": BASE_PARAMS.format(page=page),
            },
            {
                "indexName": INDEX_NAME,
                "params": SECOND_QUERY_PARAMS,
            },
        ]
    }
    # Wichtig: curl sendet JSON als string in form-urlencoded body
    return json.dumps(data, separators=(",", ":"))

def scrape_jobs(max_pages: int = 50):
    session = requests.Session()

    for page in range(max_pages):
        body = _build_payload(page)
        resp = session.post(ALGOLIA_ENDPOINT, headers=HEADERS, data=body, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        results = data.get("results") or []
        if not results:
            break

        hits = results[0].get("hits") or []
        if not hits:
            break

        for hit in hits:
            job_id = _pick(hit, ["job_id", "jobId", "requisition_id", "requisitionId", "req_id", "id", "objectID"])
            title = _pick(hit, ["job_title", "jobTitle", "title", "name", "position_title"])
            location = _pick(hit, ["location", "locations", "locations_list", "locationsList", "city", "location_name"])

            # locations_list kann eine Liste sein -> in String umwandeln
            if isinstance(location, list):
                location = ", ".join([str(x) for x in location if x])

            out = {
                "company": COMPANY,
                "job_title": title,
                "location": location,
                "job_id": str(job_id) if job_id is not None else None,
                "posted_date": _parse_posted_date(hit),
                "job_url": _build_job_url(hit),
                "source": SOURCE,
                "careers_url": CAREERS_URL,
            }
            yield out

    session.close()

if __name__ == "__main__":
    count = 0
    for job in scrape_jobs(max_pages=100):
        print(job)
        count += 1
    print(f"\nTotal: {count}")