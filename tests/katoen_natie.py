import json
import re
import sys
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.error import HTTPError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen


SOURCE_URL = "https://www.careers-page.com/katoennatiesingapore"
API_URL = (
    "https://www.careers-page.com/"
    "api/v1.0/c/katoennatiesingapore/jobs/"
)
EXPECTED_HOST = "www.careers-page.com"
CLIENT_SLUG = "katoennatiesingapore"

EMPLOYER = "Katoen Natie Singapore (Jurong) Pte Ltd"

ACCEPTED_HIRING_NAMES = {
    "katoen natie singapore",
    "katoen natie singapore (jurong) pte ltd",
}

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/json,"
        "application/xhtml+xml;q=0.9,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


class JsonLdParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.documents = []
        self._inside_json_ld = False
        self._buffer = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "script":
            return

        attributes = {
            str(key).lower(): value
            for key, value in attrs
        }

        script_type = (
            attributes.get("type") or ""
        ).lower()

        if script_type == "application/ld+json":
            self._inside_json_ld = True
            self._buffer = []

    def handle_data(self, data):
        if self._inside_json_ld:
            self._buffer.append(data)

    def handle_endtag(self, tag):
        if (
            tag.lower() == "script"
            and self._inside_json_ld
        ):
            content = "".join(self._buffer).strip()

            if content:
                try:
                    self.documents.append(
                        json.loads(content)
                    )
                except json.JSONDecodeError:
                    pass

            self._inside_json_ld = False
            self._buffer = []


class HtmlTextParser(HTMLParser):
    BLOCK_TAGS = {
        "address",
        "article",
        "blockquote",
        "br",
        "div",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "hr",
        "p",
        "section",
        "table",
        "tr",
        "ul",
        "ol",
    }

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()

        if tag == "li":
            self.parts.append("\n· ")
        elif tag in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag.lower() in self.BLOCK_TAGS | {"li"}:
            self.parts.append("\n")

    def handle_data(self, data):
        if data:
            self.parts.append(data)

    def text(self):
        raw = "".join(self.parts)
        lines = []

        for line in raw.splitlines():
            normalized = " ".join(
                line.replace("\xa0", " ").split()
            )

            if normalized:
                lines.append(normalized)

        return "\n".join(lines).strip()


def clean(value):
    if value is None:
        return None

    result = " ".join(
        str(value).replace("\xa0", " ").split()
    )

    return result or None


def utc_now():
    return (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def iso_date(value):
    value = clean(value)

    if not value:
        return None

    match = re.match(
        r"^(\d{4}-\d{2}-\d{2})",
        value,
    )

    return match.group(1) if match else None


def html_to_text(html):
    if not html:
        return None

    parser = HtmlTextParser()
    parser.feed(str(html))
    parser.close()

    return parser.text() or None


def validate_url(url):
    parsed = urlparse(url)

    if (
        parsed.scheme != "https"
        or parsed.hostname != EXPECTED_HOST
    ):
        raise RuntimeError(
            f"Unexpected URL: {url}"
        )


def fetch_text(url):
    validate_url(url)

    request = Request(
        url,
        headers={
            **REQUEST_HEADERS,
            "Accept": "text/html,*/*;q=0.8",
        },
    )

    with urlopen(request, timeout=60) as response:
        charset = (
            response.headers.get_content_charset()
            or "utf-8"
        )

        return response.read().decode(
            charset,
            errors="replace",
        )


def fetch_json(url):
    validate_url(url)

    request = Request(
        url,
        headers={
            **REQUEST_HEADERS,
            "Accept": "application/json",
        },
    )

    with urlopen(request, timeout=60) as response:
        raw = response.read()

    return json.loads(raw)


def find_job_postings(value):
    postings = []

    if isinstance(value, list):
        for item in value:
            postings.extend(
                find_job_postings(item)
            )

        return postings

    if not isinstance(value, dict):
        return postings

    schema_type = value.get("@type")

    if (
        schema_type == "JobPosting"
        or (
            isinstance(schema_type, list)
            and "JobPosting" in schema_type
        )
    ):
        postings.append(value)

    for child in value.values():
        if isinstance(child, (dict, list)):
            postings.extend(
                find_job_postings(child)
            )

    return postings


def extract_job_schema(html):
    parser = JsonLdParser()
    parser.feed(html)
    parser.close()

    for document in parser.documents:
        postings = find_job_postings(document)

        if postings:
            return postings[0]

    return None


def normalize_employment_type(value):
    if isinstance(value, list):
        normalized = [
            normalize_employment_type(item)
            for item in value
        ]

        return ", ".join(
            item for item in normalized if item
        ) or None

    value = clean(value)

    if not value:
        return None

    return value.replace("_", " ").capitalize()


def extract_experience(value):
    if isinstance(value, list):
        values = [
            extract_experience(item)
            for item in value
        ]

        return ", ".join(
            item for item in values if item
        ) or None

    if isinstance(value, dict):
        for key in (
            "name",
            "value",
            "description",
            "monthsOfExperience",
        ):
            result = clean(value.get(key))

            if result:
                return result

        return None

    return clean(value)


def extract_location(job_schema):
    locations = job_schema.get("jobLocation")

    if not locations:
        return None

    if not isinstance(locations, list):
        locations = [locations]

    parts = []

    for location in locations:
        if not isinstance(location, dict):
            continue

        address = location.get("address") or {}

        if not isinstance(address, dict):
            continue

        for key in (
            "addressLocality",
            "addressRegion",
            "addressCountry",
        ):
            value = address.get(key)

            if isinstance(value, dict):
                value = (
                    value.get("name")
                    or value.get("value")
                )

            value = clean(value)

            if value:
                parts.append(value)

    unique_parts = []
    seen = set()

    for part in parts:
        identifier = part.casefold()

        if identifier not in seen:
            seen.add(identifier)
            unique_parts.append(part)

    return ", ".join(unique_parts) or None


def extract_hashes_from_html(html):
    pattern = (
        rf'href=["\']/'
        rf'{re.escape(CLIENT_SLUG)}'
        rf'/job/([^/"\'?#]+)'
    )

    hashes = []
    seen = set()

    for job_hash in re.findall(
        pattern,
        html,
        flags=re.IGNORECASE,
    ):
        if job_hash not in seen:
            seen.add(job_hash)
            hashes.append(job_hash)

    return hashes


def fetch_api_jobs():
    jobs = []
    seen_hashes = set()
    page = 1

    while True:
        query = urlencode(
            {
                "page_size": 50,
                "page": page,
                "ordering": (
                    "-is_pinned_in_career_page,"
                    "-last_published_at"
                ),
            }
        )

        data = fetch_json(f"{API_URL}?{query}")

        if not isinstance(data, dict):
            raise RuntimeError(
                "Unexpected API response"
            )

        for job in data.get("results") or []:
            if not isinstance(job, dict):
                continue

            job_hash = clean(job.get("hash"))

            if (
                not job_hash
                or job_hash in seen_hashes
            ):
                continue

            seen_hashes.add(job_hash)
            jobs.append(job)

        if not data.get("next"):
            break

        page += 1

    return jobs


def collect_jobs():
    main_html = fetch_text(SOURCE_URL)

    main_text = html_to_text(main_html) or ""

    if EMPLOYER.casefold() not in main_text.casefold():
        raise RuntimeError(
            "The career page does not match "
            f"{EMPLOYER}"
        )

    try:
        api_jobs = fetch_api_jobs()

    except Exception:
        # Rückfall auf die serverseitig ausgegebenen Links.
        api_jobs = [
            {"hash": job_hash}
            for job_hash in extract_hashes_from_html(
                main_html
            )
        ]

    result = {
        "company": EMPLOYER,
        "source": SOURCE_URL,
        "collectedAt": utc_now(),
        "count": 0,
        "jobs": [],
    }

    for api_job in api_jobs:
        job_hash = clean(api_job.get("hash"))

        if not job_hash:
            continue

        detail_url = (
            f"{SOURCE_URL}/job/{job_hash}"
        )

        try:
            detail_html = fetch_text(detail_url)

        except HTTPError as error:
            # Zwischenzeitlich geschlossene Stellen
            # werden nicht ausgegeben.
            if error.code in {404, 410}:
                continue

            raise

        job_schema = extract_job_schema(
            detail_html
        )

        if not job_schema:
            continue

        hiring_organization = (
            job_schema.get("hiringOrganization")
            or {}
        )

        if not isinstance(
            hiring_organization,
            dict,
        ):
            continue

        hiring_name = clean(
            hiring_organization.get("name")
        )

        if (
            not hiring_name
            or hiring_name.casefold()
            not in ACCEPTED_HIRING_NAMES
        ):
            # Keine fremden oder nur ähnlichen
            # Arbeitgeber aufnehmen.
            continue

        title = clean(job_schema.get("title"))

        if not title:
            continue

        department = clean(
            api_job.get("organization_name")
        )

        if not department:
            department = clean(
                job_schema.get(
                    "occupationalCategory"
                )
            )

        result["jobs"].append(
            {
                "employer": EMPLOYER,
                "id": job_hash,
                "title": title,
                "url": detail_url,
                "postingDate": iso_date(
                    job_schema.get("datePosted")
                ),
                "closingDate": iso_date(
                    job_schema.get("validThrough")
                ),
                "specialization": department,
                "subSpecializations": [],
                "employmentType":
                    normalize_employment_type(
                        job_schema.get(
                            "employmentType"
                        )
                    ),
                "minimumExperience":
                    extract_experience(
                        job_schema.get(
                            "experienceRequirements"
                        )
                    ),
                "location": extract_location(
                    job_schema
                ),
                "description": html_to_text(
                    job_schema.get("description")
                ),
            }
        )

    result["count"] = len(result["jobs"])

    return result


def main():
    try:
        output = collect_jobs()
        exit_code = 0

    except Exception as error:
        output = {
            "company": EMPLOYER,
            "source": SOURCE_URL,
            "collectedAt": utc_now(),
            "count": 0,
            "jobs": [],
            "error": str(error),
        }

        exit_code = 1

    print(
        json.dumps(
            output,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )

    return exit_code


if __name__ == "__main__":
    sys.exit(main())