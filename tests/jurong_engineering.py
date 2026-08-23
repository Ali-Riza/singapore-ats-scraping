import json
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright


SOURCE_URL = "https://jel.applyourjobs.com/"
EXPECTED_HOST = "jel.applyourjobs.com"
EMPLOYER = "Jurong Engineering Limited"


def clean(value):
    if value is None:
        return None

    result = " ".join(str(value).replace("\xa0", " ").split())
    return result or None


def iso_date(value):
    if not value:
        return None

    try:
        return datetime.strptime(
            clean(value),
            "%d %b %Y",
        ).date().isoformat()
    except ValueError:
        return None


def collect_jobs():
    result = {
        "company": EMPLOYER,
        "source": SOURCE_URL,
        "collectedAt": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "count": 0,
        "jobs": [],
    }

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)

        try:
            listing_page = browser.new_page()

            listing_page.goto(
                SOURCE_URL,
                wait_until="domcontentloaded",
                timeout=60_000,
            )

            current_host = urlparse(listing_page.url).hostname

            if current_host != EXPECTED_HOST:
                raise RuntimeError(
                    f"Unexpected host: {current_host}"
                )

            page_title = clean(listing_page.title())

            if not page_title or (
                page_title.casefold() != EMPLOYER.casefold()
            ):
                raise RuntimeError(
                    f"Unexpected employer: {page_title}"
                )

            # Warten, bis entweder Stellen oder eine leere Liste
            # angezeigt werden.
            listing_page.wait_for_function(
                """
                () =>
                    document.querySelector(
                        "#divDynamicList .job-listing-details"
                    ) !== null ||
                    /No job\\(s\\) found/i.test(
                        document.querySelector("#divDynamicList")
                            ?.innerText || ""
                    )
                """,
                timeout=30_000,
            )

            listings = listing_page.locator(
                "#divDynamicList .job-listing-details"
            ).evaluate_all(
                """
                rows => {
                    const clean = value =>
                        String(value ?? "")
                            .replace(/\\u00a0/g, " ")
                            .replace(/\\s+/g, " ")
                            .trim();

                    const cleanValue = value => {
                        const result = clean(value)
                            .replace(/^:\\s*/, "");

                        if (
                            !result ||
                            result.startsWith("!") ||
                            result === "cls_remove_empty"
                        ) {
                            return null;
                        }

                        return result;
                    };

                    return rows
                        .map(row => {
                            const fields = {};

                            for (
                                const group of Array.from(
                                    row.children
                                )
                            ) {
                                const cells = Array.from(
                                    group.children
                                );

                                if (cells.length < 2) {
                                    continue;
                                }

                                const label = clean(
                                    cells[0].textContent
                                );

                                const value = cleanValue(
                                    cells[1].textContent
                                );

                                if (label) {
                                    fields[label] = value;
                                }
                            }

                            const link = row.querySelector(
                                ".sendto-detail-page"
                            );

                            const href = link?.getAttribute(
                                "href"
                            );

                            if (!href) {
                                return null;
                            }

                            return {
                                id: fields["Job ID"],
                                title: fields["Job Title"],
                                url: new URL(
                                    href,
                                    location.href
                                ).href,
                                postingDate:
                                    fields["Posting Date"],
                                specialization:
                                    fields[
                                        "Job Specialization"
                                    ],
                                subSpecializations:
                                    fields[
                                        "Job Sub Specialization"
                                    ]
                                        ? fields[
                                              "Job Sub Specialization"
                                          ]
                                              .split(";")
                                              .map(clean)
                                              .filter(Boolean)
                                        : [],
                                employmentType:
                                    fields["Job Type"],
                                location:
                                    fields["Work Location"]
                            };
                        })
                        .filter(Boolean);
                }
                """
            )

            if listings:
                detail_page = browser.new_page()

                for listing in listings:
                    detail_page.goto(
                        listing["url"],
                        wait_until="domcontentloaded",
                        timeout=60_000,
                    )

                    detail_page.wait_for_selector(
                        "#ctl00_body_lbljobtitle",
                        timeout=30_000,
                    )

                    # Die Detailwerte werden nach dem Seitentitel
                    # eingesetzt. Deshalb warten wir, bis der
                    # Standort keinen Platzhalter mehr enthält.
                    detail_page.wait_for_function(
                        """
                        () => [
                            ...document.querySelectorAll(
                                "[removeifnodata]"
                            )
                        ].some(row => {
                            const fields =
                                row.querySelectorAll(
                                    ".form-label"
                                );

                            return (
                                fields.length >= 2 &&
                                fields[0]
                                    .textContent
                                    .trim() ===
                                    "Work Location" &&
                                !fields[
                                    fields.length - 1
                                ].textContent.includes("!")
                            );
                        })
                        """,
                        timeout=30_000,
                    )

                    detail = detail_page.evaluate(
                        """
                        () => {
                            const clean = value =>
                                String(value ?? "")
                                    .replace(/\\u00a0/g, " ")
                                    .replace(/[ \\t]+/g, " ")
                                    .replace(
                                        /\\s*\\n\\s*/g,
                                        "\\n"
                                    )
                                    .trim();

                            const cleanValue = value => {
                                const result = clean(value)
                                    .replace(/^:\\s*/, "");

                                if (
                                    !result ||
                                    result.startsWith("!") ||
                                    result ===
                                        "cls_remove_empty"
                                ) {
                                    return null;
                                }

                                return result;
                            };

                            const header = clean(
                                document.querySelector(
                                    "#ctl00_body_dvJobHeader"
                                )?.textContent
                            );

                            const dates = header.match(
                                /Advertised on\\s*:\\s*(.*?)\\s*\\|\\s*Closing Date\\s*:\\s*(.*)$/i
                            );

                            const fields = {};

                            for (
                                const row of
                                    document.querySelectorAll(
                                        "[removeifnodata]"
                                    )
                            ) {
                                const labels =
                                    row.querySelectorAll(
                                        ".form-label"
                                    );

                                if (labels.length < 2) {
                                    continue;
                                }

                                const label = clean(
                                    labels[0].textContent
                                );

                                const value = cleanValue(
                                    labels[
                                        labels.length - 1
                                    ].textContent
                                );

                                if (
                                    label &&
                                    value !== null
                                ) {
                                    fields[label] = value;
                                }
                            }

                            const description =
                                document.querySelector(
                                    "#ctl00_body_tbJobDesc"
                                );

                            return {
                                postingDate:
                                    dates?.[1] || null,
                                closingDate:
                                    dates?.[2] || null,
                                specialization:
                                    fields[
                                        "Specialization"
                                    ] || null,
                                employmentType:
                                    fields[
                                        "Type of Employment"
                                    ] || null,
                                minimumExperience:
                                    fields[
                                        "Minimum Experience"
                                    ] || null,
                                location:
                                    fields[
                                        "Work Location"
                                    ] || null,
                                description:
                                    clean(
                                        description
                                            ?.innerText ||
                                        description
                                            ?.textContent
                                    ) || null
                            };
                        }
                        """
                    )

                    result["jobs"].append(
                        {
                            "employer": EMPLOYER,
                            "id": listing["id"],
                            "title": listing["title"],
                            "url": listing["url"],
                            "postingDate": iso_date(
                                detail["postingDate"]
                                or listing["postingDate"]
                            ),
                            "closingDate": iso_date(
                                detail["closingDate"]
                            ),
                            "specialization": (
                                detail["specialization"]
                                or listing[
                                    "specialization"
                                ]
                            ),
                            "subSpecializations": listing[
                                "subSpecializations"
                            ],
                            "employmentType": (
                                detail["employmentType"]
                                or listing[
                                    "employmentType"
                                ]
                            ),
                            "minimumExperience": detail[
                                "minimumExperience"
                            ],
                            "location": (
                                detail["location"]
                                or listing["location"]
                            ),
                            "description": detail[
                                "description"
                            ],
                        }
                    )

                detail_page.close()

            result["count"] = len(result["jobs"])
            listing_page.close()

        finally:
            browser.close()

    return result


def main():
    try:
        output = collect_jobs()
        exit_code = 0

    except Exception as error:
        output = {
            "company": EMPLOYER,
            "source": SOURCE_URL,
            "collectedAt": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
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