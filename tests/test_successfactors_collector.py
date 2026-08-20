import unittest

from src.collectors.successfactors import (
    _category_rss_url,
    _extract_job_id_from_url,
    _parse_category_rss,
    _parse_recruiting_api_job,
)


class SuccessFactorsCategoryRssTests(unittest.TestCase):
    def test_builds_category_feed_url(self) -> None:
        careers_url = "https://careers.example.com/go/PAXOCEAN-GROUP/9771866/"

        self.assertEqual(
            _category_rss_url(careers_url),
            "https://careers.example.com/services/rss/category/?catid=9771866",
        )

    def test_ignores_no_jobs_sentinel(self) -> None:
        xml = b"""<rss><channel><item>
            <title>No jobs currently available - Check out our other opportunities.</title>
            <link>https://careers.example.com</link>
        </item></channel></rss>"""

        jobs, no_jobs_available = _parse_category_rss(xml)

        self.assertEqual(jobs, [])
        self.assertTrue(no_jobs_available)

    def test_parses_category_job(self) -> None:
        xml = b"""<rss><channel><item>
            <title>Project Manager (Singapore, SG, 629350)</title>
            <link>https://careers.example.com/job/Singapore-Project-Manager-SG-629350/1361851966/?feedId=null&amp;utm_source=J2WRSS</link>
            <pubDate>Thu, 20 Aug 2026 16:00:00 GMT</pubDate>
        </item></channel></rss>"""

        jobs, no_jobs_available = _parse_category_rss(xml)

        self.assertFalse(no_jobs_available)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["title"], "Project Manager")
        self.assertEqual(jobs[0]["location"], "Singapore, SG, 629350")
        self.assertEqual(jobs[0]["posted_date"], "2026-08-20")

    def test_parses_recruiting_api_job(self) -> None:
        response = {
            "id": "417",
            "unifiedUrlTitle": "HSE-Coordinator",
            "unifiedStandardTitle": "HSE Coordinator",
            "unifiedStandardStart": "27/01/2026",
            "jobLocationShort": ["Singapore, 01, SGP, 629122<br/>"],
        }

        job = _parse_recruiting_api_job(response, "https://careers.example.com", "en_GB")

        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job["job_url"], "https://careers.example.com/job/HSE-Coordinator/417-en_GB")
        self.assertEqual(job["location"], "Singapore, 01, SGP, 629122")
        self.assertEqual(job["posted_date"], "2026-01-27")
        self.assertEqual(_extract_job_id_from_url(job["job_url"]), "417")


if __name__ == "__main__":
    unittest.main()