import unittest

from src.collectors.oracle import (
    OracleCollector,
    _job_has_singapore_location,
    _location_from_ui,
    _preferred_location,
    _rest_base_from_ui,
    _site_number_from_ui,
)


class OracleUrlTests(unittest.TestCase):
    def test_selectminds_parser_uses_dynamic_host(self) -> None:
        from bs4 import BeautifulSoup
        from src.collectors.arup_selectminds import ArupSelectMindsCollector

        row = BeautifulSoup(
            """
            <div class="job_list_row" id="job_list_12201">
              <a class="job_link" href="/jobs/example-12201">Example</a>
              <p class="jlr_location"><a class="location">Singapore</a></p>
            </div>
            """,
            "lxml",
        ).select_one(".job_list_row")

        assert row is not None
        job = ArupSelectMindsCollector()._parse_job_row(
            row,
            "https://petrofac.referrals.selectminds.com/latest-jobs",
            "https://petrofac.referrals.selectminds.com",
        )

        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job["job_url"], "https://petrofac.referrals.selectminds.com/jobs/example-12201")

    def test_resolves_ti_rest_host(self) -> None:
        url = "https://careers.ti.com/en/sites/CX/jobs?mode=location"

        self.assertIn("edbz.fa.us2.oraclecloud.com", _rest_base_from_ui(url))
        self.assertEqual(_site_number_from_ui(url), "CX")

    def test_parses_stolt_fragment(self) -> None:
        url = (
            "https://www.stolt-nielsen.com/careers/onshore-vacancies/"
            "#en/sites/CX_1/jobs?locationId=300000000345421&mode=location"
        )

        self.assertIn("eclo.fa.em2.oraclecloud.com", _rest_base_from_ui(url))
        self.assertEqual(_site_number_from_ui(url), "CX_1")
        self.assertEqual(_location_from_ui(url), ("locationId", "300000000345421"))

    def test_maps_wood_level_one_facet_to_location_id(self) -> None:
        url = (
            "https://ehif.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/jobs"
            "?location=Singapore&selectedLocationLevel1Facet=300000000275234"
        )

        self.assertEqual(_location_from_ui(url), ("locationId", "300000000275234"))

    def test_prefers_oracle_primary_location(self) -> None:
        raw = {
            "PrimaryLocation": "Singapore",
            "workLocation": [{"LocationName": "Nanterre Origine", "Country": "FR"}],
        }

        self.assertTrue(_job_has_singapore_location(raw))
        self.assertEqual(_preferred_location(raw), "Singapore")

    def test_maps_singapore_from_work_location(self) -> None:
        raw = {
            "workLocation": [{"LocationName": "Singapore", "Country": "SG"}],
        }

        self.assertTrue(_job_has_singapore_location(raw))
        self.assertEqual(_preferred_location(raw), "Singapore")

    def test_singapore_filter_ignores_unstructured_description(self) -> None:
        raw = {
            "ShortDescriptionStr": "Work with our Singapore team",
            "PrimaryLocation": "Paris",
            "workLocation": [{"LocationName": "Paris", "Country": "FR"}],
        }

        self.assertFalse(_job_has_singapore_location(raw))

if __name__ == "__main__":
    unittest.main()