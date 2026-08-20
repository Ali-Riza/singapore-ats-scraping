import unittest

from src.collectors.mycareersfuture import _extract_uen


class ExtractUenTests(unittest.TestCase):
    def test_extracts_company_uen_with_entity_type(self) -> None:
        url = "https://www.mycareersfuture.gov.sg/companies/hyundai-engineering-construction-S81FC2987D"

        self.assertEqual(_extract_uen(url, {}), "S81FC2987D")

    def test_extracts_year_based_company_uen(self) -> None:
        url = "https://www.mycareersfuture.gov.sg/companies/unimatec-singapore-200601651W"

        self.assertEqual(_extract_uen(url, {}), "200601651W")

    def test_extracts_legacy_business_uen(self) -> None:
        url = "https://www.mycareersfuture.gov.sg/companies/example-12345678A"

        self.assertEqual(_extract_uen(url, {}), "12345678A")


if __name__ == "__main__":
    unittest.main()