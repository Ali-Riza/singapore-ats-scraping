import unittest

from src.collectors.wsp import _last_page, _parse_wsp_markdown, _reported_total, _with_params

JOB_CARD = """
<a href="https://applr.io/l/9afc3039?portal=cxb" data-value="{
  'event': 'user_interaction',
  'interaction': {
    'click_type': 'job posting link',
    'link_url': 'https://applr.io/l/9afc3039?portal=cxb',
    'link_text': 'Proposal Manager',
    'job_city': ' Buona Vista',
    'job_country': 'Singapore'
  }
}" class="career-result-link" target="_blank" title="Proposal Manager">
"""


class WspCollectorTests(unittest.TestCase):
    def test_parses_job_card(self) -> None:
        jobs = _parse_wsp_markdown(JOB_CARD)

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["job_id"], "9afc3039")
        self.assertEqual(jobs[0]["title"], "Proposal Manager")
        self.assertEqual(jobs[0]["location"], "Buona Vista")
        self.assertEqual(jobs[0]["job_url"], "https://applr.io/l/9afc3039?portal=cxb")

    def test_deduplicates_repeated_job_ids(self) -> None:
        self.assertEqual(len(_parse_wsp_markdown(JOB_CARD + JOB_CARD)), 1)

    def test_falls_back_to_country_when_city_missing(self) -> None:
        card = JOB_CARD.replace("' Buona Vista'", "''")
        self.assertEqual(_parse_wsp_markdown(card)[0]["location"], "Singapore")

    def test_parses_reported_total(self) -> None:
        self.assertEqual(_reported_total("<h2>Showing 36 jobs</h2>"), 36)

    def test_reads_last_page_from_pagination(self) -> None:
        pagination = (
            '<div class="pagelist current"><a class="pageprev" name="page" data-value="1">1</a></div>'
            '<div class="pagelist"><a class="pageprev" name="page" data-value="2">2</a></div>'
            '<div class="pagelist nxt-arw"><a class="pagenext" name="page" data-value="2"></a></div>'
        )
        self.assertEqual(_last_page(pagination), 2)

    def test_last_page_defaults_to_one(self) -> None:
        self.assertEqual(_last_page("<div>no pagination</div>"), 1)

    def test_with_params_merges_into_existing_query(self) -> None:
        url = _with_params(
            "https://www.wsp.com/en-sg/careers/job-opportunities?country=SG", page=2
        )
        self.assertIn("country=SG", url)
        self.assertIn("page=2", url)
        self.assertEqual(url.count("?"), 1)


if __name__ == "__main__":
    unittest.main()
