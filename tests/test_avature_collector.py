import unittest

from src.collectors.avature import _parse_listing, _parse_markdown_listing


class ParseAvatureListingTests(unittest.TestCase):
    def test_parses_jacobs_style_job_card(self) -> None:
        html = """
        <article class="article article--card">
          <h3><a class="link" href="/en_US/careers/JobDetail/Quantity-Surveyor/43461">
            Quantity Surveyor
          </a></h3>
          <div class="article__header__text__subtitle">
            <span class="list-item-location">Singapore, All SG Regions, Singapore</span>
          </div>
        </article>
        """

        jobs = _parse_listing(html, "https://careers.jacobs.com/en_US/careers/SearchJobs/")

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["folder_id"], "43461")
        self.assertEqual(jobs[0]["job_title"], "Quantity Surveyor")
        self.assertEqual(jobs[0]["listing_location"], "Singapore, All SG Regions, Singapore")

    def test_parses_jina_markdown_listing(self) -> None:
        markdown = """### [Quantity Surveyor](https://careers.jacobs.com/en_US/careers/JobDetail/Quantity-Surveyor/43461)

Singapore, All SG Regions, Singapore 43461 Cities & Places Project Controls
"""

        jobs = _parse_markdown_listing(markdown)

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["folder_id"], "43461")
        self.assertEqual(jobs[0]["listing_location"], "Singapore, All SG Regions, Singapore")


if __name__ == "__main__":
    unittest.main()