import tempfile
import unittest
from pathlib import Path

from crawler_core import (
    is_valid_article_href,
    normalize_article_links,
    page_file_id,
    page_file_path,
    retry_delay,
)


class CrawlerCoreTests(unittest.TestCase):
    def test_article_filter_rejects_namespaces_fragments_and_external_links(self):
        self.assertTrue(is_valid_article_href("/wiki/Ada_Lovelace"))
        self.assertTrue(
            is_valid_article_href("https://en.wikipedia.org/wiki/Ada_Lovelace")
        )
        self.assertFalse(is_valid_article_href("/wiki/File:Ada.jpg"))
        self.assertFalse(is_valid_article_href("/wiki/Ada_Lovelace#Legacy"))
        self.assertFalse(is_valid_article_href("/wiki//double-slash"))
        self.assertFalse(is_valid_article_href("https://example.com/wiki/Ada"))

    def test_links_are_normalized_deduplicated_and_sorted(self):
        actual = normalize_article_links(
            [
                "/wiki/Grace_Hopper",
                "https://en.wikipedia.org/wiki/Alan_Turing",
                "/wiki/Ada_Lovelace",
                "/wiki/Grace_Hopper",
                "/wiki/Category:Computing",
            ]
        )
        self.assertEqual(
            actual,
            [
                "https://en.wikipedia.org/wiki/Ada_Lovelace",
                "https://en.wikipedia.org/wiki/Alan_Turing",
                "https://en.wikipedia.org/wiki/Grace_Hopper",
            ],
        )

    def test_file_id_is_safe_stable_and_url_specific(self):
        first = page_file_id("https://en.wikipedia.org/wiki/AC/DC")
        second = page_file_id("https://en.wikipedia.org/wiki/AC%2FDC")
        self.assertNotIn("/", first)
        self.assertNotEqual(first, second)
        self.assertEqual(first, page_file_id("https://en.wikipedia.org/wiki/AC/DC"))
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(
                page_file_path(directory, "https://en.wikipedia.org/wiki/Ada").parent,
                Path(directory),
            )

    def test_retry_delay_is_exponential_and_bounded(self):
        self.assertEqual(
            [retry_delay(i, 0.5, 2.0) for i in range(1, 6)], [0.5, 1.0, 2.0, 2.0, 2.0]
        )
        with self.assertRaises(ValueError):
            retry_delay(0, 0.5, 2.0)


if __name__ == "__main__":
    unittest.main()
