import tempfile
import unittest
from pathlib import Path

from parser import extract_article_links, parse_fetched


HTML = """
<html><body><div id="mw-content-text">
  <a href="/wiki/Grace_Hopper">Grace</a>
  <a href="/wiki/Ada_Lovelace">Ada</a>
  <a href="https://en.wikipedia.org/wiki/Alan_Turing">Alan</a>
  <a href="/wiki/Grace_Hopper">Grace again</a>
  <a href="/wiki/File:Portrait.jpg">File</a>
  <a href="/wiki/Ada_Lovelace#Legacy">Fragment</a>
</div></body></html>
"""


class ParserTests(unittest.TestCase):
    def test_extracts_only_unique_main_namespace_articles(self):
        self.assertEqual(
            extract_article_links(HTML),
            [
                "https://en.wikipedia.org/wiki/Ada_Lovelace",
                "https://en.wikipedia.org/wiki/Alan_Turing",
                "https://en.wikipedia.org/wiki/Grace_Hopper",
            ],
        )

    def test_missing_content_container_is_not_a_valid_parse(self):
        self.assertIsNone(extract_article_links("<html><body>no article</body></html>"))

    def test_parse_result_preserves_source_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "page.html"
            path.write_text(HTML, encoding="utf-8")
            result = parse_fetched(
                {
                    "file_id": "page-1",
                    "file_path": str(path),
                    "url": "https://example.test",
                }
            )
        self.assertEqual(result["file_id"], "page-1")
        self.assertEqual(result["url"], "https://example.test")
        self.assertEqual(len(result["links"]), 3)
        self.assertTrue(result["parsed_at"].endswith("+00:00"))


if __name__ == "__main__":
    unittest.main()
