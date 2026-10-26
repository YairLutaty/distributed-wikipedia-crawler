"""Pure helpers shared by the crawler services.

This module deliberately has no Kafka, MongoDB, Redis, or HTTP dependencies so
its URL and file-handling rules can be tested without running the stack.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit


WIKIPEDIA_ORIGIN = "https://en.wikipedia.org"
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


def is_valid_article_href(href: str) -> bool:
    """Return whether a link targets a main-namespace English Wikipedia page."""

    if not isinstance(href, str):
        return False
    parts = urlsplit(href)
    if parts.netloc and (parts.scheme != "https" or parts.netloc != "en.wikipedia.org"):
        return False
    if not parts.path.startswith("/wiki/"):
        return False
    title = parts.path.removeprefix("/wiki/")
    return (
        bool(title)
        and not title.startswith("/")
        and ":" not in title
        and not parts.fragment
    )


def normalize_article_links(hrefs: list[str]) -> list[str]:
    """Filter, normalize, deduplicate, and deterministically order article URLs."""

    return sorted(
        {
            f"{WIKIPEDIA_ORIGIN}{urlsplit(href).path}"
            for href in hrefs
            if is_valid_article_href(href)
        }
    )


def page_file_id(url: str) -> str:
    """Build a readable, collision-resistant identifier safe for file paths."""

    title = unquote(urlsplit(url).path.rsplit("/", 1)[-1]) or "article"
    readable = _SAFE_NAME.sub("_", title).strip("._")[:80] or "article"
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return f"{readable}-{digest}"


def page_file_path(output_dir: str | Path, url: str) -> Path:
    return Path(output_dir) / f"{page_file_id(url)}.html"


def retry_delay(attempt: int, base_seconds: float, maximum_seconds: float) -> float:
    """Return bounded exponential backoff for a one-based attempt number."""

    if attempt < 1:
        raise ValueError("attempt must be at least 1")
    return min(maximum_seconds, base_seconds * (2 ** (attempt - 1)))
