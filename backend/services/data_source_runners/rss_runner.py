"""RSS feed data source runner.

Config schema: {"url": str, "poll_interval_minutes": int, "category_filter": str?}
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from utils.logger import get_logger

logger = get_logger(__name__)


class RssRunner:
    """Fetch and normalize RSS/Atom feed entries."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.url = str(config.get("url") or "").strip()
        if not self.url:
            raise ValueError("RSS source config requires 'url'")
        self.category_filter = str(config.get("category_filter") or "").strip().lower() or None

    async def fetch(self) -> list[dict[str, Any]]:
        import feedparser
        import httpx

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(self.url)
            resp.raise_for_status()
            raw_xml = resp.text

        feed = feedparser.parse(raw_xml)
        records: list[dict[str, Any]] = []

        for entry in feed.entries or []:
            # Build stable external_id from GUID or link
            guid = getattr(entry, "id", None) or getattr(entry, "link", None) or ""
            if not guid:
                continue
            external_id = hashlib.sha256(guid.encode("utf-8")).hexdigest()[:32]

            title = str(getattr(entry, "title", "") or "").strip()
            summary = str(getattr(entry, "summary", "") or getattr(entry, "description", "") or "").strip()
            url = str(getattr(entry, "link", "") or "").strip()
            source = str(feed.feed.get("title", "") if hasattr(feed, "feed") else "").strip() or self.url

            # Parse published date
            observed_at = None
            published = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
            if published:
                try:
                    from time import mktime
                    observed_at = datetime.fromtimestamp(mktime(published), tz=timezone.utc).isoformat()
                except Exception:
                    pass

            # Extract categories/tags
            tags = []
            for tag_obj in getattr(entry, "tags", []) or []:
                term = str(getattr(tag_obj, "term", "") or "").strip()
                if term:
                    tags.append(term)

            category = tags[0].lower() if tags else None

            # Apply category filter if set
            if self.category_filter:
                if not any(self.category_filter in t.lower() for t in tags):
                    continue

            records.append({
                "external_id": external_id,
                "title": title,
                "summary": summary[:2000] if summary else None,
                "url": url or None,
                "source": source,
                "category": category,
                "observed_at": observed_at,
                "tags": tags,
            })

        logger.info("RSS fetch complete", url=self.url, records=len(records))
        return records
