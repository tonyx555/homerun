"""REST API poll data source runner.

Config schema: {"url": str, "headers": dict?, "json_path": str, "poll_interval_minutes": int}
"""
from __future__ import annotations

import hashlib
from typing import Any

from utils.logger import get_logger

logger = get_logger(__name__)


def _extract_json_path(data: Any, path: str) -> list[dict[str, Any]]:
    """Simple JSONPath-like extraction (supports $.key.key[*] patterns)."""
    parts = path.strip().lstrip("$").split(".")
    current = data
    for part in parts:
        if not part:
            continue
        if part.endswith("[*]"):
            key = part[:-3]
            if key:
                if isinstance(current, dict):
                    current = current.get(key, [])
                else:
                    return []
            if isinstance(current, list):
                # Already a list, continue
                pass
            else:
                return []
        else:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                # Apply key to each element
                current = [item.get(part) if isinstance(item, dict) else None for item in current]
                current = [x for x in current if x is not None]
            else:
                return []
    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    if isinstance(current, dict):
        return [current]
    return []


class RestApiRunner:
    """Fetch and normalize records from a REST API endpoint."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.url = str(config.get("url") or "").strip()
        if not self.url:
            raise ValueError("REST API source config requires 'url'")
        self.headers = dict(config.get("headers") or {})
        self.json_path = str(config.get("json_path") or "$[*]").strip()

    async def fetch(self) -> list[dict[str, Any]]:
        import httpx

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(self.url, headers=self.headers)
            resp.raise_for_status()
            data = resp.json()

        raw_records = _extract_json_path(data, self.json_path)
        records: list[dict[str, Any]] = []

        for idx, item in enumerate(raw_records):
            # Try to find a natural ID
            external_id = str(
                item.get("id") or item.get("external_id") or item.get("guid") or ""
            ).strip()
            if not external_id:
                # Hash the full item as fallback
                import json
                external_id = hashlib.sha256(
                    json.dumps(item, sort_keys=True, default=str).encode()
                ).hexdigest()[:32]

            records.append({
                "external_id": external_id,
                "title": str(item.get("title") or item.get("name") or "").strip() or None,
                "summary": str(item.get("summary") or item.get("description") or item.get("body") or "").strip()[:2000] or None,
                "url": str(item.get("url") or item.get("link") or "").strip() or None,
                "source": str(item.get("source") or "").strip() or None,
                "category": str(item.get("category") or item.get("type") or "").strip().lower() or None,
                "observed_at": item.get("observed_at") or item.get("created_at") or item.get("published_at") or item.get("date") or None,
                "tags": item.get("tags") if isinstance(item.get("tags"), list) else [],
                # Pass the full raw item through as payload for transform/inspection
                **{k: v for k, v in item.items() if k not in ("id", "external_id", "title", "summary", "url", "source", "category", "observed_at", "tags")},
            })

        logger.info("REST API fetch complete", url=self.url, records=len(records))
        return records
