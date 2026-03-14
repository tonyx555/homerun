#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import os
from urllib.parse import urlsplit, urlunsplit

import asyncpg

async def _postgres_ping(database_url: str) -> None:
    parsed = urlsplit(database_url)
    scheme = parsed.scheme.lower()
    if scheme == "postgresql+asyncpg":
        database_url = urlunsplit(parsed._replace(scheme="postgresql"))
    elif scheme == "postgres+asyncpg":
        database_url = urlunsplit(parsed._replace(scheme="postgres"))

    conn = await asyncpg.connect(database_url, timeout=5)
    try:
        value = await conn.fetchval("SELECT 1")
        if value != 1:
            raise RuntimeError("Postgres probe query returned unexpected result")
    finally:
        await conn.close()


async def _main_async() -> None:
    parser = argparse.ArgumentParser(description="Runtime smoke test for launcher-managed Postgres.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    args = parser.parse_args()

    database_url = str(args.database_url or "").strip()
    if not database_url:
        raise ValueError("--database-url (or DATABASE_URL) is required")

    await _postgres_ping(database_url)


if __name__ == "__main__":
    asyncio.run(_main_async())
