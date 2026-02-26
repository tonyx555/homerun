#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import os
import socket
from urllib.parse import urlsplit, urlunsplit

import asyncpg


_MIN_REDIS_VERSION_FOR_STREAMS = 5


def _redis_ping(host: str, port: int) -> None:
    payload = b"*1\r\n$4\r\nPING\r\n"
    with socket.create_connection((host, port), timeout=2.0) as sock:
        sock.settimeout(2.0)
        sock.sendall(payload)
        data = sock.recv(64)
    if b"+PONG" not in data:
        raise RuntimeError("Redis PING did not return PONG")


def _redis_version_check(host: str, port: int) -> None:
    """Check Redis version and warn if Streams are not supported."""
    payload = b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n"
    try:
        with socket.create_connection((host, port), timeout=2.0) as sock:
            sock.settimeout(2.0)
            sock.sendall(payload)
            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"redis_version:" in data:
                    break
            text = data.decode("ascii", errors="replace")
            for line in text.split("\n"):
                if line.startswith("redis_version:"):
                    version = line.split(":")[1].strip()
                    major = int(version.split(".")[0])
                    if major < _MIN_REDIS_VERSION_FOR_STREAMS:
                        print(
                            f"WARNING: Redis version {version} does NOT support "
                            f"Streams (requires >= {_MIN_REDIS_VERSION_FOR_STREAMS}.0). "
                            f"Trade signal streaming will be disabled."
                        )
                    else:
                        print(f"Redis version {version} (Streams supported)")
                    return
    except Exception:
        pass


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
    parser = argparse.ArgumentParser(description="Runtime smoke test for launcher-managed Redis + Postgres.")
    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "127.0.0.1"))
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    args = parser.parse_args()

    database_url = str(args.database_url or "").strip()
    if not database_url:
        raise ValueError("--database-url (or DATABASE_URL) is required")

    _redis_ping(args.redis_host, int(args.redis_port))
    _redis_version_check(args.redis_host, int(args.redis_port))
    await _postgres_ping(database_url)


if __name__ == "__main__":
    asyncio.run(_main_async())
