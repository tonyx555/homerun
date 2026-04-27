"""Regression test: no HTTP / external-IO inside an open DB transaction.

The "idle in transaction" pattern (PG holding a row-lock + connection
slot while the application is mid-HTTP-call) was the dominant source
of DB-pool starvation in production.  Phase 1a refactored the
verifier to a two-phase pattern.  Every other site already uses
``release_conn(session)`` to release the connection during HTTP.

This test scans every .py under backend/services and backend/workers
and fails if it finds an ``await polymarket_client.*`` /
``await live_execution_service.*`` / ``await asyncio.sleep(N>=1)`` /
``await _fetch_market_info`` / etc. INSIDE an
``async with AsyncSessionLocal() as session:`` block (or fast-tier
or audit-tier session) that is NOT wrapped by
``async with release_conn(session):``.

If you legitimately need to hold the session across IO, wrap the
IO in ``release_conn(session)`` so the pool slot is freed during the
network call.  See backend/models/database.py:release_conn for the
canonical pattern.
"""
from __future__ import annotations
import os
import re
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
SCAN_ROOTS = (BACKEND_ROOT / "services", BACKEND_ROOT / "workers")

SESSION_OPENERS = (
    "async with AsyncSessionLocal",
    "async with FastAsyncSessionLocal",
    "async with AuditAsyncSessionLocal",
)

# Heuristic — anything we know goes over the network or sleeps long
# enough to keep a PG connection idle-in-transaction.
HTTP_CALL_RE = re.compile(
    r"\bawait\s+("
    r"polymarket_client|"
    r"live_execution_service|"
    r"_les\.|"
    r"httpx\.|"
    r"aiohttp\.|"
    r"_load_mapping_with_timeout|"
    r"_fetch_market_info|"
    r"_fetch_clob_|"
    r"_load_execution_wallet_|"
    r"_load_wallet_history|"
    r"feed_manager\.polymarket_feed\.subscribe|"
    r"event_bus\.publish_blocking|"
    r"asyncio\.sleep\s*\(\s*[1-9]"
    r")"
)


def _scan_file(path: Path) -> list[tuple[int, int, str]]:
    """Return (session_open_line, http_call_line, source_line) for each
    offending site found in *path*.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return []
    lines = text.splitlines(keepends=False)

    findings: list[tuple[int, int, str]] = []
    in_with = False
    with_indent = -1
    with_line = 0
    in_release = False
    release_indent = -1
    for i, line in enumerate(lines, 1):
        stripped = line.lstrip()
        leading = len(line) - len(stripped)
        if any(opener in line for opener in SESSION_OPENERS):
            in_with = True
            with_indent = leading
            with_line = i
            in_release = False
            continue
        if in_with:
            if stripped and leading <= with_indent and not stripped.startswith("#"):
                in_with = False
                in_release = False
                continue
            if "async with release_conn" in line:
                in_release = True
                release_indent = leading
                continue
            if in_release and stripped and leading <= release_indent and not stripped.startswith("#"):
                in_release = False
            if not in_release and HTTP_CALL_RE.search(line):
                findings.append((with_line, i, line.rstrip()))
    return findings


def test_no_http_inside_open_session() -> None:
    offenders: list[tuple[Path, int, int, str]] = []
    for root in SCAN_ROOTS:
        for dp, _, files in os.walk(root):
            for fn in files:
                if not fn.endswith(".py"):
                    continue
                if fn == "__init__.py":
                    continue
                p = Path(dp) / fn
                for ws, ls, content in _scan_file(p):
                    offenders.append((p, ws, ls, content))

    if offenders:
        rel_root = BACKEND_ROOT.parent
        msg_lines = [
            f"Found {len(offenders)} site(s) holding a DB session across HTTP/IO. "
            "Wrap the IO in ``async with release_conn(session):`` (see "
            "backend/models/database.py:release_conn) or restructure to a "
            "two-phase read-then-write pattern (see verify_orders_from_bot_lineage "
            "for an example).",
        ]
        for path, ws, ls, content in offenders:
            try:
                rel = path.relative_to(rel_root)
            except ValueError:
                rel = path
            msg_lines.append(
                f"  {rel}:{ws}->{ls}\n    {content.strip()[:160]}"
            )
        pytest.fail("\n".join(msg_lines))
