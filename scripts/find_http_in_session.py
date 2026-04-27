"""Heuristic scanner for HTTP-inside-session anti-pattern.

Walks every .py under backend/services and backend/workers, identifies
``async with AsyncSessionLocal()`` / ``FastAsyncSessionLocal()`` /
``AuditAsyncSessionLocal()`` blocks, and flags any HTTP call
(``polymarket_client.*`` or ``live_execution_service.*``) that is NOT
guarded by a surrounding ``async with release_conn(session):``.

Output: one line per offending site with file path, session-open line,
HTTP call line, and the offending source line.

Limitations: heuristic only.  Doesn't track call depth or aliases.  Use
as a starting point for human review, not as a hard linter.
"""
from __future__ import annotations
import os
import re
import sys

ROOTS = ("backend/services", "backend/workers")
SESSION_OPENERS = (
    "async with AsyncSessionLocal",
    "async with FastAsyncSessionLocal",
    "async with AuditAsyncSessionLocal",
)
# Direct HTTP / external-IO call sites — anything we know goes over the
# network or sleeps long enough to hold a connection.  We also flag
# generic ``await asyncio.gather(...)`` patterns that contain HTTP
# tokens on subsequent lines (e.g. _fetch_clob_midpoint pattern).
HTTP_CALL_RE = re.compile(
    r"\bawait\s+("
    r"polymarket_client|"
    r"live_execution_service|"
    r"_les\.|"  # alias used in some files
    r"httpx\.|"
    r"aiohttp\.|"
    r"_load_mapping_with_timeout|"
    r"_fetch_market_info|"
    r"_fetch_clob_|"
    r"_load_execution_wallet_|"
    r"_load_wallet_history|"
    r"feed_manager\.polymarket_feed\.subscribe|"
    r"event_bus\.publish_blocking|"
    r"asyncio\.sleep\s*\(\s*[1-9]|"
    # Catch any await on a name containing 'http', 'fetch_', 'rpc_'
    r"\w*_?(?:http|fetch|rpc|gamma|clob)_\w*\s*\("
    r")"
)


def scan(path: str) -> list[tuple[str, int, int, str]]:
    try:
        with open(path, encoding="utf-8") as f:
            full = f.readlines()
    except Exception:
        return []

    findings: list[tuple[str, int, int, str]] = []
    in_with = False
    with_indent = -1
    with_line = 0
    in_release = False
    release_indent = -1
    for i, line in enumerate(full, 1):
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
                findings.append((path, with_line, i, line.rstrip()))
    return findings


def main() -> int:
    all_findings: list[tuple[str, int, int, str]] = []
    for root in ROOTS:
        for dp, _, files in os.walk(root):
            for fn in files:
                if not fn.endswith(".py"):
                    continue
                p = os.path.join(dp, fn).replace("\\", "/")
                all_findings.extend(scan(p))

    if not all_findings:
        print("CLEAN: no HTTP-inside-session anti-pattern found")
        return 0

    print(f"FOUND {len(all_findings)} potential offender(s):")
    for f, ws, ls, content in all_findings:
        print(f"  {f}:{ws}->{ls}")
        print(f"      {content.strip()[:160]}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
