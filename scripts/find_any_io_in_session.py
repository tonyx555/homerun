"""Conservative scan: ANY ``await`` inside an open session that isn't
operating directly on the session object or wrapped in release_conn.

This catches indirect HTTP, awaitable wrappers, asyncio.gather over
HTTP, etc.  Output is intentionally noisy — review each finding to
classify as: (a) genuinely DB-only (false positive), (b) trivially
fast (acceptable), or (c) real HTTP/IO (must wrap in release_conn).
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

# What we KNOW is safe: operations on the session object itself.
# We deliberately do NOT include arbitrary helper-function names — we
# want to surface every call site that goes through a helper so a human
# can confirm the helper is DB-only.
SAFE_AWAIT_RE = re.compile(
    r"\bawait\s+("
    r"session\.|"
    r"verify_session\.|cp_session\.|bot_session\.|cleanup_session\.|"
    r"submit_session\.|active_session\.|order_session\.|new_session\.|"
    r"audit_session\.|outer_session\.|"
    r"super\(\)\.|"
    r"asyncio\.shield"
    r")"
)
ANY_AWAIT_RE = re.compile(r"\bawait\s+\S")


def scan(path: str) -> list[tuple[int, int, str]]:
    try:
        with open(path, encoding="utf-8") as f:
            full = f.readlines()
    except Exception:
        return []

    findings: list[tuple[int, int, str]] = []
    in_with = False
    with_indent = -1
    with_line = 0
    in_release = False
    release_indent = -1
    for i, line in enumerate(full, 1):
        stripped = line.lstrip()
        leading = len(line) - len(stripped)
        if any(o in line for o in SESSION_OPENERS):
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
            if in_release:
                continue
            if ANY_AWAIT_RE.search(line) and not SAFE_AWAIT_RE.search(line):
                findings.append((with_line, i, line.rstrip()))
    return findings


def main() -> int:
    all_findings: list[tuple[str, int, int, str]] = []
    for root in ROOTS:
        for dp, _, files in os.walk(root):
            for fn in files:
                if not fn.endswith(".py"):
                    continue
                p = os.path.join(dp, fn).replace("\\", "/")
                for ws, ls, content in scan(p):
                    all_findings.append((p, ws, ls, content))

    print(f"Found {len(all_findings)} await call(s) inside session that aren't obviously session-bound:")
    for f, ws, ls, content in all_findings:
        print(f"  {f}:{ws}->{ls}\n    {content.strip()[:160]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
