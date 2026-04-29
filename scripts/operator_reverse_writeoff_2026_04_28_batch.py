"""One-shot recovery script for the 2026-04-28 incident.

Background
----------
On 2026-04-28 at 01:10:09 UTC, ``scripts/operator_writeoff_stuck_batch.py``
fired ``manual_writeoff`` on 12 stuck orders.  The script's safety
assumption was that ``pending_live_exit.status='blocked_persistent_timeout'``
implied a venue rejection.  That assumption was wrong: the SELL retries
were timing out because of the asyncpg / DB-pressure cascade fixed in
commit 4a319cd, NOT because the venue was refusing the order.

By the time the workers were restarted with the DB fix in place, 12
positions were already in ``status=closed_loss``/``resolved`` with
``actual_profit=-notional_usd`` (full-loss).  Polymarket's UI showed
the wallet still held the shares for at least 9 of them, with current
values totalling ~$200 against the $85.28 originally written off.

This script
-----------
For each of the 12 specific order IDs from that batch, calls
``reverse_manual_writeoff_order`` with:

  * ``operator_id="homerun-operator-recovery"``  (distinct identifier
    so the reversal is auditable as a recovery action, separate from
    the original writeoff operator).
  * ``reason``                                      explains the root
    cause (DB pressure → client-side timeouts mistaken for venue
    rejections; commit 4a319cd fixed the underlying issue) and links
    the original writeoff event for traceability.
  * ``expected_chain_balance_shares=0.001``         minimal sanity
    check — every original writeoff event recorded
    ``chain_balance_shares > 0``, so any value above zero passes.
    The check exists to prevent reversing rows whose wallets WERE
    actually empty at writeoff time.

The script is idempotent: rows already reversed (status no longer
``closed_loss``/``closed_win``, or whose ``position_close.close_trigger``
is no longer ``manual_writeoff``) are skipped with a clear log line.

Run with --apply to actually fire the reversals.  Default is --dry-run.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# Order IDs are 8-char prefixes; we resolve to full IDs at runtime.
_BATCH_ORDER_ID_PREFIXES: tuple[str, ...] = (
    "bed1b35d",  # F1 Ocon fastest lap
    "cdd1005a",  # White House posts 180-199
    "ce881f73",  # LoL Team WE vs JD Gaming
    "f74abe9c",  # Major US official out by Apr 30
    "8b6a1191",  # 50-74 Strait of Hormuz transits
    "350ebe8b",  # Trump insult Apr 28
    "98985b1c",  # RB Leipzig vs St. Pauli draw
    "02820ab4",  # Trump talk to Ursula von der Leyen
    "f64c7fb3",  # WTI hit $100 in April
    "56cae01c",  # Fenerbahçe Süper Lig
    "76e3a73e",  # Pliskova Madrid Open
    "fa51582a",  # Tennis: Butvilas vs Nurlanuly
)

_OPERATOR_ID = "homerun-operator-recovery"
_REASON = (
    "Reversal of the 2026-04-28 01:10:09 UTC bulk writeoff batch. "
    "Root cause: DB-pressure / asyncpg protocol corruption (commit "
    "4a319cd) caused client-side SELL retries to time out, which "
    "presented to the lifecycle as ``blocked_persistent_timeout``. "
    "The bulk writeoff script (see operator_writeoff_stuck_batch.py "
    "as it was at that commit) treated that state as venue-side "
    "rejection and applied a full-loss writeoff. Polymarket UI on "
    "2026-04-28 confirmed the wallets still held shares and the "
    "markets remained tradable. Reversing so the lifecycle re-attempts "
    "exit with the now-healthy DB."
)


async def _resolve_full_id(session, prefix: str) -> str | None:
    from sqlalchemy import select
    from models.database import TraderOrder

    rows = (
        await session.execute(
            select(TraderOrder.id).where(TraderOrder.id.like(f"{prefix}%"))
        )
    ).scalars().all()
    if len(rows) == 1:
        return str(rows[0])
    if not rows:
        return None
    raise RuntimeError(f"prefix={prefix} resolved to multiple ids: {rows}")


async def _run(apply_changes: bool) -> int:
    from models.database import AsyncSessionLocal
    from services.operator_writeoff import (
        ManualWriteoffReversalRejected,
        reverse_manual_writeoff_order,
    )

    succeeded: list[str] = []
    skipped: list[tuple[str, str]] = []
    failed: list[tuple[str, str]] = []

    for prefix in _BATCH_ORDER_ID_PREFIXES:
        try:
            async with AsyncSessionLocal() as session:
                full_id = await _resolve_full_id(session, prefix)
                if full_id is None:
                    print(f"  {prefix}  NOT FOUND")
                    failed.append((prefix, "not_found"))
                    continue
                if not apply_changes:
                    print(f"  {prefix}  [{full_id}]  WOULD REVERSE")
                    continue
                try:
                    result = await reverse_manual_writeoff_order(
                        session,
                        order_id=full_id,
                        reason=_REASON,
                        operator_id=_OPERATOR_ID,
                        expected_chain_balance_shares=0.001,
                    )
                except ManualWriteoffReversalRejected as exc:
                    print(f"  {prefix}  [{full_id}]  SKIPPED   {exc}")
                    skipped.append((full_id, str(exc)))
                    continue
                await session.commit()
                print(
                    f"  {prefix}  [{full_id}]  REVERSED  status="
                    f"{result['next_status']} verification="
                    f"{result['verification_status']}"
                )
                succeeded.append(full_id)
        except Exception as exc:
            print(f"  {prefix}  FAILED   {type(exc).__name__}: {exc}")
            failed.append((prefix, f"{type(exc).__name__}: {exc}"))

    print()
    if not apply_changes:
        print(f"Dry-run complete.  {len(_BATCH_ORDER_ID_PREFIXES)} candidates printed.")
        print("Pass --apply to fire the reversals.")
    else:
        print(
            f"Reversal complete: {len(succeeded)} reversed, "
            f"{len(skipped)} skipped, {len(failed)} failed."
        )
    return 0 if not failed else 2


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually fire the reversals.  Default is --dry-run.",
    )
    args = parser.parse_args()
    return asyncio.run(_run(apply_changes=args.apply))


if __name__ == "__main__":
    sys.exit(main())
