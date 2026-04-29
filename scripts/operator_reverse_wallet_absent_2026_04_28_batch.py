"""One-shot recovery script for the 3 wallet-absent-resolved orders
from the 2026-04-28 incident.

Background
----------
Three of the 12 orders the bulk writeoff script targeted on
2026-04-28 had ALREADY been transitioned to ``status='resolved'`` via
the wallet-absent-close path before the writeoff fired (events at
06:45 / 07:07 UTC; writeoff at 01:10 UTC of the SAME day, but the
wallet-absent transitions were the EARLIER pass — the rows stayed at
``executed`` long enough for the wallet-absent close to run later
that morning).

Root cause: the lifecycle's wallet-absent close path treated
``wallet_positions_loaded=True AND wallet_position_observed=False``
as authoritative evidence the wallet doesn't hold the token.  The
local ``live_trading_positions`` cache had been wiped by an earlier
``_persist_positions`` pass that received an empty (transient)
result from Polymarket's ``/positions`` API.  Both layers are now
guarded:

  * ``services/live_execution_service._persist_positions`` refuses
    to wipe-and-replace the table when the new positions list is
    empty but the existing table is non-empty.
  * ``services/trader_orchestrator/position_lifecycle.reconcile_live_positions``
    requires the wallet snapshot to be non-empty
    (``wallet_positions_authoritative``) before the wallet-absent
    close can fire.

This script
-----------
Calls ``reverse_wallet_absent_resolution`` for each of:

  * 56cae01c (Will Fenerbahçe win the Süper Lig?)
  * 76e3a73e (Will Karolina Pliskova win the 2026 Madrid Open?)
  * fa51582a (Shymkent 2: Edas Butvilas vs Zangar Nurlanuly)

Default ``--dry-run`` resolves the IDs and prints the targets.
``--apply`` calls the function for real.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


_BATCH_ORDER_ID_PREFIXES: tuple[str, ...] = (
    "56cae01c",  # Fenerbahçe Süper Lig
    "76e3a73e",  # Pliskova Madrid Open
    "fa51582a",  # Tennis: Butvilas vs Nurlanuly
)

_OPERATOR_ID = "homerun-operator-recovery"
_REASON = (
    "Reversal of 3 wallet-absent resolutions from the 2026-04-28 "
    "incident.  Root cause: an empty (transient) response from "
    "Polymarket /positions wiped the local live_trading_positions "
    "cache; the lifecycle's wallet-absent close path then read "
    "wallet_position_observed=False as authoritative and flipped "
    "these rows to status=resolved with pending_live_exit.status="
    "superseded_wallet_absent_no_provider.  Per-incident chain "
    "balances (recorded in the prior writeoff event payloads) "
    "confirm the wallet still held shares.  Both the wipe-on-empty "
    "and the wallet-absent close trigger are now guarded; this "
    "script restores the historic rows so the lifecycle re-attempts "
    "exit with the new guards in place."
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
        WalletAbsentResolutionReversalRejected,
        reverse_wallet_absent_resolution,
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
                    result = await reverse_wallet_absent_resolution(
                        session,
                        order_id=full_id,
                        reason=_REASON,
                        operator_id=_OPERATOR_ID,
                    )
                except WalletAbsentResolutionReversalRejected as exc:
                    print(f"  {prefix}  [{full_id}]  SKIPPED   {exc}")
                    skipped.append((full_id, str(exc)))
                    continue
                await session.commit()
                print(
                    f"  {prefix}  [{full_id}]  REVERSED  status="
                    f"{result['next_status']} verification="
                    f"{result['verification_status']} prior_pe="
                    f"{result['prior_pending_exit_status']}"
                )
                succeeded.append(full_id)
        except Exception as exc:
            print(f"  {prefix}  FAILED   {type(exc).__name__}: {exc}")
            failed.append((prefix, f"{type(exc).__name__}: {exc}"))

    print()
    if not apply_changes:
        print(
            f"Dry-run complete.  {len(_BATCH_ORDER_ID_PREFIXES)} candidates printed."
        )
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
