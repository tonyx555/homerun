"""One-shot operator script: write off currently-stuck orders.

Reads the live system's stuck-order queue and for each row whose
pending_live_exit.last_error matches a genuine venue rejection, hits
the manual-writeoff API with:

  * realized_pnl   = -notional_usd  (full loss; operator's conservative
                                     assertion since the markets are
                                     unresolved + untradable + the CLOB
                                     no longer accepts orders for them)
  * reason         = explains the chain evidence at decision time
  * operator_id    = "homerun-operator"
  * chain_evidence = the on-chain status snapshot (immutable in the
                     audit event log).

POST-INCIDENT GUARDS (added after the 2026-04-28 incident in which
~$200 of unrealised gain was bulk-written-off because our own DB
pressure made client-side SELL retries time out, and this script
treated those timeouts as venue rejections):

  1. The script now classifies each stuck order's last_error against
     a set of known venue-rejection markers BEFORE submitting a
     writeoff.  Rows whose last_error looks client-side (TimeoutError,
     ConnectionError, asyncpg protocol errors, etc.) are reported but
     NOT written off — the lifecycle should retry those once the
     underlying issue clears.
  2. The API itself now enforces the same gate
     (``override_venue_rejection_check`` defaults to False); even if
     this script regresses, the writeoff endpoint refuses non-venue
     errors without an explicit override + rationale.
  3. ``--dry-run`` (default) only prints the classification.  Pass
     ``--apply`` to actually fire the writeoffs.

Each successful writeoff produces:

  * row.actual_profit            set to the asserted P&L
  * row.status                   set to closed_loss / closed_win
  * row.verification_status      = manual_writeoff
  * row.payload_json.position_close.operator_id captured
  * an immutable TraderOrderVerificationEvent with the full payload

Re-run-safe: orders that have already been written off (no longer in
OPEN lifecycle status) will get a 400 from the API and be skipped.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request


API_BASE = "http://127.0.0.1:8000/api"
OPERATOR_ID = "homerun-operator"


# Same set as ``services.operator_writeoff._VENUE_REJECTION_MARKERS``
# — duplicated locally because this script is intentionally
# stand-alone (no backend imports) and the API enforces the canonical
# set on the server side anyway.
_VENUE_REJECTION_MARKERS: tuple[str, ...] = (
    "orderbook does not exist",
    "market not tradable",
    "market_tradable=false",
    "market is closed",
    "market closed",
    "market is resolved",
    "trading is closed",
    "not accepting orders",
    "min_order_size",
    "min order size",
    "tick_size",
    "post-only",
    "post only",
    "rejected by venue",
    "rejected by clob",
    "rejected by exchange",
    "invalid order",
    "not enough balance",
    "insufficient balance",
    "insufficient allowance",
)


def _is_venue_rejection(text: object) -> bool:
    if not text:
        return False
    body = str(text).lower()
    return any(marker in body for marker in _VENUE_REJECTION_MARKERS)


def _http_get(url: str) -> dict:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_post(url: str, body: dict) -> tuple[int, dict | str]:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8") if exc.fp else str(exc)
        try:
            return exc.code, json.loads(body_text)
        except json.JSONDecodeError:
            return exc.code, body_text


def _build_reason(row: dict) -> str:
    cs = row.get("chain_status") or {}
    cls = row.get("classification") or "unknown"
    bal = cs.get("wallet_balance_shares")
    resolved = cs.get("market_resolved")
    block_number = cs.get("block_number")
    chain_error = row.get("chain_error") or cs.get("error")
    parts = [
        "Operator manual write-off of a stuck blocked-persistent-timeout order.",
        f"classification={cls}",
        f"chain_balance_shares={bal}",
        f"market_resolved={resolved}",
    ]
    if block_number:
        parts.append(f"block_number={block_number}")
    if chain_error:
        parts.append(f"chain_error={chain_error}")
    pe_status = row.get("pending_exit_status")
    if pe_status:
        parts.append(f"pending_exit_status={pe_status}")
    blocked_streak = row.get("consecutive_blocked_failure_count")
    if blocked_streak:
        parts.append(f"consecutive_blocked_failure_count={blocked_streak}")
    parts.append(
        "CLOB no longer accepting SELL orders for the underlying market; "
        "operator electing full-loss write-off rather than waiting on "
        "uncertain market resolution."
    )
    return " | ".join(parts)


def _last_error_for_row(row: dict) -> str:
    """Pull pending_live_exit.last_error from whichever shape the
    /operator/stuck-orders feed returns it in."""
    pe = row.get("pending_live_exit") or {}
    if isinstance(pe, dict):
        last_error = pe.get("last_error")
        if last_error:
            return str(last_error)
    # Older feed shape: top-level last_error / chain_error.
    for key in ("last_error", "chain_error", "exit_last_error"):
        value = row.get(key)
        if value:
            return str(value)
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Bulk-classify and (with --apply) write off stuck orders.  Default "
            "is --dry-run: print the classification without firing any "
            "writeoffs.  Pass --apply to actually call the manual-writeoff "
            "API for rows whose last_error matches a venue-rejection marker."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually submit the manual-writeoff API call.  Without this "
            "flag the script is a dry run that prints classification only."
        ),
    )
    parser.add_argument(
        "--include-non-venue-errors",
        action="store_true",
        help=(
            "DANGEROUS: also writeoff rows whose last_error is client-side "
            "(timeouts, transport errors, DB pressure).  Requires --apply "
            "and an --override-rationale.  Use only when you have "
            "independently verified the venue is genuinely refusing the "
            "order via another channel."
        ),
    )
    parser.add_argument(
        "--override-rationale",
        type=str,
        default=None,
        help=(
            "Required when --include-non-venue-errors is set: free-form "
            "explanation of why a full-loss realisation is the correct "
            "call despite no venue-rejection evidence."
        ),
    )
    args = parser.parse_args()

    if args.include_non_venue_errors and not args.apply:
        print("--include-non-venue-errors requires --apply", file=sys.stderr)
        return 2
    if args.include_non_venue_errors and not (args.override_rationale or "").strip():
        print(
            "--include-non-venue-errors requires --override-rationale "
            "explaining why this is correct",
            file=sys.stderr,
        )
        return 2

    print(f"Fetching stuck orders from {API_BASE}/operator/stuck-orders ...")
    feed = _http_get(f"{API_BASE}/operator/stuck-orders?age_hours=6")
    rows = feed.get("rows") or []
    print(f"Found {len(rows)} stuck order(s).")
    if not args.apply:
        print("(dry run — pass --apply to fire writeoffs)")
    print()

    succeeded: list[dict] = []
    skipped_classification: list[dict] = []
    skipped_already_terminal: list[dict] = []
    failed: list[dict] = []

    for row in rows:
        order_id = row.get("order_id") or ""
        notional = float(row.get("notional_usd") or 0.0)
        if not order_id:
            failed.append({"reason": "missing order_id", "row": row})
            continue

        last_error = _last_error_for_row(row)
        venue_rejected = _is_venue_rejection(last_error)
        short = order_id[:16]
        last_error_excerpt = last_error[:80] if last_error else "(none)"

        if not venue_rejected and not args.include_non_venue_errors:
            skipped_classification.append({
                "order_id": order_id,
                "last_error": last_error,
            })
            print(
                f"  {short}  SKIPPED      client-side error (last_error={last_error_excerpt!r}); "
                f"lifecycle should retry"
            )
            continue

        if not args.apply:
            print(
                f"  {short}  WOULD WRITEOFF  notional=${notional:.2f}  "
                f"last_error={last_error_excerpt!r}"
            )
            continue

        body = {
            "realized_pnl": -float(notional),
            "reason": _build_reason(row),
            "operator_id": OPERATOR_ID,
            "chain_evidence": row.get("chain_status"),
        }
        if not venue_rejected:
            body["override_venue_rejection_check"] = True
            body["override_rationale"] = args.override_rationale
        url = f"{API_BASE}/operator/orders/{order_id}/manual-writeoff"
        status, resp = _http_post(url, body)

        if status == 200 and isinstance(resp, dict) and resp.get("realized_pnl") is not None:
            succeeded.append({"order_id": order_id, "response": resp})
            print(f"  {short}  WRITTEN OFF  realized_pnl=${resp['realized_pnl']:.2f}  status={resp['next_status']}")
        elif status == 400 and isinstance(resp, dict) and "already terminal" in str(resp.get("detail", "")).lower():
            skipped_already_terminal.append({"order_id": order_id, "reason": "already_terminal"})
            print(f"  {short}  SKIPPED      already terminal (probably written off in a prior pass)")
        else:
            failed.append({"order_id": order_id, "status": status, "response": resp})
            print(f"  {short}  FAILED       status={status}  detail={resp}")

    print()
    print(
        f"Summary: {len(succeeded)} written off, "
        f"{len(skipped_classification)} skipped (client-side error), "
        f"{len(skipped_already_terminal)} skipped (already terminal), "
        f"{len(failed)} failed."
    )
    if failed:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
