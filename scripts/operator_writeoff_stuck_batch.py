"""One-shot operator script: write off all currently-stuck orders.

Reads the live system's stuck-order queue, then for each row hits the
manual-writeoff API with:

  * realized_pnl   = -notional_usd  (full loss; operator's conservative
                                     assertion since the markets are
                                     unresolved + untradable + the CLOB
                                     no longer accepts orders for them)
  * reason         = explains the chain evidence at decision time
  * operator_id    = "homerun-operator"
  * chain_evidence = the on-chain status snapshot (immutable in the
                     audit event log).

Each writeoff produces:

  * row.actual_profit            set to the asserted P&L
  * row.status                   set to closed_loss / closed_win
  * row.verification_status      = manual_writeoff
  * row.payload_json.position_close.operator_id captured
  * an immutable TraderOrderVerificationEvent with the full payload

Re-run-safe: orders that have already been written off (no longer in
OPEN lifecycle status) will get a 400 from the API and be skipped.
"""

from __future__ import annotations

import json
import sys
import urllib.request


API_BASE = "http://127.0.0.1:8000/api"
OPERATOR_ID = "homerun-operator"


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


def main() -> int:
    print(f"Fetching stuck orders from {API_BASE}/operator/stuck-orders ...")
    feed = _http_get(f"{API_BASE}/operator/stuck-orders?age_hours=6")
    rows = feed.get("rows") or []
    print(f"Found {len(rows)} stuck order(s).")
    print()

    succeeded: list[dict] = []
    skipped: list[dict] = []
    failed: list[dict] = []

    for row in rows:
        order_id = row.get("order_id") or ""
        notional = float(row.get("notional_usd") or 0.0)
        if not order_id:
            failed.append({"reason": "missing order_id", "row": row})
            continue

        body = {
            "realized_pnl": -float(notional),
            "reason": _build_reason(row),
            "operator_id": OPERATOR_ID,
            "chain_evidence": row.get("chain_status"),
        }
        url = f"{API_BASE}/operator/orders/{order_id}/manual-writeoff"
        status, resp = _http_post(url, body)

        short = order_id[:16]
        if status == 200 and isinstance(resp, dict) and resp.get("realized_pnl") is not None:
            succeeded.append({"order_id": order_id, "response": resp})
            print(f"  {short}  WRITTEN OFF  realized_pnl=${resp['realized_pnl']:.2f}  status={resp['next_status']}")
        elif status == 400 and isinstance(resp, dict) and "already terminal" in str(resp.get("detail", "")).lower():
            skipped.append({"order_id": order_id, "reason": "already_terminal"})
            print(f"  {short}  SKIPPED      already terminal (probably written off in a prior pass)")
        else:
            failed.append({"order_id": order_id, "status": status, "response": resp})
            print(f"  {short}  FAILED       status={status}  detail={resp}")

    print()
    print(f"Summary: {len(succeeded)} written off, {len(skipped)} skipped, {len(failed)} failed.")
    if failed:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
