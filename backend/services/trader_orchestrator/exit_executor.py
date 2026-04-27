"""Exit Executor — laddered, chunked, escalating position-exit primitives.

The orchestrator's legacy exit path submits one order per close_trigger:
``execute_live_order(size=remaining_exit_size, ...)``. That works for benign
exits but is brittle on Polymarket CLOB during fast drawdowns — one large
sell hits the inside bid and walks the book unfavorably, while a single
post-only limit may rest unfilled past the move.

This module replaces that single shot with a *plan of child orders* derived
from an ``ExitPolicy`` (see ``services.strategies.base``):

* **Ladder** — up to ``policy.ladder.levels`` resting limits stepped away
  from the trigger price by ``policy.ladder.step_ticks``, distributed by
  ``uniform``/``front_loaded``/``back_loaded`` weights.
* **Chunking** — slice the position into many small orders so any single
  fill consumes only a thin slice of book depth.
* **Order-type mix** — fraction of children submitted as IOC (taker) vs.
  GTC (maker) to balance fill speed against price improvement.
* **Escalation** — if a child rests unfilled past ``after_seconds``, cancel
  and resubmit per the escalation action (``marketable_ioc`` is the common
  rapid-exit pattern).
* **Mid-drift reprice** — when the live mid drifts past
  ``reprice_on_mid_drift_bps`` from a child's resting price, cancel and
  re-quote to keep the ladder aligned with the book.

The module is split into:

* **Pure planner** functions (``plan_children``, ``aggregate_children_fills``,
  ``is_exit_complete``, selection helpers). These take dicts/dataclasses and
  return new dicts — fully unit-testable, no I/O.
* **Async submission** functions (``submit_child_order``,
  ``escalate_child_order``). These wrap ``execute_live_order`` and mutate
  the child record in place.

The orchestrator's three exit paths in ``position_lifecycle.py`` (initial
submit, retry-failed, reprice-stale) each branch on
``pending_exit.get("children")``: when present they call into this module;
when absent they keep the legacy single-order code path. Strategies that do
not declare ``exit_policies`` are entirely unaffected.

Idempotency: each child carries a deterministic ``metadata`` key derived from
the trigger event id and the child index, so a duplicate submission produced
by a retry races collapses to the same provider order rather than creating
an orphan.
"""

from __future__ import annotations

import asyncio
import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from utils.converters import safe_float
from utils.logger import get_logger
from utils.utcnow import utcnow

from services.strategies.base import (
    EscalationSpec,
    ExitPolicy,
    LadderSpec,
)

logger = get_logger(__name__)


# Polymarket CLOB venue floor — the live_execution_adapter clamps prices to
# this tick. Mirrors ``_POST_ONLY_REPRICE_TICK`` in live_execution_adapter.py
# and ``POST_ONLY_REPRICE_TICK`` in live_execution_service.py.
DEFAULT_TICK_SIZE = 0.01

# Per-trader per-pass child-submission cap. The orchestrator's existing
# ``_live_exit_submission_cap`` budget is shared across all exits in a
# trader's reconcile pass; this cap further limits how many children of a
# *single* exit can be fired in one pass to prevent one drawdown from
# starving every other position's exit budget.
DEFAULT_PER_PASS_CHILD_CAP = 8


# ── Child-order state machine ─────────────────────────────────────────────
#
# planned   → submitted | failed
# submitted → filled | partial | cancelled | failed | escalated
# partial   → filled | cancelled | escalated
# escalated → submitted | failed   (escalated children are re-planned)
# filled, cancelled, failed (after exhausted retries) are terminal.

CHILD_PLANNED = "planned"
CHILD_SUBMITTED = "submitted"
CHILD_FILLED = "filled"
CHILD_PARTIAL = "partial"
CHILD_CANCELLED = "cancelled"
CHILD_FAILED = "failed"
CHILD_ESCALATED = "escalated"

_TERMINAL_CHILD_STATES = {CHILD_FILLED, CHILD_CANCELLED}
_WORKING_CHILD_STATES = {CHILD_SUBMITTED, CHILD_PARTIAL}


@dataclass
class ChildPlan:
    """Pure-data shape produced by the planner before any submission."""

    index: int
    price: float
    size: float
    tif: str
    post_only: bool
    distribution_bucket: str  # "ladder_lvl_3" / "uniform" / etc — for audit


# ── Pure helpers ──────────────────────────────────────────────────────────


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _round_down_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return float(price)
    steps = math.floor(float(price) / float(tick) + 1e-9)
    return max(float(tick), steps * float(tick))


def _round_up_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return float(price)
    steps = math.ceil(float(price) / float(tick) - 1e-9)
    return max(float(tick), steps * float(tick))


def _clamp_binary_price(price: float, tick: float) -> float:
    p = max(float(tick), min(0.99, float(price)))
    return _round_down_to_tick(p, tick)


# ── Policy serialization ──────────────────────────────────────────────────
#
# We snapshot the policy into pending_exit so subsequent reconcile cycles
# (which load only the row payload, not the live strategy instance) can
# replan/escalate without re-resolving the strategy's class attribute.


def serialize_policy(policy: ExitPolicy) -> dict[str, Any]:
    """Serialize an ExitPolicy into a JSON-safe dict for ``pending_exit``."""
    if policy is None:
        return {}
    out: dict[str, Any] = {
        "max_chunks": int(policy.max_chunks),
        "min_chunk_notional_usd": float(policy.min_chunk_notional_usd),
        "min_reprice_interval_seconds": float(policy.min_reprice_interval_seconds),
    }
    if policy.chunk_size is not None:
        out["chunk_size"] = float(policy.chunk_size)
    if policy.reprice_on_mid_drift_bps is not None:
        out["reprice_on_mid_drift_bps"] = float(policy.reprice_on_mid_drift_bps)
    if policy.ladder is not None:
        out["ladder"] = {
            "levels": int(policy.ladder.levels),
            "step_ticks": int(policy.ladder.step_ticks),
            "offset_ticks": int(getattr(policy.ladder, "offset_ticks", 0) or 0),
            "distribution": str(policy.ladder.distribution or "uniform"),
        }
    if policy.escalation is not None:
        out["escalation"] = {
            "after_seconds": float(policy.escalation.after_seconds),
            "action": str(policy.escalation.action or "marketable_ioc"),
            "widen_bps": (
                float(policy.escalation.widen_bps)
                if policy.escalation.widen_bps is not None
                else None
            ),
            "max_escalations": int(policy.escalation.max_escalations),
        }
    if policy.order_type_mix:
        out["order_type_mix"] = [
            (str(t).upper(), float(w)) for (t, w) in policy.order_type_mix
        ]
    return out


def deserialize_policy(snapshot: dict[str, Any] | None) -> ExitPolicy | None:
    """Inverse of ``serialize_policy``. Returns ``None`` for empty input."""
    if not snapshot or not isinstance(snapshot, dict):
        return None
    ladder = None
    if isinstance(snapshot.get("ladder"), dict):
        l = snapshot["ladder"]
        ladder = LadderSpec(
            levels=int(l.get("levels", 5)),
            step_ticks=int(l.get("step_ticks", 1)),
            offset_ticks=int(l.get("offset_ticks", 0)),
            distribution=str(l.get("distribution") or "uniform"),
        )
    escalation = None
    if isinstance(snapshot.get("escalation"), dict):
        e = snapshot["escalation"]
        escalation = EscalationSpec(
            after_seconds=float(e.get("after_seconds", 5.0)),
            action=str(e.get("action") or "marketable_ioc"),
            widen_bps=(float(e["widen_bps"]) if e.get("widen_bps") is not None else None),
            max_escalations=int(e.get("max_escalations", 1)),
        )
    mix = snapshot.get("order_type_mix")
    parsed_mix: list | None = None
    if isinstance(mix, list) and mix:
        parsed_mix = [(str(t).upper(), float(w)) for (t, w) in mix]
    return ExitPolicy(
        ladder=ladder,
        chunk_size=(
            float(snapshot["chunk_size"]) if snapshot.get("chunk_size") is not None else None
        ),
        max_chunks=int(snapshot.get("max_chunks", 50)),
        order_type_mix=parsed_mix,
        escalation=escalation,
        reprice_on_mid_drift_bps=(
            float(snapshot["reprice_on_mid_drift_bps"])
            if snapshot.get("reprice_on_mid_drift_bps") is not None
            else None
        ),
        min_chunk_notional_usd=float(snapshot.get("min_chunk_notional_usd", 1.0)),
        min_reprice_interval_seconds=float(snapshot.get("min_reprice_interval_seconds", 1.0)),
    )


# ── Distribution math ─────────────────────────────────────────────────────


def _distribution_weights(levels: int, distribution: str) -> list[float]:
    """Return ``levels`` normalized weights summing to 1.0.

    * ``uniform``      = equal weight per level
    * ``front_loaded`` = inside levels (level 0) get more
    * ``back_loaded``  = outside levels (last) get more
    """
    n = max(1, int(levels))
    mode = (distribution or "uniform").strip().lower()
    if mode == "front_loaded":
        raw = [float(n - i) for i in range(n)]  # n, n-1, ..., 1
    elif mode == "back_loaded":
        raw = [float(i + 1) for i in range(n)]  # 1, 2, ..., n
    else:
        raw = [1.0] * n
    total = sum(raw)
    if total <= 0:
        return [1.0 / n] * n
    return [w / total for w in raw]


def _resolve_order_type_for_index(
    index: int,
    total: int,
    mix: list | None,
    default_tif: str,
    default_post_only: bool,
) -> tuple[str, bool]:
    """Pick (tif, post_only) for child ``index`` given the policy mix.

    ``mix`` is a list of (tif, weight) pairs. Total counts of each tif are
    proportional to weights. Within a sequence of indexes, the most
    aggressive (IOC/FAK/FOK) types are placed first so the inside-most rungs
    of a SELL ladder fire fastest — matching chris's intent of mixing
    "some at 0% slippage, some at market".
    """
    if not mix:
        return default_tif, default_post_only
    total_weight = sum(max(0.0, float(w)) for _, w in mix)
    if total_weight <= 0 or total <= 0:
        return default_tif, default_post_only

    counts: list[tuple[str, int]] = []
    assigned = 0
    for tif, weight in mix:
        n = int(round(total * (max(0.0, float(weight)) / total_weight)))
        counts.append((str(tif).upper(), n))
        assigned += n
    if assigned < total:
        # rounding shortfall: assign extras to the largest bucket
        idx_largest = max(range(len(counts)), key=lambda i: counts[i][1] or 1)
        counts[idx_largest] = (counts[idx_largest][0], counts[idx_largest][1] + (total - assigned))
    elif assigned > total:
        idx_largest = max(range(len(counts)), key=lambda i: counts[i][1] or 1)
        counts[idx_largest] = (
            counts[idx_largest][0],
            max(0, counts[idx_largest][1] - (assigned - total)),
        )

    aggression_rank = {"FOK": 0, "IOC": 1, "FAK": 1, "GTC": 2, "GTD": 3}
    counts.sort(key=lambda kv: aggression_rank.get(kv[0], 9))

    cursor = 0
    for tif, n in counts:
        if index < cursor + n:
            tif_norm = tif.upper()
            if tif_norm in {"IOC", "FAK", "FOK"}:
                return tif_norm, False
            return tif_norm, default_post_only
        cursor += n
    return default_tif, default_post_only


# ── Planner ───────────────────────────────────────────────────────────────


def plan_children(
    *,
    target_size: float,
    trigger_price: float,
    side: str,
    policy: ExitPolicy,
    tick_size: float = DEFAULT_TICK_SIZE,
    min_chunk_notional_usd: float | None = None,
    default_tif: str = "GTC",
    default_post_only: bool = True,
) -> list[ChildPlan]:
    """Plan a list of child orders for an exit.

    Args:
        target_size: total contracts to exit (must be > 0).
        trigger_price: the strategy-supplied close_price; the inside-most
            ladder rung sits at this price.
        side: "SELL" (typical exit) or "BUY".
        policy: the ExitPolicy to apply.
        tick_size: venue tick; used to round each rung's price.
        min_chunk_notional_usd: override for the min notional per child;
            defaults to ``policy.min_chunk_notional_usd``.
        default_tif: TIF used for any child not assigned by ``order_type_mix``.
        default_post_only: post_only flag for non-marketable children.

    Returns:
        Possibly-empty list of ChildPlan. Empty means "fall back to legacy
        single-order path" — the planner could not satisfy min-notional or
        the policy is empty.
    """
    if target_size <= 0 or trigger_price <= 0 or policy is None:
        return []

    side_norm = (side or "SELL").upper()
    sell = side_norm == "SELL"
    min_notional = float(
        min_chunk_notional_usd
        if min_chunk_notional_usd is not None
        else policy.min_chunk_notional_usd
    )

    levels = policy.ladder.levels if policy.ladder is not None else 1
    levels = max(1, min(int(policy.max_chunks), int(levels)))
    step_ticks = max(0, int(policy.ladder.step_ticks)) if policy.ladder is not None else 0
    offset_ticks = max(0, int(getattr(policy.ladder, "offset_ticks", 0))) if policy.ladder is not None else 0
    distribution = (
        policy.ladder.distribution
        if policy.ladder is not None
        else "uniform"
    )

    # Build base price levels (inside-most first). For SELL, levels descend.
    # The first rung sits ``offset_ticks * tick_size`` away from the trigger
    # (toward the inside) — set offset_ticks > 0 to make the ladder cross
    # the spread and become immediately marketable.
    raw_prices: list[float] = []
    for i in range(levels):
        offset = (offset_ticks + i * step_ticks) * tick_size
        if sell:
            raw = float(trigger_price) - offset
        else:
            raw = float(trigger_price) + offset
        raw_prices.append(_clamp_binary_price(raw, tick_size))

    # Drop duplicate prices that collapsed under the tick clamp.
    seen_prices: set[float] = set()
    unique_prices: list[float] = []
    for p in raw_prices:
        if p in seen_prices:
            continue
        seen_prices.add(p)
        unique_prices.append(p)
    if not unique_prices:
        return []

    weights = _distribution_weights(len(unique_prices), distribution)

    # Two planning regimes:
    #
    #   (A) chunk_size set  → emit fixed-size child orders. Total count is
    #       ceil(target / chunk_size), capped at ``policy.max_chunks``. The
    #       distribution decides how many of those fixed-size chunks go at
    #       each price level (proportional to the level's weight). Each
    #       child has ``size == chunk_size`` (last one may be smaller).
    #
    #   (B) chunk_size unset → one child per level, size = level_alloc[i]
    #       (the level's share of target_size per the distribution weight).
    #
    # Mode (A) matches chris's "50 orders of 2 shares each across 10 price
    # levels"; mode (B) is the simpler "one resting order per rung".

    plans: list[ChildPlan] = []
    flat_index = 0

    if policy.chunk_size is not None and policy.chunk_size > 0:
        nominal_chunk_size = float(policy.chunk_size)
        # Per-level allocation in contracts.
        level_alloc = [w * float(target_size) for w in weights]
        # Total chunks budget across all levels.
        nominal_chunks = max(1, math.ceil(float(target_size) / nominal_chunk_size))
        total_chunks_cap = min(int(policy.max_chunks), int(nominal_chunks))

        # For each level, derive the effective chunk size that satisfies
        # min-notional at that level's price, and the number of chunks.
        # A level may end up with zero chunks when its allocation is too
        # small to even produce one min-notional order — its size rolls
        # forward into the next level the loop visits.
        roll_forward = 0.0
        per_level: list[tuple[int, float]] = []  # (n_chunks, effective_chunk_size)
        for level_idx, price in enumerate(unique_prices):
            level_size = level_alloc[level_idx] + roll_forward
            roll_forward = 0.0
            min_chunk_for_price = (
                min_notional / price if price > 0 else nominal_chunk_size
            )
            effective_chunk = max(nominal_chunk_size, min_chunk_for_price)
            if level_size + 1e-9 < min_chunk_for_price:
                # Can't even fit one min-notional chunk at this level —
                # carry to the next level.
                roll_forward = level_size
                per_level.append((0, 0.0))
                continue
            n_chunks = max(1, int(math.floor(level_size / effective_chunk)))
            per_level.append((n_chunks, effective_chunk))
        # Trailing roll-forward (last level couldn't absorb): credit it
        # back to the most aggressive level that already has chunks. The
        # leftover loop below redistributes any per-chunk-rounding slack.
        if roll_forward > 0:
            for k in reversed(range(len(per_level))):
                if per_level[k][0] > 0:
                    n, ec = per_level[k]
                    per_level[k] = (n + max(1, int(math.floor(roll_forward / ec))), ec)
                    break

        # Cap by max_chunks: peel chunks off lowest-weighted levels until
        # we're under the cap.
        total_chunks = sum(n for n, _ in per_level)
        while total_chunks > total_chunks_cap:
            # Find the level with the most chunks AND the lowest priority
            # (smallest weight) to peel. Ties broken by smaller weight.
            candidates = [i for i, (n, _) in enumerate(per_level) if n > 0]
            if not candidates:
                break
            k = min(candidates, key=lambda i: (weights[i], -per_level[i][0]))
            per_level[k] = (per_level[k][0] - 1, per_level[k][1])
            total_chunks -= 1

        sum_chunks = total_chunks
        remaining_size = float(target_size)
        for level_idx, (n_at_level, eff_chunk) in enumerate(per_level):
            if n_at_level <= 0 or eff_chunk <= 0:
                continue
            price = unique_prices[level_idx]
            for _ in range(n_at_level):
                if remaining_size <= 0:
                    break
                sz = min(eff_chunk, remaining_size)
                tif, post_only = _resolve_order_type_for_index(
                    flat_index, sum_chunks, policy.order_type_mix,
                    default_tif=default_tif, default_post_only=default_post_only,
                )
                plans.append(
                    ChildPlan(
                        index=flat_index,
                        price=price,
                        size=sz,
                        tif=tif,
                        post_only=post_only,
                        distribution_bucket=f"lvl{level_idx}",
                    )
                )
                flat_index += 1
                remaining_size -= sz
            if remaining_size <= 0:
                break
        # Any leftover (rounding remainder) attaches to the inside-most
        # surviving child so total size matches target.
        if remaining_size > 1e-9 and plans:
            inside = plans[0]
            plans[0] = ChildPlan(
                index=inside.index,
                price=inside.price,
                size=inside.size + remaining_size,
                tif=inside.tif,
                post_only=inside.post_only,
                distribution_bucket=inside.distribution_bucket,
            )
    else:
        sum_chunks = len(unique_prices)
        for level_idx, price in enumerate(unique_prices):
            size_at_level = weights[level_idx] * float(target_size)
            tif, post_only = _resolve_order_type_for_index(
                flat_index, sum_chunks, policy.order_type_mix,
                default_tif=default_tif, default_post_only=default_post_only,
            )
            plans.append(
                ChildPlan(
                    index=flat_index,
                    price=price,
                    size=size_at_level,
                    tif=tif,
                    post_only=post_only,
                    distribution_bucket=f"lvl{level_idx}",
                )
            )
            flat_index += 1

    # Final safety pass: any plan that still violates min-notional (the
    # ``ladder`` mode without chunk_size can produce these on tiny levels)
    # gets its size promoted to clear the floor. If that's impossible we
    # drop the plan and roll its size into the most aggressive survivor.
    survivors: list[ChildPlan] = []
    forfeited_size = 0.0
    for plan in plans:
        if plan.price <= 0 or plan.size <= 0:
            forfeited_size += plan.size
            continue
        if plan.price * plan.size + 1e-9 < min_notional:
            min_size_at_price = min_notional / plan.price
            # Promote in place if total size still <= target (no double-spend).
            if min_size_at_price <= float(target_size) + 1e-9:
                survivors.append(
                    ChildPlan(
                        index=plan.index,
                        price=plan.price,
                        size=min_size_at_price,
                        tif=plan.tif,
                        post_only=plan.post_only,
                        distribution_bucket=plan.distribution_bucket,
                    )
                )
            else:
                forfeited_size += plan.size
            continue
        survivors.append(plan)

    if not survivors:
        base_price = unique_prices[0]
        if base_price * float(target_size) + 1e-9 < min_notional:
            return []
        return [
            ChildPlan(
                index=0,
                price=base_price,
                size=float(target_size),
                tif=default_tif,
                post_only=default_post_only,
                distribution_bucket="lvl0",
            )
        ]

    if forfeited_size > 0:
        survivors[0] = ChildPlan(
            index=survivors[0].index,
            price=survivors[0].price,
            size=survivors[0].size + forfeited_size,
            tif=survivors[0].tif,
            post_only=survivors[0].post_only,
            distribution_bucket=survivors[0].distribution_bucket,
        )

    # Re-index 0..N-1 contiguously so consumers can iterate by position.
    return [
        ChildPlan(
            index=i,
            price=p.price,
            size=p.size,
            tif=p.tif,
            post_only=p.post_only,
            distribution_bucket=p.distribution_bucket,
        )
        for i, p in enumerate(survivors)
    ]


def child_plan_to_record(plan: ChildPlan, *, exit_id: str) -> dict[str, Any]:
    """Materialize a ChildPlan into the dict shape stored in pending_exit."""
    return {
        "child_id": f"{exit_id}-c{plan.index}",
        "index": plan.index,
        "price": float(plan.price),
        "size": float(plan.size),
        "tif": str(plan.tif),
        "post_only": bool(plan.post_only),
        "bucket": plan.distribution_bucket,
        "status": CHILD_PLANNED,
        "filled_size": 0.0,
        "average_fill_price": None,
        "exit_order_id": None,
        "provider_clob_order_id": None,
        "submitted_at": None,
        "last_attempt_at": None,
        "escalation_count": 0,
        "last_error": None,
        "metadata_key": f"exit:{exit_id}:c{plan.index}",
    }


def build_initial_children(
    *,
    target_size: float,
    trigger_price: float,
    side: str,
    policy: ExitPolicy,
    tick_size: float = DEFAULT_TICK_SIZE,
    exit_id: str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Convenience wrapper returning ``(exit_id, list[child_dict])``.

    Returns an empty list if the planner could not produce any children
    (caller should fall back to legacy single-order path).
    """
    eid = exit_id or uuid.uuid4().hex[:12]
    plans = plan_children(
        target_size=target_size,
        trigger_price=trigger_price,
        side=side,
        policy=policy,
        tick_size=tick_size,
    )
    return eid, [child_plan_to_record(p, exit_id=eid) for p in plans]


# ── Aggregation / completion ──────────────────────────────────────────────


def aggregate_children_fills(
    children: list[dict[str, Any]],
) -> dict[str, float]:
    """Sum fills across all children. Returns dict with keys
    ``filled_size``, ``filled_notional``, ``average_fill_price`` (notional /
    filled_size, or 0.0 if nothing filled).
    """
    total_size = 0.0
    total_notional = 0.0
    for c in children or []:
        f = max(0.0, safe_float(c.get("filled_size"), 0.0) or 0.0)
        if f <= 0:
            continue
        avg = safe_float(c.get("average_fill_price"))
        if avg is None or avg <= 0:
            avg = safe_float(c.get("price"), 0.0) or 0.0
        total_size += f
        total_notional += f * float(avg)
    avg_price = (total_notional / total_size) if total_size > 0 else 0.0
    return {
        "filled_size": float(total_size),
        "filled_notional": float(total_notional),
        "average_fill_price": float(avg_price),
    }


def is_exit_complete(
    pending_exit: dict[str, Any],
    *,
    fill_threshold_ratio: float = 1.0,
) -> bool:
    """True when the laddered exit has finished.

    Completion is the *first* of:

    * Cumulative fills across children >= ``target_size * fill_threshold_ratio``
    * No working (submitted/partial) or planned children remain — i.e. every
      child is in a terminal state (filled/cancelled/failed). At that point
      the exit is "done" even if some size was forfeited (e.g. all-cancelled
      because the position was closed externally).
    """
    children = pending_exit.get("children") or []
    if not children:
        return False
    target = max(0.0, safe_float(pending_exit.get("target_size"), 0.0) or 0.0)
    agg = aggregate_children_fills(children)
    if target > 0 and agg["filled_size"] + 1e-9 >= target * fill_threshold_ratio:
        return True
    # No more work to do?
    for c in children:
        st = str(c.get("status") or "").strip().lower()
        if st == CHILD_PLANNED or st in _WORKING_CHILD_STATES or st == CHILD_ESCALATED:
            return False
    return True


def remaining_target_size(pending_exit: dict[str, Any]) -> float:
    target = max(0.0, safe_float(pending_exit.get("target_size"), 0.0) or 0.0)
    if target <= 0:
        return 0.0
    agg = aggregate_children_fills(pending_exit.get("children") or [])
    return max(0.0, target - agg["filled_size"])


# ── Selection helpers (pure) ──────────────────────────────────────────────


def pick_children_to_submit(
    pending_exit: dict[str, Any],
    *,
    max_count: int = DEFAULT_PER_PASS_CHILD_CAP,
) -> list[int]:
    """Indexes of ``planned`` children to submit on this pass."""
    children = pending_exit.get("children") or []
    out: list[int] = []
    for i, c in enumerate(children):
        if str(c.get("status") or "").strip().lower() == CHILD_PLANNED:
            out.append(i)
            if len(out) >= max_count:
                break
    return out


def pick_children_to_reprice(
    pending_exit: dict[str, Any],
    *,
    current_mid: float,
    now: datetime,
    tick_size: float = DEFAULT_TICK_SIZE,
) -> list[int]:
    """Indexes of ``submitted``/``partial`` children whose price is stale.

    Uses ``policy.reprice_on_mid_drift_bps``. A child is stale when:

      |child.price - current_mid| / current_mid * 10000 >= drift_bps

    Honors the per-policy ``min_reprice_interval_seconds`` debounce so we
    don't hammer cancel/replace.
    """
    policy_snap = pending_exit.get("policy") or {}
    drift_bps = policy_snap.get("reprice_on_mid_drift_bps")
    if drift_bps is None or current_mid <= 0:
        return []
    drift_bps = float(drift_bps)
    min_interval = float(policy_snap.get("min_reprice_interval_seconds", 1.0))
    children = pending_exit.get("children") or []
    out: list[int] = []
    for i, c in enumerate(children):
        st = str(c.get("status") or "").strip().lower()
        if st not in _WORKING_CHILD_STATES:
            continue
        price = safe_float(c.get("price"))
        if price is None or price <= 0:
            continue
        delta_bps = abs(price - current_mid) / current_mid * 10000.0
        if delta_bps + 1e-9 < drift_bps:
            continue
        last_attempt = _parse_iso(c.get("last_attempt_at"))
        if last_attempt is not None:
            elapsed = (now - last_attempt).total_seconds()
            if elapsed + 1e-9 < min_interval:
                continue
        out.append(i)
    return out


def pick_children_to_escalate(
    pending_exit: dict[str, Any],
    *,
    now: datetime,
) -> list[int]:
    """Indexes of children that have rested too long under the escalation
    spec and have not exhausted their escalation budget.
    """
    policy_snap = pending_exit.get("policy") or {}
    esc = policy_snap.get("escalation")
    if not isinstance(esc, dict):
        return []
    after_seconds = float(esc.get("after_seconds", 5.0))
    max_escalations = int(esc.get("max_escalations", 1))
    children = pending_exit.get("children") or []
    out: list[int] = []
    for i, c in enumerate(children):
        st = str(c.get("status") or "").strip().lower()
        if st not in _WORKING_CHILD_STATES:
            continue
        if int(c.get("escalation_count", 0) or 0) >= max_escalations:
            continue
        submitted_at = _parse_iso(c.get("submitted_at") or c.get("last_attempt_at"))
        if submitted_at is None:
            continue
        rested = (now - submitted_at).total_seconds()
        if rested + 1e-9 < after_seconds:
            continue
        out.append(i)
    return out


# ── Async submission helpers ──────────────────────────────────────────────


async def submit_child_order(
    *,
    child: dict[str, Any],
    token_id: str,
    side: str,
    min_order_size_usd: float,
    place_order: Callable[..., Any],
    now: datetime | None = None,
    market_question: str | None = None,
) -> dict[str, Any]:
    """Submit a single child order and update its record in place.

    ``place_order`` is the bound async callable (typically
    ``execute_live_order``). Passing it as a parameter keeps this function
    pure-by-injection and easy to unit-test with a fake.

    Returns the same ``child`` dict (mutated). On success transitions to
    ``submitted`` (or ``filled`` if the venue immediately filled). On
    failure transitions to ``failed`` with ``last_error`` populated.
    """
    now = now or utcnow()
    child["last_attempt_at"] = _iso_utc(now)
    try:
        result = await place_order(
            token_id=token_id,
            side=side,
            size=float(child["size"]),
            fallback_price=float(child["price"]),
            min_order_size_usd=float(min_order_size_usd),
            market_question=market_question,
            time_in_force=str(child.get("tif") or "GTC"),
            post_only=bool(child.get("post_only", False)),
            resolve_live_price=False,
            enforce_fallback_bound=True,
            metadata=str(child.get("metadata_key") or ""),
        )
    except Exception as exc:
        child["status"] = CHILD_FAILED
        child["last_error"] = f"{type(exc).__name__}: {exc}" or "submit_exception"
        return child

    status = str(getattr(result, "status", "") or "").strip().lower()
    payload = getattr(result, "payload", None) or {}
    child["exit_order_id"] = str(getattr(result, "order_id", "") or "") or None
    child["provider_clob_order_id"] = str(payload.get("clob_order_id") or "") or None

    incremental_fill = max(0.0, safe_float(payload.get("filled_size"), 0.0) or 0.0)
    if incremental_fill > 0:
        prior = max(0.0, safe_float(child.get("filled_size"), 0.0) or 0.0)
        child["filled_size"] = float(prior + incremental_fill)
        avg = safe_float(payload.get("average_fill_price"))
        if avg is not None and avg > 0:
            child["average_fill_price"] = float(avg)

    if status in {"executed"}:
        if child["filled_size"] + 1e-9 >= float(child["size"]):
            child["status"] = CHILD_FILLED
        else:
            child["status"] = CHILD_PARTIAL
    elif status in {"open", "submitted"}:
        child["status"] = CHILD_SUBMITTED
        child["submitted_at"] = child.get("submitted_at") or _iso_utc(now)
    else:
        child["status"] = CHILD_FAILED
        child["last_error"] = (
            getattr(result, "error_message", None) or f"submit_status:{status or 'unknown'}"
        )
    return child


async def cancel_child_order(
    *,
    child: dict[str, Any],
    cancel_fn: Callable[[str], Any],
) -> bool:
    """Cancel the resting provider order tied to ``child``. Returns True
    when the venue acknowledged the cancel (or there was nothing to cancel).
    """
    clob_id = str(child.get("provider_clob_order_id") or child.get("exit_order_id") or "").strip()
    if not clob_id:
        return True
    try:
        result = await cancel_fn(clob_id)
        return bool(result)
    except Exception:
        return False


async def escalate_child_order(
    *,
    child: dict[str, Any],
    token_id: str,
    side: str,
    min_order_size_usd: float,
    policy_snapshot: dict[str, Any],
    place_order: Callable[..., Any],
    cancel_fn: Callable[[str], Any],
    current_mid: float | None = None,
    tick_size: float = DEFAULT_TICK_SIZE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Cancel a stale child and resubmit per the escalation action.

    ``policy_snapshot`` is ``pending_exit["policy"]`` (already deserialized
    from the pending_exit dict). ``current_mid`` is required for
    ``marketable_ioc``; if missing we fall back to the child's original
    price.
    """
    now = now or utcnow()
    esc = policy_snapshot.get("escalation") if isinstance(policy_snapshot, dict) else None
    if not isinstance(esc, dict):
        return child

    action = str(esc.get("action") or "marketable_ioc").strip().lower()
    cancelled = await cancel_child_order(child=child, cancel_fn=cancel_fn)
    if not cancelled:
        # leave child as-is for the next cycle
        child["last_error"] = "escalation_cancel_failed"
        return child

    child["status"] = CHILD_ESCALATED
    child["escalation_count"] = int(child.get("escalation_count", 0) or 0) + 1

    if action == "abort":
        child["status"] = CHILD_CANCELLED
        return child

    if action == "marketable_ioc":
        # Resubmit as IOC at live mid (taker). Falls back to original price
        # if mid is unavailable.
        new_price = (
            _clamp_binary_price(float(current_mid), tick_size)
            if current_mid is not None and current_mid > 0
            else float(child["price"])
        )
        child["price"] = new_price
        child["tif"] = "IOC"
        child["post_only"] = False
    elif action == "widen_bps":
        widen = safe_float(esc.get("widen_bps"))
        if widen is None or widen <= 0:
            widen = 25.0
        # Move the price toward the inside (toward mid) by widen_bps.
        current = float(child["price"])
        ref = current_mid if (current_mid is not None and current_mid > 0) else current
        # For SELL exits, "more aggressive" = lower price; for BUY exits = higher.
        if (side or "SELL").upper() == "SELL":
            new_raw = current * (1.0 - widen / 10000.0)
        else:
            new_raw = current * (1.0 + widen / 10000.0)
        new_price = _clamp_binary_price(new_raw, tick_size)
        child["price"] = new_price
        # keep TIF/post_only as-is

    # Reset transient fields and resubmit
    child["provider_clob_order_id"] = None
    child["exit_order_id"] = None
    child["submitted_at"] = None
    return await submit_child_order(
        child=child,
        token_id=token_id,
        side=side,
        min_order_size_usd=min_order_size_usd,
        place_order=place_order,
        now=now,
    )


async def reprice_child_order(
    *,
    child: dict[str, Any],
    token_id: str,
    side: str,
    min_order_size_usd: float,
    new_price: float,
    place_order: Callable[..., Any],
    cancel_fn: Callable[[str], Any],
    tick_size: float = DEFAULT_TICK_SIZE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Cancel and resubmit the child at ``new_price``. Used when the live
    mid has drifted past the policy's reprice threshold.
    """
    now = now or utcnow()
    cancelled = await cancel_child_order(child=child, cancel_fn=cancel_fn)
    if not cancelled:
        child["last_error"] = "reprice_cancel_failed"
        return child

    child["price"] = _clamp_binary_price(float(new_price), tick_size)
    child["provider_clob_order_id"] = None
    child["exit_order_id"] = None
    child["submitted_at"] = None
    child["status"] = CHILD_PLANNED
    return await submit_child_order(
        child=child,
        token_id=token_id,
        side=side,
        min_order_size_usd=min_order_size_usd,
        place_order=place_order,
        now=now,
    )


# ── Public entry point: submit/escalate/reprice in one pass ───────────────


@dataclass
class PassBudget:
    """How many submissions/escalations/reprices remain in this pass."""

    submits: int
    escalations: int
    reprices: int

    def consume_submit(self) -> bool:
        if self.submits <= 0:
            return False
        self.submits -= 1
        return True

    def consume_escalation(self) -> bool:
        if self.escalations <= 0:
            return False
        self.escalations -= 1
        return True

    def consume_reprice(self) -> bool:
        if self.reprices <= 0:
            return False
        self.reprices -= 1
        return True


async def run_exit_pass(
    *,
    pending_exit: dict[str, Any],
    token_id: str,
    side: str,
    min_order_size_usd: float,
    place_order: Callable[..., Any],
    cancel_fn: Callable[[str], Any],
    current_mid: float | None,
    tick_size: float = DEFAULT_TICK_SIZE,
    budget: PassBudget | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """One reconcile-cycle of work on a laddered exit.

    Mutates ``pending_exit["children"]`` in place. Returns a small report
    dict the caller can log/audit:

      {
        "submitted": int, "escalated": int, "repriced": int,
        "complete": bool, "filled_size": float
      }

    The caller is responsible for persisting ``pending_exit`` back to the
    row payload after this returns.
    """
    now = now or utcnow()
    report = {"submitted": 0, "escalated": 0, "repriced": 0}
    children = pending_exit.get("children") or []
    if not children:
        report["complete"] = True
        report["filled_size"] = 0.0
        return report

    budget = budget or PassBudget(
        submits=DEFAULT_PER_PASS_CHILD_CAP,
        escalations=DEFAULT_PER_PASS_CHILD_CAP,
        reprices=DEFAULT_PER_PASS_CHILD_CAP,
    )
    policy_snap = pending_exit.get("policy") or {}

    # 1. Submit any planned children
    for idx in pick_children_to_submit(pending_exit, max_count=budget.submits):
        if not budget.consume_submit():
            break
        await submit_child_order(
            child=children[idx],
            token_id=token_id,
            side=side,
            min_order_size_usd=min_order_size_usd,
            place_order=place_order,
            now=now,
        )
        report["submitted"] += 1

    # 2. Escalate stale resting children
    for idx in pick_children_to_escalate(pending_exit, now=now):
        if not budget.consume_escalation():
            break
        await escalate_child_order(
            child=children[idx],
            token_id=token_id,
            side=side,
            min_order_size_usd=min_order_size_usd,
            policy_snapshot=policy_snap,
            place_order=place_order,
            cancel_fn=cancel_fn,
            current_mid=current_mid,
            tick_size=tick_size,
            now=now,
        )
        report["escalated"] += 1

    # 3. Reprice drifted children
    if current_mid is not None and current_mid > 0:
        for idx in pick_children_to_reprice(
            pending_exit, current_mid=current_mid, now=now, tick_size=tick_size
        ):
            if not budget.consume_reprice():
                break
            # Slide each child's price by the same delta as the mid drift,
            # preserving the level offset relative to mid.
            child = children[idx]
            old_price = safe_float(child.get("price"), 0.0) or 0.0
            # Rebuild the price = mid - (level_idx * step_ticks * tick) for SELL.
            # We don't know the original mid, so use a simple anchor: snap to
            # the new mid less the original offset.
            offset = float(child.get("price", 0.0)) - float(
                pending_exit.get("trigger_price") or current_mid
            )
            new_price = current_mid + offset
            await reprice_child_order(
                child=child,
                token_id=token_id,
                side=side,
                min_order_size_usd=min_order_size_usd,
                new_price=new_price,
                place_order=place_order,
                cancel_fn=cancel_fn,
                tick_size=tick_size,
                now=now,
            )
            report["repriced"] += 1

    # 4. Aggregate
    pending_exit["children"] = children
    agg = aggregate_children_fills(children)
    pending_exit["children_filled_size"] = agg["filled_size"]
    pending_exit["children_filled_notional"] = agg["filled_notional"]
    pending_exit["children_average_fill_price"] = agg["average_fill_price"]
    pending_exit["last_pass_at"] = _iso_utc(now)
    pending_exit["last_pass_report"] = dict(report)

    report["filled_size"] = agg["filled_size"]
    report["complete"] = is_exit_complete(pending_exit)
    return report


__all__ = [
    "ChildPlan",
    "PassBudget",
    "DEFAULT_TICK_SIZE",
    "DEFAULT_PER_PASS_CHILD_CAP",
    "CHILD_PLANNED",
    "CHILD_SUBMITTED",
    "CHILD_FILLED",
    "CHILD_PARTIAL",
    "CHILD_CANCELLED",
    "CHILD_FAILED",
    "CHILD_ESCALATED",
    "plan_children",
    "build_initial_children",
    "child_plan_to_record",
    "serialize_policy",
    "deserialize_policy",
    "aggregate_children_fills",
    "is_exit_complete",
    "remaining_target_size",
    "pick_children_to_submit",
    "pick_children_to_reprice",
    "pick_children_to_escalate",
    "submit_child_order",
    "cancel_child_order",
    "escalate_child_order",
    "reprice_child_order",
    "run_exit_pass",
]
