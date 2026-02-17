# Reliability Remediation Tasks

## Critical
- [x] Add failed `TraderDecision` writes when signal processing throws (audit trail preserved).
- [x] Mark failed signals with `trade_signals.status = "failed"` in orchestrator error path.
- [x] Remove unsafe live-order token fallback to `market_id`; require explicit token context.
- [x] Add single-owner orchestrator cycle lock for multi-process safety.

## High
- [x] Add bounded exponential retry/backoff for crypto WS feed startup.
- [x] Add token-resolution provenance (`token_id_source`) and debug attempts for rejection payloads.
- [x] Add cross-instance exposure serialization for order admission via orchestrator cycle lock.

## Medium
- [x] Debounce and batch frontend query invalidations triggered by WS messages.
- [x] Gate high-cost invalidations by active tab/view to avoid cache thrash.
- [x] Memoize heavy opportunity list item components (`OpportunityCard`, table rows).

## Time Consistency
- [x] Standardize touched orchestrator persistence timestamps to `utils.utcnow.utcnow()`.
- [ ] Sweep remaining `datetime.utcnow()` in backend worker/service modules.

## Exception Hygiene
- [x] Ensure cancellation is never swallowed in per-signal orchestrator processing.
- [ ] Split selected broad `except Exception` blocks into retryable/config/fatal classes.

## Validation
- [ ] Run targeted backend tests for orchestrator + order manager + crypto worker behavior. (blocked: missing `httpx` in this runtime)
- [x] Run frontend type/lint checks for hook and memoization changes. (`npm run build`)
- [ ] Optional: add regression tests for WS invalidation batching behavior.
