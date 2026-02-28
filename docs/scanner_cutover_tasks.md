# Scanner Architecture Cutover Tasks

This document defines the clean cutover from the legacy scanner loop to the split-lane scanner architecture.

## Acceptance Criteria

1. Fast scan lane and heavy scan lane run independently without head-of-line blocking.
2. Scanner status exposes unambiguous lane timestamps and freshness metadata.
3. Opportunity lifecycle tracks immutable first detection and mutable recency/freshness fields.
4. API/WS payloads expose freshness fields used by UI and trading.
5. UI renders freshness from recency fields, not immutable first-detection age.
6. Trading path rejects stale scanner opportunities/signals deterministically.
7. Worker heartbeats continue even while heavy lane is running.
8. Scanner lane watchdogs prevent permanent stuck state.

## Implementation Tasks

1. Split scanner worker loop into:
   - `fast_lane_loop` (reactive/timer incremental scanning)
   - `heavy_lane_loop` (full-snapshot strategies, chunked by cap)
   - `snapshot_publisher_loop` (heartbeat + snapshot writes)
2. Add lane runtime state:
   - `fast_last_started_at`
   - `fast_last_completed_at`
   - `heavy_last_started_at`
   - `heavy_last_completed_at`
   - `fast_inflight`
   - `heavy_inflight`
3. Add opportunity freshness fields:
   - `first_detected_at` (immutable per stable id)
   - `last_detected_at` (updated when rediscovered)
   - `last_priced_at` (updated when live market price refreshed)
4. Update merge and refresh logic to maintain freshness fields for each stable id.
5. Update scanner status payload:
   - keep legacy `last_scan`
   - add `last_fast_scan`
   - add `last_heavy_scan`
   - add lane watchdog metrics
6. Update API opportunity serialization to include freshness fields.
7. Update websocket init/opportunity update payloads to include freshness fields.
8. Update frontend card age rendering:
   - default to `last_priced_at` when available
   - fallback to `last_detected_at`
   - display `first_detected_at` as secondary metadata only
9. Add stale signal guard in trader orchestrator decision gate:
   - reject if scanner signal/opportunity freshness exceeds configured max age.
10. Add/adjust tests for:
   - lane watchdog progression
   - merge freshness field behavior
   - frontend age-source selection
   - stale trade signal rejection

## Cutover Rules

1. No compatibility shim paths: replace legacy behavior directly.
2. No disabled code branches left behind.
3. No TODO/FIXME/HACK placeholders.
