import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from workers.scanner_slo_worker import _coverage_ratio_breached


def test_coverage_ratio_does_not_breach_while_heavy_cycle_is_still_in_progress():
    assert (
        _coverage_ratio_breached(
            coverage_ratio=0.56,
            threshold=0.95,
            heavy_inflight=True,
            full_coverage_completion_time=None,
        )
        is False
    )


def test_coverage_ratio_breaches_after_cycle_is_no_longer_in_progress():
    assert (
        _coverage_ratio_breached(
            coverage_ratio=0.56,
            threshold=0.95,
            heavy_inflight=False,
            full_coverage_completion_time=None,
        )
        is True
    )
