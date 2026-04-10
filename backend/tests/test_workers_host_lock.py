from __future__ import annotations

import pytest

from workers import host


def test_worker_plane_lock_rejects_second_owner(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(host, "_LOCKS_DIR", tmp_path)
    first = host._WorkerPlaneLock("all")
    second = host._WorkerPlaneLock("all")
    first.acquire()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            second.acquire()
    finally:
        first.release()
