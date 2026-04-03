from __future__ import annotations

from services.machine_learning_tasks.crypto_directional import CryptoDirectionalTask


_CRYPTO_DIRECTIONAL_TASK = CryptoDirectionalTask()


def get_machine_learning_task(task_key: str) -> CryptoDirectionalTask:
    normalized = str(task_key or "").strip().lower()
    if normalized != _CRYPTO_DIRECTIONAL_TASK.task_key:
        raise ValueError(f"Unsupported machine learning task '{task_key}'")
    return _CRYPTO_DIRECTIONAL_TASK


def list_machine_learning_tasks() -> list[CryptoDirectionalTask]:
    return [_CRYPTO_DIRECTIONAL_TASK]
