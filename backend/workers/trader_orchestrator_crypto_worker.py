from __future__ import annotations

import asyncio

from workers import trader_orchestrator_worker


async def main() -> None:
    await trader_orchestrator_worker.main(
        lane="crypto",
        notifier_enabled=False,
        write_snapshot=False,
    )


async def start_loop() -> None:
    await trader_orchestrator_worker.start_loop(
        lane="crypto",
        notifier_enabled=False,
        write_snapshot=False,
    )


if __name__ == "__main__":
    asyncio.run(main())
