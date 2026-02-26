from __future__ import annotations

import asyncio
import json
from datetime import date, datetime
from typing import Any, Optional

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from config import settings
from utils.logger import get_logger

logger = get_logger("redis_streams")

# Minimum Redis version required for Streams (XADD, XREADGROUP, etc.)
_MIN_REDIS_VERSION_FOR_STREAMS = (5, 0, 0)


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, set):
        return list(value)
    return str(value)


def _parse_redis_version(version_str: str) -> tuple[int, ...]:
    """Parse a Redis version string like '7.2.4' into a tuple of ints."""
    parts: list[int] = []
    for segment in str(version_str or "").strip().split("."):
        cleaned = ""
        for ch in segment:
            if ch.isdigit():
                cleaned += ch
            else:
                break
        if cleaned:
            parts.append(int(cleaned))
    return tuple(parts) if parts else (0,)


class RedisStreamClient:
    def __init__(self) -> None:
        self._client: Optional[Redis] = None
        self._lock = asyncio.Lock()
        self._streams_available: Optional[bool] = None
        self._server_version: str = ""
        self._streams_check_logged: bool = False

    async def _close_client(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            await client.aclose()
        except Exception:
            pass

    async def _get_client(self) -> Redis:
        client = self._client
        if client is not None:
            return client
        async with self._lock:
            client = self._client
            if client is not None:
                return client
            kwargs: dict[str, Any] = {
                "host": settings.REDIS_HOST,
                "port": int(settings.REDIS_PORT),
                "db": int(settings.REDIS_DB),
                "decode_responses": True,
                "socket_connect_timeout": float(settings.REDIS_CONNECT_TIMEOUT_SECONDS),
                "socket_timeout": float(settings.REDIS_SOCKET_TIMEOUT_SECONDS),
                "health_check_interval": 30,
            }
            if settings.REDIS_PASSWORD:
                kwargs["password"] = settings.REDIS_PASSWORD
            self._client = Redis(**kwargs)
            return self._client

    async def check_streams_available(self) -> bool:
        """Check if the connected Redis server supports Streams (requires 5.0+).

        The result is cached after the first successful probe.  If the server
        version is below 5.0, a loud WARNING is emitted once so the operator
        can upgrade.  All stream operations (xadd, xreadgroup, xack, etc.)
        short-circuit to no-ops when this returns False.
        """
        if self._streams_available is not None:
            return self._streams_available
        try:
            client = await self._get_client()
            info: dict[str, Any] = await client.info("server")  # type: ignore[arg-type]
            version_str = str(info.get("redis_version", "0"))
            self._server_version = version_str
            parsed = _parse_redis_version(version_str)
            supported = parsed >= _MIN_REDIS_VERSION_FOR_STREAMS
            self._streams_available = supported
            if not supported and not self._streams_check_logged:
                self._streams_check_logged = True
                logger.warning(
                    "Redis server version %s does NOT support Streams "
                    "(requires >= %s). Trade signal streaming, consumer groups, "
                    "and stream-based IPC will be DISABLED. "
                    "Upgrade Redis (Docker redis:7-alpine, Memurai, or brew install redis).",
                    version_str,
                    ".".join(str(v) for v in _MIN_REDIS_VERSION_FOR_STREAMS),
                )
            elif supported and not self._streams_check_logged:
                self._streams_check_logged = True
                logger.info(
                    "Redis server version %s supports Streams.", version_str,
                )
            return supported
        except Exception as exc:
            logger.debug("Redis streams capability check failed", exc_info=exc)
            # Don't cache failure — allow retry on next call
            return False

    @property
    def streams_available(self) -> Optional[bool]:
        """Return the cached Streams availability flag (None if not yet checked)."""
        return self._streams_available

    @property
    def server_version(self) -> str:
        """Return the cached Redis server version string."""
        return self._server_version

    async def ping(self) -> bool:
        try:
            client = await self._get_client()
            return bool(await client.ping())
        except Exception as exc:
            logger.debug("Redis ping failed", exc_info=exc)
            await self._close_client()
            return False

    async def append_json(
        self,
        stream: str,
        payload: dict[str, Any],
        *,
        maxlen: int,
    ) -> Optional[str]:
        if not await self.check_streams_available():
            return None
        try:
            client = await self._get_client()
            body = json.dumps(payload, default=_json_default, separators=(",", ":"))
            entry_id = await client.xadd(
                stream,
                {"data": body},
                maxlen=max(1000, int(maxlen)),
                approximate=True,
            )
            return str(entry_id)
        except Exception as exc:
            logger.debug(
                "Redis stream append failed",
                stream=stream,
                exc_info=exc,
            )
            await self._close_client()
            return None

    async def ensure_consumer_group(
        self,
        *,
        stream: str,
        group: str,
        start_id: str = "$",
        create_stream: bool = True,
    ) -> bool:
        if not stream or not group:
            return False
        if not await self.check_streams_available():
            return False
        try:
            client = await self._get_client()
            await client.xgroup_create(
                name=stream,
                groupname=group,
                id=str(start_id or "$"),
                mkstream=bool(create_stream),
            )
            return True
        except ResponseError as exc:
            # Redis returns BUSYGROUP if the group already exists.
            if "BUSYGROUP" in str(exc):
                return True
            logger.debug(
                "Redis stream group create failed",
                stream=stream,
                group=group,
                exc_info=exc,
            )
            await self._close_client()
            return False
        except Exception as exc:
            logger.debug(
                "Redis stream group create failed",
                stream=stream,
                group=group,
                exc_info=exc,
            )
            await self._close_client()
            return False

    async def read_raw(
        self,
        stream: str,
        *,
        last_id: str,
        block_ms: int,
        count: int,
    ) -> list[tuple[str, str]]:
        if not await self.check_streams_available():
            return []
        try:
            client = await self._get_client()
            chunks = await client.xread(
                {stream: last_id},
                block=max(1, int(block_ms)),
                count=max(1, int(count)),
            )
        except Exception as exc:
            logger.debug(
                "Redis stream read failed",
                stream=stream,
                exc_info=exc,
            )
            await self._close_client()
            await asyncio.sleep(0.5)
            return []

        out: list[tuple[str, str]] = []
        for _stream_name, entries in chunks:
            for entry_id, fields in entries:
                raw = fields.get("data")
                if isinstance(raw, str):
                    out.append((str(entry_id), raw))
        return out

    async def read_group_raw(
        self,
        *,
        stream: str,
        group: str,
        consumer: str,
        block_ms: int,
        count: int,
        include_pending: bool = False,
    ) -> list[tuple[str, str]]:
        if not stream or not group or not consumer:
            return []
        if not await self.check_streams_available():
            return []

        read_id = "0" if include_pending else ">"
        try:
            client = await self._get_client()
            chunks = await client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: read_id},
                count=max(1, int(count)),
                block=max(1, int(block_ms)),
            )
        except ResponseError as exc:
            logger.debug(
                "Redis stream group read failed",
                stream=stream,
                group=group,
                consumer=consumer,
                exc_info=exc,
            )
            await self._close_client()
            await asyncio.sleep(0.05)
            return []
        except Exception as exc:
            logger.debug(
                "Redis stream group read failed",
                stream=stream,
                group=group,
                consumer=consumer,
                exc_info=exc,
            )
            await self._close_client()
            await asyncio.sleep(0.05)
            return []

        out: list[tuple[str, str]] = []
        for _stream_name, entries in chunks:
            for entry_id, fields in entries:
                raw = fields.get("data")
                if isinstance(raw, str):
                    out.append((str(entry_id), raw))
        return out

    async def ack(
        self,
        *,
        stream: str,
        group: str,
        entry_ids: list[str],
    ) -> int:
        if not stream or not group:
            return 0
        if not await self.check_streams_available():
            return 0
        ids = [str(entry_id).strip() for entry_id in entry_ids if str(entry_id).strip()]
        if not ids:
            return 0
        try:
            client = await self._get_client()
            acknowledged = await client.xack(stream, group, *ids)
            return int(acknowledged or 0)
        except Exception as exc:
            logger.debug(
                "Redis stream ack failed",
                stream=stream,
                group=group,
                entry_count=len(ids),
                exc_info=exc,
            )
            await self._close_client()
            return 0

    async def auto_claim_raw(
        self,
        *,
        stream: str,
        group: str,
        consumer: str,
        min_idle_ms: int,
        start_id: str = "0-0",
        count: int = 100,
    ) -> tuple[str, list[tuple[str, str]]]:
        if not stream or not group or not consumer:
            return start_id, []
        if not await self.check_streams_available():
            return start_id, []
        try:
            client = await self._get_client()
            next_start_id, entries, _deleted = await client.xautoclaim(
                name=stream,
                groupname=group,
                consumername=consumer,
                min_idle_time=max(1, int(min_idle_ms)),
                start_id=str(start_id or "0-0"),
                count=max(1, int(count)),
            )
        except Exception as exc:
            logger.debug(
                "Redis stream auto-claim failed",
                stream=stream,
                group=group,
                consumer=consumer,
                exc_info=exc,
            )
            await self._close_client()
            return start_id, []

        out: list[tuple[str, str]] = []
        for entry_id, fields in entries:
            raw = fields.get("data")
            if isinstance(raw, str):
                out.append((str(entry_id), raw))
        return str(next_start_id or start_id), out

    async def hgetall_many(self, keys: list[str]) -> list[dict[str, str]]:
        if not keys:
            return []
        try:
            client = await self._get_client()
            pipe = client.pipeline(transaction=False)
            for key in keys:
                pipe.hgetall(key)
            rows = await pipe.execute()
        except Exception as exc:
            logger.debug(
                "Redis hash read failed",
                keys_count=len(keys),
                exc_info=exc,
            )
            await self._close_client()
            return []

        out: list[dict[str, str]] = []
        for row in rows:
            if isinstance(row, dict):
                out.append({str(k): str(v) for k, v in row.items()})
            else:
                out.append({})
        return out

    async def hset_many(self, rows: dict[str, dict[str, str]], *, expire_seconds: int | None = None) -> None:
        if not rows:
            return
        try:
            client = await self._get_client()
            pipe = client.pipeline(transaction=False)
            for key, payload in rows.items():
                if not payload:
                    continue
                pipe.hset(key, mapping=payload)
                if expire_seconds and expire_seconds > 0:
                    pipe.expire(key, int(expire_seconds))
            await pipe.execute()
        except Exception as exc:
            logger.debug(
                "Redis hash write failed",
                keys_count=len(rows),
                exc_info=exc,
            )
            await self._close_client()

    async def close(self) -> None:
        await self._close_client()


redis_streams = RedisStreamClient()
