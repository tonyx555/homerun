"""Discovery Profile API routes — CRUD, validation, versioning, docs."""

from __future__ import annotations

import ast
import re
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, update

from models.database import (
    AsyncSessionLocal,
    DiscoveryProfile,
    DiscoveryProfileTombstone,
    DiscoveryProfileVersion,
)
from services.discovery_profile_loader import (
    DISCOVERY_PROFILE_TEMPLATE,
    ALLOWED_IMPORT_PREFIXES,
    DiscoveryProfileValidationError,
    discovery_profile_loader,
    validate_discovery_profile_source,
    _find_profile_class,
    _extract_class_capabilities,
)
from utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/discovery-profiles", tags=["Discovery Profiles"])

_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]{1,48}[a-z0-9]$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_slug(slug: str) -> str:
    slug = slug.strip().lower()
    if not _SLUG_RE.match(slug):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid slug '{slug}'. Must be 3-50 chars, start with a letter, "
                f"use only lowercase letters/numbers/underscores, end with letter or number."
            ),
        )
    return slug


def _detect_capabilities(source_code: str, class_name: Optional[str] = None) -> dict:
    fallback = {"has_score_wallet": False, "has_select_pool": False}
    try:
        tree = ast.parse(source_code)
        found_class = _find_profile_class(tree, class_name)
        if not found_class:
            return fallback
        return _extract_class_capabilities(tree, found_class)
    except Exception:
        return fallback


def _profile_to_dict(profile: DiscoveryProfile, include_runtime: bool = True) -> dict:
    capabilities = _detect_capabilities(profile.source_code or "", profile.class_name)
    runtime = None
    if include_runtime:
        runtime = discovery_profile_loader.get_runtime_status(profile.slug)
    return {
        "id": profile.id,
        "slug": profile.slug,
        "name": profile.name,
        "description": profile.description,
        "source_code": profile.source_code,
        "class_name": profile.class_name,
        "is_system": bool(profile.is_system),
        "enabled": bool(profile.enabled),
        "is_active": bool(profile.is_active),
        "status": profile.status,
        "error_message": profile.error_message,
        "config": dict(profile.config or {}),
        "config_schema": dict(profile.config_schema or {}),
        "profile_kind": profile.profile_kind or "python",
        "version": int(profile.version or 1),
        "sort_order": profile.sort_order or 0,
        "capabilities": capabilities,
        "runtime": runtime,
        "created_at": profile.created_at.isoformat() if profile.created_at else None,
        "updated_at": profile.updated_at.isoformat() if profile.updated_at else None,
    }


def _version_to_dict(v: DiscoveryProfileVersion, include_source: bool = False) -> dict:
    d = {
        "id": v.id,
        "profile_id": v.profile_id,
        "profile_slug": v.profile_slug,
        "version": int(v.version or 0),
        "is_latest": bool(v.is_latest),
        "name": v.name,
        "description": v.description,
        "class_name": v.class_name,
        "config": dict(v.config or {}),
        "config_schema": dict(v.config_schema or {}),
        "profile_kind": v.profile_kind or "python",
        "enabled": bool(v.enabled),
        "is_system": bool(v.is_system),
        "sort_order": v.sort_order or 0,
        "parent_version": v.parent_version,
        "created_by": v.created_by,
        "reason": v.reason,
        "created_at": v.created_at.isoformat() if v.created_at else None,
    }
    if include_source:
        d["source_code"] = v.source_code
    return d


# ---------------------------------------------------------------------------
# Pydantic request models
# ---------------------------------------------------------------------------


class DiscoveryProfileCreateRequest(BaseModel):
    slug: str = Field(..., min_length=3, max_length=128)
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=500)
    source_code: str = Field(..., min_length=10)
    profile_kind: str = Field(default="python", max_length=32)
    config: dict = Field(default_factory=dict)
    config_schema: dict = Field(default_factory=dict)
    enabled: bool = True


class DiscoveryProfileUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = None
    source_code: Optional[str] = Field(None, min_length=10)
    config: Optional[dict] = None
    config_schema: Optional[dict] = None
    enabled: Optional[bool] = None
    profile_kind: Optional[str] = Field(None, max_length=32)


class DiscoveryProfileValidateRequest(BaseModel):
    source_code: str = Field(..., min_length=10)
    class_name: Optional[str] = None


class DiscoveryProfileRestoreRequest(BaseModel):
    reason: Optional[str] = Field(default="manual_restore", max_length=300)


class DiscoveryProfileActivateRequest(BaseModel):
    profile_id: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("")
async def list_discovery_profiles(
    enabled: Optional[bool] = Query(default=None),
    profile_kind: Optional[str] = Query(default=None),
):
    async with AsyncSessionLocal() as session:
        stmt = select(DiscoveryProfile).order_by(
            DiscoveryProfile.sort_order,
            DiscoveryProfile.name,
        )
        if enabled is not None:
            stmt = stmt.where(DiscoveryProfile.enabled == enabled)
        if profile_kind is not None:
            stmt = stmt.where(DiscoveryProfile.profile_kind == profile_kind)
        result = await session.execute(stmt)
        rows = result.scalars().all()
    return [_profile_to_dict(row) for row in rows]


@router.get("/template")
async def get_discovery_profile_template():
    return {
        "template": DISCOVERY_PROFILE_TEMPLATE,
        "instructions": (
            "Create a class that extends BaseDiscoveryProfile and implements "
            "score_wallet(wallet, metrics) to rank wallets, and optionally "
            "select_pool(wallets, metrics_map) to filter the candidate pool."
        ),
        "available_imports": sorted(ALLOWED_IMPORT_PREFIXES),
    }


@router.get("/docs")
async def get_discovery_profile_docs():
    return {
        "title": "Discovery Profile Developer Reference",
        "version": "1.0",
        "overview": {
            "summary": (
                "Discovery profiles control how wallets are scored and selected "
                "for copy-trading pools. Each profile is a Python class stored in "
                "the database that extends BaseDiscoveryProfile."
            ),
            "lifecycle": (
                "Profiles are loaded from source code, validated via AST, compiled "
                "in an isolated module, and instantiated. The profile's score_wallet() "
                "method is called for each candidate wallet during discovery scans."
            ),
        },
        "base_discovery_profile": {
            "import": "from services.discovery_profile_sdk import BaseDiscoveryProfile",
            "class_attributes": {
                "name": {
                    "type": "str",
                    "required": True,
                    "description": "Human-readable profile name (shown in UI)",
                },
                "description": {
                    "type": "str",
                    "required": True,
                    "description": "What this profile does (shown in profile list)",
                },
                "default_config": {
                    "type": "dict",
                    "required": False,
                    "description": (
                        "Default configuration values. Users can override these in the UI. "
                        "Access at runtime via self.config (merged defaults + user overrides)."
                    ),
                },
            },
            "built_in_properties": {
                "self.config": "dict -- Merged default_config + user overrides (set by configure())",
            },
            "helper_methods": {
                "_calculate_rank_score(metrics)": {
                    "signature": "self._calculate_rank_score(metrics: dict) -> float",
                    "description": (
                        "Default composite rank score from wallet metrics. Use as a base "
                        "score and layer your own adjustments on top."
                    ),
                },
                "configure(config)": {
                    "signature": "self.configure(config: dict) -> None",
                    "description": (
                        "Called by the loader after instantiation. Merges default_config "
                        "with user overrides and sets self.config. You do NOT call this yourself."
                    ),
                },
            },
        },
        "scoring_interface": {
            "score_wallet": {
                "signature": "def score_wallet(self, wallet, metrics) -> float",
                "description": (
                    "Score a single wallet. Return a float rank score. Higher scores "
                    "mean better candidates. Return 0.0 to exclude a wallet."
                ),
                "parameters": {
                    "wallet": "Wallet object with address, trade history, etc.",
                    "metrics": (
                        "Dict of computed metrics: win_rate, sharpe_ratio, max_drawdown, "
                        "total_pnl, trade_count, avg_hold_time, etc."
                    ),
                },
            },
            "select_pool": {
                "signature": "def select_pool(self, wallets, metrics_map) -> list",
                "description": (
                    "Select and rank the final pool of wallets. Called after individual "
                    "scoring to apply cross-wallet filtering and sorting."
                ),
                "parameters": {
                    "wallets": "List of wallet objects.",
                    "metrics_map": "Dict mapping wallet address to metrics dict.",
                },
                "return": "Filtered and sorted list of wallets.",
            },
        },
        "wallet_data_contract": {
            "wallet_fields": {
                "address": "str -- Wallet address",
                "trade_count": "int -- Number of historical trades",
                "first_seen": "datetime -- First observed trade",
                "last_seen": "datetime -- Most recent trade",
                "tags": "list[str] -- User-assigned tags",
            },
            "metrics_fields": {
                "win_rate": "float (0-1) -- Fraction of profitable trades",
                "sharpe_ratio": "float -- Risk-adjusted return ratio",
                "max_drawdown": "float (0-1) -- Largest peak-to-trough decline",
                "total_pnl": "float -- Total profit/loss in USD",
                "trade_count": "int -- Number of trades in evaluation window",
                "avg_hold_time": "float -- Average position hold time in minutes",
                "avg_position_size": "float -- Average position size in USD",
                "roi_percent": "float -- Return on investment percentage",
            },
        },
        "insider_detection": {
            "description": (
                "Profiles can detect insider-like patterns by checking for anomalous "
                "win rates, timing clusters, or correlated wallet activity."
            ),
            "tips": [
                "Check for win_rate > 0.85 combined with high trade_count as a potential insider signal",
                "Look for wallets that consistently enter positions shortly before favorable resolution",
                "Compare wallet timing against market event timestamps for suspicious clustering",
            ],
        },
        "available_imports": sorted(ALLOWED_IMPORT_PREFIXES),
        "code_examples": {
            "minimal_profile": (
                "from services.discovery_profile_sdk import BaseDiscoveryProfile\n\n"
                "class MinimalProfile(BaseDiscoveryProfile):\n"
                "    name = 'Minimal'\n"
                "    description = 'Score by win rate only'\n\n"
                "    def score_wallet(self, wallet, metrics):\n"
                "        return metrics.get('win_rate', 0)\n"
            ),
            "advanced_profile": (
                "from services.discovery_profile_sdk import BaseDiscoveryProfile\n\n"
                "class AdvancedProfile(BaseDiscoveryProfile):\n"
                "    name = 'Advanced Multi-Factor'\n"
                "    description = 'Composite scoring with pool selection'\n"
                "    default_config = {'min_trades': 20, 'top_n': 30}\n\n"
                "    def score_wallet(self, wallet, metrics):\n"
                "        if metrics.get('trade_count', 0) < self.config.get('min_trades', 20):\n"
                "            return 0.0\n"
                "        base = self._calculate_rank_score(metrics)\n"
                "        sharpe_bonus = min(metrics.get('sharpe_ratio', 0), 3.0) * 0.1\n"
                "        return base * (1.0 + sharpe_bonus)\n\n"
                "    def select_pool(self, wallets, metrics_map):\n"
                "        scored = []\n"
                "        for w in wallets:\n"
                "            addr = getattr(w, 'address', None) or str(w)\n"
                "            score = self.score_wallet(w, metrics_map.get(addr, {}))\n"
                "            if score > 0:\n"
                "                scored.append((score, w))\n"
                "        scored.sort(key=lambda x: x[0], reverse=True)\n"
                "        return [w for _, w in scored[:self.config.get('top_n', 30)]]\n"
            ),
        },
    }


@router.post("/validate")
async def validate_discovery_profile(req: DiscoveryProfileValidateRequest):
    result = validate_discovery_profile_source(req.source_code, req.class_name)
    return result


@router.put("/active")
async def set_active_discovery_profile(req: DiscoveryProfileActivateRequest):
    async with AsyncSessionLocal() as session:
        target = await session.get(DiscoveryProfile, req.profile_id)
        if target is None:
            raise HTTPException(status_code=404, detail="Discovery profile not found")

        # Deactivate all profiles
        await session.execute(
            update(DiscoveryProfile).values(is_active=False)
        )

        # Activate the target
        target.is_active = True
        await session.commit()
        await session.refresh(target)

    # Inform the loader which profile is active
    discovery_profile_loader.set_active_slug(target.slug)

    return {
        "status": "success",
        "message": f"Profile '{target.slug}' is now active",
        "active_profile": _profile_to_dict(target),
    }


@router.get("/{profile_id}")
async def get_discovery_profile(profile_id: str):
    async with AsyncSessionLocal() as session:
        row = await session.get(DiscoveryProfile, profile_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Discovery profile not found")
    return _profile_to_dict(row)


@router.post("")
async def create_discovery_profile(req: DiscoveryProfileCreateRequest):
    slug = _validate_slug(req.slug)

    if req.profile_kind == "python":
        validation = validate_discovery_profile_source(req.source_code)
        if not validation["valid"]:
            raise HTTPException(
                status_code=400,
                detail={"message": "Profile validation failed", "errors": validation["errors"]},
            )
        class_name = validation["class_name"]
    else:
        class_name = None

    profile_name = req.name.strip()
    profile_id = uuid.uuid4().hex
    status = "unloaded"
    error_message = None

    async with AsyncSessionLocal() as session:
        existing = await session.execute(
            select(DiscoveryProfile).where(DiscoveryProfile.slug == slug)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=409, detail=f"A profile with slug '{slug}' already exists.")

        if req.enabled and req.profile_kind == "python":
            try:
                discovery_profile_loader.load(slug, req.source_code, req.config or None)
                status = "loaded"
            except DiscoveryProfileValidationError as e:
                status = "error"
                error_message = str(e)

        row = DiscoveryProfile(
            id=profile_id,
            slug=slug,
            name=profile_name,
            description=req.description,
            source_code=req.source_code,
            class_name=class_name,
            is_system=False,
            enabled=req.enabled,
            is_active=False,
            status=status,
            error_message=error_message,
            config=req.config,
            config_schema=req.config_schema,
            profile_kind=req.profile_kind,
            version=1,
            sort_order=0,
        )
        session.add(row)
        await session.flush()

        version_row = DiscoveryProfileVersion(
            id=uuid.uuid4().hex,
            profile_id=profile_id,
            profile_slug=slug,
            version=1,
            is_latest=True,
            name=profile_name,
            description=req.description,
            source_code=req.source_code,
            class_name=class_name,
            config=req.config,
            config_schema=req.config_schema,
            profile_kind=req.profile_kind,
            enabled=req.enabled,
            is_system=False,
            sort_order=0,
            parent_version=None,
            created_by="discovery_profile_manager",
            reason="profile_created",
            created_at=datetime.now(timezone.utc),
        )
        session.add(version_row)
        await session.commit()
        await session.refresh(row)
        return _profile_to_dict(row)


@router.put("/{profile_id}")
async def update_discovery_profile(profile_id: str, req: DiscoveryProfileUpdateRequest):
    async with AsyncSessionLocal() as session:
        row = await session.get(DiscoveryProfile, profile_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Discovery profile not found")

        prior_version = int(row.version or 1)
        source_changed = False
        needs_reload = False
        snapshot_needed = False

        if req.source_code is not None and req.source_code != row.source_code:
            if (req.profile_kind or row.profile_kind) == "python":
                validation = validate_discovery_profile_source(req.source_code)
                if not validation["valid"]:
                    raise HTTPException(
                        status_code=400,
                        detail={"message": "Validation failed", "errors": validation["errors"]},
                    )
                row.class_name = validation["class_name"]
            row.source_code = req.source_code
            source_changed = True
            needs_reload = True
            snapshot_needed = True

        if req.config is not None and req.config != (row.config or {}):
            row.config = req.config
            needs_reload = True
            snapshot_needed = True

        if req.config_schema is not None:
            row.config_schema = req.config_schema
            snapshot_needed = True

        if req.name is not None:
            row.name = req.name.strip()
            snapshot_needed = True

        if req.description is not None:
            row.description = req.description
            snapshot_needed = True

        if req.profile_kind is not None:
            row.profile_kind = req.profile_kind
            snapshot_needed = True

        if req.enabled is not None and req.enabled != row.enabled:
            row.enabled = req.enabled
            needs_reload = True
            snapshot_needed = True

        if needs_reload:
            if row.enabled and (row.profile_kind or "python") == "python":
                try:
                    discovery_profile_loader.reload(row.slug, row.source_code, row.config or None)
                    row.status = "loaded"
                    row.error_message = None
                except DiscoveryProfileValidationError as e:
                    row.status = "error"
                    row.error_message = str(e)
            elif not row.enabled:
                discovery_profile_loader.unload(row.slug)
                row.status = "unloaded"
                row.error_message = None

        if snapshot_needed:
            if source_changed:
                row.version = prior_version + 1

            # Mark previous versions as not latest
            await session.execute(
                update(DiscoveryProfileVersion)
                .where(DiscoveryProfileVersion.profile_id == profile_id)
                .values(is_latest=False)
            )

            version_row = DiscoveryProfileVersion(
                id=uuid.uuid4().hex,
                profile_id=profile_id,
                profile_slug=row.slug,
                version=int(row.version or 1),
                is_latest=True,
                name=row.name,
                description=row.description,
                source_code=row.source_code,
                class_name=row.class_name,
                config=row.config,
                config_schema=row.config_schema,
                profile_kind=row.profile_kind,
                enabled=row.enabled,
                is_system=bool(row.is_system),
                sort_order=row.sort_order or 0,
                parent_version=prior_version,
                created_by="discovery_profile_manager",
                reason="profile_updated",
                created_at=datetime.now(timezone.utc),
            )
            session.add(version_row)

        await session.commit()
        await session.refresh(row)
        return _profile_to_dict(row)


@router.delete("/{profile_id}")
async def delete_discovery_profile(profile_id: str):
    async with AsyncSessionLocal() as session:
        row = await session.get(DiscoveryProfile, profile_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Discovery profile not found")

        if bool(row.is_system):
            tombstone = await session.get(DiscoveryProfileTombstone, row.slug)
            if tombstone is None:
                session.add(
                    DiscoveryProfileTombstone(
                        slug=row.slug,
                        deleted_at=datetime.now(timezone.utc),
                        reason="user_deleted_system_profile",
                    )
                )
            else:
                tombstone.deleted_at = datetime.now(timezone.utc)
                tombstone.reason = "user_deleted_system_profile"

        discovery_profile_loader.unload(row.slug)
        await session.delete(row)
        await session.commit()

    return {"status": "success", "message": "Discovery profile deleted"}


@router.post("/{profile_id}/reload")
async def reload_discovery_profile(profile_id: str):
    async with AsyncSessionLocal() as session:
        row = await session.get(DiscoveryProfile, profile_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Discovery profile not found")

        if not row.enabled:
            raise HTTPException(
                status_code=400,
                detail="Cannot reload a disabled profile. Enable it first.",
            )

        try:
            discovery_profile_loader.reload(row.slug, row.source_code, row.config or None)
            row.status = "loaded"
            row.error_message = None
            await session.commit()
            return {
                "status": "success",
                "message": f"Profile '{row.slug}' reloaded",
                "runtime": discovery_profile_loader.get_runtime_status(row.slug),
            }
        except DiscoveryProfileValidationError as e:
            row.status = "error"
            row.error_message = str(e)
            await session.commit()
            raise HTTPException(
                status_code=400,
                detail={"message": f"Reload failed for '{row.slug}'", "error": str(e)},
            )


@router.get("/{profile_id}/versions")
async def list_discovery_profile_versions(
    profile_id: str,
    limit: int = Query(default=200, ge=1, le=1000),
    include_source: bool = Query(default=False),
):
    async with AsyncSessionLocal() as session:
        row = await session.get(DiscoveryProfile, profile_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Discovery profile not found")

        stmt = (
            select(DiscoveryProfileVersion)
            .where(DiscoveryProfileVersion.profile_id == profile_id)
            .order_by(DiscoveryProfileVersion.version.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        version_rows = result.scalars().all()

    return {
        "profile_id": profile_id,
        "current_version": int(row.version or 1),
        "items": [_version_to_dict(v, include_source=include_source) for v in version_rows],
        "total": len(version_rows),
    }


@router.post("/{profile_id}/versions/{version}/restore")
async def restore_discovery_profile_version(
    profile_id: str,
    version: int,
    request: DiscoveryProfileRestoreRequest | None = None,
):
    request_payload = request or DiscoveryProfileRestoreRequest()

    async with AsyncSessionLocal() as session:
        row = await session.get(DiscoveryProfile, profile_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Discovery profile not found")

        stmt = (
            select(DiscoveryProfileVersion)
            .where(
                DiscoveryProfileVersion.profile_id == profile_id,
                DiscoveryProfileVersion.version == version,
            )
        )
        result = await session.execute(stmt)
        snapshot = result.scalar_one_or_none()
        if snapshot is None:
            raise HTTPException(status_code=404, detail=f"Version {version} not found")

        prior_version = int(row.version or 1)
        new_version = prior_version + 1

        row.source_code = snapshot.source_code
        row.class_name = snapshot.class_name
        row.name = snapshot.name
        row.description = snapshot.description
        row.config = dict(snapshot.config or {})
        row.config_schema = dict(snapshot.config_schema or {})
        row.profile_kind = snapshot.profile_kind or "python"
        row.version = new_version

        # Mark previous versions as not latest
        await session.execute(
            update(DiscoveryProfileVersion)
            .where(DiscoveryProfileVersion.profile_id == profile_id)
            .values(is_latest=False)
        )

        restored_version = DiscoveryProfileVersion(
            id=uuid.uuid4().hex,
            profile_id=profile_id,
            profile_slug=row.slug,
            version=new_version,
            is_latest=True,
            name=row.name,
            description=row.description,
            source_code=row.source_code,
            class_name=row.class_name,
            config=row.config,
            config_schema=row.config_schema,
            profile_kind=row.profile_kind,
            enabled=row.enabled,
            is_system=bool(row.is_system),
            sort_order=row.sort_order or 0,
            parent_version=prior_version,
            created_by="discovery_profile_manager",
            reason=str(request_payload.reason or "manual_restore").strip() or "manual_restore",
            created_at=datetime.now(timezone.utc),
        )
        session.add(restored_version)

        if row.enabled and (row.profile_kind or "python") == "python":
            try:
                discovery_profile_loader.reload(row.slug, row.source_code, row.config or None)
                row.status = "loaded"
                row.error_message = None
            except DiscoveryProfileValidationError as e:
                row.status = "error"
                row.error_message = str(e)
        else:
            discovery_profile_loader.unload(row.slug)
            row.status = "unloaded"
            row.error_message = None

        await session.commit()
        await session.refresh(row)
        return {
            "status": "restored",
            "profile": _profile_to_dict(row),
            "restored_version": _version_to_dict(restored_version, include_source=False),
            "source_version": _version_to_dict(snapshot, include_source=False),
        }
