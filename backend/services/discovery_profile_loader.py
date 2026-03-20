"""
Discovery Profile Loader

Loads and manages user-defined discovery profiles. Each profile is a Python
file defining a class that extends ``BaseDiscoveryProfile`` and implements:

  - **score_wallet(wallet, metrics)** -- custom wallet scoring logic.
  - **select_pool(wallets, metrics_map)** -- custom pool selection/filtering.

The loader validates source via AST, enforces an import allow-list, compiles
in an isolated module, and exposes runtime status tracking for dashboards.
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import sys
import traceback
import types
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Discovery profile template -- shown to users as a starting point
# ---------------------------------------------------------------------------

DISCOVERY_PROFILE_TEMPLATE = '''"""
Discovery Profile: My Custom Profile

A custom discovery profile that scores wallets and selects the best
candidates for copy-trading based on user-defined criteria.
"""

from services.discovery_profile_sdk import BaseDiscoveryProfile


class MyCustomProfile(BaseDiscoveryProfile):
    """
    A custom discovery profile.

    Required: score_wallet() -- assign a rank score to each wallet
    Optional: select_pool() -- filter/sort the candidate pool
    """

    name = "My Custom Profile"
    description = "Describe how this profile ranks and selects wallets"

    default_config = {
        "min_sharpe": 0.5,
        "min_win_rate": 0.55,
        "max_drawdown": 0.30,
        "top_n": 50,
    }

    def score_wallet(self, wallet, metrics):
        """Score a single wallet. Return a float rank score.

        Use self.config for configurable parameters:
            min_sharpe = self.config.get("min_sharpe", 0.5)

        Use self._calculate_rank_score(metrics) for the default composite score,
        then layer your own adjustments on top.
        """
        base_score = self._calculate_rank_score(metrics)

        win_rate = metrics.get("win_rate", 0)
        if win_rate < self.config.get("min_win_rate", 0.55):
            return 0.0

        sharpe = metrics.get("sharpe_ratio", 0)
        if sharpe < self.config.get("min_sharpe", 0.5):
            return base_score * 0.5

        return base_score * (1.0 + min(sharpe, 3.0) * 0.1)

    def select_pool(self, wallets, metrics_map):
        """Select and rank the final pool of wallets.

        Args:
            wallets: List of wallet objects.
            metrics_map: Dict mapping wallet address to metrics dict.

        Returns:
            Filtered and sorted list of wallets.
        """
        scored = []
        for w in wallets:
            addr = getattr(w, "address", None) or str(w)
            m = metrics_map.get(addr, {})
            max_dd = m.get("max_drawdown", 1.0)
            if max_dd > self.config.get("max_drawdown", 0.30):
                continue
            score = self.score_wallet(w, m)
            if score > 0:
                scored.append((score, w))
        scored.sort(key=lambda x: x[0], reverse=True)
        top_n = int(self.config.get("top_n", 50))
        return [w for _, w in scored[:top_n]]
'''


# ---------------------------------------------------------------------------
# Allowed imports
# ---------------------------------------------------------------------------

ALLOWED_IMPORT_PREFIXES = {
    # Future annotations
    "__future__",
    # App modules
    "services.discovery_profile_sdk",
    "services.strategy_sdk",
    "services.traders_sdk",
    "models",
    "utils",
    "config",
    # Standard library (safe subset)
    "math",
    "statistics",
    "collections",
    "datetime",
    "re",
    "json",
    "random",
    "asyncio",
    "calendar",
    "decimal",
    "fractions",
    "itertools",
    "functools",
    "operator",
    "copy",
    "enum",
    "dataclasses",
    "typing",
    "abc",
    "time",
    "hashlib",
    "hmac",
    "base64",
    "uuid",
    "urllib.parse",
    "logging",
    "bisect",
    "heapq",
    "textwrap",
    "string",
    "concurrent",
    # Third party
    "httpx",
    "numpy",
    "scipy",
}

# Explicitly blocked imports (dangerous even if they happen to pass prefix)
BLOCKED_IMPORTS = {
    "os",
    "sys",
    "subprocess",
    "shutil",
    "importlib",
    "builtins",
    "ctypes",
    "socket",
    "http",
    "urllib",  # urllib itself is blocked; urllib.parse is allowed via prefix
    "requests",
    "aiohttp",
    "pickle",
    "shelve",
    "marshal",
    "code",
    "codeop",
    "compile",
    "compileall",
    "exec",
    "eval",
    "__import__",
    "runpy",
    "ast",
    "dis",
    "inspect",
    "signal",
    "multiprocessing",
    "io",
    "tempfile",
    "glob",
    "fnmatch",
    "webbrowser",
}

# Blocked bare function calls
_BLOCKED_CALL_NAMES = {"exec", "eval", "compile", "__import__", "open", "input"}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class DiscoveryProfileValidationError(Exception):
    """Raised when discovery profile source code fails validation or loading."""

    pass


# ---------------------------------------------------------------------------
# AST validation helpers
# ---------------------------------------------------------------------------


def _check_imports(tree: ast.AST) -> list[str]:
    """Check all imports in the AST against the allow-list. Returns violations."""
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                mod_root = alias.name.split(".")[0]
                if alias.name.startswith("urllib.parse"):
                    continue
                if mod_root in BLOCKED_IMPORTS:
                    violations.append(f"Blocked import: '{alias.name}' (line {node.lineno})")
                elif not any(alias.name.startswith(p) for p in ALLOWED_IMPORT_PREFIXES):
                    violations.append(
                        f"Disallowed import: '{alias.name}' (line {node.lineno}). "
                        f"Use standard library or app modules only."
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                mod_root = node.module.split(".")[0]
                if node.module.startswith("urllib.parse"):
                    continue
                if mod_root in BLOCKED_IMPORTS:
                    violations.append(f"Blocked import: 'from {node.module}' (line {node.lineno})")
                elif not any(node.module.startswith(p) for p in ALLOWED_IMPORT_PREFIXES):
                    violations.append(
                        f"Disallowed import: 'from {node.module}' (line {node.lineno}). "
                        f"Use standard library or app modules only."
                    )
    return violations


def _check_blocked_calls(tree: ast.AST) -> list[str]:
    """Check for dangerous bare function calls. Returns violations."""
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            if node.func.id in _BLOCKED_CALL_NAMES:
                violations.append(f"Blocked call '{node.func.id}()' (line {node.lineno})")
        elif isinstance(node.func, ast.Attribute):
            if node.func.attr in _BLOCKED_CALL_NAMES:
                target = node.func.value
                if isinstance(target, ast.Name) and target.id in {"builtins", "__builtins__"}:
                    violations.append(f"Blocked call '{target.id}.{node.func.attr}()' (line {node.lineno})")
    return violations


def _find_profile_class(tree: ast.AST, class_name: Optional[str] = None) -> Optional[str]:
    """Find a class extending BaseDiscoveryProfile in the AST.

    If *class_name* is given, look for that exact class; otherwise pick the
    first class whose name ends with ``Profile`` or fallback to the first
    class that extends ``BaseDiscoveryProfile``.
    """
    base_names = {"BaseDiscoveryProfile"}

    if class_name:
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == class_name:
                return class_name
        return None

    # Auto-detect: first class extending BaseDiscoveryProfile
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for base in node.bases:
                base_id = None
                if isinstance(base, ast.Name):
                    base_id = base.id
                elif isinstance(base, ast.Attribute):
                    base_id = base.attr
                if base_id in base_names:
                    return node.name

    # Fallback: first class whose name contains "Profile"
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and "Profile" in node.name:
            return node.name

    return None


def _extract_class_capabilities(tree: ast.AST, class_name: str) -> dict:
    """Return which methods are present on the profile class."""
    result = {
        "has_score_wallet": False,
        "has_select_pool": False,
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if item.name == "score_wallet":
                        result["has_score_wallet"] = True
                    elif item.name == "select_pool":
                        result["has_select_pool"] = True
            break
    return result


def _extract_class_attribute(tree: ast.AST, class_name: str, attr_name: str) -> Optional[str]:
    """Extract a string class attribute value from the AST."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, ast.Assign):
                    for target in item.targets:
                        if isinstance(target, ast.Name) and target.id == attr_name:
                            if isinstance(item.value, ast.Constant):
                                return str(item.value.value)
    return None


# ---------------------------------------------------------------------------
# Public validation function
# ---------------------------------------------------------------------------


def validate_discovery_profile_source(
    source_code: str,
    class_name: Optional[str] = None,
) -> dict:
    """Validate discovery profile source code without executing it.

    Returns a dict with:
        valid (bool): Whether the code is valid
        class_name (str|None): Name of the profile class found
        errors (list[str]): Validation error messages
        warnings (list[str]): Non-fatal warnings
        capabilities (dict): Which methods are defined
    """
    result: dict[str, Any] = {
        "valid": False,
        "class_name": None,
        "errors": [],
        "warnings": [],
        "capabilities": None,
    }

    # 1. Syntax check
    try:
        tree = ast.parse(source_code)
    except SyntaxError as e:
        result["errors"].append(f"Syntax error at line {e.lineno}: {e.msg}")
        return result

    # 2. Import safety check
    import_violations = _check_imports(tree)
    if import_violations:
        result["errors"].extend(import_violations)
        return result

    # 3. Blocked call check
    call_violations = _check_blocked_calls(tree)
    if call_violations:
        result["errors"].extend(call_violations)
        return result

    # 4. Find profile class
    found_class = _find_profile_class(tree, class_name)
    if not found_class:
        if class_name:
            result["errors"].append(f"Class '{class_name}' was not found in profile source.")
        else:
            result["errors"].append(
                "No class extending BaseDiscoveryProfile found. "
                "Your profile must define a class like: class MyProfile(BaseDiscoveryProfile):"
            )
        return result
    result["class_name"] = found_class

    # 5. Check capabilities
    capabilities = _extract_class_capabilities(tree, found_class)
    result["capabilities"] = capabilities

    # 6. Require at least score_wallet or select_pool
    has_any = capabilities["has_score_wallet"] or capabilities["has_select_pool"]
    if not has_any:
        result["errors"].append(
            f"Class '{found_class}' must implement at least one of: score_wallet(), select_pool(). "
            f"Use score_wallet() to assign a rank score to each wallet, or select_pool() "
            f"to filter and sort the candidate pool."
        )
        return result

    # 7. Extract metadata
    profile_name = _extract_class_attribute(tree, found_class, "name")
    profile_description = _extract_class_attribute(tree, found_class, "description")

    if not profile_name:
        result["warnings"].append(f"Class '{found_class}' has no 'name' attribute. A default name will be used.")
    if not profile_description:
        result["warnings"].append(
            f"Class '{found_class}' has no 'description' attribute. A default description will be used."
        )

    result["valid"] = True
    return result


# ---------------------------------------------------------------------------
# Runtime state for a loaded profile
# ---------------------------------------------------------------------------


@dataclass
class LoadedProfile:
    """Runtime state for a loaded discovery profile."""

    slug: str
    instance: Any  # BaseDiscoveryProfile instance
    class_name: str
    source_hash: str
    loaded_at: datetime
    module_name: str = ""
    run_count: int = 0
    error_count: int = 0
    last_run: Optional[datetime] = None
    last_error: Optional[str] = None


def _profile_runtime_hash(source_code: str, config: Optional[dict]) -> str:
    """Hash profile source + config so config-only changes trigger reloads."""
    normalized_config = config if isinstance(config, dict) else {}
    serialized = json.dumps(normalized_config, sort_keys=True, separators=(",", ":"))
    payload = f"{source_code}\n{serialized}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Discovery Profile Loader
# ---------------------------------------------------------------------------


class DiscoveryProfileLoader:
    """Manages loading, validation, and lifecycle of discovery profiles from source code."""

    def __init__(self) -> None:
        self._loaded: dict[str, LoadedProfile] = {}
        self._errors: dict[str, str] = {}
        self._module_counter: int = 0
        self._refresh_lock: Optional[asyncio.Lock] = None
        self._active_slug: str | None = None

    @property
    def refresh_lock(self) -> asyncio.Lock:
        if self._refresh_lock is None:
            self._refresh_lock = asyncio.Lock()
        return self._refresh_lock

    # -- Load / Unload -------------------------------------------------------

    def load(
        self,
        slug: str,
        source_code: str,
        config: Optional[dict] = None,
    ) -> LoadedProfile:
        """Load a discovery profile from source code.

        Validates the source via AST, compiles in an isolated module,
        instantiates the profile class, and stores it in ``_loaded``.

        Args:
            slug: Unique identifier for this profile.
            source_code: Python source code defining a BaseDiscoveryProfile subclass.
            config: Optional config overrides passed to ``instance.configure()``.

        Returns:
            LoadedProfile instance.

        Raises:
            DiscoveryProfileValidationError: If the source code is invalid or loading fails.
        """
        # Validate
        validation = validate_discovery_profile_source(source_code)
        if not validation["valid"]:
            raise DiscoveryProfileValidationError(
                "Profile validation failed:\n" + "\n".join(validation["errors"])
            )

        class_name = validation["class_name"]

        # Unload existing version if present
        if slug in self._loaded:
            self.unload(slug)

        # Create a unique module name
        self._module_counter += 1
        module_name = f"_discovery_profile_{slug}_{self._module_counter}"

        try:
            # Compile
            code = compile(source_code, f"<discovery_profile:{slug}>", "exec")

            # Create isolated module
            module = types.ModuleType(module_name)
            module.__file__ = f"<discovery_profile:{slug}>"
            module.__builtins__ = __builtins__

            # Register so imports within the module resolve
            sys.modules[module_name] = module

            # Execute in the module's namespace
            exec(code, module.__dict__)  # noqa: S102

            # Retrieve the profile class
            profile_class = getattr(module, class_name, None)
            if profile_class is None:
                raise DiscoveryProfileValidationError(
                    f"Class '{class_name}' not found after loading. "
                    f"This is likely a bug in the discovery profile loader."
                )

            # Verify it extends BaseDiscoveryProfile
            from services.discovery_profile_sdk import BaseDiscoveryProfile

            is_valid_class = isinstance(profile_class, type) and issubclass(profile_class, BaseDiscoveryProfile)
            if not is_valid_class:
                raise DiscoveryProfileValidationError(
                    f"Class '{class_name}' does not extend BaseDiscoveryProfile."
                )

            # Default name / description if missing
            if not getattr(profile_class, "name", None):
                profile_class.name = slug.replace("_", " ").title()
            if not getattr(profile_class, "description", None):
                profile_class.description = f"Discovery profile: {slug}"

            # Instantiate
            instance = profile_class()

            # Config cascade: class defaults then user overrides
            merged_config = {**(instance.default_config or {}), **(config or {})}
            instance.configure(merged_config)

            # Source hash for change detection
            source_hash = _profile_runtime_hash(source_code, merged_config)

            loaded = LoadedProfile(
                slug=slug,
                instance=instance,
                class_name=class_name,
                source_hash=source_hash,
                loaded_at=datetime.now(timezone.utc),
                module_name=module_name,
            )
            self._loaded[slug] = loaded
            self._errors.pop(slug, None)

            caps = validation.get("capabilities") or {}
            cap_parts = [k.replace("has_", "") for k, v in caps.items() if v]
            logger.info(
                "Discovery profile loaded: %s (class=%s, name='%s', capabilities=[%s])",
                slug,
                class_name,
                instance.name,
                ", ".join(cap_parts),
            )
            return loaded

        except DiscoveryProfileValidationError:
            sys.modules.pop(module_name, None)
            raise
        except Exception as e:
            sys.modules.pop(module_name, None)
            tb = traceback.format_exc()
            raise DiscoveryProfileValidationError(
                f"Failed to load discovery profile '{slug}': {e}\n\n{tb}"
            ) from e

    def unload(self, slug: str) -> None:
        """Unload a discovery profile by slug."""
        loaded = self._loaded.pop(slug, None)
        if loaded:
            if loaded.module_name and loaded.module_name in sys.modules:
                del sys.modules[loaded.module_name]
            logger.info("Discovery profile unloaded: %s", slug)

    def reload(
        self,
        slug: str,
        source_code: str,
        config: Optional[dict] = None,
    ) -> LoadedProfile:
        """Reload a discovery profile (unload + load)."""
        self.unload(slug)
        return self.load(slug, source_code, config)

    # -- Accessors -----------------------------------------------------------

    def get_instance(self, slug: str) -> Any:
        """Return the BaseDiscoveryProfile instance for *slug*, or None."""
        loaded = self._loaded.get(slug)
        return loaded.instance if loaded else None

    def get_active_instance(self) -> Any:
        """Return the BaseDiscoveryProfile instance for the active profile, or None.

        Queries the DB for the profile with is_active=True and returns its
        loaded instance.  Falls back to None if nothing is active or loaded.
        """
        if not self._loaded:
            return None
        # Check if any loaded profile is marked active in memory.
        # We track this by storing the active slug when set_active is called.
        if self._active_slug and self._active_slug in self._loaded:
            return self._loaded[self._active_slug].instance
        return None

    def set_active_slug(self, slug: str | None) -> None:
        """Set the active profile slug (called by the routes layer)."""
        self._active_slug = slug

    def get_status(self, slug: str) -> Optional[dict]:
        """Get status info for a loaded profile."""
        loaded = self._loaded.get(slug)
        if not loaded:
            if slug in self._errors:
                return {
                    "slug": slug,
                    "status": "error",
                    "error_message": self._errors[slug],
                }
            return None
        return {
            "slug": loaded.slug,
            "class_name": loaded.class_name,
            "name": getattr(loaded.instance, "name", loaded.slug),
            "description": getattr(loaded.instance, "description", ""),
            "source_hash": loaded.source_hash,
            "loaded_at": loaded.loaded_at.isoformat(),
            "run_count": loaded.run_count,
            "error_count": loaded.error_count,
            "last_run": loaded.last_run.isoformat() if loaded.last_run else None,
            "last_error": loaded.last_error,
        }

    def is_loaded(self, slug: str) -> bool:
        """Check whether a profile is currently loaded."""
        return slug in self._loaded

    def get_runtime_status(self, slug: str) -> Optional[dict]:
        """Return full runtime status for a loaded profile."""
        loaded = self._loaded.get(slug)
        if loaded is not None:
            return {
                "profile_key": loaded.slug,
                "class_name": loaded.class_name,
                "source_hash": loaded.source_hash,
                "status": "loaded",
                "error_message": None,
                "loaded_at": loaded.loaded_at.isoformat(),
            }
        if slug in self._errors:
            return {
                "profile_key": slug,
                "status": "error",
                "error_message": self._errors[slug],
            }
        return None

    def get_all_loaded(self) -> dict[str, LoadedProfile]:
        """Return dict of all loaded profiles keyed by slug."""
        return dict(self._loaded)

    # -- Run tracking --------------------------------------------------------

    def record_run(
        self,
        slug: str,
        error: Optional[str] = None,
    ) -> None:
        """Record a run result for status tracking."""
        loaded = self._loaded.get(slug)
        if not loaded:
            return
        loaded.run_count += 1
        loaded.last_run = datetime.now(timezone.utc)
        if error:
            loaded.error_count += 1
            loaded.last_error = error


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

discovery_profile_loader = DiscoveryProfileLoader()
