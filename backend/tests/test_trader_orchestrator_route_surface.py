from api.routes_trader_orchestrator import router as trader_orchestrator_router


def _route_surface(router):
    surface = set()
    for route in router.routes:
        path = getattr(route, "path", "")
        methods = set(getattr(route, "methods", set()))
        for method in methods:
            method_key = str(method or "").upper()
            if method_key in {"HEAD", "OPTIONS"}:
                continue
            surface.add((method_key, path))
    return surface


def test_trader_orchestrator_route_surface_is_complete():
    actual = _route_surface(trader_orchestrator_router)
    expected = {
        ("GET", "/trader-orchestrator/overview"),
        ("GET", "/trader-orchestrator/status"),
        ("PUT", "/trader-orchestrator/settings"),
        ("POST", "/trader-orchestrator/start"),
        ("POST", "/trader-orchestrator/stop"),
        ("POST", "/trader-orchestrator/kill-switch"),
        ("POST", "/trader-orchestrator/live/preflight"),
        ("POST", "/trader-orchestrator/live/arm"),
        ("POST", "/trader-orchestrator/live/start"),
        ("POST", "/trader-orchestrator/live/stop"),
    }
    assert actual == expected


def test_trader_orchestrator_mutation_routes_have_no_auth_dependency():
    for route in trader_orchestrator_router.routes:
        methods = {str(method or "").upper() for method in getattr(route, "methods", set())}
        if methods.isdisjoint({"POST", "PUT", "PATCH", "DELETE"}):
            continue
        dependant = getattr(route, "dependant", None)
        if dependant is None:
            continue
        dependency_calls = [getattr(dep.call, "__name__", "") for dep in dependant.dependencies]
        assert "_require_orchestrator_auth" not in dependency_calls
