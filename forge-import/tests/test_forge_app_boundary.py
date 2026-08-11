"""
Dependency-direction guard for the forge-application boundary.

The "forge application" (execution code — blueprint = ``ForgeBlueprint``) is being carved
out of Forge so it can eventually relocate into C-Star as an application (see
``docs/dev-notes/architecture-decomposition-plan.md``). For that relocation to stay a mechanical
move, the forge-application modules must NOT depend on Forge's *authoring / host* layer:

- authoring / curation: ``catalog``, ``domain_catalog``, ``forge_blueprint_resolve``,
  ``forge_blueprint_wizard``
- host resolution: ``config`` (paths/machine must be *injected* at execution time so
  C-Star can supply its own), and the transitional god-object ``_core``.

This test encodes that rule. Today the code is not yet fully compliant; each remaining
violation is listed in ``_KNOWN_VIOLATIONS`` with the phase that resolves it. The guard's
job right now is to (a) document the target boundary for collaborators and (b) fail on any
*new* violation — the allowlist may only shrink, never grow.
"""

import ast
from pathlib import Path

import cstar_forge

_PKG = Path(cstar_forge.__file__).parent


def _module_path(short_name: str) -> Path:
    """Resolve a forge-app module wherever it currently lives (top-level or under the
    ``forge/`` package), so this guard survives the incremental Phase-C relocation.
    """
    for cand in (_PKG / "forge" / f"{short_name}.py", _PKG / f"{short_name}.py"):
        if cand.exists():
            return cand
    raise FileNotFoundError(f"forge-app module not found: {short_name}")


# Modules that make up (or will make up) the relocatable forge application.
_FORGE_APP_MODULES = (
    "input_data",
    "source_data",
    "source_registry",
    "settings",
    "forge_blueprint",
    "forge_blueprint_engine",
    "executor",
    "namelist_model",
    "util",
)

# Forge modules the application must not depend on (authoring/curation + host/glue).
_FORBIDDEN = {
    "catalog",
    "domain_catalog",
    "forge_blueprint_resolve",
    "forge_blueprint_wizard",
    "config",
    "_core",
}

# Known, pre-existing violations to be resolved during the decomposition. Each entry is
# ``(forge_app_module, forbidden_module)``. This set may only SHRINK — never add to it.
# EMPTY: the guarded forge-application modules are fully config/authoring-free. Host is
# injected (HostPaths via process_forge_blueprint); Forge's disposable resolver lives in
# cstar_forge.config / cstar_forge.run. See docs/dev-notes/architecture-decomposition-plan.md.
_KNOWN_VIOLATIONS: set[tuple[str, str]] = set()


def _imported_forge_submodules(module_name: str) -> set[str]:
    """Return the set of ``cstar_forge`` submodule names imported by a module."""
    src = _module_path(module_name).read_text()
    tree = ast.parse(src)
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            # `from cstar_forge.X import ...`  or  `from cstar_forge import X, Y`
            if node.module == "cstar_forge":
                found.update(alias.name for alias in node.names)
            elif node.module.startswith("cstar_forge."):
                found.add(node.module.split(".")[1])
        elif isinstance(node, ast.Import):
            for alias in node.names:  # `import cstar_forge.X`
                if alias.name.startswith("cstar_forge."):
                    found.add(alias.name.split(".")[1])
    return found


def test_forge_app_does_not_import_authoring_or_host():
    """Every forge-application module must avoid the forbidden authoring/host modules,
    except the shrinking ``_KNOWN_VIOLATIONS`` allowlist.
    """
    current: set[tuple[str, str]] = set()
    for mod in _FORGE_APP_MODULES:
        for imported in _imported_forge_submodules(mod):
            if imported in _FORBIDDEN:
                current.add((mod, imported))

    new_violations = current - _KNOWN_VIOLATIONS
    assert not new_violations, (
        "New forge-app boundary violation(s) — a forge-application module imports an "
        f"authoring/host module: {sorted(new_violations)}. Inject the dependency (e.g. "
        "pass paths in) or move the shared piece onto ForgeBlueprint instead. See "
        "docs/dev-notes/architecture-decomposition-plan.md."
    )


def test_known_violations_allowlist_only_shrinks():
    """The allowlist must never list a violation that no longer exists (keep it honest —
    remove entries as the decomposition resolves them).
    """
    current: set[tuple[str, str]] = set()
    for mod in _FORGE_APP_MODULES:
        for imported in _imported_forge_submodules(mod):
            if imported in _FORBIDDEN:
                current.add((mod, imported))

    stale = _KNOWN_VIOLATIONS - current
    assert not stale, (
        f"Stale allowlist entries (violation resolved — remove from _KNOWN_VIOLATIONS): "
        f"{sorted(stale)}."
    )
