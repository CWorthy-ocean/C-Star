"""
Dependency-direction guard for the forge-application boundary.

The "forge application" (execution code) lives in ``cstar.applications.forge``. For its
prior relocation from the standalone ``cstar-forge`` repo to stay a mechanical move, and to
keep it relocatable in the future, the forge-application modules must NOT depend on
Forge's *authoring / host* layer:

- authoring / curation: ``cstar.catalog``, ``cstar.applications.forge.resolve``,
  ``cstar.wizard``
- host resolution: ``cstar.applications.forge.config`` (paths/machine must be *injected*
  at execution time so C-Star can supply its own), and ``cstar.applications.forge.runtime``.

This test encodes that rule. Today the code is not yet fully compliant; each remaining
violation is listed in ``_KNOWN_VIOLATIONS`` with the phase that resolves it. The guard's
job right now is to (a) document the target boundary for collaborators and (b) fail on any
*new* violation — the allowlist may only shrink, never grow.
"""

import ast
from pathlib import Path

import cstar
import cstar.applications.forge

_PKG = Path(cstar.applications.forge.__file__).parent

# Modules excluded from the guard: the authoring/host modules themselves (app.py wires
# them together at the package boundary, so it is exempt too).
_EXCLUDED_MODULES = {"__init__", "resolve", "models", "config", "runtime", "app"}

# Modules that make up (or will make up) the relocatable forge application. Discovered
# from the package directory so this list can't drift from what's actually on disk.
_FORGE_APP_MODULES = tuple(
    sorted(p.stem for p in _PKG.glob("*.py") if p.stem not in _EXCLUDED_MODULES)
)

# Dotted module prefixes the application must not depend on (authoring/curation +
# host/glue). A hit is any import that equals one of these or is a submodule of one.
_FORBIDDEN_PREFIXES = (
    "cstar.catalog",
    "cstar.applications.forge.resolve",
    "cstar.applications.forge.config",
    "cstar.applications.forge.runtime",
    "cstar.wizard",
)

# Known, pre-existing violations to be resolved during the decomposition. Each entry is
# ``(forge_app_module, forbidden_prefix)``. This set may only SHRINK — never add to it.
# EMPTY: the guarded forge-application modules are fully config/authoring-free. Host is
# injected (HostPaths via process_forge_blueprint); Forge's disposable resolver lives in
# cstar.applications.forge.config / cstar.applications.forge.runtime. See docs/dev-notes/architecture-decomposition-plan.md.
_KNOWN_VIOLATIONS: set[tuple[str, str]] = set()


def _module_path(short_name: str) -> Path:
    return _PKG / f"{short_name}.py"


def _match_forbidden(candidate: str) -> str | None:
    for prefix in _FORBIDDEN_PREFIXES:
        if candidate == prefix or candidate.startswith(prefix + "."):
            return prefix
    return None


def _forbidden_imports(module_name: str) -> set[str]:
    """Return the forbidden dotted prefixes imported by a forge-app module."""
    src = _module_path(module_name).read_text()
    tree = ast.parse(src)
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            # `from cstar.applications.forge.config import X`
            hit = _match_forbidden(node.module)
            if hit:
                found.add(hit)
            # `from cstar.applications.forge import config`  or  `from cstar import catalog`
            for alias in node.names:
                hit = _match_forbidden(f"{node.module}.{alias.name}")
                if hit:
                    found.add(hit)
        elif isinstance(node, ast.Import):
            for alias in node.names:  # `import cstar.wizard.wizard`
                hit = _match_forbidden(alias.name)
                if hit:
                    found.add(hit)
    return found


def test_forge_app_does_not_import_authoring_or_host():
    """Every forge-application module must avoid the forbidden authoring/host modules,
    except the shrinking ``_KNOWN_VIOLATIONS`` allowlist.
    """
    current: set[tuple[str, str]] = set()
    for mod in _FORGE_APP_MODULES:
        for forbidden in _forbidden_imports(mod):
            current.add((mod, forbidden))

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
        for forbidden in _forbidden_imports(mod):
            current.add((mod, forbidden))

    stale = _KNOWN_VIOLATIONS - current
    assert not stale, (
        f"Stale allowlist entries (violation resolved — remove from _KNOWN_VIOLATIONS): "
        f"{sorted(stale)}."
    )


def test_no_file_imports_legacy_cstar_forge_package():
    """Nothing under ``cstar/`` (excluding tests) may still import the pre-move
    ``cstar_forge`` top-level package — the whole point of the relocation.
    """
    root = Path(cstar.__file__).parent
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        if "tests" in path.relative_to(root).parts:
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module == "cstar_forge" or node.module.startswith(
                    "cstar_forge."
                ):
                    offenders.append(str(path))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "cstar_forge" or alias.name.startswith(
                        "cstar_forge."
                    ):
                        offenders.append(str(path))
    assert not offenders, (
        f"Files still importing legacy cstar_forge: {sorted(offenders)}"
    )
