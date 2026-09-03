"""
cstar_forge: A utility for generating regional oceanographic modeling domains
and spawning reproducible C-Star workflows.
"""

# silence UCX warnings
import os
from typing import Any

os.environ["UCX_LOG_LEVEL"] = "error"

# NOTE: no eager submodule imports here. ``cstar`` loads the ``cstar.cli``
# entry point (``cstar_forge.cli``) on *every* invocation, and importing this
# package used to pull in roms-tools/xarray/dask/copernicusmarine (~4 s) before
# even ``cstar --version`` could print. Everything below resolves lazily via
# PEP 562 ``__getattr__`` on first attribute access; ``from cstar_forge import
# source_data`` and ``import cstar_forge.forge.executor`` behave as before.

# Public name -> (module, attribute-or-None). ``None`` means the name *is* the
# submodule.
_LAZY: dict[str, tuple[str, str | None]] = {
    "catalog": ("cstar_forge.catalog", None),
    "config": ("cstar_forge.config", None),
    "diagnostics": ("cstar_forge.diagnostics", None),
    "models": ("cstar_forge.models", None),
    "settings": ("cstar_forge.forge.settings", None),
    "source_data": ("cstar_forge.forge.source_data", None),
    "DomainCatalog": ("cstar_forge.domain_catalog", "DomainCatalog"),
    "LayeredCatalog": ("cstar_forge.domain_catalog", "LayeredCatalog"),
    "build_catalog_stack": ("cstar_forge.domain_catalog", "build_catalog_stack"),
    "default_catalog": ("cstar_forge.domain_catalog", "default_catalog"),
    "default_catalog_stack": ("cstar_forge.domain_catalog", "default_catalog_stack"),
    "user_catalog_root": ("cstar_forge.domain_catalog", "user_catalog_root"),
    "ForgeExecutor": ("cstar_forge.forge.executor", "ForgeExecutor"),
}

__all__ = [
    "DomainCatalog",
    "ForgeExecutor",
    "LayeredCatalog",
    "build_catalog_stack",
    "catalog",
    "config",
    "default_catalog",
    "default_catalog_stack",
    "diagnostics",
    "models",
    "settings",
    "source_data",
    "user_catalog_root",
]


def __getattr__(name: str) -> Any:
    """Lazily resolve public names (PEP 562).

    Keeps ``import cstar_forge`` cheap: no filesystem scans (``default_catalog``
    mirrors ``domain_catalog.__getattr__``) and no heavy scientific-stack imports
    until a name is actually used. Uses ``importlib.import_module`` rather than
    ``from cstar_forge import x`` -- the latter re-enters this ``__getattr__`` via
    the fromlist ``hasattr`` check and recurses infinitely.
    """
    try:
        module_name, attr = _LAZY[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    import importlib

    module = importlib.import_module(module_name)
    value = module if attr is None else getattr(module, attr)
    if attr is None:
        # Cache submodules only; ``default_catalog`` is itself lazy and must
        # stay re-resolvable, and the others are cheap plain getattrs.
        globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
