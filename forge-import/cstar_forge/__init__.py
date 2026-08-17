"""
cstar_forge: A utility for generating regional oceanographic modeling domains
and spawning reproducible C-Star workflows.
"""

# silence UCX warnings
import os
from typing import Any

os.environ["UCX_LOG_LEVEL"] = "error"

from cstar_forge import catalog, config, diagnostics, models
from cstar_forge.domain_catalog import (
    DomainCatalog,
    LayeredCatalog,
    build_catalog_stack,
    default_catalog_stack,
    user_catalog_root,
)
from cstar_forge.forge import settings, source_data
from cstar_forge.forge.executor import ForgeExecutor

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
    """Lazily resolve ``default_catalog`` (PEP 562).

    ``import cstar_forge`` must not scan the filesystem; construction is
    deferred to first access, mirroring ``domain_catalog.__getattr__``.
    """
    if name == "default_catalog":
        from cstar_forge import domain_catalog

        return domain_catalog.default_catalog
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
