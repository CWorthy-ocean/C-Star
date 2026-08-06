"""
cstar_forge: A utility for generating regional oceanographic modeling domains
and spawning reproducible C-Star workflows.
"""

# silence UCX warnings
import os

os.environ["UCX_LOG_LEVEL"] = "error"

from cstar_forge import catalog, config, diagnostics, models
from cstar_forge.domain_catalog import DomainCatalog, default_catalog
from cstar_forge.forge import settings, source_data
from cstar_forge.forge.executor import ForgeExecutor

__all__ = [
    "DomainCatalog",
    "ForgeExecutor",
    "catalog",
    "config",
    "default_catalog",
    "diagnostics",
    "models",
    "settings",
    "source_data",
]
