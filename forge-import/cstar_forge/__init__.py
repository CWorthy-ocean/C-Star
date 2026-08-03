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
from cstar_forge.legacy_notebook.nb_engine import run_notebook, save_notebook_copy

__all__ = [
    "DomainCatalog",
    "ForgeExecutor",
    "catalog",
    "config",
    "default_catalog",
    "diagnostics",
    "models",
    "run_notebook",
    "save_notebook_copy",
    "settings",
    "source_data",
]
