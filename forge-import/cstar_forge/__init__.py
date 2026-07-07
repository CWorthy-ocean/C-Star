"""
cstar_forge: A utility for generating regional oceanographic modeling domains
and spawning reproducible C-Star workflows.
"""

from . import catalog, config, diagnostics, models, settings, source_data
from ._core import CstarSpecBuilder, CstarSpecEngine, resolve_catalog_dir
from .domain_catalog import DomainCatalog, default_catalog
from .nb_engine import run_notebook, save_notebook_copy

__all__ = [
    "CstarSpecBuilder",
    "CstarSpecEngine",
    "DomainCatalog",
    "catalog",
    "config",
    "default_catalog",
    "diagnostics",
    "models",
    "resolve_catalog_dir",
    "run_notebook",
    "save_notebook_copy",
    "settings",
    "source_data",
]
