"""
cstar_forge: A utility for generating regional oceanographic modeling domains
and spawning reproducible C-Star workflows.
"""

from cstar_forge import catalog, config, diagnostics, models, settings, source_data
from cstar_forge._core import CstarSpecBuilder, CstarSpecEngine, resolve_catalog_dir
from cstar_forge.domain_catalog import DomainCatalog, default_catalog
from cstar_forge.nb_engine import run_notebook, save_notebook_copy

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
