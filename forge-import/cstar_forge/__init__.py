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
    "run_notebook",
    "save_notebook_copy",
    "settings",
    "source_data",
]

# legacy_notebook (nb_engine/compute) is deprecated and pulls in nbformat +
# dask.distributed, neither declared as hard dependencies -- import it lazily
# on first attribute access so `import cstar_forge` never requires them.
_LEGACY_NOTEBOOK_ATTRS = {"run_notebook", "save_notebook_copy"}


def __getattr__(name: str):
    """Lazily resolve legacy_notebook re-exports (PEP 562)."""
    if name in _LEGACY_NOTEBOOK_ATTRS:
        from cstar_forge.legacy_notebook import nb_engine

        value = getattr(nb_engine, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
