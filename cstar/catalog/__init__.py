"""The C-Star domain catalog: a layered store of validated ModelSpec/DomainSpec
content, blueprints, and observations. See :mod:`cstar.catalog.domain_catalog`
for the catalog directory layout and the layering rules.

Importing this package is deliberately scan-free: ``default_catalog`` is exposed
through a lazy module ``__getattr__`` (PEP 562), mirroring
``cstar.catalog.domain_catalog``'s own lazy singleton, so ``import cstar.catalog``
never triggers an eager filesystem scan of the catalog stack.
"""

from typing import Any

from cstar.catalog.domain_catalog import (
    DomainCatalog,
    LayeredCatalog,
    build_catalog_stack,
    default_catalog_stack,
    user_catalog_root,
)

__all__ = [
    "DomainCatalog",
    "LayeredCatalog",
    "build_catalog_stack",
    "default_catalog",
    "default_catalog_stack",
    "user_catalog_root",
]


def __getattr__(name: str) -> Any:
    """Lazily delegate ``default_catalog`` to ``cstar.catalog.domain_catalog`` (PEP 562).

    Keeps ``import cstar.catalog`` scan-free: the module-level singleton is only
    constructed the first time ``cstar.catalog.default_catalog`` is accessed.
    """
    if name == "default_catalog":
        from cstar.catalog import domain_catalog

        return domain_catalog.default_catalog
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
