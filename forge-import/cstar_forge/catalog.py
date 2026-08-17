"""
Catalog module: backward-compatible wrapper around DomainCatalog.

Blueprint discovery and DataFrame loading now live in DomainCatalog.
This module keeps the ``BlueprintCatalog`` class and the ``blueprint``
convenience instance for code that already imports from here.
"""

from pathlib import Path
from typing import Any

from cstar_forge.domain_catalog import DomainCatalog


class BlueprintCatalog:
    """Thin wrapper around DomainCatalog for backward compatibility.

    New code should call ``default_catalog.blueprintDF()`` directly.
    """

    def __init__(self, blueprints_dir: Path | None = None) -> None:
        if blueprints_dir is None:
            # Local import: keeps this module import scan-free (default_catalog
            # is itself lazily constructed -- see domain_catalog.__getattr__).
            from cstar_forge.domain_catalog import default_catalog

            # default_catalog is a LayeredCatalog (duck-types DomainCatalog's
            # read/write surface -- see domain_catalog.LayeredCatalog).
            self._catalog = default_catalog
        else:
            # Infer catalog root as the parent of the supplied blueprints dir.
            self._catalog = DomainCatalog(catalog_root=Path(blueprints_dir).parent)

    # ------------------------------------------------------------------
    # Delegating methods (preserve original API)
    # ------------------------------------------------------------------

    def find_blueprint_files(self) -> list[Path]:
        return self._catalog._find_roms_marbl_blueprint_files()

    def load_blueprint(self, blueprint_path: Path) -> dict[str, Any]:
        return self._catalog._load_roms_marbl_blueprint_yaml(blueprint_path)

    def load_grid_kwargs(self, grid_yaml_path: Path) -> dict[str, Any]:
        return self._catalog._load_grid_kwargs(grid_yaml_path)

    def _extract_model_and_grid_name(self, blueprint_name: str):
        return self._catalog._extract_model_and_grid_name(blueprint_name)

    def load(self):
        """Deprecated alias for blueprintDF()."""
        return self._catalog.roms_marbl_blueprint_df()

    def blueprintDF(self):
        return self._catalog.roms_marbl_blueprint_df()


_blueprint: BlueprintCatalog | None = None


def __getattr__(name: str) -> Any:
    """Lazily construct the deprecated ``blueprint`` convenience instance (PEP 562).

    Keeps ``import cstar_forge.catalog`` scan-free; construction (which pulls
    in ``default_catalog``) only happens once ``blueprint`` is actually used.
    """
    global _blueprint
    if name == "blueprint":
        if _blueprint is None:
            _blueprint = BlueprintCatalog()
        return _blueprint
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
