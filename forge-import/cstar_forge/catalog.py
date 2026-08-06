"""
Catalog module: backward-compatible wrapper around DomainCatalog.

Blueprint discovery and DataFrame loading now live in DomainCatalog.
This module keeps the ``BlueprintCatalog`` class and the ``blueprint``
convenience instance for code that already imports from here.
"""

from pathlib import Path
from typing import Any

from cstar_forge.domain_catalog import DomainCatalog, default_catalog


class BlueprintCatalog:
    """Thin wrapper around DomainCatalog for backward compatibility.

    New code should call ``default_catalog.blueprintDF()`` directly.
    """

    def __init__(self, blueprints_dir: Path | None = None) -> None:
        if blueprints_dir is None:
            self._catalog: DomainCatalog = default_catalog
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


# Convenience instance
blueprint = BlueprintCatalog()
