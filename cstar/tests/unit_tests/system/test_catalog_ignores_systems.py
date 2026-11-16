"""The catalog does not read, move or copy the `systems/` directory beside its specs.

System files live under each catalog root (the bundled ones in
``cstar/catalog/bundled/systems/``) but are not a catalog kind: the catalog's
scans, `cstar admin migrate-catalog` and a catalog initialised from the bundled
one must leave them alone.
"""

import shutil
from pathlib import Path

from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog
from cstar.cli.admin.migrate_catalog import migrate_catalog_layout
from cstar.system.spec import BUNDLED_SYSTEMS_DIR

SYSTEMS = "systems"


def _catalog_paths(catalog: DomainCatalog) -> list[Path]:
    """Every file the catalog's scans found, current and legacy layout."""
    paths = [p for entries in catalog._specs.values() for p in entries.values()]
    paths += [p for entries in catalog._blueprints.values() for p in entries.values()]
    paths += [p for *_, p in catalog.legacy_specs]
    paths += [p for *_, p in catalog.legacy_blueprints]
    return paths


def _tree(root: Path) -> dict[str, bytes | None]:
    """Snapshot a directory tree: relative path -> file bytes (None for a directory)."""
    return {
        str(p.relative_to(root)): p.read_bytes() if p.is_file() else None
        for p in sorted(root.rglob("*"))
    }


def test_bundled_systems_sit_in_the_bundled_catalog() -> None:
    """The bundled system files are where the catalog's bundled root is."""
    assert BUNDLED_SYSTEMS_DIR == _DEFAULT_CATALOG_ROOT.resolve() / SYSTEMS
    assert list(BUNDLED_SYSTEMS_DIR.glob("*.yaml"))


def test_bundled_catalog_scan_ignores_systems() -> None:
    """The bundled catalog lists no entry from its `systems/` directory."""
    catalog = DomainCatalog()

    assert _catalog_paths(catalog)
    assert not [p for p in _catalog_paths(catalog) if SYSTEMS in p.parts]


def test_legacy_layout_scan_and_migration_ignore_systems(tmp_path: Path) -> None:
    """A catalog in the legacy layout, with a `systems/` directory, is migrated
    without its system files being read, moved or reported.
    """
    root = tmp_path / "catalog"
    shutil.copytree(BUNDLED_SYSTEMS_DIR, root / SYSTEMS)
    (root / "ModelSpec" / "m").mkdir(parents=True)
    (root / "ModelSpec" / "m" / "model.yaml").write_text("description: m\n")
    (root / "blueprints").mkdir()
    (root / "blueprints" / "B_flat.yaml").write_text("flat\n")
    before = _tree(root / SYSTEMS)

    catalog = DomainCatalog(root, suppress_validation=True)
    assert catalog.legacy_specs  # the legacy layout is seen
    assert not [p for p in _catalog_paths(catalog) if SYSTEMS in p.parts]

    report = migrate_catalog_layout(root)

    touched = [
        *(p for pair in report.moved for p in pair),
        *(p for pair in report.skipped_collisions for p in pair),
        *report.removed_dirs,
        *report.removed_files,
        *report.left_in_place,
    ]
    assert report.moved
    assert not [p for p in touched if SYSTEMS in p.parts]
    assert _tree(root / SYSTEMS) == before


def test_initialising_from_the_bundled_catalog_copies_no_systems(
    tmp_path: Path,
) -> None:
    """A catalog initialised from the bundled one gets its specs, not its systems."""
    root = tmp_path / "mine"

    DomainCatalog(root, initialize_catalog_from="local")

    assert (root / "specs").is_dir()
    assert not (root / SYSTEMS).exists()
