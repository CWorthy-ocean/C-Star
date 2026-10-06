from collections.abc import Callable, Generator
from pathlib import Path
from unittest.mock import patch

import pytest
import typer
from typer.main import get_command
from typer.testing import CliRunner

from cstar.catalog.domain_catalog import DomainCatalog
from cstar.cli.admin.migrate_catalog import (
    CatalogMigrationReport,
    app,
    migrate_catalog_layout,
)
from cstar.cli.cli import attach_subcommands


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def _tree(root: Path) -> dict[str, str | None]:
    """Snapshot a directory tree: relative path -> file text (None for a directory)."""
    return {
        str(p.relative_to(root)): p.read_text() if p.is_file() else None
        for p in sorted(root.rglob("*"))
    }


@pytest.fixture
def legacy_root(tmp_path: Path) -> Path:
    """A catalog root holding all three legacy blueprint forms.

    ``suppress_validation`` means no ModelSpec etc. is needed to read it.

    - flat forge: ``blueprints/flat-forge.forge_blueprint.yaml``
    - flat roms_marbl: ``blueprints/B_flat-roms.yml`` (a ``.yml`` source)
    - per-machine, fully movable: ``blueprints/anvil/clean/B_clean.yaml``
    - per-machine with leftovers: ``blueprints/anvil/messy/{B_messy.yaml,Build/,_grid.yaml}``
    """
    root = tmp_path / "catalog"
    bp = root / "blueprints"
    _write(bp / "flat-forge.forge_blueprint.yaml", "forge-flat")
    _write(bp / "B_flat-roms.yml", "roms-flat")
    _write(bp / "anvil" / "clean" / "B_clean.yaml", "roms-clean")
    _write(bp / "anvil" / "messy" / "B_messy.yaml", "roms-messy")
    _write(bp / "anvil" / "messy" / "_grid.yaml", "grid")
    _write(bp / "anvil" / "messy" / "Build" / "keep.txt", "build")
    return root


def test_migrate_catalog_layout_report_and_tree(legacy_root: Path) -> None:
    """Every legacy form moves to ``blueprints/<application>/<name>.yaml`` and
    leftovers pin their directory.
    """
    root = legacy_root.resolve()
    bp = root / "blueprints"

    report = migrate_catalog_layout(root)

    assert sorted(report.moved) == sorted(
        [
            (
                bp / "flat-forge.forge_blueprint.yaml",
                bp / "forge" / "flat-forge.yaml",
            ),
            (bp / "B_flat-roms.yml", bp / "roms_marbl" / "flat-roms.yaml"),
            (
                bp / "anvil" / "clean" / "B_clean.yaml",
                bp / "roms_marbl" / "clean.yaml",
            ),
            (
                bp / "anvil" / "messy" / "B_messy.yaml",
                bp / "roms_marbl" / "messy.yaml",
            ),
        ]
    )
    assert report.skipped_collisions == []
    assert report.removed_dirs == [bp / "anvil" / "clean"]
    assert report.left_in_place == [bp / "anvil" / "messy"]

    assert (bp / "forge" / "flat-forge.yaml").read_text() == "forge-flat"
    assert (bp / "roms_marbl" / "flat-roms.yaml").read_text() == "roms-flat"
    assert (bp / "roms_marbl" / "clean.yaml").read_text() == "roms-clean"
    assert (bp / "roms_marbl" / "messy.yaml").read_text() == "roms-messy"
    # `clean/` is gone, `messy/` keeps what is not a blueprint, `anvil/` stays
    # because `messy/` is still inside it (and is not itself reported).
    assert not (bp / "anvil" / "clean").exists()
    assert (bp / "anvil" / "messy" / "_grid.yaml").is_file()
    assert (bp / "anvil" / "messy" / "Build" / "keep.txt").is_file()


def test_migrated_catalog_reloads_without_legacy_entries(legacy_root: Path) -> None:
    """After migration the catalog has nothing legacy left and resolves the
    moved entries.
    """
    root = legacy_root.resolve()
    migrate_catalog_layout(root)

    reloaded = DomainCatalog(root, suppress_validation=True)

    assert reloaded.legacy_blueprints == []
    assert reloaded.blueprint_names("forge") == ["flat-forge"]
    assert reloaded.blueprint_names("roms_marbl") == ["clean", "flat-roms", "messy"]
    assert reloaded.blueprint_path("forge", "flat-forge") == (
        root / "blueprints" / "forge" / "flat-forge.yaml"
    )
    assert reloaded.blueprint_path("roms_marbl", "messy").read_text() == "roms-messy"


def test_migrate_catalog_layout_collision_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A destination that already exists is never overwritten and its source stays.

    The catalog's own shadowing rule hides a legacy entry whose current-layout
    twin it can see, so this guard is exercised by feeding the plan an entry
    directly (e.g. a destination that appeared after the scan).
    """
    root = tmp_path.resolve()
    src = _write(root / "blueprints" / "B_dup.yaml", "legacy-dup")
    dst = _write(root / "blueprints" / "roms_marbl" / "dup.yaml", "current-dup")
    monkeypatch.setattr(
        DomainCatalog,
        "legacy_blueprints",
        property(lambda self: [("roms_marbl", "dup", src)]),
    )
    snapshot = _tree(root)

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport(skipped_collisions=[(src, dst)])
    assert _tree(root) == snapshot


def test_migrate_catalog_layout_second_run_is_noop(legacy_root: Path) -> None:
    """Migrating twice moves nothing the second time."""
    root = legacy_root.resolve()
    migrate_catalog_layout(root)
    snapshot = _tree(root)

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport()
    assert _tree(root) == snapshot


def test_migrate_catalog_layout_dry_run_matches_real_run(legacy_root: Path) -> None:
    """A dry run leaves the tree byte-identical and reports what a real run does."""
    root = legacy_root.resolve()
    snapshot = _tree(root)

    planned = migrate_catalog_layout(root, dry_run=True)

    assert _tree(root) == snapshot
    assert migrate_catalog_layout(root) == planned


def test_migrate_catalog_layout_machine_named_like_application(tmp_path: Path) -> None:
    """A legacy machine directory that is also an application directory keeps
    holding the migrated file and is not removed.
    """
    root = tmp_path.resolve()
    _write(root / "blueprints" / "roms_marbl" / "x" / "B_x.yaml", "x")

    report = migrate_catalog_layout(root)

    assert (root / "blueprints" / "roms_marbl" / "x.yaml").read_text() == "x"
    assert report.moved == [
        (
            root / "blueprints" / "roms_marbl" / "x" / "B_x.yaml",
            root / "blueprints" / "roms_marbl" / "x.yaml",
        )
    ]
    assert report.removed_dirs == [root / "blueprints" / "roms_marbl" / "x"]
    assert report.left_in_place == []


def test_cli_migrate_catalog(legacy_root: Path) -> None:
    """The command moves blueprints and prints a summary."""
    result = CliRunner().invoke(app, [str(legacy_root)])

    assert result.exit_code == 0, result.output
    assert "Summary" in result.stdout
    assert "[dry-run]" not in result.stdout
    assert "left in place" in result.stdout
    assert (legacy_root / "blueprints" / "forge" / "flat-forge.yaml").is_file()


def test_cli_migrate_catalog_reports_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A skipped collision is named in the output."""
    src = _write(tmp_path / "blueprints" / "B_dup.yaml", "legacy-dup")
    _write(tmp_path / "blueprints" / "roms_marbl" / "dup.yaml", "current-dup")
    monkeypatch.setattr(
        DomainCatalog,
        "legacy_blueprints",
        property(lambda self: [("roms_marbl", "dup", src.resolve())]),
    )

    result = CliRunner().invoke(app, [str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "skipped (exists)" in result.stdout
    assert "skipped 1 collision(s)" in result.stdout


def test_cli_migrate_catalog_dry_run(legacy_root: Path) -> None:
    """`--dry-run` prefixes every line and changes nothing."""
    snapshot = _tree(legacy_root)

    result = CliRunner().invoke(app, [str(legacy_root), "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "[dry-run]" in result.stdout
    assert "Summary" in result.stdout
    assert all(
        line.startswith("[dry-run]") for line in result.stdout.splitlines() if line
    )
    assert _tree(legacy_root) == snapshot


def test_cli_migrate_catalog_prints_bracketed_path_verbatim(tmp_path: Path) -> None:
    """A `[x]` in a path is not swallowed as a Rich style tag."""
    root = tmp_path / "cat[x]"
    _write(root / "blueprints" / "w.forge_blueprint.yaml", "w")

    result = CliRunner().invoke(app, [str(root), "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "cat[x]" in result.stdout.replace("\n", "")


def test_cli_migrate_catalog_nothing_to_do(tmp_path: Path) -> None:
    """A catalog already in the current layout reports nothing to migrate."""
    _write(tmp_path / "blueprints" / "forge" / "ok.yaml", "ok")

    result = CliRunner().invoke(app, [str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "Nothing to migrate" in result.stdout
    assert "Summary" not in result.stdout


@pytest.fixture
def build_app() -> Generator[Callable[[], typer.Typer]]:
    """Build a root app with only the core subcommands (no plugins)."""
    with patch("cstar.cli.cli.entry_points", return_value=[]):

        def _build() -> typer.Typer:
            root = typer.Typer()
            attach_subcommands(root)
            return root

        yield _build


def test_migrate_catalog_is_attached_under_admin(
    build_app: Callable[[], typer.Typer],
) -> None:
    """`cstar admin migrate-catalog --help` resolves from the root app."""
    root = build_app()
    assert "migrate-catalog" in get_command(root).commands["admin"].commands  # type: ignore[attr-defined]

    result = CliRunner().invoke(root, ["admin", "migrate-catalog", "--help"])

    assert result.exit_code == 0, result.output
    assert "--dry-run" in result.stdout
