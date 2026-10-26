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
    """A catalog root holding all four legacy blueprint forms and a legacy
    spec directory of every kind.

    ``suppress_validation`` means no ModelSpec etc. is needed to read it.

    - flat forge: ``blueprints/flat-forge.forge_blueprint.yaml``
    - flat ucla_roms: ``blueprints/B_flat-roms.yml`` (a ``.yml`` source)
    - per-machine, fully movable: ``blueprints/anvil/clean/B_clean.yaml``
    - per-machine with leftovers: ``blueprints/anvil/messy/{B_messy.yaml,Build/,_grid.yaml}``
    - earlier application name: ``blueprints/roms_marbl/earlier.yaml``
    - specs: ``ModelSpec/m/model.yaml``, ``ForcingSpec/f/Forcing.yml`` (a
      ``.yml`` source), ``OutputSpec/o/Output.yaml``, ``CdrSpec/c/Cdr.yaml``
    - a domain whose ``Assets/`` holds only ``.gitkeep``:
      ``DomainSpec/clean-domain/{Domain.yaml,Assets/.gitkeep}``
    - a domain with a real asset: ``DomainSpec/asset-domain/{Domain.yaml,Assets/bathy.nc}``
    """
    root = tmp_path / "catalog"
    _write(root / "ModelSpec" / "m" / "model.yaml", "model-m")
    _write(root / "DomainSpec" / "clean-domain" / "Domain.yaml", "domain-clean")
    _write(root / "DomainSpec" / "clean-domain" / "Assets" / ".gitkeep", "")
    _write(root / "DomainSpec" / "asset-domain" / "Domain.yaml", "domain-asset")
    _write(root / "DomainSpec" / "asset-domain" / "Assets" / "bathy.nc", "asset")
    _write(root / "ForcingSpec" / "f" / "Forcing.yml", "forcing-f")
    _write(root / "OutputSpec" / "o" / "Output.yaml", "output-o")
    _write(root / "CdrSpec" / "c" / "Cdr.yaml", "cdr-c")
    bp = root / "blueprints"
    _write(bp / "flat-forge.forge_blueprint.yaml", "forge-flat")
    _write(bp / "B_flat-roms.yml", "roms-flat")
    _write(bp / "anvil" / "clean" / "B_clean.yaml", "roms-clean")
    _write(bp / "anvil" / "messy" / "B_messy.yaml", "roms-messy")
    _write(bp / "anvil" / "messy" / "_grid.yaml", "grid")
    _write(bp / "anvil" / "messy" / "Build" / "keep.txt", "build")
    _write(bp / "roms_marbl" / "earlier.yaml", "roms-earlier")
    return root


def test_migrate_catalog_layout_report_and_tree(legacy_root: Path) -> None:
    """Every legacy form moves to ``blueprints/<application>/<name>.yaml`` or
    ``<Kind>/<name>.yaml`` and leftovers pin their directory.
    """
    root = legacy_root.resolve()
    bp = root / "blueprints"
    clean = root / "DomainSpec" / "clean-domain"
    asset = root / "DomainSpec" / "asset-domain"

    report = migrate_catalog_layout(root)

    assert sorted(report.moved) == sorted(
        [
            (
                bp / "flat-forge.forge_blueprint.yaml",
                bp / "forge" / "flat-forge.yaml",
            ),
            (bp / "B_flat-roms.yml", bp / "ucla_roms" / "flat-roms.yaml"),
            (
                bp / "anvil" / "clean" / "B_clean.yaml",
                bp / "ucla_roms" / "clean.yaml",
            ),
            (
                bp / "anvil" / "messy" / "B_messy.yaml",
                bp / "ucla_roms" / "messy.yaml",
            ),
            (
                bp / "roms_marbl" / "earlier.yaml",
                bp / "ucla_roms" / "earlier.yaml",
            ),
            (root / "ModelSpec" / "m" / "model.yaml", root / "ModelSpec" / "m.yaml"),
            (asset / "Domain.yaml", root / "DomainSpec" / "asset-domain.yaml"),
            (clean / "Domain.yaml", root / "DomainSpec" / "clean-domain.yaml"),
            (
                root / "ForcingSpec" / "f" / "Forcing.yml",
                root / "ForcingSpec" / "f.yaml",
            ),
            (root / "OutputSpec" / "o" / "Output.yaml", root / "OutputSpec" / "o.yaml"),
            (root / "CdrSpec" / "c" / "Cdr.yaml", root / "CdrSpec" / "c.yaml"),
        ]
    )
    assert report.skipped_collisions == []
    assert report.removed_files == [clean / "Assets" / ".gitkeep"]
    assert report.removed_dirs == [
        bp / "anvil" / "clean",
        bp / "roms_marbl",
        root / "ModelSpec" / "m",
        clean / "Assets",
        clean,
        root / "ForcingSpec" / "f",
        root / "OutputSpec" / "o",
        root / "CdrSpec" / "c",
    ]
    assert report.left_in_place == [bp / "anvil" / "messy", asset]

    assert (bp / "forge" / "flat-forge.yaml").read_text() == "forge-flat"
    assert (bp / "ucla_roms" / "flat-roms.yaml").read_text() == "roms-flat"
    assert (bp / "ucla_roms" / "clean.yaml").read_text() == "roms-clean"
    assert (bp / "ucla_roms" / "messy.yaml").read_text() == "roms-messy"
    assert (bp / "ucla_roms" / "earlier.yaml").read_text() == "roms-earlier"
    # the earlier-name directory is gone with its only file
    assert not (bp / "roms_marbl").exists()
    # `clean/` is gone, `messy/` keeps what is not a blueprint, `anvil/` stays
    # because `messy/` is still inside it (and is not itself reported).
    assert not (bp / "anvil" / "clean").exists()
    assert (bp / "anvil" / "messy" / "_grid.yaml").is_file()
    assert (bp / "anvil" / "messy" / "Build" / "keep.txt").is_file()

    # each spec kind directory now holds only `<name>.yaml` files, apart from the
    # domain whose real asset keeps its directory
    assert _tree(root / "ModelSpec") == {"m.yaml": "model-m"}
    assert _tree(root / "ForcingSpec") == {"f.yaml": "forcing-f"}
    assert _tree(root / "OutputSpec") == {"o.yaml": "output-o"}
    assert _tree(root / "CdrSpec") == {"c.yaml": "cdr-c"}
    assert _tree(root / "DomainSpec") == {
        "asset-domain": None,
        "asset-domain.yaml": "domain-asset",
        "asset-domain/Assets": None,
        "asset-domain/Assets/bathy.nc": "asset",
        "clean-domain.yaml": "domain-clean",
    }


def test_migrated_catalog_reloads_without_legacy_entries(legacy_root: Path) -> None:
    """After migration the catalog has nothing legacy left and resolves the
    moved entries.
    """
    root = legacy_root.resolve()
    migrate_catalog_layout(root)

    reloaded = DomainCatalog(root, suppress_validation=True)

    assert reloaded.legacy_blueprints == []
    assert reloaded.legacy_specs == []
    assert reloaded.model_names == ["m"]
    assert reloaded.domain_names == ["asset-domain", "clean-domain"]
    assert reloaded.forcing_names == ["f"]
    assert reloaded.output_names == ["o"]
    assert reloaded.cdr_names == ["c"]
    assert reloaded.domain_path("clean-domain") == (
        root / "DomainSpec" / "clean-domain.yaml"
    )
    assert reloaded.blueprint_names("forge") == ["flat-forge"]
    assert reloaded.blueprint_names("ucla_roms") == [
        "clean",
        "earlier",
        "flat-roms",
        "messy",
    ]
    assert reloaded.blueprint_path("forge", "flat-forge") == (
        root / "blueprints" / "forge" / "flat-forge.yaml"
    )
    assert reloaded.blueprint_path("ucla_roms", "messy").read_text() == "roms-messy"


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
    dst = _write(root / "blueprints" / "ucla_roms" / "dup.yaml", "current-dup")
    monkeypatch.setattr(
        DomainCatalog,
        "legacy_blueprints",
        property(lambda self: [("ucla_roms", "dup", src)]),
    )
    snapshot = _tree(root)

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport(skipped_collisions=[(src, dst)])
    assert _tree(root) == snapshot


def test_migrate_catalog_layout_moves_winner_and_skips_duplicate_loser(
    tmp_path: Path,
) -> None:
    """Two legacy files for one application+name: the flat file (the one the
    catalog reads) moves and the per-machine duplicate is reported, not lost.
    """
    root = tmp_path.resolve()
    bp = root / "blueprints"
    flat = _write(bp / "B_dup.yaml", "flat")
    nested = _write(bp / "anvil" / "dup" / "B_dup.yaml", "nested")
    dst = bp / "ucla_roms" / "dup.yaml"

    report = migrate_catalog_layout(root)

    assert report.moved == [(flat, dst)]
    assert report.skipped_collisions == [(nested, dst)]
    assert dst.read_text() == "flat"
    assert nested.read_text() == "nested"
    # the loser's directories are untouched (the skip names the file)
    assert report.removed_dirs == []


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
    _write(root / "blueprints" / "ucla_roms" / "x" / "B_x.yaml", "x")

    report = migrate_catalog_layout(root)

    assert (root / "blueprints" / "ucla_roms" / "x.yaml").read_text() == "x"
    assert report.moved == [
        (
            root / "blueprints" / "ucla_roms" / "x" / "B_x.yaml",
            root / "blueprints" / "ucla_roms" / "x.yaml",
        )
    ]
    assert report.removed_dirs == [root / "blueprints" / "ucla_roms" / "x"]
    assert report.left_in_place == []


def test_migrate_catalog_layout_earlier_application_name_directory(
    tmp_path: Path,
) -> None:
    """`blueprints/roms_marbl/x.yaml` moves to `blueprints/ucla_roms/x.yaml`; a
    dry run plans that and the directory's removal and changes nothing, and the
    real run does both.
    """
    root = tmp_path.resolve()
    src = _write(root / "blueprints" / "roms_marbl" / "x.yaml", "x")
    dst = root / "blueprints" / "ucla_roms" / "x.yaml"
    snapshot = _tree(root)

    planned = migrate_catalog_layout(root, dry_run=True)

    assert planned == CatalogMigrationReport(
        moved=[(src, dst)], removed_dirs=[src.parent]
    )
    assert _tree(root) == snapshot

    report = migrate_catalog_layout(root)

    assert report == planned
    assert dst.read_text() == "x"
    assert not src.parent.exists()
    assert DomainCatalog(root, suppress_validation=True).legacy_blueprints == []


def test_migrate_catalog_layout_keeps_a_non_empty_earlier_name_directory(
    tmp_path: Path,
) -> None:
    """Whatever else sits in an earlier-name directory stays, and the directory
    with it, reported as left in place.
    """
    root = tmp_path.resolve()
    directory = root / "blueprints" / "roms_marbl"
    src = _write(directory / "x.yaml", "x")
    notes = _write(directory / "notes.txt", "keep me")
    dst = root / "blueprints" / "ucla_roms" / "x.yaml"

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport(
        moved=[(src, dst)], left_in_place=[directory]
    )
    assert dst.read_text() == "x"
    assert notes.read_text() == "keep me"
    assert not src.exists()


def test_migrate_catalog_layout_keeps_a_symlinked_earlier_name_directory(
    tmp_path: Path,
) -> None:
    """A symlinked earlier-name directory is reported as left in place, never
    removed, so the migration finishes instead of failing in `rmdir`.
    """
    root = (tmp_path / "catalog").resolve()
    shared = _write(tmp_path / "shared" / "x.yaml", "x").parent
    link = root / "blueprints" / "roms_marbl"
    link.parent.mkdir(parents=True)
    link.symlink_to(shared, target_is_directory=True)
    dst = root / "blueprints" / "ucla_roms" / "x.yaml"

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport(
        moved=[(link / "x.yaml", dst)], left_in_place=[link]
    )
    assert dst.read_text() == "x"
    assert link.is_symlink()


def test_migrate_catalog_layout_collision_in_earlier_name_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An earlier-name entry whose destination already exists is left in place,
    and so is the directory.
    """
    root = tmp_path.resolve()
    src = _write(root / "blueprints" / "roms_marbl" / "dup.yaml", "legacy-dup")
    other = _write(root / "blueprints" / "roms_marbl" / "other.yaml", "other")
    dst = root / "blueprints" / "ucla_roms" / "other.yaml"
    taken = _write(root / "blueprints" / "ucla_roms" / "dup.yaml", "current-dup")
    # the catalog hides an entry whose current twin it sees (see above), so the
    # plan is fed the entry directly
    monkeypatch.setattr(
        DomainCatalog,
        "legacy_blueprints",
        property(
            lambda self: [("ucla_roms", "dup", src), ("ucla_roms", "other", other)]
        ),
    )

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport(
        moved=[(other, dst)],
        skipped_collisions=[(src, taken)],
        left_in_place=[src.parent],
    )
    assert src.read_text() == "legacy-dup"
    assert taken.read_text() == "current-dup"


def test_migrate_catalog_layout_spec_collision_is_skipped(tmp_path: Path) -> None:
    """A legacy spec whose name already has a flat file (`.yaml` or `.yml`, the
    one the catalog reads) is left in place, its directory untouched, and the
    flat file is named as the destination; nothing else changes.
    """
    root = tmp_path.resolve()
    model = _write(root / "ModelSpec" / "dup" / "model.yaml", "legacy-model")
    model_flat = _write(root / "ModelSpec" / "dup.yml", "current-model")
    domain = _write(root / "DomainSpec" / "d" / "Domain.yaml", "legacy-domain")
    _write(root / "DomainSpec" / "d" / "Assets" / ".gitkeep", "")
    domain_flat = _write(root / "DomainSpec" / "d.yaml", "current-domain")
    snapshot = _tree(root)

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport(
        skipped_collisions=[(model, model_flat), (domain, domain_flat)]
    )
    assert _tree(root) == snapshot


def test_migrate_catalog_layout_keeps_a_symlinked_spec_directory(
    tmp_path: Path,
) -> None:
    """A spec directory that is a symbolic link is left in place and nothing
    inside its (possibly shared) target is cleaned up.
    """
    root = (tmp_path / "catalog").resolve()
    shared = _write(tmp_path / "shared" / "Domain.yaml", "domain").parent
    _write(shared / "Assets" / ".gitkeep", "")
    link = root / "DomainSpec" / "linked"
    link.parent.mkdir(parents=True)
    link.symlink_to(shared, target_is_directory=True)
    dst = root / "DomainSpec" / "linked.yaml"

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport(
        moved=[(link / "Domain.yaml", dst)], left_in_place=[link]
    )
    assert dst.read_text() == "domain"
    assert (shared / "Assets" / ".gitkeep").is_file()


def test_cli_migrate_catalog_dry_run_names_the_spec_cleanup(legacy_root: Path) -> None:
    """`--dry-run` names each spec move, the `.gitkeep` and `Assets/` removal,
    and the directory a real asset keeps; it touches nothing.
    """
    root = legacy_root.resolve()
    clean = root / "DomainSpec" / "clean-domain"
    snapshot = _tree(root)

    result = CliRunner().invoke(app, [str(root), "--dry-run"])

    out = result.stdout.replace("\n", "")
    assert result.exit_code == 0, result.output
    assert (
        f"[dry-run] moved {root / 'ModelSpec' / 'm' / 'model.yaml'} -> "
        f"{root / 'ModelSpec' / 'm.yaml'}"
    ) in out
    assert f"[dry-run] removed placeholder {clean / 'Assets' / '.gitkeep'}" in out
    assert f"[dry-run] removed empty directory {clean / 'Assets'}" in out
    assert f"[dry-run] removed empty directory {clean}" in out
    assert (
        "[dry-run] left in place (still holds other files): "
        f"{root / 'DomainSpec' / 'asset-domain'}"
    ) in out
    assert (
        "[dry-run] Summary: moved 11 entr(y/ies), skipped 0 collision(s), "
        "removed 1 placeholder(s) and 8 empty director(y/ies), "
        "left 2 director(y/ies) in place."
    ) in out
    assert _tree(root) == snapshot


def test_cli_migrate_catalog_dry_run_plans_the_earlier_name_directory(
    tmp_path: Path,
) -> None:
    """`--dry-run` names the planned move and directory removal, and touches nothing."""
    src = _write(tmp_path / "blueprints" / "roms_marbl" / "x.yaml", "x")
    snapshot = _tree(tmp_path)

    result = CliRunner().invoke(app, [str(tmp_path), "--dry-run"])

    out = result.stdout.replace("\n", "")
    assert result.exit_code == 0, result.output
    assert f"[dry-run] moved {src.resolve()} -> " in out
    assert str(tmp_path.resolve() / "blueprints" / "ucla_roms" / "x.yaml") in out
    assert f"[dry-run] removed empty directory {src.parent.resolve()}" in out
    assert "removed 0 placeholder(s) and 1 empty director(y/ies)" in out
    assert _tree(tmp_path) == snapshot


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
    _write(tmp_path / "blueprints" / "ucla_roms" / "dup.yaml", "current-dup")
    monkeypatch.setattr(
        DomainCatalog,
        "legacy_blueprints",
        property(lambda self: [("ucla_roms", "dup", src.resolve())]),
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
