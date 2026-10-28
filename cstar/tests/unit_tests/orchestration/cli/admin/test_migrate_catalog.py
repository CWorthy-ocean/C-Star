from collections.abc import Callable, Generator
from pathlib import Path
from unittest.mock import patch

import pytest
import typer
import yaml
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
    """A catalog root holding all four legacy blueprint forms, a legacy
    per-entry spec directory of every kind and a spec file at the root.

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
    - a spec file at the root rather than under ``specs/``: ``ForcingSpec/flat-f.yaml``
    """
    root = tmp_path / "catalog"
    _write(root / "ModelSpec" / "m" / "model.yaml", "description: model-m\n")
    _write(
        root / "DomainSpec" / "clean-domain" / "Domain.yaml",
        "description: domain-clean\n",
    )
    _write(root / "DomainSpec" / "clean-domain" / "Assets" / ".gitkeep", "")
    _write(
        root / "DomainSpec" / "asset-domain" / "Domain.yaml",
        "description: domain-asset\n",
    )
    _write(root / "DomainSpec" / "asset-domain" / "Assets" / "bathy.nc", "asset")
    _write(root / "ForcingSpec" / "f" / "Forcing.yml", "description: forcing-f\n")
    _write(root / "ForcingSpec" / "flat-f.yaml", "description: forcing-flat\n")
    _write(root / "OutputSpec" / "o" / "Output.yaml", "description: output-o\n")
    _write(root / "CdrSpec" / "c" / "Cdr.yaml", "description: cdr-c\n")
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
    ``specs/<Kind>/<name>.yaml`` and leftovers pin their directory.
    """
    root = legacy_root.resolve()
    bp = root / "blueprints"
    specs = root / "specs"
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
            (root / "ModelSpec" / "m" / "model.yaml", specs / "ModelSpec" / "m.yaml"),
            (asset / "Domain.yaml", specs / "DomainSpec" / "asset-domain.yaml"),
            (clean / "Domain.yaml", specs / "DomainSpec" / "clean-domain.yaml"),
            (
                root / "ForcingSpec" / "f" / "Forcing.yml",
                specs / "ForcingSpec" / "f.yaml",
            ),
            (
                root / "ForcingSpec" / "flat-f.yaml",
                specs / "ForcingSpec" / "flat-f.yaml",
            ),
            (
                root / "OutputSpec" / "o" / "Output.yaml",
                specs / "OutputSpec" / "o.yaml",
            ),
            (root / "CdrSpec" / "c" / "Cdr.yaml", specs / "CdrSpec" / "c.yaml"),
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
        # the root `<Kind>/` directories once empty; `DomainSpec/` keeps the
        # asset's directory (which alone is reported)
        root / "CdrSpec",
        root / "ForcingSpec",
        root / "ModelSpec",
        root / "OutputSpec",
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

    # `specs/` holds one `<Kind>/<name>.yaml` per entry; at the root, only the
    # domain whose real asset keeps its directory is left
    assert _tree(specs) == {
        "CdrSpec": None,
        "CdrSpec/c.yaml": "description: cdr-c\n",
        "DomainSpec": None,
        "DomainSpec/asset-domain.yaml": "description: domain-asset\n",
        "DomainSpec/clean-domain.yaml": "description: domain-clean\n",
        "ForcingSpec": None,
        "ForcingSpec/f.yaml": "description: forcing-f\n",
        "ForcingSpec/flat-f.yaml": "description: forcing-flat\n",
        "ModelSpec": None,
        "ModelSpec/m.yaml": "description: model-m\n",
        "OutputSpec": None,
        "OutputSpec/o.yaml": "description: output-o\n",
    }
    assert sorted(p.name for p in root.iterdir()) == [
        "DomainSpec",
        "blueprints",
        "specs",
    ]
    assert _tree(root / "DomainSpec") == {
        "asset-domain": None,
        "asset-domain/Assets": None,
        "asset-domain/Assets/bathy.nc": "asset",
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
    assert reloaded.forcing_names == ["f", "flat-f"]
    assert reloaded.output_names == ["o"]
    assert reloaded.cdr_names == ["c"]
    assert reloaded.domain_path("clean-domain") == (
        root / "specs" / "DomainSpec" / "clean-domain.yaml"
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
    """A legacy spec whose name already has a file under `specs/` (`.yaml` or
    `.yml`, the one the catalog reads) is left in place, its directory
    untouched, and that file is named as the destination; nothing else changes.
    """
    root = tmp_path.resolve()
    model = _write(
        root / "ModelSpec" / "dup" / "model.yaml", "description: legacy-model\n"
    )
    model_flat = _write(root / "ModelSpec" / "dup.yaml", "description: flat-model\n")
    model_current = _write(
        root / "specs" / "ModelSpec" / "dup.yml", "description: current-model\n"
    )
    domain = _write(
        root / "DomainSpec" / "d" / "Domain.yaml", "description: legacy-domain\n"
    )
    _write(root / "DomainSpec" / "d" / "Assets" / ".gitkeep", "")
    domain_current = _write(
        root / "specs" / "DomainSpec" / "d.yaml", "description: current-domain\n"
    )
    snapshot = _tree(root)

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport(
        skipped_collisions=[
            (model_flat, model_current),
            (model, model_current),
            (domain, domain_current),
        ]
    )
    assert _tree(root) == snapshot


def test_migrate_catalog_layout_two_legacy_forms_of_one_spec(tmp_path: Path) -> None:
    """A spec with both a root `<Kind>/<name>.yaml` and a per-entry directory:
    the root file (the one the catalog reads) moves, the other is reported as a
    collision with it and kept, never moved over it; a dry run plans the same.
    """
    root = tmp_path.resolve()
    flat = _write(root / "ModelSpec" / "dup.yaml", "description: flat\n")
    nested = _write(root / "ModelSpec" / "dup" / "model.yaml", "description: dir\n")
    dst = root / "specs" / "ModelSpec" / "dup.yaml"
    snapshot = _tree(root)

    planned = migrate_catalog_layout(root, dry_run=True)

    assert planned == CatalogMigrationReport(
        moved=[(flat, dst)],
        skipped_collisions=[(nested, dst)],
        left_in_place=[root / "ModelSpec"],
    )
    assert _tree(root) == snapshot

    assert migrate_catalog_layout(root) == planned
    assert dst.read_text() == "description: flat\n"
    assert nested.read_text() == "description: dir\n"


def test_migrate_catalog_layout_leaves_a_schema_backup_at_the_root(
    tmp_path: Path,
) -> None:
    """A root `<Kind>/<name>.yaml` moves into `specs/`; a `.bak` beside it is not
    a spec, stays, and keeps the `<Kind>/` directory, reported as left in place.
    """
    root = tmp_path.resolve()
    flat = _write(root / "OutputSpec" / "o.yaml", "description: output\n")
    backup = _write(_bak(flat), "description: earlier\n")
    dst = root / "specs" / "OutputSpec" / "o.yaml"

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport(
        moved=[(flat, dst)], left_in_place=[root / "OutputSpec"]
    )
    assert dst.read_text() == "description: output\n"
    assert backup.read_text() == "description: earlier\n"
    assert migrate_catalog_layout(root) == CatalogMigrationReport()


def test_migrate_catalog_layout_keeps_a_symlinked_spec_directory(
    tmp_path: Path,
) -> None:
    """A spec directory that is a symbolic link is left in place and nothing
    inside its (possibly shared) target is cleaned up.
    """
    root = (tmp_path / "catalog").resolve()
    shared = _write(tmp_path / "shared" / "Domain.yaml", "description: domain\n").parent
    _write(shared / "Assets" / ".gitkeep", "")
    link = root / "DomainSpec" / "linked"
    link.parent.mkdir(parents=True)
    link.symlink_to(shared, target_is_directory=True)
    dst = root / "specs" / "DomainSpec" / "linked.yaml"

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport(
        moved=[(link / "Domain.yaml", dst)], left_in_place=[link]
    )
    assert dst.read_text() == "description: domain\n"
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
        f"{root / 'specs' / 'ModelSpec' / 'm.yaml'}"
    ) in out
    assert f"[dry-run] removed empty directory {root / 'ModelSpec'}" in out
    assert f"[dry-run] removed placeholder {clean / 'Assets' / '.gitkeep'}" in out
    assert f"[dry-run] removed empty directory {clean / 'Assets'}" in out
    assert f"[dry-run] removed empty directory {clean}" in out
    assert (
        "[dry-run] left in place (still holds other files): "
        f"{root / 'DomainSpec' / 'asset-domain'}"
    ) in out
    assert (
        "[dry-run] Summary: moved 12 entr(y/ies), skipped 0 collision(s), "
        "removed 1 placeholder(s) and 12 empty director(y/ies), "
        "left 2 director(y/ies) in place; would rewrite 0 spec file(s), "
        "left 0 newer, 0 failed and 0 with an existing backup."
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


OUTDATED_FORCING = """\
# a comment the rewrite drops
description: old forcing
initial_conditions:
  bgc_source:
    name: GLORYS
"""
"""A ForcingSpec at schema 1.0.0 (no `schema_version`): `bgc_source` became `bgc_sources`."""

CURRENT_FORCING = """\
# a comment a current file keeps
schema_version: 2.0.0
description: current forcing
"""


def _bak(path: Path) -> Path:
    return path.with_name(path.name + ".bak")


def test_outdated_forcing_spec_is_rewritten_with_backup(tmp_path: Path) -> None:
    """A spec a migration changes is rewritten at the current version and its
    previous content is kept byte for byte as `<file>.bak`.
    """
    root = tmp_path.resolve()
    spec = _write(root / "specs" / "ForcingSpec" / "f.yaml", OUTDATED_FORCING)

    report = migrate_catalog_layout(root)

    assert report.rewritten == [(spec, "ForcingSpec", "1.0.0", "2.0.0")]
    assert report.newer_specs == report.failed_specs == report.backup_exists == []
    new = yaml.safe_load(spec.read_text())
    assert new["schema_version"] == "2.0.0"
    assert "bgc_sources" in new["initial_conditions"]
    assert "bgc_source" not in new["initial_conditions"]
    assert _bak(spec).read_text() == OUTDATED_FORCING
    assert [p.name for p in root.rglob(".*")] == []


def test_current_specs_are_left_byte_identical(tmp_path: Path) -> None:
    """A spec at the current version, and an unversioned one already in the
    current shape, are not rewritten (so their comments survive) and get no `.bak`.
    """
    root = tmp_path.resolve()
    unversioned = "# unversioned but current\ndescription: plain\n"
    _write(root / "specs" / "ForcingSpec" / "versioned.yaml", CURRENT_FORCING)
    _write(root / "specs" / "ForcingSpec" / "unversioned.yaml", unversioned)
    _write(root / "specs" / "DomainSpec" / "d.yaml", unversioned)
    snapshot = _tree(root)

    report = migrate_catalog_layout(root)

    assert report == CatalogMigrationReport()
    assert _tree(root) == snapshot


def test_outdated_output_spec_is_rewritten(tmp_path: Path) -> None:
    """An OutputSpec with `cdr_tracer_output` is rewritten to `cdr_lite_output`."""
    root = tmp_path.resolve()
    original = "cdr_tracer_output:\n  output_period: 3600\n"
    spec = _write(root / "specs" / "OutputSpec" / "o.yaml", original)

    report = migrate_catalog_layout(root)

    assert report.rewritten == [(spec, "OutputSpec", "1.0.0", "2.0.0")]
    new = yaml.safe_load(spec.read_text())
    assert new["schema_version"] == "2.0.0"
    assert "cdr_lite_output" in new
    assert "cdr_tracer_output" not in new
    assert _bak(spec).read_text() == original


def test_newer_major_spec_is_untouched_and_reported(tmp_path: Path) -> None:
    """A spec of a newer major is reported, never changed, and does not stop
    the others from being migrated.
    """
    root = tmp_path.resolve()
    original = "schema_version: 9.0.0\ndescription: from the future\n"
    newer = _write(root / "specs" / "ForcingSpec" / "newer.yaml", original)
    old = _write(root / "specs" / "ForcingSpec" / "old.yaml", OUTDATED_FORCING)

    report = migrate_catalog_layout(root)

    assert [p for p, _ in report.newer_specs] == [newer]
    assert newer.read_text() == original
    assert not _bak(newer).exists()
    assert [p for p, *_ in report.rewritten] == [old]


def test_unreadable_spec_is_reported_and_others_carry_on(tmp_path: Path) -> None:
    """A spec that cannot be parsed is reported and skipped; the rest are migrated."""
    root = tmp_path.resolve()
    broken = _write(root / "specs" / "OutputSpec" / "broken.yaml", "a: [unclosed\n")
    listy = _write(root / "specs" / "OutputSpec" / "listy.yaml", "- not\n- a mapping\n")
    old = _write(root / "specs" / "ForcingSpec" / "old.yaml", OUTDATED_FORCING)

    report = migrate_catalog_layout(root)

    assert sorted(p for p, _ in report.failed_specs) == sorted([broken, listy])
    assert broken.read_text() == "a: [unclosed\n"
    assert not _bak(broken).exists()
    assert [p for p, *_ in report.rewritten] == [old]


def test_dry_run_reports_the_rewrite_and_writes_nothing(tmp_path: Path) -> None:
    """`dry_run` names the rewrite but leaves the file alone and makes no `.bak`."""
    root = tmp_path.resolve()
    spec = _write(root / "specs" / "ForcingSpec" / "f.yaml", OUTDATED_FORCING)
    snapshot = _tree(root)

    report = migrate_catalog_layout(root, dry_run=True)

    assert report.rewritten == [(spec, "ForcingSpec", "1.0.0", "2.0.0")]
    assert _tree(root) == snapshot


def test_existing_backup_skips_the_file(tmp_path: Path) -> None:
    """A `.bak` is never overwritten: the spec is left as it is and reported."""
    root = tmp_path.resolve()
    spec = _write(root / "specs" / "ForcingSpec" / "f.yaml", OUTDATED_FORCING)
    _write(_bak(spec), "an earlier backup\n")
    snapshot = _tree(root)

    report = migrate_catalog_layout(root)

    assert report.backup_exists == [spec]
    assert report.rewritten == []
    assert _tree(root) == snapshot


def test_failed_write_leaves_neither_backup_nor_temp_file(tmp_path: Path) -> None:
    """A failure while writing is reported; the file is as it was, with no `.bak`
    and no temporary file left behind.
    """
    root = tmp_path.resolve()
    spec = _write(root / "specs" / "ForcingSpec" / "f.yaml", OUTDATED_FORCING)
    snapshot = _tree(root)

    with patch("cstar.cli.admin.migrate_catalog.os.replace", side_effect=OSError("no")):
        report = migrate_catalog_layout(root)

    assert report.rewritten == []
    assert report.failed_specs == [(spec, "no")]
    assert _tree(root) == snapshot


def test_legacy_layout_spec_is_moved_and_rewritten(tmp_path: Path) -> None:
    """One non-dry run moves a legacy-layout spec and rewrites it at its new place."""
    root = tmp_path.resolve()
    legacy = _write(root / "ForcingSpec" / "f" / "Forcing.yaml", OUTDATED_FORCING)
    dst = root / "specs" / "ForcingSpec" / "f.yaml"

    report = migrate_catalog_layout(root)

    assert report.moved == [(legacy, dst)]
    assert report.rewritten == [(dst, "ForcingSpec", "1.0.0", "2.0.0")]
    assert yaml.safe_load(dst.read_text())["schema_version"] == "2.0.0"
    assert _bak(dst).read_text() == OUTDATED_FORCING
    assert not legacy.parent.exists()


def test_legacy_layout_spec_dry_run_reports_where_the_run_rewrites(
    tmp_path: Path,
) -> None:
    """A dry run reports a legacy-layout spec's rewrite at the place the real run
    rewrites it: where the move puts it.
    """
    root = tmp_path.resolve()
    _write(root / "ForcingSpec" / "f" / "Forcing.yaml", OUTDATED_FORCING)
    snapshot = _tree(root)

    report = migrate_catalog_layout(root, dry_run=True)

    dst = root / "specs" / "ForcingSpec" / "f.yaml"
    assert report.rewritten == [(dst, "ForcingSpec", "1.0.0", "2.0.0")]
    assert _tree(root) == snapshot


def test_rewrite_keeps_the_file_mode(tmp_path: Path) -> None:
    """A rewritten spec keeps its permissions, so a shared catalog stays readable."""
    root = tmp_path.resolve()
    spec = _write(root / "specs" / "ForcingSpec" / "f.yaml", OUTDATED_FORCING)
    spec.chmod(0o664)

    migrate_catalog_layout(root)

    assert spec.stat().st_mode & 0o777 == 0o664


def test_symlinked_spec_is_reported_and_left_alone(tmp_path: Path) -> None:
    """An outdated spec that is a symbolic link is reported, not replaced by a file."""
    root = tmp_path.resolve()
    target = _write(tmp_path / "shared" / "f.yaml", OUTDATED_FORCING)
    link = root / "specs" / "ForcingSpec" / "f.yaml"
    link.parent.mkdir(parents=True)
    link.symlink_to(target)

    report = migrate_catalog_layout(root)

    assert [p for p, _ in report.failed_specs] == [link]
    assert link.is_symlink()
    assert target.read_text() == OUTDATED_FORCING


def test_multi_model_spec_is_reported_with_how_to_migrate_it(tmp_path: Path) -> None:
    """A multi-model ModelSpec file is not rewritten; the report says to split it."""
    root = tmp_path.resolve()
    original = "a:\n  code: {}\nb:\n  code: {}\n"
    spec = _write(root / "specs" / "ModelSpec" / "m.yaml", original)

    report = migrate_catalog_layout(root)

    assert len(report.failed_specs) == 1
    assert report.failed_specs[0][0] == spec
    assert "split it" in report.failed_specs[0][1]
    assert spec.read_text() == original


def test_cli_migrate_catalog_prints_the_rewrite_and_summary(tmp_path: Path) -> None:
    """The command prints one line per rewritten file and counts them in the summary."""
    root = tmp_path.resolve()
    spec = _write(root / "specs" / "ForcingSpec" / "f.yaml", OUTDATED_FORCING)
    _write(root / "specs" / "OutputSpec" / "n.yaml", "schema_version: 9.0.0\n")

    result = CliRunner().invoke(app, [str(root)])

    out = result.stdout.replace("\n", "")
    assert result.exit_code == 0, result.output
    assert (f"rewrote {spec} (ForcingSpec 1.0.0 -> 2.0.0), backup {_bak(spec)}") in out
    assert "left (written by a newer cstar-ocean)" in out
    assert (
        "rewrote 1 spec file(s), left 1 newer, 0 failed and 0 with an existing" in out
    )
    assert "Nothing to migrate" not in out


def test_cli_migrate_catalog_dry_run_prints_would_rewrite(tmp_path: Path) -> None:
    """`--dry-run` says what would be rewritten, with the prefix, and writes nothing."""
    root = tmp_path.resolve()
    spec = _write(root / "specs" / "ForcingSpec" / "f.yaml", OUTDATED_FORCING)
    snapshot = _tree(root)

    result = CliRunner().invoke(app, [str(root), "--dry-run"])

    out = result.stdout.replace("\n", "")
    assert result.exit_code == 0, result.output
    assert f"[dry-run] would rewrite {spec} (ForcingSpec 1.0.0 -> 2.0.0)" in out
    assert _tree(root) == snapshot
