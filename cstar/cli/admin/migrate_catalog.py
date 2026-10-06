import typing as t
from dataclasses import dataclass, field
from pathlib import Path

import typer
from rich.console import Console
from rich.markup import escape

from cstar.base.log import get_logger
from cstar.entrypoint.utils import ARG_DRY_RUN

log = get_logger(__name__)
app = typer.Typer()
console = Console()

short_help: t.Final[str] = (
    "Move a catalog's blueprints into the blueprints/<application>/ layout."
)
help: t.Final[str] = f"""\
{short_help}

A catalog keeps each blueprint at `blueprints/<application>/<name>.yaml`,
where the directory is the registered application name (`forge`,
`roms_marbl`, ...). Older catalogs used three other forms, which are still
read (with a warning) for one more release:

- `blueprints/<name>.forge_blueprint.yaml` (a forge blueprint),
- `blueprints/B_<name>.yaml` (a roms_marbl blueprint),
- `blueprints/<machine>/<name>/B_*.yaml` (a roms_marbl blueprint).

Given a catalog root, this command moves every blueprint found in one of
those forms to `blueprints/<application>/<name>.yaml` (always with a `.yaml`
extension). A blueprint whose destination already exists is left where it is
and reported, never overwritten. After a per-machine blueprint is moved, its
`<name>/` and `<machine>/` directories are removed if they are then empty;
anything else in them (a `Build/` directory, `_grid.yaml`, `settings_B_*.yaml`,
...) is left in place and reported.

The catalog is read with validation suppressed, so an otherwise incomplete
catalog can still be migrated. Pass `--dry-run` to see what would change
without touching anything on disk.
"""


@dataclass
class CatalogMigrationReport:
    """The result of planning (and, unless `dry_run`, performing) a migration
    of a catalog's blueprints to the `blueprints/<application>/<name>.yaml`
    layout.
    """

    moved: list[tuple[Path, Path]] = field(default_factory=list[tuple[Path, Path]])
    """Pairs of `(source, destination)` for every blueprint moved (or, in
    `dry_run` mode, that would be moved).
    """
    skipped_collisions: list[tuple[Path, Path]] = field(
        default_factory=list[tuple[Path, Path]]
    )
    """Pairs of `(source, destination)` for blueprints left in place because
    the destination already exists.
    """
    removed_dirs: list[Path] = field(default_factory=list[Path])
    """Now-empty legacy `<name>/` and `<machine>/` directories removed (or, in
    `dry_run` mode, that would be removed).
    """
    left_in_place: list[Path] = field(default_factory=list[Path])
    """Legacy directories that still hold other files after the move, and so
    were not removed.
    """


def _remaining(directory: Path, gone: set[Path]) -> list[Path]:
    """List the entries of `directory` that will still exist once `gone` is removed."""
    return [e for e in sorted(directory.iterdir()) if e not in gone]


def migrate_catalog_layout(
    root: Path, *, dry_run: bool = False
) -> CatalogMigrationReport:
    """Move a catalog's legacy-layout blueprints into `blueprints/<application>/`.

    Parameters
    ----------
    root : Path
        The catalog root (the directory holding `blueprints/`).
    dry_run : bool
        When `True`, compute and return the same report without making any
        changes on disk.

    Returns
    -------
    CatalogMigrationReport
    """
    # Imported here, as in migrate-outputs, so the catalog (which builds the
    # default catalog at import) is not loaded on every `cstar` invocation.
    from cstar.catalog.domain_catalog import DomainCatalog

    root = root.expanduser().resolve()
    report = CatalogMigrationReport()
    # Winners first, in the catalog's order: a legacy file that lost to another
    # for the same destination is then reported as a collision, not moved.
    legacy = DomainCatalog(root, suppress_validation=True).legacy_blueprints

    destinations: set[Path] = set()
    planned: set[Path] = set()
    for application, name, src in legacy:
        dst = root / "blueprints" / application / f"{name}.yaml"
        destinations.add(dst.parent)
        if dst in planned or dst.exists() or dst.is_symlink():
            report.skipped_collisions.append((src, dst))
        else:
            planned.add(dst)
            report.moved.append((src, dst))

    # Directories of the form `blueprints/<machine>/<name>/` (the file sits four
    # levels below the root) are cleaned up once their moved blueprint is gone;
    # a flat file sits three levels below it and has nothing to clean.
    gone = {src for src, _ in report.moved}
    name_dirs = sorted(
        {src.parent for src, _ in report.moved if len(src.relative_to(root).parts) == 4}
    )
    machine_dirs = sorted({d.parent for d in name_dirs})

    # A directory that is also a destination (a machine named like an
    # application) holds the migrated files now and must stay.
    kept: set[Path] = set()
    for directory in name_dirs:
        if directory in destinations:
            continue
        if _remaining(directory, gone):
            report.left_in_place.append(directory)
            kept.add(directory)
        else:
            report.removed_dirs.append(directory)
            gone.add(directory)
    for directory in machine_dirs:
        if directory in destinations:
            continue
        if _remaining(directory, gone | kept):
            report.left_in_place.append(directory)
        elif not any(d.parent == directory for d in kept):
            report.removed_dirs.append(directory)

    if not dry_run:
        for src, dst in report.moved:
            dst.parent.mkdir(parents=True, exist_ok=True)
            src.rename(dst)
        for directory in report.removed_dirs:
            directory.rmdir()

    return report


@app.command(
    name="migrate-catalog",
    help=help,
    short_help=short_help,
)
def migrate_catalog(
    path: t.Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
            help="The catalog root to migrate (the directory holding `blueprints/`).",
        ),
    ],
    dry_run: t.Annotated[
        bool,
        typer.Option(
            ARG_DRY_RUN,
            help="Report the changes that would be made without touching any files.",
        ),
    ] = False,
) -> None:
    """Move a catalog's blueprints into the blueprints/<application>/ layout."""
    report = migrate_catalog_layout(path, dry_run=dry_run)
    # Paths and the "[dry-run]" prefix are plain text: escape them so a
    # literal `[...]` is not parsed as a Rich style tag and silently dropped.
    prefix = f"{escape('[dry-run]')} " if dry_run else ""
    console.soft_wrap = True

    if not (report.moved or report.skipped_collisions):
        console.print(
            f"{prefix}Nothing to migrate: no legacy-layout blueprints under "
            f"{escape(str(path))}."
        )
        console.soft_wrap = False
        return

    for src, dst in report.moved:
        console.print(f"{prefix}moved {escape(str(src))} -> {escape(str(dst))}")
    for src, dst in report.skipped_collisions:
        console.print(
            f"{prefix}skipped (exists): {escape(str(src))} was left in place; "
            f"{escape(str(dst))} already exists"
        )
    for p in report.removed_dirs:
        console.print(f"{prefix}removed empty directory {escape(str(p))}")
    for p in report.left_in_place:
        console.print(
            f"{prefix}left in place (still holds other files): {escape(str(p))}"
        )

    console.print(
        f"{prefix}Summary: moved {len(report.moved)} blueprint(s), "
        f"skipped {len(report.skipped_collisions)} collision(s), "
        f"removed {len(report.removed_dirs)} empty director(y/ies), "
        f"left {len(report.left_in_place)} director(y/ies) in place."
    )
    console.soft_wrap = False


if __name__ == "__main__":
    app()
