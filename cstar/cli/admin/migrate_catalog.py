import os
import shutil
import tempfile
import typing as t
from dataclasses import dataclass, field
from pathlib import Path

import typer
import yaml
from rich.console import Console
from rich.markup import escape

from cstar.base.application_names import APPLICATION_ALIASES, canonical_application
from cstar.base.log import get_logger
from cstar.entrypoint.utils import ARG_DRY_RUN
from cstar.system.migration import KEY_SV, CstarSchemaTooNewError

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from cstar.catalog.domain_catalog import DomainCatalog

log = get_logger(__name__)
app = typer.Typer()
console = Console()

short_help: t.Final[str] = (
    "Move a catalog's blueprints and specs into the current layout and schema versions."
)
_EARLIER_NAME_FORMS: t.Final[str] = "\n".join(
    f"- `blueprints/{alias}/<name>.yaml` (a {current} blueprint under the "
    "application's earlier name)."
    for alias, current in APPLICATION_ALIASES.items()
)
help: t.Final[str] = f"""\
{short_help}

A catalog keeps each blueprint at `blueprints/<application>/<name>.yaml`,
where the directory is the registered application name (`forge`,
`ucla_roms`, ...), and each spec at `specs/<Kind>/<name>.yaml`, where the
directory is the spec kind (`ModelSpec`, `DomainSpec`, `ForcingSpec`,
`OutputSpec`, `CdrSpec`). Older catalogs used other forms, which are still
read (with a warning) until C-Star 0.20.0:

- `blueprints/<name>.forge_blueprint.yaml` (a forge blueprint),
- `blueprints/B_<name>.yaml` (a ucla_roms blueprint),
- `blueprints/<machine>/<name>/B_*.yaml` (a ucla_roms blueprint),
{_EARLIER_NAME_FORMS}
- `<Kind>/<name>/<File>.yaml` (a spec in a directory of its own at the
  catalog root, such as `ModelSpec/<name>/model.yaml` or
  `DomainSpec/<name>/Domain.yaml`),
- `<Kind>/<name>.yaml` (a spec at the catalog root rather than under
  `specs/`).

Given a catalog root, this command moves every entry found in one of those
forms to `blueprints/<application>/<name>.yaml` or
`specs/<Kind>/<name>.yaml` (always with a `.yaml` extension). An entry whose
destination already exists, or that another entry of the same name is moving
to, is left where it is and reported, never overwritten. After a per-machine
blueprint is moved, its `<name>/` and `<machine>/` directories are removed if
they are then empty; so is an application's earlier-name directory. After a
spec is moved, its `Assets/` directory is removed if it holds nothing but a
`.gitkeep` placeholder, then its `<name>/` directory and the `<Kind>/`
directory at the catalog root if they are empty. Anything else in them (a
`Build/` directory, `_grid.yaml`, `settings_B_*.yaml`, an asset file, a
`.bak` backup, ...) is left in place and reported, as is a directory that is
a symbolic link.

The command also brings spec files up to their kind's current
`schema_version`. A spec written for an older schema is read as migrated (with
a warning), but its file is only changed here: it is rewritten with the
current content and a `schema_version`, and its previous content is kept as
`<file>.bak`. The rewrite drops YAML comments; the `.bak` keeps them. A spec
that is already current, even if it has no `schema_version`, is not
touched, nor is one written by a newer cstar-ocean (it is reported). A file
that cannot be migrated is reported and left as it is, as is one whose
`.bak` already exists, which is never overwritten.

The catalog is read with validation suppressed, so an otherwise incomplete
catalog can still be migrated. Pass `--dry-run` to see what would change
without touching anything on disk.
"""


@dataclass
class CatalogMigrationReport:
    """The result of planning (and, unless `dry_run`, performing) a migration
    of a catalog's entries to the current layout: blueprints at
    `blueprints/<application>/<name>.yaml`, specs at `specs/<Kind>/<name>.yaml`.
    """

    moved: list[tuple[Path, Path]] = field(default_factory=list[tuple[Path, Path]])
    """Pairs of `(source, destination)` for every entry moved (or, in
    `dry_run` mode, that would be moved).
    """
    skipped_collisions: list[tuple[Path, Path]] = field(
        default_factory=list[tuple[Path, Path]]
    )
    """Pairs of `(source, destination)` for entries left in place because
    the destination already exists.
    """
    removed_files: list[Path] = field(default_factory=list[Path])
    """`.gitkeep` placeholders removed from a moved spec's `Assets/` directory
    (or, in `dry_run` mode, that would be removed).
    """
    removed_dirs: list[Path] = field(default_factory=list[Path])
    """Now-empty legacy directories removed (or, in `dry_run` mode, that would
    be removed): a blueprint's `<name>/`, `<machine>/` and
    earlier-application-name directories, a spec's `Assets/` and `<name>/`,
    and a `<Kind>/` at the catalog root.
    """
    left_in_place: list[Path] = field(default_factory=list[Path])
    """Legacy directories that still hold other files after the move, or are
    symbolic links, and so were not removed.
    """
    rewritten: list[tuple[Path, str, str, str]] = field(
        default_factory=list[tuple[Path, str, str, str]]
    )
    """Tuples of `(file, kind, found, target)` for every spec file rewritten at
    its kind's current schema version (or, in `dry_run` mode, that would be),
    its previous content kept as `<file>.bak`.
    """
    newer_specs: list[tuple[Path, str]] = field(default_factory=list[tuple[Path, str]])
    """Pairs of `(file, reason)` for spec files written by a newer cstar-ocean,
    which are never changed.
    """
    failed_specs: list[tuple[Path, str]] = field(default_factory=list[tuple[Path, str]])
    """Pairs of `(file, reason)` for spec files that could not be read, migrated
    or written, and were left as they were.
    """
    backup_exists: list[Path] = field(default_factory=list[Path])
    """Spec files that need a rewrite but were left as they were because their
    `<file>.bak` already exists, which is never overwritten.
    """


def _remaining(directory: Path, gone: set[Path]) -> list[Path]:
    """List the entries of `directory` that will still exist once `gone` is removed."""
    return [e for e in sorted(directory.iterdir()) if e not in gone]


def _kept(directory: Path, gone: set[Path]) -> bool:
    """Whether `directory` stays once `gone` is removed: it still holds other
    entries, or it is a symbolic link, which `rmdir` cannot remove and whose
    target may be shared.
    """
    return directory.is_symlink() or bool(_remaining(directory, gone))


def migrate_catalog_layout(
    root: Path, *, dry_run: bool = False
) -> CatalogMigrationReport:
    """Move a catalog's legacy-layout blueprints and specs into the current layout.

    Parameters
    ----------
    root : Path
        The catalog root (the directory holding `blueprints/` and `specs/`).
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
    catalog = DomainCatalog(root, suppress_validation=True)
    # Winners first, in the catalog's order: a legacy file that lost to another
    # for the same destination is then reported as a collision, not moved.
    legacy = catalog.legacy_blueprints

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
    # levels below the root) are cleaned up once their moved blueprint is gone,
    # as is a `blueprints/<earlier name>/` (three levels); a flat file sits two
    # levels below the root and has nothing to clean.
    gone = {src for src, _ in report.moved}
    name_dirs = sorted(
        {src.parent for src, _ in report.moved if len(src.relative_to(root).parts) == 4}
    )
    machine_dirs = sorted({d.parent for d in name_dirs})
    earlier_dirs = sorted(
        {
            src.parent
            for src, _ in report.moved
            if len(src.relative_to(root).parts) == 3
            and canonical_application(src.parent.name) != src.parent.name
        }
    )

    # A directory that is also a destination (a machine named like an
    # application) holds the migrated files now and must stay.
    kept: set[Path] = set()
    for directory in name_dirs:
        if directory in destinations:
            continue
        if _kept(directory, gone):
            report.left_in_place.append(directory)
            kept.add(directory)
        else:
            report.removed_dirs.append(directory)
            gone.add(directory)
    for directory in machine_dirs:
        if directory in destinations:
            continue
        if _kept(directory, gone | kept):
            report.left_in_place.append(directory)
        elif not any(d.parent == directory for d in kept):
            report.removed_dirs.append(directory)
    for directory in earlier_dirs:
        if _kept(directory, gone):
            report.left_in_place.append(directory)
        else:
            report.removed_dirs.append(directory)

    _plan_spec_moves(root, catalog.specs_dir, catalog.legacy_specs, report)

    if not dry_run:
        for src, dst in report.moved:
            dst.parent.mkdir(parents=True, exist_ok=True)
            src.rename(dst)
        for path in report.removed_files:
            path.unlink()
        for directory in report.removed_dirs:
            directory.rmdir()
        # The moved files now sit at their new places: rescan to find them.
        catalog = DomainCatalog(root, suppress_validation=True)

    _migrate_spec_schemas(catalog, report, dry_run=dry_run)
    return report


def _rewrite_spec(path: Path, content: dict[str, t.Any], target: str) -> None:
    """Replace `path` with `content` at schema `target`, keeping its previous
    content as `<path>.bak`.

    The new text goes to a temporary file in the same directory first and is
    moved over `path` last, so a failure leaves `path` as it was (and no
    `.bak` behind).
    """
    backup = path.with_name(path.name + ".bak")
    document = {KEY_SV: target, **content}
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    tmp = Path(tmp_name)
    made_backup = False
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            yaml.safe_dump(
                document,
                handle,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
        shutil.copymode(path, tmp)
        shutil.copy2(path, backup)
        made_backup = True
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        if made_backup:
            backup.unlink(missing_ok=True)
        raise


def _migrate_spec_schemas(
    catalog: "DomainCatalog", report: CatalogMigrationReport, *, dry_run: bool
) -> None:
    """Rewrite each spec file that a schema migration changes, adding the
    outcome of every file that needs attention to `report`.

    A file that is current is not touched. One failing file is reported and
    skipped; the others carry on. A symbolic link is left alone, as its target
    may be shared, and so is a multi-model ModelSpec file. In `dry_run` mode a
    legacy-layout file is reported at the place the moves would give it.
    """
    # Imported here: the schema module reaches the application registry.
    from cstar.catalog.spec_schema import SPEC_SCHEMA_VERSIONS, migrate_spec

    moved_to = dict(report.moved) if dry_run else {}
    kinds = t.cast("Iterable[t.Any]", SPEC_SCHEMA_VERSIONS)
    for kind in kinds:
        for source in catalog.spec_paths(kind).values():
            path = moved_to.get(source, source)
            data: t.Any = None
            try:
                data = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
                spec = migrate_spec(kind, data, path)
            except CstarSchemaTooNewError as ex:
                report.newer_specs.append((path, str(ex)))
                continue
            except Exception as ex:
                # a multi-model file fails the ModelSpec key check on its model names
                multi_model = (
                    kind == "ModelSpec"
                    and isinstance(data, dict)
                    and not {"code", "model_settings"}.intersection(data)
                )
                reason = (
                    "a multi-model ModelSpec file; split it into one <name>.yaml "
                    "per model to migrate it"
                    if multi_model
                    else str(ex)
                )
                report.failed_specs.append((path, reason))
                continue
            if not spec.changed:
                continue
            if source.is_symlink():
                report.failed_specs.append(
                    (path, "a symbolic link; update the file it points to")
                )
                continue
            if path.with_name(path.name + ".bak").exists():
                report.backup_exists.append(path)
                continue
            if not dry_run:
                try:
                    _rewrite_spec(path, spec.content, spec.target)
                except OSError as ex:
                    report.failed_specs.append((path, str(ex)))
                    continue
            report.rewritten.append((path, kind, spec.found, spec.target))


def _plan_spec_moves(
    root: Path,
    specs_dir: Path,
    legacy: "Iterable[tuple[str, str, Path]]",
    report: CatalogMigrationReport,
) -> None:
    """Add to `report` the moves of the legacy specs at the catalog `root`
    (`<Kind>/<name>.yaml` and `<Kind>/<name>/<File>.yaml`) to
    `specs_dir/<Kind>/<name>.yaml` and the cleanup of their directories.

    A spec whose name already has a `specs_dir/<Kind>/<name>.yaml` or `.yml`
    (the file the catalog reads), or whose destination an earlier entry in
    `legacy` takes, is a collision. Each moved per-entry spec's `Assets/` goes
    when it holds nothing but a `.gitkeep`, then its `<name>/` when nothing is
    left; last, each `<Kind>/` a spec moved out of goes when nothing is left.
    """
    planned: set[Path] = set()
    gone: set[Path] = set()
    kept: set[Path] = set()
    kind_dirs: set[Path] = set()
    for kind, name, src in legacy:
        dst = specs_dir / kind / f"{name}.yaml"
        taken = [
            p for p in (dst, dst.with_suffix(".yml")) if p.exists() or p.is_symlink()
        ]
        if dst in planned:
            taken.append(dst)
        if taken:
            report.skipped_collisions.append((src, taken[0]))
            continue
        planned.add(dst)
        report.moved.append((src, dst))
        gone.add(src)
        kind_dir = root / kind
        kind_dirs.add(kind_dir)
        directory = src.parent
        if directory == kind_dir:  # a `<Kind>/<name>.yaml`: no directory of its own
            continue
        assets = directory / "Assets"
        # Nothing inside a symbolic link is cleaned up: its target may be shared.
        if not directory.is_symlink() and assets.is_dir() and not assets.is_symlink():
            placeholders = sorted(assets.iterdir())
            if all(p.name == ".gitkeep" and p.is_file() for p in placeholders):
                report.removed_files.extend(placeholders)
                report.removed_dirs.append(assets)
                gone.add(assets)
        if _kept(directory, gone):
            report.left_in_place.append(directory)
            kept.add(directory)
        else:
            report.removed_dirs.append(directory)
            gone.add(directory)
    # A `<Kind>/` holding nothing but a `<name>/` left in place is not
    # reported again.
    for kind_dir in sorted(kind_dirs):
        if _kept(kind_dir, gone | kept):
            report.left_in_place.append(kind_dir)
        elif not any(d.parent == kind_dir for d in kept):
            report.removed_dirs.append(kind_dir)


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
            help=(
                "The catalog root to migrate (the directory holding `blueprints/` "
                "and `specs/`)."
            ),
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
    """Move a catalog's blueprints and specs into the current layout and schema versions."""
    report = migrate_catalog_layout(path, dry_run=dry_run)
    # Paths and the "[dry-run]" prefix are plain text: escape them so a
    # literal `[...]` is not parsed as a Rich style tag and silently dropped.
    prefix = f"{escape('[dry-run]')} " if dry_run else ""
    console.soft_wrap = True

    if not (
        report.moved
        or report.skipped_collisions
        or report.rewritten
        or report.newer_specs
        or report.failed_specs
        or report.backup_exists
    ):
        console.print(
            f"{prefix}Nothing to migrate: no legacy-layout blueprints or specs and "
            f"no outdated spec files under {escape(str(path))}."
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
    for p in report.removed_files:
        console.print(f"{prefix}removed placeholder {escape(str(p))}")
    for p in report.removed_dirs:
        console.print(f"{prefix}removed empty directory {escape(str(p))}")
    for p in report.left_in_place:
        console.print(
            f"{prefix}left in place (still holds other files): {escape(str(p))}"
        )

    for file, kind, found, target in report.rewritten:
        bak = file.with_name(file.name + ".bak")
        verb = "would rewrite" if dry_run else "rewrote"
        console.print(
            f"{prefix}{verb} {escape(str(file))} ({kind} {found} -> {target}), "
            f"backup {escape(str(bak))}"
        )
    for file, reason in report.newer_specs:
        console.print(
            f"{prefix}left (written by a newer cstar-ocean): {escape(str(file))}: "
            f"{escape(reason)}"
        )
    for file, reason in report.failed_specs:
        console.print(
            f"{prefix}failed (left as is): {escape(str(file))}: {escape(reason)}"
        )
    for file in report.backup_exists:
        console.print(
            f"{prefix}skipped (backup exists): {escape(str(file))} was left as is; "
            f"{escape(str(file.with_name(file.name + '.bak')))} already exists"
        )

    console.print(
        f"{prefix}Summary: moved {len(report.moved)} entr(y/ies), "
        f"skipped {len(report.skipped_collisions)} collision(s), "
        f"removed {len(report.removed_files)} placeholder(s) and "
        f"{len(report.removed_dirs)} empty director(y/ies), "
        f"left {len(report.left_in_place)} director(y/ies) in place; "
        f"{'would rewrite' if dry_run else 'rewrote'} {len(report.rewritten)} "
        f"spec file(s), left {len(report.newer_specs)} newer, "
        f"{len(report.failed_specs)} failed and "
        f"{len(report.backup_exists)} with an existing backup."
    )
    console.soft_wrap = False


if __name__ == "__main__":
    app()
