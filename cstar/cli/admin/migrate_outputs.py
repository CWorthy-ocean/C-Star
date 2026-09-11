import fnmatch
import shutil
import typing as t
from dataclasses import dataclass, field
from pathlib import Path

import typer
from rich.console import Console

from cstar.base.log import get_logger
from cstar.entrypoint.utils import ARG_DRY_RUN
from cstar.execution.file_system import RomsFileSystemManager

log = get_logger(__name__)
app = typer.Typer()
console = Console()

short_help: t.Final[str] = (
    "Migrate an old-layout run or step directory to the current output layout."
)
help: t.Final[str] = f"""\
{short_help}

Runs produced before `joined_output` was retired kept final, whole files in
a per-step `joined_output` directory and left ROMS's partitioned pieces in
`output`. This command walks every `joined_output` directory found under the
given path (a run root or a single step directory; subtask directories
nested under `tasks/<slug>/` are included) and, for each one:

- if it holds only symlinks, it is an old run-level `cstar workplan gather`
  result and is removed outright -- re-run `cstar workplan gather <run-id>`
  to rebuild it under its new name (`gathered_output`);
- otherwise, every file in it is moved into the sibling `output` directory
  (a filename that already exists there is left in place and reported, never
  overwritten), and `joined_output` is removed once empty;
- any partitioned pieces now sitting in `output` (or already there) are then
  moved into a sibling `temp_output` directory.

Pass `--dry-run` to see what would change without touching anything on disk.
"""


@dataclass
class MigrationReport:
    """The result of planning (and, unless `dry_run`, performing) a migration
    of a run or step directory from the old `joined_output` layout to the
    current `output` / `temp_output` layout.
    """

    moved: list[tuple[Path, Path]] = field(default_factory=list)
    """Pairs of `(source, destination)` for every file moved (or, in
    `dry_run` mode, that would be moved).
    """
    skipped_collisions: list[Path] = field(default_factory=list)
    """Files left in place because a file with the same name already exists
    at the intended destination.
    """
    removed_dirs: list[Path] = field(default_factory=list)
    """Now-empty `joined_output` directories removed (or, in `dry_run` mode,
    that would be removed).
    """
    gather_dirs_removed: list[Path] = field(default_factory=list)
    """Old run-level `cstar workplan gather` result directories (holding only
    symlinks) removed (or, in `dry_run` mode, that would be removed).
    """


def _plan_and_apply_moves(
    entries: list[Path],
    dest_dir: Path,
    *,
    dry_run: bool,
) -> tuple[list[tuple[Path, Path]], list[Path]]:
    """Move (or, in `dry_run` mode, plan the move of) every entry into `dest_dir`.

    An entry whose name already exists in `dest_dir` is left in place rather
    than overwritten.

    Parameters
    ----------
    entries : list[Path]
        The source paths to move.
    dest_dir : Path
        The directory the entries should be moved into.
    dry_run : bool
        When `True`, no filesystem changes are made.

    Returns
    -------
    tuple[list[tuple[Path, Path]], list[Path]]
        The `(source, destination)` pairs that were (or would be) moved, and
        the source paths that were (or would be) skipped due to a collision.
    """
    moved: list[tuple[Path, Path]] = []
    skipped: list[Path] = []

    for entry in entries:
        dst = dest_dir / entry.name
        if dst.exists():
            skipped.append(entry)
            continue
        moved.append((entry, dst))

    if not dry_run and moved:
        dest_dir.mkdir(parents=True, exist_ok=True)
        for src, dst in moved:
            src.rename(dst)

    return moved, skipped


def _migrate_one(jo_dir: Path, *, dry_run: bool, report: MigrationReport) -> None:
    """Migrate a single `joined_output` directory into the current layout.

    Parameters
    ----------
    jo_dir : Path
        The `joined_output` directory to migrate.
    dry_run : bool
        When `True`, no filesystem changes are made.
    report : MigrationReport
        The report to accumulate results into.
    """
    entries = sorted(jo_dir.iterdir())

    if entries and all(e.is_symlink() for e in entries):
        # An old run-level `cstar workplan gather` result: nothing here is a
        # real file worth preserving, it must simply be rebuilt.
        report.gather_dirs_removed.append(jo_dir)
        if not dry_run:
            shutil.rmtree(jo_dir)
        return

    # `RomsFileSystemManager.__init__` resolves its root, so `out_dir` /
    # `temp_dir` stay consistent with `jo_dir` only because
    # `migrate_output_layout` already resolved `root` before `rglob`.
    fsm = RomsFileSystemManager(jo_dir.parent)
    out_dir = fsm.output_dir
    temp_dir = fsm.temp_output_dir

    moved, skipped = _plan_and_apply_moves(entries, out_dir, dry_run=dry_run)
    report.moved.extend(moved)
    report.skipped_collisions.extend(skipped)

    if not skipped:
        report.removed_dirs.append(jo_dir)
        if not dry_run:
            jo_dir.rmdir()

    # Partition pieces may already have been sitting directly in `out_dir`
    # (a non-PIO run's raw ROMS output), or may be entries just moved there
    # above; either way they belong in `temp_output`. Always key by the
    # planned `out_dir` path (`dst`), even in `dry_run` mode where nothing
    # has physically moved yet: `_plan_and_apply_moves` only reads
    # `entry.name` for its collision check, so it never dereferences this
    # path, and the dry-run report ends up identical to a real run's.
    candidates: dict[str, Path] = {}
    if out_dir.exists():
        for p in out_dir.iterdir():
            if p.is_file():
                candidates[p.name] = p
    for _src, dst in moved:
        candidates.setdefault(dst.name, dst)

    partition_entries = sorted(
        (
            p
            for name, p in candidates.items()
            if fnmatch.fnmatch(name, RomsFileSystemManager.PARTITIONED_OUTPUT_GLOB)
        ),
        key=lambda p: p.name,
    )
    part_moved, part_skipped = _plan_and_apply_moves(
        partition_entries, temp_dir, dry_run=dry_run
    )
    report.moved.extend(part_moved)
    report.skipped_collisions.extend(part_skipped)


def migrate_output_layout(root: Path, *, dry_run: bool = False) -> MigrationReport:
    """Migrate every `joined_output` directory under `root` to the current layout.

    Parameters
    ----------
    root : Path
        A run root or a single step directory. Every `joined_output`
        directory found under it (including under nested `tasks/<slug>/`
        subtask directories) is migrated.
    dry_run : bool
        When `True`, compute and return the same report without making any
        changes on disk.

    Returns
    -------
    MigrationReport
    """
    # Resolved once so every downstream path (including the ones
    # `RomsFileSystemManager` derives from `jo_dir.parent`, which resolves
    # its own root) is built from the same, already-resolved base as the
    # `jo_dir` paths `rglob` below produces.
    root = root.expanduser().resolve()
    report = MigrationReport()

    for jo_dir in sorted(root.rglob("joined_output")):
        if not jo_dir.is_dir():
            continue
        _migrate_one(jo_dir, dry_run=dry_run, report=report)

    return report


@app.command(
    name="migrate-outputs",
    help=help,
    short_help=short_help,
)
def migrate_outputs(
    path: t.Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
            help="A run root or a single step directory to migrate.",
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
    """Migrate an old-layout run or step directory to the current output layout."""
    report = migrate_output_layout(path, dry_run=dry_run)

    # `markup=False` everywhere below: paths and the "[dry-run]" prefix are
    # plain text, not Rich markup (a literal `[dry-run]` would otherwise be
    # parsed as an unknown style tag and silently dropped).
    prefix = "[dry-run] " if dry_run else ""

    for src, dst in report.moved:
        console.print(f"{prefix}moved {src} -> {dst}", soft_wrap=True, markup=False)
    for p in report.skipped_collisions:
        console.print(
            f"{prefix}skipped (exists): {p} was left in place",
            soft_wrap=True,
            markup=False,
        )
    for p in report.removed_dirs:
        console.print(
            f"{prefix}removed empty directory {p}", soft_wrap=True, markup=False
        )
    for p in report.gather_dirs_removed:
        console.print(
            f"{prefix}removed old run-level gather directory {p}; "
            "re-run `cstar workplan gather <run-id>` to rebuild it",
            soft_wrap=True,
            markup=False,
        )

    console.print(
        f"{prefix}Summary: moved {len(report.moved)} file(s), "
        f"skipped {len(report.skipped_collisions)} collision(s), "
        f"removed {len(report.removed_dirs)} empty `joined_output` "
        f"director(y/ies), removed {len(report.gather_dirs_removed)} old "
        "run-level gather director(y/ies).",
        soft_wrap=True,
        markup=False,
    )


if __name__ == "__main__":
    app()
