import os
import shutil
from collections import defaultdict
from pathlib import Path

import typer

from cstar.base.log import get_logger
from cstar.cli.common import get_from_ctxmap
from cstar.cli.workplan.shared import RunIdArgument, console
from cstar.execution.file_system import RomsFileSystemManager, StateDirectoryManager
from cstar.orchestration.orchestration import LiveWorkplan

log = get_logger(__name__)
app = typer.Typer()

HELP_SHORT = "Consolidate per-step joined output into a single directory."
HELP_LONG = f"""\
{HELP_SHORT}

Every step's `joined_output` directory is scanned and a run-level
`joined_output` directory of symlinks is (re)built pointing at whatever files
currently exist. Safe to re-run at any time, including while the workplan is
still in progress: each invocation replaces the existing symlinks with a
fresh set reflecting the current state on disk.
"""


def collect_links(
    workplan: LiveWorkplan,
) -> tuple[dict[str, Path], dict[str, list[Path]]]:
    """Collect the joined-output files produced by every step in a workplan.

    Parameters
    ----------
    workplan : LiveWorkplan
        The workplan whose steps should be scanned for joined-output files.

    Returns
    -------
    tuple[dict[str, Path], dict[str, list[Path]]]
        A mapping of filename to source path for filenames that appear in
        exactly one step's `joined_output` directory, and a mapping of
        filename to the list of source paths for filenames that appear in
        more than one step's `joined_output` directory.
    """
    by_name: dict[str, list[Path]] = defaultdict(list)
    skipped: list[str] = []

    for step in workplan.steps:
        src_dir = RomsFileSystemManager(step.working_dir).joined_output_dir
        if not src_dir.exists():
            skipped.append(step.name)
            continue

        for p in sorted(src_dir.iterdir()):
            if p.is_file():
                by_name[p.name].append(p)

    if skipped:
        log.debug(
            "Skipping %d step(s) with no joined_output directory: %s",
            len(skipped),
            ", ".join(skipped),
        )

    links = {name: paths[0] for name, paths in by_name.items() if len(paths) == 1}
    conflicts = {name: paths for name, paths in by_name.items() if len(paths) > 1}

    return links, conflicts


def report_conflicts(conflicts: dict[str, list[Path]]) -> None:
    """Print a single error message describing every filename collision.

    Parameters
    ----------
    conflicts : dict[str, list[Path]]
        A mapping of filename to the list of source paths producing it.
    """
    lines = [
        f"  - {name!r}: {', '.join(str(p) for p in paths)}"
        for name, paths in sorted(conflicts.items())
    ]
    console.print(
        "Refusing to gather: the following filenames are produced by more than "
        "one step's joined_output directory:\n" + "\n".join(lines),
        soft_wrap=True,
    )


def relink(dest: Path, links: dict[str, Path]) -> None:
    """Replace a consolidated joined-output directory with a fresh set of symlinks.

    The existing directory is removed and recreated. If `dest` contains any
    entries that are not symlinks, nothing is modified and a `typer.Exit` is
    raised, since gather must never delete real files from the consolidated
    directory.

    Parameters
    ----------
    dest : Path
        The consolidated `joined_output` directory to (re)populate.
    links : dict[str, Path]
        A mapping of filename to the source path it should be linked to.

    Raises
    ------
    typer.Exit
        If `dest` exists but is not a directory, or contains entries that
        are not symlinks.
    """
    if dest.is_symlink() or (dest.exists() and not dest.is_dir()):
        console.print(
            f"Refusing to gather: the consolidated joined_output path "
            f"{str(dest)!r} exists but is not a directory.",
            soft_wrap=True,
        )
        raise typer.Exit(1)

    if dest.exists():
        non_symlinks = sorted(p for p in dest.iterdir() if not p.is_symlink())
        if non_symlinks:
            names = "\n".join(f"  - {p}" for p in non_symlinks)
            console.print(
                "Refusing to gather: the consolidated joined_output directory "
                f"{str(dest)!r} contains file(s) that are not symlinks created "
                f"by a prior gather, and will not be deleted:\n{names}",
                soft_wrap=True,
            )
            raise typer.Exit(1)

        shutil.rmtree(dest)

    dest.mkdir(parents=True)

    for name, target in links.items():
        (dest / name).symlink_to(Path(os.path.relpath(target, dest)))


@app.command(name="gather", help=HELP_LONG, short_help=HELP_SHORT)
def gather(
    context: typer.Context,
    run_id: RunIdArgument,
) -> None:
    """Consolidate per-step joined output into a run-level directory of symlinks."""
    workplan = get_from_ctxmap(context, "workplan", LiveWorkplan)

    links, conflicts = collect_links(workplan)

    if conflicts:
        report_conflicts(conflicts)
        raise typer.Exit(1)

    dest = RomsFileSystemManager(
        StateDirectoryManager.data_dir(run_id)
    ).joined_output_dir
    relink(dest, links)

    if not links:
        console.print(
            f"No joined output was found yet for run {run_id!r}. "
            f"An empty directory was left at {dest}; re-run `gather` once steps "
            "have produced output.",
            soft_wrap=True,
        )
        return

    num_step_dirs = len({p.parent for p in links.values()})
    console.print(
        f"Linked {len(links)} file(s) from {num_step_dirs} step "
        f"directories into {dest}",
        soft_wrap=True,
    )


if __name__ == "__main__":
    typer.run(gather)
