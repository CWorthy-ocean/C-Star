import os
import shutil
import typing as t
from collections import defaultdict
from pathlib import Path

import typer

from cstar.base.log import get_logger
from cstar.cli.common import get_from_ctxmap
from cstar.cli.workplan.shared import RunIdArgument, console
from cstar.execution.file_system import JobFileSystemManager, StateDirectoryManager
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan

log = get_logger(__name__)
app = typer.Typer()

GATHERED_OUTPUT_NAME: t.Final[str] = "gathered_output"
"""The name of the run-level directory of symlinks built by `gather`."""

HELP_SHORT = "Consolidate per-step output into a single directory."
HELP_LONG = f"""\
{HELP_SHORT}

Every step's `output` directory is scanned and a run-level
`{GATHERED_OUTPUT_NAME}` directory of symlinks is (re)built pointing at
whatever files currently exist. A filename produced by more than one step is
linked under a `<step>__<filename>` name so every file is still gathered
instead of aborting the run. Safe to re-run at any time, including while the
workplan is still in progress: each invocation replaces the existing symlinks
with a fresh set reflecting the current state on disk.
"""


def collect_links(
    workplan: LiveWorkplan,
) -> tuple[dict[str, Path], dict[str, list[Path]], int]:
    """Collect the output files produced by every step in a workplan.

    A filename produced by exactly one step is linked under its own name. A
    filename produced by more than one step is linked once per producing
    step, under a `f"{{step.safe_name}}__{{basename}}"` name, so that
    de-duplication does not require aborting the gather. Only a residual
    collision -- a mangled name that still collides with another link name
    -- is reported as a conflict.

    Parameters
    ----------
    workplan : LiveWorkplan
        The workplan whose steps should be scanned for output files.

    Returns
    -------
    tuple[dict[str, Path], dict[str, list[Path]], int]
        A mapping of link name to source path for every file that could be
        linked without a residual collision, a mapping of link name to the
        list of source paths for names that still collide after mangling,
        and the number of files whose link name was mangled.
    """
    by_basename: dict[str, list[tuple[LiveStep, Path]]] = defaultdict(list)
    skipped: list[str] = []

    for step in workplan.steps:
        src_dir = JobFileSystemManager(step.working_dir).output_dir
        if not src_dir.exists():
            skipped.append(step.name)
            continue

        for p in sorted(src_dir.iterdir()):
            if p.is_file():
                by_basename[p.name].append((step, p))

    if skipped:
        log.debug(
            "Skipping %d step(s) with no output directory: %s",
            len(skipped),
            ", ".join(skipped),
        )

    # Group candidate (link_name, source) pairs; a basename with a single
    # producer keeps its own name, one with several is mangled per-producer.
    candidates: list[tuple[str, Path]] = []
    mangled_count = 0
    for basename, producers in by_basename.items():
        if len(producers) == 1:
            _, path = producers[0]
            candidates.append((basename, path))
        else:
            for step, path in producers:
                candidates.append((f"{step.safe_name}__{basename}", path))
            mangled_count += len(producers)

    by_link_name: dict[str, list[Path]] = defaultdict(list)
    for name, path in candidates:
        by_link_name[name].append(path)

    links = {name: paths[0] for name, paths in by_link_name.items() if len(paths) == 1}
    conflicts = {name: paths for name, paths in by_link_name.items() if len(paths) > 1}

    return links, conflicts, mangled_count


def report_conflicts(conflicts: dict[str, list[Path]]) -> None:
    """Print a single error message describing every residual link-name collision.

    Parameters
    ----------
    conflicts : dict[str, list[Path]]
        A mapping of link name to the list of source paths still producing
        it after step-name mangling.
    """
    lines = [
        f"  - {name!r}: {', '.join(str(p) for p in paths)}"
        for name, paths in sorted(conflicts.items())
    ]
    console.print(
        "Refusing to gather: the following link names are still produced by "
        "more than one step's output directory, even after mangling with a "
        "step-name prefix:\n" + "\n".join(lines),
        soft_wrap=True,
    )


def relink(dest: Path, links: dict[str, Path]) -> None:
    """Replace a consolidated gathered-output directory with a fresh set of symlinks.

    The existing directory is removed and recreated. If `dest` contains any
    entries that are not symlinks, nothing is modified and a `typer.Exit` is
    raised, since gather must never delete real files from the consolidated
    directory.

    Parameters
    ----------
    dest : Path
        The consolidated `gathered_output` directory to (re)populate.
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
            f"Refusing to gather: the consolidated gathered_output path "
            f"{str(dest)!r} exists but is not a directory.",
            soft_wrap=True,
        )
        raise typer.Exit(1)

    if dest.exists():
        non_symlinks = sorted(p for p in dest.iterdir() if not p.is_symlink())
        if non_symlinks:
            names = "\n".join(f"  - {p}" for p in non_symlinks)
            console.print(
                "Refusing to gather: the consolidated gathered_output directory "
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
    """Consolidate per-step output into a run-level directory of symlinks."""
    workplan = get_from_ctxmap(context, "workplan", LiveWorkplan)

    links, conflicts, mangled_count = collect_links(workplan)

    if conflicts:
        report_conflicts(conflicts)
        raise typer.Exit(1)

    dest = StateDirectoryManager.data_dir(run_id) / GATHERED_OUTPUT_NAME
    relink(dest, links)

    if not links:
        console.print(
            f"No output was found yet for run {run_id!r}. "
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

    if mangled_count:
        console.print(
            f"{mangled_count} file name(s) were mangled with a step-name "
            "prefix to avoid collisions.",
            soft_wrap=True,
        )


if __name__ == "__main__":
    typer.run(gather)
