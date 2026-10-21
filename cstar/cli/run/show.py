import asyncio
import os
import typing as t
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import typer
from rich.table import Column, Table

from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
from cstar.cli.common import cb_pipeline, console, get_from_ctxmap, set_ctxmap
from cstar.cli.run.shared import is_path_like, refuse_workplan_file
from cstar.cli.workplan.shared import list_runs, preload_run_record
from cstar.entrypoint.utils import (
    ARG_FORMAT,
    ARG_FORMAT_HELP,
    ARG_NO_REFRESH,
    ARG_NO_REFRESH_HELP,
)
from cstar.execution.file_system import JobFileSystemManager
from cstar.orchestration.dag_runner import (
    get_launcher,
    load_run_state,
    refresh_run_manifest,
)
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    RunManifest,
    StepEntry,
    StepRecord,
    fold_step_records,
    read_run_manifest,
)
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.serialization import model_to_yaml
from cstar.orchestration.tracking import WorkplanRun
from cstar.system.migration import CstarSchemaTooNewError

log = get_logger(__name__)
app = typer.Typer()

COMMAND_SHOW: t.Final[str] = "show"
"""Name of the command that shows a run's manifest."""

KEY_TARGET: t.Final[str] = "show_target"
"""Context-map key under which the resolved target is stored."""

FORMATS = t.Literal["table", "yaml", "json"]
"""The output formats of the command."""

HELP_SHORT = "Show what a run was and what it produced."
HELP_LONG = f"""\
{HELP_SHORT}

The target is a run-id, or the path of a directory holding a run manifest
(`{RUN_MANIFEST_NAME}`). The manifest lists each attempt to run the workplan, each
step's status and outcome, the products the steps recorded, and each step's job
with how long it waited and ran.

For a run-id the step statuses are refreshed from the run first, as
`cstar run status` does, and the manifest is updated; {ARG_NO_REFRESH} shows
it as last recorded. A directory is read as it is: step records written since
the manifest was last refreshed are shown, nothing is written.

Exit code 0: shown. 1: the run has no readable manifest. 2: bad arguments.
"""


@dataclass(frozen=True)
class ShowTarget:
    """What the `target` argument resolved to."""

    root: Path
    """The run root, which holds the manifest."""
    run_id: str | None
    """The run-id, when the target was found through tracking."""


def _directory_target(target: str) -> ShowTarget:
    """Resolve a target spelled as the path of a directory holding a manifest.

    Parameters
    ----------
    target : str
        The user-supplied path.

    Returns
    -------
    ShowTarget

    Raises
    ------
    typer.BadParameter
        Raised when the path is not a directory holding a run manifest.
    """
    root = Path(os.path.abspath(Path(target).expanduser()))
    if not root.is_dir():
        raise typer.BadParameter(f"{str(root)!r} is not a directory.")
    if not (root / RUN_MANIFEST_NAME).is_file():
        msg = f"{str(root)!r} holds no {RUN_MANIFEST_NAME}."
        if JobFileSystemManager(root).tasks_dir.is_dir():
            msg += (
                " The run predates the manifest: re-entering it with "
                "`cstar workplan run --run-id <run-id>` records one."
            )
        raise typer.BadParameter(msg)

    return ShowTarget(root, None)


def resolve_show_target(context: typer.Context, target: str) -> str:
    """Resolve the target to a run root and store it in the context map.

    Parameters
    ----------
    context : typer.Context
        The typer context; receives the run record from `preload_run_record`
        when the target is a run-id.
    target : str
        The user-supplied run-id or directory path.

    Returns
    -------
    str
        The target, unchanged.

    Raises
    ------
    typer.BadParameter
        Raised when the target does not identify a run.
    """
    if is_path_like(target):
        resolved = _directory_target(target)
    else:
        preload_run_record(context, target)
        run = get_from_ctxmap(context, "run", WorkplanRun)
        resolved = ShowTarget(run.output_path, run.run_id)

    set_ctxmap(context, KEY_TARGET, resolved)
    return target


def _when(moment: datetime | None) -> str:
    """Format a moment in local time, or a dash when there is none."""
    return f"{moment.astimezone():%Y-%m-%d %H:%M}" if moment else "-"


def _duration(start: datetime | None, end: datetime | None) -> str:
    """Format the time from `start` to `end` as `H:MM:SS`, or a dash.

    The hours are not wrapped into days. A dash stands for a missing moment, or
    for a negative span, which clocks that disagree can produce.
    """
    if start is None or end is None:
        return "-"
    seconds = int((end - start).total_seconds())
    if seconds < 0:
        return "-"
    hours, remainder = divmod(seconds, 3600)
    return f"{hours}:{remainder // 60:02d}:{remainder % 60:02d}"


def _submitted_record(entry: StepEntry) -> StepRecord | None:
    """The record of the step's latest submission, when the entry holds it.

    An entry can pair a new submission with the record of an earlier run of the
    step: when a directory is shown without statuses, under `--no-refresh`, or
    when the new run died before writing its own. A record that started before
    the submission, or that names another SLURM job, is such an earlier one.
    """
    record = entry.record
    if record is None or entry.submitted_at is None:
        return record
    if record.start_at is not None and record.start_at < entry.submitted_at:
        return None
    job_id = record.execution.job_id
    if entry.launcher == "slurm" and job_id and job_id != entry.job_id:
        return None
    return record


def _size(nbytes: int) -> str:
    """Format a number of bytes with a binary unit."""
    size = float(nbytes)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TiB"


def _tables(manifest: RunManifest) -> list[Table]:
    """Lay the manifest out as tables.

    Parameters
    ----------
    manifest : RunManifest
        The manifest to lay out.

    Returns
    -------
    list[Table]
        The identity, attempts, steps, the steps' jobs and, when there are any,
        external runs.
    """
    identity = Table(title=f"Run {manifest.run_id}", show_header=False)
    identity.add_column(justify="right", style="bold")
    identity.add_column()
    for label, value in (
        ("Name", manifest.name),
        ("Kind", manifest.kind),
        ("Run-id", manifest.run_id),
        ("Id", manifest.id),
        ("System", manifest.system or "-"),
        ("Created", _when(manifest.created_at)),
        ("Observed", _when(manifest.observed_at)),
    ):
        identity.add_row(label, value)

    attempts = Table(
        Column("Started", no_wrap=True),
        Column("Finished", no_wrap=True),
        "Status",
        "Clobbered",
        "Resumed",
        "Pre-run",
        Column("cstar-ocean", no_wrap=True),
        "Inputs",
        title="Attempts",
    )
    for attempt in manifest.attempts:
        attempts.add_row(
            _when(attempt.start_at),
            _when(attempt.finished_at),
            attempt.status.value,
            ", ".join(attempt.clobbered) or "-",
            ", ".join(attempt.resumed) or "-",
            "yes" if attempt.pre_run else "no",
            attempt.versions.get("cstar-ocean", "-"),
            # every attempt directory is under `attempts/`; its name is the column
            Path(attempt.directory).name if attempt.directory else "-",
        )

    steps = Table(
        "Step",
        "Application",
        "Status",
        "Outcome",
        "Products",
        "Size",
        "Record",
        title="Steps",
    )
    # a table of its own: beside the steps' columns it would not fit 80 columns
    jobs = Table(
        "Step",
        Column("Job", no_wrap=True),
        Column("Waited", no_wrap=True),
        Column("Wall", no_wrap=True),
        title="Jobs",
    )
    for entry in manifest.steps:
        record = entry.record
        steps.add_row(
            entry.name,
            entry.application,
            entry.status or "-",
            record.outcome if record else "-",
            str(len(record.products)) if record else "-",
            _size(sum(p.size_bytes for p in record.products)) if record else "-",
            "yes" if record else "no",
        )
        ran = _submitted_record(entry)
        jobs.add_row(
            entry.name,
            f"{entry.launcher} {entry.job_id}".strip() if entry.job_id else "-",
            _duration(entry.submitted_at, ran.start_at) if ran else "-",
            _duration(ran.start_at, ran.finished_at) if ran else "-",
        )

    tables = [identity, attempts, steps, jobs]
    if manifest.external.runs:
        external = Table("Alias", "Run-id", "Start", title="External runs")
        for ref in manifest.external.runs:
            external.add_row(ref.alias, ref.run_id, _when(ref.start_at))
        tables.append(external)

    return tables


def _read_manifest(resolved: ShowTarget) -> RunManifest:
    """Read the manifest of a run, or end the command when there is none.

    Parameters
    ----------
    resolved : ShowTarget
        The run to read.

    Returns
    -------
    RunManifest

    Raises
    ------
    typer.Exit
        With code 1 when the manifest is missing, corrupt or too new.
    """
    try:
        manifest = read_run_manifest(resolved.root)
    except (CstarError, CstarSchemaTooNewError) as ex:
        typer.echo(str(ex), err=True)
        raise typer.Exit(1) from ex

    if manifest is None:
        typer.echo(
            f"Run {resolved.run_id or str(resolved.root)!r} has no manifest: it "
            "predates the manifest, and re-entering it with "
            "`cstar workplan run --run-id <run-id>` records one.",
            err=True,
        )
        raise typer.Exit(1)

    return manifest


@app.command(name=COMMAND_SHOW, help=HELP_LONG, short_help=HELP_SHORT)
def show(
    context: typer.Context,
    target: t.Annotated[
        str,
        typer.Argument(
            help="A run-id, or the path of a directory holding a run manifest.",
            autocompletion=list_runs,
            callback=cb_pipeline(refuse_workplan_file, resolve_show_target),
        ),
    ],
    no_refresh: t.Annotated[
        bool, typer.Option(ARG_NO_REFRESH, help=ARG_NO_REFRESH_HELP)
    ] = False,
    format: t.Annotated[
        FORMATS, typer.Option(ARG_FORMAT, help=ARG_FORMAT_HELP)
    ] = "table",
) -> None:
    """Show what a run was and what it produced."""
    resolved = get_from_ctxmap(context, KEY_TARGET, ShowTarget)
    root = resolved.root
    manifest = _read_manifest(resolved)

    if resolved.run_id is None:
        # a copied run's manifest may never have been refreshed: fold what its
        # steps recorded, in memory only
        fold_step_records(manifest, root)
    elif not no_refresh:
        run = get_from_ctxmap(context, "run", WorkplanRun)
        workplan = LiveWorkplan.try_load(run.trx_workplan_path)
        if workplan is None:
            typer.echo(
                f"Unable to load the transformed workplan {run.trx_workplan_path}; "
                "showing the manifest as last recorded.",
                err=True,
            )
        else:
            launcher = get_launcher(workplan, force_local=workplan.pre_run)
            status = asyncio.run(load_run_state(resolved.run_id, launcher))
            manifest = refresh_run_manifest(root, resolved.run_id, status) or manifest

    if format == "yaml":
        typer.echo(model_to_yaml(manifest))
    elif format == "json":
        typer.echo(manifest.model_dump_json(indent=2))
    else:
        for table in _tables(manifest):
            console.print(table)


if __name__ == "__main__":
    app()
