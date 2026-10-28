import asyncio
import typing as t

import typer
from rich.console import Console

from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
from cstar.cli.common import get_from_ctxmap
from cstar.cli.workplan.shared import (
    KEY_RUN,
    RunRecordArgument,
    display_summary,
    refresh_disk_usage,
)
from cstar.entrypoint.utils import (
    ARG_NO_REFRESH,
    ARG_NO_REFRESH_HELP,
    ARG_SIZE,
    ARG_SIZE_HELP,
)
from cstar.orchestration.dag_runner import (
    DagStatus,
    get_launcher,
    get_status_detail_map,
    load_external_runs,
    load_run_state,
    refresh_run_manifest,
)
from cstar.orchestration.orchestration import LiveWorkplan, Planner, Status
from cstar.orchestration.tracking import WorkplanRun

log = get_logger(__name__)
app = typer.Typer()
console = Console()


HELP_SHORT = "Retrieve the current status of a workplan."
HELP_LONG = f"""\
{HELP_SHORT}

The run is named by its run-id and must be tracked on this machine: its live
state is read from the scheduler. An attached run's status is what its manifest
and step records say. To see any other run, use `cstar run show`.
"""


@app.command(name="status", help=HELP_LONG, short_help=HELP_SHORT)
def status(
    ctx: typer.Context,
    run_id: RunRecordArgument,
    refresh_usage: t.Annotated[
        bool,
        typer.Option(
            ARG_SIZE,
            help=ARG_SIZE_HELP,
        ),
    ] = False,
    no_refresh: t.Annotated[
        bool,
        typer.Option(
            ARG_NO_REFRESH,
            help=ARG_NO_REFRESH_HELP,
        ),
    ] = False,
) -> None:
    """Retrieve the current status of a workplan."""
    run = get_from_ctxmap(ctx, KEY_RUN, WorkplanRun)
    try:
        wp_path = run.trx_workplan_path
    except CstarError as ex:
        raise typer.BadParameter(str(ex)) from ex

    try:
        workplan = LiveWorkplan.load(wp_path, tolerant=True)

        # a pre-run's steps are local processes, whatever the system scheduler
        launcher = get_launcher(workplan, force_local=workplan.pre_run)
        external = asyncio.run(load_external_runs(workplan, launcher))
        # the upstream run's record may be gone: show this run regardless
        for token, message in external.errors().items():
            console.print(
                f"Warning: external dependency {token} cannot be resolved: {message}",
                soft_wrap=True,
            )
        planner = Planner(workplan, external.tasks())
        if run.attached:
            # no sentinels here: the run's own records are the status
            recorded = {
                step.name: run.step_status(step.name) for step in workplan.steps
            }
            status = DagStatus(
                {
                    name: Status[value]
                    for name, value in recorded.items()
                    if value in Status.__members__
                }
            )
        else:
            status = asyncio.run(
                load_run_state(run.run_id, launcher, persist=not no_refresh)
            )
            if not no_refresh:
                refresh_run_manifest(run.output_path, run.run_id, status)
        status = DagStatus({**external.statuses(), **status.details})
        lookup = get_status_detail_map(planner, status)

        if refresh_usage:
            asyncio.run(refresh_disk_usage([run], {wp_path: workplan}))

        display_summary(run, lookup)
    except FileNotFoundError as ex:  # blueprint not found.
        log.debug("A file of the run was not found", exc_info=True)
        typer.echo(f"Unable to show the status of run {run.run_id!r}: {ex}", err=True)


if __name__ == "__main__":
    typer.run(status)
