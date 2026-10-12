import asyncio
import typing as t

import typer
from rich.console import Console

from cstar.base.log import get_logger
from cstar.cli.common import get_from_ctxmap
from cstar.cli.workplan.shared import (
    RunRecordArgument,
    display_summary,
    refresh_disk_usage,
)
from cstar.entrypoint.utils import ARG_SIZE, ARG_SIZE_HELP
from cstar.orchestration.dag_runner import (
    DagStatus,
    get_launcher,
    get_status_detail_map,
    load_external_runs,
    load_run_state,
)
from cstar.orchestration.orchestration import LiveWorkplan, Planner
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.tracking import WorkplanRun

log = get_logger(__name__)
app = typer.Typer()
console = Console()


@app.command(name="status", help="Retrieve the current status of a workplan.")
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
) -> None:
    """Retrieve the current status of a workplan."""
    run = get_from_ctxmap(ctx, "run", WorkplanRun)
    wp_path = run.trx_workplan_path

    try:
        workplan = deserialize(wp_path, LiveWorkplan)

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
        status = asyncio.run(load_run_state(run.run_id, launcher))
        status = DagStatus({**external.statuses(), **status.details})
        lookup = get_status_detail_map(planner, status)

        if refresh_usage:
            asyncio.run(refresh_disk_usage([run], {wp_path: workplan}))

        display_summary(run, lookup)
    except FileNotFoundError:  # blueprint not found.
        console.print_exception()


if __name__ == "__main__":
    typer.run(status)
