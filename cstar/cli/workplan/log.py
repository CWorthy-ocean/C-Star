import asyncio
import time
import typing as t

import typer

from cstar.base.env import ENV_CSTAR_RUNID
from cstar.base.log import get_logger
from cstar.cli.common import set_env
from cstar.cli.workplan.shared import autocomplete_step_list, list_runs
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.tracking import TrackingRepository

log = get_logger(__name__)
app = typer.Typer()

_MONITOR_POLL_INTERVAL: t.Final[float] = 0.25


HELP_SHORT = "Print the log for a workplan step."
HELP_LONG = f"""\
{HELP_SHORT}

The `run_id` may be from an in-progress or completed run.
"""

ARG_MONITOR: t.Final[str] = "--monitor"


@app.command(name="log", help=HELP_LONG, short_help=HELP_SHORT)
def workplan_log(
    run_id: t.Annotated[
        str,
        typer.Argument(
            help="The unique identifier of a specific workplan execution.",
            autocompletion=list_runs,
            callback=set_env(ENV_CSTAR_RUNID),
        ),
    ],
    step_name: t.Annotated[
        str,
        typer.Argument(
            help="The name of the step whose log should be printed.",
            autocompletion=autocomplete_step_list,
        ),
    ],
    monitor: t.Annotated[
        bool,
        typer.Option(
            ARG_MONITOR,
            help="Set this flag to stream updates from the log file indefinitely (like tail -f). "
            "Waits for the file to appear if it does not yet exist.",
        ),
    ] = False,
) -> None:
    """Print the log for a workplan step."""
    repo = TrackingRepository()
    wp_run = asyncio.run(repo.get_workplan_run(run_id))
    if not wp_run:
        raise typer.BadParameter(
            f"Unable to monitor logs for unknown run-id: {run_id}",
            param_hint="run_id",
        )

    wp = deserialize(wp_run.trx_workplan_path, Workplan)
    step = next((x for x in wp.steps if x.name == step_name), None)
    if step is None:
        valid_steps = ", ".join(f"{s.name!r}" for s in wp.steps)
        raise typer.BadParameter(
            f"Unable to monitor logs for unknown step: {step_name!r}. Valid values: {valid_steps}",
            param_hint="step_name",
        )

    log_path = LiveStep.from_step(step).log_path

    if not monitor and not log_path.exists():
        print(f"No log file found for step {step_name!r} in run {run_id!r}.")
        print(f"Expected path: {log_path}")
        raise typer.Exit(1)

    if not monitor:
        print(log_path.read_text(), end="")
        return

    # --monitor: wait for the file to appear, then stream new content
    try:
        while not log_path.exists():
            time.sleep(_MONITOR_POLL_INTERVAL)

        with log_path.open("r") as fp:
            while True:
                chunk = fp.read()
                if chunk:
                    print(chunk, end="", flush=True)
                else:
                    time.sleep(_MONITOR_POLL_INTERVAL)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    typer.run(workplan_log)
