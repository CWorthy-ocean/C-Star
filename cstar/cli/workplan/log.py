import time
import typing as t

import typer

from cstar.base.env import ENV_CSTAR_RUNID
from cstar.base.log import get_logger
from cstar.cli.common import cb_pipeline, get_from_ctxmap, set_env
from cstar.cli.workplan.shared import (
    autocomplete_step_list,
    list_runs,
    preload_run,
    set_ctxmap,
)
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import LiveStep

log = get_logger(__name__)
app = typer.Typer()

_MONITOR_POLL_INTERVAL: t.Final[float] = 0.25


HELP_SHORT = "Print the log for a workplan step."
HELP_LONG = f"""\
{HELP_SHORT}

The `run_id` may be from an in-progress or completed run.
"""

ARG_MONITOR: t.Final[str] = "--monitor"


def preload_step(context: typer.Context, step_name: str) -> str:
    """Given a step-name, ensure is a valid name in a preloaded workplan

    NOTE: Requires the workplan to be loaded into context dict as "workplan", e.g.
    `wp = cstar.cli.common.get_from_ctxmap(context, "workplan", Workplan)`

    See also: `cstar.cli.common.set_ctxmap`

    Parameters
    ----------
    context : typer.Context
        The typer context.
    step_name : str
        The user-suppplied step-name.

    Returns
    -------
    str

    Raises
    ------
    typer.BadParameter
        - Raised when the step-name cannot be found in the target workplan.
    """
    run_id = str(context.params.get("run_id", ""))
    if not run_id:
        msg = "A run-id is required to retrieve steps"
        raise typer.BadParameter(msg, param_hint="run_id")

    wp = get_from_ctxmap(context, "workplan", Workplan)
    step = next((x for x in wp.steps if x.name == step_name), None)
    if step is None:
        valid_steps = ", ".join(f"{s.name!r}" for s in wp.steps)
        raise typer.BadParameter(
            f"Unable to monitor logs for unknown step: {step_name!r}. Valid values: {valid_steps}",
            param_hint="step_name",
        )

    set_ctxmap(context, "step", step)
    set_ctxmap(context, "live_step", LiveStep.from_step(step))

    return step_name


@app.command(name="log", help=HELP_LONG, short_help=HELP_SHORT)
def workplan_log(
    context: typer.Context,
    run_id: t.Annotated[
        str,
        typer.Argument(
            help="The unique identifier of a specific workplan execution.",
            autocompletion=list_runs,
            callback=cb_pipeline(set_env(ENV_CSTAR_RUNID), preload_run),
        ),
    ],
    step_name: t.Annotated[
        str,
        typer.Argument(
            help="The name of the step whose log should be printed.",
            autocompletion=autocomplete_step_list,
            callback=preload_step,
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
    step = get_from_ctxmap(context, "step", LiveStep)
    log_path = step.log_path

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
