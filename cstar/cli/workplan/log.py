import time
import typing as t

import typer

from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.cli.workplan.shared import autocomplete_step_list, list_runs
from cstar.execution.file_system import DirectoryManager, JobFileSystemManager

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
    root_fsm = JobFileSystemManager(DirectoryManager.data_home() / run_id)
    step_fsm = root_fsm.get_subtask_manager(step_name)
    log_path = step_fsm.logs_dir / f"{slugify(step_name)}.out"

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
