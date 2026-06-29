import asyncio
import os
import textwrap
import typing as t
from pathlib import Path

import typer
from pydantic import BaseModel

from cstar.base.env import (
    ENV_CSTAR_CLI_DRY_RUN,
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_LOG_LEVEL,
    ENV_CSTAR_RUNID,
)
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import LogLevelChoices, get_logger
from cstar.cli.common import (
    cb_pipeline,
    set_env,
    set_flag,
    update_loggers,
)
from cstar.cli.workplan.shared import (
    check_and_capture_kvps,
    colored,
    console,
    list_runs,
    present,
)
from cstar.entrypoint.utils import (
    ARG_CLOBBER,
    ARG_CLOBBER_HELP,
    ARG_DRY_RUN,
    ARG_LOGLEVEL_HELP,
    ARG_LOGLEVEL_LONG,
    ARG_LOGLEVEL_SHORT,
)
from cstar.execution.file_system import local_copy
from cstar.orchestration.dag_runner import (
    ExecutiveRunSummary,
    ExecutiveStepSummary,
    build_and_run_dag,
)
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import ProcessHandle
from cstar.orchestration.serialization import (
    try_deserialize,
    validate_serialized_entity,
)
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun

if t.TYPE_CHECKING:
    from collections.abc import Mapping

    from pydantic.fields import ComputedFieldInfo, FieldInfo

app = typer.Typer()
log = get_logger(__name__)

HELP_SHORT = "Execute a workplan."
HELP_LONG = f"""\
{HELP_SHORT}

Specify a previously used `run_id` to re-start a prior run.
"""

CATEGORY_HEADER_COLOR: t.Final[str] = "white"
SECTION_HEADER_COLOR: t.Final[str] = "yellow"
ATTR_COLOR: t.Final[str] = "cyan"
INDENT = " "


def preprocess_vars(
    ctx: typer.Context,
    user_variables: list[str],
) -> list[str] | None:
    """Perform validation and formatting on user-supplied variables.

    Places the processed variables into the user data slot of the typer context object.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app
    user_variables : list[str]
        A list of key-value pairs supplied by a user.

    Returns
    -------
    list[str] | None
        The original input with leading/trailing whitespace stripped from the keys
        and values.

    Raises
    ------
    typer.BadParameter
        - If the key-value pair does not meet `key=value` convention
    """
    if not user_variables:
        return None

    ctx.obj = check_and_capture_kvps(user_variables)

    return user_variables


def preprocess_varfile(
    ctx: typer.Context,
    user_varfile_path: Path | None,
) -> Path | None:
    """Perform validation and formatting on user-supplied variables
    supplied through a path to a variables file.

    Places the processed variables into the user data slot of the typer context object.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app.
    user_varfile_path : Path | None
        A path to a file containing the variable configuration.

    Returns
    -------
    Path | None

    Raises
    ------
    typer.BadParameter
        - If the source file does not exist
        - If any individual key-value pair is malformed
    """
    if user_varfile_path is None:
        return None

    with user_varfile_path.open("r") as fp:
        lines = [x.strip() for x in fp.readlines() if x.strip()]

    ctx.obj = check_and_capture_kvps(lines)

    return user_varfile_path


def _ft(model_type: type[BaseModel], field_name: str) -> str:
    """Retrieve the field title from a type for a field."""
    field: FieldInfo | ComputedFieldInfo | None = None
    if field_name in model_type.model_fields:
        field = model_type.model_fields[field_name]
    if field_name in model_type.model_computed_fields:
        field = model_type.model_computed_fields[field_name]
    return str(field.title) if field else "INVALID-FIELD-NAME"


def _stepft(field_name: str) -> str:
    """Retrieve the field title from the step summary model."""
    return _ft(ExecutiveStepSummary, field_name)


def _runft(field_name: str) -> str:
    """Retrieve the field title from the run summary model."""
    return _ft(ExecutiveRunSummary, field_name)


def _hdr(header: str, color: str = "green") -> tuple[str, str]:
    """Generate header content and delimiter."""
    underline_char = "-"
    content = colored(header, color)
    delimiter = underline_char * len(header)
    return content, delimiter


def get_step_summary_display(summary: ExecutiveStepSummary) -> str:
    """Generate the executive summary display for a step from a workplan run."""
    step_header, step_underline = _hdr(f"{summary.name!r}", "green")
    assets_header, assets_underline = _hdr("Asset Paths", SECTION_HEADER_COLOR)
    job_header, job_underline = _hdr("Job Details", SECTION_HEADER_COLOR)

    handle = try_deserialize(summary.sentinel_path, ProcessHandle)
    task_prompt = _stepft("task_id")

    if handle is None:
        pid = "N/A"
    else:
        pid = handle.pid if handle else "N/A"
        if handle.launcher_name == "slurm":
            task_prompt = "SLURM Job ID"

    return textwrap.dedent(f"""\
        {step_header}
        {step_underline}
        {INDENT}{assets_header}
        {INDENT}{assets_underline}
        {INDENT * 2}- {_stepft("log_path")}: {summary.log_path}
        {INDENT * 2}- {_stepft("blueprint_path")}: {summary.blueprint_path}
        {INDENT * 2}- {_stepft("working_dir")}: {summary.working_dir}
        {INDENT * 2}- {_stepft("script_path")}: {summary.script_path}
        {INDENT}{job_header}
        {INDENT}{job_underline}
        {INDENT * 2}- {task_prompt}: {colored(pid, ATTR_COLOR)}
        {INDENT * 2}- {_stepft("status")}: {colored(summary.status, ATTR_COLOR)}
        """)


def get_run_summary_display(summary: ExecutiveRunSummary) -> str:
    """Generate a print-friendly summary for a workplan run."""
    prefix = "# "
    content_delimiter = "#" * 78

    header = f"{summary.workplan_name!r} Run Summary"
    if summary.dry_run:
        header = f"{header} - DRY-RUN ONLY"
    run_header, run_underline = _hdr(header, CATEGORY_HEADER_COLOR)
    section_header_steps, section_del_steps = _hdr(
        _runft("steps"), CATEGORY_HEADER_COLOR
    )
    section_header_actions, section_del_actions = _hdr(
        "Further Actions", CATEGORY_HEADER_COLOR
    )

    content = textwrap.dedent(f"""\
                {prefix}{content_delimiter}
                {prefix}{run_header}
                {prefix}{run_underline}
                {prefix}{INDENT}- {_runft("run_id")}: {colored(summary.run_id, ATTR_COLOR)}
                {prefix}{INDENT}- {_runft("source_workplan")}: {summary.source_workplan}
                {prefix}{INDENT}- {_runft("final_workplan")}: {summary.final_workplan}
                {prefix}{INDENT}- {_runft("state_dir")}: {summary.state_dir}
                {prefix}
                {prefix}{section_header_steps}
                {prefix}{section_del_steps}
                <steps>
                {prefix}{section_header_actions}
                {prefix}{section_del_actions}
                <actions>
                {prefix}
                {prefix}{content_delimiter}
                """)

    step_content = [get_step_summary_display(s) for s in summary.steps]
    step_summaries = [
        f"{prefix}{INDENT}{line}" for item in step_content for line in item.split("\n")
    ]
    steps_section = (
        "\n".join(step_summaries)
        if step_summaries
        else f"{prefix}{INDENT}- No steps found"
    )

    prompts = [
        ("For run status details", f"cstar workplan status {summary.run_id}"),
        ("To review logs", f"cstar workplan log {summary.run_id} <step-name>"),
    ]

    actions_content = [f"{prefix}{INDENT}- {present(*prompt)}" for prompt in prompts]
    actions_section = "\n".join(actions_content)

    value_map = {"<steps>": steps_section, "<actions>": actions_section}
    for tpl, value in value_map.items():
        content = content.replace(tpl, value)

    return content + "\n"


def preprocess_runid(ctx: typer.Context, run_id: str) -> str:
    """Perform validation and formatting of the run-id argument.

    - verify blueprint path or run-id is supplied
    - generate a default run id from a blueprint if run-id is not supplied

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app.
    run_id : str
        The user-supplied run-id.

    Returns
    -------
    str
    """
    if run_id := run_id.strip():
        return run_id

    path: str | None = ctx.params.get("path", None)
    if isinstance(path, str):
        path = path.strip()

    if not path:
        msg = "A run-id or workplan path is required"
        raise typer.BadParameter(msg)

    run_id = WorkplanRun.get_default_run_id(path)

    msg = f"Generated a default run-id `{run_id}` from `{path}`"
    log.debug(msg)

    return run_id


def preprocess_path(workplan_path: str | None) -> str | None:
    """Perform validation related to the workplan path.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app.
    workplan_path : str | None
        The user-supplied path to a workplan file.

    Returns
    -------
    str
    """
    if workplan_path:
        try:
            with local_copy(workplan_path) as local_path:
                if not local_path.exists():
                    msg = f"Workplan not found at path: {workplan_path}"
                    raise typer.BadParameter(msg)

                validation_result = validate_serialized_entity(local_path, Workplan)
                if not validation_result.item:
                    log.error(validation_result.error_msg)
                    msg = f"The workplan file in `{workplan_path}` is improperly formatted"
                    raise typer.BadParameter(msg)

        except FileNotFoundError as ex:
            msg = f"Workplan not found at path: {workplan_path}"
            raise typer.BadParameter(msg) from ex

    return workplan_path


def handle_run_reloading(run_id: str) -> str:
    """Locate a prior run for the run ID and update the `RunCmdContext` with
    the correct `Workplan`.

    Parameters
    ----------
    run_id : str
        The run-id to reload
    """
    repo = TrackingRepository()
    wp_run = asyncio.run(repo.get_workplan_run(run_id))
    if wp_run is None:
        msg = f"No runs with the id `{run_id}` could be found."
        raise typer.BadParameter(msg)

    # ensure the environment matches the prior run
    os.environ.update(wp_run.environment)

    path = wp_run.workplan_path
    msg = f"Re-starting run-id `{run_id}` with workplan originating in `{path}`"
    log.info(msg)

    return wp_run.trx_workplan_path.as_posix()


@app.command(name="run", help=HELP_LONG, short_help=HELP_SHORT)
def run(
    ctx: typer.Context,
    run_id: t.Annotated[
        str,
        typer.Option(
            help="The unique identifier for an execution of the workplan.",
            autocompletion=list_runs,
            callback=cb_pipeline(preprocess_runid, set_env(ENV_CSTAR_RUNID)),
        ),
    ] = "",
    user_variables: t.Annotated[
        list[str] | None,
        typer.Option(
            "--var",
            "-v",
            help=(
                "Specify 0-to-many replacements as key-value pairs in "
                "the form `key=value`."
            ),
            callback=preprocess_vars,
        ),
    ] = None,
    user_variables_path: t.Annotated[
        Path | None,
        typer.Option(
            "--varfile",
            "-f",
            help=(
                "Specify the path to a file containing one replacements per line "
                "as key-value pairs in the form `key=value`."
            ),
            callback=preprocess_varfile,
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    path: t.Annotated[
        str,
        typer.Argument(
            help="Path to a workplan file.",
            callback=preprocess_path,
        ),
    ] = "",
    dry_run: t.Annotated[
        bool,
        typer.Option(
            ARG_DRY_RUN,
            help="Set this flag to generate an execution plan without executing the workplan.",
            envvar=ENV_CSTAR_CLI_DRY_RUN,
            callback=set_flag(ENV_CSTAR_CLI_DRY_RUN),
        ),
    ] = False,
    log_level: t.Annotated[
        LogLevelChoices,
        typer.Option(
            ARG_LOGLEVEL_LONG,
            ARG_LOGLEVEL_SHORT,
            callback=cb_pipeline(set_env(ENV_CSTAR_LOG_LEVEL), update_loggers),
            help=ARG_LOGLEVEL_HELP,
            envvar=ENV_CSTAR_LOG_LEVEL,
        ),
    ] = LogLevelChoices.INFO,
    clobber: t.Annotated[
        bool,
        typer.Option(
            ARG_CLOBBER,
            callback=set_flag(ENV_CSTAR_CLOBBER_WORKING_DIR),
            help=ARG_CLOBBER_HELP,
            envvar=ENV_CSTAR_CLOBBER_WORKING_DIR,
        ),
    ] = False,
) -> None:
    """Execute a workplan.

    Specify a previously used run_id option to re-start a prior run.
    """
    if user_variables is not None and user_variables_path is not None:
        msg = "`--var` and `--varfile` must not be supplied together"
        raise typer.BadParameter(msg)

    if not path:
        path = handle_run_reloading(run_id)

    try:
        with local_copy(path) as wp_path:
            summary = asyncio.run(
                build_and_run_dag(
                    wp_path,
                    run_id,
                    user_variables=t.cast("Mapping[str, str]", ctx.obj),
                    dry_run=dry_run,
                ),
            )
            console.print(get_run_summary_display(summary))
    except CstarExpectationFailed as ex:
        msg = f"An invalid request was made: {ex}"
        log.exception(msg)
        print(msg)
        raise typer.Exit(1) from ex
    except ValueError as ex:
        msg = "Invalid inputs encountered while running the dag"
        log.exception(msg)
        raise typer.BadParameter(ex.args[0]) from ex
    except Exception as ex:
        msg = f"Workplan run {run_id!r} has failed: {ex}"
        log.exception(msg)
        print(msg)
        raise typer.Exit(3) from ex

    console.print(
        f"{summary.workplan_name!r} {'dry-run' if dry_run else 'run scheduling'} has completed"
    )


if __name__ == "__main__":
    typer.run(run)
