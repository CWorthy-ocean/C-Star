import typing as t
from pathlib import Path

import typer
from click.core import ParameterSource

from cstar.base.env import ENV_CSTAR_LOG_LEVEL, ENV_CSTAR_RUNID
from cstar.base.log import LogLevelChoices
from cstar.cli.common import (
    cb_pipeline,
    exit_on_refused_settings,
    normalize_runid,
    set_env,
    update_loggers,
)
from cstar.cli.workplan.run import (
    composed_workplan_name,
    preprocess_path,
    submit_workplan,
    workplan_resubmit,
)
from cstar.cli.workplan.shared import list_runs
from cstar.entrypoint.utils import (
    ARG_CLOBBER,
    ARG_CLOBBER_HELP,
    ARG_LOGLEVEL_HELP,
    ARG_LOGLEVEL_LONG,
    ARG_LOGLEVEL_SHORT,
    ARG_PRE_RUN,
    ARG_PRE_RUN_BLUEPRINT_SUBMIT_HELP,
    ARG_RESUME,
    ARG_RESUME_BLUEPRINT_SUBMIT_HELP,
    ARG_RUN_ID,
    OPT_CLOBBER_ALL,
)
from cstar.orchestration.models import BlueprintCore, Workplan
from cstar.orchestration.orchestration import is_transformed_workplan
from cstar.orchestration.serialization import (
    deserialize,
    try_deserialize,
    validate_serialized_entity,
)

CMD_NAME: t.Final[str] = "submit"
CMD_HELP: t.Final[str] = (
    "Submit a blueprint as a single-step workplan: schedule it and exit."
)
CMD_HELP_LONG: t.Final[str] = f"""\
{CMD_HELP}

The run-id defaults to the blueprint's name (slugified). Pass `{ARG_RESUME}` to
re-enter that run and resume its failed step. Pass `{ARG_PRE_RUN}` to prepare the
run without launching it; submitting the blueprint again without `{ARG_PRE_RUN}`
attaches to the prepared directory and launches.

The workplan composed around the blueprint is kept under C-Star's state
directory, never beside the blueprint.
"""

app = typer.Typer()


def check_blueprint_path(path: str) -> str:
    """Validate that the path names a blueprint file.

    Parameters
    ----------
    path : str
        The user-supplied path to a blueprint.

    Returns
    -------
    str
        The path, unchanged.

    Raises
    ------
    typer.BadParameter
        If the file does not exist, is a workplan, or is not a valid blueprint,
        or if nothing of the blueprint's name survives slugifying.
    """
    if not Path(path).expanduser().is_file():
        msg = f"Blueprint file not found: {path}"
        raise typer.BadParameter(msg)

    result = validate_serialized_entity(path, BlueprintCore)
    if result.item is None:
        if try_deserialize(path, Workplan) or is_transformed_workplan(Path(path)):
            msg = (
                f"{path!r} is a workplan, not a blueprint; "
                f"submit it with `{workplan_resubmit(path)}`"
            )
            raise typer.BadParameter(msg)
        raise typer.BadParameter(result.error_msg)

    try:
        composed_workplan_name(result.item)
    except ValueError as ex:
        msg = f"A run cannot be named from the blueprint's name {result.item.name!r}"
        raise typer.BadParameter(msg) from ex

    return path


def default_runid_from_blueprint(ctx: typer.Context, run_id: str) -> str:
    """Default the run-id to the name of the workplan composed around the blueprint.

    Expects a run-id already normalized by the `normalize_runid` stage that
    precedes this one in the callback pipeline, and a blueprint path already
    validated by `check_blueprint_path`.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app.
    run_id : str
        The normalized user-supplied run-id, or an empty string.

    Returns
    -------
    str
    """
    if run_id:
        return run_id

    return composed_workplan_name(deserialize(ctx.params["path"], BlueprintCore))


@app.command(name=CMD_NAME, help=CMD_HELP_LONG, short_help=CMD_HELP)
def submit(
    ctx: typer.Context,
    path: t.Annotated[
        str,
        typer.Argument(
            help="Path to a blueprint file.",
            callback=check_blueprint_path,
            is_eager=True,
        ),
    ],
    run_id: t.Annotated[
        str,
        typer.Option(
            ARG_RUN_ID,
            help=(
                "The unique identifier for an execution of the blueprint; "
                "defaults to the blueprint's name."
            ),
            autocompletion=list_runs,
            callback=cb_pipeline(
                normalize_runid,
                default_runid_from_blueprint,
                set_env(ENV_CSTAR_RUNID),
            ),
        ),
    ] = "",
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
        typer.Option(ARG_CLOBBER, help=ARG_CLOBBER_HELP),
    ] = False,
    resume: t.Annotated[
        bool,
        typer.Option(ARG_RESUME, help=ARG_RESUME_BLUEPRINT_SUBMIT_HELP),
    ] = False,
    pre_run: t.Annotated[
        bool,
        typer.Option(ARG_PRE_RUN, help=ARG_PRE_RUN_BLUEPRINT_SUBMIT_HELP),
    ] = False,
) -> None:
    """Submit a blueprint as a single-step workplan.

    Pass `--resume` to re-enter the prior run of the blueprint and resume its
    failed step; nothing is composed or written.
    """
    exit_on_refused_settings()

    problems = [
        msg
        for condition, msg in (
            (resume and clobber, f"{ARG_RESUME} cannot be combined with {ARG_CLOBBER}"),
            (resume and pre_run, f"{ARG_RESUME} cannot be combined with {ARG_PRE_RUN}"),
        )
        if condition
    ]
    if problems:
        raise typer.BadParameter("; ".join(problems))

    # a resumed run is reloaded by its run-id, as recorded; anything else is composed
    workplan_path = "" if resume else preprocess_path(path)

    submit_workplan(
        workplan_path,
        run_id,
        # the composed workplan has a single step
        clobber=[OPT_CLOBBER_ALL] if clobber else [],
        resume=resume,
        pre_run=pre_run,
        user_vars={},
        explicit_run_id=ctx.get_parameter_source("run_id")
        is not ParameterSource.DEFAULT,
        resubmit=lambda _workplan: f"cstar blueprint {CMD_NAME} {path}",
    )


if __name__ == "__main__":
    typer.run(submit)
