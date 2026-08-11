import typing as t

import typer
from rich.console import Console

from cstar.base.env import ENV_CSTAR_CLI_VERBOSE, ENV_CSTAR_RUNID
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.cli.cache.common import (
    ARG_KEY,
    ARG_MOVE,
    ARG_PATH,
    ARG_RUNID,
    ARG_YES,
    confirm_overwrite,
    key_callback,
    key_help,
    move_help,
    path_help,
    run_id_help,
    runid_callback,
    yes_help,
)
from cstar.cli.common import (
    cb_pipeline,
    set_env,
    set_flag,
)
from cstar.entrypoint.utils import ARG_VERBOSE, ARG_VERBOSE_HELP
from cstar.io.utils import get_artifact_cache
from cstar.orchestration.artifact_cache import ArtifactExistsError

log = get_logger(__name__)
app = typer.Typer()
console = Console()


command_help: t.Final[str] = "Manually insert an artifact into the cache."


@app.command(
    name="store",
    help=command_help,
)
def store(
    context: typer.Context,
    run_id: t.Annotated[
        str,
        typer.Option(
            ARG_RUNID,
            help=run_id_help,
            callback=cb_pipeline(runid_callback, set_env(ENV_CSTAR_RUNID)),
            min=1,
        ),
    ],
    key: t.Annotated[
        str,
        typer.Option(
            ARG_KEY,
            help=key_help,
            callback=key_callback,
            min=1,
        ),
    ],
    path: t.Annotated[
        str,
        typer.Option(
            ARG_PATH,
            help=path_help,
            resolve_path=True,
        ),
    ],
    overwrite: t.Annotated[
        bool,
        typer.Option(
            ARG_YES,
            help=yes_help,
        ),
    ] = False,
    move: t.Annotated[
        bool,
        typer.Option(
            ARG_MOVE,
            help=move_help,
        ),
    ] = False,
    verbose: t.Annotated[
        bool,
        typer.Option(
            ARG_VERBOSE,
            help=ARG_VERBOSE_HELP,
            callback=set_flag(ENV_CSTAR_CLI_VERBOSE),
            envvar=ENV_CSTAR_CLI_VERBOSE,
        ),
    ] = False,
) -> None:
    """Manually insert an artifact into the cache."""
    cache = get_artifact_cache()
    action = "added to"

    if location := cache.resolve(key, run_id):
        overwrite = confirm_overwrite(overwrite, location)
        action = "updated in"

    try:
        location = cache.ingest(path, key, run_id, move=move, overwrite=overwrite)
    except ArtifactExistsError:
        print(f"An artifact with key {key!r} already exists for run {run_id!r}")
    else:
        msg = f"{path} has been {action} the cache"
        if is_flag_enabled(ENV_CSTAR_CLI_VERBOSE):
            msg = f"{msg} at {str(location.path)!r}"
        print(msg)


if __name__ == "__main__":
    app()
