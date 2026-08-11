import typing as t

import typer
from rich.console import Console

from cstar.base.env import ENV_CSTAR_CLI_VERBOSE, ENV_CSTAR_RUNID
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.cli.cache.common import (
    ARG_KEY,
    ARG_RUNID,
    ARG_YES,
    confirm_remove,
    key_callback,
    key_help,
    print_not_found,
    run_id_help,
    runid_callback,
)
from cstar.cli.common import (
    cb_pipeline,
    set_env,
    set_flag,
)
from cstar.entrypoint.utils import ARG_VERBOSE, ARG_VERBOSE_HELP
from cstar.io.utils import get_artifact_cache
from cstar.orchestration.artifact_cache import ArtifactNotFoundError, UnsafePathError

log = get_logger(__name__)
app = typer.Typer()
console = Console()


command_help: t.Final[str] = "Manually remove artifacts from the cache."
yes_help: t.Final[str] = "Perform user-level deletions without confirmation."


@app.command(
    name="clean",
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
    confirm: t.Annotated[
        bool,
        typer.Option(
            ARG_YES,
            help=yes_help,
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
    """Manually remove artifacts from the cache."""
    cache = get_artifact_cache()

    if location := cache.resolve(key, run_id):
        confirm = confirm_remove(confirm, location)
        if not confirm:
            print("Artifact was not removed")
            raise typer.Exit(0)
    else:
        print_not_found(run_id, key)
        raise typer.Exit(0)

    is_removed = False

    try:
        if run_id:
            is_removed = cache.delete_user(key, run_id)
        else:
            is_removed = cache.delete_shared(key, confirm)
    except UnsafePathError:
        print("Confirmation required to remove shared artifact with key {key!r}")
    except ArtifactNotFoundError:
        print(f"Artifact with key {key!r} could not be removed for run {run_id!r}")

    if is_removed:
        msg = f"{key} has been deleted"
        if is_flag_enabled(ENV_CSTAR_CLI_VERBOSE):
            msg = f"{msg} from the {location.tier} cache"
        print(msg)


if __name__ == "__main__":
    app()
