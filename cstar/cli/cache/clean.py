import typing as t

import typer

from cstar.base.env import ENV_CSTAR_CLI_VERBOSE, ENV_CSTAR_RUNID
from cstar.base.log import get_logger
from cstar.cli.cache.common import (
    ARG_KEY,
    ARG_RUNID,
    ARG_YES,
    confirm_remove,
    confirm_remove_run,
    console,
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


command_help: t.Final[str] = "Manually remove artifacts from the cache."
yes_help: t.Final[str] = "Perform user-level deletions without confirmation."


@app.command(
    name="clean",
    help=command_help,
)
def store(
    key: t.Annotated[
        str,
        typer.Option(
            ARG_KEY,
            help=key_help,
            callback=key_callback,
            min=1,
        ),
    ] = "",
    run_id: t.Annotated[
        str,
        typer.Option(
            ARG_RUNID,
            help=run_id_help,
            callback=cb_pipeline(runid_callback, set_env(ENV_CSTAR_RUNID)),
            min=1,
        ),
    ] = "",
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

    resolve_runid: str | None = None
    if run_id:
        resolve_runid = run_id

    if key:
        if location := cache.resolve(key, resolve_runid):
            confirm = confirm_remove(confirm, location)
            if not confirm:
                console.print("Artifact was not removed")
                raise typer.Exit(0)
        else:
            print_not_found(run_id, key)
            raise typer.Exit(0)
    elif run_id:
        # Gate the deletion on the answer. Computing it and then deleting
        # regardless would discard a refusal, and a whole run is a lot to lose.
        if not confirm_remove_run(run_id, confirm):
            console.print("No artifacts were removed")
            raise typer.Exit(0)
    else:
        console.print("A key or run-id is required")
        raise typer.Exit(2)

    is_removed = False

    try:
        if key and run_id:
            is_removed = cache.delete_user(key, run_id)
        elif key:
            is_removed = cache.delete_shared(key, confirm)
        else:
            is_removed = cache.delete_user_run(run_id)
    except UnsafePathError:
        console.print(
            f"Confirmation required to remove shared artifact with key {key!r}"
        )
    except ArtifactNotFoundError:
        console.print(
            f"Artifact with key {key!r} could not be removed for run {run_id!r}"
        )

    if is_removed:
        msg = (
            f"{key} has been deleted"
            if key
            else f"Artifacts from run {run_id} have been deleted"
        )
        console.print(msg)


if __name__ == "__main__":
    app()
