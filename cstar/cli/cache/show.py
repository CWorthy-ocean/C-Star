import typing as t

import typer
from rich.console import Console

from cstar.base.env import ENV_CSTAR_CLI_VERBOSE, ENV_CSTAR_RUNID
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.cli.cache.common import (
    ARG_KEY,
    ARG_RUNID,
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

log = get_logger(__name__)
app = typer.Typer()
console = Console()

command_help: t.Final[str] = "Display metadata about artifacts contained in the cache."


@app.command(
    name="show",
    help=command_help,
)
def show(
    context: typer.Context,
    run_id: t.Annotated[
        str,
        typer.Option(
            ARG_RUNID,
            help=run_id_help,
            callback=cb_pipeline(runid_callback, set_env(ENV_CSTAR_RUNID)),
            min=1,
        ),
    ] = "",
    key: t.Annotated[
        str,
        typer.Option(
            ARG_KEY,
            help=key_help,
            callback=key_callback,
            min=1,
        ),
    ] = "",
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
    """Display metadata about artifacts contained in the cache."""
    cache = get_artifact_cache()

    cached_runs = cache.list_runs()
    if not cached_runs:
        print("There are no cache entries available")
        raise typer.Exit(0)

    if not run_id and not key:
        print("Cache entries for the following runs can be managed:")
        for run in cached_runs:
            print(f"* {run}")

        locations = cache.list_shared_artifacts()
        print("The following shared artifacts were found:")
        for loc in locations:
            msg = f"* {loc.name}"
            if is_flag_enabled(ENV_CSTAR_CLI_VERBOSE):
                msg = f"{msg} (cache path: {loc.path})"
            print(msg)
        raise typer.Exit(0)
        raise typer.Exit(0)

    if run_id and run_id not in cached_runs:
        print(f"The run-id {run_id!r} has no cached entries.")
        raise typer.Exit(0)

    if run_id and not key:
        locations = cache.list_user_artifacts(run_id)
        print(f"The following artifacts were found for run-id {run_id}:")
        for loc in locations:
            msg = f"* {loc.name}"
            if is_flag_enabled(ENV_CSTAR_CLI_VERBOSE):
                msg = f"{msg} (cache path: {loc.path})"
            print(msg)
        raise typer.Exit(0)

    resolve_runid: str | None = None
    if run_id:
        resolve_runid = run_id

    if location := cache.resolve(key, resolve_runid):
        msg = f"Artifact {key!r} is cached at: {location.path}"
        if verbose:
            msg = f"Artifact {key!r} is cached at: {location!r}"
        print(msg)
    else:
        print_not_found(run_id, key)


if __name__ == "__main__":
    app()
