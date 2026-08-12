import typing as t
from pathlib import Path

import typer

from cstar.base.env import ENV_CSTAR_CLI_VERBOSE
from cstar.base.log import get_logger
from cstar.cli.cache.common import console
from cstar.cli.common import colored, set_flag
from cstar.entrypoint.utils import ARG_VERBOSE, ARG_VERBOSE_HELP
from cstar.io.utils import CACHE_DIR, SHARED_DIR, get_artifact_cache
from cstar.orchestration.artifact_cache import (
    Tier,
)

log = get_logger(__name__)
app = typer.Typer()


command_help: t.Final[str] = "Migrate cache content into a new root directory."
path_help: t.Final[str] = "Path to a new root directory for the cache level."

ARG_TIER: t.Final[str] = "--tier"
ARG_TIER_HELP: t.Final[str] = "Select which cache level to migrate"


def path_callback(context: typer.Context, value: Path) -> Path:
    """Expand and resolve the path."""
    return value.expanduser().resolve()


@app.command(
    name="migrate",
    help=command_help,
)
def migrate(
    path: t.Annotated[
        Path,
        typer.Argument(
            help=path_help,
            callback=path_callback,
            resolve_path=True,
        ),
    ],
    tier: t.Annotated[
        Tier,
        typer.Option(
            ARG_TIER,
            help=ARG_TIER_HELP,
            case_sensitive=False,
        ),
    ] = Tier.USER,
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
    """Promote a user-level cached artifact into the shared, group cache."""
    cache = get_artifact_cache()

    if tier == Tier.USER:
        source = cache.user_root
        target = path / CACHE_DIR
    else:
        source = cache.shared_root
        target = path / SHARED_DIR

    target = source.rename(target)

    msg = f"Cache content for {tier} moved from {str(source)!r} to {str(target)!r}"
    console.print(msg)

    export = f"export CSTAR_DATA_HOME={str(path)!r}"
    msg = f'{colored("WARNING!", "red")} When relocating user-level cache, you must update your shell so it is discoverable, (e.g. "{export}")'
    console.print(msg)


if __name__ == "__main__":
    app()
