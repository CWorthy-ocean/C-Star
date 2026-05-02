import typing as t
from pathlib import Path

import typer

from cstar.base.env import ENV_CSTAR_CLI_DRY_RUN, ENV_CSTAR_CLI_VERBOSE
from cstar.base.exceptions import CstarExpectationFailed
from cstar.cli.common import dryrun_callback, verbose_callback
from cstar.cli.workplan.shared import get_registered_bp
from cstar.entrypoint.utils import (
    ARG_DRY_RUN,
    ARG_OUTPUT_LONG,
    ARG_OUTPUT_SHORT,
    ARG_VERBOSE,
)
from cstar.execution.file_system import is_remote_resource, local_copy
from cstar.orchestration.models import Blueprint
from cstar.orchestration.serialization import (
    serialize,
    validate_serialized_entity,
)
from cstar.system.migration import Migration, RomsBlueprintMigration

app = typer.Typer()


HELP_SHORT = "Migrate the schema of a blueprint file."
HELP_LONG = f"""\
{HELP_SHORT}

The schema will be updated to the latest available version. If an output
path is not provided, it will be written next to the existing
file with the version number appended to the file name.
"""


def path_callback(value: str | None) -> str | None:
    """Ensure a path that was provided by a user has been expanded and resolved.

    Returns
    -------
    Path
    """
    if value and not is_remote_resource(value):
        return Path(value).expanduser().resolve().as_posix()
    return value


def migrate_dryrun_callback(ctx: typer.Context, value: bool | None) -> bool | None:
    """Display informational message to user if a parameter conflict is found."""
    value = dryrun_callback(ctx, value)

    output: str | None = ctx.params.get("output", None)

    if value is not None and value and output is not None:
        print(f"Ignoring output path {output!r} during dry-run")

    return value


@app.command(name="migrate", help=HELP_LONG, short_help=HELP_SHORT)
def migrate(
    path: t.Annotated[
        str,
        typer.Argument(
            help="Path to a blueprint file.",
            callback=path_callback,
        ),
    ],
    output: t.Annotated[
        str | None,
        typer.Option(
            ARG_OUTPUT_LONG,
            ARG_OUTPUT_SHORT,
            help="Path to the output file",
            callback=path_callback,
        ),
    ] = None,
    dry_run: t.Annotated[
        bool,
        typer.Option(
            ARG_DRY_RUN,
            help="Generate the migration plan without executing it.",
            callback=migrate_dryrun_callback,
            envvar=ENV_CSTAR_CLI_DRY_RUN,
        ),
    ] = False,
    verbose: t.Annotated[
        bool,
        typer.Option(
            ARG_VERBOSE,
            help="Enable printing verbose migration outputs.",
            callback=verbose_callback,
            envvar=ENV_CSTAR_CLI_VERBOSE,
        ),
    ] = False,
) -> None:
    """Migrate the schema of an old blueprint to the latest version."""
    result = validate_serialized_entity(path, Blueprint)
    application: str = "unknown"
    if result.item is None:
        raise typer.BadParameter(result.error_msg)
    else:
        application = result.item.application

    migrators: list[type[Migration]] = [RomsBlueprintMigration]
    klass = next(filter(lambda x: x.application == application, migrators))

    migrator = klass()
    with local_copy(path) as local_path:
        dumped = result.item.model_dump()

        try:
            source, target, plan = migrator.plan(dumped)
        except CstarExpectationFailed as ex:
            msg = f"Unable to complete migration plan: {ex}"
            raise typer.BadParameter(msg) from ex

        print(f"Migration from {source!r} to {target!r} will take {len(plan)} steps.")

        try:
            updated = migrator.adapt(dumped)
        except CstarExpectationFailed as ex:
            msg = f"Unable to complete migration: {ex}"
            raise typer.BadParameter(msg) from ex
        else:
            print("Migration complete")

        persist_to = (
            Path(f"./{local_path.stem}_{target}{local_path.suffix}")
            if output is None
            else Path(output)
        )
        persist_to = persist_to.expanduser().resolve()

        try:
            bp_type = get_registered_bp(result.item.application)
            updated_bp = bp_type(**updated)
            nbytes = serialize(persist_to, updated_bp)
            assert nbytes, "The migrated blueprint failed to write content"
        except SyntaxError as ex:
            msg = f"Unable to complete migration: {ex}"
            raise typer.BadParameter(msg) from ex
        else:
            print(f"Migrated blueprint persisted to {str(persist_to)!r}")
