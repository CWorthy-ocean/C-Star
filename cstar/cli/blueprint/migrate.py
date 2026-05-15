import typing as t
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Column, Table

from cstar.applications.core import get_application
from cstar.base.env import ENV_CSTAR_CLI_DRY_RUN, ENV_CSTAR_CLI_VERBOSE
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.feature import is_flag_enabled
from cstar.cli.common import cb_pipeline, path_callback, set_env
from cstar.entrypoint.utils import (
    ARG_DRY_RUN,
    ARG_OUTPUT_LONG,
    ARG_OUTPUT_SHORT,
    ARG_VERBOSE,
)
from cstar.execution.file_system import local_copy
from cstar.orchestration.models import Blueprint
from cstar.orchestration.serialization import serialize, validate_serialized_entity
from cstar.system.migration import BlueprintMigration, MigrationPlan

console = Console()
app = typer.Typer()


HELP_SHORT = "Migrate the schema of a blueprint file."
HELP_LONG = f"""\
{HELP_SHORT}

The schema will be updated to the latest available version. If an output
path is not provided, it will be written next to the existing
file with the version number appended to the file name.
"""


def display_summary(bp_path: Path, migration_plan: MigrationPlan) -> None:
    """Display a summary of the migration plan.

    Parameters
    ----------
    bp_path : Path
        The path to the blueprint being migrated.
    migration_plan : MigrationPlan
        Details of the planned migration.
    """
    source, target, plan = migration_plan

    if not is_flag_enabled(ENV_CSTAR_CLI_VERBOSE):
        print(f"Migration from {source!r} to {target!r} will take {len(plan)} steps.")
        return

    source, target, plan = migration_plan
    padding = (0, 1)

    table = Table(
        Column(header="Step", justify="center"),
        Column(header="From", justify="center"),
        Column(header="To", justify="center"),
        title=f"Migration Plan for [yellow]{bp_path.name}[/yellow]",
        show_lines=True,
        padding=padding,
        pad_edge=False,
        row_styles=["", "dim"],
        min_width=40,
        caption=f"Initial Schema: [green]{source}[/green]\nFinal Schema: [red]{target}[/red]",
    )

    for i, adapter in enumerate(plan):
        table.add_row(
            str(i + 1),
            adapter.source(),
            adapter.target(),
        )

    console.print(table)


def migrate_dryrun_callback(ctx: typer.Context, value: bool | None) -> bool | None:
    """Display informational message to user if a parameter conflict is found."""
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
            callback=cb_pipeline(
                set_env(ENV_CSTAR_CLI_DRY_RUN),
                migrate_dryrun_callback,
            ),
            envvar=ENV_CSTAR_CLI_DRY_RUN,
        ),
    ] = False,
    verbose: t.Annotated[
        bool,
        typer.Option(
            ARG_VERBOSE,
            help="Enable printing verbose migration outputs.",
            callback=set_env(ENV_CSTAR_CLI_VERBOSE),
            envvar=ENV_CSTAR_CLI_VERBOSE,
        ),
    ] = False,
) -> None:
    """Migrate the schema of an old blueprint to the latest version."""
    result = validate_serialized_entity(path, Blueprint)
    if result.item is None:
        raise typer.BadParameter(result.error_msg)

    migrator = BlueprintMigration()
    with local_copy(path) as local_path:
        dumped = result.item.model_dump()

        try:
            plan = migrator.plan(dumped)
        except CstarExpectationFailed as ex:
            msg = f"Unable to complete migration plan: {ex}"
            raise typer.BadParameter(msg) from ex

        display_summary(local_path, plan)

        if plan.source == plan.target:
            print(f"The blueprint uses the latest schema ({plan.source})")
            raise typer.Exit(0)

        if dry_run:
            raise typer.Exit(0)

        try:
            updated = migrator.migrate(dumped)
        except CstarExpectationFailed as ex:
            msg = f"Unable to complete migration: {ex}"
            raise typer.BadParameter(msg) from ex
        else:
            print("Migration complete")

        persist_to = (
            Path(f"./{local_path.stem}_{plan.target}{local_path.suffix}")
            if output is None
            else Path(output)
        )
        persist_to = persist_to.expanduser().resolve()

        try:
            bp_type = get_application(result.item.application).blueprint
            updated_bp = bp_type(**updated)
            nbytes = serialize(persist_to, updated_bp)
            assert nbytes, "The migrated blueprint failed to write content"
        except SyntaxError as ex:
            msg = f"Unable to complete migration: {ex}"
            raise typer.BadParameter(msg) from ex
        else:
            print(f"Migrated blueprint persisted to {str(persist_to)!r}")
