import importlib
import os
import typing as t
from collections.abc import Callable
from pathlib import Path

import typer

import cstar
from cstar.applications.core import get_application
from cstar.base.env import (
    ENV_CSTAR_CLI_VERBOSE,
    ENV_CSTAR_LOG_LEVEL,
    FLAG_ON,
)
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.feature import is_flag_enabled
from cstar.base.log import LogLevelChoices, reset_log_level
from cstar.execution.file_system import local_copy
from cstar.orchestration.models import Blueprint
from cstar.orchestration.serialization import serialize, validate_serialized_entity
from cstar.system.migration import (
    BlueprintMigration,
    CstarMigrationError,
    MigrationPlan,
    MigrationResult,
)

app = typer.Typer()

HELP_SHORT = "Print the current version of the C-Star package and exit."


BoolCallback: t.TypeAlias = Callable[[typer.Context, bool], bool]
StrCallback: t.TypeAlias = Callable[[typer.Context, str], str]


def version_callback(value: bool) -> bool:
    """Print the version of C-Star and exit."""
    if value:
        typer.echo(cstar.__version__)
        raise typer.Exit(0)

    return value


def common_callback(
    ctx: typer.Context,
    version: t.Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            is_eager=True,
            callback=version_callback,
            help=HELP_SHORT,
        ),
    ] = False,
) -> None:
    pass


P = t.ParamSpec("P")
A = t.TypeVar("A")


def cb_pipeline(
    *callbacks: Callable[[typer.Context, A], A],
) -> Callable[[typer.Context, A], A]:
    """Create a typer Callback function composed of a series of individual callbacks.

    Parameters
    ----------
    callbacks : Callable[[typer.Context, A], A]
        The sequence of callbacks to be executed

    Returns
    -------
    Callable[[typer.Context, A], A]
    """

    def _callback(ctx: typer.Context, value: A) -> A:
        """Callback wrapper that executes all specified callbacks."""
        for callback in callbacks:
            value = callback(ctx, value)

        return value

    return _callback


def set_flag(
    key: str,
    extra: BoolCallback | None = None,
) -> BoolCallback:
    """Create a typer Callback function that sets the value of an environment
    variable to the value of the argument supplied by the user.

    Parameters
    ----------
    key : str
        The environment variable to be set
    extra : StrCallback
        Additional logic called after setting the environment variable.

    Returns
    -------
    StrCallback
    """

    def _callback(ctx: typer.Context, value: bool) -> bool:
        """Callback that sets the specified environment variable to `FLAG_ON`
        if `value` is `True`.
        """
        if value:
            os.environ[key] = FLAG_ON

        if extra:
            value = extra(ctx, value)

        return value

    return _callback


def set_env(
    key: str,
    extra: StrCallback | None = None,
) -> StrCallback:
    """Create a typer Callback function that sets the value of an environment
    variable to the value of the argument supplied by the user.

    Parameters
    ----------
    key : str
        The environment variable to be set
    extra : StrCallback
        Additional logic called after setting the environment variable.

    Returns
    -------
    StrCallback
    """

    def _callback(ctx: typer.Context, value: str) -> str:
        """Callback that sets the specified environment variable to the specified value."""
        value = value.strip()
        os.environ[key] = value
        if extra:
            value = extra(ctx, value)
        return value

    return _callback


def update_loggers(ctx: typer.Context, value: str) -> str:
    """Perform a log-level reset on all loggers if the log level is updated via CLI."""
    reset_log_level(LogLevelChoices[value])
    return value


log_level_callback = cb_pipeline(set_env(ENV_CSTAR_LOG_LEVEL), update_loggers)


def locate_app_modules() -> list[str]:
    """Return a list of absolute import strings where the module matches the
    application module naming standard of ending with `app.py`
    (e.g. cstar/applications/hello_app.py).

    Returns
    -------
    str
        The absolute import path, e.g. `cstar.applications.hello_app`
    """
    root = Path(__file__).parent.parent  # <path>/cstar
    location = root / "applications"
    apps = [
        f.relative_to(root.parent) for f in location.rglob("*app.py") if f.is_file()
    ]
    return [str(f.with_suffix("")).replace("/", ".") for f in apps]


def autoimport_apps(module_names: list[str]) -> None:
    for module_name in module_names:
        importlib.import_module(module_name)


def display_migration_plan(bp_path: Path, migration_plan: MigrationPlan) -> None:
    """Display a summary of the migration plan.

    Parameters
    ----------
    bp_path : Path
        The path to the blueprint being migrated.
    migration_plan : MigrationPlan
        Details of the planned migration.
    """
    from rich.console import Console  # noqa: PLC0415
    from rich.table import Column, Table  # noqa: PLC0415

    source, target, plan = migration_plan
    padding = (0, 1)
    console = Console()

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


def execute_migration(
    path: str,
    output: str | None = None,
    dry_run: bool = False,
) -> MigrationResult:
    """Execute the schema migration for a blueprint.

    Parameters
    ----------
    path : str
        The path to the blueprint file.
    output : str | None
        The path to the output file.
    dry_run : bool
        If True, simulate the migration without persisting changes.

    Returns
    -------
    MigrationResult
        NamedTuple containing the migrated blueprint and the persistence path.
    """
    result = validate_serialized_entity(path, Blueprint)
    if result.item is None:
        raise typer.BadParameter(result.error_msg)

    migrator = BlueprintMigration()
    with local_copy(path) as local_path:
        dumped = result.item.model_dump()

    try:
        plan = migrator.plan(dumped)
    except (CstarExpectationFailed, CstarMigrationError) as ex:
        msg = f"Unable to complete migration plan: {ex}"
        raise typer.BadParameter(msg) from ex

    # The summary/latest-check logic must be inside the local_copy context
    # to ensure the local file is available if needed by the summary function.
    if plan.source == plan.target:
        print(f"The blueprint uses the latest schema ({plan.source})")
        bp_type = get_application(result.item.application).blueprint
        return MigrationResult(bp_type(**dumped), None)

    if is_flag_enabled(ENV_CSTAR_CLI_VERBOSE):
        display_migration_plan(local_path, plan)
    else:
        num_steps = len(plan.adapters)
        print(f"Migrating {plan.source!r}->{plan.target!r} in {num_steps} steps.")

    if dry_run:
        bp_type = get_application(result.item.application).blueprint
        return MigrationResult(bp_type(**dumped), None)

    try:
        updated = migrator.migrate(dumped)
    except (CstarExpectationFailed, CstarMigrationError) as ex:
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

    return MigrationResult(updated_bp, persist_to)
