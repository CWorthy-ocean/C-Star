import functools
import os
import sys
import typing as t
from collections.abc import Callable
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

import typer
from pydantic import ValidationError

import cstar
from cstar.applications.core import get_application
from cstar.base.env import (
    ENV_CSTAR_CLI_VERBOSE,
    ENV_CSTAR_DISABLE_MIGRATION,
    ENV_CSTAR_LOG_LEVEL,
    FLAG_ON,
)
from cstar.base.feature import is_flag_enabled
from cstar.base.log import LogLevelChoices, get_logger, reset_log_level
from cstar.base.utils import slugify
from cstar.execution.file_system import DirectoryManager, is_remote_resource, local_copy
from cstar.orchestration.models import BlueprintCore
from cstar.orchestration.serialization import (
    PersistenceMode,
    serialize,
    validate_serialized_entity,
)
from cstar.system.migration import (
    BlueprintMigration,
    CStarMigrationNotRegisteredError,
    MigrateResult,
    MigrationPlan,
    MigrationRequest,
)

app = typer.Typer()
log = get_logger(__name__)

HELP_SHORT = (
    "Print the cstar executable location, the C-Star version, and the versions "
    "of installed companion packages, then exit."
)


BoolCallback: t.TypeAlias = Callable[[typer.Context, bool], bool]
StrCallback: t.TypeAlias = Callable[[typer.Context, str], str]


def version_callback(value: bool) -> bool:
    """Print the executable location and installed package versions, then exit."""
    if value:
        typer.echo(f"cstar executable location: {Path(sys.argv[0]).resolve()}")
        typer.echo(f"C-Star version: {cstar.__version__}")
        # cstar-forge / roms-tools are optional in a given environment -- show
        # each only if installed.
        for pkg in ("cstar-forge", "roms-tools"):
            try:
                typer.echo(f"{pkg} version: {_pkg_version(pkg)}")
            except PackageNotFoundError:
                pass
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


def normalize_runid(_context: typer.Context, run_id: str) -> str:
    """Normalize a user-supplied run-id (slugify) so the same identifier is
    used everywhere (directories, tracking records, environment).

    Empty values are returned unchanged so callers can apply their own
    presence/default handling.

    Parameters
    ----------
    _context : typer.Context
        The typer context (unused).
    run_id : str
        The run-id value received from the user.

    Returns
    -------
    str
    """
    if not (run_id := run_id.strip()):
        return run_id

    normalized = slugify(run_id)
    if normalized != run_id:
        msg = f"Normalized run-id `{run_id}` to `{normalized}`"
        log.debug(msg)

    return normalized


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


def path_callback(value: str | None) -> str | None:
    """Ensure a path that was provided by a user has been expanded and resolved.

    Returns
    -------
    Path
    """
    if value and not is_remote_resource(value):
        return Path(value).expanduser().resolve().as_posix()
    return value


class PersistedMigrateResult(t.NamedTuple):
    migration_result: MigrateResult
    target: str | Path


def on_planned_callback(bp_path: Path, plan: MigrationPlan) -> None:
    """Display a summary of the migration plan.

    Parameters
    ----------
    bp_path : Path
        The path to the blueprint being migrated.
    migration_plan : MigrationPlan
        Details of the planned migration.
    """
    if not is_flag_enabled(ENV_CSTAR_CLI_VERBOSE) or not plan.adapters:
        if plan.is_latest:
            print(f"No migration needed for schema {plan.source!r} in {str(bp_path)!r}")
            return

        num_steps = len(plan.adapters)
        msg = f"Migrating {plan.source!r}->{plan.target!r} in {num_steps} step(s)."
        print(msg)
        return

    from rich.console import Console  # noqa: PLC0415
    from rich.table import Column, Table  # noqa: PLC0415

    source, target, adapters = plan
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

    for i, adapter in enumerate(adapters):
        table.add_row(
            str(i + 1),
            adapter.source(),
            adapter.target(),
        )

    console.print(table)


def on_migrated_callback(plan: MigrationPlan) -> None:
    print(f"Migration from {plan.source!r}->{plan.target!r} is complete.")


def get_persist_to(source: Path, target: Path | None, plan: MigrationPlan) -> Path:
    """Determine the persistence path for a migrated model.

    If a target is not supplied by the user, write to the `CSTAR_STATE_HOME`
    directory.

    The resulting filename follows the convention:
        &lt;original_stem&gt;_&lt;latest_version&gt;.&lt;ext&gt;

    Example:
    - Input: foo.yaml with any prior version, latest == 3.0.0
    - Output: foo_3.0.0.yaml

    Parameters
    ----------
    source : Path
        Path to the file containing the original, serialized model.
    target : Path | None
        The user-supplied path
    """
    if target is not None:
        output = target
    else:
        stem = source.stem
        suffix = source.suffix
        state_dir = DirectoryManager.state_home()
        output = state_dir / f"{stem}_{plan.target}{suffix}"

    return output.expanduser().resolve()


def persist_migration(request: MigrationRequest, result: MigrateResult) -> Path:
    """Serialize the migrated entity to disk.

    Returns
    -------
    Path
        The path to the persisted entity file.
    """
    if result.plan is None:
        msg = "Unable to persist an unplanned migration"
        raise ValueError(msg)

    persist_to = get_persist_to(request.source, request.target, result.plan)

    try:
        bp_type = get_application(result.application).blueprint
        updated_bp = bp_type(**result.migrated)

        nbytes = serialize(
            persist_to,
            updated_bp,
            mode=PersistenceMode.auto,
        )
        assert nbytes, "The migrated blueprint failed to write content"
    except SyntaxError as ex:
        msg = f"Unable to complete migration: {ex}"
        raise typer.BadParameter(msg) from ex

    return persist_to


def execute_migration(request: MigrationRequest) -> PersistedMigrateResult:
    """Execute the schema migration for a blueprint.

    Parameters
    ----------
    request : MigrationRequest
        Parameters to pass to the migrator.

    Returns
    -------
    PersistedMigrationResult
        Named tuple containing the migration result and path where it was persisted.

    Raises
    ------
    CStarMigrationNotRegisteredError
        If there are no registered migrations for the requested schema.
    typer.Exit
        If the blueprint requires migration but migration is disabled via
        `CSTAR_DISABLE_MIGRATION`.
    """
    validation_result = validate_serialized_entity(request.source, BlueprintCore)
    if validation_result.item is None:
        raise typer.BadParameter(validation_result.error_msg)

    dumped = validation_result.item.model_dump()
    app_name = validation_result.item.application
    app_def = get_application(app_name)
    adapters = app_def.migrations or []

    if not adapters:
        msg = f"No schema adapters are registered for {app_def.name!r}"
        raise CStarMigrationNotRegisteredError(msg)

    migrator = BlueprintMigration(
        adapters=adapters,
        on_planned=functools.partial(on_planned_callback, request.source),
        on_migrated=on_migrated_callback,
    )

    if request.dry_run():
        migrator.plan(dumped)
        raise typer.Exit(0)

    migration_result = migrator.plan_and_migrate(dumped)
    if migration_result.error:
        print(migration_result.error)
        raise typer.Exit(1)

    if not migration_result.plan:
        print("Migration failed to produce a plan.")
        raise typer.Exit(2)

    # An up-to-date blueprint needs no migration: return it untouched unless
    # the user explicitly requested an output copy.
    if not migration_result.plan.adapters and request.target is None:
        return PersistedMigrateResult(migration_result, request.source)

    if migration_result.plan.adapters and is_flag_enabled(ENV_CSTAR_DISABLE_MIGRATION):
        from rich.console import Console  # noqa: PLC0415

        Console().print(
            f"Blueprint at '{request.source}' requires schema migration from "
            f"[green]{migration_result.plan.source}[/green] to "
            f"[red]{migration_result.plan.target}[/red], but migration is "
            f"disabled ({ENV_CSTAR_DISABLE_MIGRATION}=1). Unset "
            f"{ENV_CSTAR_DISABLE_MIGRATION} to allow migration, or update the "
            "blueprint to the current schema."
        )
        raise typer.Exit(1)

    persisted_to = persist_migration(request, migration_result)

    return PersistedMigrateResult(migration_result, persisted_to)


def localize_and_migrate(path: str) -> Path:
    """Copy the blueprint locally and auto-migrate its schema if necessary.

    Parameters
    ----------
    path : str
        The path to the blueprint.

    Returns
    -------
    str
        The path to the local blueprint (or the newly migrated blueprint file).
    """
    with local_copy(path) as local_path:
        request = MigrationRequest(path=local_path)
        try:
            persist_result = execute_migration(request)

            if persist_result.migration_result.error:
                print(persist_result.migration_result.error)
                raise typer.Exit(1)

            local_path = Path(persist_result.target)
        except CStarMigrationNotRegisteredError:
            log.debug("Skipping schema migration; no registered adapters")
        return local_path


def format_validation_errors(ex: ValidationError) -> str:
    """Display the contents of a validation error in a user-friendly format."""
    messages: list[str] = []

    for error in ex.errors():
        msg = f"{error['msg']!r}"

        if "loc" in error and error["loc"]:
            msg = "`Invalid {} value ({}): {}`".format(
                error["loc"][0],
                error["input"],
                error["msg"],
            )
        messages.append(msg)

    return ", ".join(messages)


_TValue = t.TypeVar("_TValue")


def get_from_ctxmap(context: typer.Context, key: str, klass: type[_TValue]) -> _TValue:
    """Retrieve a strongly-typed value from the typer context from the supplied key."""
    context_map: dict[str, t.Any] | None = context.obj

    if not context_map:
        log.error("Unable to retrieve value from empty context map")
        raise typer.Exit(100)

    if key not in context_map:
        items = ", ".join(context_map.keys())
        log.warning(
            f"Unable to retrieve key {key!r} from context. Available data: {items}"
        )

    value: _TValue | None = context.obj[key]
    if value is None:
        log.debug(f"Context map contains null value for key: {key}")
        raise typer.Exit(101)

    if not isinstance(value, klass):
        log.warning(
            "Context map contains value with type mismatch. "
            f"Expected {klass.__name__!r} but received {value.__class__.__name__!r}."
        )
        raise typer.Exit(102)

    return value


def set_ctxmap(context: typer.Context, key: str, value: object) -> None:
    """Prepare a mapping in the typer context and store the supplied value at the chosen key."""
    if context.obj is None:
        context.obj = {}

    context_map: dict[str, t.Any] = context.obj

    if key in context_map and context_map[key] is not None:
        print(f"Value in context map using key {key!r} will be overwritten")

    context_map[key] = value
