import functools
import hashlib
import json
import os
import sys
import typing as t
import warnings
from collections.abc import Callable
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

import typer
from rich.console import Console

import cstar
from cstar.applications.core import get_application
from cstar.base.deprecation import CstarDeprecationWarning
from cstar.base.env import (
    ENV_CSTAR_DISABLE_MIGRATION,
    ENV_CSTAR_LOG_LEVEL,
    REMOVED_SETTINGS,
    load_user_config,
)
from cstar.base.exceptions import CstarExpectationFailed, CstarUserConfigError
from cstar.base.feature import is_flag_enabled
from cstar.base.log import LogLevelChoices, get_logger, reset_log_level
from cstar.base.utils import slugify
from cstar.execution.file_system import (
    DirectoryManager,
    get_backup_path,
    is_remote_resource,
    local_copy,
    write_bytes_atomic,
)
from cstar.orchestration.formatting import (
    format_validation_errors as format_validation_errors,  # re-export
)
from cstar.orchestration.models import BlueprintCore
from cstar.orchestration.serialization import (
    anchor_blueprint_file,
    anchor_locations,
    model_to_yaml,
    validate_serialized_entity,
)
from cstar.system.migration import (
    BlueprintMigration,
    CstarManualMigrationError,
    CstarMigrationError,
    CstarSchemaTooNewError,
    CstarUnsupportedMigrationError,
    MigrateResult,
    MigrationPlan,
    MigrationRequest,
    OnMigratedCallback,
    OnPlannedCallback,
)

if t.TYPE_CHECKING:
    from pydantic import BaseModel

app = typer.Typer()
console = Console()
log = get_logger(__name__)

HELP_SHORT = (
    "Print the cstar executable location, the C-Star version, the versions of "
    "installed companion packages, and the newest ucla-roms release this C-Star "
    "knows, then exit."
)


SCHEMA_ERRORS: t.Final[tuple[type[CstarMigrationError], ...]] = (
    CstarSchemaTooNewError,
    CstarManualMigrationError,
    CstarUnsupportedMigrationError,
)
"""Errors raised while planning a migration that no CLI command can recover from."""

StrCallback: t.TypeAlias = Callable[[typer.Context, str], str]


def colored(msg: str, color: str = "cyan") -> str:
    """Wrap a message in rich markup applying the given color.

    Parameters
    ----------
    msg : str
        The message to decorate.
    color : str
        The rich color name to apply.

    Returns
    -------
    str
    """
    return f"[{color}]{msg}[/{color}]"


def italic(msg: str) -> str:
    """Wrap a message in rich markup applying an italic style.

    Parameters
    ----------
    msg : str
        The message to decorate.

    Returns
    -------
    str
    """
    return f"[italic]{msg}[/italic]"


def checkmark(color: str) -> str:
    """Create a checkmark glyph in the given color.

    Parameters
    ----------
    color : str
        The rich color name to apply.

    Returns
    -------
    str
    """
    return colored(":heavy_check_mark:", color)


def present(prompt: str, value: str, color: str = "cyan", width: int = 0) -> str:
    """Format a prompt and its colored value for display.

    Parameters
    ----------
    prompt : str
        The text labeling the value.
    value : str
        The value to display.
    color : str
        The rich color name applied to the value.
    width : int
        The minimum width the prompt is right-justified within.

    Returns
    -------
    str
    """
    return f"{prompt.rjust(width)}: {colored(value, color)}"


def label(name: str, app: str | None) -> str:
    """Format an entity name, including its application when available.

    Parameters
    ----------
    name : str
        The name of the entity.
    app : str | None
        The name of the application associated with the entity.

    Returns
    -------
    str
    """
    return f"{name} ({italic(app)})" if app else name


def id_label(id: int, name: str, app: str | None) -> str:
    """Format an entity name prefixed with its numeric identifier.

    Parameters
    ----------
    id : int
        The numeric identifier of the entity.
    name : str
        The name of the entity.
    app : str | None
        The name of the application associated with the entity.

    Returns
    -------
    str
    """
    return f"{id}. {label(name, app)}"


def version_callback(value: bool) -> bool:
    """Print the executable location, installed package versions and the newest
    ucla-roms release this C-Star knows, then exit.
    """
    if value:
        typer.echo(f"cstar executable location: {Path(sys.argv[0]).resolve()}")
        typer.echo(f"C-Star version: {cstar.__version__}")
        # roms-tools is optional in a given environment -- show it only if installed.
        for pkg in ("roms-tools",):
            try:
                typer.echo(f"{pkg} version: {_pkg_version(pkg)}")
            except PackageNotFoundError:
                pass
        # Imported here: the namelist models are not needed to start the CLI.
        from cstar.roms.namelist import NEWEST_KNOWN_UCLA_ROMS, format_roms_version

        typer.echo(
            "Newest known ucla-roms release: "
            f"{format_roms_version(NEWEST_KNOWN_UCLA_ROMS)}"
        )
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
    """Load the user's configuration file, then warn on stderr about each
    exported setting that no longer has an effect.

    A deprecation is reported by its log line alone: the Python warning that
    `deprecated` also emits would repeat it, with a source line, on the terminal.

    The file is loaded before any command's options are parsed, so an option's
    ``envvar`` binding sees it; an exported variable still wins over it. A file
    the loader refuses (one it cannot read) stops every command except the ones
    that manage it, ``cstar init`` and ``cstar env``.

    Raises
    ------
    typer.Exit
        With code 2 when the loader refuses the configuration file.
    """
    warnings.simplefilter("ignore", CstarDeprecationWarning)
    try:
        load_user_config()
    except CstarUserConfigError as ex:
        from cstar.cli.environment import NAME as NAME_ENV
        from cstar.cli.init import COMMAND_INIT

        typer.echo(str(ex), err=True)
        if ctx.invoked_subcommand not in {NAME_ENV, COMMAND_INIT}:
            raise typer.Exit(2) from ex

    for name in sorted(REMOVED_SETTINGS.keys() & os.environ.keys()):
        typer.echo(f"{name} has no effect; {REMOVED_SETTINGS[name].guidance}", err=True)


def exit_on_refused_settings() -> None:
    """Stop a command that a `refused` removed setting used to change.

    The commands such a setting used to change call this before they touch
    anything: running them while it is exported, or named in the configuration
    file (which skips it), would do more than was asked (a dry run becoming a
    real one).

    Raises
    ------
    typer.Exit
        With code 2 when a refused setting is exported or in the file.
    """
    config = load_user_config()
    named = os.environ.keys() | config.skipped.keys()
    refused = [
        name
        for name in sorted(REMOVED_SETTINGS.keys() & named)
        if REMOVED_SETTINGS[name].refused
    ]
    if refused:
        msg = (
            f"Refusing to run while {', '.join(refused)} is exported or set in "
            f"{config.path}; remove it to continue."
        )
        typer.echo(msg, err=True)
        raise typer.Exit(2)


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


WORKPLAN_FILE_SUFFIXES: t.Final[frozenset[str]] = frozenset({".yaml", ".yml"})
"""Suffixes that identify a workplan file, which `cstar workplan check` validates."""


def refuse_workplan_file(_context: typer.Context, target: str) -> str:
    """Refuse a workplan file: a run command acts on a run, not a workplan.

    Parameters
    ----------
    _context : typer.Context
        The typer context (unused).
    target : str
        The user-supplied target.

    Returns
    -------
    str

    Raises
    ------
    typer.BadParameter
        Raised when the target names a YAML file.
    """
    if Path(target).suffix.casefold() in WORKPLAN_FILE_SUFFIXES:
        msg = (
            f"{target!r} is a workplan file, not a run. "
            f"To validate it, use `cstar workplan check {target}`."
        )
        raise typer.BadParameter(msg)

    return target


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


def schema_message(path: str | Path, ex: CstarMigrationError) -> str:
    """Format a schema-planning error for display, prefixed with the blueprint path.

    Parameters
    ----------
    path : str | Path
        The path to the blueprint that could not be planned.
    ex : CstarMigrationError
        The error raised by the migration planner.

    Returns
    -------
    str
    """
    return f"Blueprint {str(path)!r}: {ex}"


def report_schema_error(path: str | Path, ex: CstarMigrationError) -> t.NoReturn:
    """Print a schema-planning error with the blueprint path and exit the command.

    Parameters
    ----------
    path : str | Path
        The path to the blueprint that could not be planned.
    ex : CstarMigrationError
        The error raised by the migration planner.
    """
    console.print(schema_message(path, ex), soft_wrap=True, markup=False)
    raise typer.Exit(1) from ex


def get_migrator(
    application: str,
    on_planned: OnPlannedCallback | None = None,
    on_migrated: OnMigratedCallback | None = None,
) -> BlueprintMigration:
    """Build the migrator for an application's blueprints.

    Parameters
    ----------
    application : str
        The name of the application the blueprint belongs to.
    on_planned : OnPlannedCallback | None
        Callback executed when a migration plan is complete.
    on_migrated : OnMigratedCallback | None
        Callback executed when a migration is complete.

    Returns
    -------
    BlueprintMigration
    """
    app_def = get_application(application)
    return BlueprintMigration(
        adapters=app_def.migrations or [],
        targets={app_def.name: app_def.schema_version},
        on_planned=on_planned,
        on_migrated=on_migrated,
    )


def on_planned_callback(
    bp_path: Path, plan: MigrationPlan, verbose: bool = False
) -> None:
    """Display a summary of a migration plan.

    Only called for plans that require adapters.

    Parameters
    ----------
    bp_path : Path
        The path to the blueprint being migrated.
    plan : MigrationPlan
        Details of the planned migration.
    verbose : bool
        Display every migration step in a table instead of a one-line summary.
    """
    if not verbose:
        num_steps = len(plan.adapters)
        msg = f"Migrating {plan.source!r}->{plan.target!r} in {num_steps} step(s)."
        console.print(msg)
        return

    from rich.table import Column, Table

    source, target, adapters = plan
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

    for i, adapter in enumerate(adapters):
        table.add_row(
            str(i + 1),
            adapter.source(),
            adapter.target(),
        )

    console.print(table)


def on_migrated_callback(plan: MigrationPlan) -> None:
    """Display a notification that the migration is complete.

    Parameters
    ----------
    plan : MigrationPlan
        Details of the completed migration.
    """
    console.print(f"Migration from {plan.source!r}->{plan.target!r} is complete.")


def get_persist_to(request: MigrationRequest, plan: MigrationPlan) -> Path:
    """Determine the persistence path for a migrated model.

    If a target is not supplied by the user, write to the `CSTAR_STATE_HOME`
    directory.

    The resulting filename follows the convention:
        <original_stem>-<digest>_<latest_version>.<ext>

    where `<digest>` is taken from the source's resolved path and content, so
    sources sharing a filename in different directories never share a copy,
    an edited source never replaces the copy a running step may still read,
    and re-migrating an unchanged source reuses its copy.

    Example:
    - Input: foo.yaml with any prior version, latest == 3.0.0
    - Output: foo-1a2b3c4d5e6f_3.0.0.yaml

    Parameters
    ----------
    request : MigrationRequest
        The request naming the source file and optional user-supplied target.
    plan : MigrationPlan
        The migration plan providing the target schema version.

    Returns
    -------
    Path
        The path where the migrated model will be persisted.
    """
    if request.target is not None:
        output = request.target
    else:
        source = request.source.resolve()
        digest = hashlib.sha256(str(source).encode() + b"\0" + source.read_bytes())
        name = f"{source.stem}-{digest.hexdigest()[:12]}_{plan.target}{source.suffix}"
        output = DirectoryManager.state_home() / name

    return output.expanduser().resolve()


def persist_migration(request: MigrationRequest, result: MigrateResult) -> Path:
    """Serialize the migrated entity to disk.

    For an in-place migration, the original source file is preserved in a
    backup file before it is overwritten. A migrated copy written to a different
    directory than the source (`CSTAR_STATE_HOME` by default) has its relative
    `location` values anchored to the source's directory first, so they keep
    naming the same files.

    Parameters
    ----------
    request : MigrationRequest
        Parameters passed to the migrator.
    result : MigrateResult
        The result of the executed migration.

    Returns
    -------
    Path
        The path to the persisted entity file.

    Raises
    ------
    ValueError
        If the result does not contain a migration plan.
    typer.BadParameter
        If the migrated content cannot be serialized, or a relative location
        would resolve differently once the copy is written.
    """
    if result.plan is None:
        msg = "Unable to persist an unplanned migration"
        raise ValueError(msg)

    persist_to = get_persist_to(request, result.plan)

    try:
        bp_type = get_application(result.application).blueprint
        migrated = result.migrated
        source_dir = request.source.expanduser().resolve().parent
        if persist_to.expanduser().resolve().parent != source_dir:
            migrated = anchor_locations(migrated, source_dir)
        updated_bp = bp_type(**migrated)
        if request.in_place:
            backup_path = get_backup_path(request.source)
            backup_path.write_bytes(request.source.read_bytes())

        content = model_to_yaml(updated_bp).encode()
        assert content, "The migrated blueprint failed to serialize"
        # atomic, so a concurrent run of the same source never reads half a file
        write_bytes_atomic(persist_to, content)
    except (SyntaxError, CstarExpectationFailed) as ex:
        msg = f"Unable to complete migration: {ex}"
        raise typer.BadParameter(msg) from ex

    return persist_to


def execute_migration(
    request: MigrationRequest, verbose: bool = False
) -> PersistedMigrateResult:
    """Execute the schema migration for a blueprint.

    Parameters
    ----------
    request : MigrationRequest
        Parameters to pass to the migrator.
    verbose : bool
        Display every step of a planned migration instead of a summary.

    Returns
    -------
    PersistedMigrateResult
        Named tuple containing the migration result and path where it was
        persisted; a target matching the source indicates no change occurred.

    Raises
    ------
    typer.BadParameter
        If the blueprint fails content validation.
    typer.Exit
        If the planned migration fails to complete, or the blueprint requires
        migration but migration is disabled via `CSTAR_DISABLE_MIGRATION`.
    CstarSchemaTooNewError
        If the blueprint's schema is newer than this build reads (see `VersionedDocument`).
    CstarManualMigrationError
        If the blueprint's schema has no automatic migration to this build's.
    CstarUnsupportedMigrationError
        If the application or the schema version is not recognized.
    """
    validation_result = validate_serialized_entity(request.source, BlueprintCore)
    if validation_result.item is None:
        raise typer.BadParameter(validation_result.error_msg)

    dumped = validation_result.item.model_dump()
    migrator = get_migrator(
        validation_result.item.application,
        on_planned=functools.partial(
            on_planned_callback, request.source, verbose=verbose
        ),
        on_migrated=on_migrated_callback,
    )

    plan = migrator.plan(dumped)

    if request.dry_run or plan.is_compatible:
        log.debug("Short-circuiting migration after planning")
        result = MigrateResult(dumped, dumped, plan=plan)
        # result with target == source indicates no change occurred
        return PersistedMigrateResult(result, request.source)

    try:
        result = migrator.migrate(dumped, plan)
    except CstarMigrationError as ex:
        msg = f"Unable to complete migration: {ex}"
        result = MigrateResult(dumped, {}, plan=plan, error=msg)

    if result.error:
        console.print(result.error)
        raise typer.Exit(1)

    if plan.adapters and is_flag_enabled(ENV_CSTAR_DISABLE_MIGRATION):
        console.print(
            f"Blueprint at '{request.source}' requires schema migration from "
            f"{colored(plan.source, 'green')} to "
            f"{colored(plan.target, 'red')}, but migration is "
            f"disabled ({ENV_CSTAR_DISABLE_MIGRATION}=1). Unset "
            f"{ENV_CSTAR_DISABLE_MIGRATION} to allow migration, or update the "
            "blueprint to the current schema."
        )
        raise typer.Exit(1)

    persisted_to = persist_migration(request, result)

    return PersistedMigrateResult(result, persisted_to)


def localize_and_migrate(path: str, verbose: bool = False) -> tuple[Path, bool]:
    """Copy the blueprint locally and auto-migrate its schema if necessary.

    Parameters
    ----------
    path : str
        The path to the blueprint.
    verbose : bool
        Display every step of a planned migration instead of a summary.

    A blueprint downloaded from a URL has no directory of its own, so the
    relative locations in its local copy are anchored to the working directory,
    where they have always been read from, before any later step re-reads the copy.

    Returns
    -------
    tuple[Path, bool]
        Tuple containing:
        - Path to the localized blueprint (the migrated copy when one was created)
        - True if the blueprint was migrated and persisted to a new location,
          meaning references to the original path must be updated. False when
          the blueprint was left untouched (compatible with this build, or
          dry-run).

    Raises
    ------
    CstarSchemaTooNewError
        If the blueprint's schema is newer than this build reads (see `VersionedDocument`).
    CstarManualMigrationError
        If the blueprint's schema has no automatic migration to this build's.
    CstarUnsupportedMigrationError
        If the application or the schema version is not recognized.
    typer.BadParameter
        If executing a planned migration fails.
    """
    with local_copy(path) as local_path:
        if is_remote_resource(path):
            anchor_blueprint_file(local_path, Path.cwd())

        request = MigrationRequest(path=local_path)
        is_migrated = False

        try:
            persist_result = execute_migration(request, verbose=verbose)

            if persist_result.migration_result.error:
                console.print(persist_result.migration_result.error)
                raise typer.Exit(1)

            # a migration occurred only when the blueprint was persisted to a
            # new location; up-to-date and dry-run requests return the source
            is_migrated = Path(persist_result.target) != local_path
            local_path = Path(persist_result.target)
        except SCHEMA_ERRORS:
            # the caller reports these with the blueprint path
            raise
        except CstarMigrationError as ex:
            msg = f"Migration failed for {path!r}"
            log.exception(msg)
            raise typer.BadParameter(msg) from ex
        return local_path, is_migrated


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


def try_get_from_ctxmap(
    context: typer.Context, key: str, klass: type[_TValue]
) -> _TValue | None:
    """Retrieve a strongly-typed value from the typer context when it holds one.

    Unlike `get_from_ctxmap`, an absent key is not an error.

    Parameters
    ----------
    context : typer.Context
        The typer context object.
    key : str
        The key the value is stored under.
    klass : type[_TValue]
        The type the value must have.

    Returns
    -------
    _TValue | None
        The value, or `None` when the key is absent or holds `None`.

    Raises
    ------
    typer.Exit
        With code 102 when the value is not a `klass`.
    """
    value = (context.obj or {}).get(key)
    if value is None:
        return None

    if not isinstance(value, klass):
        log.warning(
            "Context map contains value with type mismatch. "
            f"Expected {klass.__name__!r} but received {value.__class__.__name__!r}."
        )
        raise typer.Exit(102)

    return value


def set_ctxmap(context: typer.Context, key: str, value: object) -> None:
    """Prepare a mapping in the typer context and store the supplied value at
    the chosen key.

    Parameters
    ----------
    context : typer.Context
        The typer context object.
    key : str
        The key the value is stored under.
    value : object
        The value to store in the context map.
    """
    if context.obj is None:
        context.obj = {}

    context_map: dict[str, t.Any] = context.obj

    if key in context_map and context_map[key] is not None:
        console.print(f"Value in context map using key {key!r} will be overwritten")

    context_map[key] = value


def write_model_schema(model: "type[BaseModel]", path: Path) -> Path:
    """Write the JSON schema of a model to a file, creating its directory.

    Parameters
    ----------
    model : type[BaseModel]
        The model to generate the schema of.
    path : Path
        The file to write.

    Returns
    -------
    Path
        The file written.

    Raises
    ------
    typer.Exit
        Raised when nothing was written.
    """
    content = json.dumps(model.model_json_schema(), indent=4)
    path.parent.mkdir(parents=True, exist_ok=True)

    if not path.write_text(f"{content}\n"):
        print("Empty schema write occurred")
        raise typer.Exit(1)

    return path
