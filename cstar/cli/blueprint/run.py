import asyncio
import typing as t
from pathlib import Path

import typer
from pydantic import ValidationError

from cstar.applications.core import (
    RunnerRequest,
    get_app_for_blueprint,
)
from cstar.base.env import (
    ENV_CSTAR_CLI_VERBOSE,
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_LOG_LEVEL,
    get_env_item,
)
from cstar.base.feature import ENV_FF_CLI_BP_MIGRATE_AUTO, is_feature_enabled
from cstar.base.log import LogLevelChoices, get_logger
from cstar.cli.common import (
    MigrationRequest,
    cb_pipeline,
    execute_migration,
    format_validation_errors,
    set_env,
    set_flag,
    update_loggers,
)
from cstar.entrypoint.config import get_job_config, get_service_config
from cstar.entrypoint.utils import (
    ARG_CLOBBER,
    ARG_CLOBBER_HELP,
    ARG_DIRECTIVES_URI_LONG,
    ARG_DIRECTIVES_URI_SHORT,
    ARG_LOGLEVEL_HELP,
    ARG_LOGLEVEL_LONG,
    ARG_LOGLEVEL_SHORT,
    ARG_VERBOSE,
    ARG_VERBOSE_HELP,
)
from cstar.execution.file_system import local_copy
from cstar.orchestration.serialization import deserialize, validate_serialized_entity
from cstar.orchestration.transforms import DirectiveConfig
from cstar.system.migration import CStarMigrationNotRegisteredError

if t.TYPE_CHECKING:
    ...


CMD_NAME: t.Final[str] = "run"
CMD_HELP: t.Final[str] = "Execute a blueprint in a local worker service."

app = typer.Typer()
log = get_logger(__name__)


def path_callback(
    ctx: typer.Context,
    path: str,
) -> str:
    """Validate the blueprint content after typer has parsed the path.

    Additionally, loads the blueprint, performs automatic schema migration
    if necessary, and stores the updated blueprint in the context for later use.

    Parameters
    ----------
    ctx : typer.Context
        The context of the command.
    path : str
        The path to the blueprint.

    Returns
    -------
    str
        The path to the blueprint (or the newly migrated blueprint file).
    """
    try:
        with local_copy(path) as local_path:
            bp_path = local_path

            if is_feature_enabled(ENV_FF_CLI_BP_MIGRATE_AUTO):
                request = MigrationRequest(path=local_path)
                try:
                    persist_result = execute_migration(request)

                    if persist_result.migration_result.error:
                        print(persist_result.migration_result.error)
                        raise typer.Exit(1)

                    bp_path = Path(persist_result.target)
                except CStarMigrationNotRegisteredError:
                    log.info("Skipping schema migration; no registered adapters")
            return str(bp_path)

    except FileNotFoundError as ex:
        msg = f"Blueprint file not found: {ex.filename}"
        raise typer.BadParameter(msg) from ex
    except ValidationError as ex:
        errors = format_validation_errors(ex)
        msg = f"Blueprint {path!r} is invalid. Details: {errors}"
        raise typer.BadParameter(msg) from ex

    return path


def directives_callback(path: str | None) -> str | None:
    """Validate the directive content after typer has parsed the path.

    Parameters
    ----------
    path : str
        The path to the directive file to validate.

    Returns
    -------
    str.
    """
    if path is None:
        return path

    try:
        with local_copy(path) as local_path:
            if not local_path.exists():
                msg = f"Directive file not found at path: {path}"
                raise typer.BadParameter(msg)

            # deserialize content to validate
            _ = deserialize(path, DirectiveConfig)

    except FileNotFoundError as ex:
        msg = f"Directive file not found: {path}"
        raise typer.BadParameter(msg) from ex
    except (ValueError, ValidationError) as ex:
        msg = f"Directive file {path!r} is malformed: {ex}"
        raise typer.BadParameter(msg) from ex

    return path


@app.command(name=CMD_NAME, help=CMD_HELP)
def run(
    ctx: typer.Context,
    uri: t.Annotated[
        str,
        typer.Argument(
            help="The URI (or path) to the blueprint to execute",
            callback=path_callback,
        ),
    ],
    log_level: t.Annotated[
        LogLevelChoices,
        typer.Option(
            ARG_LOGLEVEL_LONG,
            ARG_LOGLEVEL_SHORT,
            callback=cb_pipeline(set_env(ENV_CSTAR_LOG_LEVEL), update_loggers),
            help=ARG_LOGLEVEL_HELP,
            envvar=ENV_CSTAR_LOG_LEVEL,
        ),
    ] = LogLevelChoices.INFO,
    clobber: t.Annotated[
        bool,
        typer.Option(
            ARG_CLOBBER,
            callback=set_flag(ENV_CSTAR_CLOBBER_WORKING_DIR),
            help=ARG_CLOBBER_HELP,
            envvar=ENV_CSTAR_CLOBBER_WORKING_DIR,
        ),
    ] = False,
    directive_uri: t.Annotated[
        str | None,
        typer.Option(
            ARG_DIRECTIVES_URI_LONG,
            ARG_DIRECTIVES_URI_SHORT,
            help="The URI (or path) to a file containing directive configuration to be applied.",
            callback=directives_callback,
        ),
    ] = None,
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
    """Execute a blueprint in a local worker service."""
    bp_path = Path(uri)
    app_config = get_app_for_blueprint(bp_path)

    name = f"{app_config.name}_runner"
    job_cfg = get_job_config()
    service_cfg = get_service_config(get_env_item(ENV_CSTAR_LOG_LEVEL).value, name=name)

    msg = f"Executing {app_config.name!r} blueprint in a {type(app_config.runner).__name__} service"
    log.debug(msg)

    result = validate_serialized_entity(bp_path, app_config.blueprint)
    if not result.is_valid:
        print(result.error_msg)
        return

    if directive_uri:
        uri = DirectiveConfig.apply_directives(directive_uri, uri)

    request = RunnerRequest(uri, app_config.blueprint)

    runner = app_config.runner(request, service_cfg, job_cfg)
    asyncio.run(runner.execute())

    if runner.result and runner.result.errors:
        print(f"Errors occurred: {', '.join(runner.result.errors)}")

    rc = len(runner.result.errors) if runner.result else 0

    if rc:
        print("Blueprint execution failed")
        raise typer.Exit(code=rc)

    print("Blueprint execution completed")


if __name__ == "__main__":
    typer.run(run)
