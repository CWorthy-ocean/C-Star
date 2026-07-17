import typer

from cstar.base.feature import (
    ENV_FF_CLI_BP_MIGRATE_SHOW,
    ENV_FF_DEVELOPER_MODE,
    is_feature_enabled,
)
from cstar.cli.blueprint.check import app as app_check
from cstar.cli.blueprint.run import app as app_run

app = typer.Typer(
    name="blueprint",
    help="Perform the validation and execution of blueprints.",
)

app.add_typer(app_run)
app.add_typer(app_check)

if is_feature_enabled(ENV_FF_CLI_BP_MIGRATE_SHOW):
    from cstar.cli.blueprint.migrate import app as app_migrate

    app.add_typer(app_migrate)

if is_feature_enabled(ENV_FF_DEVELOPER_MODE):
    from cstar.cli.blueprint.schemas import app as app_schemas

    app.add_typer(app_schemas)
