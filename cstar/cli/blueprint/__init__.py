import typer

from cstar.base.feature import ENV_FF_CLI_BP_MIGRATE_SHOW, is_feature_enabled
from cstar.cli.blueprint.check import app as app_check
from cstar.cli.blueprint.migrate import app as app_migrate
from cstar.cli.blueprint.run import app as app_run

app = typer.Typer(
    name="blueprint",
    help="Perform the validation and execution of blueprints.",
)

app.add_typer(app_run)
app.add_typer(app_check)

if is_feature_enabled(ENV_FF_CLI_BP_MIGRATE_SHOW):
    app.add_typer(app_migrate)
