import typer

from cstar.base.feature import is_feature_enabled
from cstar.cli.environment.show import app as app_show

app = typer.Typer(
    name="env",
    help="Manage the environment variables consumed by C-Star.",
)

if is_feature_enabled("CSTAR_FF_CLI_ENV_SHOW"):
    app.add_typer(app_show)
