import typer

from cstar.base.feature import is_feature_enabled
from cstar.cli.environment.show import app as app_show

app = typer.Typer()

if is_feature_enabled("CSTAR_FF_CLI_ENV_SHOW"):
    app.add_typer(app_show)
