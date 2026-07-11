import typer

from cstar.base.feature import (
    ENV_FF_DEVELOPER_MODE,
    is_feature_enabled,
)
from cstar.cli.admin.clean import app as app_clean

app = typer.Typer(
    name="admin",
    help="Perform administrative tasks related to your C-Star installation and runs.",
)

if is_feature_enabled(ENV_FF_DEVELOPER_MODE):
    app.add_typer(app_clean)
