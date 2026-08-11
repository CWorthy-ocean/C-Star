import typer

from cstar.base.feature import (
    ENV_FF_DEVELOPER_MODE,
    is_feature_enabled,
)

app = typer.Typer(
    name="admin",
    help="Perform administrative tasks related to your C-Star installation and runs.",
)

if is_feature_enabled(ENV_FF_DEVELOPER_MODE):
    from cstar.cli.admin.clean import app as app_clean

    app.add_typer(app_clean)
