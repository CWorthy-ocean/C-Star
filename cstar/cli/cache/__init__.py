import typer

from cstar.base.feature import (
    ENV_FF_CLI_MANAGE_CACHE,
    is_feature_enabled,
)

app = typer.Typer(
    name="cache",
    help="Perform administrative tasks related to your C-Star artifact cache",
)


if is_feature_enabled(ENV_FF_CLI_MANAGE_CACHE):
    from cstar.cli.cache.promote import app as app_promote

    app.add_typer(app_promote)
