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
    from cstar.cli.cache.clean import app as app_clean
    from cstar.cli.cache.promote import app as app_promote
    from cstar.cli.cache.show import app as app_show
    from cstar.cli.cache.store import app as app_store

    app.add_typer(app_clean)
    app.add_typer(app_promote)
    app.add_typer(app_show)
    app.add_typer(app_store)
