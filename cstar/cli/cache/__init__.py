import typer

from cstar.base.feature import (
    ENV_FF_CACHE,
    is_feature_enabled,
)

app = typer.Typer(
    name="cache",
    help="Inspect and manage the C-Star artifact cache.",
)

if is_feature_enabled(ENV_FF_CACHE):
    from cstar.cli.cache.commands import app as app_commands

    app.add_typer(app_commands)
