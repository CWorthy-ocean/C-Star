import typer

from cstar.base.feature import (
    ENV_FF_DEVELOPER_MODE,
    is_feature_enabled,
)
from cstar.cli.admin.migrate_outputs import app as app_migrate_outputs

app = typer.Typer(
    name="admin",
    help="Perform administrative tasks related to your C-Star installation and runs.",
)

# migrate-outputs is a user-facing upgrade step, so it is not developer-gated.
app.add_typer(app_migrate_outputs)

if is_feature_enabled(ENV_FF_DEVELOPER_MODE):
    from cstar.cli.admin.clean import app as app_clean

    app.add_typer(app_clean)
