import typer

from cstar.base.feature import ENV_FF_DEVELOPER_MODE, is_feature_enabled
from cstar.cli.run.check import app as app_check
from cstar.cli.run.show import app as app_show
from cstar.cli.run.verbs import NAME, RUN_LEVEL_VERBS
from cstar.cli.workplan.gather import app as app_gather
from cstar.cli.workplan.log import app as app_log
from cstar.cli.workplan.ls import app as app_ls
from cstar.cli.workplan.path import app as app_path
from cstar.cli.workplan.status import app as app_status

__all__ = ["NAME", "RUN_LEVEL_VERBS", "app"]

app = typer.Typer(
    name=NAME,
    help=(
        "Inspect and manage existing workplan runs. "
        "To launch a run, use `cstar workplan run`."
    ),
)

app.add_typer(app_check)
app.add_typer(app_gather)
app.add_typer(app_log)
app.add_typer(app_path)
app.add_typer(app_status)
app.add_typer(app_ls)
app.add_typer(app_show)

if is_feature_enabled(ENV_FF_DEVELOPER_MODE):
    from cstar.cli.run.schemas import app as app_schemas

    app.add_typer(app_schemas)
