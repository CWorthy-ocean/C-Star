import typer

from cstar.base.feature import is_feature_enabled
from cstar.cli.workplan.check import app as app_check
from cstar.cli.workplan.generate import app as app_gen
from cstar.cli.workplan.plan import app as app_plan
from cstar.cli.workplan.run import app as app_run
from cstar.cli.workplan.status import app as app_status

app = typer.Typer()

app.add_typer(app_check)

if is_feature_enabled("CLI_WORKPLAN_GEN"):
    app.add_typer(app_gen)

if is_feature_enabled("CLI_WORKPLAN_PLAN"):
    app.add_typer(app_plan)

app.add_typer(app_run)

if is_feature_enabled("CLI_WORKPLAN_STATUS"):
    app.add_typer(app_status)
