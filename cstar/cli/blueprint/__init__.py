import typer

from cstar.cli.blueprint.check import app as app_check
from cstar.cli.blueprint.run import app as app_run

app = typer.Typer()

app.add_typer(app_run)
app.add_typer(app_check)
