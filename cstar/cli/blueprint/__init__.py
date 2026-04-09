import typer

from cstar.cli.blueprint.check import app as app_check
from cstar.cli.blueprint.run import app as app_run

app = typer.Typer(
    name="blueprint",
    help="Perform the validation and execution of blueprints.",
)

app.add_typer(app_run)
app.add_typer(app_check)
