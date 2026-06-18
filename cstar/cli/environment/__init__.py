import typer

from cstar.cli.environment.show import app as app_show

app = typer.Typer(
    name="env",
    help="Manage the environment variables consumed by C-Star.",
)

app.add_typer(app_show)
