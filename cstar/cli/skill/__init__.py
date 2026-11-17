import typer

from cstar.cli.skill.install import app as app_install
from cstar.cli.skill.ls import app as app_ls
from cstar.cli.skill.path import app as app_path
from cstar.cli.skill.verbs import NAME

__all__ = ["NAME", "app"]

app = typer.Typer(
    name=NAME,
    help="List and install the agent skills C-Star ships for coding agents.",
)

app.add_typer(app_ls)
app.add_typer(app_install)
app.add_typer(app_path)
