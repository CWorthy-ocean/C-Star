import typer

from cstar.cli.system.add import app as app_add
from cstar.cli.system.check import app as app_check
from cstar.cli.system.ls import app as app_ls
from cstar.cli.system.show import app as app_show
from cstar.cli.system.template import app as app_template

NAME = "system"
"""Name of the `cstar system` command group."""

app = typer.Typer(
    name=NAME,
    help="Describe and check the systems (machines) C-Star can run on.",
)

app.add_typer(app_ls)
app.add_typer(app_show)
app.add_typer(app_check)
app.add_typer(app_add)
app.add_typer(app_template)
