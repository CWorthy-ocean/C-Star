import typer

from cstar.cli.environment.config_file import app_set, app_unset
from cstar.cli.environment.register_kernel import app as app_register_kernel
from cstar.cli.environment.shell_init import app as app_shell_init
from cstar.cli.environment.show import app as app_show

NAME = "env"
"""Name of the `cstar env` command group."""

app = typer.Typer(
    name=NAME,
    help="Manage the environment variables consumed by C-Star.",
)

app.add_typer(app_show)
app.add_typer(app_set)
app.add_typer(app_unset)
app.add_typer(app_register_kernel)
app.add_typer(app_shell_init)
