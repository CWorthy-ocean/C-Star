import typing as t

import typer

from cstar.cli.workplan.check import app as app_check
from cstar.cli.workplan.gather import app as app_gather
from cstar.cli.workplan.log import app as app_log
from cstar.cli.workplan.ls import app as app_ls
from cstar.cli.workplan.path import app as app_path
from cstar.cli.workplan.run import app as app_run
from cstar.cli.workplan.status import app as app_status

NAME: t.Final[str] = "workplan"
"""Name under which this subcommand tree is attached."""
ALIAS = "wp"
"""Short alias under which this subcommand tree is also attached."""

app = typer.Typer(
    name=NAME,
    help=(
        f"Perform validation and execution of workplans. (alias: {ALIAS})\n\n"
        "The commands that inspect an existing run are also available as "
        "`cstar run <command>`."
    ),
)


app.add_typer(app_check)
app.add_typer(app_gather)
app.add_typer(app_log)
app.add_typer(app_path)
app.add_typer(app_run)
app.add_typer(app_status)
app.add_typer(app_ls)
