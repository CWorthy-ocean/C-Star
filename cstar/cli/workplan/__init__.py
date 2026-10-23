import copy
import typing as t

import typer

from cstar.cli.workplan.check import app as app_check
from cstar.cli.workplan.gather import app as app_gather
from cstar.cli.workplan.log import app as app_log
from cstar.cli.workplan.ls import app as app_ls
from cstar.cli.workplan.path import app as app_path
from cstar.cli.workplan.run import ALIAS as ALIAS_SUBMIT
from cstar.cli.workplan.run import HELP_LONG as HELP_SUBMIT_LONG
from cstar.cli.workplan.run import HELP_SHORT as HELP_SUBMIT_SHORT
from cstar.cli.workplan.run import app as app_run
from cstar.cli.workplan.run import submit
from cstar.cli.workplan.status import app as app_status

NAME: t.Final[str] = "workplan"
"""Name under which this subcommand tree is attached."""
ALIAS = "wp"
"""Short alias under which this subcommand tree is also attached."""

app = typer.Typer(
    name=NAME,
    help=(
        f"Validate workplans and submit them. (alias: {ALIAS})\n\n"
        "To inspect or manage a run once it is submitted, use "
        "`cstar run <command>`."
    ),
)


def _add_hidden(sub_app: typer.Typer) -> None:
    """Register every command of `sub_app` on the group, hidden from `--help`.

    The run-level verbs are listed under `cstar run`; their `cstar workplan`
    spellings keep working unlisted. Each is the same command (callback, options
    and help) as under `cstar run`. `add_typer` without a name would merge the
    commands listed, ignoring `hidden`, so each command is copied instead.
    """
    for info in sub_app.registered_commands:
        hidden = copy.copy(info)
        hidden.hidden = True
        app.registered_commands.append(hidden)


app.add_typer(app_check)
_add_hidden(app_gather)
_add_hidden(app_log)
_add_hidden(app_path)
app.add_typer(app_run)
# `run` is the earlier name of `submit`: the same command under a second name,
# hidden from `--help` so the listing shows a single entry. It is registered here
# rather than in its module so that the launch sub-app keeps a single command.
app.command(
    name=ALIAS_SUBMIT,
    hidden=True,
    help=HELP_SUBMIT_LONG,
    short_help=HELP_SUBMIT_SHORT,
)(submit)
_add_hidden(app_status)
_add_hidden(app_ls)
