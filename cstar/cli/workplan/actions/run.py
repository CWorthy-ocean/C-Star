import argparse
import os
import typing as t
from pathlib import Path

from cstar.cli.core import RegistryResult, cli_activity
from cstar.orchestration.dag_runner import build_and_run_dag


async def handle(ns: argparse.Namespace) -> None:
    """The action handler for the workplan-run action.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
    # TODO: load from ~/.cstar/config (e.g. cstar config init)
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "shared"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    os.environ["CSTAR_RUNID"] = ns.name

    await build_and_run_dag(Path(ns.path))
    print(f"Completed handling command: {ns}")


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the workplan-run command into the CLI.

    Returns
    -------
    RegistryResult
        A 2-tuple containing ((command name, action name), parser function)
    """
    command: t.Literal["workplan"] = "workplan"
    action: t.Literal["run"] = "run"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a parser for the command: `cstar workplan run path/to/workplan.yaml`"""
        parser = sp.add_parser(
            action,
            help="Execute a workplan",
            description="Path to the workplan (YAML)",
        )
        parser.add_argument(
            "-n",
            "--name",
            help="Unique name used to identify the run",
        )
        parser.add_argument(
            dest="path",
            help="Path to the workplan (YAML)",
        )
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn
