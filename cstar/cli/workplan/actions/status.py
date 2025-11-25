import argparse
import os
import typing as t
from pathlib import Path

from cstar.cli.core import PathConverterAction, RegistryResult, cli_activity
from cstar.orchestration.dag_runner import load_dag_status


async def handle(ns: argparse.Namespace) -> None:
    """The action handler for the workplan-status action.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "shared"
    os.environ["CSTAR_ORCHESTRATED"] = "1"
    os.environ["CSTAR_RUNID"] = ns.name

    await load_dag_status(Path(ns.path))


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the workplan-plan command into the CLI.

    Returns
    -------
    RegistryResult
        A 2-tuple containing ((command name, action name), parser function)
    """
    command: t.Literal["workplan"] = "workplan"
    action: t.Literal["status"] = "status"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a parser for the command: `cstar workplan status -n run01`"""
        parser = sp.add_parser(
            action,
            help="Review the current status of a running workplan",
            description="Path to the workplan (YAML)",
        )
        parser.add_argument(
            "-n",
            "--name",
            help="Unique name used to identify the run",
            required=True,
        )
        parser.add_argument(
            dest="path",
            help="Path to the workplan (YAML)",
            action=PathConverterAction,
        )
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn


# if __name__ == "__main__":
#     NS = t.NamedTuple("ns", [("name", str), ("path", str)])
#     ns: NS = NS(name="run109", path="/home/x-cmcbride/workplans/fanout.yaml")
#     asyncio.run(handle(ns))  # type: ignore
