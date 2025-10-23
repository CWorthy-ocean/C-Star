import argparse
import typing as t

from cstar.cli.core import RegistryResult, cli_activity


def handle(ns: argparse.Namespace) -> None:
    """The action handler for the workplan-run action.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
    print("mock - running the workplan...")

    print(f"Received command: {ns}")


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the workplan-run command into the CLI."""
    command: t.Literal["workplan"] = "workplan"
    action: t.Literal["run"] = "run"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a parser for the command: `cstar workplan run path/to/workplan.yaml`"""
        parser = sp.add_parser(
            "run",
            help="Execute a workplan",
            description="Path to the workplan (YAML)",
        )
        parser.add_argument(
            dest="path",
            help="Path to the workplan (YAML)",
        )
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn
