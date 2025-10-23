import argparse
import typing as t

from cstar.cli.core import PathConverterAction, RegistryResult, cli_activity
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize


def handle(ns: argparse.Namespace) -> None:
    """The action handler for the workplan-check action.

    Perform content validation on the workplan supplied by the user.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
    try:
        model = deserialize(ns.path, Workplan)
        assert model, "Model was not deserialized"
        print(f"{ns.command.capitalize()} is valid")
    except ValueError as ex:
        print(f"Error occurred: {ex}")


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the workplan-check command into the CLI."""
    command: t.Literal["workplan"] = "workplan"
    action: t.Literal["check"] = "check"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a parser for the command: `cstar workplan check path/to/workplan.yaml`"""
        parser = sp.add_parser(
            action,
            help="Validate the contents of a workplan",
            description="Validate the contents of a workplan.",
        )
        parser.add_argument(
            dest="path",
            help="Path to the workplan (YAML)",
            action=PathConverterAction,
        )
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn
