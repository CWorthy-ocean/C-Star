import argparse
import typing as t

from cstar.cli.core import PathConverterAction, RegistryResult, cli_activity
from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import deserialize


def handle(ns: argparse.Namespace) -> None:
    """The action handler for the blueprint-check action.

    Perform content validation on the blueprint supplied by the user.
    """
    try:
        model = deserialize(ns.path, RomsMarblBlueprint)
        assert model, "Blueprint was not deserialized"
        print(f"{ns.command.capitalize()} is valid")
    except ValueError as ex:
        print(f"Error occurred: {ex}")


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the blueprint-check command into the CLI."""
    command: t.Literal["blueprint"] = "blueprint"
    action: t.Literal["check"] = "check"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a parser for the command: `cstar blueprint check path/to/blueprint.yaml`"""
        parser = sp.add_parser(
            action,
            help="Validate the contents of a blueprint",
            description="Validate the contents of a blueprint.",
        )
        parser.add_argument(
            dest="path",
            help="Path to the blueprint (YAML)",
            action=PathConverterAction,
        )
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn
