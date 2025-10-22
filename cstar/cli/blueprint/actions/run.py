import argparse
import typing as t

from cstar.cli.core import RegistryResult, cli_activity


def handle(ns: argparse.Namespace) -> None:
    """The action handler for the blueprint-run action."""
    print("mock - running the blueprint...")

    print(f"Received command: {ns}")


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the blueprint-run command into the CLI."""
    command: t.Literal["blueprint"] = "blueprint"
    action: t.Literal["run"] = "run"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a parser for the command: `cstar blueprint run path/to/blueprint.yaml`"""
        parser = sp.add_parser(
            "run",
            help="Execute a blueprint",
            description="Path to the blueprint (YAML)",
        )
        parser.add_argument(
            dest="path",
            help="Path to the blueprint (YAML)",
        )
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn
