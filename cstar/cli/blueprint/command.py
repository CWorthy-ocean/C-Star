import argparse
import typing as t

from cstar.cli.core import RegistryResult, cli_activity


@cli_activity
def create_command_root() -> RegistryResult:
    """Create the root subparser for blueprint commands."""
    command: t.Literal["blueprint"] = "blueprint"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a subparser to house actions for the command: `cstar blueprint`"""
        parser = sp.add_parser(
            command,
            help="Work with custom blueprints",
            description="Work with custom blueprints",
        )

        subparsers = parser.add_subparsers(
            help="Blueprint actions",
            required=True,
            dest="action",
        )
        return subparsers

    return (command, None), _fn
