import argparse
import typing as t

from cstar.cli.core import RegistryResult, cli_activity


@cli_activity
def create_command_root() -> RegistryResult:
    """Create the root subparser for workplan commands."""
    command: t.Literal["workplan"] = "workplan"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a subparser to house actions for the command: `cstar workplan`"""
        parser = sp.add_parser(
            command,
            help="Work with custom workplans",
            description="Work with custom workplans",
        )

        wp_subparsers = parser.add_subparsers(
            help="Workplan actions",
            required=True,
            dest="action",
        )
        return wp_subparsers

    return (command, None), _fn
