import argparse
import typing as t

from cstar.cli.core import RegistryResult, cli_activity


@cli_activity
def create_command_root() -> RegistryResult:
    """Create the root subparser for template commands."""
    command: t.Literal["template"] = "template"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a subparser to house actions for the command: `cstar template *`"""
        parser = sp.add_parser(
            command,
            help="Create blueprint and workplan templates",
            description="Create blueprint and workplan templates",
        )

        wp_subparsers = parser.add_subparsers(
            help="Template actions",
            required=True,
            dest="action",
        )
        return wp_subparsers

    return (command, None), _fn
