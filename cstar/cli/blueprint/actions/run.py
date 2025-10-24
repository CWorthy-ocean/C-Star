import argparse
import typing as t
from pathlib import Path

from cstar.cli.core import PathConverterAction, RegistryResult, cli_activity


async def run_worker(path: Path) -> None:
    """Execute a blueprint synchronously using the worker service.

    Parameters:
    -----------
    path : Path
        The path to the blueprint to execute
    """
    print("running blocking worker...")


async def run_worker_process(path: Path) -> None:
    """Execute a non-blocking blueprint in an external process.

    Parameters:
    -----------
    path : Path
        The path to the blueprint to execute
    """
    print("running non-blocking worker...")


async def handle(ns: argparse.Namespace) -> None:
    """The action handler for the blueprint-run action."""
    # TODO: consider just letting the worker do it's own validation instead.
    # handle_check(ns)

    print(f"Received command: {ns}")

    if ns.blocking:
        await run_worker(ns.path)
    else:
        await run_worker_process(ns.path)


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the blueprint-run command into the CLI.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
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
            action=PathConverterAction,
        )
        parser.add_argument(
            "-b",
            "--blocking",
            action="store_true",
        )
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn
