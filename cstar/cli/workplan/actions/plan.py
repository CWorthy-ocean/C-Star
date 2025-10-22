import argparse
import typing as t
from pathlib import Path

from cstar.cli.core import PathConverterAction, RegistryResult, cli_activity
from cstar.orchestration.models import Workplan
from cstar.orchestration.planning import GraphPlanner
from cstar.orchestration.serialization import deserialize


def handle(ns: argparse.Namespace) -> None:
    """Generate the execution plan for a workplan."""
    plan_path: Path | None = None

    try:
        if workplan := deserialize(ns.path, Workplan):
            planner = GraphPlanner(workplan)
            plan_path = planner.render(
                planner.graph,
                planner.color_map,
                planner.name_map,
                workplan.name,
                Path.cwd(),
            )
        else:
            print(f"The workplan at `{ns.path}` could not be loaded")

    except ValueError as ex:
        print(f"Error occurred: {ex}")

    if plan_path is None:
        raise ValueError("Unable to generate plan")

    print(f"The plan has been generated and stored at: {plan_path}")


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the workplan-plan command into the CLI."""
    command: t.Literal["workplan"] = "workplan"
    action: t.Literal["plan"] = "plan"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a parser for the command: `cstar workplan plan -o ouput/directory`"""
        parser = sp.add_parser(
            action,
            help="Review the execution plan for the Workplan",
            description="Path to the workplan (YAML)",
        )
        parser.add_argument(
            dest="path",
            help="Path to the workplan (YAML)",
            action=PathConverterAction,
        )
        parser.add_argument(
            "-o",
            "--output_dir",
            help="Directory to write plan outputs to.",
            default=Path.cwd(),
            dest="output_dir",
            action=PathConverterAction,
        )
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn
