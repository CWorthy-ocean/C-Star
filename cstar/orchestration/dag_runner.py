import argparse
import asyncio
import os
import sys
import typing as t
from datetime import datetime, timezone
from itertools import cycle
from pathlib import Path

from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import (
    Launcher,
    Orchestrator,
    Planner,
    RunMode,
)
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import Transform, get_transform


def incremental_delays() -> t.Generator[float, None, None]:
    """Return a value from an infinite cycle of incremental delays.

    Returns
    -------
    Generator[float]
    """
    delays = [0.1, 1, 2, 5, 15, 30]

    if os.getenv("CSTAR_ORCHESTRATION_DELAYS", ""):
        try:
            custom_delays = os.getenv("CSTAR_ORCHESTRATION_DELAYS", "")
            delays = [float(d) for d in custom_delays.split(",")]
        except ValueError:
            print(f"Malformed delay provided: {custom_delays}. Using defaults.")

    delay_cycle = cycle(delays)
    yield from delay_cycle


def display_summary(
    open_set: t.Iterable[str] | None,
    closed_set: t.Iterable[str] | None,
    orchestrator: Orchestrator,
) -> None:
    print("The remaining steps in the plan are: ")
    if open_set:
        for node in open_set:
            print(f"\t- {node}")
    else:
        print("\t[N/A]")

    print("The completed steps in the plan are: ")
    if closed_set:
        for node in closed_set:
            print(f"\t- {node}")
    else:
        print("\t[N/A]")


async def retrieve_run_progress(orchestrator: Orchestrator) -> None:
    """Load the run state.

    Parameters
    ----------
    orchestrator : Orchestrator
        The orchestrator to be used for processing a plan.
    mode : RunMode
        The execution mode during processing.

        - RunMode.Schedule submits all processes in the plan in a non-blocking manner.
        - RunMode.Monitor waits for all processes in the plan to complete.
    """
    mode = RunMode.Monitor
    closed_set = orchestrator.get_closed_nodes(mode=mode)
    open_set = orchestrator.get_open_nodes(mode=mode)

    # Run through all the tasks until we're caught up
    while open_set is not None:
        await orchestrator.run(mode=mode)

        closed_set = orchestrator.get_closed_nodes(mode=mode)
        open_set = orchestrator.get_open_nodes(mode=mode)

    display_summary(open_set, closed_set, orchestrator)


async def load_dag_status(path: Path) -> None:
    """Determine the current status of the workplan.

    Parameters
    ----------
    path : Path
        The path to the blueprint being executed.
    """
    wp = deserialize(path, Workplan)
    print(f"Loading status of workplan: {wp.name}")

    planner = Planner(workplan=wp)
    launcher: Launcher = SlurmLauncher()
    orchestrator = Orchestrator(planner, launcher)

    await retrieve_run_progress(orchestrator)


async def process_plan(orchestrator: Orchestrator, mode: RunMode) -> None:
    """Execute a plan from start to finish.

    Parameters
    ----------
    orchestrator : Orchestrator
        The orchestrator to be used for processing a plan.
    mode : RunMode
        The execution mode during processing.

        - RunMode.Schedule submits all processes in the plan in a non-blocking manner.
        - RunMode.Monitor waits for all processes in the plan to complete.
    """
    closed_set = orchestrator.get_closed_nodes(mode=mode)
    open_set = orchestrator.get_open_nodes(mode=mode)
    delay_iter = iter(incremental_delays())

    while open_set is not None:
        print(f"[on-enter::{mode}] Open nodes: {open_set}, Closed: {closed_set}")
        await orchestrator.run(mode=mode)

        curr_closed = orchestrator.get_closed_nodes(mode=mode)
        curr_open = orchestrator.get_open_nodes(mode=mode)

        if curr_closed != closed_set or curr_open != curr_open:
            # reset to initial delay when a task is found or completed
            delay_iter = iter(incremental_delays())

        open_set = curr_open
        closed_set = curr_closed

        print(f"[on-exit::{mode}] Open nodes: {open_set}, Closed: {closed_set}")

        sleep_duration = next(delay_iter)
        print(f"Sleeping for {sleep_duration:4.1f} seconds before next {mode}.")
        await asyncio.sleep(sleep_duration)

    print(f"Workplan {mode} is complete.")


def transform_workplan(wp: Workplan) -> Workplan:
    """Create a new workplan with appropriate transforms applied.

    Parameters
    ----------
    wp : Workplan
        The workplan to transform.

    Returns
    -------
    Workplan
        The transformed workplan.
    """
    steps = []
    for step in wp.steps:
        transform: Transform | None = get_transform(step.application)
        if not transform:
            steps.append(step)
            continue

        transformed = list(transform(step))
        steps.extend(transformed)

        tweaks = [s for s in wp.steps if step.name in s.depends_on]
        for tweak in tweaks:
            tweak.depends_on.remove(step.name)
            tweak.depends_on.append(transformed[-1].name)

    wp_attrs = wp.model_dump()
    wp_attrs.update({"steps": steps})

    return Workplan(**wp_attrs)


def persist_workplan(wp: Workplan, source_path: Path) -> Path:
    """Persist a transformed workplan to a file.

    Parameters
    ----------
    wp : Workplan
        The workplan to transform.
    source_path : Path
        The path to the original workplan file.

    Returns
    -------
    tuple[Workplan, Path]
        The transformed workplan and the path where it has been written.
    """
    persist_path = source_path.with_stem(f"{source_path.stem}_transformed")
    persist_path.write_text(wp.model_dump_json())

    return persist_path


# @flow(log_prints=True)
async def build_and_run_dag(path: Path) -> None:
    """Execute the steps in the workplan.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute
    """
    wp = deserialize(path, Workplan)
    print(f"Executing workplan: {wp.name}")
    original_wp = wp

    wp = transform_workplan(wp)
    _ = persist_workplan(wp, path)

    if original_wp.model_dump() != wp.model_dump():
        print("Transformed workplan will be used for execution.")

    planner = Planner(workplan=wp)
    # from cstar.orchestration.launch.local import LocalLauncher
    # launcher: Launcher = LocalLauncher()
    launcher: Launcher = SlurmLauncher()
    orchestrator = Orchestrator(planner, launcher)

    # schedule the tasks without waiting for completion
    await process_plan(orchestrator, RunMode.Schedule)

    # monitor the scheduled tasks until they complete
    await process_plan(orchestrator, RunMode.Monitor)


bp_default: t.Final[str] = (
    "~/code/cstar/cstar/additional_files/templates/blueprint.yaml"
)


def get_parser() -> argparse.ArgumentParser:
    """Simple parser for testing the dag runner in a debugger."""
    tpl_choices: t.TypeAlias = t.Literal["single_step", "linear", "fanout", "parallel"]
    choices: list[tpl_choices] = ["single_step", "linear", "fanout", "parallel"]

    parser = argparse.ArgumentParser("dag_runner")
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        type=Path,
        help="The path where outputs must be written",
    )
    parser.add_argument(
        "-b",
        "--blueprint",
        type=Path,
        help="The path to the blueprint to execute within the workflow",
        default=None,
    )
    parser.add_argument(
        "-w",
        "--workplan",
        type=Path,
        help="The path to the workplan to execute",
        default=None,
    )
    parser.add_argument(
        "-t",
        "--template",
        choices=choices,
        type=str,
        help="The template a standalone blueprint will be executed in",
        default="single_step",
    )

    return parser


def create_host_workplan(output_path: Path, template: str, bp_path: Path) -> Path:
    """Replace the default blueprint path in a template and write the
    modified workplan in a new location.
    """
    cstar_dir = Path(__file__).parent.parent
    templates_dir = cstar_dir / "additional_files/templates"
    template_path = templates_dir / "wp" / f"{template}.yaml"

    bp_source_path = templates_dir / "bp/blueprint.yaml"
    bp_target_path = output_path / bp_path.name

    # copy the original blueprint into the working directory
    bp_target_path.write_text(bp_source_path.read_text())

    wp_content = template_path.read_text()
    wp_content = wp_content.replace(bp_default, bp_target_path.as_posix())

    wp_path = output_path / f"{template}-host.yaml"
    wp_path.write_text(wp_content)

    return wp_path


def main() -> None:
    """Execute the dag runner using parameters supplied from the CLI."""
    args = sys.argv[1:]
    ns = get_parser().parse_args(args)

    o_path = Path(ns.output)
    if not o_path.exists():
        o_path.mkdir(parents=True)

    wp_path = Path(ns.workplan) if ns.workplan is not None else None
    bp_path = Path(ns.blueprint) if ns.blueprint is not None else None
    template = ns.template

    if wp_path is None and bp_path is None:
        print("Runner not executed\n\t- A workplan or blueprint path must be provided")
        sys.exit(1)

    if bp_path:
        # host the blueprint in a workplan template
        wp_path = create_host_workplan(o_path, template, bp_path)
        print(f"Running workplan at `{wp_path}` with blueprint at `{bp_path}`")
    else:
        bp_path = Path(bp_default)
        print(f"Running unmodified workplan at `{wp_path}`")

    run_id = (
        f"dag-main-{template}-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )
    os.environ["CSTAR_RUNID"] = run_id

    if wp_path is None:
        raise ValueError("Workplan path is malformed.")

    asyncio.run(build_and_run_dag(wp_path))


if __name__ == "__main__":
    main()
