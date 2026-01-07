import argparse
import asyncio
import os
import sys
import typing as t
from itertools import cycle
from pathlib import Path

from cstar.base.feature import is_feature_enabled
from cstar.base.log import get_logger
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import (
    Launcher,
    Orchestrator,
    Planner,
    RunMode,
    configure_environment,
    get_run_id,
)
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.transforms import (
    RomsMarblTimeSplitter,
    WorkplanTransformer,
)
from cstar.orchestration.utils import ENV_CSTAR_ORCH_DELAYS

WorkplanTemplate: t.TypeAlias = t.Literal["single_step", "linear", "fanout", "parallel"]
log = get_logger(__name__)


def incremental_delays() -> t.Generator[float, None, None]:
    """Return a value from an infinite cycle of incremental delays.

    Returns
    -------
    Generator[float]
    """
    delays = [0.1, 1, 2, 5, 15, 30, 60]

    if custom_delays := os.getenv(ENV_CSTAR_ORCH_DELAYS, ""):
        try:
            delays = [float(d) for d in custom_delays.split(",")]
        except ValueError:
            log.warning(f"Malformed delay provided: {custom_delays}. Using defaults.")

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
    log.info(f"Loading status of workplan: {wp.name}")

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
        await orchestrator.run(mode=mode)

        curr_closed = orchestrator.get_closed_nodes(mode=mode)
        curr_open = orchestrator.get_open_nodes(mode=mode)

        if curr_closed != closed_set or curr_open != curr_open:
            # reset to initial delay when a task is found or completed
            delay_iter = iter(incremental_delays())

        open_set = curr_open
        closed_set = curr_closed

        sleep_duration = next(delay_iter)
        await asyncio.sleep(sleep_duration)

    log.info(f"Workplan {mode} is complete.")


async def prepare_workplan(
    wp_path: Path,
    output_dir: Path,
) -> tuple[Workplan, Path]:
    """Load the workplan and apply any applicable transforms.

    Parameters
    ----------
    wp_path : Path
        The path to the workplan to load.
    output_dir : Path
        The directory where workplan outputs will be written.

    Returns
    -------
    Workplan
    """
    wp_orig = await asyncio.to_thread(deserialize, wp_path, Workplan)

    if is_feature_enabled("ORCH_TRANSFORM_AUTO"):
        transformer = WorkplanTransformer(wp_orig, RomsMarblTimeSplitter())
        wp = transformer.apply()

        if transformer.is_modified:
            log.info("A time-split workplan will be executed.")
    else:
        wp = wp_orig

    # make a copy of the original and modified blueprint in the output directory
    persist_orig = WorkplanTransformer.derived_path(wp_path, output_dir, "_orig")
    persist_as = WorkplanTransformer.derived_path(wp_path, output_dir)
    _ = await asyncio.gather(
        asyncio.to_thread(serialize, persist_orig, wp_orig),
        asyncio.to_thread(serialize, persist_as, wp),
    )

    return wp, persist_as


# @flow(log_prints=True)
async def build_and_run_dag(wp_path: Path, output_dir: Path) -> None:
    """Execute the steps in the workplan.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute
    output_dir : Path
        The path to the output directory.
    """
    run_id = get_run_id()
    configure_environment(output_dir, run_id)
    wp, wp_path = await prepare_workplan(wp_path, output_dir)

    planner = Planner(workplan=wp)
    # from cstar.orchestration.launch.local import LocalLauncher
    # launcher: Launcher = LocalLauncher()
    launcher = SlurmLauncher()
    orchestrator = Orchestrator(planner, launcher)

    # schedule the tasks without waiting for completion
    await process_plan(orchestrator, RunMode.Schedule)

    # monitor the scheduled tasks until they complete
    await process_plan(orchestrator, RunMode.Monitor)


bp_outputdir_default: t.Final[str] = "output_dir: ."
bp_default: t.Final[str] = (
    "~/code/cstar/cstar/additional_files/templates/blueprint.yaml"
)


def get_parser() -> argparse.ArgumentParser:
    """Simple parser for testing the dag runner in a debugger."""
    choices: list[WorkplanTemplate] = ["single_step", "linear", "fanout", "parallel"]

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

    # update the workplan output directory found in the template
    bp_content = bp_source_path.read_text()
    bp_content = bp_content.replace(
        bp_outputdir_default, f"output_dir: {output_path.as_posix()}"
    )
    # write the modified blueprint to the working directory
    bp_target_path.parent.mkdir(parents=True, exist_ok=True)
    bp_target_path.write_text(bp_content)

    # set paths in the template workplan to the just-created blueprint path
    wp_content = template_path.read_text()
    wp_content = wp_content.replace(bp_default, bp_target_path.as_posix())

    # write the modified workplan to the working directory. 
    wp_path = output_path / f"{template}-host.yaml"
    wp_path.parent.mkdir(parents=True, exist_ok=True)
    wp_path.write_text(wp_content)

    return wp_path


def main() -> None:
    """Execute the dag runner using parameters supplied from the CLI."""
    args = sys.argv[1:]
    ns = get_parser().parse_args(args)

    output_dir = Path(ns.output)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    wp_path = Path(ns.workplan) if ns.workplan is not None else None
    bp_path = Path(ns.blueprint) if ns.blueprint is not None else None
    template = ns.template
    # reset_base = ns.reset_base

    if wp_path is None and bp_path is None:
        log.error("Run aborted. A workplan or blueprint path must be provided")
        sys.exit(1)

    if bp_path:
        # host the blueprint in a workplan template
        wp_path = create_host_workplan(output_dir, template, bp_path)
        log.info(f"Running workplan at `{wp_path}` with blueprint at `{bp_path}`")
    else:
        bp_path = Path(bp_default)
        log.info(f"Running unmodified workplan at `{wp_path}`")

    if wp_path is None:
        raise ValueError("Workplan path is malformed.")

    asyncio.run(build_and_run_dag(wp_path, output_dir))


if __name__ == "__main__":
    main()
