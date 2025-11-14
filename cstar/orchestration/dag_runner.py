import asyncio
import os
import sys
import typing as t
from contextlib import contextmanager
from itertools import cycle
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest  # todo: remove after moving test to unit-tests

from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.orchestration import (
    CWorkplan,
    Launcher,
    Orchestrator,
    Planner,
)
from cstar.orchestration.serialization import deserialize

JobId = str
JobStatus = str

# these are little mocks you can uncomment if you want to run this locally and not on anvil

# import random # noqa: E402, I001
# from cstar.execution.scheduler_job import SchedulerJob  # noqa: E402, I001
# def create_scheduler_job(*args, **kwargs) -> "SchedulerJob":  # noqa: F811
#     class DummySchedulerJob:
#         """Dummy job with minimum interface necessary for mock job execution."""
#
#         def __init__(self) -> None:
#             print("Creating dummy scheduler job.")
#             self._id = random.randint(1, 100_000_000)
#
#         def submit(self) -> None:
#             print("Performing dummy job submission.")
#
#         @property
#         def id(self) -> int | None:
#             return self._id
#
#     return DummySchedulerJob()


# def get_status_of_slurm_job(job_id: str, param) -> ExecutionStatus:
#     sleep(LOCAL_SLEEP_DURATION)
#     return ExecutionStatus.COMPLETED


# def cache_key_func(context: TaskRunContext, params: dict[str, t.Any]) -> str:
#     """Cache on a combination of the task name and user-assigned run id.
#
#     Parameters
#     ----------
#     context : TaskRunContext
#         The prefect context object for the currently running task
#     params : dict[str, t.Any]
#         A dictionary containing all thee input values to the task
#     """
#     cache_key = f"{os.getenv('CSTAR_RUNID')}_{params['step'].name}_{context.task.name}"
#     print(f"Cache check: {cache_key}")
#     return cache_key


# @task(persist_result=True, cache_key_fn=cache_key_func, log_prints=True)
# def submit_job(step: Step, job_dep_ids: list[str] | None = None) -> JobId:
#     bp_path = step.blueprint
#     bp = deserialize(Path(bp_path), RomsMarblBlueprint)
#     if job_dep_ids is None:
#         job_dep_ids = []
#
#     job = create_scheduler_job(
#         commands=f"python3 -m cstar.entrypoint.worker.worker -b {bp_path}",
#         account_key=os.getenv("CSTAR_ACCOUNT_KEY", ""),
#         cpus=bp.cpus_needed,
#         nodes=None,  # let existing logic handle this
#         cpus_per_node=None,  # let existing logic handle this
#         script_path=None,  # puts it in current dir
#         run_path=bp.runtime_params.output_dir,
#         job_name=None,  # to fill with some convention
#         output_file=None,  # to fill with some convention
#         queue_name=os.getenv("CSTAR_QUEUE_NAME"),
#         walltime="00:10:00",  # TODO how to determine this one?
#         depends_on=job_dep_ids,
#     )
#
#     job.submit()
#     print(f"Submitted {step.name} with id {job.id}")
#     return str(job.id)


# @task(persist_result=True, cache_key_fn=cache_key_func, log_prints=True)
# def check_job(step: Step, job_id: JobId, deps: list[str] = []) -> ExecutionStatus:
#     t_start = time()
#     dur = 10 * 60
#     status = ExecutionStatus.UNKNOWN
#
#     while time() - t_start < dur:
#         status = get_status_of_slurm_job(job_id)
#         print(f"status of {step.name} is {status}")
#
#         if ExecutionStatus.is_terminal(status):
#             return status
#
#         sleep(10)
#     return status


def incremental_delays() -> t.Generator[float, None, None]:
    """Return a value from an infinite cycle of incremental delays.

    Returns
    -------
    float
    """
    # TODO: load delays from config to enable dynamic changes for tests.
    delays = [2, 2, 5, 5, 15, 15]
    delay_cycle = cycle(delays)
    yield from delay_cycle


# @flow(log_prints=True)
async def build_and_run_dag(path: Path) -> None:
    """Execute the steps in the workplan.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute
    """
    wp = deserialize(path, CWorkplan)
    print(f"Executing workplan: {wp.name}")

    planner = Planner(workplan=wp)
    # from cstar.orchestration.launch.local import LocalLauncher
    # launcher: Launcher = LocalLauncher()
    launcher: Launcher = SlurmLauncher()

    orchestrator = Orchestrator(planner, launcher)

    closed_set = orchestrator.get_closed_nodes()
    open_set = orchestrator.get_open_nodes()
    delay_iter = iter(incremental_delays())

    while open_set is not None:
        print(f"[on-enter] Open nodes: {open_set}, Closed: {closed_set}")

        await orchestrator.run()

        closed_set = orchestrator.get_closed_nodes()
        open_set = orchestrator.get_open_nodes()

        print(f"[on-exit] Open nodes: {open_set}, Closed: {closed_set}")

        sleep_duration = next(delay_iter)
        print(f"Sleeping for {sleep_duration} seconds before next check.")
        await asyncio.sleep(sleep_duration)

    print(f"Workplan `{wp}` execution is complete.")


@contextmanager
def templated_plan(plan_name: str) -> t.Generator[Path, None, None]:
    wp_template_path = (
        Path(__file__).parent.parent / f"additional_files/templates/wp/{plan_name}.yaml"
    )
    bp_template_path = (
        Path(__file__).parent.parent / "additional_files/templates/bp/blueprint.yaml"
    )

    wp_tpl = wp_template_path.read_text()
    bp_tpl = bp_template_path.read_text()

    with (
        NamedTemporaryFile("w", delete_on_close=False) as wp,
        NamedTemporaryFile("w", delete_on_close=False) as bp,
    ):
        wp_path = Path(wp.name)
        bp_path = Path(bp.name)

        wp_tpl = wp_tpl.replace("{application}", "sleep")
        wp_tpl = wp_tpl.replace("{blueprint_path}", bp_path.as_posix())

        wp.write(wp_tpl)
        bp.write(bp_tpl)
        wp.close()
        bp.close()

        try:
            print(
                f"Temporary workplan located at {wp_path} with blueprint at {bp_path}"
            )
            yield wp_path
        finally:
            print(f"populated workplan template:\n{'#' * 80}\n{wp_tpl}\n{'#' * 80}")


@pytest.mark.asyncio
async def test_build_and_run() -> None:
    """Temporary unit test to trigger workflow execution."""
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "shared"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    for template in ["single_step", "fanout", "parallel"]:
        with templated_plan(template) as wp_path:
            my_run_name = f"{sys.argv[1]}_{template}"
            os.environ["CSTAR_RUNID"] = my_run_name
            asyncio.run(build_and_run_dag(wp_path))


if __name__ == "__main__":
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "wholenode"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    wp_path = Path("/Users/eilerman/git/C-Star/personal_testing/workplan_local.yaml")
    wp_path = Path("/home/x-seilerman/wp_testing/workplan.yaml")

    my_run_name = sys.argv[1]
    os.environ["CSTAR_RUNID"] = my_run_name
    asyncio.run(build_and_run_dag(wp_path))
