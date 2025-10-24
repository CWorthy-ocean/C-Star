"""
This is a hacky POC and not a production-level solution, it should be removed or heavily improved
before going into develop/main
"""

import asyncio
import os
import sys
from pathlib import Path
from time import sleep, time

from prefect import flow, task
from prefect.context import TaskRunContext
from prefect.futures import wait

from cstar.execution.handler import ExecutionStatus
from cstar.execution.scheduler_job import create_scheduler_job, get_status_of_slurm_job
from cstar.orchestration.models import RomsMarblBlueprint, Step, Workplan
from cstar.orchestration.serialization import deserialize

JobId = str
JobStatus = str

# these are little mocks you can uncomment if you want to run this locally and not on anvil

# def create_scheduler_job(*args, **kwargs):
#     class dummy:
#         def submit(self):
#             pass
#
#         @property
#         def id(self ):
#             return uuid4()
#
#     return dummy()
#
#
# def get_status_of_slurm_job(*args, **kwargs):
#     sleep(30)
#     return ExecutionStatus.COMPLETED


def cache_func(context: TaskRunContext, params) -> str:
    """Cache on a combination of the task name and user-assigned run id.

    Parameters
    ----------
    context : TaskRunContext
        The prefect context object for the currently running task.
    params : t.Any
        Extra params
        #TODO: look this up in the prefect docs
    """
    cache_key = f"{os.getenv('CSTAR_RUNID')}_{params['step'].name}_{context.task.name}"
    print(f"Cache check: {cache_key}")
    return cache_key


@task(persist_result=True, cache_key_fn=cache_func, log_prints=True)
def submit_job(step: Step, job_dep_ids: list[str] = []) -> JobId:
    bp_path = step.blueprint
    bp = deserialize(Path(bp_path), RomsMarblBlueprint)

    job = create_scheduler_job(
        commands=f"python3 -m cstar.entrypoint.worker.worker -b {bp_path}",
        account_key=os.getenv("CSTAR_ACCOUNT_KEY", ""),
        cpus=bp.cpus_needed,
        nodes=None,  # let existing logic handle this
        cpus_per_node=None,  # let existing logic handle this
        script_path=None,  # puts it in current dir
        run_path=bp.runtime_params.output_dir,
        job_name=None,  # to fill with some convention
        output_file=None,  # to fill with some convention
        queue_name=os.getenv("CSTAR_QUEUE_NAME"),
        walltime="00:10:00",  # TODO how to determine this one?
        depends_on=job_dep_ids,
    )

    job.submit()
    print(f"Submitted {step.name} with id {job.id}")
    return str(job.id)


@task(persist_result=True, cache_key_fn=cache_func, log_prints=True)
def check_job(step: Step, job_id: JobId) -> ExecutionStatus:
    t_start = time()
    dur = 10 * 60
    while time() - t_start < dur:
        status = get_status_of_slurm_job(job_id)
        print(f"status of {step.name} is {status}")
        if status in [
            ExecutionStatus.CANCELLED,
            ExecutionStatus.FAILED,
            ExecutionStatus.COMPLETED,
        ]:
            return status
        sleep(10)
    return status


@flow
async def build_and_run_dag(path: Path) -> None:
    """Execute the steps in the workplan.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute
    """
    wp = deserialize(path, Workplan)

    id_dict = {}
    status_dict = {}

    no_dep_steps = []
    follow_up_steps = []

    for step in wp.steps:
        if not step.depends_on:
            print(f"No dependencies found for step: {step.name}")
            no_dep_steps.append(step)
        else:
            print(f"Adding dependencies for step: {step.name}")
            follow_up_steps.append(step)

    for step in no_dep_steps:
        print(f"Submitting step: {step.name}")
        id_dict[step.name] = submit_job(step)

        print(f"Retrieving status of step: {step.name}")
        status_dict[step.name] = check_job.submit(step, id_dict[step.name])
        print(f"Step {step.name} current status: {status_dict[step.name]}")

    while True:
        for step in follow_up_steps:
            print(f"Checking step: {step.name}")

            if all(s in id_dict for s in step.depends_on):
                print(f"Prerequisites met for step: {step.name}. Starting...")
                id_dict[step.name] = submit_job(
                    step, [id_dict[s] for s in step.depends_on]
                )
                status_dict[step.name] = check_job.submit(
                    step, id_dict[step.name], [status_dict[s] for s in step.depends_on]
                )
        if len(id_dict) == len(wp.steps):
            break

    wait(list(status_dict.values()))
    print("All steps completed.")


if __name__ == "__main__":
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "wholenode"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    wp_path = "/Users/eilerman/git/C-Star/personal_testing/workplan_local.yaml"
    wp_path = "/home/x-seilerman/wp_testing/workplan.yaml"

    my_run_name = sys.argv[1]
    os.environ["CSTAR_RUNID"] = my_run_name
    # t = Thread(target=check_job.serve)
    # t.start()
    asyncio.run(build_and_run_dag(wp_path))
    # t.join()
