import asyncio
import os
import random
import typing as t
from pathlib import Path

from prefect import task
from prefect.context import TaskRunContext

from cstar.execution.handler import ExecutionStatus
from cstar.execution.scheduler_job import create_scheduler_job, get_status_of_slurm_job
from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.orchestration import (
    CStep,
    Launcher,
    ProcessHandle,
    Status,
    Task,
)
from cstar.orchestration.serialization import deserialize


def duration_fn() -> int:
    """Mock task execution via randomly selecting a duration for the step."""
    return random.randint(5, 12)


def cache_key_func(context: TaskRunContext, params: dict[str, t.Any]) -> str:
    """Cache on a combination of the task name and user-assigned run id.

    Parameters
    ----------
    context : TaskRunContext
        The prefect context object for the currently running task
    params : dict[str, t.Any]
        A dictionary containing all thee input values to the task
    """
    cache_key = f"{os.getenv('CSTAR_RUNID')}_{params['step'].name}_{context.task.name}"
    print(f"Cache check: {cache_key}")
    return cache_key


class SlurmHandle(ProcessHandle):
    """Handle enabling reference to a task running in SLURM."""

    duration: int

    def __init__(self, job_id: str) -> None:
        """Initialize the SLURM handle.

        Parameters
        ----------
        job_id : str
            The SLURM_JOB_ID identifying a job.
        """
        self.duration = duration_fn()
        super().__init__(pid=job_id)

    # async def simulate_progress(self) -> None:
    #     """TEMPORARY task simulation."""
    #     # SIMULATED TASK PROGRESS -->
    #     self.duration -= 1
    #     await asyncio.sleep(1)

    #     # simulate task failure.
    #     if self.duration < 5 and random.randint(0, 100) < 5:
    #         self.duration = -100
    #     # <--- SIMULATED TASK PROGRESS


class SlurmLauncher(Launcher[SlurmHandle]):
    """A launcher that executes steps in a SLURM-enabled cluster."""

    @task(persist_result=True, cache_key_fn=cache_key_func)
    @staticmethod
    async def _submit(step: CStep, dependencies: list[SlurmHandle]) -> SlurmHandle:
        """Submit a step to SLURM as a new batch allocation.

        Parameters
        ----------
        step : Step
            The step to submit to SLURM.
        """
        job_dep_ids = [d.pid for d in dependencies]
        # TODO: get rid of conditional after implementing dep ID passing
        # or adding to the CStep...
        if False and job_dep_ids:
            bp_path = step.blueprint
            bp = deserialize(Path(bp_path), RomsMarblBlueprint)
            if job_dep_ids is None:
                job_dep_ids = []

            # todo: have the returned job include the name? let step give it to me?
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

            if job.id:
                print(f"Submitted {step.name} with id {job.id}")
                handle = SlurmHandle(
                    job_id=str(job.id)
                )  # todo: store ID as int, name as string? abandon name?
                return handle

            raise RuntimeError(f"Unable to retrieve scheduled job ID for: {step.name}")

        # # TODO: replace with non-mock implementation
        # job_id = f"mock-slurm-job-id-{step.name}"
        # print(f"Submitting run of {step.application} created job_id: {job_id}")
        #
        # handle = SlurmHandle(job_id=job_id)
        # await handle.simulate_progress()

        return handle

    # @task(persist_result=True, cache_key_fn=cache_key_func, log_prints=True)
    # def check_job(step: Step, job_id: JobId, deps: list[str] = []) -> ExecutionStatus:
    @staticmethod
    async def _status(step: CStep, handle: SlurmHandle) -> ExecutionStatus:
        """Retrieve the status of a step running in SLURM.

        Parameters
        ----------
        step : CStep
            The step triggering the job.
        handle : SlurmHandle
            A handle object for a SLURM-based task.
        """
        if False:
            # TODO: use the non-mock implementation instead...
            t_start = time()
            dur = 10 * 60
            status = ExecutionStatus.UNKNOWN

            # todo: avoid this loop. let the orchestrator query when it wants to.
            while time() - t_start < dur:
                status = get_status_of_slurm_job(handle.pid)
                print(f"status of job {handle.job_id} is {status}")

                if ExecutionStatus.is_terminal(status):
                    return status

                # sleep(10)
            return status

        # TODO: replace with non-mock implementation
        # Simulate "making progress" by adjusting the duration, here.
        job_id = handle.pid
        await handle.simulate_progress()
        print(f"\tRemaining for {handle.pid}: {handle.duration}")
        status = "RUNNING"
        if handle.duration == 0:
            status = "COMPLETED"
        elif handle.duration < 0:
            status = "FAILED"
        print(f"Retrieved status `{status}` for job_id `{job_id}`")
        return status

        # # TODO: replace with non-mock implementation
        # # Simulate "making progress" by adjusting the duration, here.
        # job_id = handle.pid
        # await handle.simulate_progress()
        # print(f"\tRemaining for {handle.pid}: {handle.duration}")
        # status = "RUNNING"
        # if handle.duration == 0:
        #     status = "COMPLETED"
        # elif handle.duration < 0:
        #     status = "FAILED"
        # print(f"Retrieved status `{status}` for job_id `{job_id}`")
        # return status

    @classmethod
    async def launch(cls, step: CStep, dependencies: list[SlurmHandle]) -> Task:
        """Launch a step in SLURM.

        Parameters
        ----------
        step : Step
            The step to submit to SLURM.
        """
        handle = await SlurmLauncher._submit(step, dependencies)
        # TODO: confirm handle did not insta-fail on launch
        return Task(
            status=(
                # TODO: remove this simulation of insta-fail using duration of 0
                # - consider using Status.Submitted to let it update...
                Status.Submitted
                if handle.pid and handle.duration > 0
                else Status.Failed
            ),
            step=step,
            handle=handle,
        )

    @classmethod
    async def query_status(cls, step: CStep, item: Task | SlurmHandle) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        item : Task | ProcessHandle
            An item with a handle to be used to execute a status query.
        """
        handle = t.cast(SlurmHandle, item.handle if isinstance(item, Task) else item)
        slurm_status = await SlurmLauncher._status(step, handle)

        print(f"SLURM job `{handle.pid}` status is `{slurm_status}`")

        if slurm_status in [
            ExecutionStatus.PENDING,
            ExecutionStatus.RUNNING,
            ExecutionStatus.ENDING,
            ExecutionStatus.HELD,
        ]:
            return Status.Running
        if slurm_status in [ExecutionStatus.COMPLETED]:
            return Status.Done
        if slurm_status in [ExecutionStatus.CANCELLED]:
            return Status.Cancelled
        if slurm_status in [ExecutionStatus.FAILED]:
            return Status.Failed

        return Status.Unsubmitted

    @classmethod
    async def cancel(cls, item: Task) -> Task:
        """Cancel a task, if possible.

        Parameters
        ----------
        item : Task or ProcessHandle
            A task or process handle to cancel.

        Returns
        -------
        Status
            The current status of the item.
        """
        handle = t.cast(SlurmHandle, item.handle)
        if handle.duration < 2:
            # pretend that i can't cancel...
            print(f"Unable to cancel this running task `{handle.pid}")
            return item

        item.status = Status.Cancelled
        return item
