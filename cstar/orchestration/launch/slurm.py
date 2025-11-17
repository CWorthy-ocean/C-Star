import os
import re
import typing as t
from pathlib import Path

from prefect import task
from prefect.context import TaskRunContext

from cstar.base.utils import _run_cmd
from cstar.execution.handler import ExecutionStatus
from cstar.execution.scheduler_job import (
    create_scheduler_job,
    get_status_of_slurm_job,
)
from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.orchestration import (
    CStep,
    Launcher,
    ProcessHandle,
    Status,
    Task,
)
from cstar.orchestration.serialization import deserialize


def slugify(source: str) -> str:
    """Convert a source string into a URL-safe slug.

    Parameters
    ----------
    source : str
        The string to be converted.

    Returns
    -------
    str
        The slugified version of the source string.
    """
    if not source:
        raise ValueError

    return re.sub(r"\s+", "-", source.casefold())


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

    job_name: str | None
    """The user-friendly, task-based job name."""

    def __init__(self, job_id: str, job_name: str | None = None) -> None:
        """Initialize the SLURM handle.

        Parameters
        ----------
        job_id : str
            The SLURM_JOB_ID identifying a job.
        """
        super().__init__(pid=job_id)
        self.job_name = job_name


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
        job_name = slugify(step.name)
        bp_path = step.blueprint
        bp = deserialize(Path(bp_path), RomsMarblBlueprint)
        job_dep_ids = [d.pid for d in dependencies]

        print(f"Submitting {step.name}")
        job = create_scheduler_job(
            # commands=f"python3 -m cstar.entrypoint.worker.worker -b {bp_path}",
            commands="sleep 10",
            account_key=os.getenv("CSTAR_ACCOUNT_KEY", ""),
            cpus=bp.cpus_needed,
            nodes=None,  # let existing logic handle this
            cpus_per_node=None,  # let existing logic handle this
            script_path=None,  # puts it in current dir
            run_path=bp.runtime_params.output_dir,
            job_name=job_name,
            output_file=None,  # to fill with some convention
            queue_name=os.getenv("CSTAR_QUEUE_NAME"),
            walltime="00:10:00",  # TODO how to determine this one?
            depends_on=job_dep_ids,
        )

        job.submit()

        if job.id:
            print(f"Submission of {step.name} created Job ID `{job.id}`")
            handle = SlurmHandle(job_id=str(job.id), job_name=job_name)
            return handle

        print(f"Job submission failed: {job}")
        raise RuntimeError(f"Unable to retrieve scheduled job ID for: {step.name}")

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
        status = ExecutionStatus.UNKNOWN

        print(f"requesting status of job {handle.pid} for step {step.name}")
        status = get_status_of_slurm_job(handle.pid)
        print(f"status of job {handle.pid} is {status} for step {step.name}")

        return status

    @classmethod
    async def launch(
        cls, step: CStep, dependencies: list[SlurmHandle]
    ) -> Task[SlurmHandle]:
        """Launch a step in SLURM.

        Parameters
        ----------
        step : Step
            The step to submit to SLURM.
        """
        handle = await SlurmLauncher._submit(step, dependencies)
        return Task(
            status=Status.Submitted,
            step=step,
            handle=handle,
        )

    @classmethod
    async def query_status(
        cls, step: CStep, item: Task[SlurmHandle] | SlurmHandle
    ) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        item : Task[SlurmHandle] | SlurmHandle
            An item with a handle to be used to execute a status query.
        """
        handle = item.handle if isinstance(item, Task) else item
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
    async def cancel(cls, item: Task[SlurmHandle]) -> Task[SlurmHandle]:
        """Cancel a task, if possible.

        Parameters
        ----------
        item : Task[SlurmHandle]
            A task to cancel.

        Returns
        -------
        Task[SlurmHandle]
            The task after the cancellation attempt has completed.
        """
        handle = item.handle

        try:
            _run_cmd(
                f"scancel {handle.pid}",
                cwd=None,
                raise_on_error=True,
                msg_post=f"Job {handle.pid} cancelled",
                msg_err="Non-zero exit code when cancelling job.",
            )
            item.status = Status.Cancelled
        except RuntimeError:
            print(f"Unable to cancel the task `{handle.pid}")

        return item
