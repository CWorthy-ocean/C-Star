import os
import sys
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
from cstar.orchestration.models import Application, RomsMarblBlueprint, Step
from cstar.orchestration.orchestration import (
    Launcher,
    ProcessHandle,
    Status,
    Task,
)
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.utils import slugify


def cache_key_func(context: TaskRunContext, params: dict[str, t.Any]) -> str:
    """Cache on a combination of the task name and user-assigned run id.

    Parameters
    ----------
    context : TaskRunContext
        The prefect context object for the currently running task
    params : dict[str, t.Any]
        A dictionary containing all thee input values to the task

    Returns
    -------
    str
        The cache key for the current context.
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
        job_name : str or None
            The job name assigend to the job.
        """
        super().__init__(pid=job_id)
        self.job_name = job_name


StepToCommandConversionFn: t.TypeAlias = t.Callable[[Step], str]
"""Convert a `Step` into a command to be executed.

Parameters
----------
step : Step
    The step to be converted.

Returns
-------
str
    The complete CLI command.
"""


def convert_roms_step_to_command(step: Step) -> str:
    """Convert a `Step` into a command to be executed.

    This function converts ROMS/ROMS-MARBL applications into a command triggering
    a C-Star worker to run a simulation.

    Parameters
    ----------
    step : Step
        The step to be converted.

    Returns
    -------
    str
        The complete CLI command.
    """
    bp_path = Path(step.blueprint).as_posix()
    return f"{sys.executable} -m cstar.entrypoint.worker.worker -b {bp_path}"


def convert_step_to_placeholder(step: Step) -> str:
    """Convert a `Step` into a command to be executed.

    This function converts applications into mocks by starting a process that
    executes a blocking sleep.

    Parameters
    ----------
    step : Step
        The step to be converted.

    Returns
    -------
    str
        The complete CLI command.
    """
    return f'{sys.executable} -c "import time; time.sleep(10)"'


app_to_cmd_map: dict[str, StepToCommandConversionFn] = {
    Application.ROMS.value: convert_roms_step_to_command,
    Application.ROMS_MARBL.value: convert_roms_step_to_command,
    "sleep": convert_step_to_placeholder,
}
"""Map application types to a function that converts a step to a CLI command."""


class SlurmLauncher(Launcher[SlurmHandle]):
    """A launcher that executes steps in a SLURM-enabled cluster."""

    @task(persist_result=True, cache_key_fn=cache_key_func)
    @staticmethod
    async def _submit(step: Step, dependencies: list[SlurmHandle]) -> SlurmHandle:
        """Submit a step to SLURM as a new batch allocation.

        Parameters
        ----------
        step : Step
            The step to submit to SLURM.
        dependencies : list[SlurmHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        SlurmHandle
            A ProcessHandle identifying the newly submitted job.
        """
        job_name = slugify(step.name)
        bp_path = Path(step.blueprint)
        bp = deserialize(bp_path, RomsMarblBlueprint)
        job_dep_ids = [d.pid for d in dependencies]

        step_converter = app_to_cmd_map[step.application]
        if converter_override := os.getenv("CSTAR_CMD_CONVERTER_OVERRIDE", ""):
            print(
                f"Overriding command converter for `{step.application}` to `{converter_override}`"
            )
            step_converter = app_to_cmd_map[converter_override]

        command = step_converter(step)
        job = create_scheduler_job(
            commands=command,
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

        print(f"Submitting step `{step.name}` as command `{command}`")
        job.submit()

        if job.id:
            print(f"Submission of `{step.name}` created Job ID `{job.id}`")
            handle = SlurmHandle(job_id=str(job.id), job_name=job_name)
            return handle

        print(f"Job submission for step `{step.name}` failed: {job}")
        raise RuntimeError(f"Unable to retrieve scheduled job ID for: {step.name}")

    @staticmethod
    async def _status(step: Step, handle: SlurmHandle) -> ExecutionStatus:
        """Retrieve the status of a step running in SLURM.

        Parameters
        ----------
        step : Step
            The step triggering the job.
        handle : SlurmHandle
            A handle object for a SLURM-based task.

        Returns
        -------
        ExecutionStatus
            The current status of the step.
        """
        status = ExecutionStatus.UNKNOWN

        print(f"requesting status of job {handle.pid} for step {step.name}")
        status = get_status_of_slurm_job(handle.pid)
        print(f"status of job {handle.pid} is {status} for step {step.name}")

        return status

    @classmethod
    async def launch(
        cls, step: Step, dependencies: list[SlurmHandle]
    ) -> Task[SlurmHandle]:
        """Launch a step in SLURM.

        Parameters
        ----------
        step : Step
            The step to submit to SLURM.
        dependencies : list[SlurmHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        Task[SlurmHandle]
            A Task containing information about the newly submitted job.
        """
        handle = await SlurmLauncher._submit(step, dependencies)
        return Task(
            status=Status.Submitted,
            step=step,
            handle=handle,
        )

    @classmethod
    async def query_status(
        cls, step: Step, item: Task[SlurmHandle] | SlurmHandle
    ) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        step : Step
            The step that will be queried for.
        item : Task[SlurmHandle] | SlurmHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Status
            The current status of the item.
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
            print(f"Unable to cancel the task `{handle.pid}`")

        return item
