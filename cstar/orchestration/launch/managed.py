import typing as t
from pathlib import Path

from prefect import State, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.objects import StateType
from prefect.context import TaskRunContext
from prefect.states import Cancelled

from cstar.base.log import get_logger
from cstar.base.utils import get_env_item
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.converter.converter import (
    get_command_mapping,
)
from cstar.orchestration.models import Application, RomsMarblBlueprint, Step
from cstar.orchestration.orchestration import (
    Launcher,
    ProcessHandle,
    Status,
    Task,
)
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.utils import (
    ENV_CSTAR_MANAGED_ACCOUNT,
    ENV_CSTAR_MANAGED_MAX_WALLTIME,
    ENV_CSTAR_MANAGED_QUEUE,
    ENV_CSTAR_ORCH_RUNID,
)

if t.TYPE_CHECKING:
    import uuid

    from prefect.client.schemas.objects import TaskRun
    from prefect.client.schemas.responses import OrchestrationResult

log = get_logger(__name__)


def convert_managed_status(state: State | None) -> ExecutionStatus:
    """Convert the state type from the managed task engine into a
    C-Star `ExecutionStatus`.
    """
    if state is None:
        return ExecutionStatus.UNSUBMITTED

    state_type = state.type

    match state_type:
        case [StateType.SCHEDULED, StateType.PENDING]:
            exec_status = ExecutionStatus.PENDING
        case [StateType.RUNNING]:
            exec_status = ExecutionStatus.RUNNING
        case [StateType.COMPLETED]:
            exec_status = ExecutionStatus.COMPLETED
        case [StateType.CANCELLED, StateType.CANCELLING]:
            exec_status = ExecutionStatus.CANCELLED
        case [StateType.PAUSED]:
            exec_status = ExecutionStatus.HELD
        case [StateType.CRASHED, StateType.FAILED]:
            exec_status = ExecutionStatus.CANCELLED
        case _:
            exec_status = ExecutionStatus.UNKNOWN

    return exec_status


async def get_status_of_managed_job(job_id: str) -> ExecutionStatus:
    """Check the status of a managed job.

    Parameters
    ----------
    job_id: str
        The job_id to check

    Returns
    -------
    status: ExecutionStatus
        The status of the job
    """
    task_run_id = t.cast("uuid.UUID", job_id)

    async with get_client() as client:
        task_run: TaskRun = await client.read_task_run(task_run_id)

    return convert_managed_status(task_run.state)


async def cancel_managed_job(job_id: str, source: str) -> ExecutionStatus:
    """Attempt to cancel a managed job.

    Parameters
    ----------
    job_id: str
        The job_id to cancel
    source : str
        The source of the cancellation request

    Returns
    -------
    status: ExecutionStatus
        The status of the job
    """
    task_run_id = t.cast("uuid.UUID", job_id)

    async with get_client() as client:
        orchestration_result: OrchestrationResult = await client.set_task_run_state(
            task_run_id,
            Cancelled(message=f"Job cancellation requested due to: {source}"),
            force=True,
        )

    return convert_managed_status(orchestration_result.state)


@task
def run_script(script_path: Path, output_file: Path) -> None: ...


async def schedule_managed_job(script_path: Path, output_file: Path) -> str:
    """Schedule execution of a script in prefect.

    Parameters
    ----------
    script_path : Path
        The path to the script to be executed.
    output_file : Path
        The path to the desired output file.

    Returns
    -------
    str
        The unique identifier of the job.
    """
    result = run_script.submit(script_path, output_file)
    return str(result.task_run_id)


def orchestrated_step_cache_key_func(
    context: TaskRunContext,
    params: dict[str, t.Any],
) -> str:
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
    # TODO (ankona): verify get_env_item(run-id) doesn't generate a new value every time!  # noqa: FIX002, TD003
    run_id = get_env_item(ENV_CSTAR_ORCH_RUNID).value
    cache_key = f"{run_id}_{params['step'].name}_{context.task.name}"

    msg = f"Cache check: {cache_key}"
    log.debug(msg)
    return cache_key


class ManagedHandle(ProcessHandle):
    """Handle enabling reference to a managed task."""

    job_name: str | None
    """The user-friendly, task-based job name."""

    def __init__(self, job_id: str, job_name: str | None = None) -> None:
        """Initialize the managed handle.

        Parameters
        ----------
        job_id : str
            The MANAGED_JOB_ID identifying a job.
        job_name : str or None
            The job name assigned to the job.
        """
        super().__init__(pid=job_id)
        self.job_name = job_name


class ManagedLauncher(Launcher[ManagedHandle]):
    """A launcher that executes steps on local compute resources."""

    @staticmethod
    def configured_queue() -> str:
        """Get the queue to use for jobs.

        Read from environment variables.

        Returns
        -------
        str
            The queue to use for jobs.
        """
        return get_env_item(ENV_CSTAR_MANAGED_QUEUE).value

    @staticmethod
    def configured_walltime() -> str:
        """Get the max-walltime to use for jobs.

        Read from environment variables.

        Returns
        -------
        str
            The max-walltime to use for jobs.
        """
        return get_env_item(ENV_CSTAR_MANAGED_MAX_WALLTIME).value

    @staticmethod
    def configured_account() -> str:
        """Get the account to use for jobs.

        Read from environment variables.

        Returns
        -------
        str
            The account to use for jobs.
        """
        return get_env_item(ENV_CSTAR_MANAGED_ACCOUNT).value

    @task(persist_result=True, cache_key_fn=orchestrated_step_cache_key_func)
    @staticmethod
    async def _submit(step: Step, dependencies: list[ManagedHandle]) -> ManagedHandle:  # noqa: ARG004
        """Submit a step as a new batch allocation.

        Parameters
        ----------
        step : Step
            The step to submit.
        dependencies : list[ManagedHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        ManagedHandle
            A ProcessHandle identifying the newly submitted job.
        """
        job_name = step.safe_name
        bp_path = Path(step.blueprint_path)
        bp = deserialize(bp_path, RomsMarblBlueprint)
        # job_dep_ids = [d.pid for d in dependencies]

        step_fs = step.file_system(bp)

        step_converter = get_command_mapping(
            Application(step.application), ManagedLauncher
        )

        script_path = step_fs.work_dir / "script.sh"
        output_file = step_fs.logs_dir / f"{job_name}.out"

        if not script_path.parent.exists():
            script_path.parent.mkdir(parents=True)

        if not output_file.parent.exists():
            output_file.parent.mkdir(parents=True)
            output_file.write_text("ready\n")

        command = step_converter(step)
        short_command = command.replace("\n", "")[:40]  # shorten and omit newlines

        msg = f"Submitting command `{short_command}...` for step `{step.name}`."
        log.debug(msg)

        job_id = await schedule_managed_job(script_path, output_file)

        if job_id:
            msg = f"Submission of `{step.name}` created Job ID `{job_id}`"
            log.debug(msg)
            return ManagedHandle(job_id=job_id, job_name=job_name)

        msg = f"Unable to retrieve job ID for step `{step.name}`. Job `{job_id}` failed"
        raise RuntimeError(msg)

    @staticmethod
    async def _status(step: Step, handle: ManagedHandle) -> ExecutionStatus:
        """Retrieve the status of a step.

        Parameters
        ----------
        step : Step
            The step triggering the job.
        handle : ManagedHandle
            A handle object for a task.

        Returns
        -------
        ExecutionStatus
            The current status of the step.
        """
        msg = f"Requesting status of job {handle.pid} for step {step.name}"
        log.debug(msg)

        status = await get_status_of_managed_job(handle.pid)

        msg = f"Status of job {handle.pid} is {status} for step {step.name}"
        log.debug(msg)

        return status

    @classmethod
    async def launch(
        cls,
        step: Step,
        dependencies: list[ManagedHandle],
    ) -> Task[ManagedHandle]:
        """Launch a step.

        Parameters
        ----------
        step : Step
            The step to submit.
        dependencies : list[ManagedHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        Task[ManagedHandle]
            A Task containing information about the newly submitted job.
        """
        handle = await ManagedLauncher._submit(step, dependencies)
        return Task(
            status=Status.Submitted,
            step=step,
            handle=handle,
        )

    @classmethod
    async def query_status(
        cls, step: Step, item: Task[ManagedHandle] | ManagedHandle
    ) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        step : Step
            The step that will be queried for.
        item : Task[ManagedHandle] | ManagedHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Status
            The current status of the item.
        """
        handle = item.handle if isinstance(item, Task) else item
        exec_status = await ManagedLauncher._status(step, handle)

        msg = f"Manged job `{handle.pid}` status is `{exec_status}`"
        log.debug(msg)

        if exec_status in [
            ExecutionStatus.PENDING,
            ExecutionStatus.RUNNING,
            ExecutionStatus.ENDING,
            ExecutionStatus.HELD,
        ]:
            return Status.Running
        if exec_status == ExecutionStatus.COMPLETED:
            return Status.Done
        if exec_status == ExecutionStatus.CANCELLED:
            return Status.Cancelled
        if exec_status == ExecutionStatus.FAILED:
            return Status.Failed

        return Status.Unsubmitted

    @classmethod
    async def cancel(cls, item: Task[ManagedHandle]) -> Task[ManagedHandle]:
        """Cancel a task, if possible.

        Parameters
        ----------
        item : Task[ManagedHandle]
            A task to cancel.

        Returns
        -------
        Task[ManagedHandle]
            The task after the cancellation attempt has completed.
        """
        handle = item.handle

        try:
            _ = await cancel_managed_job(handle.pid, "Managed Launcher")
            item.status = Status.Cancelled
        except RuntimeError:
            msg = f"Unable to cancel the task `{handle.pid}`"
            log.exception(msg)

        return item
