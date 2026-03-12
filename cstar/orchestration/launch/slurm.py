import os
import typing as t

from prefect import task

from cstar.base.env import ENV_CSTAR_RUNID, get_env_item
from cstar.base.log import get_logger
from cstar.base.utils import _run_cmd
from cstar.execution.handler import ExecutionStatus
from cstar.execution.scheduler_job import (
    create_scheduler_job,
    get_slurm_batch,
)
from cstar.orchestration.converter.converter import get_command_mapping
from cstar.orchestration.models import Application, RomsMarblBlueprint
from cstar.orchestration.orchestration import (
    Launcher,
    ProcessHandle,
    Status,
    Task,
)
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)

if t.TYPE_CHECKING:
    from prefect.context import TaskRunContext

    from cstar.orchestration.orchestration import LiveStep

log = get_logger(__name__)


def cache_key_func(context: "TaskRunContext", params: dict[str, t.Any]) -> str:
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
    run_id = os.getenv(ENV_CSTAR_RUNID)
    cache_key = f"{run_id}_{params['step'].name}_{context.task.name}"

    log.debug("Cache check: %s", cache_key)
    return cache_key


class SlurmHandle(ProcessHandle):
    """Handle enabling reference to a task running in SLURM."""


class SlurmLauncher(Launcher[SlurmHandle]):
    """A launcher that executes steps in a SLURM-enabled cluster."""

    @staticmethod
    def configured_queue() -> str:
        """Get the queue to use for SLURM jobs.

        Read from the environment variable `CSTAR_SLURM_QUEUE`.

        Returns
        -------
        str
            The queue to use for SLURM jobs.
        """
        return get_env_item(ENV_CSTAR_SLURM_QUEUE).value

    @staticmethod
    def configured_walltime() -> str:
        """Get the max-walltime to use for SLURM jobs.

        Read from the environment variable `CSTAR_SLURM_MAX_WALLTIME`.

        Returns
        -------
        str
            The max-walltime to use for SLURM jobs.
        """
        return get_env_item(ENV_CSTAR_SLURM_MAX_WALLTIME).value

    @staticmethod
    def configured_account() -> str:
        """Get the account to use for SLURM jobs.

        Read from the environment variable `CSTAR_SLURM_ACCOUNT`.

        Returns
        -------
        str
            The account to use for SLURM jobs.
        """
        return get_env_item(ENV_CSTAR_SLURM_ACCOUNT).value

    @task(persist_result=True, cache_key_fn=cache_key_func)
    @staticmethod
    async def _submit(step: "LiveStep", dependencies: list[SlurmHandle]) -> SlurmHandle:
        """Submit a step to SLURM as a new batch allocation.

        Parameters
        ----------
        step : LiveStep
            The step to submit to SLURM.
        dependencies : list[SlurmHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        SlurmHandle
            A ProcessHandle identifying the newly submitted job.
        """
        job_name = step.safe_name
        bp = deserialize(step.blueprint_path, RomsMarblBlueprint)
        job_dep_ids = [d.pid for d in dependencies]

        step_converter = get_command_mapping(
            Application(step.application),
            SlurmLauncher,
        )

        script_path = step.fsm.work_dir / "script.sh"
        output_file = step.fsm.logs_dir / f"{job_name}.out"

        if not script_path.parent.exists():
            script_path.parent.mkdir(parents=True)

        if not output_file.parent.exists():
            output_file.parent.mkdir(parents=True)
            output_file.write_text("ready\n")

        command = step_converter(step)
        job = create_scheduler_job(
            commands=command,
            account_key=SlurmLauncher.configured_account(),
            cpus=bp.cpus_needed,
            nodes=None,  # let existing logic handle this
            cpus_per_node=None,  # let existing logic handle this
            script_path=script_path,
            run_path=script_path.parent,
            job_name=job_name,
            output_file=output_file,
            queue_name=SlurmLauncher.configured_queue(),
            walltime=SlurmLauncher.configured_walltime(),
            depends_on=job_dep_ids,
        )

        short_command = command.replace("\n", "")[:40]  # shorten and omit newlines

        msg = f"Submitting command `{short_command}...` for step `{step.name}`."
        log.debug(msg)
        job.submit()

        if job.id:
            log.debug("Submission of `%s` created Job ID `%s`", step.name, job.id)
            return SlurmHandle(pid=str(job.id), name=job_name)

        msg = f"Unable to retrieve job ID for step `{step.name}`. Job `{job}` failed"
        raise RuntimeError(msg)

    @staticmethod
    async def _status(pid: str) -> ExecutionStatus:
        """Retrieve the status of a step running in SLURM.

        Parameters
        ----------
        step : LiveStep
            The step triggering the job.
        handle : SlurmHandle
            A handle object for a SLURM-based task.

        Returns
        -------
        ExecutionStatus
            The current status of the step.
        """
        batch = await get_slurm_batch(pid)
        status = batch.job.status

        msg = f"Status of job `{pid}` is {status}"
        log.debug(msg)

        return status

    @classmethod
    async def launch(
        cls,
        step: "LiveStep",
        dependencies: list[SlurmHandle],
    ) -> Task[SlurmHandle]:
        """Launch a step in SLURM.

        Parameters
        ----------
        step : LiveStep
            The step to submit to SLURM.
        dependencies : list[SlurmHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        Task[SlurmHandle]
            A Task containing information about the newly submitted job.
        """
        # item is persisted to a name that is shared by all instances
        persist_as = Task.persist_step_as(step)
        prior = Task.load_persisted(persist_as)
        handle: ProcessHandle | None = None
        submit_fn = SlurmLauncher._submit

        # TODO: consider loading persisted values all at once during startup instead
        if prior:  #  and prior.task.status != Status.Done:
            # always retrieve real-deal in case persisting status updates failed.
            last_status = await SlurmLauncher.query_status(prior.handle)
            if Status.is_failure(last_status):
                # force cache refresh for any tasks that didn't succeed
                step.fsm.clear_prior()
                submit_fn = SlurmLauncher._submit.with_options(refresh_cache=True)


        handle = await submit_fn(step, dependencies)

        return Task(
            step=step,
            handle=handle,
            status=Status.Submitted,
        )

    @classmethod
    def _map_status(cls, status: ExecutionStatus) -> Status:
        match status:
            case (
                ExecutionStatus.PENDING
                | ExecutionStatus.RUNNING
                | ExecutionStatus.ENDING
                | ExecutionStatus.HELD
            ):
                return Status.Running
            case ExecutionStatus.COMPLETED:
                return Status.Done
            case ExecutionStatus.CANCELLED:
                return Status.Cancelled
            case ExecutionStatus.FAILED:
                return Status.Failed

        return Status.Unsubmitted

    @classmethod
    async def query_status(
        cls,
        item: Task[SlurmHandle] | SlurmHandle,
    ) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        step : LiveStep
            The step that will be queried for.
        item : Task[SlurmHandle] | SlurmHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Status
            The current status of the item.
        """
        handle = item.handle if isinstance(item, Task) else item
        exec_status = await SlurmLauncher._status(handle.pid)

        msg = f"SLURM job `{handle.pid}` status is `{exec_status}`"
        log.debug(msg)

        return SlurmLauncher._map_status(exec_status)

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
            log.exception("Unable to cancel the task `%s`", handle.pid)

        return item
