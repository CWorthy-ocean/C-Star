import asyncio
import os
import typing as t
from collections.abc import Mapping

from prefect import State, task
from prefect import Task as PrefectTask
from prefect.client.schemas import TaskRun
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from cstar.base.adapter import ConfiguredModelAdapter, CstarAdaptationError
from cstar.base.env import (
    ENV_CSTAR_RUNID,
    ENV_CSTAR_SLURM_POST_SUBMIT_DELAY,
    get_env_item,
)
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.base.utils import WALLTIME_RE, _run_cmd
from cstar.execution.handler import ExecutionStatus
from cstar.execution.scheduler_job import (
    SchedulerJob,
    create_scheduler_job,
    get_slurm_batch,
    get_slurm_batches,
)
from cstar.orchestration.adapter import StepToRunRequestAdapter
from cstar.orchestration.models import KeyValueStore
from cstar.orchestration.orchestration import (
    Launcher,
    ProcessHandle,
    RunRequestCommandFormatter,
    Status,
    Task,
)
from cstar.orchestration.state import (
    StateRepository,
    load_sentinels,
)
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)

if t.TYPE_CHECKING:
    from prefect.context import TaskRunContext

    from cstar.orchestration.orchestration import LiveStep

log = get_logger(__name__)


async def on_submit_complete(
    task: PrefectTask[["LiveStep", list["SlurmHandle"]], "SlurmHandle"],
    task_run: TaskRun,
    state: State["SlurmHandle"],
) -> None:
    """Perform actions required when a job submission completes
    successfully.
    """
    if state.is_completed() and state.name == "Cached":
        handle = await state.aresult()
        log.debug(f"Re-using result from cached SLURM job: {handle}")


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

    log.trace("Cache check: %s", cache_key)
    return cache_key


class SlurmComputeSpec(BaseModel):
    num_cpus: int = 0
    """Total number of CPUs required by the job."""
    num_nodes: int | None = None
    """The number of nodes to request."""
    cpus_per_node: int | None = None
    """The number of CPUs to request per node."""
    max_walltime: str = Field(default="", pattern=WALLTIME_RE)
    """The maximum walltime for the job in the format `HH:MM:SS`."""
    queue_name: str = ""
    """The priority of the job."""
    account_name: str = ""
    """The SLURM account to run the job under."""

    model_config: t.ClassVar[ConfigDict] = ConfigDict(str_strip_whitespace=True)
    """Configure model to ignore empty strings."""

    @property
    def environment(self) -> dict[str, str]:
        environment: dict[str, str] = {}
        if self.account_name and self.account_name != os.getenv(
            ENV_CSTAR_SLURM_ACCOUNT
        ):
            environment[ENV_CSTAR_SLURM_ACCOUNT] = self.account_name

        if self.queue_name and self.queue_name != os.getenv(ENV_CSTAR_SLURM_QUEUE):
            environment[ENV_CSTAR_SLURM_QUEUE] = self.queue_name

        if self.max_walltime and self.max_walltime != os.getenv(
            ENV_CSTAR_SLURM_MAX_WALLTIME
        ):
            environment[ENV_CSTAR_SLURM_MAX_WALLTIME] = self.max_walltime

        return environment


class SlurmComputeAdapter(ConfiguredModelAdapter[KeyValueStore, SlurmComputeSpec]):
    """Adapts a `KeyValueStore` containing optional overrides into a `SlurmComputeSpec`."""

    def adapt(self, model: KeyValueStore) -> SlurmComputeSpec:
        """Adapt the input into a `SlurmComputeSpec`.

        Parameters
        ----------
        model : KeyValueStore
            The `KeyValueStore` to be adapted.

        Returns
        -------
        SlurmComputeSpec

        Raises
        ------
        CstarExpectationFailed
            If the input cannot be used to attempt adaptation.
        CstarAdaptationError
            If the input cannot be successfully adapted to the target type.
        """
        if not model:
            msg = "Compute overrides were not supplied to the SlurmComputeAdapter"
            raise CstarExpectationFailed(msg)

        if overrides_ := t.cast("dict[str, str | int]", model.get("slurm", {})):
            try:
                compute = SlurmComputeSpec.model_validate(overrides_)

                if not compute.model_dump(exclude_defaults=True):
                    msg = "Non-default SLURM compute overrides were not specified."
                    log.debug(msg)

                return compute
            except ValidationError:
                msg = "Invalid compute overrides were specified"
                log.error(msg)

        msg = f"Unable to adapt model {model!r} into SlurmComputeSpec"
        raise CstarAdaptationError(msg)


class SlurmHandle(ProcessHandle):
    """Handle enabling reference to a task running in SLURM."""

    status: Status = Status.Unsubmitted
    """The current status of the task."""

    launcher_name: str = "slurm"
    """The launcher used to launch the process."""


class SlurmLauncher(Launcher[SlurmHandle]):
    """A launcher that executes steps in a SLURM-enabled cluster."""

    POST_SUBMIT_DELAY: t.Final[float] = float(
        get_env_item(ENV_CSTAR_SLURM_POST_SUBMIT_DELAY).value
    )
    """Delay after a submission to ensure status for a SLURM job can be queried."""

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

    @staticmethod
    def _get_default_compute_spec(step: "LiveStep") -> SlurmComputeSpec:
        """Create the default compute spec for SLURM.

        Returns
        -------
        SlurmComputeSpec
        """
        return SlurmComputeSpec(
            num_cpus=step.blueprint.cpus_needed,
            max_walltime=SlurmLauncher.configured_walltime(),
            queue_name=SlurmLauncher.configured_queue(),
            account_name=SlurmLauncher.configured_account(),
        )

    @staticmethod
    def _get_compute_spec(step: "LiveStep") -> SlurmComputeSpec:
        """Return a compute spec that has been customized with any
        overrides specified in the step.

        Parameters
        ----------
        step : LiveStep
            The step to use for configuring overrides

        Returns
        -------
        SlurmComputeSpec
        """
        default_compute = SlurmLauncher._get_default_compute_spec(step)
        compute = default_compute

        if step.compute_overrides:
            try:
                overrides = SlurmComputeAdapter().adapt(step.compute_overrides)
                compute = default_compute.model_copy(
                    update=overrides.model_dump(exclude_defaults=True)
                )

            except CstarAdaptationError:
                msg = f"SLURM overrides did not result in valid compute spec: {step.compute_overrides}"
                log.warning(msg, exc_info=True)
        return compute

    @staticmethod
    def adapt_step(
        step: "LiveStep",
        dependencies: list[SlurmHandle],
    ) -> SchedulerJob:
        """Create a `SchedulerJob` that will execute the desired command for a
        `Step` while also waiting for any dependencies to complete.

        Returns
        -------
        str
        """
        compute = SlurmLauncher._get_compute_spec(step)

        job_dep_ids = [d.pid for d in dependencies]
        request_adapter = StepToRunRequestAdapter()
        run_request = request_adapter.adapt(step)
        if compute.environment:
            run_request.environment.update(compute.environment)

        command = RunRequestCommandFormatter().format(run_request)

        return create_scheduler_job(
            commands=command,
            account_key=compute.account_name,
            cpus=compute.num_cpus,
            nodes=compute.num_nodes,
            cpus_per_node=compute.cpus_per_node,
            script_path=step.script_path,
            run_path=step.script_path.parent,
            job_name=step.safe_name,
            output_file=step.log_path,
            queue_name=compute.queue_name,
            walltime=compute.max_walltime,
            depends_on=job_dep_ids,
        )

    @task(
        persist_result=True,
        cache_key_fn=cache_key_func,
        on_completion=[on_submit_complete],
    )
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
        if not step.blueprint:
            msg = f"Step cannot resolve blueprint from: {step.blueprint_path}"
            raise CstarError(msg)

        step.script_path.parent.mkdir(parents=True, exist_ok=True)
        step.log_path.parent.mkdir(parents=True, exist_ok=True)

        run_id = os.getenv(ENV_CSTAR_RUNID, "")
        step.log_path.write_text(f"ready for run {run_id!r} step {step.name!r}!\n")

        job = SlurmLauncher.adapt_step(step, dependencies)
        short_command = job.commands.replace("\n", "")[:40]  # shorten and omit newlines

        msg = f"Submitting command `{short_command}...` for step `{step.name}`."
        log.debug(msg)
        job.submit()

        if job.id:
            # introduce slight delay so `sacct` queries can locate this job
            await asyncio.sleep(SlurmLauncher.POST_SUBMIT_DELAY)

            log.debug("Submission of `%s` created Job ID `%s`", step.name, job.id)
            return SlurmHandle(
                pid=str(job.id),
                name=step.name,
                run_id=run_id,
            )

        msg = f"Unable to retrieve job ID for step `{step.name}`. Job `{job}` failed"
        raise RuntimeError(msg)

    @staticmethod
    async def _get_status(job_id: str) -> ExecutionStatus:
        """Retrieve the status of a step running in SLURM.

        Parameters
        ----------
        job_id : str
            The slurm job ID to retrieve status for.

        Returns
        -------
        ExecutionStatus
            The current status of the step.
        """
        batch = await get_slurm_batch(job_id)
        return batch.status

    @staticmethod
    async def _locate_priors() -> Mapping[str, SlurmHandle]:
        """Retrieve all task sentinels discovered in the output path.


        Returns
        -------
        Mapping[str, Task[SlurmHandle]]
            Mapping of all previously run PIDs to their sentinel content.
        """
        sentinels = await load_sentinels(SlurmHandle)
        return {h.pid: h for h in sentinels}

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
        state_repo = StateRepository()

        prior_handle = await state_repo.get_sentinel(step.name, SlurmHandle)
        submit_fn = SlurmLauncher._submit

        if prior_handle:
            # use persisted task as sentinel only; query SLURM for up-to-date status
            last_status = await SlurmLauncher.query_status(prior_handle)

            if Status.is_failure(last_status):
                # force cache refresh for any tasks that didn't succeed
                step.fsm.clear_prior()
                submit_fn = SlurmLauncher._submit.with_options(refresh_cache=True)

                # SLURM cannot use dependencies on previously completed jobs
                pid_to_task = await cls._locate_priors()
                batch_map = await get_slurm_batches(pid_to_task.keys())
                successes = {
                    k
                    for k, v in batch_map.items()
                    if v.status == ExecutionStatus.COMPLETED
                }
                if dependencies and successes:
                    reusable = set(x.pid for x in dependencies).intersection(successes)
                    msg = f"Dependencies previously satisfied: {', '.join(reusable)}"
                    log.info(msg)

                    # only keep dependencies that are not re-usable
                    active = set(x.pid for x in dependencies).difference(successes)
                    dependencies = list(filter(lambda x: x.pid in active, dependencies))

        handle = await submit_fn(step, dependencies)
        await SlurmLauncher.update_status(handle)

        return Task(
            step=step,
            handle=handle,
        )

    @staticmethod
    def _map_status(status: ExecutionStatus) -> Status:
        """Map SLURM execution status to CSTAR status.

        Parameters
        ----------
        status : ExecutionStatus
            The raw SLURM status.

        Returns
        -------
        Status
            The C-Star status.
        """
        match status:
            case ExecutionStatus.PENDING:
                return Status.Submitted
            case (
                ExecutionStatus.RUNNING | ExecutionStatus.ENDING | ExecutionStatus.HELD
            ):
                return Status.Running
            case ExecutionStatus.COMPLETED:
                return Status.Done
            case ExecutionStatus.CANCELLED | ExecutionStatus.TIMEOUT:
                return Status.Cancelled
            case ExecutionStatus.FAILED:
                return Status.Failed
            case _:
                return Status.Unsubmitted

    @classmethod
    async def query_status(
        cls,
        item: Task[SlurmHandle] | SlurmHandle,
    ) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        item : Task[SlurmHandle] | SlurmHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Status
            The current status of the item.
        """
        handle = item.handle if isinstance(item, Task) else item
        exec_status = await SlurmLauncher._get_status(handle.pid)

        msg = f"Retrieved status `{exec_status}` for SLURM job `{handle.pid}`"
        log.trace(msg)

        return SlurmLauncher._map_status(exec_status)

    @classmethod
    async def update_status(
        cls,
        item: Task[SlurmHandle] | SlurmHandle,
    ) -> tuple[bool, SlurmHandle]:
        """Query and update the status for a running task.

        Parameters
        ----------
        item : Task[SlurmHandle] | SlurmHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Task[SlurmHandle] | SlurmHandle
        """
        handle = item.handle if isinstance(item, Task) else item
        prior = handle.status
        current = await SlurmLauncher.query_status(item)

        if changed := (prior != current):
            handle.status = current

        return changed, handle

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

    @classmethod
    def handle_klass(cls) -> type[SlurmHandle]:
        return SlurmHandle
