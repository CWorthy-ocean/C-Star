import asyncio
import datetime
import os
import subprocess
import typing as t
from pathlib import Path
from subprocess import run as sprun

from psutil import NoSuchProcess
from psutil import Process as PsProcess
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from cstar.base.adapter import (
    ConfiguredModelAdapter,
    CstarAdaptationError,
    ModelEnricher,
)
from cstar.base.env import ENV_CSTAR_ORCH_LOCAL_DELAY, ENV_CSTAR_RUNID, get_env_item
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.feature import ENV_FF_ENABLE_LOCAL_PROXY, is_feature_enabled
from cstar.base.log import get_logger
from cstar.base.utils import WALLTIME_RE, additional_files_dir
from cstar.orchestration.adapter import StepToRunRequestAdapter
from cstar.orchestration.formatting import ModelFormatter
from cstar.orchestration.models import KeyValueStore
from cstar.orchestration.orchestration import (
    Launcher,
    LiveStep,
    ProcessHandle,
    RunRequest,
    RunRequestScriptFormatter,
    Status,
    Task,
)
from cstar.orchestration.state import StateRepository
from cstar.system.scheduler import parse_walltime

if t.TYPE_CHECKING:
    from cstar.orchestration.models import Step


log = get_logger(__name__)


def run_as_process(step: "Step", cmd: list[str], log_file: Path) -> dict[str, int]:
    with log_file.open("w+") as log:
        p = sprun(args=cmd, text=True, check=True, stdout=log, stderr=log)
    return {step.name: p.returncode}


class LocalHandle(ProcessHandle):
    """Handle enabling reference to a task running in local processes."""

    start_at: datetime.datetime | float
    """The process creation time as a posix timestamp (in seconds)."""

    _process: subprocess.Popen[bytes] = PrivateAttr()
    """The process handle (used only for simulating local processes)."""

    status: Status = Status.Unsubmitted
    """The current status of the task."""

    launcher_name: str = "local"
    """The launcher used to launch the process."""

    @property
    def start_ts(self) -> float:
        if isinstance(self.start_at, datetime.datetime):
            self.start_at = self.start_at.timestamp()
        return self.start_at

    @property
    def elapsed(self) -> float:
        """The number of seconds passed since the task was started.

        Returns
        -------
        float
        """
        now = datetime.datetime.now(tz=datetime.UTC).timestamp()
        return now - self.start_ts

    @property
    def process(self) -> subprocess.Popen[bytes]:
        return self._process

    @process.setter
    def process(self, value: subprocess.Popen[bytes]) -> None:
        self.status = Status.Submitted
        self._process = value

    @property
    def is_expired(self) -> bool:
        return not hasattr(self, "_process")


class LocalComputeSpec(BaseModel):
    """Compute configuration options when using the local launcher."""

    DEFAULT_MAX_WALLTIME: t.ClassVar[str] = "00:10:00"
    DEFAULT_FK_TIMEOUT: t.ClassVar[str] = "00:00:02"

    max_walltime: str = Field(default=DEFAULT_MAX_WALLTIME, pattern=WALLTIME_RE)
    """Maximum amount of time a process should be allowed to run (D-HH:MM:SS format)."""
    force_kill_timeout: str = Field(default=DEFAULT_FK_TIMEOUT, pattern=WALLTIME_RE)
    """Grace period before force-killing a local process after timeout is exceeded (D-HH:MM:SS format)."""

    model_config: t.ClassVar[ConfigDict] = ConfigDict(str_strip_whitespace=True)
    """Configure model to ignore empty strings."""

    @property
    def walltime_seconds(self) -> int:
        hh, mm, ss = parse_walltime(self.max_walltime)
        return hh * 3600 + mm * 60 + ss

    @property
    def force_kill_seconds(self) -> int:
        hh, mm, ss = parse_walltime(self.force_kill_timeout)
        return hh * 3600 + mm * 60 + ss


class LocalComputeAdapter(ConfiguredModelAdapter[KeyValueStore, LocalComputeSpec]):
    """Adapts a `KeyValueStore` containing optional overrides into a `LocalComputeSpec`."""

    allow_unmodified: bool = False
    """When set to `True` the adapter will return a default LocalComputeSpec if
    no user-supplied customizations were supplied.
    """

    def __init__(self, allow_unmodified: bool = False) -> None:
        """Initialize the adapter.

        Parameters
        ----------
        allow_unmodified : bool
            Pass `True` to return a default `LocalComputeSpec` instance when no custom
            configuration is supplied.
        """
        self.allow_unmodified = allow_unmodified

    def adapt(self, model: KeyValueStore) -> LocalComputeSpec:
        """Adapt the input into a `LocalComputeSpec`.

        Parameters
        ----------
        model : KeyValueStore
            The `KeyValueStore` to be adapted.

        Returns
        -------
        LocalComputeSpec

        Raises
        ------
        CstarExpectationFailed
            If the input cannot be used to attempt adaptation.
        CstarAdaptationError
            If the input cannot be successfully adapted to the target type.
        """
        if not model:
            msg = "Compute overrides were not supplied to the LocalComputeAdapter"
            raise CstarExpectationFailed(msg)

        if overrides_ := model.get("local", {}):
            compute = LocalComputeSpec.model_validate(overrides_)

            if not compute.model_dump(exclude_defaults=True):
                msg = "Non-default local compute overrides were not specified."
                log.debug(msg)

            return compute

        if self.allow_unmodified:
            return LocalComputeSpec()

        msg = f"Unable to adapt model {model!r} into LocalComputeSpec"
        raise CstarAdaptationError(msg)


class TimeConstrainedRunRequestEnricher(ModelEnricher[RunRequest]):
    """Format a `RunRequest` as a CLI command that honors user-supplied
    computing resource overrides.
    """

    compute: LocalComputeSpec
    """The compute spec to use when enriching a run request."""

    TIMEOUT_EXE: t.ClassVar[str] = "timeout"
    """The executable used to timeout a process."""
    ARG_FORCEKILL_TIMEOUT: t.ClassVar[str] = "-k"
    """A CLI argument used to specify a grace period before force-killing a run."""

    def __init__(self, compute: LocalComputeSpec) -> None:
        """Enrich the run request to support constraining the time allotted for the request
        to complete.

        Parameters
        ----------
        compute : LocalComputeSpec | None
            Local compute overrides used to configure timeout behavior.
        """
        self.compute = compute

    def enrich(
        self,
        model: RunRequest,
    ) -> RunRequest:
        """Enrich the run request to support constraining the time allotted for the request
        to complete.

        Configures the total duration and force-kill grace period of a command run via `timeout`.

        Parameters
        ----------
        model : RunRequest
            The  original `RunRequest` to enrich
        compute : LocalComputeSpec | None
            Local compute overrides used to configure timeout behavior.
        """
        enriched_cmd = [
            self.TIMEOUT_EXE,
            f"{self.compute.walltime_seconds}s",
            self.ARG_FORCEKILL_TIMEOUT,
            f"{self.compute.force_kill_seconds}s",
            *model.command,
        ]

        return RunRequest(
            command=enriched_cmd,
            environment=model.environment,
        )


class LocalLauncher(Launcher[LocalHandle]):
    """A launcher that executes steps in a local process."""

    tasks: t.ClassVar[dict[str, str]] = {}
    """Mapping of task name to process ID."""
    use_proxy: t.ClassVar[bool] = is_feature_enabled(ENV_FF_ENABLE_LOCAL_PROXY)
    """Set flag to `True` to use a proxy script to enable asynchronous scheduling."""

    @classmethod
    def check_preconditions(cls) -> None:
        """Perform launcher-specific startup validation."""

    @staticmethod
    def adapt_step(
        step: "LiveStep",
        dependencies: list[LocalHandle],
    ) -> str:
        """Create a script that will execute the desired command for a
        `Step` while also waiting for any dependencies to complete.

        Returns
        -------
        str
        """
        formatter: ModelFormatter[RunRequest] = RunRequestScriptFormatter()
        if LocalLauncher.use_proxy:
            formatter = ProxiedRunRequestFormatter(step, dependencies)

        enricher: ModelEnricher[RunRequest] | None = None

        if step.compute_overrides:
            try:
                if step.compute_overrides:
                    compute = LocalComputeAdapter().adapt(step.compute_overrides)
                    enricher = TimeConstrainedRunRequestEnricher(compute)
            except CstarAdaptationError:
                msg = f"Local overrides did not result in valid compute spec: {step.compute_overrides}"
                log.warning(msg, exc_info=True)

        adapter = StepToRunRequestAdapter(enricher)
        request = adapter.adapt(step)

        return formatter.format(request)

    @staticmethod
    async def _submit(step: "LiveStep", dependencies: list[LocalHandle]) -> LocalHandle:
        """Submit a step to a local process.

        Parameters
        ----------
        step : LiveStep
            The step to execute in a local process.
        dependencies : list[LocalHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        LocalHandle | None
            A ProcessHandle identifying the newly submitted job.
        """
        script = LocalLauncher.adapt_step(step, dependencies)

        step.fsm.prepare()
        step.script_path.write_text(script)
        log.debug(f"Created run script at path: {step.script_path}")
        log_file = step.log_path

        try:
            if not step.fsm.root_dir.exists():
                step.fsm.prepare()

            cmd = ["sh", str(step.script_path)]

            local_process = subprocess.Popen(
                cmd,
                cwd=step.fsm.run_dir,
                stdin=subprocess.PIPE,
                stdout=step.log_path.open("w"),
                stderr=subprocess.STDOUT,
            )

            create_time = datetime.datetime.now(tz=datetime.UTC)

            if pid := local_process.pid:
                msg = f"Local run of {step.application!r} created pid: {pid}"
                log.debug(msg)
                msg = f"Logs for step {step.safe_name!r} can be found at: {log_file}"
                log.info(msg)
                LocalLauncher.tasks[step.name] = str(pid)

                try:
                    ps_process = PsProcess(pid)
                    create_timestamp = ps_process.create_time()
                    create_time = datetime.datetime.fromtimestamp(
                        create_timestamp, tz=datetime.UTC
                    )
                except NoSuchProcess:
                    msg = f"Unable to retrieve exact start time for pid: {pid}"
                    log.debug(msg)

                handle = LocalHandle(
                    pid=str(pid),
                    name=step.name,
                    run_id=str(os.getenv(ENV_CSTAR_RUNID, "")),
                    start_at=create_time,
                    status=Status.Submitted,
                )

                handle.process = local_process
                return handle

        finally:
            ...

        msg = "Unable to retrieve process ID for local process."
        raise RuntimeError(msg)

    @staticmethod
    async def _status(handle: LocalHandle) -> str:
        """Retrieve the status of a step running in local process.

        Parameters
        ----------
        handle : LocalHandle
            A handle object for a process-based task.

        Returns
        -------
        str
            The current status of the step.
        """
        if handle.is_expired:
            if not Status.is_terminal(handle.status):
                return "RUNNING"
            return "COMPLETED"

        # poll() reaps the child and records its exit code; reading
        # `returncode` alone never observes an exit the process made on
        # its own, leaving the task RUNNING forever.
        rc = handle.process.poll()

        if rc is None:
            status = "RUNNING"
        elif rc == 0:
            status = "COMPLETED"
            msg = f"Return code for handle {handle!r} is `{rc}`."
            log.debug(msg)
        else:
            status = "FAILED"
            msg = f"Failure code for handle {handle!r} is `{rc}`."
            log.warning(msg)

        return status

    @classmethod
    async def launch(
        cls,
        step: "LiveStep",
        dependencies: list[LocalHandle],
    ) -> Task[LocalHandle]:
        """Launch a step in local process.

        Parameters
        ----------
        step : LiveStep
            The step to run in a local process.
        dependencies : list[LocalHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        Task[LocalHandle]
            A Task containing information about the newly submitted job.
        """
        tasks = [asyncio.Task(cls.query_status(h)) for h in dependencies]
        statuses = await asyncio.gather(*tasks)

        failure_found = any(map(Status.is_failure, statuses))

        if not LocalLauncher.use_proxy:
            # without the proxy, the launcher must sit and wait for processes to end.
            active_found = any(map(Status.is_in_progress, statuses))

            # wait for the dependencies to complete before launching
            while active_found and not failure_found:
                await asyncio.sleep(1)

                tasks = [asyncio.Task(cls.query_status(h)) for h in dependencies]
                statuses = await asyncio.gather(*tasks)
                active_found = any(map(Status.is_in_progress, statuses))
                failure_found = any(map(Status.is_failure, statuses))

        if failure_found:
            msg = f"Dependency of step {step.name} failed. Unable to continue."
            raise CstarExpectationFailed(msg)

        live_step = LiveStep.from_step(step)
        handle = await LocalLauncher._submit(live_step, dependencies)
        return Task[LocalHandle](
            step=live_step,
            handle=handle,
        )

    @classmethod
    async def query_status(cls, item: Task[LocalHandle] | LocalHandle) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        item : Task[LocalHandle] | LocalHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Status
            The current status of the item.
        """
        handle = item.handle if isinstance(item, Task) else item
        raw_status = await LocalLauncher._status(handle)

        match raw_status:
            case "PENDING":
                return Status.Submitted
            case "RUNNING" | "ENDING":
                return Status.Running
            case "COMPLETED":
                return Status.Done
            case "CANCELLED":
                return Status.Cancelled
            case "FAILED":
                return Status.Failed
            case _:
                return Status.Unsubmitted

    @classmethod
    async def update_status(
        cls,
        item: Task[LocalHandle] | LocalHandle,
    ) -> tuple[bool, LocalHandle]:
        """Query and update the status for a running task.

        Parameters
        ----------
        item : Task[LocalHandle] | LocalHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Task[LocalHandle] | LocalHandle
        """
        handle = item.handle if isinstance(item, Task) else item
        prior = handle.status
        current = await LocalLauncher.query_status(item)

        if changed := prior != current:
            handle.status = current

        return changed, handle

    @classmethod
    async def cancel(cls, item: Task[LocalHandle]) -> Task[LocalHandle]:
        """Cancel a task, if possible.

        Parameters
        ----------
        item : Task or ProcessHandle
            A task or process handle to cancel.

        Returns
        -------
        Task[LocalHandle]
            The task after the cancellation attempt has completed.
        """
        process = item.handle.process

        if not item.handle.is_expired:  # wonky is-null check...
            if process.returncode is not None:
                msg = f"Unable to cancel a completed task `{process.pid}"
                log.debug(msg)
            else:
                process.kill()
                item.status = Status.Cancelled

        return item

    @classmethod
    def handle_klass(cls) -> type[LocalHandle]:
        return LocalHandle


class ProxiedRunRequestFormatter(ModelFormatter[RunRequest]):
    """Format a `RunRequest` as script content that will proxy the original command
    to enable the local launcher to support status checks and dependencies.
    """

    dependencies: list[LocalHandle]
    step: "LiveStep"
    updates: dict[str, str]

    def __init__(
        self,
        step: LiveStep,
        dependencies: list[LocalHandle] | None = None,
        updates: dict[str, str] | None = None,
    ) -> None:
        super().__init__()
        if not step:
            msg = "Step is required for formatting"
            raise CstarExpectationFailed(msg)

        self.dependencies = dependencies or []
        self.step = step
        self.updates = updates or {}
        self.delay = ""

    @t.override
    def _to_string(self, value: RunRequest) -> str:
        """Create a script that will execute the desired command for a
        `Step` while also waiting for any dependencies to complete.

        Returns
        -------
        str
        """
        pids = " "
        if self.dependencies:
            pids = " ".join([f'"{h.pid}"' for h in self.dependencies])

        delay = get_env_item(ENV_CSTAR_ORCH_LOCAL_DELAY).value
        declarations = [f"export {k}='{v}'\n" for k, v in value.environment.items()]
        env_vars = " ".join(declarations)

        proxyscript_model = {
            "sentinel_path": str(StateRepository.sentinel_path(self.step.name)),
            "blueprint_path": str(self.step.blueprint_path),
            "pids": pids,
            "running": str(Status.Running.value),
            "done": str(Status.Done.value),
            "failed": str(Status.Failed.value),
            "delay": delay,
            "command": " ".join(value.command),
            "env_vars": env_vars,
        }
        files_dir = additional_files_dir()
        proxy_tpl_path = files_dir / "templates/launchers/local_job_proxy.sh"

        tpl = proxy_tpl_path.read_text()
        return tpl.format(**proxyscript_model)
