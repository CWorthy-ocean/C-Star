import asyncio
import contextlib
import datetime
import os
import shlex
import shutil
import subprocess
import time
import typing as t
from pathlib import Path
from subprocess import run as sprun

from psutil import STATUS_ZOMBIE, AccessDenied, NoSuchProcess
from psutil import Process as PsProcess
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError

from cstar.base.adapter import (
    ConfiguredModelAdapter,
    CstarAdaptationError,
    ModelEnricher,
)
from cstar.base.env import ENV_CSTAR_RUNID
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.base.utils import WALLTIME_RE, additional_files_dir, utc_now
from cstar.execution.file_system import rotate_file
from cstar.orchestration.adapter import StepToRunRequestAdapter
from cstar.orchestration.formatting import ModelFormatter
from cstar.orchestration.launch.common import (
    build_attempt_log_header,
    cancel_prior_attempt,
    is_foreign_handle,
    prior_of_this_run,
    resolve_prior_attempt,
    waits_on_replaced_dependency,
)
from cstar.orchestration.models import KeyValueStore
from cstar.orchestration.orchestration import (
    Launcher,
    LiveStep,
    ProcessHandle,
    RunRequest,
    Status,
    Task,
)
from cstar.orchestration.serialization import try_deserialize
from cstar.orchestration.state import StateRepository
from cstar.system.scheduler import parse_walltime

if t.TYPE_CHECKING:
    from collections.abc import Sequence

    from cstar.orchestration.models import Step, Workplan


log = get_logger(__name__)

PROXY_POLL_DELAY: t.Final[float] = 2.0
"""Seconds between the status checks made by the local launcher's proxy script."""

CANCEL_GRACE: t.Final[float] = 5.0
"""Seconds a cancelled step's processes are given to exit before they are killed."""


def _still_running(processes: "Sequence[PsProcess]", timeout: float) -> list[PsProcess]:
    """Wait up to `timeout` seconds for processes to exit.

    A zombie has exited: one whose parent is a stopped proxy stays unreaped
    until the proxy is killed.

    Parameters
    ----------
    processes : Sequence[PsProcess]
        The processes to wait for.
    timeout : float
        The most seconds to wait.

    Returns
    -------
    list[PsProcess]
        The processes still running when they all exited or the time ran out.
    """

    def running(process: PsProcess) -> bool:
        try:
            return process.is_running() and process.status() != STATUS_ZOMBIE
        except NoSuchProcess:
            return False

    deadline = time.monotonic() + timeout
    while (alive := [p for p in processes if running(p)]) and (
        time.monotonic() < deadline
    ):
        time.sleep(0.1)
    return alive


def run_as_process(step: "Step", cmd: list[str], log_file: Path) -> dict[str, int]:
    with log_file.open("w+") as log:
        p = sprun(args=cmd, text=True, check=True, stdout=log, stderr=log)
    return {step.name: p.returncode}


class LocalHandle(ProcessHandle):
    """Handle enabling reference to a task running in local processes."""

    start_at: datetime.datetime | float | None = Field(default=None)
    """The process creation time as a posix timestamp (in seconds).

    `None` only on a handle deserialized from another launcher's sentinel."""

    _process: subprocess.Popen[bytes] = PrivateAttr()
    """The process handle (used only for simulating local processes)."""

    status: Status = Status.Unsubmitted
    """The current status of the task."""

    @property
    def start_ts(self) -> float:
        if self.start_at is None:
            msg = (
                f"Local handle for {self.name} has no start time; "
                "it was not created by the local launcher"
            )
            raise CstarExpectationFailed(msg)
        if isinstance(self.start_at, datetime.datetime):
            return self.start_at.timestamp()
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

        overrides_ = model.get("local", {})
        if not overrides_:
            if self.allow_unmodified:
                return LocalComputeSpec()
            msg = "No local overrides were supplied to the LocalComputeAdapter"
            raise CstarExpectationFailed(msg)

        try:
            compute = LocalComputeSpec.model_validate(overrides_)
        except ValidationError as ex:
            msg = f"Unable to adapt model {model!r} into LocalComputeSpec"
            raise CstarAdaptationError(msg) from ex

        if not compute.model_dump(exclude_defaults=True):
            msg = "Non-default local compute overrides were not specified."
            log.debug(msg)

        return compute


def local_compute_spec(step: "Step") -> LocalComputeSpec | None:
    """Adapt a step's compute overrides into the local compute spec it requests.

    Parameters
    ----------
    step : Step
        The step whose compute overrides are inspected.

    Returns
    -------
    LocalComputeSpec | None
        The spec, or `None` if the step carries no usable local overrides; such
        a step runs without a walltime.
    """
    if not step.compute_overrides:
        return None

    try:
        return LocalComputeAdapter().adapt(step.compute_overrides)
    except CstarExpectationFailed:
        # overrides carry nothing for this launcher (e.g. the slurm
        # cpu requirement the workplan transformer records)
        msg = f"No local overrides for step {step.name!r}"
        log.debug(msg)
    except CstarAdaptationError:
        msg = f"Local overrides did not result in valid compute spec: {step.compute_overrides}"
        log.warning(msg, exc_info=True)
    return None


def find_timeout_exe() -> str | None:
    """Locate a GNU `timeout` executable.

    macOS has no `timeout`; Homebrew's coreutils installs it as `gtimeout`.

    Returns
    -------
    str | None
        The path of the first executable found, or `None`.
    """
    return shutil.which(TimeConstrainedRunRequestEnricher.TIMEOUT_EXE) or shutil.which(
        f"g{TimeConstrainedRunRequestEnricher.TIMEOUT_EXE}"
    )


TIMEOUT_MISSING_GUIDANCE = (
    "install coreutils (conda install coreutils, or brew install coreutils "
    "for gtimeout) or remove the walltime."
)


class TimeConstrainedRunRequestEnricher(ModelEnricher[RunRequest]):
    """Format a `RunRequest` as a CLI command that honors user-supplied
    computing resource overrides.
    """

    compute: LocalComputeSpec
    """The compute spec to use when enriching a run request."""

    TIMEOUT_EXE: t.ClassVar[str] = "timeout"
    """The preferred executable used to timeout a process; see `find_timeout_exe`."""
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

        Raises
        ------
        CstarExpectationFailed
            If no GNU timeout executable can be found.
        """
        if not (timeout_exe := find_timeout_exe()):
            msg = f"A local walltime needs GNU timeout; {TIMEOUT_MISSING_GUIDANCE}"
            raise CstarExpectationFailed(msg)

        # GNU timeout stops option parsing at the first non-option argument (the
        # duration), so -k must precede it: `timeout -k 2s 600s cmd ...`.
        # make the path absolute because the step runs from its own run directory;
        # the formatters quote every token when they render the command
        enriched_cmd = [
            os.path.abspath(timeout_exe),
            self.ARG_FORCEKILL_TIMEOUT,
            f"{self.compute.force_kill_seconds}s",
            f"{self.compute.walltime_seconds}s",
            *model.command,
        ]

        return RunRequest(
            command=enriched_cmd,
            environment=model.environment,
        )


class LocalLauncher(Launcher[LocalHandle]):
    """A launcher that executes steps in a local process."""

    name: t.ClassVar[str] = "local"
    """Value recorded as `ProcessHandle.launcher_name` on handles this launcher creates."""

    tasks: t.ClassVar[dict[str, str]] = {}
    """Mapping of task name to process ID."""

    @classmethod
    def check_preconditions(cls, workplan: "Workplan | None" = None) -> None:
        """Perform launcher-specific startup validation.

        Parameters
        ----------
        workplan : Workplan, optional
            The workplan about to run.

        Raises
        ------
        CstarExpectationFailed
            If steps request a local walltime and no GNU timeout is installed.
        """
        if workplan is None or find_timeout_exe():
            return

        if names := [s.name for s in workplan.steps if local_compute_spec(s)]:
            msg = (
                f"Steps {', '.join(map(repr, names))} set a local walltime, which "
                f"needs GNU timeout; {TIMEOUT_MISSING_GUIDANCE}"
            )
            raise CstarExpectationFailed(msg)

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
        formatter: ModelFormatter[RunRequest] = ProxiedRunRequestFormatter(
            step, dependencies
        )

        compute = local_compute_spec(step)
        enricher: ModelEnricher[RunRequest] | None = (
            TimeConstrainedRunRequestEnricher(compute) if compute else None
        )

        adapter = StepToRunRequestAdapter(enricher)
        request = adapter.adapt(step)

        return formatter.format(request)

    @staticmethod
    async def _submit(
        step: "LiveStep",
        dependencies: list[LocalHandle],
        *,
        after_pre_run: bool = False,
    ) -> LocalHandle:
        """Submit a step to a local process.

        Parameters
        ----------
        step : LiveStep
            The step to execute in a local process.
        dependencies : list[LocalHandle]
            The list of tasks that must complete prior to execution of the submitted Step.
        after_pre_run : bool
            Whether this submission launches a real run from a completed pre-run.

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

        run_id = str(os.getenv(ENV_CSTAR_RUNID, ""))
        rotated_log = rotate_file(step.log_path)
        header = build_attempt_log_header(
            step.name,
            run_id,
            rotated_log,
            resume=step.resume,
            after_pre_run=after_pre_run,
        )
        step.log_path.write_text(header)

        try:
            if not step.fsm.root_dir.exists():
                step.fsm.prepare()

            cmd = ["sh", str(step.script_path)]

            local_process = subprocess.Popen(
                cmd,
                cwd=step.fsm.run_dir,
                stdin=subprocess.PIPE,
                stdout=step.log_path.open("a"),
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            # the wall clock, unlike the process creation time read below: on Linux
            # that is up to a second early, and submission times are compared with
            # the start of an attempt
            submitted_at = utc_now()

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
                    run_id=run_id,
                    launcher_name=LocalLauncher.name,
                    start_at=create_time,
                    status=Status.Submitted,
                    pre_run=step.pre_run,
                    submitted_at=submitted_at,
                )

                handle.process = local_process
                return handle

        finally:
            ...

        msg = "Unable to retrieve process ID for local process."
        raise RuntimeError(msg)

    @staticmethod
    def _process_of(handle: LocalHandle) -> PsProcess | None:
        """Return the running OS process recorded on a handle, if there is one.

        The creation time is compared with the recorded `start_at` so a
        recycled PID belonging to an unrelated process is not mistaken for
        the step.

        Parameters
        ----------
        handle : LocalHandle
            A handle created by the local launcher.

        Returns
        -------
        PsProcess | None
            The step's proxy process, or `None` when it has exited.
        """
        try:
            process = PsProcess(int(handle.pid))
            if process.status() == STATUS_ZOMBIE:
                return None
            if abs(process.create_time() - handle.start_ts) <= 2.0:
                return process
        except (NoSuchProcess, AccessDenied, ValueError):
            pass
        return None

    @staticmethod
    def _is_alive(handle: LocalHandle) -> bool:
        """Return `True` if the OS process recorded on a handle is still running.

        Parameters
        ----------
        handle : LocalHandle
            A deserialized handle with no live `Popen` attached.

        Returns
        -------
        bool
        """
        return LocalLauncher._process_of(handle) is not None

    @staticmethod
    def _terminate(handle: LocalHandle) -> bool:
        """Stop a step's proxy and every process it started.

        The proxy is stopped first and killed last: stopped, it starts nothing
        more, and cannot record the step `Failed` in the sentinel, which the
        attempt replacing it owns, when its descendants exit. They are asked to
        terminate and are killed after `CANCEL_GRACE` seconds. If one outlives
        that, the proxy is left stopped, so the step still reads as in progress
        and is not submitted again beside it.

        Parameters
        ----------
        handle : LocalHandle
            A handle created by the local launcher.

        Returns
        -------
        bool
            `True` once every process has exited; `False` when the proxy was
            not running or a process could not be stopped.
        """
        if (proxy := LocalLauncher._process_of(handle)) is None:
            log.debug(f"Process {handle.pid} of step {handle.name!r} is not running")
            return False

        try:
            proxy.suspend()
            descendants = proxy.children(recursive=True)
            for process in descendants:
                with contextlib.suppress(NoSuchProcess):
                    process.terminate()
            if alive := _still_running(descendants, CANCEL_GRACE):
                for process in alive:
                    with contextlib.suppress(NoSuchProcess):
                        process.kill()
                alive = _still_running(alive, CANCEL_GRACE)
        except NoSuchProcess:
            log.debug(f"Process {handle.pid} of step {handle.name!r} ended by itself")
            return False
        except AccessDenied as ex:
            log.warning(f"Unable to stop the processes of step {handle.name!r}: {ex}")
            return False

        if alive:
            pids = ", ".join(str(p.pid) for p in alive)
            log.warning(
                f"Processes {pids} of step {handle.name!r} outlived the kill; its "
                f"process {handle.pid} is left stopped"
            )
            return False

        try:
            proxy.kill()
            if handle.is_expired:
                alive = _still_running([proxy], CANCEL_GRACE)
            else:
                # reaped through the Popen, which then records the exit
                handle.process.wait(timeout=CANCEL_GRACE)
        except NoSuchProcess:
            pass
        except subprocess.TimeoutExpired:
            alive = [proxy]

        if alive:
            log.warning(
                f"Process {handle.pid} of step {handle.name!r} outlived the kill"
            )
        return not alive

    @staticmethod
    def _terminal_status(status: Status) -> str:
        """Map a terminal status to the name used by `_status`."""
        if status == Status.Failed:
            return "FAILED"
        if status == Status.Cancelled:
            return "CANCELLED"
        return "COMPLETED"

    @staticmethod
    async def _finalized_status(handle: LocalHandle) -> Status | None:
        """Read the terminal status the step's proxy recorded for this attempt.

        Parameters
        ----------
        handle : LocalHandle
            A handle read from an earlier attempt's sentinel.

        Returns
        -------
        Status | None
            The status of the sentinel on disk when it is terminal and was written
            for the same process (same pid and start time); `None` when it is
            missing, unreadable, not terminal, or belongs to another attempt.
        """
        path = StateRepository.sentinel_path(handle, run_id=handle.run_id)
        on_disk = await asyncio.to_thread(try_deserialize, path, LocalHandle)
        if (
            on_disk is not None
            and (on_disk.pid, on_disk.start_at) == (handle.pid, handle.start_at)
            and Status.is_terminal(on_disk.status)
        ):
            return on_disk.status
        return None

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
            # a reused (deserialized) handle from a prior attempt has no live
            # Popen: report its persisted terminal outcome, and treat a
            # non-terminal status whose process is gone as a failure so a
            # step killed before finalizing its sentinel is re-run, not adopted
            if Status.is_terminal(handle.status):
                return LocalLauncher._terminal_status(handle.status)
            if LocalLauncher._is_alive(handle):
                # the proxy records Running once its dependencies are done
                return "RUNNING" if Status.is_running(handle.status) else "PENDING"
            # the proxy writes its final status before it exits, so a handle read
            # while the step ran can outlive it: re-read the sentinel, or the
            # stale status would be persisted over the finished step's
            if (final := await LocalLauncher._finalized_status(handle)) is not None:
                return LocalLauncher._terminal_status(final)
            msg = (
                f"Process {handle.pid} for step {handle.name!r} is gone but its "
                f"status was never finalized ({handle.status.name}); treating as failed."
            )
            log.warning(msg)
            return "FAILED"

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
        *,
        run_uuid: str,
    ) -> Task[LocalHandle]:
        """Launch a step in local process.

        Parameters
        ----------
        step : LiveStep
            The step to run in a local process.
        dependencies : list[LocalHandle]
            The list of tasks that must complete prior to execution of the submitted Step.
        run_uuid : str
            The uuid of the run's manifest; see `Launcher.launch`.

        Returns
        -------
        Task[LocalHandle]
            A Task containing information about the newly submitted job.

        Raises
        ------
        CstarError
            If a dependency was created by another launcher or run: a local
            process cannot wait on it.
        CstarExpectationFailed
            If a dependency failed, or a prior attempt still in progress is
            replaced and cannot be cancelled (see `cancel_prior_attempt`).
        """
        run_id = os.getenv(ENV_CSTAR_RUNID, "")
        if outsiders := [
            h
            for h in dependencies
            if is_foreign_handle(h, cls.name) or h.run_id != run_id
        ]:
            names = ", ".join(f"{h.name!r} (run {h.run_id!r})" for h in outsiders)
            msg = (
                f"Step {step.name!r} cannot wait on {names}: the local launcher "
                "cannot wait on a step of another run, so external steps must "
                "be Done"
            )
            raise CstarError(msg)

        tasks = [asyncio.Task(cls.query_status(h)) for h in dependencies]
        statuses = await asyncio.gather(*tasks)

        # in-progress dependencies are awaited by the submitted proxy script
        failure_found = any(map(Status.is_failure, statuses))

        if failure_found:
            msg = f"Dependency of step {step.name} failed. Unable to continue."
            raise CstarExpectationFailed(msg)

        live_step = LiveStep.from_step(step)

        state_repo = StateRepository()
        prior_handle = prior_of_this_run(
            await state_repo.get_sentinel(live_step.name, LocalHandle), run_uuid
        )
        last_status: Status = Status.Unsubmitted
        reuse_prior: bool = False

        if prior_handle:
            last_status = await LocalLauncher.query_status(prior_handle)
            reuse_prior = resolve_prior_attempt(
                live_step,
                last_status,
                prior_pre_run=prior_handle.pre_run,
                prior_foreign=is_foreign_handle(prior_handle, cls.name),
                replaced_dependency=waits_on_replaced_dependency(
                    prior_handle, dependencies
                ),
            )
            if not reuse_prior and Status.is_in_progress(last_status):
                await cancel_prior_attempt(cls, live_step, prior_handle)

        if reuse_prior and prior_handle:
            handle = prior_handle
            handle.status = last_status
        else:
            handle = await LocalLauncher._submit(
                live_step,
                dependencies,
                after_pre_run=bool(
                    prior_handle and prior_handle.pre_run and not live_step.pre_run
                ),
            )

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
        if is_foreign_handle(handle, cls.name):
            msg = (
                f"Handle for {handle.name!r} was created by the "
                f"{handle.launcher_name!r} launcher; using its persisted status"
            )
            log.debug(msg)
            return handle.status

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

        Stops the step's proxy and every process it started (see `_terminate`),
        whether the handle was created by this process or read from a sentinel.

        Parameters
        ----------
        item : Task[LocalHandle]
            A task to cancel.

        Returns
        -------
        Task[LocalHandle]
            The task after the cancellation attempt has completed; its status is
            `Cancelled` only when every process of the step has exited.
        """
        handle = item.handle
        if not handle.is_expired and handle.process.poll() is not None:
            log.debug(f"Unable to cancel a completed task `{handle.pid}`")
            return item

        if await asyncio.to_thread(LocalLauncher._terminate, handle):
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

    @t.override
    def _to_string(self, value: RunRequest) -> str:
        """Create a script that will execute the desired command for a
        `Step` while also waiting for any dependencies to complete.

        Returns
        -------
        str
        """
        pids = " "
        dep_sentinels = " "
        if self.dependencies:
            pids = " ".join([f'"{h.pid}"' for h in self.dependencies])
            dep_sentinels = " ".join(
                [
                    f'"{StateRepository.sentinel_path(h.name)}"'
                    for h in self.dependencies
                ]
            )

        declarations = [f"export {k}='{v}'\n" for k, v in value.environment.items()]
        env_vars = " ".join(declarations)

        proxyscript_model = {
            "{sentinel_path}": str(StateRepository.sentinel_path(self.step.name)),
            "{blueprint_path}": str(self.step.blueprint_path),
            "{pids}": pids,
            "{dep_sentinels}": dep_sentinels,
            "{running}": str(Status.Running.value),
            "{done}": str(Status.Done.value),
            "{failed}": str(Status.Failed.value),
            "{delay}": str(PROXY_POLL_DELAY),
            "{command}": " ".join(shlex.quote(token) for token in value.command),
            "{env_vars}": env_vars,
        }
        files_dir = additional_files_dir()
        proxy_tpl_path = files_dir / "templates/launchers/local_job_proxy.sh"
        content = proxy_tpl_path.read_text()
        for k, v in proxyscript_model.items():
            content = content.replace(k, v)

        return content
