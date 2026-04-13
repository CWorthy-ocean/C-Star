import asyncio
import datetime
import multiprocessing as _mp
import typing as t
from subprocess import run as sprun

from psutil import NoSuchProcess
from psutil import Process as PsProcess
from pydantic import PrivateAttr

from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.orchestration.converter.converter import get_command_mapping
from cstar.orchestration.models import Application
from cstar.orchestration.orchestration import (
    Launcher,
    LiveStep,
    ProcessHandle,
    Status,
    Task,
)
from cstar.orchestration.state import put_sentinel

if t.TYPE_CHECKING:
    from cstar.orchestration.models import Step


log = get_logger(__name__)

mp = _mp.get_context("forkserver")


def run_as_process(step: "Step", cmd: list[str]) -> dict[str, int]:
    p = sprun(args=cmd, text=True, check=True)
    return {step.name: p.returncode}


class LocalHandle(ProcessHandle):
    """Handle enabling reference to a task running in local processes."""

    start_at: datetime.datetime | float
    """The process creation time as a posix timestamp (in seconds)."""

    _process: _mp.Process = PrivateAttr()
    """The process handle (used only for simulating local processes)."""

    status: Status = Status.Unsubmitted
    """The current status of the task."""

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
        now = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        return now - self.start_ts

    @property
    def process(self) -> _mp.Process:
        return self._process

    @process.setter
    def process(self, value: _mp.Process) -> None:
        self.status = Status.Submitted
        self._process = value

    @property
    def safe_name(self) -> str:
        return slugify(self.name)


class LocalLauncher(Launcher[LocalHandle]):
    """A launcher that executes steps in a local process."""

    @classmethod
    def check_preconditions(cls) -> None:
        """Perform launcher-specific startup validation."""

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
        step_converter = get_command_mapping(
            Application(step.application),
            LocalLauncher,
        )
        cmd = step_converter(step)

        try:
            if not step.fsm.root.exists():
                step.fsm.prepare()

            mp_process = mp.Process(
                target=run_as_process,
                name=step.safe_name,
                args=(step, cmd.split()),
                daemon=True,
            )
            mp_process.start()
            create_time = datetime.datetime.now(tz=datetime.timezone.utc)
            if pid := mp_process.pid:
                print(f"Local run of `{step.application}` created pid: {pid}")

                try:
                    ps_process = PsProcess(pid)
                    create_timestamp = ps_process.create_time()
                    create_time = datetime.datetime.fromtimestamp(
                        create_timestamp, tz=datetime.timezone.utc
                    )
                except NoSuchProcess:
                    print(f"Unable to retrieve exact start time for pid: {pid}")

                handle = LocalHandle(
                    pid=str(pid),
                    name=step.safe_name,
                    start_at=create_time,
                    status=Status.Submitted,
                )

                handle.process = mp_process  # type: ignore[assignment]
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
        rc = handle.process.exitcode

        if rc is None:
            status = "RUNNING"
        elif rc == 0:
            status = "COMPLETED"
            log.debug(f"Return code for handle `{handle}` is `{rc}`.")
        else:
            status = "FAILED"
            log.warning(f"Failure code for handle `{handle}` is `{rc}`.")

        return status

    @classmethod
    async def launch(cls, step: "LiveStep", dependencies: list[LocalHandle]) -> Task:
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
        active_found = any(map(Status.is_in_progress, statuses))
        failure_found = any(map(Status.is_failure, statuses))

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
        return Task(
            status=Status.Submitted,
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
    ) -> Task[LocalHandle] | LocalHandle:
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

        if prior != current:
            handle.status = current
            await put_sentinel(handle)

        return item

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

        if process is not None:
            if process.exitcode is not None:
                # can't cancel a completed process
                print(f"Unable to cancel a completed task `{process.pid}")
            else:
                process.kill()
                item.status = Status.Cancelled

        return item
