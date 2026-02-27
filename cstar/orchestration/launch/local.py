import asyncio
import datetime
import typing as t
from multiprocessing import Process as MpProcess
from subprocess import run as sprun

from psutil import NoSuchProcess
from psutil import Process as PsProcess

from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.utils import slugify
from cstar.orchestration.converter.converter import get_command_mapping
from cstar.orchestration.models import Application
from cstar.orchestration.orchestration import Launcher, ProcessHandle, Status, Task

if t.TYPE_CHECKING:
    from cstar.orchestration.models import Step
    from cstar.orchestration.orchestration import LiveStep


def run_as_process(step: "Step", cmd: list[str]) -> dict[str, int]:
    p = sprun(args=cmd, text=True, check=False)
    return {step.name: p.returncode}


class LocalHandle(ProcessHandle):
    """Handle enabling reference to a task running in local processes."""

    process: MpProcess
    """The process handle (used only for simulating local processes)."""
    start_at: float
    """The process creation time as a posix timestamp (in seconds)."""

    def __init__(
        self,
        step: "Step",
        process: MpProcess,
        pid: int,
        start_at: datetime.datetime | float,
    ) -> None:
        """Initialize the local handle.

        Parameters
        ----------
        step : Step
            The step used to create the task.
        pid : int
            The process ID.
        start_at : datetime
            The process start time.
        """
        super().__init__(pid=str(pid))

        self.step = step
        self.process = process
        self.start_at = (
            start_at.timestamp()
            if isinstance(start_at, datetime.datetime)
            else start_at
        )

    @property
    def elapsed(self) -> float:
        """The number of seconds passed since the task was started.

        Returns
        -------
        float
        """
        now = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        return now - self.start_at


class LocalLauncher(Launcher[LocalHandle]):
    """A launcher that executes steps in a local process."""

    @staticmethod
    async def _submit(step: "Step", dependencies: list[LocalHandle]) -> LocalHandle:
        """Submit a step to SLURM as a new batch allocation.

        Parameters
        ----------
        step : Step
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
            mp_process = MpProcess(
                target=run_as_process,
                name=slugify(step.name),
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

                return LocalHandle(
                    step,
                    mp_process,
                    pid,
                    create_time,
                )
        finally:
            ...

        msg = "Unable to retrieve process ID for local process."
        raise RuntimeError(msg)

    @staticmethod
    async def _status(step: "Step", handle: LocalHandle) -> str:
        """Retrieve the status of a step running in local process.

        Parameters
        ----------
        step : Step
            The step triggering the job.
        handle : LocalHandle
            A handle object for a process-based task.

        Returns
        -------
        str
            The current status of the step.
        """
        # await LocalLauncher._update_processes()
        rc = handle.process.exitcode

        if rc is None:
            status = "RUNNING"
        elif rc == 0:
            status = "COMPLETED"
            print(f"Return code for pid `{handle.pid}` is `{rc}` for `{step.name}`")
        else:
            status = "FAILED"
            print(f"Failure code for pid `{handle.pid}` is `{rc}` for `{step.name}`")

        return status

    @classmethod
    async def launch(cls, step: "LiveStep", dependencies: list[LocalHandle]) -> Task:
        """Launch a step in local process.

        Parameters
        ----------
        step : Step
            The step to run in a local process.
        dependencies : list[LocalHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        Task[LocalHandle]
            A Task containing information about the newly submitted job.
        """
        tasks = [asyncio.Task(cls.query_status(h.step, h)) for h in dependencies]
        statuses = await asyncio.gather(*tasks)
        active_found = any(map(Status.is_running, statuses))
        failure_found = any(map(Status.is_failure, statuses))

        # wait for the dependencies to complete before launching
        while active_found and not failure_found:
            await asyncio.sleep(1)

            tasks = [asyncio.Task(cls.query_status(h.step, h)) for h in dependencies]
            statuses = await asyncio.gather(*tasks)
            active_found = any(map(Status.is_running, statuses))
            failure_found = any(map(Status.is_failure, statuses))

        if failure_found:
            msg = f"Dependency of step {step.name} failed. Unable to continue."
            raise CstarExpectationFailed(msg)

        handle = await LocalLauncher._submit(step, dependencies)
        return Task(
            status=Status.Submitted,
            step=step,
            handle=handle,
        )

    @classmethod
    async def query_status(
        cls, step: "Step", item: Task[LocalHandle] | LocalHandle
    ) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        step : Step
            The step that will be queried for.
        item : Task[LocalHandle] | LocalHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Status
            The current status of the item.
        """
        handle = item.handle if isinstance(item, Task) else item
        raw_status = await LocalLauncher._status(step, handle)
        if raw_status in ["PENDING", "RUNNING", "ENDING"]:
            return Status.Running
        if raw_status in ["COMPLETED", "FAILED"]:
            return Status.Done
        if raw_status == "CANCELLED":
            return Status.Cancelled
        if raw_status == "FAILED":
            return Status.Failed

        return Status.Unsubmitted

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
