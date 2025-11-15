import datetime
import random
import typing as t
from subprocess import Popen

from psutil import Process

from cstar.orchestration.orchestration import (
    CStep,
    Launcher,
    ProcessHandle,
    Status,
    Task,
)


class LocalHandle(ProcessHandle):
    """Handle enabling reference to a task running in local processes."""

    popen: Popen
    """The process handle (used only for simulating local processes)."""
    start_at: float
    """The process creation time as a posix timestamp (in seconds)."""

    def __init__(
        self,
        step: CStep,
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
        self.step = step
        self.start_at = (
            start_at.timestamp()
            if isinstance(start_at, datetime.datetime)
            else start_at
        )
        super().__init__(pid=str(pid))

    @property
    def elapsed(self) -> float:
        """The number of seconds passed since the task was started.

        Returns
        -------
        int
        """
        now = datetime.datetime.now().timestamp()
        return now - self.start_at


class LocalLauncher(Launcher[LocalHandle]):
    """A launcher that executes steps in a local process."""

    processes: t.ClassVar[dict[str, Popen]] = {}
    """Mapping from step name to process."""

    @staticmethod
    async def _submit(step: CStep, dependencies: list[LocalHandle]) -> LocalHandle:
        """Submit a step to SLURM as a new batch allocation.

        Parameters
        ----------
        step : Step
            The step to execute in a local process.
        """
        print(f"Checking deps for `{step.name}`: {[d.step.name for d in dependencies]}")
        tracked = {
            d.step.name: (d.step, LocalLauncher.processes.get(d.step.name, None))
            for d in dependencies
        }

        if any(
            process
            for name, (step, process) in tracked.items()
            if not process or (process and process.returncode is None)
            # and process.create_time() == # TODO: solve for create date...
        ):
            raise RuntimeError(
                f"Unsatisfied prerequisites. Unable to start task: {step.name}"
            )

        cmd = ["sleep", str(random.randint(5, 12))]
        print(f"Creating local process from cmd: {' '.join(cmd)}")

        popen = Popen(cmd)
        LocalLauncher.processes[step.name] = popen

        pid = popen.pid
        process = Process(pid)
        start_at = process.create_time()

        handle = LocalHandle(step, pid, start_at)
        print(f"Local run of `{step.application}` created pid: {pid}")

        return handle

    @staticmethod
    async def _status(step: CStep, handle: LocalHandle) -> str:
        """Retrieve the status of a step running in local process.

        Parameters
        ----------
        handle : LocalHandle
            A handle object for a process-based task.
        """
        to_query = [p for p in LocalLauncher.processes.values() if p.returncode is None]
        for process in to_query:
            process.poll()

        rc: int | None = None
        if handle.step.name in LocalLauncher.processes:
            rc = LocalLauncher.processes[handle.step.name].returncode

        print(f"Return code for pid `{handle.pid}` is `{rc}` for `{step.name}`")
        if rc is None:
            status = "RUNNING"
        elif rc == 0:
            status = "COMPLETED"
        else:
            status = "FAILED"

        print(f"Status `{status}` for pid `{handle.pid}` after {handle.elapsed} sec")
        return status

    @classmethod
    async def launch(cls, step: CStep, dependencies: list[LocalHandle]) -> Task:
        """Launch a step in local process.

        Parameters
        ----------
        step : Step
            The step to run in a local process.
        """
        handle = await LocalLauncher._submit(step, dependencies)
        status = await LocalLauncher.query_status(step, handle)
        return Task(
            status=status,
            step=step,
            handle=handle,
        )

    @classmethod
    async def query_status(
        cls, step: CStep, item: Task[LocalHandle] | LocalHandle
    ) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        item : Task | ProcessHandle
            An item with a handle to be used to execute a status query.
        """
        handle = item.handle if isinstance(item, Task) else item
        raw_status = await LocalLauncher._status(step, handle)
        if raw_status in ["PENDING", "RUNNING", "ENDING"]:
            return Status.Running
        if raw_status in ["COMPLETED", "FAILED"]:
            return Status.Done
        if raw_status in ["CANCELLED"]:
            return Status.Cancelled
        if raw_status in ["FAILED"]:
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
        Status
            The current status of the item.
        """
        handle = item.handle
        process = LocalLauncher.processes.get(item.step.name, None)

        if process and process.returncode is not None:
            # can't cancel it if it's already done
            print(f"Unable to cancel this running task `{handle.pid}")
            return item

        if process and process.returncode:
            handle.popen.kill()
            item.status = Status.Cancelled

        return item
