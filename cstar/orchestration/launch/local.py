import datetime
import random
import typing as t
from subprocess import Popen

from cstar.orchestration.orchestration import (
    CStep,
    Launcher,
    ProcessHandle,
    Status,
    Task,
)


def duration_fn() -> int:
    """Mock task execution via randomly selecting a duration for the step."""
    return random.randint(5, 12)


class LocalHandle(ProcessHandle):
    """Handle enabling reference to a task running in local processes."""

    duration: int
    popen: Popen
    start_at: datetime.datetime

    def __init__(
        self,
        step: CStep,
    ) -> None:
        """Initialize the local handle.

        Parameters
        ----------
        proc : subprocess.Popen
            The subprocess handle.
        """
        self.duration = duration_fn()
        self.step = step
        self.start_at = datetime.datetime.now()
        cmd = ["sleep", str(self.duration)]
        print(f"Creating local handle from cmd: {' '.join(cmd)}")
        self.popen = Popen(cmd)
        self.popen.poll()
        super().__init__(pid=str(self.popen.pid))


class LocalLauncher(Launcher):
    """A launcher that executes steps in a local process."""

    @staticmethod
    async def _submit(step: CStep) -> LocalHandle:
        """Submit a step to SLURM as a new batch allocation.

        Parameters
        ----------
        step : Step
            The step to execute in a local process.
        """
        handle = LocalHandle(step)
        print(
            f"Local run of `{step.application}` created pid: {handle.popen.pid} with duration {handle.duration}"
        )

        return handle

    @staticmethod
    async def _status(handle: LocalHandle) -> str:
        """Retrieve the status of a step running in local process.

        Parameters
        ----------
        handle : LocalHandle
            A handle object for a process-based task.
        """
        # TODO: replace with non-mock implementation
        handle.popen.poll()
        print(f"Return code for pid `{handle.pid}`is `{handle.popen.returncode}")
        if handle.popen.returncode is None:
            status = "RUNNING"
        elif handle.popen.returncode == 0:
            status = "COMPLETED"
        else:
            status = "FAILED"

        elapsed = datetime.datetime.now() - handle.start_at
        print(
            f"Retrieved status `{status}` for pid `{handle.pid}` after {elapsed.total_seconds()} seconds"
        )
        return status

    @classmethod
    async def launch(cls, step: CStep) -> Task:
        """Launch a step in local process.

        Parameters
        ----------
        step : Step
            The step to run in a local process.
        """
        handle = await LocalLauncher._submit(step)
        status = await LocalLauncher.query_status(handle)
        return Task(
            status=status,
            step=step,
            handle=handle,
        )

    @classmethod
    async def query_status(cls, item: Task | ProcessHandle) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        item : Task | ProcessHandle
            An item with a handle to be used to execute a status query.
        """
        handle = t.cast(LocalHandle, item.handle if isinstance(item, Task) else item)
        raw_status = await LocalLauncher._status(handle)
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
    async def cancel(cls, item: Task) -> Task:
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
        handle = t.cast(LocalHandle, item.handle)
        if handle.popen.returncode is not None:
            # can't cancel it if it's already done
            print(f"Unable to cancel this running task `{handle.pid}")
            return item
        else:
            handle.popen.kill()
            item.status = Status.Cancelled

        return item
