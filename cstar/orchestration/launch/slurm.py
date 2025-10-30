import asyncio
import random
import typing as t

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


class SlurmHandle(ProcessHandle):
    """Handle enabling reference to a task running in SLURM."""

    duration: int

    def __init__(self, job_id: str) -> None:
        """Initialize the SLURM handle.

        Parameters
        ----------
        job_id : str
            The SLURM_JOB_ID identifying a job.
        """
        self.duration = duration_fn()
        super().__init__(pid=job_id)

    async def simulate_progress(self) -> None:
        """TEMPORARY task simulation."""
        # SIMULATED TASK PROGRESS -->
        self.duration -= 1
        await asyncio.sleep(1)

        # simulate task failure.
        if self.duration < 5 and random.randint(0, 100) < 5:
            self.duration = -100
        # <--- SIMULATED TASK PROGRESS


class SlurmLauncher(Launcher):
    """A launcher that executes steps in a SLURM-enabled cluster."""

    @staticmethod
    async def _submit(step: CStep) -> SlurmHandle:
        """Submit a step to SLURM as a new batch allocation.

        Parameters
        ----------
        step : Step
            The step to submit to SLURM.
        """
        # TODO: replace with non-mock implementation
        job_id = f"mock-slurm-job-id-{step.name}"
        print(f"Submitting run of {step.application} created job_id: {job_id}")

        handle = SlurmHandle(job_id=job_id)
        await handle.simulate_progress()

        return handle

    @staticmethod
    async def _status(handle: SlurmHandle) -> str:
        """Retrieve the status of a step running in SLURM.

        Parameters
        ----------
        handle : SlurmHandle
            A handle object for a SLURM-based task.
        """
        # TODO: replace with non-mock implementation
        # Simulate "making progress" by adjusting the duration, here.
        job_id = handle.pid
        await handle.simulate_progress()
        print(f"\tRemaining for {handle.pid}: {handle.duration}")
        status = "RUNNING"
        if handle.duration == 0:
            status = "COMPLETED"
        elif handle.duration < 0:
            status = "FAILED"
        print(f"Retrieved status `{status}` for job_id `{job_id}`")
        return status

    @classmethod
    async def launch(cls, step: CStep) -> Task:
        """Launch a step in SLURM.

        Parameters
        ----------
        step : Step
            The step to submit to SLURM.
        """
        handle = await SlurmLauncher._submit(step)
        # TODO: confirm handle did not insta-fail on launch
        return Task(
            status=(
                # TODO: remove this simulation of insta-fail using duration of 0
                Status.Running if handle.pid and handle.duration > 0 else Status.Failed
            ),
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
        handle = t.cast(SlurmHandle, item.handle if isinstance(item, Task) else item)
        raw_status = await SlurmLauncher._status(handle)
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
        handle = t.cast(SlurmHandle, item.handle)
        if handle.duration < 2:
            # pretend that i can't cancel...
            print(f"Unable to cancel this running task `{handle.pid}")
            return item

        item.status = Status.Cancelled
        return item
