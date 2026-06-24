import asyncio
import time
from abc import ABC, abstractmethod
from enum import Enum, auto
from pathlib import Path
from typing import TextIO

from cstar.base.log import LoggingMixin

STATUS_RECHECK_SECONDS = 30


class ExecutionStatus(Enum):
    """Enum representing possible states of a process to be executed.

    Attributes
    ----------
    UNSUBMITTED : ExecutionStatus
        The task is to be handled by a scheduler, but has not yet been submitted
    PENDING : ExecutionStatus
        The task is to be handled by a scheduler, but is waiting to start
    RUNNING : ExecutionStatus
        The task is currently executing.
    COMPLETED : ExecutionStatus
        The task finished successfully.
    TIMEOUT : ExecutionStatus
        The task was cancelled by the system for exceeding it's walltime allotment.
    CANCELLED : ExecutionStatus
        The task was cancelled before completion.
    FAILED : ExecutionStatus
        The task finished unsuccessfully.
    HELD : ExecutionStatus
        The task is to be handled by a scheduler, but is currently on hold pending release
    ENDING : ExecutionStatus
        The task is in the process of ending but not fully completed.
    UNKNOWN : ExecutionStatus
        The task state is unknown or not recognized.
    """

    UNSUBMITTED = auto()
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    TIMEOUT = auto()
    CANCELLED = auto()
    FAILED = auto()
    HELD = auto()
    ENDING = auto()
    UNKNOWN = auto()

    def __str__(self) -> str:
        return self.name.lower()  # Convert enum name to lowercase for display

    @classmethod
    def is_terminal(cls, status: "ExecutionStatus") -> bool:
        return status in [
            ExecutionStatus.COMPLETED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.FAILED,
        ]


class ExecutionHandler(ABC, LoggingMixin):
    """Abstract base class for managing the execution of a task or process.

    This class defines the interface and common behavior for handling task
    execution, such as monitoring the status and providing live updates
    from an output file.

    Attributes
    ----------
    status : ExecutionStatus
        Represents the current status of the task (e.g., RUNNING, COMPLETED).
        This is an abstract property that must be implemented by subclasses.

    Methods
    -------
    updates(seconds=10)
        Stream live updates from the task's output file for a specified duration.
    """

    _log_position: int = 0
    """The current position in the log file.

    Log position is updated after each successful read. It enables resuming reads
    after relinquishing control to the calling process.
    """
    _enabled: bool = True
    """Flag used to disable processing update requests after the task has terminated."""

    @property
    @abstractmethod
    def status(self) -> ExecutionStatus:
        """Abstract property representing the current status of the task.

        Subclasses must implement this property to query the underlying
        process and return the appropriate `ExecutionStatus` enum value.

        Returns
        -------
        ExecutionStatus
            The current status of the task, such as `ExecutionStatus.RUNNING`,
            `ExecutionStatus.COMPLETED`, or other valid states defined in
            the `ExecutionStatus` enum.

        Notes
        -----
        The specific implementation should query the underlying process or
        system to determine the task's status.
        """
        pass

    @property
    @abstractmethod
    def output_file(self) -> Path:
        """Abstract property representing the output file.

        Returns
        -------
        output_file (Path):
            Path to the file in which stdout and stderr will be written.
        """
        pass

    async def on_ready(self) -> None:
        msg = "This job is still pending. Updates will be available after it starts running."
        self.log.info(msg)

    async def on_running(self, seconds: float) -> None:
        """Forward logs from the process until time budget elapses."""
        try:
            with open(self.output_file) as f:
                f.seek(self._log_position)
                start_time = time.time()
                while seconds == 0 or (time.time() - start_time < seconds):
                    line = f.readline()
                    self._log_position = f.tell()

                    if line:
                        self.log.info(line.rstrip())
                        continue

                    # reached EOF; wait before checking for updates
                    await asyncio.sleep(0.1)
        except KeyboardInterrupt:
            self.log.info("Live status updates stopped by user.")

    async def on_shutdown(self) -> None:
        """Handle a normal shutdown."""
        msg = (
            f"This job has ended ({self.status}). Live updates will no longer be provided."
            f" See {self.output_file.resolve()} for job output"
        )
        self.log.warning(msg)

    async def on_exceptional_shutdown(self) -> None:
        """Handle a shutdown where no logs were forwarded."""
        msg = f"This job has ended ({self.status}) and produced no outputs."
        self.log.warning(msg)

    async def updates(self, seconds: float = 1) -> None:
        """Stream live updates from the task's output file.

        This method streams updates from the task's output file for the
        specified duration. If the task is not running or pending, a message will
        indicate the inability to provide updates. If the output file
        exists and the task has already finished, the method provides a
        reference to the output file for review.

        Parameters
        ----------
        seconds : int, optional, default = 10
            The duration (in seconds) for which updates should be streamed.
            If set to 0, updates will be streamed indefinitely until
            interrupted by the user.

        Notes
        -----
        - This method moves to the end of the output file and streams only
          new lines appended during the specified duration.
        - When streaming indefinitely (`seconds=0`), user confirmation is
          required before proceeding.
        """
        if not self._enabled:
            return

        match (ExecutionStatus.is_terminal(self.status), self.output_file.exists()):
            case [False, False]:
                # ready state - process is running but hasn't produced output, yet.
                await self.on_ready()
            case [False, True]:
                # running state - process is running with logs to forward
                await self.on_running(seconds)
            case [True, True]:
                # shutdown state - process has completed with logs to finalize
                await self.on_running(seconds=1)
                await self.on_shutdown()
                self._enabled = False
            case [True, False]:
                # exception state - the process has terminated, but no logs were produced.
                await self.on_exceptional_shutdown()
                self._enabled = False

    def _forward_available(self, file_handle: TextIO) -> None:
        """Forward all currently-available lines from ``file_handle`` to the log.

        Reads from the handle's current position to end-of-file, logging each line,
        and records the new read position so subsequent calls resume without gaps or
        duplication.

        Parameters
        ----------
        file_handle : TextIO
            An open text handle for the task's output file, positioned at the point
            from which to begin reading.
        """
        while True:
            line = file_handle.readline()
            if not line:
                break
            self.log.info(line.rstrip())
        self._log_position = file_handle.tell()
