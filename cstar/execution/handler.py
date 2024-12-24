import time
from abc import ABC, abstractmethod
from enum import Enum, auto


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
    CANCELLED = auto()
    FAILED = auto()
    HELD = auto()
    ENDING = auto()
    UNKNOWN = auto()

    def __str__(self) -> str:
        return self.name.lower()  # Convert enum name to lowercase for display


class ExecutionHandler(ABC):
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

    @property
    @abstractmethod
    def status(self):
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

    def updates(self, seconds=10):
        """Stream live updates from the task's output file.

        This method streams updates from the task's output file for the
        specified duration. If the task is not running, a message will
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

        if self.status != ExecutionStatus.RUNNING:
            print(
                f"This job is currently not running ({self.status}). Live updates cannot be provided."
            )
            if (self.status in {ExecutionStatus.FAILED, ExecutionStatus.COMPLETED}) or (
                self.status == ExecutionStatus.CANCELLED and self.output_file.exists()
            ):
                print(f"See {self.output_file.resolve()} for job output")
            return

        if seconds == 0:
            # Confirm indefinite tailing
            confirmation = (
                input(
                    "This will provide indefinite updates to your job. You can stop it anytime using Ctrl+C. "
                    "Do you want to continue? (y/n): "
                )
                .strip()
                .lower()
            )
            if confirmation not in {"y", "yes"}:
                return

        try:
            with open(self.output_file, "r") as f:
                f.seek(0, 2)  # Move to the end of the file
                start_time = time.time()

                while seconds == 0 or (time.time() - start_time < seconds):
                    line = f.readline()
                    if line:
                        print(line, end="")
                    else:
                        time.sleep(0.1)  # 100ms delay between updates
        except KeyboardInterrupt:
            print("\nLive status updates stopped by user.")
