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
    @abstractmethod
    def updates(self):
        pass

    @property
    @abstractmethod
    def status(self):
        pass
