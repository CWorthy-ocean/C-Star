import shutil
from pathlib import Path
from typing import Literal, override

from cstar.base.log import LoggingMixin


class JobFileSystem(LoggingMixin):
    """Establishes the convention for the set of directories each job
    will use and ensures they exist.
    """

    INPUT_NAME: Literal["input"] = "input"
    WORK_NAME: Literal["work"] = "work"
    TASKS_NAME: Literal["tasks"] = "tasks"
    LOGS_NAME: Literal["logs"] = "logs"
    OUTPUT_NAME: Literal["output"] = "output"

    root: Path
    """The root directory for job output."""

    input_dir: Path
    """The directory for job input."""

    work_dir: Path
    """The directory for job work."""

    tasks_dir: Path
    """The directory for job tasks."""

    logs_dir: Path
    """The directory for job logs."""

    output_dir: Path
    """The directory for job output."""

    def __init__(self, root_directory: Path) -> None:
        """Initialize the job file system.

        Parameters
        ----------
        root_directory : Path
            The root directory for all job outputs.
        """
        self.root = root_directory
        self.input_dir = self.root / self.INPUT_NAME
        self.work_dir = self.root / self.WORK_NAME
        self.tasks_dir = self.root / self.TASKS_NAME
        self.logs_dir = self.root / self.LOGS_NAME
        self.output_dir = self.root / self.OUTPUT_NAME

    @property
    def _dir_set(self) -> set[Path]:
        """Return the complete set of directories for the job."""
        return {
            self.input_dir,
            self.work_dir,
            self.tasks_dir,
            self.logs_dir,
            self.output_dir,
        }

    def prepare(self) -> None:
        """Ensure that the desired directories exist."""
        for task_dir in self._dir_set:
            task_dir.mkdir(parents=True, exist_ok=True)
            self.log.debug(f"Created {task_dir.name} directory `{task_dir}`")

    def clear(self, targets: list[str]) -> None:
        """Clear the step's working directory.

        Parameters
        ----------
        targets : list[str]
            The list of job subdirectories to clear.
        """
        for target in targets:
            target_dir = self.root / target
            if target_dir.exists():
                shutil.rmtree(target_dir)
                self.log.debug(f"Removed {target_dir.name} directory `{target_dir}`")


class RomsJobFileSystem(JobFileSystem):
    COMPILE_TIME_NAME: Literal["compile_time_code"] = "compile_time_code"
    RUNTIME_NAME: Literal["runtime_code"] = "runtime_code"
    INPUT_DATASETS_NAME: Literal["input_datasets"] = "input_datasets"
    CODEBASES_NAME: Literal["codebases"] = "codebases"
    JOINED_OUTPUT_NAME: Literal["joined_output"] = "joined_output"

    compile_time_code_dir: Path
    """The directory for compile-time code."""

    runtime_code_dir: Path
    """The directory for runtime code."""

    input_datasets_dir: Path
    """The directory for input datasets."""

    codebases_dir: Path
    """The directory for codebases."""

    def __init__(self, root_directory: Path) -> None:
        super().__init__(root_directory)

        self.compile_time_code_dir = (
            self.input_dir / RomsJobFileSystem.COMPILE_TIME_NAME
        )
        self.runtime_code_dir = self.input_dir / RomsJobFileSystem.RUNTIME_NAME
        self.input_datasets_dir = self.input_dir / RomsJobFileSystem.INPUT_DATASETS_NAME
        self.codebases_dir = self.input_dir / RomsJobFileSystem.CODEBASES_NAME
        self.joined_output_dir = self.output_dir / RomsJobFileSystem.JOINED_OUTPUT_NAME

    @override
    @property
    def _dir_set(self) -> set[Path]:
        return super()._dir_set.union(
            {
                self.compile_time_code_dir,
                self.runtime_code_dir,
                self.input_datasets_dir,
                self.codebases_dir,
                self.joined_output_dir,
            }
        )
