import shutil
import typing as t
from pathlib import Path

from cstar.execution.file_system import JobFileSystemManager


class RomsFileSystemManager(JobFileSystemManager):
    _COMPILE_TIME_NAME: t.ClassVar[t.Literal["compile_time_code"]] = "compile_time_code"
    _RUNTIME_NAME: t.ClassVar[t.Literal["runtime_code"]] = "runtime_code"
    _INPUT_DATASETS_NAME: t.ClassVar[t.Literal["input_datasets"]] = "input_datasets"
    _CODEBASES_NAME: t.ClassVar[t.Literal["codebases"]] = "codebases"
    _TEMP_OUTPUT_NAME: t.ClassVar[t.Literal["temp_output"]] = "temp_output"
    PARTITIONED_OUTPUT_GLOB: t.ClassVar[str] = "*.??????????????.*.nc"
    """Glob matching one partition piece of a ROMS output: a 14-digit timestamp
    followed by a partition index, e.g. `output_rst.20120201000000.000.nc`.
    """

    def __init__(self, root_directory: Path) -> None:
        super().__init__(root_directory)

    @t.override
    def _dir_set(self) -> set[Path]:
        return (
            super()
            ._dir_set()
            .union(
                {
                    self.compile_time_code_dir,
                    self.runtime_code_dir,
                    self.input_datasets_dir,
                    self._codebases_dir,
                    self.temp_output_dir,
                }
            )
        )

    @property
    def compile_time_code_dir(self) -> Path:
        """The directory for compile-time code."""
        return self.input_dir / self._COMPILE_TIME_NAME

    @property
    def runtime_code_dir(self) -> Path:
        """The directory for runtime code."""
        return self.input_dir / self._RUNTIME_NAME

    @property
    def input_datasets_dir(self) -> Path:
        """The directory for input datasets."""
        return self.input_dir / self._INPUT_DATASETS_NAME

    @property
    def _codebases_dir(self) -> Path:
        """The directory for codebases."""
        return self.input_dir / self._CODEBASES_NAME

    @property
    def temp_output_dir(self) -> Path:
        """The directory for partitioned outputs written by a non-ParallelIO
        run before they are joined into `output`.
        """
        return self.root_dir / self._TEMP_OUTPUT_NAME

    def codebase_subdir(self, key: str) -> Path:
        """Return a codebase subdirectory path.

        Returns
        -------
        str
        """
        return self._codebases_dir / key

    def clear(self) -> None:
        """Ensure the job's working directories are empty."""
        msg = f"Emptying ROMS working directories for job `{self.root_dir.name}`"
        self.log.debug(msg)

        for directory in [
            self.compile_time_code_dir,
            self.runtime_code_dir,
            self.input_datasets_dir,
            self._codebases_dir,
            self.temp_output_dir,
        ]:
            if directory.exists():
                shutil.rmtree(directory)

        # clear everything from workdir except blueprints
        if self.run_dir.exists():
            for f in self.run_dir.iterdir():
                if not f.name.endswith(".yml") and not f.name.endswith(".yaml"):
                    f.unlink()
