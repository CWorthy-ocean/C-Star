import functools
import os
import shutil
import typing as t
from dataclasses import dataclass
from pathlib import Path

from cstar.base.log import LoggingMixin
from cstar.base.utils import (
    DEFAULT_CACHE_HOME,
    DEFAULT_CONFIG_HOME,
    DEFAULT_DATA_HOME,
    DEFAULT_STATE_HOME,
    ENV_CSTAR_CACHE_HOME,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_STATE_HOME,
    SCRATCH_DIRS,
)


def hpc_data_directory() -> Path | None:
    """A path-locator function that looks for standard scratch file-systems.

    Returns
    -------
    Path | None
        If a scratch file system is identified, return it's paty, otherwise return None.
    """
    for env_var in SCRATCH_DIRS:
        if scratch_path := os.getenv(env_var, ""):
            return Path(scratch_path)

    return None


@dataclass(slots=True)
class XdgMetaItem:
    """Metadata specifying how to select a specific XDG-compliant setting."""

    purpose: str
    """Plain-text description of a setting."""
    xdg_var_name: str
    """The standard XDG environment variable name used for the setting."""
    default_value: str
    """The default XDG-compliant value for the setting."""
    var_name: str
    """The C-Star environment variable used to override default settings."""
    cstar_subdirectory: str
    """The name of a subdirectory to be used in the setting home folder."""
    override_fn: t.Callable[[], Path | None] | None = None
    """A callable used to identify overridden values at runtime."""


@dataclass(slots=True)
class XdgMetaContainer:
    """Collection of metadata used to locate configuration values related to
    the XDG-compliant directories C-Star will use at runtime.
    """

    cache: XdgMetaItem
    """Metadata used to identify the cache directory."""
    config: XdgMetaItem
    """Metadata used to identify the config directory."""
    data: XdgMetaItem
    """Metadata used to identify the data directory."""
    state: XdgMetaItem
    """Metadata used to identify the state directory."""

    def __iter__(self) -> t.Iterator[XdgMetaItem]:
        """Return an iterable containing all settings contained in the instance."""
        yield self.cache
        yield self.config
        yield self.data
        yield self.state

    def __getitem__(
        self, key: t.Literal["cache", "config", "data", "state"]
    ) -> XdgMetaItem:
        """Return configuration sections by subscript."""
        return getattr(self, key)


@functools.cache
def load_xdg_metadata() -> XdgMetaContainer:
    """Retrieve the configuration used to identify XDG-compliant directories."""
    return XdgMetaContainer(
        cache=XdgMetaItem(
            purpose="Specify the root directory where C-Star will cache temporary files.",
            xdg_var_name="XDG_CACHE_HOME",
            default_value=DEFAULT_CACHE_HOME,
            var_name=ENV_CSTAR_CACHE_HOME,
            cstar_subdirectory="cstar",
        ),
        config=XdgMetaItem(
            purpose="Specify the root directory where C-Star will store configuration files.",
            xdg_var_name="XDG_CONFIG_HOME",
            default_value=DEFAULT_CONFIG_HOME,
            var_name=ENV_CSTAR_CONFIG_HOME,
            cstar_subdirectory="cstar",
        ),
        data=XdgMetaItem(
            purpose="Specify the root directory where C-Star will store datasets.",
            xdg_var_name="XDG_DATA_HOME",
            default_value=DEFAULT_DATA_HOME,
            var_name=ENV_CSTAR_DATA_HOME,
            cstar_subdirectory="cstar",
            override_fn=hpc_data_directory,
        ),
        state=XdgMetaItem(
            purpose="Specify the root directory where C-Star will store state.",
            xdg_var_name="XDG_STATE_HOME",
            default_value=DEFAULT_STATE_HOME,
            var_name=ENV_CSTAR_STATE_HOME,
            cstar_subdirectory="cstar",
        ),
    )


class DirectoryManager:
    """Manage the directories used by C-Star."""

    @classmethod
    def xdg_dir(cls, var_set: XdgMetaItem) -> Path:
        """Calculate an XDG-compliant path honoring standard precedence rules.

        Returns a value in the order:
        - user-overrides provided via environment variable
        - overrides resulting from custom "locator" functions
        - subdirectories provided in XDG_***_HOME environment variables
        - default XDG-compliant folders if no env vars are provided.

        Parameters
        ----------
        var_set : XdgVarSet
            The configuration used to identify the pertinent environment variables
            and default values for a given directory.

        Returns
        -------
        Path
        """
        dir_name = var_set.cstar_subdirectory
        override_fn = var_set.override_fn
        path = Path(var_set.default_value) / dir_name

        if env_value := os.getenv(var_set.var_name, ""):
            # check user-provided environment variables
            path = Path(env_value)
        elif override_fn and (override_value := override_fn()):
            # check functions that return alternative locations
            path = override_value / dir_name
        elif xdg_value := os.getenv(var_set.xdg_var_name, ""):
            # check user provided XDG-.*-HOME environment variables
            path = Path(xdg_value) / dir_name

        return path.expanduser().resolve()

    @classmethod
    def cache_home(cls) -> Path:
        """Get the C-Star cache directory.

        Used to cache temporary files to disk (e.g. git repositories).
        """
        return cls.xdg_dir(load_xdg_metadata().cache)

    @classmethod
    def config_home(cls) -> Path:
        """Get the C-Star config directory.

        Used to store persistent C-Star configuration on disk (e.g .env files).
        """
        return cls.xdg_dir(load_xdg_metadata().config)

    @classmethod
    def data_home(cls) -> Path:
        """Get the C-Star data directory.

        Used to store large data files to disk (e.g. simulation input datasets).
        """
        return cls.xdg_dir(load_xdg_metadata().data)

    @classmethod
    def state_home(cls) -> Path:
        """Get the C-Star state directory.

        Used to store C-Star state files (databases, logs, etc.).
        """
        return cls.xdg_dir(load_xdg_metadata().state)


class JobFileSystemManager(LoggingMixin):
    """JobFileSystem establishes a common directory structure convention across
    all jobs.
    """

    _INPUT_NAME: t.ClassVar[t.Literal["input"]] = "input"
    _WORK_NAME: t.ClassVar[t.Literal["work"]] = "work"
    _TASKS_NAME: t.ClassVar[t.Literal["tasks"]] = "tasks"
    _LOGS_NAME: t.ClassVar[t.Literal["logs"]] = "logs"
    _OUTPUT_NAME: t.ClassVar[t.Literal["output"]] = "output"

    root: t.Final[Path]
    """The root directory of the job. It is provided via user configuration."""

    def __init__(self, root_directory: Path) -> None:
        """Initialize the job file system.

        Parameters
        ----------
        root_directory : Path
            The root directory that contains all job byproducts.
        """
        self.root = root_directory

    def _dir_set(self) -> set[Path]:
        """Return the complete set of directories for the job.

        Returns
        -------
        set[Path]
        """
        return {
            self.input_dir,
            self.work_dir,
            self.tasks_dir,
            self.logs_dir,
            self.output_dir,
        }

    @property
    def input_dir(self) -> Path:
        """The directory for job inputs, such as datasets, code repositories, and
        runtime configuration.
        """
        return self.root / self._INPUT_NAME

    @property
    def work_dir(self) -> Path:
        """The directory for job work items, such as shell scripts generated by the
        system.
        """
        return self.root / self._WORK_NAME

    @property
    def tasks_dir(self) -> Path:
        """The directory for subtasks of a job, such as automatic system tasks
        or those generated via transformations of the inputs.
        """
        return self.root / self._TASKS_NAME

    @property
    def logs_dir(self) -> Path:
        """The directory for log files created during the job."""
        return self.root / self._LOGS_NAME

    @property
    def output_dir(self) -> Path:
        """The directory for writing any outputs from the job."""
        return self.root / self._OUTPUT_NAME

    def prepare(self) -> None:
        """Construct the directory tree for the job."""
        for task_dir in self._dir_set():
            task_dir.mkdir(parents=True, exist_ok=True)
            self.log.debug(f"Created {task_dir.name} directory `{task_dir}`")

    def clear(self) -> None:
        """Ensure the job's working directory is empty."""
        shutil.rmtree(self.root)
        self.root.mkdir(parents=True)
        self.log.debug(f"Created empty working directory for job `{self.root.name}`")

    def __getstate__(self) -> dict[str, str]:
        """Return the state of the object."""
        return {"root": self.root.as_posix()}

    def __setstate__(self, state: dict[str, str]) -> None:
        """Restore the object from a pickle."""
        self.__dict__.update({"root": Path(state["root"])})


class RomsFileSystemManager(JobFileSystemManager):
    _COMPILE_TIME_NAME: t.ClassVar[t.Literal["compile_time_code"]] = "compile_time_code"
    _RUNTIME_NAME: t.ClassVar[t.Literal["runtime_code"]] = "runtime_code"
    _INPUT_DATASETS_NAME: t.ClassVar[t.Literal["input_datasets"]] = "input_datasets"
    _CODEBASES_NAME: t.ClassVar[t.Literal["codebases"]] = "codebases"
    _JOINED_OUTPUT_NAME: t.ClassVar[t.Literal["joined_output"]] = "joined_output"

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
                    self.joined_output_dir,
                }
            )
        )

    @property
    def compile_time_code_dir(self) -> Path:
        """The directory for compile-time code."""
        return self.root / self._COMPILE_TIME_NAME

    @property
    def runtime_code_dir(self) -> Path:
        """The directory for runtime code."""
        return self.root / self._RUNTIME_NAME

    @property
    def input_datasets_dir(self) -> Path:
        """The directory for input datasets."""
        return self.root / self._INPUT_DATASETS_NAME

    @property
    def _codebases_dir(self) -> Path:
        """The directory for codebases."""
        return self.root / self._CODEBASES_NAME

    @property
    def joined_output_dir(self) -> Path:
        """The directory for de-partitioned outputs."""
        return self.root / self._JOINED_OUTPUT_NAME

    def codebase_subdir(self, key: str) -> Path:
        """Return a codebase subdirectory path.

        Returns
        -------
        str
        """
        return self._codebases_dir / key
