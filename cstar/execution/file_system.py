import asyncio
import functools
import os
import shutil
import sys
import typing as t
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

from requests import request

from cstar.base.env import (
    ENV_CSTAR_CACHE_HOME,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_STATE_HOME,
    get_env_item,
)
from cstar.base.log import LoggingMixin
from cstar.base.utils import slugify

if t.TYPE_CHECKING:
    from cstar.base.env import EnvItem


@dataclass(slots=True)
class XdgMetaContainer:
    """Collection of metadata used to locate configuration values related to
    the XDG-compliant directories C-Star will use at runtime.
    """

    cache: "EnvItem"
    """Metadata used to identify the cache directory."""
    config: "EnvItem"
    """Metadata used to identify the config directory."""
    data: "EnvItem"
    """Metadata used to identify the data directory."""
    state: "EnvItem"
    """Metadata used to identify the state directory."""

    def __iter__(self) -> t.Iterator["EnvItem"]:
        """Return an iterable containing all settings contained in the instance."""
        yield self.cache
        yield self.config
        yield self.data
        yield self.state


@functools.cache
def load_xdg_metadata() -> XdgMetaContainer:
    """Retrieve the configuration used to identify XDG-compliant directories."""
    return XdgMetaContainer(
        cache=get_env_item(ENV_CSTAR_CACHE_HOME),
        config=get_env_item(ENV_CSTAR_CONFIG_HOME),
        data=get_env_item(ENV_CSTAR_DATA_HOME),
        state=get_env_item(ENV_CSTAR_STATE_HOME),
    )


class DirectoryManager:
    """Manage the directories used by C-Star."""

    _PKG_SUBDIR: t.Literal["cstar"] = "cstar"

    @classmethod
    def xdg_dir(cls, env_item: "EnvItem") -> Path:
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
        override_fn = env_item.default_factory
        path = Path(env_item.default) / DirectoryManager._PKG_SUBDIR

        if os.getenv(env_item.name, ""):
            # check user-provided environment variables
            path = Path(env_item.value)
        elif override_fn and override_fn(env_item):
            # check functions that return alternative locations
            path = Path(env_item.value) / DirectoryManager._PKG_SUBDIR
        elif os.getenv(env_item.indirect_var, ""):
            # check user provided XDG-.*-HOME environment variables
            path = Path(env_item.value) / DirectoryManager._PKG_SUBDIR

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
    """The name of the subfolder where job inputs will be written."""
    _WORK_NAME: t.ClassVar[t.Literal["work"]] = "work"
    """The name of the subfolder where job scripts will be written."""
    _TASKS_NAME: t.ClassVar[t.Literal["tasks"]] = "tasks"
    """The name of the subfolder where job sub-tasks will be executed."""
    _LOGS_NAME: t.ClassVar[t.Literal["logs"]] = "logs"
    """The name of the subfolder where job logs will be written."""
    _OUTPUT_NAME: t.ClassVar[t.Literal["output"]] = "output"
    """The name of the subfolder where job outputs will be written."""
    _root: t.Final[Path]
    """The root directory of the job."""

    def __init__(self, root_directory: Path) -> None:
        """Initialize the job file system.

        Parameters
        ----------
        root_directory : Path
            The root directory that contains all job byproducts.
        """
        self._root = root_directory.expanduser().resolve()

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
    def root(self) -> Path:
        """The root directory containing all job outputs.

        Returns
        -------
        Path
        """
        return self._root

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
            self.log.debug("Created `%s` directory `%s`", task_dir.name, task_dir)

    def clear(self) -> None:
        """Ensure the job's working directories are empty."""
        if not self.root.exists():
            return

        msg = f"Emptying working directories for job `{self.root.name}`"
        self.log.debug(msg)

        for directory in [
            self.input_dir,
            self.work_dir,
            self.tasks_dir,
            self.logs_dir,
            self.output_dir,
        ]:
            if directory.exists():
                shutil.rmtree(directory)
            directory.mkdir(parents=True)

    def get_subtask_manager(self, task_name: str) -> t.Self:
        """Create a JobFileSystemManager instance with a root directory
        configured for a subtask.

        Parameters
        ----------
        task_name : str
            The subtask name

        Returns
        -------
        JobFileSystemManager
            A file system manager with a root directory relative to this instance.
        """
        task_dir = self.tasks_dir / slugify(task_name)
        return self.__class__(task_dir)

    def __getstate__(self) -> dict[str, str]:
        """Return the state of the object."""
        return {"_root": self._root.as_posix()}

    def __setstate__(self, state: dict[str, str]) -> None:
        """Restore the object from a pickle."""
        self.__dict__.update({"_root": Path(state["_root"])})


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

    def clear(self) -> None:
        """Ensure the job's working directories are empty."""
        super().clear()

        for directory in [
            self.compile_time_code_dir,
            self.runtime_code_dir,
            self.input_datasets_dir,
            self._codebases_dir,
            self.joined_output_dir,
        ]:
            if directory.exists():
                shutil.rmtree(directory)


def is_remote_resource(uri: str) -> bool:
    """Determine if the supplied string contains a local or remote path.

    Parameters
    ----------
    path : str
        The string to be tested

    Returns
    -------
    bool
    """
    remote_schemes = {"http", "https", "smb", "ftp", "s3"}
    split_url = urlsplit(uri.casefold())
    return split_url.scheme in remote_schemes


def write_local_copy(url: str, directory: Path) -> Path:
    """Copy a remote resource to a local file.

    Returns
    -------
    Path
        The local path where the remote resource was copied
    """
    parsed_url = urlsplit(url)
    resource_path = Path(parsed_url.path)
    resource_name = resource_path.name
    http_ok: t.Final[int] = 200

    get_request = request("GET", url, timeout=2.0)
    if get_request.status_code != http_ok:
        msg = f"Unable to retrieve file from: {url}"
        raise FileNotFoundError(msg)

    local_path = directory / resource_name
    local_path.write_text(get_request.text)

    return local_path.expanduser().resolve()


@contextmanager
def local_copy(uri: str) -> t.Generator[Path, None, None]:
    """Context manager used to create a local copy of a remote resource.

    When the uri references a local resource, it is used without being copied.

    Parameters
    ----------
    uri_or_path : str
        The resource URI

    Returns
    -------
    Path
        A path to a local copy of the resource
    """
    is_remote = is_remote_resource(uri)

    with TemporaryDirectory() as tmp_dir:
        bp_path = write_local_copy(uri, Path(tmp_dir)) if is_remote else Path(uri)
        try:
            yield bp_path
        finally:
            if is_remote and bp_path.exists():
                bp_path.unlink()


@asynccontextmanager
async def local_copy_async(uri: str) -> t.AsyncGenerator[Path, None]:
    """Asynchronous context manager used to create a local copy of a remote resource.

    When the uri references a local resource, it is used without being copied.

    Parameters
    ----------
    uri_or_path : str
        The resource URI

    Returns
    -------
    Path
        A path to a local copy of the resource
    """
    sync_local_copy = local_copy(uri)
    resource_path = await asyncio.to_thread(sync_local_copy.__enter__)

    try:
        yield resource_path
    finally:
        exc_type, exc_val, exc_tb = sys.exc_info()
        await asyncio.to_thread(sync_local_copy.__exit__, exc_type, exc_val, exc_tb)
