import asyncio
import contextlib
import errno
import fcntl
import functools
import os
import shlex
import shutil
import stat
import sys
import time
import typing as t
import uuid
from collections.abc import AsyncGenerator, Generator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

from requests import RequestException, request

from cstar.base.env import (
    ENV_CSTAR_CACHE_HOME,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_PROJECT_HOME,
    ENV_CSTAR_RUNID,
    ENV_CSTAR_STATE_HOME,
    get_env_item,
    hpc_data_directory,
)
from cstar.base.exceptions import CstarError
from cstar.base.log import LoggingMixin, get_logger
from cstar.base.utils import _run_cmd, slugify

if t.TYPE_CHECKING:
    from cstar.base.env import EnvItem

log = get_logger(__name__)


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

    def __iter__(self) -> Iterator["EnvItem"]:
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
    _SOURCE_DATA_SUBDIR: t.Literal["source-data"] = "source-data"
    """The source-data cache's name under ``<root>/cstar``."""
    _RUNS_SUBDIR: t.Literal["runs"] = "runs"
    """The name, under ``<project home>/cstar``, of the project's runs area."""
    _LEGACY_SOURCE_DATA_SUBDIR: t.Literal["cstar-forge-data/source-data"] = (
        "cstar-forge-data/source-data"
    )
    """Where cstar-forge placed the source-data cache under the same root."""

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
        # The declared default already names the final directory (``~/cstar``,
        # ``~/.cache/cstar``, ...), so it is used as is; the ``cstar`` subdirectory is
        # appended only to locations derived from a locator or an XDG variable.
        path = Path(env_item.default)

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

    @classmethod
    def project_home(cls) -> Path | None:
        """Get the durable project directory, when one is configured.

        The value of ``CSTAR_PROJECT_HOME``, which defaults to ``$PROJECT``.
        Unlike the root of the source-data cache, it does not fall back to
        scratch or the home directory: a project area is shared and durable, so
        its absence means there is none.

        Returns
        -------
        Path | None
            The directory, or `None` when neither variable is set.
        """
        value = get_env_item(ENV_CSTAR_PROJECT_HOME).value
        return Path(value).expanduser().resolve() if value else None

    @classmethod
    def project_runs_home(cls) -> Path | None:
        """Get the project's runs area, ``<project home>/cstar/runs``.

        Holds the runs a project keeps durable and shares. Does not create the
        directory.

        Returns
        -------
        Path | None
            The directory, or `None` when there is no project home.
        """
        home = cls.project_home()
        return home / cls._PKG_SUBDIR / cls._RUNS_SUBDIR if home else None

    @classmethod
    def _source_data_paths(cls) -> tuple[Path, Path]:
        """Return the standard and legacy source-data cache paths, on the same root."""
        root = (
            cls.project_home()
            or Path(hpc_data_directory() or Path.home()).expanduser().resolve()
        )
        return (
            root / cls._PKG_SUBDIR / cls._SOURCE_DATA_SUBDIR,
            root / cls._LEGACY_SOURCE_DATA_SUBDIR,
        )

    @classmethod
    def source_data_home(cls) -> Path:
        """Get the shared source-data cache directory.

        Holds downloaded and user-staged source datasets (GLORYS, TPXO, ...) that
        every domain reuses, at ``<root>/cstar/source-data``. The root is
        ``CSTAR_PROJECT_HOME`` (default ``$PROJECT``), else the scratch file system
        (:func:`cstar.base.env.hpc_data_directory`), else the home directory. The
        cache is durable and deliberately not derived from ``CSTAR_DATA_HOME``.
        Does not create the directory; see :meth:`ensure_source_data_home`.
        """
        return cls._source_data_paths()[0]

    @classmethod
    def ensure_source_data_home(cls) -> Path:
        """Create the source-data cache directory and return it.

        A cache at the legacy ``<root>/cstar-forge-data/source-data`` is adopted:
        when it exists and the standard directory does not, the standard directory
        is created as a relative symlink to it.

        Raises
        ------
        CstarError
            If the standard directory is a symlink whose target is missing (an
            unmounted file system, say), rather than replacing it with an empty
            directory.
        """
        home, legacy = cls._source_data_paths()
        if home.is_symlink() and not home.exists():
            msg = (
                f"The source-data cache {home} links to {home.readlink()}, which "
                "does not exist. Restore the target or remove the link."
            )
            raise CstarError(msg)
        if not home.exists() and legacy.is_dir():
            # Another process sharing the root may have linked it first.
            with contextlib.suppress(FileExistsError):
                symlink_relative(home, legacy)
                log.info("Linked source-data cache %s to existing %s", home, legacy)
        home.mkdir(parents=True, exist_ok=True)
        return home


class JobFileSystemManager(LoggingMixin):
    """JobFileSystem establishes a common directory structure convention across
    all jobs.
    """

    _INPUT_NAME: t.ClassVar[t.Literal["input"]] = "input"
    """The name of the subfolder where job inputs will be written."""
    _RUN_NAME: t.ClassVar[t.Literal["work"]] = "work"
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
            self.run_dir,
            self.tasks_dir,
            self.logs_dir,
            self.output_dir,
        }

    @property
    def root_dir(self) -> Path:
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
        return self.root_dir / self._INPUT_NAME

    @property
    def run_dir(self) -> Path:
        """The directory for job work items, such as shell scripts generated by the
        system.
        """
        return self.root_dir / self._RUN_NAME

    @property
    def tasks_dir(self) -> Path:
        """The directory for subtasks of a job, such as automatic system tasks
        or those generated via transformations of the inputs.
        """
        return self.root_dir / self._TASKS_NAME

    @property
    def logs_dir(self) -> Path:
        """The directory for log files created during the job."""
        return self.root_dir / self._LOGS_NAME

    @property
    def output_dir(self) -> Path:
        """The directory for writing any outputs from the job."""
        return self.root_dir / self._OUTPUT_NAME

    def prepare(self) -> None:
        """Construct the directory tree for the job."""
        for task_dir in self._dir_set():
            task_dir.mkdir(parents=True, exist_ok=True)
            self.log.debug("Created `%s` directory `%s`", task_dir.name, task_dir)

    def clear(self) -> None:
        """Ensure the job's working directories are empty."""
        if not self.root_dir.exists():
            return

        msg = f"Emptying working directories for job `{self.root_dir.name}`"
        self.log.debug(msg)

        for directory in [
            self.input_dir,
            self.run_dir,
            self.tasks_dir,
            self.logs_dir,
            self.output_dir,
        ]:
            if directory.exists():
                shutil.rmtree(directory)
            directory.mkdir(parents=True)

    def clear_prior(self) -> None:
        """Ensure the job's working directories are empty."""
        if not self.root_dir.exists():
            return

        msg = f"Emptying working directories of outputs for previous run of `{self.root_dir.name}`"
        self.log.debug(msg)

        for directory in [
            self.input_dir,
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


class StateDirectoryManager:
    """Manage the system file system."""

    _RUN_STATE_NAME: t.ClassVar[t.Literal["run_state"]] = "run_state"
    """The name of the directory where run-state files are written."""

    _RUN_TRACKING_NAME: t.ClassVar[t.Literal["run_tracking"]] = "run_tracking"
    """The name of the directory where run-tracking files are written."""

    _USER_VIEW_NAME: t.ClassVar[t.Literal["gathered_output"]] = "gathered_output"
    """The name of the directory where an aggregated user view of outputs is created."""

    _BLUEPRINT_RUNS_NAME: t.ClassVar[t.Literal["blueprint_runs"]] = "blueprint_runs"
    """The name of the data-home directory holding blueprint runs made outside a workplan."""

    _WORKPLAN_RUNS_NAME: t.ClassVar[t.Literal["workplan_runs"]] = "workplan_runs"
    """The name of the data-home directory holding workplan runs."""

    @classmethod
    def root_dir(cls) -> Path:
        """The root directory containing all job outputs.

        Returns
        -------
        Path
        """
        return DirectoryManager.state_home()

    @classmethod
    def run_state_dir(cls, *, run_id: str | None = None) -> Path:
        """The directory for run-state files.

        The result is a _run-specific_ directory that varies
        based on the current value of environment variables.

        Returns
        -------
        Path
        """
        run_id = run_id or get_env_item(ENV_CSTAR_RUNID).value
        return cls.root_dir() / cls._RUN_STATE_NAME / run_id

    @classmethod
    def tracking_dir(cls) -> Path:
        """The directory for run-tracking files.

        The result is a _non-run-specific_ directory.

        Returns
        -------
        Path
        """
        root = cls.root_dir()
        return root / cls._RUN_TRACKING_NAME

    @classmethod
    def workplan_runs_dir(cls) -> Path:
        """The data-home directory holding the workplan runs.

        ``<data home>/workplan_runs``; each run is a directory of its run-id.

        Returns
        -------
        Path
        """
        return DirectoryManager.data_home() / cls._WORKPLAN_RUNS_NAME

    @classmethod
    def blueprint_runs_dir(cls) -> Path:
        """The data-home directory holding the blueprint runs made outside a workplan.

        ``<data home>/blueprint_runs``; each run is a directory of its name
        under the directory of its application.

        Returns
        -------
        Path
        """
        return DirectoryManager.data_home() / cls._BLUEPRINT_RUNS_NAME

    @classmethod
    def data_dir(cls, run_id: str | None = None) -> Path:
        """The directory for data files used by a run.

        The result is a _run-specific_ directory,
        ``<data home>/workplan_runs/<run id>``. A run started before that
        layout keeps its ``<data home>/<run id>`` root, so re-running or
        resuming it finds the steps where they ran.

        Returns
        -------
        Path
        """
        data_home = DirectoryManager.data_home()
        run_id = run_id or get_env_item(ENV_CSTAR_RUNID).value
        run_dir = cls.workplan_runs_dir() / run_id

        legacy_dir = data_home / run_id
        if not run_dir.exists() and JobFileSystemManager(legacy_dir).tasks_dir.is_dir():
            log.debug(
                "Using run directory %s, which predates the %s/ layout",
                legacy_dir,
                cls._WORKPLAN_RUNS_NAME,
            )
            return legacy_dir

        return run_dir

    @classmethod
    def blueprint_run_dir(cls, application: str, name: str) -> Path:
        """The default working directory of a blueprint run outside a workplan.

        ``<data home>/blueprint_runs/<application>/<name>``: where a blueprint
        that declares no ``working_dir`` is executed from. The application
        segment keeps a producer and the blueprint it emits apart when they
        share a name (forge and its ``roms_marbl`` blueprint).

        Parameters
        ----------
        application : str
            The application the blueprint runs.
        name : str
            The blueprint name, slugified for use as a directory name.

        Returns
        -------
        Path
        """
        return cls.blueprint_runs_dir() / application / slugify(name)

    @classmethod
    def user_dir(cls, run_id: str | None = None) -> Path:
        """The directory containing a user-view of assets created during a run.

        The result is a _run-specific_ directory.

        Returns
        -------
        Path
        """
        run_id = run_id or get_env_item(ENV_CSTAR_RUNID).value
        return cls.user_view_dir(cls.data_dir(run_id))

    @classmethod
    def user_view_dir(cls, run_root: Path) -> Path:
        """The user-view directory (`gathered_output`) inside a run directory.

        Parameters
        ----------
        run_root : Path
            The run directory.

        Returns
        -------
        Path
        """
        return run_root / cls._USER_VIEW_NAME


def is_remote_resource(uri: str) -> bool:
    """Determine if the supplied string contains a local or remote path.

    Parameters
    ----------
    path : str
        The string to be tested

    Returns
    -------
    bool

    Raises
    ------
    ValueError
        If an empty URI is received.
    """
    uri = uri.strip()
    if not uri:
        raise ValueError("Invalid resource URI received")

    remote_schemes = {"http", "https", "smb", "ftp", "sftp", "s3"}
    split_url = urlsplit(uri.casefold())
    return split_url.scheme in remote_schemes


def write_local_copy(url: str, directory: Path) -> Path:
    """Copy a remote resource to a local file.

    Returns
    -------
    Path
        The local path where the remote resource was copied

    Raises
    ------
    ValueError
        If an empty URL is received.
    FileNotFoundError
        If the remote URL cannot be retrieved (error status, timeout, or
        connection failure).
    """
    url = url.strip()
    if not url:
        raise ValueError("Invalid resource URI received")

    parsed_url = urlsplit(url)
    resource_path = Path(parsed_url.path)
    resource_name = resource_path.name
    http_ok: t.Final[int] = 200

    try:
        get_request = request("GET", url, timeout=10.0)
    except RequestException as ex:
        msg = f"Unable to retrieve remote file ({type(ex).__name__})"
        raise FileNotFoundError(errno.ENOENT, msg, url) from ex
    if get_request.status_code != http_ok:
        msg = "Unable to retrieve remote file"
        raise FileNotFoundError(errno.ENOENT, msg, url)

    local_path = directory / resource_name
    local_path.write_text(get_request.text)

    return local_path.expanduser().resolve()


@contextmanager
def local_copy(uri: str) -> Generator[Path, None, None]:
    """Context manager used to create a local copy of a remote resource.

    When the uri references a local resource, it is used without being copied.

    Parameters
    ----------
    uri : str
        The resource URI

    Returns
    -------
    Path
        A path to a local copy of the resource
    """
    if is_remote_resource(uri):
        with TemporaryDirectory(delete=False) as tmp_dir:
            bp_path = write_local_copy(uri, Path(tmp_dir))
            yield bp_path
    else:
        bp_path = Path(uri).expanduser().resolve()
        if not bp_path.exists():
            msg = f"File not found at path: {bp_path}"
            raise FileNotFoundError(errno.ENOENT, msg, bp_path)
        yield bp_path


@asynccontextmanager
async def local_copy_async(uri: str) -> AsyncGenerator[Path, None]:
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


def remove_files(file_dir: Path, wildcard_pattern: str) -> bool:
    """Removes files in the input dir matching the wildcard pattern.

    Parameters
    ----------
    file_dir: directory in which to search for and remove matching files
    wildcard_pattern: the glob pattern to match files against

    Returns
    -------
    True if any files were removed, False otherwise

    Raises
    ------
    ValueError
        If the provided search path is not a directory.
    """
    log.debug(f"Removing files matching wildcard pattern {wildcard_pattern}...")
    if not file_dir.exists():
        log.trace(f"Directory {str(file_dir)!r} does not exist; skipping deletion.")
        return False

    if file_dir.is_file():
        msg = f"Directory is required. Received file path: {file_dir}"
        raise ValueError(msg)

    removed = False
    for p in file_dir.glob(wildcard_pattern):
        if p.is_file():
            p.unlink()
        elif p.is_dir():
            shutil.rmtree(p)
        removed = True

    return removed


UNKNOWN_SIZE: t.Final[int] = -1
"""Sentinel value for a disk-usage measurement that was not taken or failed."""


async def _du_mb(path: Path) -> int:
    """Return the disk usage of a path in MB via `du -sm`.

    Parameters
    ----------
    path : Path
        The file or directory to measure.

    Returns
    -------
    int
        The size in MB, or `UNKNOWN_SIZE` when `du` produces no parseable total.
    """
    # _run_cmd logs and returns "" on a non-zero exit rather than raising
    result = await asyncio.to_thread(_run_cmd, f"du -sm {shlex.quote(str(path))}")

    try:
        return int(result.split()[0])
    except (ValueError, IndexError):
        return UNKNOWN_SIZE


async def step_disk_usage(step_root: Path) -> int:
    """Measure the disk usage of a single step's own directory tree.

    Nested sub-steps live under `JobFileSystemManager._TASKS_NAME` and are
    measured independently, so that subtree is subtracted here to avoid
    double-counting.

    Parameters
    ----------
    step_root : Path
        The root directory of the step.

    Returns
    -------
    int
        The size of the step's directory in MB, excluding nested sub-steps;
        0 when the directory does not exist; `UNKNOWN_SIZE` when the
        measurement fails.
    """
    tasks_dir = step_root / JobFileSystemManager._TASKS_NAME

    try:
        if not step_root.is_dir():
            return 0
        has_nested = tasks_dir.is_dir()
    except OSError:
        return UNKNOWN_SIZE

    total = await _du_mb(step_root)
    if total < 0 or not has_nested:
        return total

    nested = await _du_mb(tasks_dir)
    return UNKNOWN_SIZE if nested < 0 else max(total - nested, 0)


def symlink_relative(link: Path, target: Path, *, within: Path | None = None) -> None:
    """Create `link` pointing at `target` with a relative link text.

    The link text is `target` relative to the directory holding `link`, so the
    pair keeps working after the tree containing both is copied or moved as a
    unit. Both are measured from where they physically are (symlinked
    directories on either path are resolved, the final component of `target` is
    not), since the kernel resolves `..` in link text physically. Parent
    directories of `link` are created as needed.

    Parameters
    ----------
    link : Path
        The symlink to create.
    target : Path
        The file or directory the link should point at.
    within : Path | None
        When given, the link is relative only if both `link` and `target` lie
        inside this directory, the tree that moves as a unit. Otherwise it is
        absolute: a chain of `..` that climbs out of one tree and into another
        breaks as soon as only one of the two moves, while an absolute path keeps
        naming the file where it still is.
    """
    link.parent.mkdir(parents=True, exist_ok=True)
    physical_link = link.parent.resolve() / link.name
    physical_target = target.parent.resolve() / target.name
    if within is not None:
        root = within.resolve()
        if not (
            physical_link.is_relative_to(root) and physical_target.is_relative_to(root)
        ):
            link.symlink_to(physical_target)
            return

    link.symlink_to(Path(os.path.relpath(physical_target, physical_link.parent)))


def rotate_file(path: Path) -> Path | None:
    """Rotate an existing file out of the way, logrotate-style.

    If `path` exists, it is renamed to the lowest-numbered unused
    `<name>.<i>` sibling (starting at `.1`), so a fresh file can be written
    at `path` without discarding the previous attempt's content.

    Parameters
    ----------
    path : Path
        The file to rotate out of the way.

    Returns
    -------
    Path | None
        The path the existing file was renamed to, or `None` when `path`
        did not exist and nothing was rotated.
    """
    if not path.exists():
        return None

    i = 1
    rotated = path.with_name(f"{path.name}.{i}")
    while rotated.exists():
        i += 1
        rotated = path.with_name(f"{path.name}.{i}")

    path.rename(rotated)
    log.debug("Rotated existing file `%s` to `%s`", path, rotated)
    return rotated


def get_backup_path(path: Path, backup_ext: str = ".bak") -> Path:
    """Identify a unique backup path for the input.

    Adds `.bak` on first execution and appends `.bak.<i>` for each subsequent
    backup to ensure the original is never lost.

    Parameters
    ----------
    path : Path
        The source path
    backup_ext : str
        An extension differentiating the backup files from the source file.

    Returns
    -------
    Path
    """
    if not backup_ext.startswith("."):
        # ensure a new extension is always added
        backup_ext = f".{backup_ext}"

    suffix = f"{path.suffix}{backup_ext}"
    backup_path = path.with_suffix(suffix)
    i = 1
    while backup_path.exists():
        suffix = f"{path.suffix}{backup_ext}.{i:03d}"
        backup_path = path.with_suffix(suffix)
        i += 1
    return backup_path


def write_bytes_atomic(path: Path, data: bytes, *, sync: bool = True) -> None:
    """Replace the file at `path` with `data`, or leave it exactly as it was.

    The data is written, flushed and (unless `sync` is `False`) synced to a
    temporary file beside the target, given the target's permissions (or the umask
    default for a new file), and renamed over it. A failure at any point, such as a
    full disk, leaves the original intact and removes the temporary file. A
    symlink at `path` keeps its link: its target is replaced. The target becomes a
    new inode, so hard links to it, its ownership and its extended attributes are
    not preserved.

    Parameters
    ----------
    path : Path
        The file to write; its parent directories are created.
    data : bytes
        The new content.
    sync : bool
        Whether to fsync the temporary file before the rename. When `False` the
        rename is still atomic, but a crash within the OS flush window can lose
        the content: pass `False` for files rewritten on every state change.
    """
    target = path.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    mode = stat.S_IMODE(target.stat().st_mode) if target.exists() else None

    # The kernel applies the umask to the creation mode atomically, so a new file
    # needs no process-global umask toggle.
    temp = target.parent / f".{target.name}.{uuid.uuid4().hex}.tmp"
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            if sync:
                os.fsync(handle.fileno())
        if mode is not None:
            os.chmod(temp, mode)
        os.replace(temp, target)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise


def write_text_atomic(
    path: Path, text: str, *, encoding: str = "utf-8", sync: bool = True
) -> None:
    """Replace the file at `path` with `text`, or leave it exactly as it was.

    Encodes the text and delegates to `write_bytes_atomic`, which documents the
    guarantees.

    Parameters
    ----------
    path : Path
        The file to write; its parent directories are created.
    text : str
        The new content.
    encoding : str
        The codec used to encode `text`.
    sync : bool
        Whether to fsync before the rename; see `write_bytes_atomic`.
    """
    write_bytes_atomic(path, text.encode(encoding), sync=sync)


LOCK_POLL_INTERVAL: t.Final[float] = 5.0
"""Seconds between attempts to take a lock that has a timeout."""

LOCK_LOG_INTERVAL: t.Final[float] = 60.0
"""Seconds between the messages logged while waiting for a lock that has a timeout."""

_FLOCK_UNSUPPORTED_ERRNOS: t.Final[frozenset[int]] = frozenset(
    {errno.ENOLCK, errno.EOPNOTSUPP, errno.ENOTSUP, errno.ENOSYS, errno.EINVAL}
)
"""`flock` errors meaning the file system cannot lock, rather than that the call failed."""


class LockTimeoutError(TimeoutError):
    """Another holder still had a lock when the caller's timeout ran out."""


class LockUnsupportedError(OSError):
    """The file system holding a lock file does not support `flock`."""


def _flock(lock_file: t.IO[str], path: Path, operation: int) -> None:
    """Apply a `flock` operation, naming a file system that cannot lock.

    Parameters
    ----------
    lock_file : IO[str]
        The open lock file.
    path : Path
        The lock file's path, for the error message.
    operation : int
        The `fcntl.LOCK_*` flags.

    Raises
    ------
    LockUnsupportedError
        If the file system does not support `flock`.
    """
    try:
        fcntl.flock(lock_file, operation)
    except OSError as ex:
        if ex.errno in _FLOCK_UNSUPPORTED_ERRNOS:
            msg = f"Cannot lock {path}: {ex.strerror}"
            raise LockUnsupportedError(ex.errno, msg) from ex
        raise


def _acquire(lock_file: t.IO[str], path: Path, timeout: float | None) -> None:
    """Take an exclusive `flock` on `lock_file`, waiting up to `timeout` seconds.

    Parameters
    ----------
    lock_file : IO[str]
        The open lock file.
    path : Path
        The lock file's path, for messages.
    timeout : float | None
        Seconds to wait for another holder to release the lock; `None` waits as
        long as it takes.

    Raises
    ------
    LockTimeoutError
        If another holder still has the lock after `timeout` seconds.
    LockUnsupportedError
        If the file system does not support `flock`.
    """
    if timeout is None:
        _flock(lock_file, path, fcntl.LOCK_EX)
        return

    start = time.monotonic()
    next_log = start
    while True:
        try:
            _flock(lock_file, path, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            pass

        now = time.monotonic()
        waited = now - start
        if waited >= timeout:
            msg = f"Another holder kept the lock at {path} for {waited:.0f} s"
            raise LockTimeoutError(msg)
        if now >= next_log:
            log.info(
                "Waiting for the lock at %s (%.0f s so far, giving up after %.0f s)",
                path,
                waited,
                timeout,
            )
            next_log = now + LOCK_LOG_INTERVAL
        time.sleep(min(LOCK_POLL_INTERVAL, timeout - waited))


@contextmanager
def locked(path: Path, *, timeout: float | None = None) -> Iterator[None]:
    """Hold an exclusive `flock` on the file at `path`.

    The lock file and its parent directories are created as needed and left in
    place afterwards. The lock belongs to the open file, so it serializes threads
    as well as processes, and the kernel releases it if the holder dies.

    Without a `timeout` the call blocks until the lock is free. With one, it tries
    every `LOCK_POLL_INTERVAL` seconds and logs that it is waiting when first
    refused and every `LOCK_LOG_INTERVAL` seconds after that.

    Parameters
    ----------
    path : Path
        The lock file.
    timeout : float | None
        Seconds to wait for another holder to release the lock; `None` waits as
        long as it takes.

    Yields
    ------
    None
        While the lock is held.

    Raises
    ------
    LockTimeoutError
        If another holder still has the lock after `timeout` seconds.
    LockUnsupportedError
        If the file system holding `path` does not support `flock`.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as lock_file:
        _acquire(lock_file, path, timeout)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
