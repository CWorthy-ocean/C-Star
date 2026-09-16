import asyncio
import fcntl
import os
import typing as t
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field

from cstar.base.env import max_concurrency
from cstar.base.log import LoggingMixin, get_logger
from cstar.base.utils import slugify, utc_now
from cstar.execution.file_system import (
    UNKNOWN_SIZE,
    StateDirectoryManager,
    local_copy,
    step_disk_usage,
)
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import (
    PersistenceMode,
    deserialize,
    deserialize_all,
    serialize,
)

log = get_logger(__name__)

KEY_RUN_NAME: t.Final[str] = "name"
"""Key used to store the workplan name in the run metadata."""


class WorkplanRun(BaseModel):
    """A record containing metadata about an individual execution of a `Workplan`."""

    workplan_path: Path
    """The path to the original workplan."""

    trx_workplan_path: Path
    """The path to the transformed workplan."""

    output_path: Path
    """The path where workplan output is written."""

    run_id: str
    """The unique identifier used to reference the run."""

    start_at: datetime = Field(default_factory=utc_now)
    """The date and time when the workplan run was triggered."""

    environment: dict[str, str] = Field(default_factory=dict)
    """The environment variables at the time of the run."""

    user_variables: Mapping[str, str] = Field(default_factory=dict)
    """User-supplied runtime variable overrides."""

    sentinels: set[Path] = Field(default_factory=set[Path])
    """State files expected to be created during execution of the run."""

    metadata: dict[str, str] = Field(default_factory=dict)
    """Optional metadata for the run."""

    step_sizes: dict[str, int] = Field(default_factory=dict)
    """Disk usage in MB per step name (each step's own tree, excluding nested sub-steps)."""

    size_measured_at: datetime | None = Field(default=None)
    """When `step_sizes` was last measured; `None` until measured."""

    @property
    def size_mb(self) -> int:
        """Total measured disk usage in MB, or `UNKNOWN_SIZE` when nothing has been measured.

        Returns
        -------
        int
        """
        return sum(self.step_sizes.values()) if self.step_sizes else UNKNOWN_SIZE

    def record_step_sizes(self, sizes: Mapping[str, int]) -> None:
        """Merge measured per-step sizes into the record and stamp the measurement time.

        Parameters
        ----------
        sizes : Mapping[str, int]
            Successfully measured disk usage (MB) keyed on step name.
        """
        self.step_sizes.update(sizes)
        self.size_measured_at = utc_now()

    @staticmethod
    def get_default_run_id(uri: str) -> str:
        """Generate a run-id based on the name of a `Workplan`

        Parameters
        ----------
        uri : str
            The local or remote path to a persisted workplan.

        Returns
        -------
        str
        """
        with local_copy(uri) as local_path:
            wp = deserialize(local_path, Workplan)

        try:
            return slugify(wp.name)
        except ValueError as ex:
            msg = f"Unable to generate a default run-id from workplan at: {uri}"
            raise ValueError(msg) from ex

    @property
    def state_dir(self) -> Path:
        """The path to the directory containing state files for the run.

        Returns
        -------
        Path
        """
        return StateDirectoryManager.run_state_dir(run_id=self.run_id)


async def measure_step_sizes(
    run: WorkplanRun, step_dirs: Mapping[str, Path], sem: asyncio.Semaphore
) -> dict[str, int]:
    """Measure and record disk usage for a set of a run's step directories.

    Successful measurements are recorded on `run` (in memory only) and
    returned so callers can apply them to the persisted record atomically
    via `TrackingRepository.update_workplan_run`; a step whose measurement
    fails retains whatever value (if any) was previously recorded for it.

    Parameters
    ----------
    run : WorkplanRun
        The run whose `step_sizes` will be updated in place.
    step_dirs : Mapping[str, Path]
        Mapping of step name to the step's root directory.
    sem : asyncio.Semaphore
        A semaphore bounding concurrent disk-usage measurements.

    Returns
    -------
    dict[str, int]
        The successfully measured sizes (MB) keyed on step name.
    """

    async def _measure(name: str, step_dir: Path) -> tuple[str, int]:
        async with sem:
            return name, await step_disk_usage(step_dir)

    if not step_dirs:
        return {}

    results = await asyncio.gather(*(_measure(n, d) for n, d in step_dirs.items()))
    measured = {name: size for name, size in results if size >= 0}

    if failed := [name for name, size in results if size < 0]:
        log.debug(f"Disk usage measurement failed for step(s): {', '.join(failed)}")

    run.record_step_sizes(measured)
    return measured


class TrackingRepository(LoggingMixin):
    """The API for persisting tracking data."""

    _LATEST_DIR: t.Final[str] = "latest"
    """The directory containing a mapping to the last run using a given run-id."""

    _HISTORY_DIR: t.Final[str] = "history"
    """The directory containing all run history."""

    _MODE: PersistenceMode = PersistenceMode.yaml
    """The serialization mode to use."""

    _sem: asyncio.Semaphore | None = None
    """A semaphore used to limit concurrent disk accesses."""

    @property
    def _root(self) -> Path:
        """Return the root directory where tracking files are stored."""
        return StateDirectoryManager.tracking_dir()

    @property
    def latest_dir(self) -> Path:
        """Return the path to the directory containing latest-run records.

        Returns
        -------
        Path
        """
        target_path = self._root / self._LATEST_DIR
        if not target_path.exists():
            target_path.mkdir(parents=True, exist_ok=True)
        return target_path

    @property
    def history_dir(self) -> Path:
        """Return the path to the directory containing history records for all runs.

        Returns
        -------
        Path
        """
        target_path = self._root / self._HISTORY_DIR
        if not target_path.exists():
            target_path.mkdir(parents=True, exist_ok=True)
        return target_path

    @classmethod
    def _format_run_date(cls, run_date: datetime) -> str:
        """Format a run date as a unique name for writing the run record to disk.

        Parameters
        ----------
        run_date : datetime
            The date to format

        Returns
        -------
        str
        """
        return run_date.strftime("%Y%m%d%H%M%S.%f")

    def _runfile_name(self, run_date: datetime) -> str:
        """Generate the file name for persisting a `WorkplanRun` history entry to disk.

        Parameters
        ----------
        run_date : datetime
            The start time of the run.

        Returns
        -------
        str
        """
        formatted_dt = self._format_run_date(run_date)
        return f"{formatted_dt}.{TrackingRepository._MODE.value}"

    def _latestfile_name(self, run_id: str) -> str:
        """Generate the file name for persisting the latest `WorkplanRun` history entry to disk.

        Parameters
        ----------
        run_id : str
            The run_id of the WorkplanRun

        Returns
        -------
        str
        """
        return f"{run_id}.{TrackingRepository._MODE.value}"

    def latest_path(self, run_id: str) -> Path:
        """Generate the full path for persisting a `WorkplanRun` to disk as
        the "latest run" record.

        Parameters
        ----------
        run_id : str
            The run_id of the WorkplanRun

        Returns
        -------
        Path
        """
        runfile_name = self._latestfile_name(run_id)
        return self.latest_dir / runfile_name

    def run_history_dir(self, run_id: str) -> Path:
        """Generate the path to the history directory."""
        return self.history_dir / run_id

    def history_path(self, run_id: str, run_date: datetime) -> Path:
        """Generate the full path for persisting a `WorkplanRun` to disk as
        a history record.

        Parameters
        ----------
        run_id : str
            The run_id of the WorkplanRun
        run_date : datetime
            The datetime the run was executed

        Returns
        -------
        Path
        """
        runfile_name = self._runfile_name(run_date)
        return self.run_history_dir(run_id) / runfile_name

    def _find_run_path(self, run_id: str, run_date: datetime | None) -> Path:
        """Identify a path where a `WorkplanRun` is persisted to disk.

        Looks for an exact match in history if `run_date` is supplied and falls back
        to latest run if it cannot be found.

        Parameters
        ----------
        run_id : str
            The run_id of the WorkplanRun
        run_date : datetime | None
            The datetime the run was executed or `None`.

        Returns
        -------
        Path
        """
        if run_date:
            path = self.history_path(run_id, run_date)
            if path.exists():
                msg = f"Located workplan run in history for {run_id!r} at: {path}"
                self.log.trace(msg)
                return path

        path = self.latest_path(run_id)
        if path.exists():
            msg = f"Located latest run of {run_id!r} at: {path}"
            self.log.trace(msg)

        return path

    def list_runtracking_paths(
        self,
        run_id: str,
        all_history: bool = False,
    ) -> Sequence[Path]:
        """Identify all paths to state information for a specific run-id.

        Returns all paths previously used with the run-id, regardless of the
        associated workplan.

        Parameters
        ----------
        run_id : str
            The run_id of the WorkplanRun
        all_history : bool
            Pass `True` to return the paths for any run that shares the run-id. Otherwise,
            only the latest path from the history collection is returned.

        Returns
        -------
        Sequence[Path]
        """
        run_paths: list[Path] = []

        latest = self.latest_path(run_id)
        lock_path = self._get_lock_path(run_id)
        with lock_path.open("w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)

            if latest.exists():
                run_paths.append(latest)

            search_dir = self.run_history_dir(run_id=run_id)
            all_runs = search_dir.iterdir() if search_dir.exists() else list[Path]()

            if all_history:
                run_paths.extend(all_runs)
            else:
                if ordered_runs := sorted(
                    all_runs,
                    key=lambda p: os.path.getctime(p),
                    reverse=True,
                ):
                    run_paths.append(ordered_runs[0])

        items = tuple(filter(lambda p: p.exists(), run_paths))
        return tuple(sorted(list({p.resolve() for p in items})))

    def get_workplan_run_sync(
        self, run_id: str, run_date: datetime | None = None
    ) -> WorkplanRun | None:
        """Locate a WorkplanRun record.

        Parameters
        ----------
        run_id : str
            The run_id of the WorkplanRun
        run_date : datetime | None
            The datetime the run was executed or `None`.

        Returns
        -------
        WorkplanRun | None
            The record when it can be located in history or latest runs, otherwise `None`.
        """
        try:
            run_id = slugify(run_id)
        except ValueError as ex:
            msg = "A valid run-id was not provided; unable to retrieve run"
            raise ValueError(msg) from ex

        run_path = self._find_run_path(run_id, run_date)

        lock_path = self._get_lock_path(run_id)

        with lock_path.open("w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)

            if not run_path.exists():
                rd_out = run_date or "latest"
                msg = f"No run file for `{run_id}` on `{rd_out}` found in {run_path}`"
                self.log.warning(msg)
                return None

            return deserialize(run_path, WorkplanRun)

    async def get_workplan_run(
        self,
        run_id: str,
        run_date: datetime | None = None,
    ) -> WorkplanRun | None:
        """Locate a WorkplanRun record.

        Parameters
        ----------
        run_id : str
            The run_id of the WorkplanRun
        run_date : datetime | None
            The datetime the run was executed or `None`.

        Returns
        -------
        WorkplanRun | None
            The record when it can be located in history or latest runs, otherwise `None`.
        """
        if self._sem:
            async with self._sem:
                return await asyncio.to_thread(
                    self.get_workplan_run_sync, run_id, run_date
                )
        return await asyncio.to_thread(self.get_workplan_run_sync, run_id, run_date)

    def _write_run_locked(self, run: WorkplanRun) -> Path:
        """Persist a run record and repoint its "latest" link; caller holds the lock.

        Parameters
        ----------
        run : WorkplanRun
            The record to persist.

        Returns
        -------
        Path
            The path to the persisted history record
        """
        run_path = self.history_path(run.run_id, run.start_at)
        latest_path = self.latest_path(run.run_id)

        if not serialize(run_path, run):
            self.log.warning("Run could not be persisted")

        latest_path.unlink(missing_ok=True)
        latest_path.symlink_to(run_path)

        msg = f"Run persisted to: {run_path}"
        self.log.debug(msg)
        return run_path

    def update_workplan_run_sync(
        self, run_id: str, mutate: Callable[[WorkplanRun], None]
    ) -> WorkplanRun | None:
        """Atomically load, mutate and persist the latest record for a run-id.

        The run lock is held across the whole read-modify-write, so concurrent
        updates to the same run (e.g. several steps finishing in one poll
        tick) cannot overwrite each other.

        Parameters
        ----------
        run_id : str
            The run-id whose latest record will be updated.
        mutate : Callable[[WorkplanRun], None]
            Applies the change to the freshly loaded record in place.

        Returns
        -------
        WorkplanRun | None
            The persisted record, or `None` when no record exists for the run-id.
        """
        try:
            run_id = slugify(run_id)
        except ValueError as ex:
            msg = "A valid run-id was not provided; unable to update run"
            raise ValueError(msg) from ex

        run_path = self._find_run_path(run_id, None)
        lock_path = self._get_lock_path(run_id)

        with lock_path.open("w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)

            if not run_path.exists():
                self.log.warning(f"No run file for `{run_id}` found in {run_path}`")
                return None

            run = deserialize(run_path, WorkplanRun)
            mutate(run)
            self._write_run_locked(run)
            return run

    async def update_workplan_run(
        self, run_id: str, mutate: Callable[[WorkplanRun], None]
    ) -> WorkplanRun | None:
        """Atomically load, mutate and persist the latest record for a run-id.

        Parameters
        ----------
        run_id : str
            The run-id whose latest record will be updated.
        mutate : Callable[[WorkplanRun], None]
            Applies the change to the freshly loaded record in place.

        Returns
        -------
        WorkplanRun | None
            The persisted record, or `None` when no record exists for the run-id.
        """
        if self._sem:
            async with self._sem:
                return await asyncio.to_thread(
                    self.update_workplan_run_sync, run_id, mutate
                )
        return await asyncio.to_thread(self.update_workplan_run_sync, run_id, mutate)

    def put_workplan_run_sync(self, run: WorkplanRun) -> Path:
        """Persist a run record to disk.

        Inserts a new history record and updates the "latest" record for the run-id.

        Parameters
        ----------
        run : WorkplanRun
            The run to persist

        Returns
        -------
        Path
            The path to the persisted history record
        """
        # use the latest path (e.g. /tmp/<run-id>.yaml) as a lock in case
        # multiple runs occur simultaneously.
        lock_path = self._get_lock_path(run.run_id)

        with lock_path.open("w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            return self._write_run_locked(run)

    async def put_workplan_run(self, run: WorkplanRun) -> Path:
        """Persist a run record to disk.

        Inserts a new history record and updates the "latest" record for the run-id

        Parameters
        ----------
        run : WorkplanRun
            The run to persist

        Returns
        -------
        Path
            The path to the persisted history record
        """
        if self._sem:
            async with self._sem:
                coro = asyncio.to_thread(self.put_workplan_run_sync, run)
                return await coro

        return await asyncio.to_thread(self.put_workplan_run_sync, run)

    def _get_lock_path(self, run_id: str) -> Path:
        """Identify the path to the lock-file for a run id.

        Parameters
        ----------
        run_id : str
            The run-id to be locked.

        Returns
        -------
        Path
        """
        latest_path = self.latest_path(run_id)
        lock_path = latest_path.with_suffix(".lock")

        if not lock_path.parent.exists():
            lock_path.parent.mkdir(parents=True, exist_ok=True)

        return lock_path

    async def _deserialize_readable_runs(
        self, run_paths: Sequence[Path]
    ) -> Sequence[WorkplanRun]:
        """Deserialize run records, dropping any that cannot be read.

        Unreadable paths are collapsed into a single warning log entry
        rather than one per path.

        Parameters
        ----------
        run_paths : Sequence[Path]
            The candidate paths to deserialize.

        Returns
        -------
        Sequence[WorkplanRun]
            The successfully deserialized run records.
        """
        limit = max_concurrency()
        results = await deserialize_all(
            list(run_paths), WorkplanRun, limit, sem=self._sem
        )

        runs: list[WorkplanRun] = []
        unreadable: list[Path] = []
        for path, run in zip(run_paths, results, strict=True):
            if run is None:
                unreadable.append(path)
            else:
                runs.append(run)

        if unreadable:
            paths_str = ", ".join(str(p) for p in unreadable)
            msg = (
                f"{len(unreadable)} run record(s) could not be read and were "
                f"skipped: {paths_str}"
            )
            self.log.warning(msg)

        return runs

    async def list_latest_runs(self, run_id_filter: str = "") -> Sequence[WorkplanRun]:
        """Retrieve a list of the latest WorkplanRun for all known run-id's.

        Parameters
        ----------
        run_id_filter : str
            A run-id prefix used to filter records. Matches will be included
            in results.

        Returns
        -------
        Sequence[WorkplanRun]
            The latest run record per matching run-id; a record that cannot
            be read is skipped.
        """
        run_paths = list(self.latest_dir.glob(f"{run_id_filter}*.{self._MODE}"))
        return await self._deserialize_readable_runs(run_paths)

    async def list_history_runs(self, run_id_filter: str) -> Sequence[WorkplanRun]:
        """Retrieve a list of all WorkplanRun instances executed with a given run-id.

        Parameters
        ----------
        run_id_filter : str
            A run-id prefix used to filter records. Matches will be included
            in results.

        Returns
        -------
        Sequence[WorkplanRun]
            Every historical run record for matching run-ids; a record that
            cannot be read is skipped.
        """
        # Filter run-id subfolder w/filename format YYYYMMDDHHMMSS.XXXXXX.yaml
        glob_pattern = f"{run_id_filter}*/??????????????.??????.{self._MODE}"
        run_paths = list(self.history_dir.rglob(glob_pattern))
        return await self._deserialize_readable_runs(run_paths)

    @classmethod
    def bound(cls, limit: int) -> AbstractAsyncContextManager["TrackingRepository"]:
        """Create a repository whose disk operations are concurrency-bounded.

        Parameters
        ----------
        limit : int
            The maximum number of concurrent disk operations.

        Returns
        -------
        AsyncContextManager[TrackingRepository]
            An async context manager yielding a new repository that holds a
            semaphore for the lifetime of the context.
        """

        @asynccontextmanager
        async def _manager() -> AsyncGenerator[TrackingRepository]:
            tracking = TrackingRepository()
            try:
                tracking._sem = asyncio.Semaphore(limit)
                yield tracking
            finally:
                tracking._sem = None

        return _manager()
