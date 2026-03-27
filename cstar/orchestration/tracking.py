import asyncio
import typing as t
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field

from cstar.base.log import LoggingMixin
from cstar.base.utils import slugify, utc_now
from cstar.execution.file_system import (
    StateDirectoryManager,
    is_remote_resource,
    local_copy,
)
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import PersistenceMode, deserialize, serialize


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

    @staticmethod
    def get_default_run_id(path: Path | str) -> str:
        """Generate a run-id based on the name of a `Workplan`

        Parameters
        ----------
        path : Path
            The path to a persisted workplan.

        Returns
        -------
        str
        """
        path_string = str(path)
        if is_remote_resource(path_string):
            with local_copy(path_string) as local_path:
                wp = deserialize(local_path, Workplan)
        else:
            wp = deserialize(Path(path), Workplan)

        return slugify(wp.name)


class TrackingRepository(LoggingMixin):
    """The API for persisting tracking data."""

    _root: t.Final[Path]
    """The root directory where tracking files are stored."""

    _LATEST_DIR: t.Final[str] = "latest"
    """The directory containing a mapping to the last run using a given run-id."""

    _HISTORY_DIR: t.Final[str] = "history"
    """The directory containing all run history."""

    _MODE: PersistenceMode = PersistenceMode.yaml
    """The serialization mode to use."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._root = StateDirectoryManager.tracking_dir()

    @property
    def latest_dir(self) -> Path:
        """Return the path to the directory containing latest-run records.

        Returns
        -------
        Path
        """
        target_path = self._root / self._LATEST_DIR
        if not target_path.exists():
            target_path.mkdir(parents=True)
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
            target_path.mkdir(parents=True)
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

    def _runfile_name(self, run_id: str) -> str:
        """Generate the file name for persisting a `WorkplanRun` to disk.

        Parameters
        ----------
        run_id : str
            The run_id of the WorkplanRun

        Returns
        -------
        str
        """
        return f"{run_id}.{TrackingRepository._MODE.value}"

    def _latest_path(self, run_id: str) -> Path:
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
        runfile_name = self._runfile_name(run_id)
        return self.latest_dir / runfile_name

    def _history_path(self, run_id: str, run_date: datetime) -> Path:
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
        formatted_dt = self._format_run_date(run_date)
        runfile_name = self._runfile_name(formatted_dt)
        return self.history_dir / run_id / runfile_name

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
            path = self._history_path(run_id, run_date)
            if path.exists():
                return path

        return self._latest_path(run_id)

    async def get_workplan_run(
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
        run_path = self._find_run_path(run_id, run_date)

        if not run_path.exists():
            rd_out = run_date or "latest"
            msg = f"No run file for `{run_id}` on `{rd_out}` found in {run_path}`"
            self.log.warning(msg)
            return None

        return await asyncio.to_thread(deserialize, run_path, WorkplanRun)

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
        run_path = self._history_path(run.run_id, run.start_at)
        latest_path = self._latest_path(run.run_id)

        if not await asyncio.to_thread(serialize, run_path, run):
            self.log.warning("Run could not be persisted")

        if not latest_path.parent.exists():
            latest_path.parent.mkdir(parents=True)

        await asyncio.to_thread(latest_path.unlink, missing_ok=True)
        await asyncio.to_thread(latest_path.symlink_to, run_path)

        msg = f"Run persisted to: {run_path}"
        self.log.debug(msg)
        return run_path

    async def list_latest_runs(self, run_id_filter: str) -> list[WorkplanRun]:
        """Retrieve a list of the latest WorkplanRun for all known run-id's.

        run_id_filter : str
            A run-id used to filter records. Matches will be included in results.

        Returns
        -------
        list[WorkplanRun]
        """
        run_paths = list(self.latest_dir.glob(f"{run_id_filter}*.{self._MODE}"))
        coros = [
            asyncio.to_thread(deserialize, run_path, WorkplanRun)
            for run_path in run_paths
        ]
        return await asyncio.gather(*coros)

    async def list_history_runs(self, run_id_filter: str) -> list[WorkplanRun]:
        """Retrieve a list of the latest WorkplanRun for all known run-id's.

        run_id_filter : str
            A run-id used to filter records. Matches will be included in results.

        Returns
        -------
        list[WorkplanRun]
        """
        # Filter run-id subfolder w/filename format YYYYMMDDHHMMSS.XXXXXX.yaml
        glob_pattern = f"{run_id_filter}*/??????????????.??????.{self._MODE}"
        run_paths = list(self.history_dir.rglob(glob_pattern))
        coros = [
            asyncio.to_thread(deserialize, run_path, WorkplanRun)
            for run_path in run_paths
        ]
        return await asyncio.gather(*coros)
