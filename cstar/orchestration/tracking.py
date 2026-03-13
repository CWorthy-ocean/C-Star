import typing as t
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field

from cstar.base.log import LoggingMixin
from cstar.base.utils import slugify
from cstar.execution.file_system import (
    StateDirectoryManager,
    is_remote_resource,
    local_copy,
)
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import PersistenceMode, deserialize, serialize


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


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

    environment: dict[str, str] = {}
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
        return self._root / self._LATEST_DIR

    @property
    def history_dir(self) -> Path:
        return self._root / self._HISTORY_DIR

    def _format_run_date_as_path(self, run_date: datetime) -> str:
        return run_date.strftime("%Y/%m/%d/%H/%M/%S")

    def _format_run_date(self, run_date: datetime) -> str:
        return run_date.strftime("%Y%m%d%H%M%S")

    def _runfile_name(self, run_id: str) -> str:
        return f"{run_id}.{TrackingRepository._MODE.value}"

    def _latest_path(self, run_id: str) -> Path:
        runfile_name = self._runfile_name(run_id)
        return self.latest_dir / runfile_name

    def _history_path(self, run_id: str, run_date: datetime) -> Path:
        formatted_dt = self._format_run_date(run_date)
        runfile_name = self._runfile_name(formatted_dt)
        return self.history_dir / run_id / runfile_name

    def _find_run_path(self, run_id: str, run_date: datetime | None) -> Path:
        if run_date:
            path = self._history_path(run_id, run_date)
            if path.exists():
                return path

        return self._latest_path(run_id)

    def get_workplan_run(
        self, run_id: str, run_date: datetime | None = None
    ) -> WorkplanRun | None:
        run_path = self._find_run_path(run_id, run_date)

        if not run_path.exists():
            rd_out = run_date if run_date else "latest"
            msg = f"No run file for `{run_id}` on `{rd_out}` found in {run_path}`"
            self.log.warning(msg)
            return None

        return deserialize(run_path, WorkplanRun)

    def put_workplan_run(self, run: WorkplanRun) -> Path:
        run_path = self._history_path(run.run_id, run.start_at)
        latest_path = self._latest_path(run.run_id)

        if not serialize(run_path, run):
            self.log.warning("Run could not be persisted")

        if not latest_path.parent.exists():
            latest_path.parent.mkdir(parents=True)

        latest_path.unlink(missing_ok=True)
        latest_path.symlink_to(run_path)

        msg = f"Run persisted to: {run_path}"
        self.log.debug(msg)
        return run_path
