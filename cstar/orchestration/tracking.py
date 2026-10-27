import asyncio
import fcntl
import os
import typing as t
from collections.abc import (
    AsyncGenerator,
    Callable,
    Generator,
    Mapping,
    Sequence,
)
from contextlib import (
    AbstractAsyncContextManager,
    asynccontextmanager,
    contextmanager,
)
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    TypeAdapter,
    ValidationError,
    model_validator,
)

from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.log import LoggingMixin, get_logger
from cstar.base.utils import MAX_CONCURRENCY, slugify, utc_now
from cstar.execution.file_system import (
    UNKNOWN_SIZE,
    StateDirectoryManager,
    local_copy,
    step_disk_usage,
    symlink_relative,
)
from cstar.orchestration.manifest import (
    DOCUMENT_READ_ERRORS,
    RunManifest,
    find_transformed_workplan,
    read_run_manifest,
    recorded_step_status,
)
from cstar.orchestration.models import LEGACY_TRANSFORMED_NAME_SUFFIX
from cstar.orchestration.orchestration import (
    LiveWorkplan,
    ProcessHandle,
    read_workplan,
)
from cstar.orchestration.serialization import (
    read_all,
    read_document,
    serialize,
    try_deserialize,
)
from cstar.orchestration.state import StateRepository
from cstar.system.migration import (
    TOLERATE_UNKNOWN,
    CstarSchemaTooNewError,
    VersionedDocument,
    warn_unknown_keys,
)

log = get_logger(__name__)

KEY_RUN_UUID: t.Final[str] = "uuid"
"""Key used to cache the run manifest's uuid in the run metadata; the manifest owns it."""

_LEGACY_KEY_NAME: t.Final[str] = "name"
"""Metadata key under which records written by cstar 0.17.0 and earlier held the
workplan name; the manifest holds it now. The three `_LEGACY_KEY_*` keys leave
with `LegacyRecord` in release 0.20.0."""

_LEGACY_KEY_ATTACHED: t.Final[str] = "attached"
"""Metadata key whose value `"1"` marked an attached run in records written by
cstar 0.17.0 and earlier; the `attached` field holds it now."""

_LEGACY_KEY_ACCESS: t.Final[str] = "access"
"""Metadata key that held the `Access` value in records written by cstar 0.17.0
and earlier; the `access` field holds it now."""


class Access(StrEnum):
    """Whether verbs may write into a tracked run's directory.

    A record without the field is `owned`: every run this machine scheduled, and
    records written before the field existed.
    """

    owned = "owned"
    """The run is the user's: verbs that write into its directory (gather,
    check --repair, --record-digests, admin clean) may."""

    read_only = "read-only"
    """The run is read and referenced; nothing writes into its directory."""


class LegacyRecord(BaseModel):
    """What a record written by cstar 0.17.0 or earlier said about its run.

    This release derives these from the run's manifest and records none of
    them for a run it schedules. A record that arrived with them keeps them,
    under `legacy`, for the run that has no readable manifest to ask: one
    scheduled before the manifest existed, or whose directory is gone.

    Slated for removal in release 0.20.0, together with the folding of the
    0.17.0 metadata keys in `WorkplanRun._read_older_record`: by then no run
    scheduled before the manifest existed is expected to matter, and a record
    without one degrades to an unknown name and the transformed workplan the
    run root holds.
    """

    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True, use_attribute_docstrings=True
    )

    name: str = Field(default="")
    """The workplan name the record held; empty when it held none."""

    start_at: datetime | None = Field(default=None)
    """When the record said the run was triggered."""

    workplan_path: Path | None = Field(default=None)
    """The authored workplan the record named."""

    trx_workplan_path: Path | None = Field(default=None)
    """The transformed workplan the record named."""


@dataclass(frozen=True)
class _Identity:
    """What the manifest says about a run, read once per record instance."""

    name: str
    start_at: datetime
    workplan_path: Path | None
    """The authored workplan as given when the run was scheduled; `None` for a
    manifest that records no workplan."""


def aware_utc(moment: datetime) -> datetime:
    """Return a moment with a time zone, a naive one being taken as UTC.

    Parameters
    ----------
    moment : datetime
        A timestamp read from a document.

    Returns
    -------
    datetime
    """
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _legacy_moment(value: t.Any) -> datetime | None:
    """Read a timestamp an older record held, `None` when it held none or junk."""
    if value is None:
        return None
    try:
        return TypeAdapter(datetime).validate_python(value)
    except ValidationError:
        return None


def _legacy_path(value: t.Any) -> Path | None:
    """Read a path an older record held, `None` when it held none."""
    return Path(str(value)) if value else None


class WorkplanRun(VersionedDocument):
    """The record this machine keeps of a run: what is live and local to it.

    The record lives in the state home and holds the run directory, the
    sentinels, the environment the run was scheduled with and what this machine
    measured. The run's name, start time and workplan paths are the manifest's
    (`cstar-run.yaml` in the run directory) and are derived from it, so a record
    never disagrees with the run it points at. A record written by cstar 0.17.0
    or earlier holds them too; those are kept as `legacy` and read when the run
    has no manifest to ask.
    """

    SCHEMA_LABEL: t.ClassVar[str] = "run record"

    schema_version: str = Field(default="1.1.0")
    """The version of the record's format. A record without one was written by
    cstar 0.17.0 or earlier and reads as 1.0.0; a newer one than this build's
    loads only when it holds nothing this build does not know (see
    `VersionedDocument`)."""

    output_path: Path
    """The path where workplan output is written: the run directory."""

    run_id: str
    """The unique identifier used to reference the run."""

    recorded_at: datetime = Field(default_factory=utc_now)
    """When this record was written on this machine: the key of its history
    entry, and the start time of last resort for a run whose manifest cannot
    be read."""

    environment: dict[str, str] = Field(default_factory=dict)
    """The environment variables at the time of the run."""

    user_variables: Mapping[str, str] = Field(default_factory=dict)
    """User-supplied runtime variable overrides."""

    sentinels: set[Path] = Field(default_factory=set[Path])
    """State files expected to be created during execution of the run."""

    metadata: dict[str, str] = Field(default_factory=dict)
    """Optional metadata for the run; the manifest's uuid is cached under
    `KEY_RUN_UUID`."""

    attached: bool = Field(default=False)
    """Whether the run was attached from elsewhere (by `cstar run attach`, or a
    move) rather than scheduled here: it has no sentinels on this machine and is
    never relaunched."""

    access: Access = Field(default=Access.owned)
    """Whether verbs may write into the run's directory."""

    step_sizes: dict[str, int] = Field(default_factory=dict)
    """Disk usage in MB per step name (each step's own tree, excluding nested sub-steps)."""

    size_measured_at: datetime | None = Field(default=None)
    """When `step_sizes` was last measured; `None` until measured."""

    legacy: LegacyRecord = Field(default_factory=LegacyRecord)
    """What the record said when cstar 0.17.0 or earlier wrote it; empty for a
    record of this release, and never written when empty."""

    _identity: _Identity | None = PrivateAttr(default=None)
    """The manifest's account of the run, once read."""

    _manifest_cache: RunManifest | None = PrivateAttr(default=None)
    """The manifest of an attached run, kept after its first read."""

    @model_validator(mode="before")
    @classmethod
    def _read_older_record(cls, data: t.Any) -> t.Any:
        """Accept a record written by cstar 0.17.0 or earlier.

        Such a record marks attachment and access in its metadata, and holds the
        workplan name there and the start time and workplan paths as fields.
        The marks become the typed fields, the start time keys the history
        entry as it did, and the rest moves under `legacy`, where this release
        keeps it. An access value this release does not know (a newer release
        wrote it) is read as read-only, the safe side, with a warning: the run
        stays listed and nothing writes into it.
        """
        if not isinstance(data, Mapping):
            return data
        data = dict(data)
        metadata = dict(data.get("metadata") or {})
        attached = metadata.pop(_LEGACY_KEY_ATTACHED, None)
        access = metadata.pop(_LEGACY_KEY_ACCESS, None)
        name = metadata.pop(_LEGACY_KEY_NAME, "")
        data["metadata"] = metadata
        if attached is not None:
            data.setdefault("attached", attached == "1")
        if access is not None:
            data.setdefault("access", access)
        if "access" in data and data["access"] not in set(Access):
            log.warning(
                f"Run {data.get('run_id')!r} records an access this release does "
                f"not know ({data['access']!r}); treating it as read-only."
            )
            data["access"] = Access.read_only

        start_at = _legacy_moment(data.pop("start_at", None))
        if start_at is not None:
            data.setdefault("recorded_at", start_at)
        held = {
            "name": str(name or ""),
            "start_at": start_at,
            "workplan_path": _legacy_path(data.pop("workplan_path", None)),
            "trx_workplan_path": _legacy_path(data.pop("trx_workplan_path", None)),
        }
        legacy = dict(data.get("legacy") or {})
        for key, value in held.items():
            if value and not legacy.get(key):
                legacy[key] = value
        data["legacy"] = legacy
        return data

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

    def reanchored(self) -> "WorkplanRun":
        """Follow the run to where the current data home keeps it, if it has moved.

        The record lives in the state home but names the run directory in the
        data home. When the recorded run directory is gone and the data home
        holds a directory for the run-id, the run was copied or moved and
        `CSTAR_DATA_HOME` repointed: the run directory follows, and with it
        everything derived from the manifest there. A record whose run
        directory exists is left alone, wherever it is, and so is one whose
        directories cannot be inspected.

        Returns
        -------
        WorkplanRun
            This record, or a copy pointing at the relocated run directory.
        """
        try:
            if self.output_path.exists():
                return self

            current = StateDirectoryManager.data_dir(self.run_id)
            if current == self.output_path or not current.is_dir():
                return self
        except OSError as ex:
            # e.g. an unreadable directory: it cannot be told whether the run moved
            log.debug(f"Unable to tell whether run {self.run_id!r} has moved: {ex}")
            return self

        log.debug(f"Run {self.run_id!r} moved from {self.output_path} to {current}")
        moved = self.model_copy(update={"output_path": current})
        moved._identity = None
        return moved

    def _from_manifest(self) -> _Identity | None:
        """The manifest's account of the run, read once; `None` when it cannot be read.

        A failed read is not remembered: the manifest may be being written.
        """
        if self._identity is None:
            try:
                manifest = self.manifest
            except CstarError as ex:
                log.debug(f"Run {self.run_id!r}: no manifest to derive from: {ex}")
                return None
            started = (
                manifest.attempts[-1].start_at
                if manifest.attempts
                else manifest.created_at
            )
            self._identity = _Identity(
                name=manifest.name,
                start_at=aware_utc(started),
                workplan_path=manifest.workplan_path,
            )
        return self._identity

    @property
    def name(self) -> str:
        """The name of the workplan the run was made from.

        Returns
        -------
        str
            The manifest's name; for a run without a readable manifest, the
            name an older record held, else an empty string.
        """
        if (found := self._from_manifest()) is not None:
            return found.name
        return self.legacy.name.removesuffix(LEGACY_TRANSFORMED_NAME_SUFFIX)

    @property
    def start_at(self) -> datetime:
        """When the run was last scheduled.

        Returns
        -------
        datetime
            The start of the manifest's latest attempt (the manifest's creation
            when it has none), always aware; for a run without a readable
            manifest, the start an older record held, else `recorded_at`.
        """
        if (found := self._from_manifest()) is not None:
            return found.start_at
        if self.legacy.start_at is not None:
            return self.legacy.start_at
        return self.recorded_at

    @property
    def trx_workplan_path(self) -> Path:
        """The transformed workplan in the run directory.

        The scheduler writes it next to the manifest as `<stem>_transformed.yaml`,
        the stem being the authored workplan's. A run directory of an earlier
        release holds it under that release's naming (`.yml` for a `.yml`
        workplan). A record of an older release holds the path it was found at,
        which answers when the directory holds none, or more than one and the
        manifest names none of them.

        Returns
        -------
        Path

        Raises
        ------
        CstarError
            If the run directory holds no transformed workplan, or more than one
            and nothing says which.
        """
        root = self.output_path
        found = self._from_manifest()
        authored = found.workplan_path if found is not None else None
        try:
            plan = find_transformed_workplan(root, authored)
        except CstarError:
            if self.legacy.trx_workplan_path is not None:
                return self.legacy.trx_workplan_path
            raise
        except OSError as ex:
            log.debug(f"Unable to list the run directory {root}: {ex}")
            plan = None
        if plan is not None:
            return plan
        if self.legacy.trx_workplan_path is not None:
            return self.legacy.trx_workplan_path
        msg = f"Run {self.run_id!r} at {root} holds no transformed workplan"
        raise CstarError(msg)

    def try_trx_workplan_path(self) -> Path | None:
        """The transformed workplan in the run directory, or `None` when it cannot be located.

        For the callers that treat a run without one as a run whose workplan
        does not load: the reason is logged at debug.

        Returns
        -------
        Path | None
        """
        try:
            return self.trx_workplan_path
        except CstarError as ex:
            log.debug(str(ex))
            return None

    @property
    def workplan_path(self) -> Path:
        """The authored workplan the run was scheduled from, as given at the time.

        Returns
        -------
        Path
            The manifest's `workplan.path`; for a run without a readable
            manifest, the path an older record held, else the transformed
            workplan.

        Raises
        ------
        CstarError
            If nothing is known and the run has no transformed workplan either.
        """
        found = self._from_manifest()
        if found is not None and found.workplan_path is not None:
            return found.workplan_path
        if self.legacy.workplan_path is not None:
            return self.legacy.workplan_path
        return self.trx_workplan_path

    @property
    def uuid(self) -> str:
        """The uuid of the run, as its manifest names it.

        The uuid cached in the metadata when the run was scheduled, else the
        `id` of the manifest at `output_path`, read on every access.

        Returns
        -------
        str
            The uuid, or an empty string when the record does not cache it and
            the run has no readable manifest.
        """
        if cached := self.metadata.get(KEY_RUN_UUID, ""):
            return cached
        try:
            return self.manifest.id
        except CstarError as ex:
            log.debug(f"Unable to read the manifest of run {self.run_id!r}: {ex}")
            return ""

    @property
    def live(self) -> bool:
        """Whether the run's sentinels and launcher are on this machine.

        Returns
        -------
        bool
            `False` for an attached run, whose status is what its records say.
        """
        return not self.attached

    @property
    def manifest(self) -> RunManifest:
        """The run's manifest, read from the run directory.

        Read on every access for a run scheduled here, whose manifest changes
        as the run goes on. An attached run's manifest is never refreshed by
        this machine, so it is parsed once and kept.

        Returns
        -------
        RunManifest

        Raises
        ------
        CstarError
            If the run directory holds no manifest, or one that cannot be read.
        """
        if self._manifest_cache is not None:
            return self._manifest_cache
        try:
            manifest = read_run_manifest(self.output_path, tolerant=True)
        except DOCUMENT_READ_ERRORS as ex:
            msg = (
                f"Unable to read the manifest of run {self.run_id!r} at "
                f"{self.output_path}: {ex}"
            )
            raise CstarError(msg) from ex
        if manifest is None:
            msg = f"Run {self.run_id!r} has no run manifest at {self.output_path}"
            raise CstarError(msg)
        if self.attached:
            self._manifest_cache = manifest
        return manifest

    def step_status(self, name: str) -> str:
        """The status of a step: its sentinel's, or for an attached run its record's.

        A run scheduled on this machine holds a sentinel per step. An attached
        run has none, so its step records are read, as for a run known only by
        its manifest.

        Parameters
        ----------
        name : str
            The name of the step.

        Returns
        -------
        str
            The orchestration status name, or an empty string when the step
            has no readable sentinel (no readable status, for an attached run).
        """
        if self.attached:
            try:
                return recorded_step_status(self.output_path, self.manifest, name)
            except CstarError as ex:
                log.debug(f"No recorded status for step {name!r}: {ex}")
                return ""

        try:
            path = StateRepository.sentinel_path(name, run_id=self.run_id)
        except ValueError as ex:
            log.debug(f"No sentinel path for step {name!r}: {ex}")
            return ""
        handle = try_deserialize(path, ProcessHandle)
        return handle.status.name if handle else ""

    @staticmethod
    def get_default_run_id(uri: str) -> str:
        """Generate a run-id based on the name of a `Workplan`

        Parameters
        ----------
        uri : str
            The local or remote path to a persisted workplan, authored or
            transformed.

        Returns
        -------
        str
        """
        with local_copy(uri) as local_path:
            wp = read_workplan(local_path)
            if isinstance(wp, LiveWorkplan):
                # a transformed workplan belongs to the run whose directory holds
                # it, which may have been started under an explicit run-id
                run_dir = local_path.resolve().parent
                if (
                    run_dir.name
                    and StateDirectoryManager.data_dir(run_dir.name).resolve()
                    == run_dir
                ):
                    return run_dir.name

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


def require_writable(run: WorkplanRun, *, action: str) -> None:
    """Refuse a write into the directory of a run recorded as read-only.

    The one chokepoint every verb that writes into a tracked run's directory
    calls before it writes anything.

    Parameters
    ----------
    run : WorkplanRun
        The tracked run.
    action : str
        What the verb would do, for the message: a verb or flag name.

    Raises
    ------
    CstarError
        If the run is recorded as read-only.
    """
    if run.access is Access.read_only:
        msg = (
            f"Run {run.run_id!r} is attached read-only: {action} writes into "
            f"{run.output_path}. `cstar run attach {run.output_path} --own` takes "
            "ownership of it."
        )
        raise CstarError(msg)


def require_relaunchable(run: WorkplanRun) -> None:
    """Refuse to relaunch a run that was attached from elsewhere.

    The one chokepoint every route that launches a run calls: an attached run
    has no sentinels on this machine, and relaunching it would displace the
    tracking of the run it stands for.

    Parameters
    ----------
    run : WorkplanRun
        The tracked run, as its latest record has it.

    Raises
    ------
    CstarExpectationFailed
        If the run was attached from elsewhere.
    """
    if run.attached:
        msg = (
            f"Run {run.run_id!r} was attached from {run.output_path}: attached runs "
            "can be inspected and referenced, and relaunching after a move is not "
            "supported. Start a new run with `--run-id <new>` that references it "
            "(see 'Extending a finished run' in the workplan docs)."
        )
        raise CstarExpectationFailed(msg)


def read_run_record(path: Path, *, tolerant: bool = False) -> WorkplanRun:
    """Read a run record, refusing one this build cannot read.

    Parameters
    ----------
    path : Path
        The record file.
    tolerant : bool
        Read a record newer than this build, within its major, with the keys
        this build does not know ignored; for readers that never write it.

    Returns
    -------
    WorkplanRun

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    CstarSchemaTooNewError
        If the record is newer than this build reads (see `VersionedDocument`);
        the message names the record.
    ValueError
        If the file is not a valid run record.
    """
    raw = read_document(path)
    if not isinstance(raw, Mapping):
        msg = f"The run record at {path} is not a mapping"
        raise ValueError(msg)
    try:
        run = WorkplanRun.model_validate(raw, context={TOLERATE_UNKNOWN: tolerant})
    except CstarSchemaTooNewError as ex:
        label = f"{WorkplanRun.SCHEMA_LABEL} {path}"
        raise CstarSchemaTooNewError(
            label, ex.found, ex.target, ex.unknown, ex.invalid
        ) from ex
    warn_unknown_keys(run, path)
    return run


def _try_read_run(path: Path, refused: set[Path]) -> WorkplanRun | None:
    """Read a run record, following the run if it has moved; `None` if unreadable.

    A record of a newer major, or of a newer minor that does not validate, is
    reported with one warning that names it and the upgrade, and its path is
    added to `refused`; one that only holds keys this build does not know loads,
    with a warning.
    """
    try:
        run = read_run_record(path, tolerant=True)
    except CstarSchemaTooNewError as ex:
        log.warning(str(ex))
        refused.add(path)
        return None
    except Exception:
        log.debug(f"Unable to read the run record at {path}", exc_info=True)
        return None
    return run.reanchored()


@contextmanager
def _replacing(path: Path) -> Generator[Path]:
    """Yield a sibling of `path` to build, then rename it onto `path` atomically.

    The caller holds the run lock, so one fixed staging name per path suffices;
    a leftover from a crashed writer is cleared first. The staged name is removed
    if building or renaming it fails.

    Parameters
    ----------
    path : Path
        The file or link to replace.
    """
    staged = path.with_name(f".{path.name}.tmp")
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.unlink(missing_ok=True)
    try:
        yield staged
        os.replace(staged, path)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise


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

    _EXT: t.Final[str] = "yaml"
    """The file extension of tracking records."""

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
        return f"{formatted_dt}.{TrackingRepository._EXT}"

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
        return f"{run_id}.{TrackingRepository._EXT}"

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
            all_runs = [
                path
                for path in (search_dir.iterdir() if search_dir.exists() else ())
                # a dotted name is a record a crashed writer left half-staged
                if not path.name.startswith(".")
            ]

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
        self,
        run_id: str,
        run_date: datetime | None = None,
        *,
        tolerant: bool = False,
    ) -> WorkplanRun | None:
        """Locate a WorkplanRun record.

        Parameters
        ----------
        run_id : str
            The run_id of the WorkplanRun
        run_date : datetime | None
            The datetime the run was executed or `None`.
        tolerant : bool
            Passed to `read_run_record`: for callers that never act on the run
            or write the record.

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

        lock_path = self._get_lock_path(run_id)

        with lock_path.open("w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)

            run_path = self._find_run_path(run_id, run_date)
            if not run_path.exists():
                rd_out = run_date or "latest"
                msg = f"No run file for `{run_id}` on `{rd_out}` found in {run_path}`"
                self.log.debug(msg)
                return None

            return read_run_record(run_path, tolerant=tolerant).reanchored()

    async def get_workplan_run(
        self,
        run_id: str,
        run_date: datetime | None = None,
        *,
        tolerant: bool = False,
    ) -> WorkplanRun | None:
        """Locate a WorkplanRun record.

        Parameters
        ----------
        run_id : str
            The run_id of the WorkplanRun
        run_date : datetime | None
            The datetime the run was executed or `None`.
        tolerant : bool
            Passed to `get_workplan_run_sync`.

        Returns
        -------
        WorkplanRun | None
            The record when it can be located in history or latest runs, otherwise `None`.
        """
        if self._sem:
            async with self._sem:
                return await asyncio.to_thread(
                    self.get_workplan_run_sync, run_id, run_date, tolerant=tolerant
                )
        return await asyncio.to_thread(
            self.get_workplan_run_sync, run_id, run_date, tolerant=tolerant
        )

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
        run_path = self.history_path(run.run_id, run.recorded_at)
        latest_path = self.latest_path(run.run_id)

        # Readers that take no lock (the listings) must see the old record or
        # the new one, never a missing or half-written one: each is built under
        # a dotted name the listing globs skip, then renamed into place.
        with _replacing(run_path) as staged_record:
            if not serialize(staged_record, run):
                self.log.warning("Run could not be persisted")

        with _replacing(latest_path) as staged_link:
            symlink_relative(staged_link, run_path)

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

        lock_path = self._get_lock_path(run_id)

        with lock_path.open("w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)

            run_path = self._find_run_path(run_id, None)
            if not run_path.exists():
                self.log.warning(f"No run file for `{run_id}` found in {run_path}`")
                return None

            run = read_run_record(run_path).reanchored()
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
        refused: set[Path] = set()
        results = await read_all(
            run_paths,
            lambda path: _try_read_run(path, refused),
            MAX_CONCURRENCY,
            sem=self._sem,
        )

        runs: list[WorkplanRun] = []
        unreadable: list[Path] = []
        for path, run in zip(run_paths, results, strict=True):
            if run is not None:
                runs.append(run)
            elif path not in refused:
                unreadable.append(path)

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
        run_paths = list(self.latest_dir.glob(f"{run_id_filter}*.{self._EXT}"))
        return await self._deserialize_readable_runs(run_paths)

    def list_latest_runs_sync(self) -> Sequence[WorkplanRun]:
        """Retrieve the latest `WorkplanRun` of every run-id, for a synchronous caller.

        The same records as `list_latest_runs`, read one after another: for a
        caller that may run inside an event loop and cannot start another.

        Returns
        -------
        Sequence[WorkplanRun]
            The latest run record per run-id; a record that cannot be read is
            skipped.
        """
        paths = self.latest_dir.glob(f"*.{self._EXT}")
        refused: set[Path] = set()
        runs = [(path, _try_read_run(path, refused)) for path in paths]
        if unreadable := [
            str(path) for path, run in runs if run is None and path not in refused
        ]:
            self.log.debug(f"Skipped unreadable run record(s): {', '.join(unreadable)}")
        return [run for _, run in runs if run is not None]

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
        glob_pattern = f"{run_id_filter}*/??????????????.??????.{self._EXT}"
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
