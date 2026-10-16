import os
import re
import typing as t
import warnings
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

from pydantic import (
    BaseModel,
    PrivateAttr,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cstar.applications.core import Transform
from cstar.applications.roms_marbl.file_system import RomsFileSystemManager
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.base.exceptions import CstarError
from cstar.base.feature import (
    ENV_FF_ORCH_TRX_TIMESPLIT,
    ENV_FF_ORCH_TRX_TIMESPLIT_LONGNAME,
    is_feature_enabled,
)
from cstar.base.log import get_logger
from cstar.base.utils import (
    DEFAULT_OUTPUT_ROOT_NAME,
    coerce_datetime,
    deep_merge,
    min_padded_index,
    slugify,
)
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import serialize
from cstar.orchestration.transforms import (
    ApplyOverridesDirective,
    DirectiveConfig,
    OverrideDirective,
    SplitFrequency,
    get_time_slices,
    lookup_step,
)
from cstar.orchestration.utils import ENV_CSTAR_ORCH_TRX_FREQ

if t.TYPE_CHECKING:
    from cstar.base.log import TraceLogger
    from cstar.orchestration.orchestration import LiveWorkplan

log = get_logger(__name__)


class RomsMarblTimeSplitter(Transform[LiveStep]):
    """A step tranformation that splits a ROMS-MARBL simulation into
    multiple sub-steps based on the timespan covered by the simulation.
    """

    frequency: str
    """The step splitting frequency used to generate new time steps."""

    def __init__(self, frequency: str = SplitFrequency.Monthly.value) -> None:
        """Initialize the transform instance."""
        freq_config = os.getenv(ENV_CSTAR_ORCH_TRX_FREQ, frequency)
        self.frequency = freq_config.lower()

    @classmethod
    def is_active(cls) -> bool:
        """Return `True` when time splitting is enabled via feature flag.

        Returns
        -------
        bool
        """
        return is_feature_enabled(ENV_FF_ORCH_TRX_TIMESPLIT)

    def get_subtask_name(
        self,
        i: int,
        n: int,
        sd: datetime,
        ed: datetime,
        name: str,
    ) -> str:
        """Generate an appropriate subtask name given subtask-specific metadata.

        Returns
        -------
        str
        """
        padded_idx = min_padded_index(i, n)
        dynamic_name = f"{self.suffix()}{padded_idx}"

        if is_feature_enabled(ENV_FF_ORCH_TRX_TIMESPLIT_LONGNAME):
            compact_fmt = "%Y%m%d%H%M"
            compact_sd = sd.strftime(compact_fmt)
            compact_ed = ed.strftime(compact_fmt)
            dynamic_name = f"{padded_idx}_{name}_{compact_sd}_{compact_ed}"

        return slugify(dynamic_name)

    def __call__(self, step: LiveStep) -> Sequence[LiveStep]:
        """Split a step into multiple sub-steps.

        Parameters
        ----------
        step : Step
            The step to split.

        Returns
        -------
        Sequence[LiveStep]
            Steps for each subtask resulting from the split.
        """
        blueprint = t.cast("RomsMarblBlueprint", step.blueprint)
        start_date = blueprint.runtime_params.start_date
        end_date = blueprint.runtime_params.end_date

        bp_path = step.fsm.run_dir / Path(step.blueprint_path).name
        serialize(bp_path, blueprint)

        time_slices = list(get_time_slices(start_date, end_date, self.frequency))
        n_slices = len(time_slices)

        if end_date <= start_date:
            msg = "end_date must be after start_date"
            raise ValueError(msg)

        depends_on = step.depends_on
        last_restart_file: RestartFile | None = None
        output_root_name = DEFAULT_OUTPUT_ROOT_NAME

        results: list[LiveStep] = []
        for i, (sd, ed) in enumerate(time_slices):
            bp_copy = RomsMarblBlueprint(
                **blueprint.model_dump(
                    exclude_unset=True,
                    exclude_defaults=True,
                    exclude_computed_fields=True,
                    exclude={"$schema"},
                ),
            )

            child_step_name = self.get_subtask_name(i, n_slices, sd, ed, step.safe_name)

            child_fs = step.fsm.get_subtask_manager(child_step_name)

            description = f"Subtask {i + 1} of {n_slices}; Timespan: {sd} to {ed}; {bp_copy.description}"
            overrides: dict[str, t.Any] = {
                "name": child_step_name,
                "description": description,
                "runtime_params": {
                    "start_date": sd,
                    "end_date": ed,
                },
                "working_dir": child_fs.root_dir.as_posix(),
            }

            if last_restart_file:
                overrides = deep_merge(
                    overrides,
                    RestartFileTrxAdapter.adapt(last_restart_file),
                )

            child_bp_path = child_fs.run_dir / f"{child_step_name}_bp.yaml"
            serialize(child_bp_path, bp_copy)

            updates: dict[str, t.Any] = {
                "blueprint": child_bp_path.as_posix(),
                "blueprint_overrides": overrides,
                "depends_on": depends_on,
                "name": child_step_name,
                "parent": step,
            }
            child_step = LiveStep.from_step(step, update=updates)
            results.append(child_step)

            if i == len(time_slices) - 1:
                break

            # use dependency on the prior substep to chain all the dynamic steps
            depends_on = [child_step.name]

            # post_run always leaves a whole restart file in `output`, so the
            # predicted name for the follow-up step's initial conditions never
            # carries a partition segment.
            restart_file = RestartFile.from_parts(
                output_root_name, ed, None, child_fs.output_dir
            )

            # use output dir of the last step as the input for the next step
            last_restart_file = restart_file

        return tuple(results)

    @staticmethod
    def suffix() -> str:
        """Return the standard prefix to be used when persisting
        a resource modified by this transform.
        """
        return "split"


class RestartFile(BaseModel):
    """Reference to a path that contains restart checkpoints."""

    path: Path
    """The path to a restart file."""
    _base: str = PrivateAttr()
    """The base name of the file."""
    _segment: str | None = PrivateAttr(default=None)
    """The segment identifier of the file."""
    _ts: datetime = PrivateAttr()
    """The timestamp parsed from the file name."""

    EXT: t.ClassVar[t.Literal["nc"]] = "nc"
    """The expected file extension for a restart file."""
    FMT_TS: t.ClassVar[t.Literal["%Y%m%d%H%M%S"]] = "%Y%m%d%H%M%S"
    """The expected timestamp format in the restart file name"""
    TS_GLOB: t.ClassVar[str] = "[0-9]" * 14
    """Glob fragment matching the fixed-width (14-digit) timestamp (see `FMT_TS`)."""
    PATTERN_RST: t.ClassVar[t.Literal[r"^(.*?)_rst\.(\d{14})(?:\.(\d{1,9}))?\.nc$"]] = (
        r"^(.*?)_rst\.(\d{14})(?:\.(\d{1,9}))?\.nc$"
    )
    """A regex identifying full restart or partitioned files."""
    SUFFIX: t.ClassVar[t.Literal["_rst"]] = "_rst"
    """A unique suffix found in the name of restart files"""

    @classmethod
    def candidates(cls, search_path: Path, *, partitioned: bool) -> list["RestartFile"]:
        """Enumerate the restart files under a directory.

        Parameters
        ----------
        search_path : Path
            The directory to search recursively.
        partitioned : bool
            If True, match partition pieces (`*_rst.<ts>.<part>.nc`); otherwise
            match whole files (`*_rst.<ts>.nc`).

        Returns
        -------
        list[RestartFile]
            Every matching file, in filesystem order.
        """
        parted_clause = ".*" if partitioned else ""
        glob_pattern = f"*{cls.SUFFIX}.{cls.TS_GLOB}{parted_clause}.{cls.EXT}"
        return [
            RestartFile(path=match)
            for match in search_path.rglob(glob_pattern)
            if re.fullmatch(cls.PATTERN_RST, match.name, flags=re.ASCII)
        ]

    @classmethod
    def _first_piece_at(
        cls, rst_files: Sequence["RestartFile"], timestamp: datetime
    ) -> "RestartFile | None":
        """Select the restart file dated exactly `timestamp`.

        Parameters
        ----------
        rst_files : Sequence[RestartFile]
            The restart files to choose from.
        timestamp : datetime
            The timestamp the file name must carry.

        Returns
        -------
        RestartFile | None
            The match with the lowest partition (a whole file counts as the
            0th), or `None` if no file is dated `timestamp`.
        """
        return min(
            (rst for rst in rst_files if rst.timestamp == timestamp),
            key=lambda rst: rst.partition or 0,
            default=None,
        )

    @classmethod
    def find(cls, search_path: Path, notfound_ok: bool = True) -> "RestartFile | None":
        """Search for a restart file in the specified location.

        If `search_path` identifies a directory, the item matching the _latest_
        timestamp and the 0th partition piece (if partitioned) is returned.

        If `search_path` identifies a file, that `RestartFile` will be returned.


        Parameters
        ----------
        search_path : Path
            The path to search
        notfound_ok : bool
            If False, raise an exception if no restart files are found.

        Returns
        -------
        ResetFile

        Raises
        ------
        ValueError
            If no directory or file exists at the search path.
        FileNotFoundError
            If no recognizable restart files are found.
        """
        search_path = search_path.expanduser().resolve()

        if search_path.is_file():
            return RestartFile(path=search_path)

        if not search_path.exists():
            msg = f"No directory or file found at path: {search_path!r}"
            raise ValueError(msg)

        for partitioned in (True, False):
            rst_files = cls.candidates(search_path, partitioned=partitioned)
            if rst_files:
                return cls._first_piece_at(
                    rst_files, max(rst.timestamp for rst in rst_files)
                )

        if not notfound_ok:
            msg = f"No restart files located. Unable to continue from {search_path!r}"
            raise FileNotFoundError(msg)

        return None

    @classmethod
    def find_at(cls, search_path: Path, timestamp: datetime) -> "RestartFile":
        """Search for the restart file dated exactly `timestamp`.

        If `search_path` identifies a directory, the item whose name carries
        `timestamp` and the 0th partition piece (if partitioned) is returned;
        as in `find`, partitioned files are preferred over whole files.

        If `search_path` identifies a file, that `RestartFile` will be returned
        provided its name carries `timestamp`.

        Parameters
        ----------
        search_path : Path
            The path to search
        timestamp : datetime
            The timestamp the restart file name must carry; matched exactly.

        Returns
        -------
        RestartFile

        Raises
        ------
        ValueError
            If no directory or file exists at the search path.
        FileNotFoundError
            If no restart file is dated `timestamp`; the message lists the
            timestamps that are present.
        """
        search_path = search_path.expanduser().resolve()

        if search_path.is_file():
            groups = [[RestartFile(path=search_path)]]
        elif search_path.exists():
            groups = [
                cls.candidates(search_path, partitioned=partitioned)
                for partitioned in (True, False)
            ]
        else:
            msg = f"No directory or file found at path: {search_path!r}"
            raise ValueError(msg)

        for rst_files in groups:
            if match := cls._first_piece_at(rst_files, timestamp):
                return match

        found = sorted({rst.timestamp for rst_files in groups for rst in rst_files})
        listing = ", ".join(str(ts) for ts in found) or "none"
        msg = (
            f"No restart file dated {timestamp} located in {search_path!r}; "
            f"restart timestamps found there: {listing}"
        )
        raise FileNotFoundError(msg)

    @classmethod
    def from_parts(
        cls,
        base: str,
        timestamp: datetime,
        segment: str | None = None,
        directory: Path | None = None,
    ) -> "RestartFile":
        """Create a ResetFile from components.

        Parameters
        ----------
        base : str
            The base name for the restart file.
        timestamp : datetime
            The timestamp for the restart file.
        segment : str | None
            The 0-padded segment number if partitioned, otherwise `None`.
        directory : Path | None
            The directory to contain the file. If not specified, defaults to cwd.

        Returns
        -------
        ResetFile

        Raises
        ------
        ValueError
            If the search path does not exist or contains no recognizable restart files.
        """
        ts = timestamp.strftime(cls.FMT_TS)
        parted_clause = f".{segment}" if segment is not None else ""
        filename = f"{base}{cls.SUFFIX}.{ts}{parted_clause}.{cls.EXT}"

        if directory:
            return RestartFile(path=directory / filename)

        return RestartFile(path=Path(filename))

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: Path, _info: "ValidationInfo") -> Path:
        """Verify the supplied path meets the restart file naming convention.

        Parameters
        ----------
        value : str
            The value of the checkpoint frequency property
        _info : ValidationInfo
            Metadata for the current validation context
        """
        if value.suffix != f".{RestartFile.EXT}":
            msg = f"File extension does not match expected naming convention: {value.suffix}"
            raise ValueError(msg)

        if re.fullmatch(RestartFile.PATTERN_RST, value.name, flags=re.ASCII):
            return value.expanduser().resolve()

        msg = f"File name does not match expected naming convention: {value}"
        raise ValueError(msg)

    @model_validator(mode="after")
    def _model_validate(self) -> "RestartFile":
        """Perform post-processing on the restart file path.

        Returns
        -------
        ResetFile
        """
        matches = re.fullmatch(
            RestartFile.PATTERN_RST, self.path.as_posix(), flags=re.ASCII
        )
        if not matches:
            msg = f"File name does not match expected naming convention: {self.path}"
            raise ValueError(msg)

        self._base = matches.group(1)
        self._ts = datetime.strptime(matches.group(2), RestartFile.FMT_TS)
        # look for segment for partition number, e.g. <base>.<ts>.000.nc vs. <base>.<ts>.nc
        self._segment = matches.group(3)
        return self

    @property
    def timestamp(self) -> datetime:
        """Return a datetime derived from the timestamp in the restart file name.

        Returns
        -------
        datetime
        """
        return self._ts

    @property
    def is_partitioned(self) -> bool:
        """Return `True` if the restart file belongs to a partitioned dataset.

        Returns
        -------
        datetime
        """
        return self._segment is not None

    @property
    def partition(self) -> int | None:
        if self._segment:
            return int(self._segment)
        return None

    @property
    def formatted_timestamp(self) -> str:
        return self._ts.strftime(self.FMT_TS)


def restart_timestamp(location: str | Path) -> datetime | None:
    """Return the timestamp encoded in a restart-style file name, or `None`.

    Only the file name is inspected (via `RestartFile`'s validators); the file
    need not exist.

    Parameters
    ----------
    location : str | Path
        A path or file name to inspect.

    Returns
    -------
    datetime | None
        The timestamp encoded in the file name, or `None` if `location` does
        not match the restart file naming convention.
    """
    try:
        return RestartFile(path=Path(location)).timestamp
    except ValueError:
        return None


def warn_on_restart_start_date_mismatch(
    location: str | Path, start_date: datetime, *, log: "TraceLogger"
) -> None:
    """Warn when a restart-style initial-conditions file is dated differently
    from `start_date`.

    ROMS takes its clock from the restart file while `ntimes` is computed from
    `start_date`, so a mismatch means the run will not cover the intended
    period.

    Parameters
    ----------
    location : str | Path
        The path or file name of the initial-conditions file.
    start_date : datetime
        The blueprint's configured start date.
    log : TraceLogger
        The logger to emit the warning to.
    """
    ts = restart_timestamp(location)
    if ts is not None and ts != start_date:
        log.warning(
            "Restart file %s is dated %s but start_date is %s. ROMS starts "
            "from the restart's time; the previous segment may not have "
            "reached its end date, or the restart directory / start_date is "
            "wrong.",
            Path(location).name,
            ts,
            start_date,
        )


class RestartFileTrxAdapter:
    """Convert a restart file into a dictionary useful for use in an OverrideTransform."""

    @classmethod
    def adapt(cls, rst_file: RestartFile | None) -> dict[str, t.Any]:
        """Given a restart file, create a dictionary containing the overrides necessary to
        execute a simulation with the restart file specified in the initial conditions.

        Parameters
        ----------
        restart_file : ResetFile
            The restart file metadata used to convert into an override mapping.

        Returns
        -------
        Mapping[str, t.Any]
        """
        if rst_file is None:
            return {}

        return {
            "runtime_params": {
                "start_date": rst_file.timestamp,
            },
            "initial_conditions": {
                "data": [
                    {
                        "location": rst_file.path.as_posix(),
                        "partitioned": rst_file.is_partitioned,
                    },
                ],
            },
        }


class TimestampedOutputFile(BaseModel):
    """Reference to a ROMS output file named
    `<base><SUFFIX>.<timestamp>[.<segment>].nc`.

    Subclasses set `SUFFIX` (and `LABEL`) and inherit the naming validation,
    the timestamp and partition accessors, and the directory search. A
    segment in the name marks one piece of a partitioned file.
    """

    path: Path
    """The path to an output file."""
    _base: str = PrivateAttr()
    """The base name of the file."""
    _segment: str | None = PrivateAttr(default=None)
    """The segment identifier of the file."""
    _ts: datetime = PrivateAttr()
    """The timestamp parsed from the file name."""

    EXT: t.ClassVar[t.Literal["nc"]] = "nc"
    """The expected file extension for an output file."""
    FMT_TS: t.ClassVar[t.Literal["%Y%m%d%H%M%S"]] = "%Y%m%d%H%M%S"
    """The expected timestamp format in the output file name"""
    SUFFIX: t.ClassVar[str]
    """A unique suffix found in the name of this kind of output file."""
    LABEL: t.ClassVar[str]
    """A human-readable name for this kind of output file, used in messages."""
    PATTERN: t.ClassVar[str]
    """A regex identifying full or partitioned files; derived from `SUFFIX`."""

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: t.Any) -> None:
        """Derive the file name pattern from the subclass's `SUFFIX`."""
        super().__pydantic_init_subclass__(**kwargs)
        cls.PATTERN = (
            rf"^(.*?){re.escape(cls.SUFFIX)}\.(\d{{14}})(?:\.(\d{{1,9}}))?\.{cls.EXT}$"
        )

    @classmethod
    def find(
        cls, search_path: Path, notfound_ok: bool = True
    ) -> Sequence[t.Self] | None:
        """Search for output files of this kind in the specified location.

        If `search_path` identifies a file, that file alone is returned.

        Parameters
        ----------
        search_path : Path
            The directory (searched recursively) or file to search
        notfound_ok : bool
            If False, raise an exception if no files are found.

        Returns
        -------
        Sequence[Self] | None

        Raises
        ------
        ValueError
            If the search path does not exist, or names a file that does not
            follow this kind of file's naming convention.
        FileNotFoundError
            If no recognizable files are found in the search path
        """
        search_path = search_path.expanduser().resolve()

        if search_path.is_file():
            return (cls(path=search_path),)

        if not search_path.exists():
            msg = f"No directory or file found at path: {search_path!r}"
            raise ValueError(msg)

        matches = sorted(search_path.rglob(f"*{cls.SUFFIX}*.{cls.EXT}"))
        if matches:
            return tuple(cls(path=m) for m in matches)

        if not notfound_ok:
            msg = (
                f"No {cls.LABEL} files located. Unable to continue from {search_path!r}"
            )
            raise FileNotFoundError(msg)

        return None

    @classmethod
    def from_parts(
        cls,
        base: str,
        timestamp: datetime,
        segment: str | None = None,
        directory: Path | None = None,
    ) -> t.Self:
        """Create an output file reference from components.

        Parameters
        ----------
        base : str
            The base name for the file.
        timestamp : datetime
            The timestamp for the file.
        segment : str | None
            The 0-padded segment number if partitioned, otherwise `None`.
        directory : Path | None
            The directory to contain the file. If not specified, defaults to cwd.

        Returns
        -------
        Self
        """
        ts = timestamp.strftime(cls.FMT_TS)
        parted_clause = f".{segment}" if segment is not None else ""
        filename = f"{base}{cls.SUFFIX}.{ts}{parted_clause}.{cls.EXT}"

        path = directory / filename if directory else Path(filename)
        return cls(path=path)

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: Path, _info: "ValidationInfo") -> Path:
        """Verify the supplied path meets this kind of file's naming convention.

        Parameters
        ----------
        value : str
            The value of the checkpoint frequency property
        _info : ValidationInfo
            Metadata for the current validation context
        """
        if value.suffix != f".{cls.EXT}":
            msg = f"File extension does not match expected naming convention: {value.suffix}"
            raise ValueError(msg)

        if re.fullmatch(cls.PATTERN, value.name, flags=re.ASCII):
            return value

        msg = f"File name does not match expected naming convention: {value}"
        raise ValueError(msg)

    @model_validator(mode="after")
    def _model_validate(self) -> t.Self:
        """Perform post-processing on the output file path.

        Returns
        -------
        Self
        """
        matches = re.fullmatch(type(self).PATTERN, self.path.as_posix(), flags=re.ASCII)
        if not matches:
            msg = f"File name does not match expected naming convention: {self.path}"
            raise ValueError(msg)

        self._base = matches.group(1)
        self._ts = datetime.strptime(matches.group(2), self.FMT_TS)
        # look for segment for partition number, e.g. <base>.<ts>.000.nc vs. <base>.<ts>.nc
        self._segment = matches.group(3)
        return self

    @property
    def timestamp(self) -> datetime:
        """Return a datetime derived from the timestamp in the file name.

        Returns
        -------
        datetime
        """
        return self._ts

    @property
    def is_partitioned(self) -> bool:
        """Return `True` if the file belongs to a partitioned dataset.

        Returns
        -------
        bool
        """
        return self._segment is not None

    @property
    def partition(self) -> int | None:
        if self._segment:
            return int(self._segment)
        return None


class BoundaryFile(TimestampedOutputFile):
    """Reference to a path that contains boundary forcing."""

    SUFFIX: t.ClassVar[str] = "_bry"
    """A unique suffix found in the name of boundary files"""
    LABEL: t.ClassVar[str] = "boundary"
    PATTERN_BRY: t.ClassVar[str]
    """A regex identifying full boundary or partitioned files (alias of `PATTERN`)."""


BoundaryFile.PATTERN_BRY = BoundaryFile.PATTERN


class CarbonateSensitivityFile(TimestampedOutputFile):
    """Reference to a path that contains carbonate sensitivities
    (`ddic_dco2`, `ddic_dalk`), as written by a ROMS-MARBL run that sets
    `cdr_gas_exch_output.do_cdr_gas_exch_output`.
    """

    SUFFIX: t.ClassVar[str] = "_cdrgas"
    """A unique suffix found in the name of carbonate sensitivity files"""
    LABEL: t.ClassVar[str] = "carbonate sensitivity"


def _forcing_data_overrides(
    key: str, files: Sequence[TimestampedOutputFile]
) -> dict[str, t.Any]:
    """Create the override that lists `files` as the blueprint's `forcing.<key>` dataset.

    Parameters
    ----------
    key : str
        The name of the forcing dataset to set.
    files : Sequence[TimestampedOutputFile]
        The files making up the dataset, in order.

    Returns
    -------
    dict[str, t.Any]
    """
    return {
        "forcing": {
            key: {
                "data": [
                    {
                        "location": file.path.as_posix(),
                        "partitioned": file.is_partitioned,
                    }
                    for file in files
                ],
            },
        },
    }


class BoundaryFileTrxAdapter:
    """Convert boundary files into a dictionary useful for use in an OverrideTransform."""

    @classmethod
    def adapt(cls, bry_files: Sequence[BoundaryFile]) -> dict[str, t.Any]:
        """Given boundary files, create a dictionary containing the overrides necessary to
        execute a simulation with those files as its boundary forcing.

        Parameters
        ----------
        bry_files : Sequence[BoundaryFile]
            The boundary files used to convert into an override mapping.

        Returns
        -------
        dict[str, t.Any]
        """
        return _forcing_data_overrides("boundary", bry_files)


class CarbonateSensitivityTrxAdapter:
    """Convert carbonate sensitivity files into a dictionary useful for use in an OverrideTransform."""

    @classmethod
    def adapt(cls, files: Sequence[CarbonateSensitivityFile]) -> dict[str, t.Any]:
        """Given carbonate sensitivity files, create a dictionary containing the
        overrides necessary to execute a simulation with those files as its
        carbonate sensitivity forcing.

        Parameters
        ----------
        files : Sequence[CarbonateSensitivityFile]
            The carbonate sensitivity files used to convert into an override mapping.

        Returns
        -------
        dict[str, t.Any]
        """
        return _forcing_data_overrides("carbonate_sensitivity", files)


_LEGACY_LAYOUT_HINT: t.Final[str] = (
    "Runs completed before this version kept joined files in a `joined_output` "
    "directory; run `cstar admin migrate-outputs <run dir>` to move them, or "
    "point `path:` at that directory."
)
"""Appended to not-found errors so users of old run directories know what to do."""


def _require_step_output_dir(workplan: "LiveWorkplan", name: str) -> Path:
    """Resolve a step's `output` directory and require it to exist.

    Parameters
    ----------
    workplan : LiveWorkplan
        The workplan containing the named step.
    name : str
        The name of the step whose output directory is requested.

    Returns
    -------
    Path

    Raises
    ------
    KeyError
        If `workplan` does not contain a step named `name`.
    CstarError
        If `name` is malformed or names a step of an external run that
        cannot be resolved.
    FileNotFoundError
        If the step has no `output` directory yet (it has not run, or failed
        before producing output).
    """
    search_path = resolve_step_output_dir(workplan, name)
    if not search_path.is_dir():
        msg = (
            f"Step {name!r} has no output directory at {search_path}; "
            "it may not have run yet."
        )
        raise FileNotFoundError(msg)
    return search_path


def _reject_partitioned_step_output(name: str, partitioned: bool, found: Path) -> None:
    """Refuse partition pieces found in a step's `output` directory.

    Under the current layout `output` only ever holds whole files, so a
    partition piece there means the step ran under the old layout (which left
    partitioned restarts in `output`) and must be migrated first.

    Parameters
    ----------
    name : str
        The referenced step's name.
    partitioned : bool
        Whether the located file is a partition piece.
    found : Path
        The located file, for the error message.

    Raises
    ------
    FileNotFoundError
        If `partitioned` is true.
    """
    if partitioned:
        msg = (
            f"Step {name!r} output holds only partitioned files ({found.name}), "
            f"not a whole file. {_LEGACY_LAYOUT_HINT}"
        )
        raise FileNotFoundError(msg)


def resolve_step_output_dir(workplan: "LiveWorkplan", name: str) -> Path:
    """Resolve the `output` directory of a named, completed workplan step.

    Every step type writes its final, whole files to `output` (ROMS-MARBL
    steps write partitioned pieces to `temp_output` first and join them
    into `output`; other apps, e.g. nest_ic, write straight to `output`), so a
    step's outputs are always found there regardless of how the step ran.

    Parameters
    ----------
    workplan : LiveWorkplan
        The workplan containing the named step.
    name : str
        The name of the step whose output directory is requested; a step of
        an external run is named `<step>@<alias>`.

    Returns
    -------
    Path
        The step's `output` directory.

    Raises
    ------
    KeyError
        If `workplan` does not contain a step named `name`.
    CstarError
        If `name` is malformed or names a step of an external run that
        cannot be resolved.
    """
    fsm = RomsFileSystemManager(lookup_step(workplan, name).fsm.root_dir)
    return fsm.output_dir


def _rst_path_continue_from_conflict_message(step_name: str) -> str:
    """Build the error message for a conflicting `rst_path` + `continue-from`.

    Shared between `NestingDirective.__call__` (runtime) and
    `NestingDirective.validate_directives` (schedule time); both are
    invoked with the step and so always know its name.

    Parameters
    ----------
    step_name : str
        The step's name.

    Returns
    -------
    str
    """
    return (
        "nest-from rst_path and continue-from both set initial conditions for "
        f"step {step_name!r}; remove rst_path"
    )


def _split_sources(value: t.Any, delimiter: str) -> list[str]:
    """Split a delimited scalar into an ordered list of trimmed, non-empty tokens.

    Parameters
    ----------
    value : t.Any
        The scalar value to split; coerced to `str` before splitting.
    delimiter : str
        The delimiter separating tokens.

    Returns
    -------
    list[str]
    """
    return [token for raw in str(value).split(delimiter) if (token := raw.strip())]


SOURCE_KEY_PATH: t.Final[str] = "path"
"""Directive config key naming a filesystem path as the content source."""
SOURCE_KEY_STEP: t.Final[str] = "step"
"""Directive config key naming another workplan step as the content source."""
SOURCE_DELIMITER: t.Final[str] = ";"
"""Delimiter separating multiple sources in a directive's `path`/`step` value."""

_OutputFileT = t.TypeVar("_OutputFileT", bound=TimestampedOutputFile)


def collect_output_files(
    file_cls: type[_OutputFileT],
    path_value: t.Any,
    step_names: Sequence[str],
    workplan: "LiveWorkplan | None",
) -> list[_OutputFileT]:
    """Gather the output files of one kind from a directive's sources.

    Every `SOURCE_DELIMITER`-separated token of `path_value` is searched as a
    path, and the `output` directory of every step in `step_names` is
    searched as a step source; every source must contain files of the kind.
    Step outputs only ever hold whole files, so a step source holding a
    partition piece is rejected.

    Parameters
    ----------
    file_cls : type[TimestampedOutputFile]
        The kind of file to collect.
    path_value : t.Any
        The directive's `path` value (several sources joined by
        `SOURCE_DELIMITER`), or a falsy value if it has none.
    step_names : Sequence[str]
        The names of the steps whose `output` directories are searched.
    workplan : LiveWorkplan | None
        The workplan the steps belong to; only required when `step_names` is
        not empty.

    Returns
    -------
    list[TimestampedOutputFile]
        The files of every source in the order given, without duplicates.

    Raises
    ------
    CstarError
        If `step_names` is not empty and no workplan was supplied.
    FileNotFoundError
        If a source contains no files of the kind, or a step has no `output`
        directory or holds only partitioned files.
    """
    sources: list[tuple[Path, str | None]] = []

    if path_value:
        sources.extend(
            (Path(token), None)
            for token in _split_sources(path_value, SOURCE_DELIMITER)
        )

    if step_names:
        if workplan is None:
            raise CstarError("Directive did not receive workplan")

        sources.extend(
            (_require_step_output_dir(workplan, name), name) for name in step_names
        )

    found_files: list[_OutputFileT] = []
    for search_path, step_name in sources:
        try:
            found = file_cls.find(search_path, notfound_ok=False) or ()
        except FileNotFoundError as err:
            raise FileNotFoundError(f"{err} {_LEGACY_LAYOUT_HINT}") from err

        if step_name is not None:
            partitioned = next((f for f in found if f.is_partitioned), None)
            if partitioned is not None:
                _reject_partitioned_step_output(step_name, True, partitioned.path)

        found_files.extend(found)

    deduped: dict[Path, _OutputFileT] = {}
    for file in found_files:
        deduped.setdefault(file.path, file)

    return list(deduped.values())


class ContinuanceDirective(OverrideDirective):
    """A transform that locates a restart file with an unknown path at the
    time the task was scheduled, and applies it as the step's initial
    conditions.

    By default the latest restart file in the source is used; the optional
    `timestamp` key selects the restart whose file name carries exactly that
    timestamp instead.

    Warns only when the step explicitly overrode `start_date` (via the
    workplan's `blueprint_overrides.runtime_params.start_date`, packaged by
    `package_runtime_overrides` into the runtime `apply-overrides` directive)
    and that explicit value disagrees with the restart this directive
    located. Chained steps that leave `start_date` to the directive (e.g.
    `continue-from: step: <prev>` with no explicit `start_date` override)
    stay quiet -- the base blueprint's `start_date` is unrelated to whatever
    restart gets discovered in that case.
    """

    REPLACE_LISTS = True

    KEY_PATH: t.Final[str] = SOURCE_KEY_PATH
    """Key used to specify a path as the source for continuance."""
    KEY_STEP: t.Final[str] = SOURCE_KEY_STEP
    """Key used to specify a step name as the source for continuance."""
    KEY_TIMESTAMP: t.Final[str] = "timestamp"
    """Optional key selecting the restart dated exactly this timestamp instead of the latest."""

    @classmethod
    def key(cls) -> str:
        return "continue-from"

    @classmethod
    def _parse_timestamp(cls, value: t.Any) -> datetime:
        """Parse a `timestamp` config value into the restart time it selects.

        YAML delivers the value as a `datetime`, `date`, `int` or `str`
        depending on how it was written; all are parsed from their string form.
        A date alone means midnight. Nothing else is completed or guessed.

        Parameters
        ----------
        value : t.Any
            The configured value: an ISO 8601 date or date-time, or the
            14-digit timestamp found in restart file names.

        Returns
        -------
        datetime
            The naive, whole-second time the value denotes.

        Raises
        ------
        ValueError
            If the value is not such a date or time, or carries a timezone or
            fractional seconds (restart file names have neither).
        """
        text = str(value).strip()
        bare_digits = text.isascii() and text.isdigit()
        msg = (
            f"{cls.KEY_TIMESTAMP!r} must be an ISO 8601 date or date-time (e.g. "
            "2012-02-01 00:00:00) or a 14-digit restart file timestamp (e.g. "
            f"20120201000000), got {text!r}"
        )

        # `fromisoformat` takes any one character as the date/time separator, so
        # it would read 13 digits as YYYYMMDD0HHMM; bare digits must be a date
        # (8) or a restart file timestamp (14).
        if bare_digits and len(text) not in (8, 14):
            raise ValueError(msg)

        try:
            if bare_digits and len(text) == 14:
                parsed = datetime.strptime(text, RestartFile.FMT_TS)
            else:
                parsed = datetime.fromisoformat(text)
        except ValueError:
            raise ValueError(msg) from None

        if parsed.tzinfo is not None:
            msg = (
                f"{cls.KEY_TIMESTAMP!r} must not carry a timezone (restart file "
                f"names do not), got {text!r}"
            )
            raise ValueError(msg)

        if parsed.microsecond:
            msg = (
                f"{cls.KEY_TIMESTAMP!r} must be a whole number of seconds (restart "
                f"file names have no fractions), got {text!r}"
            )
            raise ValueError(msg)

        return parsed

    @classmethod
    def _config_problems(cls, config: Mapping[str, t.Any]) -> list[str]:
        """Return config-shape problems in a `continue-from` directive config.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.

        Returns
        -------
        list[str]
        """
        found_keys = set(config.keys())
        source_keys = {cls.KEY_PATH, cls.KEY_STEP}
        supported_keys = source_keys | {cls.KEY_TIMESTAMP}
        problems: list[str] = []

        if (found_keys - supported_keys) or not found_keys.intersection(source_keys):
            if found_keys == {cls.KEY_TIMESTAMP}:
                problems.append(
                    "Invalid continuance transform configuration: "
                    f"{cls.KEY_TIMESTAMP!r} selects a restart from a source; also "
                    f"supply {cls.KEY_STEP!r} or {cls.KEY_PATH!r}."
                )
            else:
                problems.append(
                    "Invalid continuance transform configuration; supported "
                    f"configuration: {', '.join(sorted(supported_keys))}, provided "
                    f"configuration: {', '.join(sorted(found_keys))}"
                )
            return problems

        if source_keys.issubset(found_keys):
            problems.append(
                f"Invalid continuance transform configuration: {cls.KEY_PATH!r} and "
                f"{cls.KEY_STEP!r} are mutually exclusive; supply only one restart source."
            )

        if cls.KEY_TIMESTAMP in config:
            try:
                cls._parse_timestamp(config[cls.KEY_TIMESTAMP])
            except ValueError as err:
                problems.append(f"Invalid continuance transform configuration: {err}")

        return problems

    @classmethod
    def validate_directives(
        cls, config: Mapping[str, t.Any], step: LiveStep
    ) -> Sequence[str]:
        """Validate a `continue-from` directive's config at schedule time.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.
        step : LiveStep
            The step the directive is configured on.

        Returns
        -------
        Sequence[str]
        """
        return cls._config_problems(config)

    @classmethod
    def referenced_steps(cls, config: Mapping[str, t.Any]) -> Sequence[str]:
        """Return the step named by this directive's `step` config, if any.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.

        Returns
        -------
        Sequence[str]
        """
        if not (value := config.get(cls.KEY_STEP)):
            return ()
        return (str(value).strip(),)

    @t.override
    def __call__(self, step: LiveStep) -> Sequence[LiveStep]:
        """Warn on an explicit start_date/restart mismatch, then transform.

        Parameters
        ----------
        step : LiveStep
            The step to be transformed.

        Returns
        -------
        Sequence[LiveStep]
            Zero-to-many steps resulting from applying the transform.
        """
        data = self._system_overrides.get("initial_conditions", {}).get("data", [])
        location = data[0].get("location") if data else None

        explicit_start: t.Any = None
        config = step.directives.get(ApplyOverridesDirective.key(), {})
        if isinstance(config, Mapping):
            overrides = config.get(ApplyOverridesDirective.KEY_OVERRIDES)
            if isinstance(overrides, Mapping):
                runtime_params = overrides.get("runtime_params")
                if isinstance(runtime_params, Mapping):
                    explicit_start = runtime_params.get("start_date")

        if location and explicit_start is not None:
            warn_on_restart_start_date_mismatch(
                location, coerce_datetime(explicit_start), log=log
            )

        return super().__call__(step)

    def _generate_overrides(self) -> dict[str, t.Any]:
        """Create an overrides dictionary that will result in the modified blueprint.

        ContinuanceDirective creates overrides to modify the initial conditions
        using the output from another step or a fixed directory path.

        Returns
        -------
        dict[str, t.Any]

        Raises
        ------
        NotImplementedError
            If the supplied configuration is not supported.
        ValueError
            If a restart file cannot be located with the supplied configuration.
        FileNotFoundError
            If the source holds no restart files, or none is dated the
            requested `timestamp`.
        """
        if problems := self._config_problems(self._config):
            raise NotImplementedError("; ".join(problems))

        search_path: Path | None = None

        if target_path := self._config.get(self.KEY_PATH, None):
            search_path = Path(target_path)

        step_refs = self.referenced_steps(self._config)
        name = step_refs[0] if step_refs else None
        if name:
            search_path = _require_step_output_dir(self.workplan, name)

        if search_path:
            try:
                restart_file = RestartFile.find(search_path, notfound_ok=False)
            except FileNotFoundError as err:
                raise FileNotFoundError(f"{err} {_LEGACY_LAYOUT_HINT}") from err
            if self.KEY_TIMESTAMP in self._config:
                # `find` above raised with the legacy-layout hint if the source
                # holds no restarts; a timestamp miss instead lists what exists.
                restart_file = RestartFile.find_at(
                    search_path, self._parse_timestamp(self._config[self.KEY_TIMESTAMP])
                )
            if restart_file:
                if name:
                    _reject_partitioned_step_output(
                        name, restart_file.is_partitioned, restart_file.path
                    )
                return RestartFileTrxAdapter.adapt(restart_file)

        msg = f"No restart file located in search path: {search_path!r}"
        raise ValueError(msg)

    @t.override
    @staticmethod
    def suffix() -> str:
        """Return a suffix used when persisting a resource modified by this transform.

        Returns
        -------
        str
        """
        return "cfrom"


class NestingDirective(OverrideDirective):
    """A transform that supplies boundary forcing from a parent (or sibling)
    simulation for a nested child run.

    The boundary source is exactly one of `path` (a directory or file) or
    `step` (a step name resolved via the workplan), the same shape used by
    `ContinuanceDirective` for initial conditions -- the two directives now
    touch disjoint blueprint keys (`forcing.boundary` here vs.
    `initial_conditions` there) and compose in any order. `path`, `bry_path`,
    and `step` each accept several sources in one string, separated by `;`
    (surrounding whitespace ignored); boundary files from every listed source
    are combined, in the order given, and every listed source must contain
    boundary files.

    `bry_path` is a deprecated alias for `path` (conflicts with `path`/
    `step` if both are supplied). `rst_path` is a deprecated, optional key
    that restores the historical "nest-from also sets initial conditions"
    behavior by applying a restart-file override directly; it is rejected
    when a `continue-from` directive is also present on the step, since both
    would set `initial_conditions`. Both deprecated keys emit a
    `FutureWarning` and a log warning.
    """

    REPLACE_LISTS = True

    KEY_PATH: t.Final[str] = SOURCE_KEY_PATH
    """Key used to specify a path as the source for the boundary forcing."""
    KEY_STEP: t.Final[str] = SOURCE_KEY_STEP
    """Key used to specify a step name as the source for the boundary forcing."""
    KEY_BRY_PATH: t.Final[str] = "bry_path"
    """Deprecated alias for `KEY_PATH`."""
    KEY_RST_PATH: t.Final[str] = "rst_path"
    """Deprecated key that also applies a restart-file override."""
    SOURCE_DELIMITER: t.Final[str] = SOURCE_DELIMITER
    """Delimiter separating multiple sources in a `path`/`bry_path`/`step` value."""

    @classmethod
    def key(cls) -> str:
        return "nest-from"

    @classmethod
    def _config_problems(cls, config: Mapping[str, t.Any]) -> list[str]:
        """Return config-shape problems in a `nest-from` directive config.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.

        Returns
        -------
        list[str]
        """
        found_keys = set(config.keys())
        boundary_keys = {cls.KEY_PATH, cls.KEY_STEP, cls.KEY_BRY_PATH}
        supported_keys = boundary_keys | {cls.KEY_RST_PATH}
        problems: list[str] = []

        if cls.KEY_BRY_PATH in found_keys and found_keys.intersection(
            {cls.KEY_PATH, cls.KEY_STEP}
        ):
            problems.append(
                f"Invalid nesting transform configuration: {cls.KEY_BRY_PATH!r} "
                f"conflicts with {cls.KEY_PATH!r}/{cls.KEY_STEP!r}; supply "
                "only one boundary source."
            )

        if (found_keys - supported_keys) or not found_keys.intersection(boundary_keys):
            problems.append(
                "Invalid nesting transform configuration; supported configuration: "
                f"{', '.join(sorted(supported_keys))}, provided configuration: "
                f"{', '.join(found_keys)}"
            )
            return problems

        if {cls.KEY_PATH, cls.KEY_STEP}.issubset(found_keys):
            problems.append(
                f"Invalid nesting transform configuration: {cls.KEY_PATH!r} and "
                f"{cls.KEY_STEP!r} are mutually exclusive; supply only one boundary source."
            )

        target_value = config.get(cls.KEY_PATH) or config.get(cls.KEY_BRY_PATH)
        sources = (
            list(_split_sources(target_value, cls.SOURCE_DELIMITER))
            if target_value
            else []
        )
        sources.extend(cls.referenced_steps(config))

        if not sources:
            key = next(
                k
                for k in (cls.KEY_PATH, cls.KEY_BRY_PATH, cls.KEY_STEP)
                if k in found_keys
            )
            problems.append(
                f"Invalid nesting transform configuration: no boundary source "
                f"given in {key!r}"
            )

        return problems

    @classmethod
    def validate_directives(
        cls, config: Mapping[str, t.Any], step: LiveStep
    ) -> Sequence[str]:
        """Validate a `nest-from` directive's config at schedule time.

        In addition to the config-shape rules in `_config_problems`, rejects
        a `rst_path` config combined with a `continue-from` directive on the
        same step (both would set `initial_conditions`), a conflict that
        would otherwise only surface on the compute node.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.
        step : LiveStep
            The step the directive is configured on; `step.directives`
            carries its full directives mapping (this directive's key
            included).

        Returns
        -------
        Sequence[str]
        """
        problems = list(cls._config_problems(config))
        if cls.KEY_RST_PATH in config and ContinuanceDirective.key() in step.directives:
            problems.append(_rst_path_continue_from_conflict_message(step.name))
        return problems

    @classmethod
    def referenced_steps(cls, config: Mapping[str, t.Any]) -> Sequence[str]:
        """Return the steps named by this directive's `step` config, if any.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.

        Returns
        -------
        Sequence[str]
        """
        if not (value := config.get(cls.KEY_STEP)):
            return ()
        return _split_sources(value, cls.SOURCE_DELIMITER)

    @t.override
    def __call__(self, step: LiveStep) -> Sequence[LiveStep]:
        """Reject a conflicting `rst_path` + `continue-from` combination, then transform.

        Parameters
        ----------
        step : LiveStep
            The step to be transformed.

        Returns
        -------
        Sequence[LiveStep]
            Zero-to-many steps resulting from applying the transform.

        Raises
        ------
        ValueError
            If the deprecated `rst_path` key is set alongside a
            `continue-from` directive on the same step; both would set
            `initial_conditions`.
        """
        if (
            self.KEY_RST_PATH in self._config
            and ContinuanceDirective.key() in step.directives
        ):
            raise ValueError(_rst_path_continue_from_conflict_message(step.name))
        return super().__call__(step)

    def _generate_overrides(self) -> dict[str, t.Any]:
        """Create an overrides dictionary that will result in the modified blueprint.

        NestingDirective creates overrides that set the step's boundary
        forcing, and -- only when the deprecated `rst_path` key is present
        -- its initial conditions as well.

        Returns
        -------
        dict[str, t.Any]

        Raises
        ------
        NotImplementedError
            If the supplied configuration is not supported, or if the
            deprecated `bry_path` key conflicts with `path`/`step`, or the
            value is empty after splitting.
        ValueError
            If `rst_path` is supplied and no restart file can be located.
        FileNotFoundError
            If a listed source contains no boundary files, or a listed step
            has no `output` directory.
        """
        if self.KEY_BRY_PATH in self._config:
            msg = (
                f"{self.key()!r} config key {self.KEY_BRY_PATH!r} is deprecated "
                f"and will be removed in a future release; use {self.KEY_PATH!r} "
                "instead."
            )
            warnings.warn(msg, FutureWarning, stacklevel=2)
            log.warning(msg)

        if problems := self._config_problems(self._config):
            raise NotImplementedError("; ".join(problems))

        boundary_files = collect_output_files(
            BoundaryFile,
            self._config.get(self.KEY_PATH) or self._config.get(self.KEY_BRY_PATH),
            self.referenced_steps(self._config),
            self._workplan,
        )

        overrides = BoundaryFileTrxAdapter.adapt(boundary_files)

        if rst_path := self._config.get(self.KEY_RST_PATH):
            msg = (
                f"{self.key()!r} config key {self.KEY_RST_PATH!r} is deprecated "
                "and will be removed in a future release; use "
                f"{ContinuanceDirective.key()!r}: {{{ContinuanceDirective.KEY_PATH!r}: "
                f"{rst_path!r}}} instead."
            )
            warnings.warn(msg, FutureWarning, stacklevel=2)
            log.warning(msg)

            rst_search_path = Path(rst_path)
            if restart_file := RestartFile.find(rst_search_path, notfound_ok=False):
                overrides = {**overrides, **RestartFileTrxAdapter.adapt(restart_file)}
            else:
                msg = f"No restart file located in search path: {rst_search_path!r}"
                raise ValueError(msg)

        return overrides

    @t.override
    @staticmethod
    def suffix() -> str:
        """Return a suffix used when persisting a resource modified by this transform.

        Returns
        -------
        str
        """
        return "nfrom"


_CDR_GAS_OUTPUT_HINT: t.Final[str] = (
    "Carbonate sensitivities come from the `_cdrgas` files a ROMS-MARBL run "
    "writes only when MARBL is enabled and its namelist sets "
    "`do_cdr_gas_exch_output` to true (`cdr_gas_exch_output_settings` in a "
    "blueprint's `namelist_overrides`, `cdr_gas_exch_output` in a forge output spec)."
)
"""Appended to not-found errors so users know what the producing step must enable."""


class CarbonateSensitivityDirective(OverrideDirective):
    """A transform that supplies the carbonate sensitivities (`ddic_dco2`,
    `ddic_dalk`) a CDR-lite run reads as surface forcing, from the output of an
    earlier ROMS-MARBL run.

    The source is exactly one of `path` (a directory or file) or `step` (a step
    name resolved via the workplan), the same shape used by `NestingDirective`
    for boundary forcing; the directive sets `forcing.carbonate_sensitivity`
    and nothing else, so it composes with the other directives in any order.
    `path` and `step` each accept several sources in one string, separated by
    `;` (surrounding whitespace ignored); the `_cdrgas` files from every listed
    source are combined, in the order given, and every listed source must
    contain such files. The producing step must be a MARBL run whose namelist
    sets `do_cdr_gas_exch_output` for ROMS to write them.
    """

    REPLACE_LISTS = True

    KEY_PATH: t.Final[str] = SOURCE_KEY_PATH
    """Key used to specify a path as the source for the carbonate sensitivities."""
    KEY_STEP: t.Final[str] = SOURCE_KEY_STEP
    """Key used to specify a step name as the source for the carbonate sensitivities."""
    SOURCE_DELIMITER: t.Final[str] = SOURCE_DELIMITER
    """Delimiter separating multiple sources in a `path`/`step` value."""

    @classmethod
    def key(cls) -> str:
        return "carbonate-sensitivity-from"

    @classmethod
    def _config_problems(cls, config: Mapping[str, t.Any]) -> list[str]:
        """Return config-shape problems in a `carbonate-sensitivity-from` directive config.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.

        Returns
        -------
        list[str]
        """
        found_keys = set(config.keys())
        source_keys = {cls.KEY_PATH, cls.KEY_STEP}
        problems: list[str] = []

        if (found_keys - source_keys) or not found_keys.intersection(source_keys):
            problems.append(
                "Invalid carbonate sensitivity transform configuration; supported "
                f"configuration: {', '.join(sorted(source_keys))}, provided "
                f"configuration: {', '.join(sorted(found_keys))}"
            )
            return problems

        if source_keys.issubset(found_keys):
            problems.append(
                f"Invalid carbonate sensitivity transform configuration: {cls.KEY_PATH!r} "
                f"and {cls.KEY_STEP!r} are mutually exclusive; supply only one "
                "carbonate sensitivity source."
            )

        path_value = config.get(cls.KEY_PATH)
        sources = _split_sources(path_value, cls.SOURCE_DELIMITER) if path_value else []
        sources.extend(cls.referenced_steps(config))

        if not sources:
            key = next(k for k in (cls.KEY_PATH, cls.KEY_STEP) if k in found_keys)
            problems.append(
                "Invalid carbonate sensitivity transform configuration: no "
                f"carbonate sensitivity source given in {key!r}"
            )

        return problems

    @classmethod
    def validate_directives(
        cls, config: Mapping[str, t.Any], step: LiveStep
    ) -> Sequence[str]:
        """Validate a `carbonate-sensitivity-from` directive's config at schedule time.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.
        step : LiveStep
            The step the directive is configured on.

        Returns
        -------
        Sequence[str]
        """
        return cls._config_problems(config)

    @classmethod
    def referenced_steps(cls, config: Mapping[str, t.Any]) -> Sequence[str]:
        """Return the steps named by this directive's `step` config, if any.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.

        Returns
        -------
        Sequence[str]
        """
        if not (value := config.get(cls.KEY_STEP)):
            return ()
        return _split_sources(value, cls.SOURCE_DELIMITER)

    def _generate_overrides(self) -> dict[str, t.Any]:
        """Create an overrides dictionary that will result in the modified blueprint.

        CarbonateSensitivityDirective creates overrides that set the step's
        carbonate sensitivity forcing from the `_cdrgas` files of the
        configured sources.

        Returns
        -------
        dict[str, t.Any]

        Raises
        ------
        NotImplementedError
            If the supplied configuration is not supported, or the value is
            empty after splitting.
        FileNotFoundError
            If a listed source contains no carbonate sensitivity files, or a
            listed step has no `output` directory.
        """
        if problems := self._config_problems(self._config):
            raise NotImplementedError("; ".join(problems))

        try:
            files = collect_output_files(
                CarbonateSensitivityFile,
                self._config.get(self.KEY_PATH),
                self.referenced_steps(self._config),
                self._workplan,
            )
        except FileNotFoundError as err:
            raise FileNotFoundError(f"{err} {_CDR_GAS_OUTPUT_HINT}") from err

        return CarbonateSensitivityTrxAdapter.adapt(files)

    @t.override
    @staticmethod
    def suffix() -> str:
        """Return a suffix used when persisting a resource modified by this transform.

        Returns
        -------
        str
        """
        return "csfrom"


DirectiveConfig.register(ContinuanceDirective.key(), ContinuanceDirective)
DirectiveConfig.register(NestingDirective.key(), NestingDirective)
DirectiveConfig.register(
    CarbonateSensitivityDirective.key(), CarbonateSensitivityDirective
)
