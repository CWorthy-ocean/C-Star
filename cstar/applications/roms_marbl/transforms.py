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
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
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
from cstar.execution.file_system import RomsFileSystemManager
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import serialize
from cstar.orchestration.transforms import (
    ApplyOverridesDirective,
    DirectiveConfig,
    OverrideDirective,
    SplitFrequency,
    get_time_slices,
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

        partitioned_glob = f"*{cls.SUFFIX}.{cls.TS_GLOB}.*.{cls.EXT}"
        joined_glob = f"*{cls.SUFFIX}.{cls.TS_GLOB}.{cls.EXT}"

        for glob_pattern in (partitioned_glob, joined_glob):
            rst_files = [
                RestartFile(path=match)
                for match in search_path.rglob(glob_pattern)
                if re.fullmatch(cls.PATTERN_RST, match.name, flags=re.ASCII)
            ]
            if rst_files:
                latest_ts = max(rst.timestamp for rst in rst_files)
                return min(
                    (rst for rst in rst_files if rst.timestamp == latest_ts),
                    key=lambda rst: rst.partition or 0,
                )

        if not notfound_ok:
            msg = f"No restart files located. Unable to continue from {search_path!r}"
            raise FileNotFoundError(msg)

        return None

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


class BoundaryFile(BaseModel):
    path: Path
    """The path to a boundary file."""
    _base: str = PrivateAttr()
    """The base name of the file."""
    _segment: str | None = PrivateAttr(default=None)
    """The segment identifier of the file."""
    _ts: datetime = PrivateAttr()
    """The timestamp parsed from the file name."""

    EXT: t.ClassVar[t.Literal["nc"]] = "nc"
    """The expected file extension for a boundary file."""
    FMT_TS: t.ClassVar[t.Literal["%Y%m%d%H%M%S"]] = "%Y%m%d%H%M%S"
    """The expected timestamp format in the boundary file name"""
    PATTERN_BRY: t.ClassVar[t.Literal[r"^(.*?)_bry\.(\d{14})(?:\.(\d{1,9}))?\.nc$"]] = (
        r"^(.*?)_bry\.(\d{14})(?:\.(\d{1,9}))?\.nc$"
    )
    """A regex identifying full boundary or partitioned files."""
    SUFFIX: t.ClassVar[t.Literal["_bry"]] = "_bry"
    """A unique suffix found in the name of boundary files"""

    @classmethod
    def find(
        cls, search_path: Path, notfound_ok: bool = True
    ) -> Sequence["BoundaryFile"] | None:
        """Search for boundary files in the specified location.

        Parameters
        ----------
        search_path : Path
            The path to search
        notfound_ok : bool
            If False, raise an exception if no boundary files are found.

        Returns
        -------
        Sequence["BoundaryFile"] | None

        Raises
        ------
        ValueError
            If the search path does not exist.
        FileNotFoundError
            If no recognizable boundary files are found in the search path
        """
        search_path = search_path.expanduser().resolve()

        if not search_path.exists():
            msg = f"No directory found at path: {search_path!r}"
            raise ValueError(msg)

        matches = sorted(search_path.rglob(f"*{cls.SUFFIX}*.{cls.EXT}"))
        if matches:
            return tuple(BoundaryFile(path=m) for m in matches)

        if not notfound_ok:
            msg = f"No boundary files located. Unable to continue from {search_path!r}"
            raise FileNotFoundError(msg)

        return None

    @classmethod
    def from_parts(
        cls,
        base: str,
        timestamp: datetime,
        segment: str | None = None,
        directory: Path | None = None,
    ) -> "BoundaryFile":
        """Create a BoundaryFile from components.

        Parameters
        ----------
        base : str
            The base name for the boundary file.
        timestamp : datetime
            The timestamp for the boundary file.
        segment : str | None
            The 0-padded segment number if partitioned, otherwise `None`.
        directory : Path | None
            The directory to contain the file. If not specified, defaults to cwd.

        Returns
        -------
        BoundaryFile

        Raises
        ------
        ValueError
            If the search path does not exist or contains no recognizable boundary files.
        """
        ts = timestamp.strftime(cls.FMT_TS)
        parted_clause = f".{segment}" if segment is not None else ""
        filename = f"{base}{cls.SUFFIX}.{ts}{parted_clause}.{cls.EXT}"

        path = directory / filename if directory else Path(filename)
        return BoundaryFile(path=path)

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: Path, _info: "ValidationInfo") -> Path:
        """Verify the supplied path meets the boundary file naming convention.

        Parameters
        ----------
        value : str
            The value of the checkpoint frequency property
        _info : ValidationInfo
            Metadata for the current validation context
        """
        if value.suffix != f".{BoundaryFile.EXT}":
            msg = f"File extension does not match expected naming convention: {value.suffix}"
            raise ValueError(msg)

        if re.fullmatch(BoundaryFile.PATTERN_BRY, value.name, flags=re.ASCII):
            return value

        msg = f"File name does not match expected naming convention: {value}"
        raise ValueError(msg)

    @model_validator(mode="after")
    def _model_validate(self) -> "BoundaryFile":
        """Perform post-processing on the boundary file path.

        Returns
        -------
        BoundaryFile
        """
        matches = re.fullmatch(
            BoundaryFile.PATTERN_BRY, self.path.as_posix(), flags=re.ASCII
        )
        if not matches:
            msg = f"File name does not match expected naming convention: {self.path}"
            raise ValueError(msg)

        self._base = matches.group(1)
        self._ts = datetime.strptime(matches.group(2), BoundaryFile.FMT_TS)
        # look for segment for partition number, e.g. <base>.<ts>.000.nc vs. <base>.<ts>.nc
        self._segment = matches.group(3)
        return self

    @property
    def timestamp(self) -> datetime:
        """Return a datetime derived from the timestamp in the boundary file name.

        Returns
        -------
        datetime
        """
        return self._ts

    @property
    def is_partitioned(self) -> bool:
        """Return `True` if the boundary file belongs to a partitioned dataset.

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


class BoundaryFileTrxAdapter:
    """Convert a boundary file into a dictionary useful for use in an OverrideTransform."""

    @classmethod
    def adapt(cls, bry_files: Sequence[BoundaryFile]) -> dict[str, t.Any]:
        """Given a tuple of boundary files, create a dictionary containing the overrides necessary to
        execute a simulation with the restart file specified in the initial conditions.

        Parameters
        ----------
        restart_file : ResetFile
            The restart file metadata used to convert into an override mapping.

        Returns
        -------
        Mapping[str, t.Any]
        """
        return {
            "forcing": {
                "boundary": {
                    "data": [
                        {
                            "location": bry.path.as_posix(),
                            "partitioned": bry.is_partitioned,
                        }
                        for bry in bry_files
                    ],
                },
            },
        }


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
        The name of the step whose output directory is requested.

    Returns
    -------
    Path
        The step's `output` directory.

    Raises
    ------
    KeyError
        If `workplan` does not contain a step named `name`.
    """
    if name not in workplan:
        msg = f"Unable to locate step {name!r} in workplan"
        raise KeyError(msg)

    fsm = RomsFileSystemManager(workplan[name].fsm.root_dir)
    return fsm.output_dir


def _rst_path_continue_from_conflict_message(step_name: str | None) -> str:
    """Build the error message for a conflicting `rst_path` + `continue-from`.

    Shared between `NestingDirective.__call__` (runtime, where the step name
    is known) and `NestingDirective.validate_directives` (schedule time,
    where only the directives mapping is available).

    Parameters
    ----------
    step_name : str | None
        The step's name, when known; `None` when unavailable.

    Returns
    -------
    str
    """
    subject = f"step {step_name!r}" if step_name is not None else "a step"
    return (
        f"nest-from rst_path and continue-from both set initial conditions for "
        f"{subject}; remove rst_path"
    )


class ContinuanceDirective(OverrideDirective):
    """A transform that locates a restart file with an unknown path at the
    time the task was scheduled, and applies it as the step's initial
    conditions.

    Warns only when the step explicitly overrode `start_date` (via the
    workplan's `blueprint_overrides.runtime_params.start_date`, packaged by
    `package_runtime_overrides` into the runtime `apply-overrides` directive)
    and that explicit value disagrees with the restart this directive
    located. Chained steps that leave `start_date` to the directive (e.g.
    `continue-from: step: <prev>` with no explicit `start_date` override)
    stay quiet -- the base blueprint's `start_date` is unrelated to whatever
    restart gets discovered in that case.
    """

    KEY_PATH: t.Final[str] = "path"
    """Key used to specify a path as the source for continuance."""
    KEY_STEP: t.Final[str] = "step"
    """Key used to specify a step name as the source for continuance."""

    @classmethod
    def key(cls) -> str:
        return "continue-from"

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
        """
        found_keys = set(self._config.keys())
        minimal_keys = {self.KEY_PATH, self.KEY_STEP}

        if found_keys and not found_keys.intersection(minimal_keys):
            msg = (
                "Invalid continuance transform configuration; supported configuration: "
                f"{', '.join(minimal_keys)}, provided configuration: {', '.join(found_keys)}"
            )
            raise NotImplementedError(msg)

        if minimal_keys.issubset(found_keys):
            msg = (
                f"Invalid continuance transform configuration: {self.KEY_PATH!r} and "
                f"{self.KEY_STEP!r} are mutually exclusive; supply only one restart source."
            )
            raise NotImplementedError(msg)

        search_path: Path | None = None

        if target_path := self._config.get(self.KEY_PATH, None):
            search_path = Path(target_path)

        if name := self._config.get(self.KEY_STEP, None):
            search_path = _require_step_output_dir(self.workplan, name)

        if search_path:
            try:
                restart_file = RestartFile.find(search_path, notfound_ok=False)
            except FileNotFoundError as err:
                raise FileNotFoundError(f"{err} {_LEGACY_LAYOUT_HINT}") from err
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
    `initial_conditions` there) and compose in any order.

    `bry_path` is a deprecated alias for `path` (conflicts with `path`/
    `step` if both are supplied). `rst_path` is a deprecated, optional key
    that restores the historical "nest-from also sets initial conditions"
    behavior by applying a restart-file override directly; it is rejected
    when a `continue-from` directive is also present on the step, since both
    would set `initial_conditions`. Both deprecated keys emit a
    `FutureWarning` and a log warning.
    """

    KEY_PATH: t.Final[str] = ContinuanceDirective.KEY_PATH
    """Key used to specify a path as the source for the boundary forcing."""
    KEY_STEP: t.Final[str] = ContinuanceDirective.KEY_STEP
    """Key used to specify a step name as the source for the boundary forcing."""
    KEY_BRY_PATH: t.Final[str] = "bry_path"
    """Deprecated alias for `KEY_PATH`."""
    KEY_RST_PATH: t.Final[str] = "rst_path"
    """Deprecated key that also applies a restart-file override."""

    @classmethod
    def key(cls) -> str:
        return "nest-from"

    @classmethod
    def validate_directives(
        cls, config: Mapping[str, t.Any], directives: Mapping[str, t.Any]
    ) -> None:
        """Reject a `rst_path` config combined with a `continue-from` directive.

        Called at schedule time (see `package_runtime_overrides`) so the
        conflict, which would otherwise only surface on the compute node, is
        caught before the workplan runs.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.
        directives : Mapping[str, t.Any]
            The step's full directives mapping (this directive's key
            included).

        Raises
        ------
        ValueError
            If `rst_path` is set alongside a `continue-from` directive.
        """
        if cls.KEY_RST_PATH in config and ContinuanceDirective.key() in directives:
            raise ValueError(_rst_path_continue_from_conflict_message(None))

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
            deprecated `bry_path` key conflicts with `path`/`step`.
        ValueError
            If no boundary files, or (when `rst_path` is supplied) no
            restart file, can be located with the supplied configuration.
        """
        if self.KEY_BRY_PATH in self._config:
            msg = (
                f"{self.key()!r} config key {self.KEY_BRY_PATH!r} is deprecated "
                f"and will be removed in a future release; use {self.KEY_PATH!r} "
                "instead."
            )
            warnings.warn(msg, FutureWarning, stacklevel=2)
            log.warning(msg)

            if self.KEY_PATH in self._config or self.KEY_STEP in self._config:
                msg = (
                    f"Invalid nesting transform configuration: {self.KEY_BRY_PATH!r} "
                    f"conflicts with {self.KEY_PATH!r}/{self.KEY_STEP!r}; supply "
                    "only one boundary source."
                )
                raise NotImplementedError(msg)

        found_keys = set(self._config.keys())
        boundary_keys = {self.KEY_PATH, self.KEY_STEP, self.KEY_BRY_PATH}

        if not found_keys.intersection(boundary_keys):
            msg = (
                "Invalid nesting transform configuration; supported configuration: "
                f"{', '.join(sorted(boundary_keys))}, provided configuration: "
                f"{', '.join(found_keys)}"
            )
            raise NotImplementedError(msg)

        if {self.KEY_PATH, self.KEY_STEP}.issubset(found_keys):
            msg = (
                f"Invalid nesting transform configuration: {self.KEY_PATH!r} and "
                f"{self.KEY_STEP!r} are mutually exclusive; supply only one boundary source."
            )
            raise NotImplementedError(msg)

        search_path: Path | None = None

        if target_path := self._config.get(self.KEY_PATH) or self._config.get(
            self.KEY_BRY_PATH
        ):
            search_path = Path(target_path)

        if name := self._config.get(self.KEY_STEP):
            search_path = _require_step_output_dir(self.workplan, name)

        boundary_files: Sequence[BoundaryFile] | None = None
        if search_path:
            try:
                boundary_files = BoundaryFile.find(search_path, notfound_ok=False)
            except FileNotFoundError as err:
                raise FileNotFoundError(f"{err} {_LEGACY_LAYOUT_HINT}") from err

        if boundary_files and name:
            partitioned = next((b for b in boundary_files if b.is_partitioned), None)
            if partitioned is not None:
                _reject_partitioned_step_output(name, True, partitioned.path)

        if boundary_files:
            overrides = BoundaryFileTrxAdapter.adapt(boundary_files)
        else:
            msg = f"No boundary files located in search path: {search_path!r}"
            raise ValueError(msg)

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


DirectiveConfig.register(ContinuanceDirective.key(), ContinuanceDirective)
DirectiveConfig.register(NestingDirective.key(), NestingDirective)
