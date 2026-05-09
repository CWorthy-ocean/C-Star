import os
import re
import typing as t
from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from enum import StrEnum
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
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.utils import DEFAULT_OUTPUT_ROOT_NAME, deep_merge, slugify
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.transforms import Directive, DirectiveConfig, OverrideTransform
from cstar.orchestration.utils import ENV_CSTAR_ORCH_TRX_FREQ


class SplitFrequency(StrEnum):
    Daily = "daily"
    Weekly = "weekly"
    Monthly = "monthly"


def _dailies(
    start_date: datetime, end_date: datetime
) -> Iterable[tuple[datetime, datetime]]:
    """Get daily time slices for the given start and end dates."""
    current_date = datetime(start_date.year, start_date.month, start_date.day)
    while current_date < end_date:
        day_start = current_date
        day_end = day_start + timedelta(days=1)
        yield (day_start, day_end)
        current_date = day_end


def _weeklies(
    start_date: datetime, end_date: datetime
) -> Iterable[tuple[datetime, datetime]]:
    """Get weekly time slices for the given start and end dates."""
    current_date = datetime(start_date.year, start_date.month, start_date.day)
    while current_date < end_date:
        week_start = current_date
        week_end = week_start + timedelta(days=7)
        yield (week_start, week_end)
        current_date = week_end


def _monthlies(
    start_date: datetime, end_date: datetime
) -> Iterable[tuple[datetime, datetime]]:
    """Get monthly time slices for the given start and end dates."""
    current_date = datetime(start_date.year, start_date.month, 1)
    while current_date < end_date:
        month_start = current_date

        if month_start.month == 12:
            month_end = datetime(current_date.year + 1, 1, 1)
        else:
            month_end = datetime(current_date.year, month_start.month + 1, 1)

        yield (month_start, month_end)
        current_date = month_end


SLICE_FUNCTIONS = defaultdict(
    lambda: _monthlies,
    {
        SplitFrequency.Daily.value: _dailies,
        SplitFrequency.Weekly.value: _weeklies,
        SplitFrequency.Monthly.value: _monthlies,
    },
)


def get_time_slices(
    start_date: datetime,
    end_date: datetime,
    frequency: str = SplitFrequency.Monthly.value,
) -> Iterable[tuple[datetime, datetime]]:
    """Get the time slices for the given start and end dates.

    Parameters
    ----------
    start_date : datetime
        The start date.
    end_date : datetime
        The end date.
    frequency : str
        The desired frequency (daily, weekly, monthly).

    Returns
    -------
    Iterable[tuple[datetime, datetime]]
        Iterable containing 2-tuples of (start_date, end_date).
    """
    slice_fn = SLICE_FUNCTIONS[frequency]
    time_slices = list(slice_fn(start_date, end_date))

    # adjust when the start date is not the first day of the month
    if start_date > time_slices[0][0]:
        time_slices[0] = (start_date, time_slices[0][1])

    # adjust when the end date is not the last day of the month
    if end_date < time_slices[-1][1]:
        time_slices[-1] = (time_slices[-1][0], end_date)

    return time_slices


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

    def __call__(self, step: LiveStep) -> Sequence[LiveStep]:
        """Split a step into multiple sub-steps.

        Parameters
        ----------
        step : Step
            The step to split.

        Returns
        -------
        Sequence[Step]
            Steps for each subtask resulting from the split.
        """
        blueprint = deserialize(step.blueprint_path, RomsMarblBlueprint)
        start_date = blueprint.runtime_params.start_date
        end_date = blueprint.runtime_params.end_date

        bp_path = step.fsm.work_dir / Path(step.blueprint_path).name
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
                ),
            )

            compact_fmt = "%Y%m%d%H%M%S"
            compact_sd = sd.strftime(compact_fmt)
            compact_ed = ed.strftime(compact_fmt)

            dynamic_name = f"{i + 1:03d}_{step.safe_name}_{compact_sd}_{compact_ed}"
            child_step_name = slugify(dynamic_name)

            child_fs = step.fsm.get_subtask_manager(child_step_name)

            description = f"Subtask {i + 1} of {n_slices}; Timespan: {sd} to {ed}; {bp_copy.description}"
            overrides: dict[str, t.Any] = {
                "name": dynamic_name,
                "description": description,
                "runtime_params": {
                    "start_date": sd,
                    "end_date": ed,
                    "output_dir": child_fs.root.as_posix(),  # child_fs.output_dir,
                },
            }

            if last_restart_file:
                overrides = deep_merge(
                    overrides,
                    RestartFileTrxAdapter.adapt(last_restart_file),
                )

            child_bp_path = child_fs.work_dir / f"{child_step_name}_bp.yaml"
            serialize(child_bp_path, bp_copy)

            updates: dict[str, t.Any] = {
                "blueprint": child_bp_path.as_posix(),
                "blueprint_overrides": overrides,
                "depends_on": depends_on,
                "name": child_step_name,
            }
            child_step = LiveStep.from_step(step, parent=step, update=updates)
            results.append(child_step)

            if i == len(time_slices) - 1:
                break

            # use dependency on the prior substep to chain all the dynamic steps
            depends_on = [child_step.name]

            # determine padding on partition segment of name from number of partitions
            partition_segment: str | None = None
            if partitioning := bp_copy.partitioning:
                num_partitions = partitioning.n_procs_x * partitioning.n_procs_y
                pad_size = len(str(num_partitions - 1))
                partition_segment = "0".zfill(pad_size)

            # Use the last restart file as initial conditions for the follow-up step
            restart_file = RestartFile.from_parts(
                output_root_name, ed, partition_segment, child_fs.output_dir
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
    PATTERN_RST: t.ClassVar[t.Literal[r"^(.*?)_rst\.(\d{14})(?:\.(\d{1,9}))?\.nc$"]] = (
        r"^(.*?)_rst\.(\d{14})(?:\.(\d{1,9}))?\.nc$"
    )
    """A regex identifying full restart or partitioned files."""
    SUFFIX: t.ClassVar[t.Literal["_rst"]] = "_rst"
    """A unique suffix found in the name of restart files"""

    @classmethod
    def find(cls, search_path: Path, notfound_ok: bool = True) -> "RestartFile | None":
        """Search for a restart file in the specified location.

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
            If the search path does not exist or contains no recognizable restart files.
        """
        search_path = search_path.expanduser().resolve()

        if not search_path.exists():
            msg = f"No directory found at path: {search_path!r}"
            raise ValueError(msg)

        if matches := sorted(search_path.rglob(f"*{cls.SUFFIX}.*.*.{cls.EXT}")):
            # prefer use of pre-partitioned data when available
            return RestartFile(path=matches[0])

        matches = sorted(search_path.rglob(f"*{cls.SUFFIX}*.{cls.EXT}"), reverse=True)
        if matches:
            return RestartFile(path=matches.pop(0))

        if not notfound_ok:
            msg = f"No restart files located. Unable to continue from {search_path!r}"
            raise CstarExpectationFailed(msg)
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
            return value

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
        """Return `True` if the restart file belongs to a partioned dataset.

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


class RestartFileTrxAdapter:
    """Convert a restart file into a dictionary useful for use in an OverrideTransform."""

    @classmethod
    def adapt(cls, rst_file: RestartFile) -> dict[str, t.Any]:
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


class ContinuanceTransform(Directive, OverrideTransform):
    """A transform that locates a restart file with an unknown path at the
    time the task was scheduled.
    """

    def __init__(self, config: dict[str, t.Any]) -> None:
        """Initialize the instance.


        Parameters
        ----------
        config : dict[str, t.Any] | None
            A dictionary containing configuration for the directive.
        """
        Directive.__init__(self, config)
        OverrideTransform.__init__(self, self._create_initial_condition_overrides())

    def _create_initial_condition_overrides(
        self,
    ) -> dict[str, t.Any]:
        """Create an overrides dictionary that will result in the modified blueprint
        using a

        Returns
        -------
        dict[str, t.Any]

        Raises
        ------
        ValueError
            If unknown or invalid configuration is supplied.
        """
        if "path" not in self._config:
            msg = "Invalid continuance transform configuration. Only restart paths are supported."
            raise NotImplementedError(msg)

        search_path = Path(self._config["path"])
        if restart_file := RestartFile.find(search_path, notfound_ok=False):
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


DirectiveConfig.register("continue-from", ContinuanceTransform)
