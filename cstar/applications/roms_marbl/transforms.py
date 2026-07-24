import os
import re
import typing as t
from collections.abc import Sequence
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
from cstar.base.utils import (
    DEFAULT_OUTPUT_ROOT_NAME,
    deep_merge,
    min_padded_index,
    slugify,
)
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.transforms import (
    Directive,
    DirectiveConfig,
    OverrideTransform,
    SplitFrequency,
    get_time_slices,
)
from cstar.orchestration.utils import ENV_CSTAR_ORCH_TRX_FREQ


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
        if not is_feature_enabled(ENV_FF_ORCH_TRX_TIMESPLIT):
            return [step]

        blueprint = deserialize(step.blueprint_path, RomsMarblBlueprint)
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
                partition_segment = min_padded_index(0, num_partitions)

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

        If `search_path` identifies a directory, the first item matching the `RestartFile`
        naming convention is returned.

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

        if matches := sorted(search_path.rglob(f"*{cls.SUFFIX}.*.*.{cls.EXT}")):
            # prefer use of pre-partitioned data when available
            return RestartFile(path=matches[0])

        matches = sorted(search_path.rglob(f"*{cls.SUFFIX}*.{cls.EXT}"), reverse=True)
        if matches:
            return RestartFile(path=matches.pop(0))

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
    PATTERN_RST: t.ClassVar[t.Literal[r"^(.*?)_bry\.(\d{14})(?:\.(\d{1,9}))?\.nc$"]] = (
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

        if re.fullmatch(BoundaryFile.PATTERN_RST, value.name, flags=re.ASCII):
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
            BoundaryFile.PATTERN_RST, self.path.as_posix(), flags=re.ASCII
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


class OverrideDirective(Directive, OverrideTransform):
    _overrides: dict[str, t.Any]

    def __init__(
        self,
        config: dict[str, t.Any],
        *,
        workplan: LiveWorkplan | None = None,
    ) -> None:
        """Initialize the instance.

        Parameters
        ----------
        config : dict[str, t.Any] | None
            A dictionary containing configuration for the directive.
        workplan : LiveWorkplan | None
            The workplan instance containing contextual information for the directive.
        """
        Directive.__init__(self, config, workplan=workplan)
        OverrideTransform.__init__(self, self._generate_overrides())

    def _generate_overrides(self) -> dict[str, t.Any]:
        """Generate any system overrides required by the directive.

        Returns
        -------
        dict[str, t.Any]
        """
        return {}


class ContinuanceDirective(OverrideDirective):
    """A transform that locates a restart file with an unknown path at the
    time the task was scheduled.
    """

    KEY_PATH: t.Final[str] = "path"
    """Key used to specify a path as the source for continuance."""
    KEY_STEP: t.Final[str] = "step"
    """Key used to specify a step name as the source for continuance."""

    @classmethod
    def key(cls) -> str:
        return "continue-from"

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

        search_path: Path | None = None

        if target_path := self._config.get(self.KEY_PATH, None):
            search_path = Path(target_path)

        if name := self._config.get(self.KEY_STEP, None):
            if name in self.workplan:
                step = self.workplan[name]
                search_path = step.fsm.output_dir
            else:
                msg = f"Unable to locate step {name!r} in workplan"
                raise KeyError(msg)

        if search_path and (
            restart_file := RestartFile.find(search_path, notfound_ok=False)
        ):
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
    """A transform that uses a restart file and boundary conditions from a previous parent simulation."""

    @classmethod
    def key(cls) -> str:
        return "nest-from"

    def _generate_overrides(self) -> dict[str, t.Any]:
        """Create an overrides dictionary that will result in the modified blueprint.

        NestingDirective creates overrides that will modify the initial conditions
        and boundary conditions.

        Returns
        -------
        dict[str, t.Any]
        """
        if "rst_path" not in self._config:
            msg = "Invalid nesting transform configuration. Must include rst_path."
            raise NotImplementedError(msg)

        if "bry_path" not in self._config:
            msg = "Invalid nesting transform configuration. Must include bry_path."
            raise NotImplementedError(msg)

        search_path = Path(self._config["rst_path"])
        if restart_file := RestartFile.find(search_path, notfound_ok=False):
            rst_override_dict = RestartFileTrxAdapter.adapt(restart_file)
        else:
            msg = f"No restart file located in search path: {search_path!r}"
            raise ValueError(msg)

        search_path = Path(self._config["bry_path"])
        if boundary_files := BoundaryFile.find(search_path, notfound_ok=False):
            bry_override_dict = BoundaryFileTrxAdapter.adapt(boundary_files)
        else:
            msg = f"No boundary files located in search path: {search_path!r}"
            raise ValueError(msg)

        return {**rst_override_dict, **bry_override_dict}

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
