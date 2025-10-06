import typing as t
from abc import ABC
from copy import deepcopy
from datetime import datetime
from enum import StrEnum, auto
from pathlib import Path

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    HttpUrl,
    PlainSerializer,
    PositiveInt,
    StringConstraints,
    ValidationInfo,
    WithJsonSchema,
    field_validator,
    model_validator,
)
from pytimeparse import parse

RequiredString: t.TypeAlias = t.Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]
"""A non-empty string with no leading or trailing whitespace."""

KeyValueStore: t.TypeAlias = dict[str, str | float | list[str] | list[float]]
"""A collection of user-defined key-value pairs."""

TargetDirectoryPath = t.Annotated[
    Path,
    PlainSerializer(lambda p: str(p), return_type=str),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]
"""Path to a directory that may not exist until runtime."""


class ConfiguredBaseModel(BaseModel):
    """BaseModel with configuration options that we want as default for other models."""

    model_config = ConfigDict(extra="forbid", from_attributes=True)
    """Pydantic ConfigDict with options we want changed."""


class Resource(ConfiguredBaseModel):
    location: FilePath | HttpUrl
    """Location of the file to retrieve."""

    partitioned: bool = Field(default=False, init=False)


class VersionedResource(Resource):
    hash: RequiredString
    """Expected hash of the file."""


class DocLocMixin(ConfiguredBaseModel):
    """Mixin model for documentation and locking fields that are used throughout the schema."""

    documentation: str = Field(default="", validate_default=False)
    """Description of input data provenance; used in provenance roll-up."""

    locked: bool = Field(default=False, init=False, frozen=True)
    """Mutability of the parameter set."""


DataResource: t.TypeAlias = Resource | VersionedResource


class Dataset(DocLocMixin):
    """A dataset contains a data block alongside documentation and locking fields."""

    data: list[DataResource]
    """A list of one or more data resources."""

    def __len__(self) -> int:
        if isinstance(self.data, list):
            return len(self.data)
        return 1 if self.data else 0


class PathFilter(ConfiguredBaseModel):
    """A filter used to specify a subset of files."""

    directory: str | None = Field(default="", validate_default=False)
    """Subdirectory that should be searched or kept."""

    files: list[str] = Field(default_factory=list, validate_default=False)
    """List of specific file names that must be kept.

    File name filtering is combined with the directory filter, if one
    is provided."""


class ForcingConfiguration(ConfiguredBaseModel):
    """Configuration of the forcing parameters of the model."""

    boundary: Dataset
    """Boundary forcing."""

    surface: Dataset
    """Surface forcing"""

    tidal: Dataset | None = Field(default=None, validate_default=False)
    """Tidal forcing."""

    river: Dataset | None = Field(default=None, validate_default=False)
    """River forcing."""

    corrections: Dataset | None = Field(default=None, validate_default=False)
    """Wind or other forcing corrections."""


class CodeRepository(DocLocMixin, ConfiguredBaseModel):
    """Reference to a remote code repository with optional path filtering
    and point-in-time specification.
    """

    location: HttpUrl | str
    """Location of the remote code repository."""

    commit: str = Field(default="", min_length=1, validate_default=False)
    """A specific commit to be used."""

    branch: str = Field(default="", min_length=1, validate_default=False)
    """A specific branch to be used."""

    filter: PathFilter | None = Field(default=None, validate_default=False)
    """A filter specifying the files to be retrieved and persisted from the repository."""

    @property
    def checkout_target(self) -> str:
        """Return the commit if specified, else the branch"""
        return self.commit or self.branch

    @model_validator(mode="after")
    def _model_validator(self) -> "CodeRepository":
        """Perform validation on the model after field-level validation is complete.

        - Ensure that one of commit or branch checkout target is supplied
        """
        if self.commit and self.branch:
            msg = "Supply only one of commit hash or branch."
            raise ValueError(msg)

        if not self.commit and not self.branch:
            msg = "Either the commit hash or branch must be supplied."
            raise ValueError(msg)

        return self


class ROMSCompositeCodeRepository(ConfiguredBaseModel):
    """Collection of repositories used to build, configure, and execute ROMS."""

    roms: CodeRepository
    """The baseline ROMS repository."""

    run_time: CodeRepository
    """Codebase used to modify the runtime behavior of ROMS."""

    compile_time: CodeRepository
    """Codebase used to modify base ROMS compilation."""

    marbl: CodeRepository | None = Field(default=None, validate_default=False)
    """Codebase used to add MARBL to the simulation."""


class BlueprintState(StrEnum):
    """The allowed states for a work plan."""

    NotSet = auto()
    """Default, unset value."""

    Draft = auto()
    """A blueprint that has not been validated."""

    Validated = auto()
    """A blueprint that has been validated."""


class Application(StrEnum):
    """The supported application types."""

    ROMS = auto()
    """A UCLA-ROMS simulation that will not make use of biogeochemical data."""
    ROMS_MARBL = auto()
    """A UCLA-ROMS simulation coupled with a MARBL biogeochemical component."""
    SLEEP = auto()
    """A call to the hostname executable to simplify testing."""


class ParameterSet(DocLocMixin, ConfiguredBaseModel):
    hash: str | None = Field(default=None, init=False, validate_default=False)
    """Hash used to verify the parameters are unchanged."""

    @model_validator(mode="after")
    def _model_validator(self) -> "ParameterSet":
        """Perform validation on the model after field-level validation is complete.

        Ensure the dynamically added parameters meet the minimum naming standard.
        """
        if self.locked and not self.hash:
            msg = "A locked parameter set must include a hash"
            raise ValueError(msg)

        return self


class RuntimeParameterSet(ParameterSet):
    """Parameters for the execution of the model.

    These parameters can be varied (within bounds defined elsewhere in the blueprint, e.g. valid_start/end_date),
    without changing the validity of the model solution.
    """

    start_date: datetime
    """Start of data time range to be used in the simulation."""

    end_date: datetime
    """End of data time range to be used in the simulation."""

    checkpoint_frequency: str = Field(
        default="1d",
        min_length=2,
        pattern="(?P<scalar>[1-9][0-9]*)(?P<unit>[hdwmy])",
    )
    """Time period between creation of checkpoint files.

    Supply a string representing the desired time period, such as:
    - every day: "1d"
    - every week: "1w"
    - every month: "1m" or "4w" or "31d"
    - every 2.5 days: "2d 12h" or "60h"

    A short time period will reduce data re-processing upon restart at the cost
    of additional disk usage and compute used for checkpointing.
    """

    output_dir: TargetDirectoryPath = Path()
    """Directory where runtime outputs will be stored."""

    @field_validator("checkpoint_frequency", mode="after")
    @classmethod
    def _validate_checkpoint_frequency(
        cls,
        value: str,
        _info: ValidationInfo,
    ) -> str:
        """Verify a valid range string for the checkpoint frequency was supplied.

        Parameters:
            value : str
                The value of the checkpoint frequency property
            _info : ValidationInfo
                Metadata for the current validation context
        """
        if not parse(value):
            msg = "Invalid checkpoint frequency supplied."
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _model_validator(self) -> "RuntimeParameterSet":
        """Perform validation on the model after field-level validation is complete."""
        if self.end_date <= self.start_date:
            msg = "start_date must precede end_date"
            raise ValueError(msg)

        return self


class PartitioningParameterSet(ParameterSet):
    """Parameters for the partitioning of the model."""

    n_procs_x: PositiveInt
    """Number of processes used to subdivide the domain on the x-axis."""

    n_procs_y: PositiveInt
    """Number of processes used to subdivide the domain on the y-axis."""


class ModelParameterSet(ParameterSet):
    """
    Parameters that can override ROMS.in values. Unlike RuntimeParameters, these affect the validity of the
    model solution, and should be locked for validated blueprints.
    """

    time_step: PositiveInt
    """The time step the model integrates over."""


class Blueprint(ConfiguredBaseModel, ABC):
    """Common elements of all blueprints."""

    name: RequiredString
    """A unique, user-friendly name for this blueprint."""

    description: RequiredString
    """A user-friendly description of the scenario to be executed by the blueprint."""

    application: Application = Application.ROMS
    """The process type to be executed by the blueprint."""

    state: BlueprintState = BlueprintState.NotSet
    """The current validation status of the blueprint."""

    @property
    def cpus_needed(self) -> int:
        return 1


class RomsMarblBlueprint(Blueprint, ConfiguredBaseModel):
    """Blueprint schema for running a ROMS-MARBL simulation."""

    valid_start_date: datetime
    """Beginning of the time range for the available data."""

    valid_end_date: datetime
    """End of the time range for the available data."""

    code: ROMSCompositeCodeRepository
    """Code repositories used to build, configure, and execute the ROMS simulation."""

    initial_conditions: Dataset = Field(min_length=1, max_length=1)
    """File containing the starting conditions of the simulation."""

    grid: Dataset = Field(min_length=1, max_length=1)
    """File defining the grid geometry."""

    forcing: ForcingConfiguration
    """Forcing configuration."""

    partitioning: PartitioningParameterSet
    """User-defined partitioning parameters."""

    model_params: ModelParameterSet
    """User-defined model parameters."""

    runtime_params: RuntimeParameterSet
    """User-defined runtime parameters."""

    cdr_forcing: Dataset | None = Field(default=None)
    """Location of CDR input file for this run. Optional. User has more control over this compared to other forcing."""

    @model_validator(mode="after")
    def _model_validator(self) -> "RomsMarblBlueprint":
        """Perform validation on the model after field-level validation is complete."""
        if self.valid_end_date <= self.valid_start_date:
            msg = "valid_start_date must precede valid_end_date"
            raise ValueError(msg)

        if self.runtime_params.end_date > self.valid_end_date:
            msg = "end_date is outside the valid range"
            raise ValueError(msg)

        if self.runtime_params.start_date < self.valid_start_date:
            msg = "start_date is outside the valid range"
            raise ValueError(msg)

        return self

    @property
    def cpus_needed(self) -> int:
        return self.partitioning.n_procs_x * self.partitioning.n_procs_y


class WorkplanState(StrEnum):
    """The allowed states for a work plan."""

    NotSet = auto()

    Draft = auto()
    """A workflow that has not been validated."""

    Validated = auto()
    """A workflow that has been validated."""


class ComputePlatform(StrEnum):
    """Supported execution platforms."""

    Local = auto()
    """Indicate that execution should take place locally."""

    AWS = auto()
    """Indicate that execution should take place on AWS resources."""

    Perlmutter = auto()
    """Indicate that execution should take place on Perlmutter."""


class Step(BaseModel):
    """An individual unit of execution within a workplan."""

    name: RequiredString
    """The user-friendly name of the step."""

    application: RequiredString
    """The user-friendly name of the application executed in the step."""

    blueprint: FilePath
    """The blueprint that will be executed in this step."""

    depends_on: list[RequiredString] = Field(
        default_factory=list,
        frozen=True,
    )
    """An optional list of external step names that must execute prior to this step.

    Cycles are not permitted.
    """

    blueprint_overrides: KeyValueStore = Field(
        default_factory=dict,
        validate_default=False,
        frozen=True,
    )
    """A collection of key-value pairs specifying overrides for blueprint attributes."""

    compute_overrides: KeyValueStore = Field(
        default_factory=dict,
        validate_default=False,
        frozen=True,
    )
    """A collection of key-value pairs specifying overrides for compute attributes."""

    workflow_overrides: KeyValueStore = Field(
        default_factory=dict,
        validate_default=False,
        frozen=True,
    )
    """A collection of key-value pairs specifying overrides for workflow attributes."""


class Workplan(BaseModel):
    """A collection of executable steps and the associated configuration to run them."""

    name: RequiredString
    """The user-friendly name of the workplan."""

    description: RequiredString
    """A user-friendly description of the workplan."""

    steps: t.Sequence[Step] = Field(
        default_factory=list,
        min_length=1,
        frozen=True,
    )
    """The steps to be executed by the workplan."""

    state: WorkplanState = Field(default=WorkplanState.NotSet)
    """The current validation status of the workplan."""

    compute_environment: KeyValueStore = Field(
        default_factory=dict,
        frozen=True,
    )
    """A collection of key-value pairs specifying attributes for the target compute environment."""

    runtime_vars: list[str] = Field(
        default_factory=list,
        validate_default=False,
        frozen=True,
    )
    """A collection of user-defined variables that will be populated at runtime."""

    @field_validator("steps", mode="before")
    @classmethod
    def _deep_copy_steps(cls, value: list[Step]) -> list[Step]:
        """Ensure the steps provided are deep copied to avoid external change propagation.

        Parameters
        ----------
        value : list[Step]
            The list of steps assigned to the instance

        Returns
        -------
        list[Step]
            The deep-copied step list

        """
        return deepcopy(value)

    @field_validator("runtime_vars", mode="after")
    @classmethod
    def _check_runtime_vars(cls, value: list[str]) -> list[str]:
        """Ensure no duplicate runtime vars are passed.

        Parameters
        ----------
        value : list[str]
            Variable names used at runtime

        """
        var_counter = t.Counter(value)
        most_common = var_counter.most_common(1)
        var_name, var_count = most_common[0] if most_common else ("", 0)

        if var_count > 1:
            msg = f"Duplicate runtime variables provided: {var_count} copies of {var_name}"
            raise ValueError(msg)

        return value

    @field_validator("steps", mode="after")
    @classmethod
    def _check_steps(cls, value: list[Step]) -> list[Step]:
        """Xxx.

        Parameters
        ----------
        value : list[Step]
            Variable names used at runtime

        """
        name_counter = t.Counter(step.name for step in value)
        most_common = name_counter.most_common(1)
        step_name, step_count = most_common[0]
        step_name, step_count = most_common[0] if most_common else ("", 0)

        if step_count > 1:
            msg = f"Step names must be unique. Found {step_count} steps with name {step_name}"
            raise ValueError(msg)

        return value

    @model_validator(mode="after")
    def _model_validator(self) -> "Workplan":
        """Validate attribute relationships."""
        return self
