import typing as t
from datetime import datetime

from pydantic import (
    Field,
    HttpUrl,
    PositiveInt,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pytimeparse import parse

from cstar.orchestration.models import (
    Blueprint,
    ConfiguredBaseModel,
    Dataset,
    DocLocMixin,
    PathFilter,
)

APP_NAME: t.Literal["roms_marbl"] = "roms_marbl"


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


class CodeRepository(DocLocMixin):
    """Reference to a remote code repository with optional path filtering
    and point-in-time specification.
    """

    location: HttpUrl | str
    """Location of the remote code repository."""

    commit: str = Field(default="", validate_default=False)
    """A specific commit to be used."""

    branch: str = Field(default="", validate_default=False)
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


class ParameterSet(DocLocMixin):
    """A base class for parameter sets exposed on a blueprint."""

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
        pattern="([1-9][0-9]*)([hdwmy])",
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

    @field_validator("checkpoint_frequency", mode="after")
    @classmethod
    def _validate_checkpoint_frequency(
        cls,
        value: str,
        _info: "ValidationInfo",
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


class RomsMarblBlueprint(Blueprint):
    """Blueprint schema for running a ROMS-MARBL simulation."""

    schema_version: str = Field("2.0.0", frozen=True)
    """The blueprint schema version."""

    application: str = APP_NAME
    """The process type to be executed by the blueprint."""

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

    nesting_info: Dataset | None = Field(default=None)
    """Location of nesting info input file for this run. Optional."""

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
        """Number of CPUs needed for ROMS (derived from the partitioning parameters)."""
        return self.partitioning.n_procs_x * self.partitioning.n_procs_y
