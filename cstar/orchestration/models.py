import typing as t
from copy import deepcopy
from enum import StrEnum, auto

from pydantic import BaseModel, Field, FilePath, StringConstraints, field_validator

RequiredString: t.TypeAlias = t.Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]
KeyValueStore: t.TypeAlias = dict[str, str | int]


class WorkPlanState(StrEnum):
    """The allowed states for a work plan."""

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
    """An optional list of external step names that must execute prior to this step."""
    blueprint: FilePath
    """The blueprint that will be executed in this step."""
    depends_on: t.Annotated[
        list[RequiredString],
        Field(
            default_factory=list,
            frozen=True,
        ),
    ] = []
    """A collection of key-value pairs specifying overrides for blueprint attributes."""
    blueprint_overrides: t.Annotated[
        KeyValueStore,
        Field(
            default_factory=dict,
            validate_default=False,
            frozen=True,
        ),
    ] = {}
    """The user-friendly name of the application executed in the step."""
    compute_overrides: t.Annotated[
        KeyValueStore,
        Field(
            default_factory=dict,
            validate_default=False,
            frozen=True,
        ),
    ] = {}
    """A collection of key-value pairs specifying overrides for compute attributes."""
    workflow_overrides: t.Annotated[
        KeyValueStore,
        Field(
            default_factory=dict,
            validate_default=False,
            frozen=True,
        ),
    ] = {}
    """A collection of key-value pairs specifying overrides for workflow attributes."""


class WorkPlan(BaseModel):
    """A collection of executable steps and the associated configuration to run them."""

    name: RequiredString
    """The user-friendly name of the workplan."""
    description: RequiredString
    """A user-friendly description of the workplan."""
    steps: t.Annotated[
        t.Sequence[Step],
        Field(
            default_factory=list,
            min_length=1,
            frozen=True,
        ),
    ]
    """The steps to be executed by the workplan."""
    state: WorkPlanState = WorkPlanState.Draft
    """The current validation status of the workplan."""
    compute_environment: t.Annotated[
        KeyValueStore,
        Field(
            default_factory=dict,
            frozen=True,
        ),
    ] = {}
    """A collection of key-value pairs specifying attributes for the target compute environment."""
    runtime_vars: t.Annotated[
        list[str],
        Field(
            default_factory=list,
            validate_default=False,
            frozen=True,
        ),
    ] = []
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
