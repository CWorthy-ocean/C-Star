import itertools
import typing as t
from abc import ABC
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from enum import StrEnum, auto
from pathlib import Path

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    HttpUrl,
    PlainSerializer,
    PrivateAttr,
    SerializationInfo,
    StringConstraints,
    ValidationInfo,
    WithJsonSchema,
    field_validator,
    model_serializer,
    model_validator,
)

from cstar.base.utils import generate_schema_ref, slugify
from cstar.orchestration.serialization import register_representer, strenum_representer

RequiredString: t.TypeAlias = t.Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]
"""A non-empty string with no leading or trailing whitespace."""

KeyValueStore: t.TypeAlias = dict[
    str,
    str
    | float
    | list[str]
    | list[float]
    | dict[
        str,
        t.Any,
    ],
]
"""A collection of user-defined key-value pairs."""

TargetDirectoryPath = t.Annotated[
    Path,
    PlainSerializer(str, return_type=str),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]
"""Path to a directory that may not exist until runtime."""


class ConfiguredBaseModel(BaseModel):
    """Base-model configuring common instantiation and validation behavior
    for subclasses.
    """

    model_config: t.ClassVar[ConfigDict] = ConfigDict(
        extra="allow",
        from_attributes=True,
        str_strip_whitespace=True,
        use_attribute_docstrings=True,
    )
    """Configures the behavior of the pydantic model."""


class PolymorphicBaseModel(BaseModel):
    """Base-model configuring common instantiation and validation behavior
    for serialization of covariant references.
    """

    model_config: t.ClassVar[ConfigDict] = ConfigDict(
        extra="allow",
        from_attributes=True,
        str_strip_whitespace=True,
        use_attribute_docstrings=True,
        polymorphic_serialization=True,
    )
    """Configures the behavior of the pydantic model."""


class Resource(ConfiguredBaseModel):
    location: FilePath | HttpUrl | str
    """Location of the file to retrieve."""

    partitioned: bool = Field(default=False, init=False)
    """Flag indicating whether the resource is pre-partitioned."""


class VersionedResource(Resource):
    """A physical asset that is used as an input or configuration and
    has an associated hash used to identify a specific version.
    """

    hash: RequiredString
    """Expected hash of the file."""


class DocLocMixin(ConfiguredBaseModel):
    """Mixin model for documentation and locking fields that are used throughout the schema."""

    documentation: str = Field(default="", validate_default=False)
    """Description of input data provenance; used in provenance roll-up."""

    locked: bool = Field(default=False, init=False, frozen=True)
    """Mutability of the parameter set."""


DataResource: t.TypeAlias = Resource | VersionedResource
"""A physical resource identifying a source of data."""


class Dataset(DocLocMixin):
    """A dataset contains a data block alongside documentation and locking fields."""

    data: list[DataResource]
    """A list of one or more data resources."""

    def __len__(self) -> int:
        """Return the number of data resources in the dataset."""
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

    ROMS_MARBL = "roms_marbl"
    """A UCLA-ROMS simulation coupled with a MARBL biogeochemical component."""
    SLEEP = "sleep"
    """A call to the hostname executable to simplify testing."""
    HELLO_WORLD = "hello_world"
    """Sample custom application."""
    PLOTTER = "plotter"
    """Demo plotting application."""
    NEST_IC = "nest_ic"
    """Application performing a nested simulation run."""
    UPSCALER = "upscaler"
    """Application performing an upscaled simulation run."""


class Blueprint(ConfiguredBaseModel, ABC):
    """Common elements of all blueprints."""

    name: RequiredString
    """A unique, user-friendly name for this blueprint."""

    description: RequiredString
    """A user-friendly description of the scenario to be executed by the blueprint."""

    application: str
    """The process type to be executed by the blueprint."""

    state: BlueprintState = BlueprintState.NotSet
    """The current validation status of the blueprint."""

    schema_version: str = Field("1.0.0", frozen=True)
    """The schema version for the document."""

    working_dir: TargetDirectoryPath = Path()
    """Path to a directory where assets are stored when executing the blueprint."""

    @property
    def cpus_needed(self) -> int:
        """The number of CPUs needed to run this blueprint.

        Defaults to 1. Can be overridden by subclasses.
        """
        return 1

    @field_validator("working_dir", mode="after")
    @classmethod
    def _resolve_out_dir(
        cls,
        value: Path,
        _info: "ValidationInfo",
    ) -> Path:
        return value.expanduser().resolve()

    @model_serializer(mode="wrap")
    def serialize_with_schema_ref(
        self,
        handler: Callable[[BaseModel], dict[str, t.Any]],
        info: "SerializationInfo",
    ) -> dict[str, t.Any]:
        data = handler(self)
        return {
            "$schema": generate_schema_ref(
                self.application,
                self.schema_version,
            ),
            **data,
        }


class WorkplanState(StrEnum):
    """The allowed states for a work plan."""

    NotSet = auto()

    Draft = auto()
    """A workflow that has not been validated."""

    Validated = auto()
    """A workflow that has been validated."""


class Step(PolymorphicBaseModel):
    """An individual unit of execution within a workplan."""

    name: RequiredString
    """The user-friendly name of the step."""

    application: RequiredString
    """The user-friendly name of the application executed in the step."""

    blueprint_path: FilePath | str = Field(alias="blueprint")
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

    directives: KeyValueStore = Field(
        default_factory=dict,
        validate_default=False,
        frozen=True,
    )
    """A collection of key-value pairs specifying configuration for runtime directives."""

    @property
    def safe_name(self) -> str:
        """Return a URL-safe version of the step name.

        Returns
        -------
        str
        """
        return slugify(self.name)


class Workplan(PolymorphicBaseModel):
    """A collection of executable steps and the associated configuration to run them."""

    name: RequiredString
    """The user-friendly name of the workplan."""

    description: RequiredString
    """A user-friendly description of the workplan."""

    steps: Sequence[Step] = Field(
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
        var_counter = Counter(value)
        most_common = var_counter.most_common(1)
        var_name, var_count = most_common[0] if most_common else ("", 0)

        if var_count > 1:
            msg = f"Duplicate runtime variables provided: {var_count} copies of {var_name}"
            raise ValueError(msg)

        return value

    @field_validator("steps", mode="after")
    @classmethod
    def _check_steps(cls, value: list[Step]) -> list[Step]:
        """Verify step names are unique.

        Parameters
        ----------
        value : list[Step]
            The steps in the workplan.
        """
        # use a map to avoid generating safe names repeatedly
        n2s = {s.name: s.safe_name for s in value}

        # identify all steps that resolve to a given safe name
        safe_name_map = {
            n2s[s.name]: [x.name for x in value if n2s[x.name] == n2s[s.name]]
            for s in value
        }

        if collisions := {k: v for k, v in safe_name_map.items() if len(v) > 1}:
            line_errors: list[str] = []
            for v in collisions.values():
                names = ", ".join(f"{x!r}" for x in v)
                line_errors.append(f"Name collision among: {names}.")

            msg = f"Step names must be unique. {' '.join(line_errors)}"
            raise ValueError(msg)

        return value

    @field_validator("steps", mode="after")
    @classmethod
    def _check_dependencies(cls, value: list[Step]) -> list[Step]:
        """Verify the keys named in dependencies are valid step names.

        Parameters
        ----------
        value : list[Step]
            The steps in the workplan.
        """
        names = {step.name for step in value}
        dependencies = set(itertools.chain.from_iterable([x.depends_on for x in value]))
        if diff := dependencies.difference(names):
            msg = f"Unknown dependency specified. No step(s) named: {diff}"
            raise ValueError(msg)

        return value

    @model_validator(mode="after")
    def _model_validator(self) -> "Workplan":
        """Validate attribute relationships."""
        return self


class UserDefinedVariables(ConfiguredBaseModel):
    """A collection of key-value pairs that provides validation of the static
    keys specified at design time and dynamically configured keys at runtime.

    Verification checks:
    - report keys that are configured but not declared (e.g. extra configuration).
    - report keys that are declared but not configured (e.g. missing configuration).
    """

    keys: set[str] = Field(default_factory=set)
    """The set of valid keys available for runtime replacement."""
    mapping: Mapping[str, str] = Field(default_factory=dict)
    """Key-value pairs specifying the value to be used for a key replacement."""

    require_coverage: bool = Field(default=False, frozen=True)
    """Flag indicating if all available keys must be provided."""
    require_declaration: bool = Field(default=True, frozen=True)
    """Flag indicating if unknown keys should be treated as errors."""

    _unknown_keys: set[str] | None = PrivateAttr(None)
    """The set of keys that are configured but not declared."""
    _missing_keys: set[str] | None = PrivateAttr(None)
    """The set of keys that are declared but not configured."""
    _error: str | None = PrivateAttr(None)
    """An error message for the collection."""

    model_config: t.ClassVar[ConfigDict] = ConfigDict(str_strip_whitespace=True)
    """Configure the model to strip whitespace off inputs."""

    @property
    def unknown_keys(self) -> set[str]:
        """Return the set of keys that are configured but not declared.

        Returns
        -------
        set[str]
        """
        if self._unknown_keys is None:
            configured_keys = set(self.mapping.keys())
            self._unknown_keys = configured_keys.difference(self.keys)
        return self._unknown_keys

    @property
    def missing_keys(self) -> set[str]:
        """Return the set of keys that are declared but not configured.

        Returns
        -------
        set[str]
        """
        if self._missing_keys is None:
            configured_keys = set(self.mapping.keys())
            self._missing_keys = self.keys.difference(configured_keys)
        return self._missing_keys

    @property
    def error(self) -> str:
        """Generate the appropriate error message given the `require_xxx` settings.

        Returns
        -------
        str
            An error message when validation fails, otherwise an empty string.
        """
        if self._error is not None:
            return self._error

        unknown_msg = ""
        if self.require_declaration and self.unknown_keys:
            unknown_msg = ", ".join(self.unknown_keys)

        missing_msg = ""
        if self.require_coverage and self.missing_keys:
            missing_msg = ", ".join(self.missing_keys)

        self._error = ""

        err_prefix = "User-defined variables have"
        if unknown_msg and missing_msg:
            self._error = f"{err_prefix} unknown keys: {unknown_msg} and missing keys: {missing_msg}"

        if unknown_msg:
            self._error = f"{err_prefix} unknown keys: {unknown_msg}"

        if missing_msg:
            self._error = f"{err_prefix} missing keys: {missing_msg}"

        return self._error

    @model_validator(mode="after")
    def _ensure_keys(self) -> "UserDefinedVariables":
        """Validate the complete set of runtime variables as a unit.

        Returns
        -------
        UserDefinedVariables

        Raises
        ------
        ValueError
            - If an unknown variable is configured
            - If configured to require all variables
        """
        configured_keys = set(self.mapping.keys())
        self._unknown_keys = configured_keys.difference(self.keys)
        self._missing_keys = self.keys.difference(configured_keys)

        return self

    def __getitem__(self, key: str):
        try:
            return self.mapping[key]
        except KeyError as ex:
            csv = ", ".join(k for k in self.mapping)
            msg = f"Unable to resolve variable {key!r}. Available variables: {csv}"
            raise KeyError(msg) from ex


register_representer(WorkplanState, strenum_representer)
register_representer(BlueprintState, strenum_representer)
