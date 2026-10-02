import itertools
import typing as t
from abc import ABC
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from enum import StrEnum, auto
from pathlib import Path
from urllib.parse import quote, unquote

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
from pydantic.json_schema import GetJsonSchemaHandler, JsonSchemaValue
from pydantic_core import CoreSchema

from cstar.base.utils import generate_schema_ref, lazy_import, slugify
from cstar.execution.file_system import StateDirectoryManager
from cstar.orchestration.serialization import register_representer, strenum_representer

RequiredString: t.TypeAlias = t.Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]
"""A non-empty string with no leading or trailing whitespace."""

nx = lazy_import("networkx")

KeyValueStore: t.TypeAlias = dict[
    str,
    bool
    | str
    | float
    | list[str]
    | list[float]
    | dict[
        str,
        t.Any,
    ],
]
"""A collection of user-defined key-value pairs."""

KEY_CLOBBER: t.Final[str] = "clobber"
"""The `workflow_overrides` key indicating a step's prior state should be
cleared and re-executed."""

KEY_RESUME: t.Final[str] = "resume"
"""The `workflow_overrides` key indicating a step's failed prior attempt should
be resumed in place (no clobber) by an application that supports it."""

KEY_PRE_RUN: t.Final[str] = "pre_run"
"""The `workflow_overrides` key indicating a step should perform every stage
before its model launch (stage inputs, clone and compile, generate the
namelist) and then stop, by an application that supports it. A later run of
the same step attaches to the prepared working directory."""

COMPUTE_OVERRIDE_NAMESPACES: t.Final[frozenset[str]] = frozenset({"local", "slurm"})
"""Launcher namespaces recognized as top-level `Step.compute_overrides` keys.

Compute overrides are nested under the launcher that consumes them, e.g.
`{"slurm": {"num_cpus": 16}}`; a top-level key outside this set would be
silently ignored at submission and is rejected as a misconfiguration."""

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
        extra="forbid",
        from_attributes=True,
        str_strip_whitespace=True,
        use_attribute_docstrings=True,
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
    HELLO_WORLD = "hello_world"
    """Sample custom application."""
    PLOTTER = "plotter"
    """Demo plotting application."""
    NEST_IC = "nest_ic"
    """Application performing a nested simulation run."""
    UPSCALER = "upscaler"
    """Application performing an upscaled simulation run."""


class BlueprintCore(ConfiguredBaseModel):
    """Model used for metadata-only loading of blueprints"""

    name: RequiredString
    """A unique, user-friendly name for this blueprint."""

    application: RequiredString
    """The process type to be executed by the blueprint."""

    schema_version: str = "1.0.0"
    """The schema version for the document."""

    model_config: t.ClassVar[ConfigDict] = ConfigDict(
        extra="allow",
    )
    """Configuration allowing extra field data."""


class Blueprint(ConfiguredBaseModel, ABC):
    """Common elements of all blueprints."""

    name: RequiredString
    """A unique, user-friendly name for this blueprint."""

    description: RequiredString
    """A user-friendly description of the scenario to be executed by the blueprint."""

    application: RequiredString
    """The process type to be executed by the blueprint."""

    state: BlueprintState = BlueprintState.NotSet
    """The current validation status of the blueprint."""

    schema_version: str = "1.0.0"
    """The schema version for the document."""

    working_dir: TargetDirectoryPath | None = Field(default=None)
    """Directory the application writes to; omitted, C-Star uses ``CSTAR_DATA_HOME/blueprint_runs/<application>/<name>``."""

    @property
    def cpus_needed(self) -> int:
        """The number of CPUs needed to run this blueprint.

        Defaults to 1. Can be overridden by subclasses.
        """
        return 1

    @property
    def single_node(self) -> bool:
        """Whether this blueprint's work is confined to a single node.

        `True` marks an application that runs as one process (threads, or a
        local dask scheduler) rather than an MPI job, so its CPU requirement
        cannot usefully span nodes. The scheduler launcher then pins the job
        to one node and clamps `cpus_needed` to the target queue's CPUs per
        node instead of requesting additional nodes it could never use.

        Defaults to `False`. Can be overridden by subclasses.
        """
        return False

    @property
    def effective_working_dir(self) -> Path:
        """The directory this blueprint runs in.

        ``working_dir`` when the blueprint declares one, otherwise C-Star's
        default location for its application and name
        (:meth:`~cstar.execution.file_system.StateDirectoryManager.blueprint_run_dir`).
        Consumers read this, not ``working_dir``, so an omitted value is
        resolved in one place.
        """
        if self.working_dir is not None:
            return self.working_dir
        return StateDirectoryManager.blueprint_run_dir(self.application, self.name)

    @field_validator("working_dir", mode="after")
    @classmethod
    def _resolve_out_dir(
        cls,
        value: Path | None,
        _info: "ValidationInfo",
    ) -> Path | None:
        return None if value is None else value.expanduser().resolve()

    @model_serializer(mode="wrap")
    def serialize_with_schema_ref(
        self,
        handler: Callable[[BaseModel], dict[str, t.Any]],
        info: "SerializationInfo",
    ) -> dict[str, t.Any]:
        data = handler(self)
        if not info.exclude or "$schema" not in info.exclude:
            return {
                "$schema": generate_schema_ref(
                    self.application,
                    self.schema_version,
                ),
                **data,
            }
        return data


class WorkplanState(StrEnum):
    """The allowed states for a work plan."""

    NotSet = auto()

    Draft = auto()
    """A workflow that has not been validated."""

    Validated = auto()
    """A workflow that has been validated."""


class DeferredBlueprintRef(BaseModel):
    """A reference to a blueprint that is generated by an upstream step.

    The blueprint file does not exist when the workplan is scheduled; it is
    located in the producing step's output directory at runtime.
    """

    SCHEME: t.ClassVar[str] = "step://"
    """URI scheme used as the command-line wire form of a deferred reference."""

    from_step: RequiredString
    """The name of the step that generates the blueprint."""

    filename: str = Field(default="")
    """Optional name of the blueprint file within the producing step's output
    directory.

    When omitted, exactly one blueprint file must exist in the producing
    step's output directory.
    """

    model_config: t.ClassVar[ConfigDict] = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        use_attribute_docstrings=True,
    )
    """Configures the behavior of the pydantic model."""

    def __str__(self) -> str:
        """Return the reference as a ``step://`` URI.

        The step name and filename are URL-quoted so the URI can be split
        unambiguously on ``/``.

        Returns
        -------
        str
        """
        uri = f"{self.SCHEME}{quote(self.from_step, safe='')}"
        if self.filename:
            uri = f"{uri}/{quote(self.filename, safe='')}"
        return uri

    @classmethod
    def matches(cls, uri: str) -> bool:
        """Return `True` when the supplied URI uses the deferred-blueprint scheme.

        Parameters
        ----------
        uri : str
            The URI to inspect.

        Returns
        -------
        bool
        """
        return uri.strip().casefold().startswith(cls.SCHEME)

    @classmethod
    def from_uri(cls, uri: str) -> "DeferredBlueprintRef":
        """Parse a ``step://`` URI produced by `__str__` back into a reference.

        Parameters
        ----------
        uri : str
            The URI to parse.

        Returns
        -------
        DeferredBlueprintRef

        Raises
        ------
        ValueError
            If the URI does not use the deferred-blueprint scheme.
        """
        if not cls.matches(uri):
            msg = f"Not a deferred blueprint URI: {uri!r}"
            raise ValueError(msg)

        # the scheme matched case-insensitively; strip it by length
        remainder = uri.strip()[len(cls.SCHEME) :]
        step_part, _, file_part = remainder.partition("/")
        return cls(
            from_step=unquote(step_part),
            filename=unquote(file_part),
        )


class InlineBlueprintRef(BaseModel):
    """A marker declaring that a step's blueprint is built from its overrides.

    No blueprint file exists; the blueprint is synthesized from the
    application's model plus the step's ``blueprint_overrides``.
    """

    TOKEN: t.ClassVar[str] = "inline"
    """YAML token declaring that a step's blueprint is built from its overrides."""

    model_config: t.ClassVar[ConfigDict] = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        use_attribute_docstrings=True,
    )
    """Configures the behavior of the pydantic model."""

    def __str__(self) -> str:
        """Return the ``inline`` token.

        Returns
        -------
        str
        """
        return self.TOKEN

    @classmethod
    def matches(cls, value: str) -> bool:
        """Return `True` when the supplied value is the inline token.

        Parameters
        ----------
        value : str
            The value to inspect.

        Returns
        -------
        bool
        """
        return value.strip().casefold() == cls.TOKEN

    @model_serializer(mode="plain")
    def _serialize(self) -> str:
        """Serialize as the ``inline`` token so a dumped step reads as authored."""
        return self.TOKEN

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        core_schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        """Advertise the ``inline`` token in the generated JSON schema."""
        return {
            "const": cls.TOKEN,
            "description": "Build the blueprint from the application's model "
            "plus this step's `blueprint_overrides`.",
        }


class Step(ConfiguredBaseModel):
    """An individual unit of execution within a workplan."""

    name: RequiredString
    """The user-friendly name of the step."""

    application: RequiredString
    """The user-friendly name of the application executed in the step."""

    blueprint_path: FilePath | DeferredBlueprintRef | InlineBlueprintRef | str = Field(
        alias="blueprint"
    )
    """The blueprint that will be executed in this step.

    A mapping with a `from_step` key defers the blueprint to one generated by
    an upstream step at runtime. The token `inline` builds the blueprint from the
    application's model plus `blueprint_overrides`."""

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

    model_config: t.ClassVar[ConfigDict] = ConfigDict(
        polymorphic_serialization=True,
    )
    """Configures the behavior of the pydantic model."""

    @field_validator("blueprint_path", mode="before")
    @classmethod
    def _inline_token(cls, value: t.Any) -> t.Any:
        """Convert the ``inline`` token to an `InlineBlueprintRef`.

        A relative file literally named ``inline`` must be written ``./inline``.
        An empty mapping is rejected so the token is the only spelling of an
        inline blueprint.
        """
        if isinstance(value, str) and InlineBlueprintRef.matches(value):
            return InlineBlueprintRef()
        if isinstance(value, Mapping) and not value:
            msg = (
                "blueprint mapping must name `from_step`; use the token "
                f"{InlineBlueprintRef.TOKEN!r} for an inline blueprint"
            )
            raise ValueError(msg)
        return value

    @field_validator("workflow_overrides", mode="after")
    @classmethod
    def _exclusive_rerun_modes(cls, value: KeyValueStore) -> KeyValueStore:
        """Reject a step marked for resume together with clobber or pre-run."""
        if value.get(KEY_CLOBBER, False) and value.get(KEY_RESUME, False):
            raise ValueError(
                f"workflow_overrides {KEY_CLOBBER!r} and {KEY_RESUME!r} are "
                "mutually exclusive: a step is either re-run from scratch or resumed"
            )
        if value.get(KEY_PRE_RUN, False) and value.get(KEY_RESUME, False):
            raise ValueError(
                f"workflow_overrides {KEY_PRE_RUN!r} and {KEY_RESUME!r} are "
                "mutually exclusive: a step is either prepared without launching "
                "or resumed"
            )
        return value

    @field_validator("compute_overrides", mode="after")
    @classmethod
    def _known_compute_namespaces(cls, value: KeyValueStore) -> KeyValueStore:
        """Reject compute overrides that no launcher would consume."""
        if unknown := sorted(set(value) - COMPUTE_OVERRIDE_NAMESPACES):
            raise ValueError(
                f"unknown compute_overrides key(s) {unknown}: compute overrides "
                "must be nested under a launcher namespace "
                f"({', '.join(sorted(COMPUTE_OVERRIDE_NAMESPACES))}), "
                'e.g. {"slurm": {"num_cpus": 16}}'
            )
        return value

    @property
    def safe_name(self) -> str:
        """Return a URL-safe version of the step name.

        Returns
        -------
        str
        """
        return slugify(self.name)

    @property
    def clobber(self) -> bool:
        """Return `True` if this step's prior state should be cleared and
        re-executed.

        Returns
        -------
        bool
        """
        return bool(self.workflow_overrides.get(KEY_CLOBBER, False))

    @property
    def resume(self) -> bool:
        """Return `True` if this step's failed prior attempt should be resumed
        in place instead of cleared and re-executed.

        Returns
        -------
        bool
        """
        return bool(self.workflow_overrides.get(KEY_RESUME, False))

    @property
    def pre_run(self) -> bool:
        """Return `True` if this step should perform every stage before its
        model launch and then stop, leaving the working directory for a later
        run to attach to.

        Returns
        -------
        bool
        """
        return bool(self.workflow_overrides.get(KEY_PRE_RUN, False))

    @property
    def is_deferred(self) -> bool:
        """Whether the blueprint is generated at runtime by an upstream step.

        Returns
        -------
        bool
        """
        return isinstance(self.blueprint_path, DeferredBlueprintRef)

    @property
    def is_inline(self) -> bool:
        """Whether the blueprint is built from the application's model plus
        the step's ``blueprint_overrides``.

        Returns
        -------
        bool
        """
        return isinstance(self.blueprint_path, InlineBlueprintRef)

    @field_validator("name")
    @classmethod
    def reject_reserved_keywords(cls, value: str) -> str:
        keyword = "all"
        if value == keyword:
            raise ValueError(f"The value {keyword!r} is a reserved keyword")
        return value


class Workplan(ConfiguredBaseModel):
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

    model_config: t.ClassVar[ConfigDict] = ConfigDict(
        polymorphic_serialization=True,
    )
    """Configures the behavior of the pydantic model."""

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

    @field_validator("steps", mode="after")
    @classmethod
    def _check_dependency_cycles(cls, value: Sequence[Step]) -> Sequence[Step]:
        """Verify the step dependency graph contains no cycles.

        Parameters
        ----------
        value : Sequence[Step]
            The steps in the workplan.
        """
        graph = nx.DiGraph(
            (dep, step.name) for step in value for dep in step.depends_on
        )
        try:
            cycle = nx.find_cycle(graph)
        except nx.NetworkXNoCycle:
            return value

        names = " -> ".join([cycle[0][0], *(dst for _, dst in cycle)])
        msg = f"Dependency cycle detected: {names}"
        raise ValueError(msg)

    @field_validator("steps", mode="after")
    @classmethod
    def _check_deferred_blueprints(cls, value: Sequence[Step]) -> Sequence[Step]:
        """Verify deferred blueprint references point at valid producer steps.

        Parameters
        ----------
        value : Sequence[Step]
            The steps in the workplan.
        """
        names = {step.name for step in value}
        for step in value:
            if not isinstance(step.blueprint_path, DeferredBlueprintRef):
                continue

            producer = step.blueprint_path.from_step
            if producer not in names:
                msg = (
                    f"Step {step.name!r} defers its blueprint to unknown "
                    f"step {producer!r}"
                )
                raise ValueError(msg)
            if producer not in step.depends_on:
                msg = (
                    f"Step {step.name!r} defers its blueprint to step "
                    f"{producer!r} but does not list it in `depends_on`"
                )
                raise ValueError(msg)

        return value


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
