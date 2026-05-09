import abc
import typing as t
from collections.abc import Mapping, Sequence
from copy import deepcopy

from cstar.base.adapter import ModelAdapter

APP_ROMS_MARBL: t.Literal["roms_marbl"] = "roms_marbl"
APP_ROMS_MARBL_SCHEMA_1_0_0: t.Literal["1.0.0"] = "1.0.0"
APP_ROMS_MARBL_SCHEMA_2_0_0: t.Literal["2.0.0"] = "2.0.0"


APP_HW: t.Literal["hello_world"] = "hello_world"
APP_HW_SCHEMA_1_0_0: t.Literal["1.0.0"] = "1.0.0"


class SchemaBounds(t.TypedDict):
    """Typed dictionary for capturing the min and max schema versions for an app."""

    min: str
    max: str


hw_bounds: SchemaBounds = {
    "min": APP_HW_SCHEMA_1_0_0,
    "max": APP_HW_SCHEMA_1_0_0,
}
"""Schema bounds for the hello_world blueprint schema."""
rm_bounds: SchemaBounds = {
    "min": APP_ROMS_MARBL_SCHEMA_1_0_0,
    "max": APP_ROMS_MARBL_SCHEMA_2_0_0,
}
"""Schema bounds for the roms_marbl blueprint schema."""
default_schema_bounds: Mapping[str, SchemaBounds] = {
    APP_HW: hw_bounds,
    APP_ROMS_MARBL: rm_bounds,
}


class CstarMigrationError(Exception):
    """Base class for errors arising from a schema migration."""


class CstarUnsupportedMigrationError(CstarMigrationError):
    """An error that occurs due to an unknown source or target schema version."""


class SchemaAdapter(abc.ABC, ModelAdapter[dict[str, t.Any], dict[str, t.Any]]):
    """Contract exposing a mechanism to adapt a source model to a target type."""

    def __init__(
        self,
        model: dict[str, str],
    ) -> None:
        super().__init__(model)

    @classmethod
    @abc.abstractmethod
    def application(cls) -> str: ...

    @classmethod
    @abc.abstractmethod
    def source(cls) -> str: ...

    @classmethod
    @abc.abstractmethod
    def target(cls) -> str: ...


RawModelVersionAdapterType: t.TypeAlias = type[SchemaAdapter]
"""An adapter that converts the content of a dumped model into another version."""
ConverterMap: t.TypeAlias = Mapping[tuple[str, str, str], RawModelVersionAdapterType]
"""A mapping of (application, source version, target version) keys to adapters."""


class MigrationPlan(t.NamedTuple):
    """Results describing the plan that will be used to complete a migration."""

    source: str
    """The version of the schema that the document will be upgraded from."""
    target: str
    """The version of the schema that the document will be upgraded to."""
    plan: list[RawModelVersionAdapterType]
    """The ordered list of adapaters to apply to complete the migration."""


class Migration(abc.ABC):
    """Base class for types that will execute a version migration by executing
    one to many SchemaAdapters.
    """

    @abc.abstractmethod
    def plan(self, dumped: dict[str, t.Any]) -> MigrationPlan:
        """Identify the upgrade path."""
        ...

    @abc.abstractmethod
    def migrate(self, dumped: dict[str, t.Any]) -> dict[str, t.Any]:
        """Execute the upgrade path."""
        ...


class RomsMarblSchemaAdapter2025v1(SchemaAdapter):
    """Convert RomsMarblBlueprint from the 1.0.0 schema into the 2.0.0 schema."""

    @classmethod
    def application(cls) -> str:
        return APP_ROMS_MARBL

    @classmethod
    def source(cls) -> str:
        return APP_ROMS_MARBL_SCHEMA_1_0_0

    @classmethod
    def target(cls) -> str:
        return APP_ROMS_MARBL_SCHEMA_2_0_0

    @t.override
    def adapt(self) -> dict[str, str]:
        runtime_params = t.cast(
            "dict[str, str]",
            self.model.get("runtime_params", {"output_dir": None}),
        )
        if output_dir := runtime_params.pop("output_dir", None):
            self.model["output_dir"] = output_dir
        self.model["version"] = self.target()
        return {**self.model}


class HelloWorldSchemaAdapter2025v1(SchemaAdapter):
    """Convert RomsMarblBlueprint from the 1.0.0 schema into the 2.0.0 schema."""

    @classmethod
    def application(cls) -> str:
        return APP_HW

    @classmethod
    def source(cls) -> str:
        return APP_HW_SCHEMA_1_0_0

    @classmethod
    def target(cls) -> str:
        return APP_HW_SCHEMA_1_0_0

    @t.override
    def adapt(self) -> dict[str, str]:
        self.model["version"] = self.target()
        return {**self.model}


class BlueprintMigration(Migration):
    """A migration controller for RomsMarblBlueprints."""

    adapters: Sequence[RawModelVersionAdapterType]
    """The adapters available to migrate the blueprint."""
    adapter_lookup: t.Final[ConverterMap]
    """A mapping of unique converter key tuples (app, source, target) to adapters."""
    schema_bounds: Mapping[str, SchemaBounds]
    """A mapping of unique app names to their minimum and maximum schema version."""

    def __init__(
        self,
        adapters: Sequence[RawModelVersionAdapterType] | None = None,
        schema_bounds: Mapping[str, SchemaBounds] | None = None,
    ) -> None:
        self.adapters = adapters or [RomsMarblSchemaAdapter2025v1]
        self.schema_bounds = schema_bounds or default_schema_bounds
        self.adapter_lookup = self._build_adapter_lookup()

    def _build_adapter_lookup(self) -> ConverterMap:
        """Build the mapping that enables the lookup of adapters.

        Returns
        -------
        ConverterMap
        """
        results: dict[tuple[str, str, str], t.Any] = {}
        for klass in self.adapters:
            results[(klass.application(), klass.source(), klass.target())] = klass
        return results

    def plan(self, dumped: dict[str, t.Any]) -> MigrationPlan:
        """Determine the available upgrade path.

        `plan` assumes only 1 mapping for any source schema version exists. It
        will not backtrack to locate an alternative upgrade path if an adapter
        cannot traverse to the goal state and becomes stuck.

        Returns
        -------
        ConversionPlan
            NamedTuple containing (source version, target version, plan)

        Raises
        ------
        CstarUnsupportedMigrationError
            If unable to identify a complete migration upgrade path.
        """
        plan: list[RawModelVersionAdapterType] = []
        application = dumped["application"]

        if application not in self.schema_bounds:
            msg = f"No schema bounds registered for application {application!r}"
            raise CstarUnsupportedMigrationError(msg)

        found_version = str(dumped.get("version", "")).strip()
        initial_version = found_version or self.schema_bounds[application]["min"]
        version = initial_version
        goal = self.schema_bounds[application]["max"]

        while version != goal:
            # find a migrations with current version as the source
            key = next(
                (
                    (app, vsource, vtarget)
                    for (app, vsource, vtarget) in self.adapter_lookup
                    if app == application and vsource == version
                ),
                None,
            )
            if not key:
                msg = f"No migration adapter from {version!r} to {goal!r}"
                raise CstarUnsupportedMigrationError(msg)

            # store adapter and prepare to traverse the next edge
            plan.append(self.adapter_lookup[key])
            _, _, version = key

        if version != goal:
            msg = f"Incomplete migration from {initial_version!r} to {goal!r}"
            raise CstarUnsupportedMigrationError(msg)

        return MigrationPlan(initial_version, goal, plan)

    def migrate(self, dumped: dict[str, t.Any]) -> dict[str, t.Any]:
        """Execute the plan to upgrade the blueprint to the latest version.

        Returns
        -------
        dict[str, t.Any]
            The raw dictionary of model attributes with schema changes applied.

        Raises
        ------
        CstarMigrationError
            If the migration cannot be completed.
        """
        source, target, plan = self.plan(dumped)
        model = deepcopy(dumped)
        for klass in plan:
            converter = klass(model)
            result = converter.adapt()
            if not result:
                msg = f"Schema migration from {source!r} to {target!r} failed."
                raise CstarMigrationError(msg)
            model = result

        return model
