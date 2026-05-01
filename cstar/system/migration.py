import abc
import typing as t
from collections.abc import Mapping

from cstar.base.adapter import ModelAdapter


class SchemaMigration(ModelAdapter[dict[str, t.Any], dict[str, t.Any]]):
    """Contract exposing a mechanism to adapt a source model to a target type."""

    def __init__(
        self,
        model: dict[str, str],
    ) -> None:
        super().__init__(model)

    @property
    @abc.abstractmethod
    def source(self) -> str:
        """The supported version of the input model."""
        ...

    @property
    @abc.abstractmethod
    def target(self) -> str:
        """The version of the output model."""
        ...


class BlueprintVersionAdapter2025v1(SchemaMigration):
    """Convert RomsMarblBlueprint from the 2025.1 schema into the 2026.1 schema."""

    @property
    def source(self) -> str:
        """The supported version of the input model."""
        return "2025.1"

    @property
    def target(self) -> str:
        """The version of the output model."""
        return "2026.1"

    @t.override
    def adapt(self) -> dict[str, str]:
        runtime_params = t.cast(
            "dict[str, str]",
            self.model.get("runtime_params", {"output_dir": None}),
        )
        if output_dir := runtime_params.pop("output_dir", None):
            self.model["output_dir"] = output_dir
        return {**self.model}


RawModelVersionAdapterType: t.TypeAlias = type[SchemaMigration]
"""An adapter that converts the content of a dumped model into another version."""
ConverterMap: t.TypeAlias = Mapping[tuple[str, str], RawModelVersionAdapterType]
"""A mapping of (source version, target version) keys to adapters."""


class ConversionPlan(t.NamedTuple):
    """Results describing the plan that will be used to complete a migration."""

    source: str
    """The version of the schema that the document will be upgraded from."""
    target: str
    """The version of the schema that the document will be upgraded to."""
    plan: list[RawModelVersionAdapterType]
    """The ordered list of adapaters to apply to complete the migration."""


class AppAwareMetaclass(abc.ABCMeta):
    """Metaclass used to add a class-level `application` attribute."""

    _application: t.ClassVar[str] = ""

    @property
    def application(cls) -> str:
        return cls._application


class Migration(metaclass=AppAwareMetaclass):
    @abc.abstractmethod
    def plan(self, dumped: dict[str, t.Any]) -> ConversionPlan:
        """Identify the upgrade path."""
        ...

    @abc.abstractmethod
    def adapt(self, dumped: dict[str, t.Any]) -> dict[str, t.Any]:
        """Execute the upgrade path."""
        ...


class CstarMigrationError(Exception): ...


class CstarUnsupportedMigrationError(CstarMigrationError): ...


class RomsBlueprintMigration(Migration):
    _application: t.Literal["roms_marbl"] = "roms_marbl"
    """The registered application name for the blueprint."""
    converters: list[RawModelVersionAdapterType]
    """The registered migrations for the blueprint."""
    INITIAL: t.ClassVar[t.Literal["2025.1"]] = "2025.1"
    """The default version number to use if the blueprint omits it."""
    LATEST: t.ClassVar[t.Literal["2026.1"]] = "2026.1"
    """The schema version the migrator currently targets."""

    map: t.Final[ConverterMap]

    def __init__(
        self,
        converters: list[RawModelVersionAdapterType] | None = None,
    ) -> None:
        self.converters = converters or [BlueprintVersionAdapter2025v1]
        self.map = self._build_map()

    def _build_map(self) -> ConverterMap:
        results: dict[tuple[str, str], t.Any] = {}
        for klass in self.converters:
            converter = klass({})
            source, target = converter.source, converter.target
            results[(source, target)] = klass
        return results

    def plan(self, dumped: dict[str, t.Any]) -> ConversionPlan:
        """Execute the upgrade path.

        Raises
        ------
        CstarUnsupportedMigrationError
            If unable to identify a complete migration upgrade path.
        """
        plan: list[RawModelVersionAdapterType] = []
        start_at_version = t.cast(
            "str", dumped.get("version", RomsBlueprintMigration.INITIAL)
        )
        limit = 20
        current_version = start_at_version

        while limit > 0 and current_version != self.LATEST:
            limit -= 1
            options = [(x, y) for (x, y) in self.map if x == current_version]
            if not options:
                msg = f"No migration adapter from {start_at_version!r} to {self.LATEST}"
                raise CstarUnsupportedMigrationError(msg)

            # TODO: either use recursion to pop back to a path or use networkx
            opt_source, opt_target = options.pop(0)
            current_version = opt_target

            adapter_type = self.map[(opt_source, opt_target)]
            plan.append(adapter_type)

        is_planned = current_version != RomsBlueprintMigration.LATEST
        if is_planned:
            msg = f"Incomplete migration from {start_at_version!r} to {self.LATEST}"
            raise CstarUnsupportedMigrationError(msg)

        return ConversionPlan(start_at_version, self.LATEST, plan)

    def adapt(self, dumped: dict[str, t.Any]) -> dict[str, t.Any]:
        """Execute the plan to upgrade the blueprint to the latest version.

        Raises
        ------
        CstarMigrationError
            If unable to complete the migration.
        """
        v_source, v_target, plan = self.plan(dumped)
        model = dumped
        for klass in plan:
            converter = klass(dumped)
            result = converter.adapt()
            if not result:
                msg = f"Schema migration from {v_source!r} to {v_target!r} failed."
                raise CstarMigrationError(msg)
            model = result

        return model
