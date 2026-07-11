import abc
import typing as t
from collections import defaultdict
from collections.abc import Callable, Sequence
from copy import deepcopy
from pathlib import Path

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
)

from cstar.base.adapter import SchemaAdapter
from cstar.base.env import ENV_CSTAR_CLI_DRY_RUN, ENV_CSTAR_CLOBBER_WORKING_DIR
from cstar.base.feature import is_flag_enabled
from cstar.base.log import LoggingMixin

KEY_SV: t.Final[str] = "schema_version"
KEY_APP: t.Final[str] = "application"


class SchemaBounds(t.TypedDict):
    """Typed dictionary for capturing the min and max schema versions for an app."""

    min: str
    max: str


class CstarMigrationError(Exception):
    """Base class for errors arising from a schema migration."""


class CstarUnsupportedMigrationError(CstarMigrationError):
    """An error that occurs due to an unknown source or target schema version."""


class CStarMigrationNotRegisteredError(CstarMigrationError):
    """An error that occurs due to no registered adapters."""


ConverterMap: t.TypeAlias = dict[tuple[str, str, str], type[SchemaAdapter]]
"""A mapping of (application, source version, target version) keys to adapters."""


class MigrationRequest(BaseModel):
    source: FilePath = Field(
        frozen=True,
        description="Path to a file containing a serialized blueprint",
        alias="path",
    )
    target: Path | None = Field(
        default=None,
        description="Path where the migrated blueprint will be serialized",
        frozen=True,
        alias="output",
    )

    config: t.ClassVar[ConfigDict] = ConfigDict(str_strip_whitespace=True)
    """Model configuration ensuring attributes have whitespace stripped."""

    @classmethod
    def dry_run(cls) -> bool:
        """Return `True` if dry-run is enabled."""
        return is_flag_enabled(ENV_CSTAR_CLI_DRY_RUN)

    @classmethod
    def clobber(cls) -> bool:
        """Return `True` if clobber is enabled."""
        return is_flag_enabled(ENV_CSTAR_CLOBBER_WORKING_DIR)


class MigrationPlan(t.NamedTuple):
    """Results describing the plan that will be used to complete a migration."""

    source: str
    """The version of the schema that the document will be upgraded from."""
    target: str
    """The version of the schema that the document will be upgraded to."""
    adapters: Sequence[type[SchemaAdapter]]
    """An ordered list of adapters that will complete the migration when applied."""

    @property
    def is_latest(self) -> bool:
        return self.source == self.target


class MigrateResult(t.NamedTuple):
    """The results of an executed migration."""

    original: dict[str, t.Any]
    """The original model content."""
    migrated: dict[str, t.Any]
    """The migrated model content."""
    error: str = ""
    """Error(s) causing the migration to fail."""
    plan: MigrationPlan | None = None
    """The migration plan if migration was possible, otherwise `None`."""

    @property
    def application(self) -> str:
        return self.migrated.get(KEY_APP, "")


class Migration(abc.ABC, LoggingMixin):
    """Base class for types that will execute a version migration by executing
    one to many SchemaAdapters.
    """

    @abc.abstractmethod
    def plan(self, dumped: dict[str, t.Any]) -> MigrationPlan:
        """Identify the upgrade path."""
        ...

    @abc.abstractmethod
    def migrate(
        self,
        dumped: dict[str, t.Any],
        plan: MigrationPlan,
    ) -> dict[str, t.Any]:
        """Execute the upgrade path."""
        ...

    @abc.abstractmethod
    def plan_and_migrate(
        self,
        dumped: dict[str, t.Any],
    ) -> MigrateResult:
        """Execute the upgrade path."""
        ...


OnPlannedCallback: t.TypeAlias = Callable[[MigrationPlan], None]
OnMigratedCallback: t.TypeAlias = Callable[[MigrationPlan], None]


class BlueprintMigration(Migration):
    """A migration controller for RomsMarblBlueprints."""

    adapters: list[type[SchemaAdapter]]
    """The adapters available to migrate the blueprint."""
    adapter_lookup: t.Final[ConverterMap]
    """A mapping of unique converter key tuples (app, source, target) to adapters."""
    schema_bounds: dict[str, SchemaBounds]
    """A mapping of unique app names to their minimum and maximum schema version."""

    on_planned_callback: OnPlannedCallback | None = None
    """Callback executed when the migrator completes a plan."""
    on_migrated_callback: OnMigratedCallback | None = None
    """Callback executed when the migrator completes a migration."""

    def __init__(
        self,
        adapters: Sequence[type[SchemaAdapter]],
        schema_bounds: dict[str, SchemaBounds] | None = None,
        on_planned: OnPlannedCallback | None = None,
        on_migrated: OnMigratedCallback | None = None,
    ) -> None:
        self.adapters = list(adapters or [])
        self.schema_bounds = schema_bounds or identify_bounds(self.adapters)
        self.adapter_lookup = self._build_adapter_lookup()
        self.on_planned_callback = on_planned
        self.on_migrated_callback = on_migrated

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
        adapters: list[type[SchemaAdapter]] = []
        application = str(dumped[KEY_APP])

        if application not in self.schema_bounds:
            msg = f"No schema bounds registered for application {application!r}"
            raise CstarUnsupportedMigrationError(msg)

        found_version = str(dumped.get(KEY_SV, "")).strip()
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
            adapters.append(self.adapter_lookup[key])
            _, _, version = key

        if version != goal:
            msg = f"Incomplete migration from {initial_version!r} to {goal!r}"
            raise CstarUnsupportedMigrationError(msg)

        migration_plan = MigrationPlan(initial_version, goal, adapters)
        if self.on_planned_callback:
            self.on_planned_callback(migration_plan)
        return migration_plan

    def migrate(
        self,
        dumped: dict[str, t.Any],
        plan: MigrationPlan,
    ) -> dict[str, t.Any]:
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
        model = deepcopy(dumped)

        for klass in plan.adapters:
            if model := klass(model).adapt():
                model[KEY_SV] = klass.target()
                continue

            msg = f"Schema migration from {plan.source!r} to {plan.target!r} failed."
            raise CstarMigrationError(msg)

        if self.on_migrated_callback:
            self.on_migrated_callback(plan)
        return model

    def plan_and_migrate(self, dumped: dict[str, t.Any]) -> MigrateResult:
        """Create a migration plan and execute it."""
        try:
            plan = self.plan(dumped)
        except CstarUnsupportedMigrationError as ex:
            msg = f"Unable to plan migration: {ex}"
            return MigrateResult(dumped, {}, error=msg)

        if plan.is_latest:
            return MigrateResult(dumped, dumped, plan=plan)

        try:
            migrated = self.migrate(dumped, plan)
        except CstarMigrationError as ex:
            msg = f"Unable to complete migration: {ex}"
            return MigrateResult(dumped, {}, plan=plan, error=msg)

        return MigrateResult(
            dumped,
            migrated,
            plan=plan,
        )


def identify_bounds(
    adapters: Sequence[type[SchemaAdapter]],
) -> dict[str, SchemaBounds]:
    """Given a collection of adapters, identify the migration boundaries (the
    minimum and maximum versions).

    Parameters
    ----------
    adapters : Sequence[type[SchemaAdapter]]
        The adapters to process

    Returns
    -------
    dict[str, SchemaBounds]
        A lookup mapping the application name to the schema bounds.
    """
    app_adapters: dict[str, list[type[SchemaAdapter]]] = defaultdict(list)

    for adapter in adapters:
        app_adapters[adapter.application()].append(adapter)

    schema_bounds: dict[str, SchemaBounds] = {}

    for app_name, adapter_list in app_adapters.items():
        sources = sorted(x.source() for x in adapter_list)
        targets = sorted((x.target() for x in adapter_list), reverse=True)

        vmin = next(iter(sources))
        vmax = next(iter(targets))

        schema_bounds[app_name] = SchemaBounds(min=vmin, max=vmax)
    return schema_bounds
