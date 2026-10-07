import abc
import typing as t
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from pathlib import Path

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
)

from cstar.base.adapter import SchemaAdapter, SchemaBreak
from cstar.base.env import ENV_CSTAR_CLI_DRY_RUN, ENV_CSTAR_CLOBBER_WORKING_DIR
from cstar.base.feature import is_flag_enabled
from cstar.base.log import LoggingMixin

KEY_SV: t.Final[str] = "schema_version"
KEY_APP: t.Final[str] = "application"


class CstarMigrationError(Exception):
    """Base class for errors arising from a schema migration."""


class CstarUnsupportedMigrationError(CstarMigrationError):
    """An error that occurs when the application is unknown to the migrator."""


class CstarSchemaTooNewError(CstarMigrationError):
    """An error that occurs when a document's schema is newer than this build reads."""

    def __init__(self, application: str, found: str, target: str) -> None:
        self.application = application
        self.found = found
        self.target = target
        super().__init__(
            f"{application} schema {found} is newer than this build of "
            f"cstar-ocean reads ({target}). Upgrade cstar-ocean to read this file."
        )


class CstarManualMigrationError(CstarMigrationError):
    """An error that occurs when an older major version has no automatic migration."""

    def __init__(
        self, application: str, found: str, target: str, guidance: str = ""
    ) -> None:
        self.application = application
        self.found = found
        self.target = target
        guidance = guidance or (
            f"Update the blueprint by hand to the {target} schema (published under "
            "docs/schemas), then validate it with: cstar blueprint check <path>"
        )
        super().__init__(
            f"{application} schema {found} has no automatic migration to {target} "
            f"from {_major(found)}.x. {guidance}"
        )


ConverterMap: t.TypeAlias = dict[tuple[str, str], type[SchemaAdapter]]
"""A mapping of (application, source version) keys to adapters."""


class MigrationRequest(BaseModel):
    """User-supplied parameters describing a blueprint schema migration."""

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
    in_place: bool = Field(
        default=False,
        description="Enable to overwrite the source file with the migrated content",
        frozen=True,
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
    def is_compatible(self) -> bool:
        """True when the document needs no adapters because its major version
        matches the build's.
        """
        return not self.adapters


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
    """A mapping of unique converter key tuples (app, source) to adapters."""
    targets: Mapping[str, str]
    """A mapping of application names to the schema version this build reads."""

    on_planned_callback: OnPlannedCallback | None = None
    """Callback executed when the migrator completes a plan."""
    on_migrated_callback: OnMigratedCallback | None = None
    """Callback executed when the migrator completes a migration."""

    def __init__(
        self,
        adapters: Sequence[type[SchemaAdapter]],
        targets: Mapping[str, str],
        on_planned: OnPlannedCallback | None = None,
        on_migrated: OnMigratedCallback | None = None,
    ) -> None:
        self.adapters = list(adapters or [])
        self.targets = targets
        self.adapter_lookup = self._build_adapter_lookup()
        self.on_planned_callback = on_planned
        self.on_migrated_callback = on_migrated

    def _build_adapter_lookup(self) -> ConverterMap:
        """Build the mapping that enables the lookup of adapters.

        Returns
        -------
        ConverterMap

        Raises
        ------
        ValueError
            If an adapter does not advance the version, or two adapters share
            an (application, source) key.
        """
        results: ConverterMap = {}
        for klass in self.adapters:
            key = (klass.application(), klass.source())
            if _version_key(klass.target()) <= _version_key(klass.source()):
                msg = (
                    f"Adapter {klass.__name__} must advance the schema version, "
                    f"not {klass.source()!r} -> {klass.target()!r}"
                )
                raise ValueError(msg)
            if key in results:
                msg = (
                    f"Adapters {results[key].__name__} and {klass.__name__} share {key}"
                )
                raise ValueError(msg)
            results[key] = klass
        return results

    def plan(self, dumped: dict[str, t.Any]) -> MigrationPlan:
        """Determine the available upgrade path.

        A document is classified against the build's schema version for its
        application: newer is refused, the same major version is compatible
        as-is, and an older major version is walked forward through adapters.
        `plan` assumes only 1 mapping for any source schema version exists. It
        will not backtrack to locate an alternative upgrade path if an adapter
        cannot traverse to the goal state and becomes stuck.

        Returns
        -------
        MigrationPlan
            NamedTuple containing (source version, target version, adapters)

        Raises
        ------
        CstarUnsupportedMigrationError
            If the application is unknown to the migrator, or the document's
            schema version is not dotted integers.
        CstarSchemaTooNewError
            If the document's schema is newer than the build's.
        CstarManualMigrationError
            If an older major version has no automatic upgrade path.
        """
        application = str(dumped[KEY_APP])

        if application not in self.targets:
            msg = f"No schema version registered for application {application!r}"
            raise CstarUnsupportedMigrationError(msg)

        # a missing version marks a pre-versioning document; every schema began at 1.0.0
        found = str(dumped.get(KEY_SV) or "").strip() or "1.0.0"
        target = self.targets[application]

        try:
            _version_key(found)
        except ValueError as ex:
            msg = f"Unrecognized schema_version {found!r}; expected dotted integers like 1.0.0"
            raise CstarUnsupportedMigrationError(msg) from ex

        if _version_key(found) > _version_key(target):
            raise CstarSchemaTooNewError(application, found, target)

        if _major(found) == _major(target):
            return MigrationPlan(found, target, [])

        candidates = [k for k in self.adapter_lookup if k[0] == application]
        adapters: list[type[SchemaAdapter]] = []
        version = found

        while _major(version) < _major(target):
            # the newest adapter of this major that the document has reached
            sources = [
                source
                for _, source in candidates
                if _major(source) == _major(version)
                and _version_key(source) <= _version_key(version)
            ]
            if not sources:
                raise CstarManualMigrationError(application, found, target)

            klass = self.adapter_lookup[(application, max(sources, key=_version_key))]
            if _version_key(klass.target()) <= _version_key(version):
                # the document is already past this adapter's target
                raise CstarManualMigrationError(application, found, target)

            if issubclass(klass, SchemaBreak):
                raise CstarManualMigrationError(
                    application, found, target, klass.guidance()
                )

            adapters.append(klass)
            version = klass.target()

        migration_plan = MigrationPlan(found, target, adapters)
        if self.on_planned_callback:
            self.on_planned_callback(migration_plan)
        return migration_plan

    def migrate(
        self,
        dumped: dict[str, t.Any],
        plan: MigrationPlan,
    ) -> MigrateResult:
        """Execute the plan to upgrade the blueprint to the latest version.

        Parameters
        ----------
        dumped : dict[str, t.Any]
            The raw dictionary of model attributes to migrate.
        plan : MigrationPlan
            The plan describing the adapters to apply.

        Returns
        -------
        MigrateResult
            Named tuple containing the original and migrated model attributes
            along with the executed plan.

        Raises
        ------
        CstarMigrationError
            If the migration cannot be completed.
        """
        model = deepcopy(dumped)

        for klass in plan.adapters:
            if model := klass(model).adapt():
                continue

            msg = f"Schema migration from {plan.source!r} to {plan.target!r} failed."
            raise CstarMigrationError(msg)

        # the last adapter may stop at an older minor of the build's major
        model[KEY_SV] = plan.target

        if self.on_migrated_callback:
            self.on_migrated_callback(plan)

        return MigrateResult(
            dumped,
            model,
            plan=plan,
        )


def _version_key(version: str) -> tuple[int, ...]:
    """Return a numeric sort key for a dotted-integer schema version.

    Schema versions must compare numerically per component: lexicographic
    string comparison would order ``"10.0.0"`` before ``"2.0.0"``.
    """
    return tuple(int(part) for part in version.split("."))


def _major(version: str) -> int:
    """Return the major component of a dotted-integer schema version."""
    return _version_key(version)[0]
