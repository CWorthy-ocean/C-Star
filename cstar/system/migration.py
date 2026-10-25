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
    PrivateAttr,
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    model_validator,
)

from cstar.base.adapter import SchemaAdapter, SchemaBreak
from cstar.base.application_names import canonical_application
from cstar.base.exceptions import CstarError
from cstar.base.log import LoggingMixin, get_logger

log = get_logger(__name__)

KEY_SV: t.Final[str] = "schema_version"
KEY_APP: t.Final[str] = "application"

TOLERATE_UNKNOWN: t.Final[str] = "cstar_tolerate_unknown"
"""Validation-context key: when true, a same-major newer document whose only
problems are keys this build does not know loads with them ignored, and is
marked so that no writer persists it."""

_ADMITTED: t.Final[str] = "cstar_admitted"
"""Validation-context key set while a newer document is validated against this
build's model, so the validators it re-enters take the plain path."""


class CstarMigrationError(CstarError):
    """Base class for errors arising from a schema migration."""


class CstarUnsupportedMigrationError(CstarMigrationError):
    """An error that occurs when the application is unknown to the migrator."""


class CstarSchemaTooNewError(CstarMigrationError):
    """An error that occurs when a document's schema is newer than this build reads."""

    def __init__(
        self,
        application: str,
        found: str,
        target: str,
        unknown: Sequence[str] = (),
        invalid: Sequence[str] = (),
        *,
        action: str = "read",
    ) -> None:
        self.application = application
        self.found = found
        self.target = target
        self.unknown = tuple(unknown)
        self.invalid = tuple(invalid)
        details = [
            f"sets {', '.join(self.unknown)}, which this build does not know"
            if self.unknown
            else "",
            f"does not validate at {', '.join(self.invalid)}" if self.invalid else "",
        ]
        said = " and ".join(detail for detail in details if detail)
        super().__init__(
            f"{application} schema {found} is newer than this build of "
            f"cstar-ocean reads ({target}){f' and {said}' if said else ''}. "
            f"Upgrade cstar-ocean to {action} this file."
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
    dry_run: bool = Field(
        default=False,
        description="Enable to plan the migration without executing or persisting it",
        frozen=True,
    )

    config: t.ClassVar[ConfigDict] = ConfigDict(str_strip_whitespace=True)
    """Model configuration ensuring attributes have whitespace stripped."""


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
    """A migration controller for UclaRomsBlueprints."""

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
            if version_key(klass.target()) <= version_key(klass.source()):
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
        application: a newer major is refused, the same major version (older
        or newer) is compatible as-is, and an older major version is walked
        forward through adapters.
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
            If the document's major version is newer than the build's.
        CstarManualMigrationError
            If an older major version has no automatic upgrade path.
        """
        application = canonical_application(str(dumped[KEY_APP]))

        if application not in self.targets:
            msg = f"No schema version registered for application {application!r}"
            raise CstarUnsupportedMigrationError(msg)

        # a missing version marks a pre-versioning document; every schema began at 1.0.0
        found = str(dumped.get(KEY_SV) or "").strip() or "1.0.0"
        target = self.targets[application]

        try:
            version_key(found)
        except ValueError as ex:
            msg = f"Unrecognized schema_version {found!r}; expected dotted integers like 1.0.0"
            raise CstarUnsupportedMigrationError(msg) from ex

        # a newer major is refused; a newer minor is compatible, and the
        # blueprint's own validation refuses it if it holds what this build
        # does not know
        newer_in_major(application, found, target)
        if _major(found) == _major(target):
            return MigrationPlan(found, target, [])

        adapters: list[type[SchemaAdapter]] = []
        version = found

        while _major(version) < _major(target):
            klass = self._next_adapter(application, version)
            if klass is None or issubclass(klass, SchemaBreak):
                guidance = self._break_guidance(application, version)
                raise CstarManualMigrationError(application, found, target, guidance)

            adapters.append(klass)
            version = klass.target()

        migration_plan = MigrationPlan(found, target, adapters)
        if self.on_planned_callback:
            self.on_planned_callback(migration_plan)
        return migration_plan

    def _next_adapter(
        self, application: str, version: str
    ) -> type[SchemaAdapter] | None:
        """Return the newest adapter of ``version``'s major that the document has
        reached, or `None` when the document is past every adapter of its major.
        """
        sources = [
            source
            for app, source in self.adapter_lookup
            if app == application
            and _major(source) == _major(version)
            and version_key(source) <= version_key(version)
        ]
        if not sources:
            return None

        klass = self.adapter_lookup[(application, max(sources, key=version_key))]
        if version_key(klass.target()) <= version_key(version):
            return None
        return klass

    def _break_guidance(self, application: str, version: str) -> str:
        """Return the guidance of a `SchemaBreak` registered for ``version``'s
        major, so every older file of a retired major gets the same instructions.
        """
        for (app, source), klass in self.adapter_lookup.items():
            if (
                app == application
                and _major(source) == _major(version)
                and issubclass(klass, SchemaBreak)
            ):
                return klass.guidance()
        return ""

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
        model[KEY_APP] = canonical_application(str(model[KEY_APP]))

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


def newer_in_major(label: str, found: t.Any, target: str) -> bool:
    """Classify a document's schema version against the version this build reads.

    A missing or empty version marks a document written before its kind was
    versioned; every schema began at 1.0.0. A newer major is refused: its
    documents are not this build's to read. A newer minor or patch is additive,
    so the document may still be read; whether it is depends on its content.

    Parameters
    ----------
    label : str
        What the document is called in messages.
    found : t.Any
        The `schema_version` the document holds, or `None` when it has none.
    target : str
        The schema version this build reads and writes.

    Returns
    -------
    bool
        Whether the document is newer than `target` within `target`'s major.

    Raises
    ------
    CstarSchemaTooNewError
        If the document's major version is newer than `target`'s.
    ValueError
        If the document's version is not dotted integers.
    """
    version = str(found or "").strip() or "1.0.0"
    try:
        key = version_key(version)
    except ValueError as ex:
        msg = f"Unrecognized schema_version {version!r}; expected dotted integers like 1.0.0"
        raise ValueError(msg) from ex

    if key[0] > _major(target):
        raise CstarSchemaTooNewError(label, version, target)
    return key > version_key(target)


def stamped_version(found: str, target: str) -> str:
    """Return the schema version a document is written at: never older than it was.

    A document read from a newer file holds only fields this build knows, all of
    which the newer schema has, so it keeps the newer version. A document read
    from an older file may now hold fields of `target`, so it takes `target`;
    fields left at their defaults are not written, so a build that reads the
    older version still reads it.

    Parameters
    ----------
    found : str
        The version the document holds.
    target : str
        The schema version this build writes.

    Returns
    -------
    str
    """
    try:
        newer = version_key(found) > version_key(target)
    except ValueError:  # not a dotted version: the build's own is the safe stamp
        return target
    return found if newer else target


def _failed_locations(ex: ValidationError) -> tuple[list[str], list[str]]:
    """Locate a validation error's problems: the unknown keys, and the rest."""
    unknown: list[str] = []
    invalid: list[str] = []
    for error in ex.errors():
        location = ".".join(str(part) for part in error["loc"]) or "."
        (unknown if error["type"] == "extra_forbidden" else invalid).append(location)
    return unknown, invalid


class VersionedDocument(BaseModel):
    """A persisted document whose `schema_version` decides whether this build reads it.

    A document of an older or the same version, or of an older major, is
    validated as the model says. One of a newer major is refused. One that is
    newer within this build's major is validated with unknown keys forbidden at
    every level, so a key this build does not know is never silently dropped;
    any failure then refuses it as too new, naming what failed. A reader that
    only displays the document may pass `TOLERATE_UNKNOWN` in the validation
    context: a document whose only problems are unknown keys then loads with them
    ignored, and `unknown_keys` names them, so that no writer persists it; the
    reader reports them with `warn_unknown_keys`.

    The subclass declares `schema_version` with the version this build reads as
    its default.
    """

    SCHEMA_LABEL: t.ClassVar[str] = "document"
    """What the document is called in messages."""

    _unknown_keys: tuple[str, ...] = PrivateAttr(default=())

    @property
    def unknown_keys(self) -> tuple[str, ...]:
        """The keys of a newer file this build ignored when it read the document."""
        return self._unknown_keys

    @classmethod
    def schema_label(cls) -> str:
        """Name the document in messages."""
        return cls.SCHEMA_LABEL

    @model_validator(mode="wrap")
    @classmethod
    def _admit_newer_minor(
        cls,
        data: t.Any,
        handler: ValidatorFunctionWrapHandler,
        info: ValidationInfo,
    ) -> t.Any:
        """Validate a newer document strictly, or tolerantly when the reader allows.

        Raises
        ------
        CstarSchemaTooNewError
            If the document's major is newer than this build's, or it is newer
            within the major and fails to validate (other than only by unknown
            keys, for a tolerant reader).
        """
        context = dict(info.context or {})
        if not isinstance(data, Mapping) or context.get(_ADMITTED):
            return handler(data)

        label = cls.schema_label()
        target = str(cls.model_fields[KEY_SV].default)
        found = data.get(KEY_SV)
        if not newer_in_major(label, found, target):
            return handler(data)

        context[_ADMITTED] = True
        try:
            return cls.model_validate(data, extra="forbid", context=context)
        except ValidationError as ex:
            unknown, invalid = _failed_locations(ex)
            if invalid or not context.get(TOLERATE_UNKNOWN):
                raise CstarSchemaTooNewError(
                    label, str(found), target, unknown, invalid
                ) from ex

        document = cls.model_validate(data, extra="ignore", context=context)
        document._unknown_keys = tuple(unknown)
        return document


def _held_unknown_keys(
    value: t.Any,
) -> tuple[VersionedDocument | None, tuple[str, ...]]:
    """Find a document read with unknown keys ignored, in a model or nested in it."""
    if isinstance(value, VersionedDocument) and value.unknown_keys:
        return value, value.unknown_keys
    if isinstance(value, BaseModel):
        value = list(value.__dict__.values())
    elif isinstance(value, Mapping):
        value = list(value.values())
    if isinstance(value, list | tuple):
        for item in value:
            found = _held_unknown_keys(item)
            if found[0] is not None:
                return found
    return None, ()


def holds_unknown_keys(model: t.Any) -> bool:
    """Whether a model, or a document nested in it, was read with unknown keys ignored.

    Parameters
    ----------
    model : t.Any
        The model to inspect.

    Returns
    -------
    bool
    """
    return _held_unknown_keys(model)[0] is not None


def warn_unknown_keys(model: t.Any, where: str | Path) -> None:
    """Warn that a document was read with keys a newer cstar-ocean wrote ignored.

    Parameters
    ----------
    model : t.Any
        The document just read.
    where : str | Path
        The file it was read from.
    """
    document, unknown = _held_unknown_keys(model)
    if document is None:
        return
    target = type(document).model_fields[KEY_SV].default
    log.warning(
        f"The {type(document).schema_label()} in {where} is schema "
        f"{getattr(document, KEY_SV)}, newer than this build of cstar-ocean reads "
        f"({target}); ignoring {', '.join(unknown)}, which this build does not "
        "know. This build will not write the file; upgrade cstar-ocean to change it."
    )


def refuse_unwritable(model: t.Any) -> None:
    """Refuse to write a document that holds content read with unknown keys ignored.

    Writing it would drop the keys while the file kept claiming the newer schema.

    Parameters
    ----------
    model : t.Any
        The model about to be written.

    Raises
    ------
    CstarSchemaTooNewError
        If `model`, or a document nested in it, was read with unknown keys ignored.
    """
    document, unknown = _held_unknown_keys(model)
    if document is None:
        return
    raise CstarSchemaTooNewError(
        type(document).schema_label(),
        str(getattr(document, KEY_SV)),
        str(type(document).model_fields[KEY_SV].default),
        unknown,
        action="write",
    )


def version_key(version: str) -> tuple[int, ...]:
    """Return a numeric sort key for a dotted-integer schema version.

    Schema versions must compare numerically per component: lexicographic
    string comparison would order ``"10.0.0"`` before ``"2.0.0"``.

    Parameters
    ----------
    version : str
        A dotted version of integers.

    Returns
    -------
    tuple[int, ...]
    """
    return tuple(int(part) for part in version.split("."))


def _major(version: str) -> int:
    """Return the major component of a dotted-integer schema version."""
    return version_key(version)[0]
