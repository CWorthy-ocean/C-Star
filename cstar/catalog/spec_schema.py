"""Schema versions of catalog spec files, and the one path every spec is read through.

Each spec kind has its own `schema_version`, semver read from an older file's
point of view as for blueprints: a file without one is ``1.0.0``; a major bump
means older files need a migration, registered as a `SchemaAdapter` in
``cstar.applications.forge.migration.SPEC_MIGRATIONS``; a minor bump only adds
top-level keys. `read_spec` applies that rule to a spec's parsed content, and
writers stamp `SPEC_SCHEMA_VERSIONS`. Reading never writes the file:
``cstar admin migrate-catalog`` does that.
"""

from __future__ import annotations

import typing as t
from collections.abc import Mapping
from copy import deepcopy
from functools import cache
from pathlib import Path

from cstar.base.log import get_logger
from cstar.system.migration import (
    KEY_APP,
    KEY_SV,
    BlueprintMigration,
    CstarSchemaTooNewError,
    refuse_newer_major,
    schema_version_of,
    version_key,
)

if t.TYPE_CHECKING:
    from cstar.orchestration.models import CatalogSpecKind

log = get_logger(__name__)

SPEC_SCHEMA_VERSIONS: t.Final[Mapping[str, str]] = {
    "ModelSpec": "2.0.0",
    "DomainSpec": "1.0.0",
    "ForcingSpec": "2.0.0",
    "OutputSpec": "2.0.0",
    "CdrSpec": "1.0.0",
}
"""The schema version this build reads and writes, per spec kind
(`CatalogSpecKind`)."""

DOMAIN_SPEC_KEYS: t.Final[frozenset[str]] = frozenset(
    {
        "description",
        "model_name",
        "grid_name",
        "start_time",
        "end_time",
        "grid_kwargs",
        "grid_kwargs_parent",
        "grid_kwargs_child",
        "metadata_child",
        "nesting_include_pressure_fluxes",
        "open_boundaries",
        "partitioning",
        "v_sponge",
        "dt",
        "topography_source",
        "topography_path",
        "_parent_grid_name",
        "_child_grid_name",
    }
)
"""The top-level keys of a DomainSpec, which has no model of its own."""

FORCING_SPEC_KEYS: t.Final[frozenset[str]] = frozenset(
    {
        "description",
        "initial_conditions",
        "forcing",
        "cdr_forcing",
        "river_frc",
        "cdr_frc",
    }
)
"""The top-level keys of a ForcingSpec."""

_STRICT_KINDS: t.Final[frozenset[str]] = frozenset({"ModelSpec"})
"""Kinds whose unknown top-level keys are refused at every version, not only in a
newer file: their model forbids extra keys, so one would otherwise be dropped."""

_WARNED: set[tuple[str, str]] = set()
"""(file, version) pairs `read_spec` has warned about: a wizard rebuild reads the
same spec many times, and the user needs to hear about it once per process."""


class MigratedSpec(t.NamedTuple):
    """A spec's content at its kind's current schema."""

    content: dict[str, t.Any]
    """The content, without `schema_version`."""
    found: str
    """The version the file holds."""
    target: str
    """The version `content` is at."""
    changed: bool
    """Whether a migration changed the content (not only its version)."""


@cache
def known_spec_keys(kind: CatalogSpecKind) -> frozenset[str]:
    """Return the top-level keys a spec of `kind` may hold in this build.

    Taken from the model that owns the content where there is one; imported
    here so that only a spec that needs the check pays for the import.
    """
    if kind == "ModelSpec":
        from cstar.applications.forge.models import ModelSpec

        return frozenset(ModelSpec.model_fields) - {"name"}
    if kind == "CdrSpec":
        from cstar.applications.forge.models import CdrSpec

        return frozenset(CdrSpec.model_fields) | {"description"}
    if kind == "OutputSpec":
        from cstar.applications.forge.resolve import (
            OUTPUT_SECTIONS,
            PARTIAL_OUTPUT_SECTIONS,
        )

        return frozenset({"description", *OUTPUT_SECTIONS, *PARTIAL_OUTPUT_SECTIONS})
    if kind == "ForcingSpec":
        return FORCING_SPEC_KEYS
    return DOMAIN_SPEC_KEYS


def migrate_spec(
    kind: CatalogSpecKind, data: Mapping[str, t.Any], where: str | Path
) -> MigratedSpec:
    """Bring a spec's parsed content to its kind's current schema.

    A file of an older major goes through the registered migrations. One of a
    newer major is refused; one newer within the major is read as it is, unless
    it holds a top-level key this build does not know.

    Parameters
    ----------
    kind : CatalogSpecKind
        The spec's kind.
    data : Mapping[str, t.Any]
        The spec file's parsed content; not changed.
    where : str | Path
        The file (or argument) the content came from, for messages.

    Returns
    -------
    MigratedSpec

    Raises
    ------
    CstarSchemaTooNewError
        If the file is of a newer major, or newer within the major and holds a
        key this build does not know.
    ValueError
        If the version is not dotted integers, the content is not a mapping, a
        migration refuses it, or a ModelSpec holds a key a ModelSpec does not have.
    """
    if not isinstance(data, Mapping):
        raise ValueError(f"{kind} {where} is not a mapping of keys to values")
    label = f"{kind} {where}"
    target = SPEC_SCHEMA_VERSIONS[kind]
    found = schema_version_of(data.get(KEY_SV))
    original = {key: value for key, value in data.items() if key != KEY_SV}

    refuse_newer_major(label, found, target)
    if version_key(found) > version_key(target):
        if unknown := sorted(set(original) - known_spec_keys(kind)):
            raise CstarSchemaTooNewError(label, found, target, unknown)
        return MigratedSpec(original, found, target, False)

    content = original
    if version_key(found)[0] < version_key(target)[0]:
        # The planner reads the document's name from its application key.
        from cstar.applications.forge.migration import SPEC_MIGRATIONS

        migration = BlueprintMigration(SPEC_MIGRATIONS, SPEC_SCHEMA_VERSIONS)
        dumped = {**deepcopy(original), KEY_APP: kind, KEY_SV: found}
        migrated = migration.migrate(dumped, migration.plan(dumped)).migrated
        content = {k: v for k, v in migrated.items() if k not in (KEY_APP, KEY_SV)}

    if kind in _STRICT_KINDS and (
        unknown := sorted(set(content) - known_spec_keys(kind))
    ):
        raise ValueError(
            f"{label} sets {', '.join(unknown)}, which a {kind} does not have; "
            f"its keys are {', '.join(sorted(known_spec_keys(kind)))}."
        )
    return MigratedSpec(content, found, target, content != original)


def read_spec(
    kind: CatalogSpecKind, data: Mapping[str, t.Any], where: str | Path
) -> dict[str, t.Any]:
    """Return a spec's content at its kind's current schema, without `schema_version`.

    Every reader of a spec goes through here (see `migrate_spec` for the rule).
    When a migration changed the content, warns once per file per process; the
    file itself is not changed.

    Parameters
    ----------
    kind : CatalogSpecKind
        The spec's kind.
    data : Mapping[str, t.Any]
        The spec file's parsed content; not changed.
    where : str | Path
        The file (or argument) the content came from, for messages.

    Returns
    -------
    dict[str, t.Any]
    """
    spec = migrate_spec(kind, data, where)
    if spec.changed and (str(where), spec.found) not in _WARNED:
        _WARNED.add((str(where), spec.found))
        # only a file can be updated on disk; `where` may name an argument
        update = (
            " Run `cstar admin migrate-catalog` on its catalog to update the file."
            if isinstance(where, Path)
            else ""
        )
        log.warning(
            f"{kind} {where} is schema {spec.found}; it was migrated to "
            f"{spec.target} on read.{update}"
        )
    return spec.content
