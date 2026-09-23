"""
Equivalence test: :data:`cstar.roms.namelist_keys.NAMELIST_KEYS` (plus
:data:`cstar.applications.forge.namelist_settings_overlay.NAMELIST_SETTINGS_OVERLAY`
for the Forge side) against the real hand-written classes it was transcribed
from -- :mod:`cstar.roms.namelist` and :mod:`cstar.applications.forge.namelist_model`.

This is step 1 of the table-driven namelist schema migration: the table is a
new, independent fact base, and this test is the proof that it agrees with
the classes that remain authoritative. A discrepancy is always one of two
things:

* a **table transcription error** -- fix the row in ``namelist_keys.py`` /
  ``namelist_settings_overlay.py``.
* a **genuine C-Star/Forge inconsistency** -- leave the classes alone, add it
  to :data:`KNOWN_INCONSISTENCIES` below (with a one-line reason), and report
  it in the migration PR. Loosening a general assertion to paper over a
  mismatch instead of recording it here defeats the point of this test.
"""

from __future__ import annotations

from typing import Any

import annotated_types

from cstar.applications.forge.namelist_model import (
    _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA,
)
from cstar.applications.forge.namelist_settings_overlay import (
    NAMELIST_SETTINGS_OVERLAY,
)
from cstar.roms.namelist import NAMELIST_SCHEMA_REGISTRY, RomsNamelistBase
from cstar.roms.namelist_keys import NAMELIST_KEYS, REQUIRED, NamelistKey

# ---------------------------------------------------------------------------
# Known, deliberate C-Star/Forge inconsistencies the walk below would
# otherwise flag. Each is a (group, key, reason) triple; the per-field
# comparison consults this set and skips exactly these checks, so fixing one
# for real makes its entry here dead -- remove it when that happens.
# ---------------------------------------------------------------------------
KNOWN_INCONSISTENCIES: frozenset[tuple[str, str]] = frozenset(
    {
        # design doc §2: the overlay's rename map is described as "doubling
        # as serialization_alias", but these two renames happen by hand in
        # build_namelist (explicit constructor kwargs) with no
        # serialization_alias behind them at all.
        ("vertical_mixing_settings", "akv_bak"),  # vertical_mixing.akv, hand-mapped
        ("simulation_name_settings", "title"),  # title.casename, hand-mapped
    }
)


def _type_repr(t: Any) -> str:
    """Canonical string form of a type/annotation for `==`-free comparison."""
    if isinstance(t, str):
        return t
    if isinstance(t, type):
        return t.__name__
    return str(t)


def _normalize(v: Any) -> Any:
    """Lists and tuples compare equal regardless of which one a default uses."""
    if isinstance(v, (list, tuple)):
        return tuple(_normalize(x) for x in v)
    return v


def _row_active(row: NamelistKey, probe: tuple[int, int, int] | None) -> bool:
    """Is `row` present in the tier whose lower registry bound is `probe`?

    `probe=None` is the base tier (`RomsNamelist`, ucla-roms < 0.5.0): only
    rows with no `since` are present, and no `until` excludes anything (every
    `until` value in this table is >= the first versioned release, which is
    after "before every version").
    """
    if probe is None:
        return row.since is None
    since_ok = row.since is None or probe >= row.since
    until_ok = row.until is None or probe < row.until
    return since_ok and until_ok


def _expected_groups(
    probe: tuple[int, int, int] | None,
) -> dict[str, list[NamelistKey]]:
    groups: dict[str, list[NamelistKey]] = {}
    for row in NAMELIST_KEYS:
        if _row_active(row, probe):
            groups.setdefault(row.group, []).append(row)
    return groups


# ---------------------------------------------------------------------------
# C-Star side: RomsNamelistBase tiers, taken from NAMELIST_SCHEMA_REGISTRY.
# ---------------------------------------------------------------------------
_CSTAR_TIERS = [(cls, lower) for lower, _upper, cls in NAMELIST_SCHEMA_REGISTRY]


def test_cstar_tiers_match_table_group_order():
    """Every tier's `model_fields` group order matches the table's."""
    for cls, probe in _CSTAR_TIERS:
        expected = list(_expected_groups(probe).keys())
        actual = list(cls.model_fields.keys())
        assert actual == expected, f"{cls.__name__}: group order/set mismatch"


def test_cstar_tiers_match_table_fields():
    """Every tier's group models match the table field-for-field, in order."""
    for cls, probe in _CSTAR_TIERS:
        expected_groups = _expected_groups(probe)
        for group_name, rows in expected_groups.items():
            group_field = cls.model_fields[group_name]
            group_cls = group_field.annotation
            assert isinstance(group_cls, type), (
                f"{cls.__name__}.{group_name}: annotation is not a model class"
            )
            actual_keys = list(group_cls.model_fields.keys())
            expected_keys = [row.key for row in rows]
            assert actual_keys == expected_keys, (
                f"{cls.__name__}.{group_name}: key order/set mismatch"
            )

            validated_fields = {
                f
                for dec in group_cls.__pydantic_decorators__.field_validators.values()
                for f in dec.info.fields
            }

            for row in rows:
                field = group_cls.model_fields[row.key]
                loc = f"{cls.__name__}.{group_name}.{row.key}"

                assert _type_repr(field.annotation) == _type_repr(row.type), (
                    f"{loc}: annotation {field.annotation!r} != table {row.type!r}"
                )

                if row.default is REQUIRED:
                    assert field.is_required(), (
                        f"{loc}: table says REQUIRED, field has a default"
                    )
                else:
                    assert not field.is_required(), (
                        f"{loc}: table has a default, field is required"
                    )
                    actual_default = field.get_default(call_default_factory=True)
                    assert _normalize(actual_default) == _normalize(row.default), (
                        f"{loc}: default {actual_default!r} != table {row.default!r}"
                    )

                assert (field.serialization_alias or row.key) == row.key, (
                    f"{loc}: unexpected serialization_alias "
                    f"{field.serialization_alias!r} (table has no C-Star-side alias)"
                )

                assert field.description == row.doc, (
                    f"{loc}: docstring {field.description!r} != table {row.doc!r}"
                )

                if row.constraint:
                    ge = next(
                        (
                            m.ge
                            for m in field.metadata
                            if isinstance(m, annotated_types.Ge)
                        ),
                        None,
                    )
                    if "ge" in row.constraint:
                        assert ge == row.constraint["ge"], (
                            f"{loc}: ge constraint mismatch"
                        )
                else:
                    ge = next(
                        (
                            m.ge
                            for m in field.metadata
                            if isinstance(m, annotated_types.Ge)
                        ),
                        None,
                    )
                    assert ge is None, (
                        f"{loc}: field has a `ge` constraint the table doesn't record"
                    )

                has_validator = row.key in validated_fields
                assert has_validator == (row.validator is not None), (
                    f"{loc}: field_validator presence ({has_validator}) != "
                    f"table validator={row.validator!r}"
                )


def test_no_duplicate_rows_and_versions_are_real():
    """No two rows for the same (group, key) have overlapping version windows,
    and every `since`/`until` used is a real registry version.
    """
    known_versions = {
        v
        for lower, upper, _ in NAMELIST_SCHEMA_REGISTRY
        for v in (lower, upper)
        if v is not None
    }
    by_group_key: dict[tuple[str, str], list[NamelistKey]] = {}
    for row in NAMELIST_KEYS:
        if row.since is not None:
            assert row.since in known_versions, (
                f"{row.group}.{row.key}: since={row.since} is not a real tier version"
            )
        if row.until is not None:
            assert row.until in known_versions, (
                f"{row.group}.{row.key}: until={row.until} is not a real tier version"
            )
        by_group_key.setdefault((row.group, row.key), []).append(row)

    def _overlaps(a: NamelistKey, b: NamelistKey) -> bool:
        a_lo, a_hi = a.since, a.until
        b_lo, b_hi = b.since, b.until
        lo_ok = a_hi is None or b_lo is None or b_lo < a_hi
        hi_ok = b_hi is None or a_lo is None or a_lo < b_hi
        return lo_ok and hi_ok

    for (group, key), rows in by_group_key.items():
        for i, a in enumerate(rows):
            for b in rows[i + 1 :]:
                assert not _overlaps(a, b), (
                    f"duplicate/overlapping version window for {group}.{key}: "
                    f"({a.since}, {a.until}) vs ({b.since}, {b.until})"
                )


# ---------------------------------------------------------------------------
# Forge side: RunTimeSettings tiers, taken from _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA
# composed with NAMELIST_SCHEMA_REGISTRY (Forge tier -> namelist tier -> version).
# ---------------------------------------------------------------------------
_NS_LOWER_BOUND: dict[type[RomsNamelistBase], tuple[int, int, int] | None] = {
    cls: lower for lower, _upper, cls in NAMELIST_SCHEMA_REGISTRY
}
_FORGE_TIERS = [
    (rt_cls, _NS_LOWER_BOUND[ns_cls])
    for ns_cls, rt_cls in _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA.items()
]
_OVERLAY_BY_GROUP = {row.group: row for row in NAMELIST_SETTINGS_OVERLAY}


def test_every_table_group_has_an_overlay_row():
    table_groups = {row.group for row in NAMELIST_KEYS}
    overlay_groups = set(_OVERLAY_BY_GROUP)
    assert overlay_groups == table_groups


def _row_lookup(
    probe: tuple[int, int, int] | None,
) -> dict[tuple[str, str], NamelistKey]:
    return {
        (row.group, row.key): row for row in NAMELIST_KEYS if _row_active(row, probe)
    }


def test_forge_sections_match_table_via_overlay():
    for rt_cls, probe in _FORGE_TIERS:
        table_rows = _row_lookup(probe)

        for overlay in NAMELIST_SETTINGS_OVERLAY:
            synthetic_names = {s.name for s in overlay.synthetic}

            if not overlay.settings_sections:
                # Bare top-level RunTimeSettings field(s) (gamma2, ubind) --
                # the "section" is the RunTimeSettings model itself, and its
                # one relevant field shares the group's own key name.
                if overlay.group not in rt_cls.model_fields:
                    continue  # e.g. rho0_settings: no top-level field at all
                _check_field(
                    rt_cls.model_fields[overlay.group],
                    field_name=overlay.group,
                    group=overlay.group,
                    overlay=overlay,
                    table_rows=table_rows,
                    synthetic_names=synthetic_names,
                    loc=f"{rt_cls.__name__}.{overlay.group}",
                )
                continue

            for section_name in overlay.settings_sections:
                if section_name not in rt_cls.model_fields:
                    continue  # version-gated section absent from this tier
                section_cls = rt_cls.model_fields[section_name].annotation
                assert isinstance(section_cls, type), (
                    f"{rt_cls.__name__}.{section_name}: annotation is not a model class"
                )
                for field_name, field in section_cls.model_fields.items():
                    _check_field(
                        field,
                        field_name=field_name,
                        group=overlay.group,
                        overlay=overlay,
                        table_rows=table_rows,
                        synthetic_names=synthetic_names,
                        loc=f"{rt_cls.__name__}.{section_name}.{field_name}",
                    )


def _check_field(
    field, *, field_name, group, overlay, table_rows, synthetic_names, loc
):
    if field_name in synthetic_names:
        return  # no namelist-key counterpart to check by design

    if field_name in overlay.hand_mapped:
        assert field.serialization_alias is None, (
            f"{loc}: table says this is hand-mapped (no alias), but a real "
            f"serialization_alias={field.serialization_alias!r} now exists -- "
            f"move it from `hand_mapped` to `renames`"
        )
        canonical_key = overlay.hand_mapped[field_name]
    else:
        if field.serialization_alias is not None:
            assert overlay.renames.get(field_name) == field.serialization_alias, (
                f"{loc}: serialization_alias={field.serialization_alias!r} "
                f"not recorded in overlay.renames"
            )
        canonical_key = field.serialization_alias or field_name

    if (group, canonical_key) in KNOWN_INCONSISTENCIES:
        return

    row = table_rows.get((group, canonical_key))
    assert row is not None, f"{loc}: no table row for ({group}, {canonical_key})"

    pending = field_name in overlay.pending
    if pending:
        assert not field.is_required(), (
            f"{loc}: overlay marks pending, field is required"
        )
        assert field.get_default(call_default_factory=True) is None, (
            f"{loc}: overlay marks pending, default isn't None"
        )
        return  # type/default intentionally loosened (Optional, "set dynamically")

    if field_name in overlay.required_in_forge:
        assert field.is_required(), (
            f"{loc}: overlay marks required_in_forge, but the field has a default"
        )
        return  # required-ness deliberately diverges from the table's default

    if row.default is REQUIRED:
        assert field.is_required(), f"{loc}: table says REQUIRED, field has a default"
    else:
        assert not field.is_required(), f"{loc}: table has a default, field is required"
