"""Tests for the wizard label glossary (cstar.wizard.ui.labels)."""

from __future__ import annotations

import pytest

pytest.importorskip("ipywidgets")

import ipywidgets as W

from cstar.wizard.ui import labels
from cstar.wizard.wizard import ForgeBlueprintWizard, _ForcingEditor

# The 11 grid kwargs shared by the main/child/parent grids (see
# cstar.wizard.wizard._GRID_INT/_GRID_FLOAT/_SCOORD).
_GRID_KWARGS = (
    "nx",
    "ny",
    "N",
    "size_x",
    "size_y",
    "center_lon",
    "center_lat",
    "rot",
    "theta_s",
    "theta_b",
    "hc",
)

# The ic.*/boundary.* attributes of _ForcingEditor.
_IC_ATTRS = (
    "ic_name",
    "ic_layout",
    "ic_path",
    "ic_bgc_interp",
    "ic_flex_time",
    "ic_validate",
    "ic_prefill",
    "ic_regrid_method",
    "ic_extrap_method",
    "ic_options",
)
_BOUNDARY_ATTRS = (
    "boundary_name",
    "boundary_layout",
    "boundary_path",
    "boundary_bgc_interp",
    "boundary_prefill",
    "boundary_regrid_method",
    "boundary_extrap_method",
    "boundary_validate",
    "boundary_options",
)

# Section ids required by the WP1 spec.
_REQUIRED_SECTION_IDS = (
    "start",
    "model",
    "grid",
    "forcing",
    "run",
    "advanced",
    "review",
    "grid.geometry",
    "grid.vertical",
    "grid.bathymetry",
    "grid.gridfile",
    "grid.derived",
    "grid.nesting",
    "run.window",
    "run.partitioning",
    "run.cdr",
    "review.save_specs",
    "review.run",
    "review.workplan",
)


@pytest.fixture(scope="module")
def wizard():
    """A real ``ForgeBlueprintWizard`` (construction takes a few seconds)."""
    return ForgeBlueprintWizard()


@pytest.fixture(scope="module")
def forcing_editor():
    """A bare ``_ForcingEditor`` -- mirrors the ``editor`` fixture in
    tests/test_forge_blueprint_wizard.py.
    """
    return _ForcingEditor(W, {}, on_change=lambda: None)


def test_glossary_loads():
    g = labels.glossary()
    assert "sections" in g
    assert "fields" in g


def test_known_keys_non_empty():
    assert len(labels.known_keys()) > 0
    assert len(labels.known_sections()) > 0


def test_parse_rejects_unknown_field_key():
    with pytest.raises(ValueError, match="unknown key"):
        labels._parse({"fields": {"bad": {"label": "Bad", "nope": 1}}})


def test_parse_rejects_unknown_section_key():
    with pytest.raises(ValueError, match="unknown key"):
        labels._parse({"sections": {"bad": {"title": "Bad", "nope": 1}}})


def test_parse_rejects_missing_label():
    with pytest.raises(ValueError, match="label"):
        labels._parse({"fields": {"bad": {"symbol": "bad"}}})


def test_parse_rejects_missing_title():
    with pytest.raises(ValueError, match="title"):
        labels._parse({"sections": {"bad": {"desc": "no title"}}})


def test_parse_rejects_non_mapping_entry():
    with pytest.raises(ValueError, match="must be a mapping"):
        labels._parse({"fields": {"bad": "not a dict"}})


def test_parse_accepts_well_formed_entries():
    parsed = labels._parse(
        {
            "sections": {"s": {"title": "S", "desc": "d", "required": True}},
            "fields": {"f": {"label": "F", "symbol": "f", "required": True}},
        }
    )
    assert parsed["sections"]["s"].title == "S"
    assert parsed["fields"]["f"].label == "F"


def test_label_for_missing_key_falls_back_to_last_segment():
    lab = labels.label_for("some.namespaced.key")
    assert lab.label == "key"
    assert lab.required is False


def test_label_for_missing_key_uses_default():
    lab = labels.label_for("totally_unknown_key", default="Fallback label")
    assert lab.label == "Fallback label"


def test_section_for_missing_key_falls_back_to_key():
    sec = labels.section_for("totally.unknown.section")
    assert sec.title == "totally.unknown.section"


def test_section_for_missing_key_uses_default_title():
    sec = labels.section_for("totally.unknown.section", default_title="Fallback")
    assert sec.title == "Fallback"


def test_every_bare_field_key_is_a_wizard_attribute(wizard):
    fields = labels.glossary()["fields"]
    bare_keys = [k for k in fields if "." not in k]
    assert bare_keys, "expected at least one bare (non-namespaced) field key"
    for key in bare_keys:
        assert hasattr(wizard, key), f"ForgeBlueprintWizard has no attribute {key!r}"


def test_every_grid_key_is_one_of_the_11_grid_kwargs():
    fields = labels.glossary()["fields"]
    grid_keys = [k.split(".", 1)[1] for k in fields if k.startswith("grid.")]
    assert grid_keys, "expected at least one grid.<k> field key"
    for k in grid_keys:
        assert k in _GRID_KWARGS, f"grid.{k} is not one of the 11 grid kwargs"


def test_every_ic_key_is_a_forcing_editor_attribute(forcing_editor):
    fields = labels.glossary()["fields"]
    ic_keys = [k.split(".", 1)[1] for k in fields if k.startswith("ic.")]
    assert ic_keys, "expected at least one ic.* field key"
    for k in ic_keys:
        assert hasattr(forcing_editor, k), f"_ForcingEditor has no attribute {k!r}"
        assert k in _IC_ATTRS, f"ic.{k} is not in the documented ic.* attribute set"


def test_every_boundary_key_is_a_forcing_editor_attribute(forcing_editor):
    fields = labels.glossary()["fields"]
    boundary_keys = [k.split(".", 1)[1] for k in fields if k.startswith("boundary.")]
    assert boundary_keys, "expected at least one boundary.* field key"
    for k in boundary_keys:
        assert hasattr(forcing_editor, k), f"_ForcingEditor has no attribute {k!r}"
        assert k in _BOUNDARY_ATTRS, (
            f"boundary.{k} is not in the documented boundary.* attribute set"
        )


def test_all_required_section_ids_present():
    known = labels.known_sections()
    missing = [s for s in _REQUIRED_SECTION_IDS if s not in known]
    assert not missing, f"missing section ids: {missing}"


def test_required_top_level_sections_flagged_required():
    for key in ("model", "grid", "forcing", "run"):
        assert labels.section_for(key).required is True
    for key in ("start", "advanced", "review"):
        assert labels.section_for(key).required is False


# ---------------------------------------------------------------------------
# WP3 (inner editors): forcing.row.*, settings.<section>.<field>, section ids.
# ---------------------------------------------------------------------------

# The row categories _ForcingEditor._make_row's generic add/remove machinery
# manages (see cstar.wizard.wizard._ROW_CATEGORIES). "boundary"
# itself is excluded -- boundary's physics source is a required scalar (the
# boundary.* pane), not a row list; only "boundary_bgc" is row-based.
_ROW_CATEGORY_SEEDS: dict[str, dict] = {
    "ic_bgc": {"source": {"name": "UNIFIED"}},
    "boundary_bgc": {"source": {"name": "UNIFIED"}},
    "surface": {"type": "physics", "source": {"name": "ERA5"}},
    "tidal": {"source": {"name": "TPXO"}},
    "river": {"source": {"name": "DAI"}},
}


def _all_row_widget_keys(forcing_editor) -> set[str]:
    """Union of every widget-dict key ``_make_row`` produces across every row
    category, built from one minimal seed item per category (see
    ``_ROW_CATEGORY_SEEDS``). Every conditionally-built widget (e.g.
    "glorys_layout" for "surface", "constants"/"esper_method" for the bgc row
    categories) is unconditional on the *seed content* -- only on the
    category -- so a single row per category is enough to enumerate the full
    set. Keys starting with "_" (``_remove_btn``, ``_custom_file``, ...) are
    plain state/buttons, not glossary-backed field widgets, and are excluded,
    as are the river custom-file attach/upload/status widgets: the Attach
    button's caption comes from ``buttons.attach`` (see deliverable 4, not
    ``forcing.row.*``), and the upload control/status HTML carry no
    field-style label at all.
    """
    non_row_keys = {
        "custom_file_attach_btn",
        "custom_file_upload",
        "custom_file_status",
    }
    keys: set[str] = set()
    for cat, seed in _ROW_CATEGORY_SEEDS.items():
        w = forcing_editor._make_row(cat, seed)
        keys.update(k for k in w if not k.startswith("_") and k not in non_row_keys)
    return keys


def test_every_forcing_row_key_is_produced_by_make_row(forcing_editor):
    """Every ``fields.forcing.row.<key>`` glossary entry names a key
    ``_make_row`` actually builds, and every key it builds has a glossary
    entry -- keeps the YAML from drifting from the real widget-dict keys
    (see the WP3 punchlist: several drafted keys, e.g. "layout"/"corr_rad",
    didn't match the real "glorys_layout"/"correct_radiation" keys).
    """
    fields = labels.glossary()["fields"]
    glossary_keys = {k.split(".", 2)[2] for k in fields if k.startswith("forcing.row.")}
    produced_keys = _all_row_widget_keys(forcing_editor)
    missing_from_glossary = produced_keys - glossary_keys
    assert not missing_from_glossary, (
        f"_make_row builds these keys with no forcing.row.* glossary entry: "
        f"{sorted(missing_from_glossary)}"
    )
    stale_in_glossary = glossary_keys - produced_keys
    assert not stale_in_glossary, (
        f"forcing.row.* glossary entries for keys _make_row never builds: "
        f"{sorted(stale_in_glossary)}"
    )


# Settings sections with no RunTimeSettings-tier model at all -- rendered via
# plain value-type inference (see _VERSION_GATED_SECTIONS/_base_type), so
# there is no pydantic sub-model to validate their field keys against.
_UNMODELED_SETTINGS_SECTIONS = frozenset({"cppdefs"})


def _all_settings_section_field_names() -> dict[str, set[str]]:
    """``{section: {field_name, ...}}`` unioned across every registered
    ``RunTimeSettings`` tier (a version-gated section like ``pio_settings``
    only exists on some tiers, e.g. ``RunTimeSettingsV0_6_0``).
    """
    from cstar.applications.forge.namelist_model import (
        RunTimeSettings,
        RunTimeSettingsV0_5_0,
        RunTimeSettingsV0_6_0,
        RunTimeSettingsV0_7_0,
    )
    from cstar.wizard.wizard import _unwrap_type

    tiers = (
        RunTimeSettings,
        RunTimeSettingsV0_5_0,
        RunTimeSettingsV0_6_0,
        RunTimeSettingsV0_7_0,
    )
    sections: dict[str, set[str]] = {}
    for cls in tiers:
        for section, info in cls.model_fields.items():
            sub = _unwrap_type(info.annotation)
            sections.setdefault(section, set()).update(getattr(sub, "model_fields", {}))
    return sections


def test_every_settings_section_id_names_a_real_section():
    section_fields = _all_settings_section_field_names()
    section_ids = [
        s
        for s in labels.known_sections()
        if s.startswith("settings.") and s.count(".") == 1
    ]
    assert section_ids
    for section_id in section_ids:
        section = section_id.split(".", 1)[1]
        if section in _UNMODELED_SETTINGS_SECTIONS:
            continue
        assert section in section_fields, (
            f"sections.{section_id}: {section!r} is not a RunTimeSettings section"
        )


def test_every_settings_field_key_names_a_real_namelist_field():
    section_fields = _all_settings_section_field_names()
    fields = labels.glossary()["fields"]
    field_keys = [k for k in fields if k.startswith("settings.") and k.count(".") == 2]
    assert field_keys
    for key in field_keys:
        _, section, field_name = key.split(".", 2)
        if section in _UNMODELED_SETTINGS_SECTIONS:
            continue
        assert section in section_fields, f"fields.{key}: unknown section {section!r}"
        assert field_name in section_fields[section], (
            f"fields.{key}: {field_name!r} is not a field of section {section!r}"
        )


def test_namelist_label_uses_glossary_override():
    from cstar.wizard.wizard import _namelist_label

    assert _namelist_label("lateral_visc", "visc2") == "Horizontal viscosity"


def test_namelist_label_falls_back_to_field_name_when_unknown():
    from cstar.wizard.wizard import _namelist_label

    assert _namelist_label("lateral_visc", "not_a_real_field") == "not_a_real_field"


def test_output_table_and_variable_grid_fields_exist():
    """Every field name referenced by ``_OUTPUT_TABLES``/``_VARIABLE_GRIDS`` (the
    Advanced-settings output-stream table / per-variable checkbox-grid layout --
    see ``_SettingsEditor._build_section``) must be a real field of the
    ``RunTimeSettings`` section it claims, on at least one registered tier
    (``RunTimeSettings``/``V0_5_0``/``V0_6_0``/``V0_7_0``). Mirrors
    ``test_every_settings_field_key_names_a_real_namelist_field``'s glossary
    check, but for the table/grid definitions rather than the label glossary.
    """
    from cstar.wizard.wizard import _OUTPUT_TABLES, _VARIABLE_GRIDS

    section_fields = _all_settings_section_field_names()

    for section, rows in _OUTPUT_TABLES.items():
        assert section in section_fields, f"_OUTPUT_TABLES: unknown section {section!r}"
        for row in rows:
            for key in (row["write"], row["period"], row["records"], *row["extra"]):
                if key is None:
                    continue
                assert key in section_fields[section], (
                    f"_OUTPUT_TABLES[{section!r}] row {row['label']!r}: {key!r} is "
                    f"not a field of section {section!r}"
                )

    for section, grids in _VARIABLE_GRIDS.items():
        assert section in section_fields, (
            f"_VARIABLE_GRIDS: unknown section {section!r}"
        )
        for title, keys in grids:
            for key in keys:
                assert key in section_fields[section], (
                    f"_VARIABLE_GRIDS[{section!r}] {title!r}: {key!r} is not a "
                    f"field of section {section!r}"
                )
