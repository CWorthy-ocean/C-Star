"""Tests for the ipywidgets ForgeBlueprintWizard UI (cstar.wizard.wizard).

These cover the wizard as a whole and the helpers in ``wizard.py``. Tests that build
only one of its parts live beside this file: the forcing editor in
``test_forcing_editor.py``, the settings editor and its placement tables in
``test_settings_editor.py``, the field helpers in ``test_fields.py`` and the app in
``test_app.py``. A test class stays whole, so a class with any test that builds a
wizard is here. Widget construction is lightweight (no grid/network I/O — the live
preview resolves via ``dt``, never building a roms_tools.Grid), so these run as fast
unit tests.
"""

import copy
from datetime import date, datetime
from pathlib import Path
from typing import ClassVar

import pytest
import yaml

import cstar
from cstar.applications.forge.blueprint import ForgeBlueprint, ForgeProvenance
from cstar.applications.forge.namelist_model import (
    RunTimeSettingsV0_13_0,
    RunTimeSettingsV0_14_0,
    RunTimeSettingsV0_16_0,
)
from cstar.applications.forge.resolve import build_forge_blueprint, cfl_dt
from cstar.base.env import BUNDLED_CATALOG_ROOT
from cstar.catalog.spec_schema import known_spec_keys
from cstar.tests.unit_tests.wizard.helpers import (
    _BLEND_SECTIONS,
    _INTERP_KEY,
    _LITE_STREAM,
    _built_fields,
    _descendants,
    _display,
    _is_in_tree,
    _iter_widget_tree,
)
from cstar.wizard.app import ForgeBlueprintWizardApp
from cstar.wizard.settings_editor import (
    _ACCORDION_EXCLUDED_FIELDS,
    _OUTPUT_CARD,
    _OUTPUT_MORE_TITLE,
    _OUTPUT_TABLES,
    _in_output_card,
    _section_submodel,
)
from cstar.wizard.wizard import (
    _STICKY_STEPS,
    WIZARD_TOOL,
    ForgeBlueprintWizard,
    WorkplanHooks,
    _drain_stream_buffer,
    _is_output_key,
)


def _find_section(w, title_fragment):
    """Recursively find the VBox whose direct HTML title child contains the given
    fragment (mirrors the wizard's ``section()`` layout helper).
    """
    for c in getattr(w, "children", []):
        html = getattr(c, "value", None)
        if isinstance(html, str) and title_fragment in html:
            return w
        found = _find_section(c, title_fragment)
        if found is not None:
            return found
    return None


def _find_card(root, key):
    """Recursively find the ``components.card`` VBox tagged ``forge_key == key``
    under ``root`` (see ``cstar.wizard.ui.components.card``).
    """
    if getattr(root, "forge_key", None) == key:
        return root
    for c in getattr(root, "children", []):
        found = _find_card(c, key)
        if found is not None:
            return found
    return None


@pytest.mark.parametrize("section", _BLEND_SECTIONS)
@pytest.mark.parametrize("typed", ["deep", "-5", "nan"])
def test_invalid_bottom_fill_blend_m_shows_an_invalid_blueprint_banner(section, typed):
    """Typed-but-invalid text must reach blueprint validation and surface in the
    status area (not raise out of the wizard, and not be dropped); clearing the box
    makes the blueprint valid again.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config is not None, wiz.derived.value

    box = getattr(wiz._forcing_editor, f"{section}_bottom_fill_blend_m")
    box.value = typed
    assert wiz.config is None
    assert "Invalid blueprint" in wiz.derived.value

    box.value = ""
    assert wiz.config is not None, wiz.derived.value


# ===========================================================================
# Surface blend: a regional primary layered over a global fallback.
# ===========================================================================


def test_blend_with_auto_coarse_mode_surfaces_the_forcing_error():
    """A user who sets ``auto`` back on a blended row sees the Forcing error."""
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    row = wiz._forcing_editor._rows["surface"][0]
    row["blend_fallback"].value = "ERA5"
    assert wiz.config is not None, wiz.derived.value
    row["coarse_grid_mode"].value = "auto"
    assert wiz.config is None
    assert "Invalid blueprint" in wiz.derived.value
    assert wiz.card_errors["forcing"].layout.display == ""
    assert "Surface forcing, row 1" in wiz.card_errors["forcing"].value
    assert "1 problem" in wiz.card_chips["forcing"].value


# ===========================================================================
# F5: `_sources_to_inputs`'s `bgc_section()`/`src()` now build on the generic
# `plain()` helper instead of a hand-listed field set -- must still round-trip
# every field losslessly.
# ===========================================================================


def test_river_temp_source_round_trips_through_yaml(tmp_path):
    """Full round trip: build → to_yaml → load into a fresh wizard → gather.
    Both fields must survive and the reloaded blueprint must hash identically.
    """
    wiz = _new_wizard()
    fe = wiz._forcing_editor
    w = fe._rows["river"][0]
    w["surface_forcing_source_name"].value = "ERA5"
    w["surface_forcing_source_path"].value = "/data/era5/*.nc"
    w["river_temp_smoothing_window_days"].value = 10.0
    assert wiz.config is not None, wiz.derived.value

    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)
    # the wizard never sets working_dir, so a saved file simply omits it
    assert "working_dir" not in saved.read_text()

    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    assert wiz2.config is not None
    assert wiz2.config.content_hash() == wiz.config.content_hash()

    fe2 = wiz2._forcing_editor
    w2 = fe2._rows["river"][0]
    assert w2["surface_forcing_source_name"].value == "ERA5"
    assert w2["surface_forcing_source_path"].value == "/data/era5/*.nc"
    assert w2["river_temp_smoothing_window_days"].value == 10.0

    item = fe2._gather_item("river", w2)
    assert item["surface_forcing_source"] == {
        "name": "ERA5",
        "path": "/data/era5/*.nc",
    }
    assert item["river_temp_smoothing_window_days"] == 10.0


def test_topo_source_dropdown_includes_emod():
    """The topography-source dropdown must offer EMOD alongside ETOPO5/SRTM15."""
    wiz = ForgeBlueprintWizard()
    assert "EMOD" in wiz.topo_source.options


def test_run_warning_names_the_selected_system(monkeypatch):
    """The run note names the system C-Star uses, saying `CSTAR_SYSTEM` chose it."""
    monkeypatch.setenv("CSTAR_SYSTEM", "linux_x86_64")
    wiz = ForgeBlueprintWizard()
    assert (
        "System: <code>linux_x86_64 (set by CSTAR_SYSTEM)</code>"
        in wiz.run_warning.value
    )


def test_wizard_smoke_assembles_widget():
    """The redesigned Grid card assembles cleanly, with geometry, open
    boundaries, and nesting appearing in the expected relative order (mirrors
    the pre-redesign flat-``section()`` version of this smoke test).
    """
    wiz = ForgeBlueprintWizard()
    root = wiz.widget  # must not raise
    grid_card = _find_card(root, "grid")
    assert grid_card is not None

    texts = [
        html
        for w in _descendants(grid_card)
        if isinstance((html := getattr(w, "value", None)), str)
    ]

    def _first_containing(fragment):
        return next(i for i, t in enumerate(texts) if fragment in t)

    geometry_i = _first_containing("Grid geometry")
    obc_i = _first_containing("Open boundaries")
    nest_i = _first_containing("Child grid")
    assert geometry_i < obc_i < nest_i


def test_load_catalog_dropdown_populated_from_catalog():
    """The 'From catalog' picker offers the bundled forge blueprints and is enabled."""
    wiz = ForgeBlueprintWizard()
    values = wiz._dd_values(wiz.load_catalog_dd)
    assert "wio-toy-simple" in values
    # bundled catalog is non-empty, so the load button is not greyed out
    assert wiz.load_catalog_btn.disabled is False


def test_load_from_catalog_resolves_path_and_delegates(monkeypatch):
    """Selecting a catalog blueprint resolves its path into ``load_path`` and reuses
    the existing ``_on_load_path`` load path (identical parse/state handling).
    """
    wiz = ForgeBlueprintWizard()
    called = []
    monkeypatch.setattr(wiz, "_on_load_path", lambda _=None: called.append(True))

    wiz.load_catalog_dd.value = "wio-toy-simple"
    wiz._on_load_from_catalog(None)

    assert called == [True]
    assert Path(wiz.load_path.value).parts[-3:] == (
        "blueprints",
        "forge",
        "wio-toy-simple.yaml",
    )


def test_load_from_catalog_no_selection_surfaces_error(monkeypatch):
    """Defensive guard: if the dropdown ever holds no selection (e.g. an empty
    catalog, where the button is also disabled), the handler reports an error and
    does not load. On a populated catalog the dropdown auto-selects the first
    name, so this state is not normally reachable by the user.
    """
    wiz = ForgeBlueprintWizard()
    called = []
    monkeypatch.setattr(wiz, "_on_load_path", lambda _=None: called.append(True))

    wiz.load_catalog_dd.value = None
    wiz._on_load_from_catalog(None)

    assert called == []
    assert wiz._load_status_is_error is True
    assert "No catalog blueprint selected" in wiz.load_status.value


def test_specs_section_has_forcing_and_output_dropdowns():
    """The ForcingSpec selector lives in the Model card, the OutputSpec selector
    at the top of the Output card (above the output streams).
    """
    wiz = ForgeBlueprintWizard()
    model_card = _find_card(wiz.widget, "model")
    assert model_card is not None
    assert wiz.forcing_dd in list(_descendants(model_card))
    assert wiz.output_dd not in list(_descendants(model_card))
    output_card = _find_card(wiz.widget, "output")
    assert output_card is not None
    body = output_card.children[-1]  # the card body (children: anchor, header, body)
    assert body.children[0] is wiz.card_errors["output"]  # atop every card body
    assert wiz.output_dd in list(_descendants(body.children[1]))
    assert wiz.output_editor_box is body.children[2]


def test_roms_ref_prefilled_and_placed_next_to_model_dropdown():
    """ucla-roms ref is prefilled from the selected model's pinned default
    (stays editable) and lives in the Model card, in its own field row below
    the Model dropdown (the card/field_row redesign no longer puts it
    literally in the same row as Model -- see components.field_row).
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.roms_ref.value == wiz._model_default_roms_ref()
    assert wiz.roms_ref.value  # this model.yaml pins a concrete commit

    model_card = _find_card(wiz.widget, "model")
    assert model_card is not None
    assert wiz.roms_ref in _descendants(model_card)


def test_roms_ref_repopulates_on_model_change(monkeypatch):
    """Switching models refreshes ucla-roms ref to the new model's pinned default."""
    wiz = ForgeBlueprintWizard()
    monkeypatch.setattr(wiz, "_model_default_roms_ref", lambda: "some-other-ref")
    wiz._on_model_change(None)
    assert wiz.roms_ref.value == "some-other-ref"


def test_marbl_ref_prefilled_and_placed_next_to_model_dropdown():
    """MARBL ref mirrors the ucla-roms ref: prefilled from the selected model's
    pinned default (stays editable) and lives in the Model card, after
    (in document order) BGC mode's own row -- MARBL ref only matters once BGC
    mode is "marbl".
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.marbl_ref.value == wiz._model_default_marbl_ref()
    assert wiz.marbl_ref.value  # this model.yaml pins a concrete MARBL tag

    model_card = _find_card(wiz.widget, "model")
    assert model_card is not None
    descendants = list(_descendants(model_card))
    assert wiz.marbl_ref in descendants
    assert wiz.bgc_dd in descendants
    assert descendants.index(wiz.marbl_ref) > descendants.index(wiz.bgc_dd)


def test_marbl_ref_hidden_unless_bgc_is_marbl():
    """The MARBL ref field only shows when BGC mode is "marbl". Its value is
    kept (not cleared) while hidden, so toggling BGC back restores the pin.
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.bgc_dd.value == "marbl"
    assert wiz.marbl_ref.layout.display != "none"
    kept = wiz.marbl_ref.value

    wiz.bgc_dd.value = "none"
    assert wiz.marbl_ref.layout.display == "none"
    assert wiz.marbl_ref.value == kept

    wiz.bgc_dd.value = "marbl"
    assert wiz.marbl_ref.layout.display != "none"


def test_marbl_ref_repopulates_on_model_change(monkeypatch):
    """Switching models refreshes the MARBL ref to the new model's pinned default."""
    wiz = ForgeBlueprintWizard()
    monkeypatch.setattr(wiz, "_model_default_marbl_ref", lambda: "some-other-marbl")
    wiz._on_model_change(None)
    assert wiz.marbl_ref.value == "some-other-marbl"


def test_co2_tvarying_is_not_user_editable():
    """co2_tvarying is controlled solely by the presence of an MBL_co2 bgc surface
    source; the wizard must not expose a checkbox letting the user override it --
    that derivation happens behind the scenes in the resolver/input_generator.
    """
    wiz = ForgeBlueprintWizard()
    assert not hasattr(wiz, "co2_tvarying_chk")

    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    # The default model's forcing includes an MBL_co2 bgc surface source, so the
    # resolver still auto-derives co2_tvarying=True with no UI toggle involved.
    assert wiz.config.model_settings["cppdefs"]["co2_tvarying"] is True


def test_resolver_derived_cppdefs_fields_have_no_accordion_widget():
    """obc_*/marbl/use_pio/cdr_forcing/co2_tvarying/sal_restore/tides are all fully
    resolver-derived (like co2_tvarying above) -- the advanced settings accordion
    must never expose a competing editor for any of them.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    for field in (
        "obc_west",
        "obc_east",
        "obc_north",
        "obc_south",
        "marbl",
        "use_pio",
        "cdr_forcing",
        "co2_tvarying",
        "sal_restore",
        "tides",
    ):
        assert ("cppdefs", field) not in wiz.editor._widgets


def test_sponge_tune_editable_via_advanced_settings_accordion():
    """Unlike co2_tvarying, SPONGE_TUNE has no resolver-side derivation -- it's a
    plain ModelSpec default (False) reachable only through the advanced settings
    accordion's generic (section, field) override mechanism.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["sponge_tune"] is False
    assert ("cppdefs", "sponge_tune") in wiz.editor._widgets

    wiz._overrides[("cppdefs", "sponge_tune")] = True
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["sponge_tune"] is True


def test_nt_passive_editable_via_advanced_settings_accordion():
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.model_settings["param"]["nt_passive"] == 0
    assert ("param", "nt_passive") in wiz.editor._widgets
    assert ("param", "ntrc_bio") not in wiz.editor._widgets

    wiz._overrides[("param", "nt_passive")] = 2
    wiz._rebuild()
    assert wiz.config.model_settings["param"]["nt_passive"] == 2


def test_nhy_nox_forcing_editable_in_bgc_advanced_settings_pane():
    """NHY_FORCING/NOX_FORCING default True from the ModelSpec and are editable as
    checkboxes in the Biogeochemistry (BGC / MARBL) advanced settings pane.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["nhy_forcing"] is True
    assert wiz.config.model_settings["cppdefs"]["nox_forcing"] is True
    assert ("cppdefs", "nhy_forcing") in wiz.editor._widgets
    assert ("cppdefs", "nox_forcing") in wiz.editor._widgets

    wiz._overrides[("cppdefs", "nhy_forcing")] = False
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["nhy_forcing"] is False
    assert wiz.config.model_settings["cppdefs"]["nox_forcing"] is True  # untouched


def test_bgc_dd_none_forces_nhy_nox_forcing_off_in_the_wizard():
    """Flipping BGC mode to 'none' in the wizard must force NHY_FORCING/NOX_FORCING
    off through the real compose -> override -> sync pipeline, not just at the
    resolver. Strip every BGC signal from the bundled ForcingSpec's widgets first
    (mirrors _PHYSICS_ONLY_FORCING in test_forge_blueprint.py) so bgc_mode="none"
    doesn't raise.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    fe = wiz._forcing_editor
    for ws in list(fe._rows["surface"]):
        if ws.get("type") is not None and ws["type"].value == "bgc":
            fe._remove("surface", ws)
    for ws in list(fe._rows["boundary_bgc"]):
        fe._remove("boundary_bgc", ws)
    for ws in list(fe._rows["river"]):
        if "include_bgc" in ws:
            ws["include_bgc"].value = False
    for ws in list(fe._rows["ic_bgc"]):
        fe._remove("ic_bgc", ws)

    wiz.bgc_dd.value = "none"
    wiz._rebuild()
    assert wiz.config is not None, wiz.derived.value
    assert wiz.config.model_settings["cppdefs"]["nhy_forcing"] is False
    assert wiz.config.model_settings["cppdefs"]["nox_forcing"] is False


def test_bgc_dd_default_and_placement():
    """BGC mode defaults to 'marbl' and lives in the Model card."""
    wiz = ForgeBlueprintWizard()
    assert wiz.bgc_dd.value == "marbl"
    assert set(wiz.bgc_dd.options) == {"marbl", "none", "cdr_lite"}

    model_card = _find_card(wiz.widget, "model")
    assert model_card is not None
    assert wiz.bgc_dd in _descendants(model_card)


def test_bgc_dd_marbl_gathers_into_cppdefs():
    """The default 'marbl' choice flows through _gather()/build_forge_blueprint into
    cppdefs.marbl -- the happy path (the bundled ForcingSpec carries BGC forcing, so
    switching to 'none' without changing forcing is expected to raise; that's
    covered at the resolver level, see test_resolver_bgc_mode_none_raises_with_bgc_forcing).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["marbl"] is True
    assert wiz.config.code.marbl is not None


def test_bgc_dd_none_with_non_default_bgc_forcing_surfaces_error_legibly():
    """Flipping to 'none' while a (BGC-carrying) ForcingSpec other than the default
    is selected must not crash the wizard -- _rebuild()'s existing exception
    handling should catch the resolver's ValueError and surface it in the status
    area, as one line pointing at the full message in the preview.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.forcing_dd.value = "glorys-era5-esper"
    wiz.bgc_dd.value = "none"
    wiz._rebuild()
    assert wiz.forcing_dd.value == "glorys-era5-esper"
    assert wiz.config is None
    assert "Invalid" in wiz.derived.value
    assert "full message in the preview below" in wiz.derived.value
    assert "\n" not in wiz.derived.value


@pytest.mark.parametrize(
    ("message", "suffixed"),
    [
        ("short single line", False),
        ("x" * 250, True),
        ("first line\nsecond line", True),
    ],
)
def test_rebuild_error_status_points_at_the_preview_only_when_truncated(
    monkeypatch, message, suffixed
):
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    def _raise(**_kw):
        raise ValueError(message)

    monkeypatch.setattr("cstar.wizard.wizard.build_forge_blueprint", _raise)
    wiz._rebuild()
    assert wiz.config is None
    assert ("full message in the preview below" in wiz.derived.value) is suffixed


class TestBgcModeForcingDefault:
    """The default ForcingSpec follows the BGC mode while it is untouched: the
    BGC-carrying default under MARBL, the physics-only one without (the resolver
    rejects BGC forcing there). A hand-edited, non-default or loaded forcing is
    never swapped.
    """

    PHYSICS = "glorys-era5-physics"
    UNIFIED = "glorys-era5-unified"

    @staticmethod
    def _wizard(catalog=None):
        wiz = (
            ForgeBlueprintWizard() if catalog is None else ForgeBlueprintWizard(catalog)
        )
        wiz.start.value = date(2012, 1, 1)
        wiz.end.value = date(2012, 1, 2)
        wiz.use_pio_chk.value = False  # off: no CDF-5 conversion warning per rebuild
        return wiz

    @staticmethod
    def _count_resolves(monkeypatch) -> list[int]:
        from cstar.wizard import wizard as wizard_module

        calls: list[int] = []
        real = wizard_module.build_forge_blueprint

        def _counted(**kw):
            calls.append(1)
            return real(**kw)

        monkeypatch.setattr(wizard_module, "build_forge_blueprint", _counted)
        return calls

    def test_marbl_default_is_the_unified_spec(self):
        assert self._wizard().forcing_dd.value == self.UNIFIED

    def test_init_default_follows_the_model_specs_bgc_mode(self, tmp_path):
        import shutil

        from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog
        from cstar.wizard.wizard import _DEFAULT_MODEL

        root = tmp_path / "catalog"
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        model_yaml = root / "specs" / "ModelSpec" / f"{_DEFAULT_MODEL}.yaml"
        text = model_yaml.read_text()
        assert "bgc_mode: marbl" in text
        model_yaml.write_text(text.replace("bgc_mode: marbl", "bgc_mode: cdr_lite"))

        wiz = ForgeBlueprintWizard(catalog=DomainCatalog(catalog_root=root))
        assert wiz.bgc_dd.value == "cdr_lite"
        assert wiz.forcing_dd.value == self.PHYSICS

    @pytest.mark.parametrize("mode", ["none", "cdr_lite"])
    def test_leaving_marbl_switches_the_untouched_default_to_physics(self, mode):
        wiz = self._wizard()
        wiz.cdr_mode_dd.value = "simple"  # cdr_lite needs a CDR forcing to resolve
        wiz.bgc_dd.value = mode
        assert wiz.forcing_dd.value == self.PHYSICS
        assert wiz.config is not None, wiz.derived.value
        assert wiz.config.composition.forcing.name == self.PHYSICS
        assert wiz.config.composition.forcing.modified is False

    def test_entering_marbl_switches_the_untouched_physics_spec_back(self):
        wiz = self._wizard()
        wiz.bgc_dd.value = "none"
        assert wiz.forcing_dd.value == self.PHYSICS
        wiz.bgc_dd.value = "marbl"
        assert wiz.forcing_dd.value == self.UNIFIED
        assert wiz.config is not None, wiz.derived.value

    def test_between_modes_without_marbl_nothing_switches(self, monkeypatch):
        wiz = self._wizard()
        wiz.bgc_dd.value = "none"
        calls = self._count_resolves(monkeypatch)
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.forcing_dd.value == self.PHYSICS
        assert len(calls) == 1

    @pytest.mark.parametrize("mode", ["none", "cdr_lite"])
    def test_one_resolve_per_mode_change(self, monkeypatch, mode):
        wiz = self._wizard()
        calls = self._count_resolves(monkeypatch)
        wiz.bgc_dd.value = mode  # switches the forcing too
        assert len(calls) == 1
        calls.clear()
        wiz.bgc_dd.value = "marbl"
        assert len(calls) == 1

    def test_one_resolve_when_the_forcing_stays(self, monkeypatch):
        wiz = self._wizard()
        wiz.forcing_dd.value = "glorys-era5-esper"
        calls = self._count_resolves(monkeypatch)
        wiz.bgc_dd.value = "none"
        assert len(calls) == 1

    def test_an_edited_forcing_is_left_alone(self):
        wiz = self._wizard()
        wiz._forcing_editor._add("surface")
        assert wiz._forcing_modified()
        wiz.bgc_dd.value = "none"
        assert wiz.forcing_dd.value == self.UNIFIED

    def test_a_non_default_spec_is_left_alone(self):
        wiz = self._wizard()
        wiz.forcing_dd.value = "glorys-era5-esper"
        wiz.bgc_dd.value = "none"
        assert wiz.forcing_dd.value == "glorys-era5-esper"
        wiz.bgc_dd.value = "marbl"
        assert wiz.forcing_dd.value == "glorys-era5-esper"

    def test_nothing_switches_while_suspended(self, monkeypatch):
        wiz = self._wizard()
        calls = self._count_resolves(monkeypatch)
        with wiz._suspend():
            wiz.bgc_dd.value = "none"
        assert wiz.forcing_dd.value == self.UNIFIED
        assert calls == []

    def test_loading_a_blueprint_keeps_its_recorded_forcing(self, tmp_path):
        """A file recorded with the BGC-carrying default but a ``bgc_mode`` of none
        (invalid at resolve, but the load must not rewrite what the file says)
        loads with that forcing still selected.
        """
        w1 = self._wizard()
        w1._boundaries_touched = True
        path = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(path)
        w1._on_save(None)
        text = path.read_text()
        assert "marbl: true" in text
        path.write_text(text.replace("marbl: true", "marbl: false"))

        w2 = self._wizard()
        w2.load_path.value = str(path)
        w2._on_load_path(None)
        assert w2.bgc_dd.value == "none"
        assert w2.forcing_dd.value == self.UNIFIED


class TestBgcModeSettings:
    """The Advanced-settings editor follows the BGC mode: settings that need MARBL
    are neither shown nor kept as overrides without it, and the CDR-lite stream
    that ``cdr_lite`` forces on is displayed as on.
    """

    @staticmethod
    def _wizard():
        wiz = ForgeBlueprintWizard()
        wiz.start.value = date(2012, 1, 1)
        wiz.end.value = date(2012, 1, 2)
        wiz.use_pio_chk.value = False
        wiz.cdr_mode_dd.value = "simple"  # cdr_lite needs a CDR forcing to resolve
        return wiz

    @staticmethod
    def _toggle(wiz, key):
        widget = wiz.editor._widgets[key][0]
        widget.value = not widget.value

    def test_editor_is_rebuilt_for_the_mode(self):
        wiz = self._wizard()
        assert wiz.editor._bgc_mode == "marbl"
        assert ("cdr_output", "do_cdr_output") in wiz.editor._widgets
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config is not None, wiz.derived.value
        assert wiz._editor_key.bgc_mode == "cdr_lite"
        assert wiz.editor._bgc_mode == "cdr_lite"
        assert ("cdr_output", "do_cdr_output") not in wiz.editor._widgets
        wiz.bgc_dd.value = "marbl"
        assert ("cdr_output", "do_cdr_output") in wiz.editor._widgets

    def test_marbl_only_overrides_are_hidden_on_leaving_marbl(self):
        wiz = self._wizard()
        before = wiz.config.model_settings["bgc"]["wrt_his"]
        self._toggle(wiz, ("bgc", "wrt_his"))
        self._toggle(wiz, ("cppdefs", "nhy_forcing"))
        self._toggle(wiz, ("cppdefs", "sponge_tune"))
        # bgc's write-control is an Output-card setting, the other two are Advanced
        assert "1 modified" in wiz.card_chips["output"].value
        assert "2 modified" in wiz.card_chips["advanced"].value

        wiz.bgc_dd.value = "cdr_lite"

        # the user's record keeps all three; only the applicable one is active,
        # beside the wizard's carbonate-sensitivity default (a Model-card setting)
        assert len(wiz._overrides) == 4
        assert set(wiz._active_overrides) == {("cppdefs", "sponge_tune"), _INTERP_KEY}
        assert wiz.config is not None, wiz.derived.value
        assert wiz.config.composition.overrides == {
            "cppdefs": {"sponge_tune": True},
            **_INTERP_HOLD,
        }
        settings = wiz.config.model_settings
        assert settings.get("bgc", {}).get("wrt_his", before) is before
        assert settings["cppdefs"]["nhy_forcing"] is False
        assert settings["cppdefs"]["sponge_tune"] is True
        assert "At preset defaults" in wiz.card_chips["output"].value
        assert "1 modified" in wiz.card_chips["advanced"].value

    def test_hidden_overrides_come_back_with_their_mode(self):
        wiz = self._wizard()
        before = wiz.config.model_settings["bgc"]["wrt_his"]
        self._toggle(wiz, ("bgc", "wrt_his"))
        self._toggle(wiz, ("cppdefs", "nhy_forcing"))
        marbl_overrides = wiz.config.composition.overrides
        assert marbl_overrides == {
            "bgc": {"wrt_his": not before},
            "cppdefs": {"nhy_forcing": False},
        }

        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config.composition.overrides == _INTERP_HOLD
        wiz.bgc_dd.value = "marbl"

        assert wiz.config is not None, wiz.derived.value
        assert wiz.config.composition.overrides == marbl_overrides
        assert wiz.config.model_settings["bgc"]["wrt_his"] is (not before)
        assert wiz.config.model_settings["cppdefs"]["nhy_forcing"] is False
        assert wiz.editor._widgets[("bgc", "wrt_his")][0].value is (not before)
        assert "1 modified" in wiz.card_chips["output"].value
        assert "1 modified" in wiz.card_chips["advanced"].value

    def test_overrides_are_kept_between_modes_that_share_them(self):
        wiz = self._wizard()
        wiz.bgc_dd.value = "cdr_lite"
        self._toggle(wiz, ("cdr_lite_output", "wrt_alk"))
        wiz.bgc_dd.value = "marbl"
        assert ("cdr_lite_output", "wrt_alk") in wiz._active_overrides

    def test_a_suspended_mode_change_loses_nothing(self):
        """A load changes the mode while suspended: the record is untouched, and the
        load's own rebuild applies only what the new mode allows.
        """
        wiz = self._wizard()
        self._toggle(wiz, ("bgc", "wrt_his"))
        with wiz._suspend():
            wiz.bgc_dd.value = "cdr_lite"
        assert ("bgc", "wrt_his") in wiz._overrides
        wiz._rebuild()  # (resolve fails: a load would also have set the forcing)
        assert ("bgc", "wrt_his") in wiz._overrides
        assert wiz._active_overrides == {_INTERP_KEY: False}  # the wizard's own

    def test_a_failed_mode_switch_swaps_the_stale_editor_for_a_placeholder(self):
        """cdr_lite with no CDR forcing yet does not resolve; the MARBL-mode editor
        must not stay on screen offering streams that no longer apply. The user's
        record survives, and the next valid resolve rebuilds the editor.
        """
        wiz = ForgeBlueprintWizard()
        wiz.start.value = date(2012, 1, 1)
        wiz.end.value = date(2012, 1, 2)
        wiz.use_pio_chk.value = False
        self._toggle(wiz, ("bgc", "wrt_his"))
        marbl_editor = wiz.editor

        wiz.bgc_dd.value = "cdr_lite"

        assert wiz.config is None
        assert wiz.editor is not marbl_editor and not wiz._editor_key.resolved
        (placeholder,) = wiz.editor_box.children
        assert wiz.output_editor_box.children == (placeholder,)
        assert "once the blueprint is valid" in placeholder.value
        for box in (wiz.editor_box, wiz.output_editor_box):
            assert not any(
                w is marbl_editor.accordion or w is marbl_editor.output_box
                for w in _descendants(box)
            )
        # nothing is lost: kept for its mode, inactive under this one
        assert ("bgc", "wrt_his") in wiz._overrides
        assert wiz._active_overrides == {_INTERP_KEY: False}  # the wizard's own

        wiz.cdr_mode_dd.value = "simple"  # a CDR forcing: the config resolves again
        assert wiz.config is not None, wiz.derived.value
        assert wiz.editor is not marbl_editor
        assert wiz.editor._bgc_mode == "cdr_lite"
        assert ("cdr_output", "do_cdr_output") not in wiz.editor._widgets
        assert ("cdr_gas_exch_output", "do_cdr_gas_exch_output") not in (
            wiz.editor._widgets
        )
        assert wiz.editor_box.children == (wiz.editor.accordion,)
        assert wiz.output_editor_box.children == (wiz.editor.output_box,)
        assert wiz.config.composition.overrides == _INTERP_HOLD

        wiz.bgc_dd.value = "marbl"
        assert wiz.config.composition.overrides == {
            "bgc": {"wrt_his": wiz.config.model_settings["bgc"]["wrt_his"]}
        }

    def test_a_nesting_toggle_on_an_invalid_blueprint_swaps_the_stale_editor(self):
        """Ticking "This grid is a parent" while the blueprint does not resolve must
        not leave an editor without child-grid extraction on screen; the next valid
        resolve builds it with extract_data.
        """
        wiz = self._wizard()
        wiz.cdr_mode_dd.value = "netcdf"  # no file attached: the config is invalid
        assert wiz.config is None
        stale = wiz.editor
        assert stale is not None  # same model, mode and role: kept

        wiz.nest_enable.value = True

        assert wiz.editor is not stale and not wiz._editor_key.resolved
        (placeholder,) = wiz.output_editor_box.children
        assert "once the blueprint is valid" in placeholder.value

        wiz.cdr_mode_dd.value = "none"
        assert wiz.config is not None, wiz.derived.value
        assert wiz._editor_key.is_parent
        assert "extract_data" in wiz.editor._group_fields[_OUTPUT_CARD]

    @pytest.mark.parametrize("failure", ["no_end_date", "invalid_grid"])
    def test_a_failure_under_the_same_model_and_mode_keeps_the_editor(self, failure):
        wiz = self._wizard()
        editor = wiz.editor

        if failure == "no_end_date":
            wiz.end.value = None
        else:
            wiz.grid_w["nx"].value = 0  # the resolver rejects a non-positive grid

        assert wiz.config is None
        assert wiz.editor is editor
        assert wiz.editor_box.children == (editor.accordion,)
        assert wiz.output_editor_box.children == (editor.output_box,)

        wiz.end.value = date(2012, 1, 2)
        wiz.grid_w["nx"].value = 10
        assert wiz.config is not None, wiz.derived.value
        assert wiz.editor is editor

    def test_loading_a_marbl_blueprint_keeps_its_marbl_only_overrides(self):
        wiz = self._wizard()
        wiz.bgc_dd.value = "marbl"
        wiz.cdr_mode_dd.value = "none"
        self._toggle(wiz, ("bgc", "wrt_his"))
        assert wiz.config is not None, wiz.derived.value

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)

        assert ("bgc", "wrt_his") in wiz._overrides
        assert wiz2._overrides == wiz._overrides
        assert wiz2.config is not None, wiz2.derived.value

    def test_loaded_marbl_only_overrides_are_reported_and_not_applied(self):
        """A blueprint whose mode forbids a MARBL-only output (hand-edited or
        stale) loads with the problems reported; the wizard's blueprint and the
        next save carry neither flag.
        """
        wiz = self._wizard()
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config is not None, wiz.derived.value
        settings = copy.deepcopy(wiz.config.model_settings)
        settings["cdr_output"]["do_cdr_output"] = True
        settings["upscale_output"]["do_upscale"] = True
        settings["cppdefs"]["sponge_tune"] = True
        edited = wiz.config.model_copy(update={"model_settings": settings})

        wiz2 = ForgeBlueprintWizard()
        problems = wiz2._populate_from(edited)

        assert any("cdr_output.do_cdr_output=True" in p for p in problems)
        assert any("upscale_output.do_upscale=True" in p for p in problems)
        # the time-interpolation hold is the file's own value, carried as authored
        assert set(wiz2._active_overrides) == {("cppdefs", "sponge_tune"), _INTERP_KEY}
        assert wiz2.config is not None, wiz2.derived.value
        assert wiz2.config.model_settings["cdr_output"]["do_cdr_output"] is False
        assert wiz2.config.model_settings["upscale_output"]["do_upscale"] is False
        assert wiz2.config.composition.overrides == {
            "cppdefs": {"sponge_tune": True},
            **_INTERP_HOLD,
        }

    def test_cdr_lite_stream_is_shown_on_without_recording_an_override(self):
        wiz = self._wizard()
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config is not None, wiz.derived.value
        master = wiz.editor._widgets[_LITE_STREAM][0]
        assert master.value is True
        assert master.disabled is True
        assert _display(wiz.editor._widgets[("cdr_lite_output", "nrpf")][0]) == ""
        assert _LITE_STREAM not in wiz._overrides
        assert "cdr_lite_output" not in wiz.config.composition.overrides
        # display only: the authored value is what is saved
        assert (
            wiz.config.model_settings["cdr_lite_output"]["do_cdr_lite_output"] is False
        )

    def test_round_trips_a_cdr_lite_blueprint_with_overrides(self):
        wiz = self._wizard()
        wiz.bgc_dd.value = "cdr_lite"
        self._toggle(wiz, ("cppdefs", "sponge_tune"))
        self._toggle(wiz, ("cdr_lite_output", "wrt_alk"))
        assert wiz.config is not None, wiz.derived.value

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)

        assert wiz2.bgc_dd.value == "cdr_lite"
        assert wiz2._overrides == wiz._overrides
        assert wiz2._editor_key.bgc_mode == "cdr_lite"
        assert wiz2.config is not None, wiz2.derived.value
        assert wiz2.config.content_hash() == wiz.config.content_hash()


def test_use_pio_chk_default_seeded_from_model_spec():
    """use_pio_chk mirrors bgc_dd: it is seeded from the selected ModelSpec's
    top-level use_pio (True for roms-marbl-0.16-default, the wizard's default
    model; False for cson_roms-marbl_v0.1), and reseeded on a model switch.
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.model_dd.value == "roms-marbl-0.16-default"
    assert wiz.use_pio_chk.value is True
    assert wiz._model_default_use_pio() is True

    wiz.show_archived_models.value = True
    wiz.model_dd.value = "cson_roms-marbl_v0.1"
    assert wiz.use_pio_chk.value is False
    assert wiz._model_default_use_pio() is False


def test_model_dd_hides_archived_specs_by_default():
    """The model dropdown lists only non-archived specs until "Show archived" is
    checked; checking it adds the archived ones, labelled, in catalog order.
    """
    wiz = ForgeBlueprintWizard()
    archived = wiz.catalog.archived_model_names
    assert archived  # the bundled layer ships archived specs
    assert wiz.show_archived_models.value is False
    assert set(wiz._dd_values(wiz.model_dd)) == set(wiz.catalog.model_names) - set(
        archived
    )
    assert wiz.model_dd.value == "roms-marbl-0.16-default"

    wiz.show_archived_models.value = True
    assert wiz._dd_values(wiz.model_dd) == list(wiz.catalog.model_names)
    assert wiz.model_dd.value == "roms-marbl-0.16-default"
    labels = {value: label for label, value in wiz.model_dd.options}
    for name in archived:
        assert labels[name] == f"{name} (bundled, archived)"
    assert labels["roms-marbl-0.16-default"] == "roms-marbl-0.16-default (bundled)"

    wiz.show_archived_models.value = False
    assert set(wiz._dd_values(wiz.model_dd)) == set(wiz.catalog.model_names) - set(
        archived
    )


def test_dd_options_labels_archived_names_homogeneously():
    wiz = ForgeBlueprintWizard()
    assert wiz._dd_options(["a", "b"], "model") == ["a", "b"]
    options = wiz._dd_options(["a", "b"], "model", prefix=["<custom>"], archived=["b"])
    assert options == [
        ("<custom>", "<custom>"),
        ("a", "a"),
        ("b (archived)", "b"),
    ]


def test_unchecking_show_archived_keeps_an_archived_selection():
    wiz = ForgeBlueprintWizard()
    wiz.show_archived_models.value = True
    wiz.model_dd.value = "roms-marbl-0.7-default"
    overrides = {("param", "nt_passive"): 1}
    wiz._overrides = dict(overrides)

    wiz.show_archived_models.value = False

    assert wiz.model_dd.value == "roms-marbl-0.7-default"
    assert "roms-marbl-0.7-default" in wiz._dd_values(wiz.model_dd)
    assert "roms-marbl-0.6-default" not in wiz._dd_values(wiz.model_dd)
    # re-optioning is not a model switch: overrides are kept
    assert wiz._overrides == overrides

    wiz.model_dd.value = "roms-marbl-0.14-default"
    wiz.show_archived_models.value = True
    wiz.show_archived_models.value = False
    assert "roms-marbl-0.7-default" not in wiz._dd_values(wiz.model_dd)


def test_loading_a_blueprint_with_an_archived_model_selects_it(tmp_path):
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.show_archived_models.value = True
    wiz.model_dd.value = "roms-marbl-0.7-default"
    wiz._rebuild()
    assert wiz.config is not None, wiz.derived.value
    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    assert wiz2.show_archived_models.value is False
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    assert wiz2.show_archived_models.value is True
    assert wiz2.model_dd.value == "roms-marbl-0.7-default"
    assert "roms-marbl-0.7-default" in wiz2._dd_values(wiz2.model_dd)


def test_use_pio_chk_emit_is_unconditional():
    """The wizard must send an explicit use_pio=False to the resolver when the
    checkbox is unchecked (not simply omit the kwarg) -- otherwise a ModelSpec
    that declares use_pio: true could never be turned off in the UI, since the
    resolver's None fallback re-reads the ModelSpec default.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    # roms-marbl-0.14-default (the default model) declares use_pio: true --
    # exactly the ModelSpec this test guards against: unchecking must emit an
    # explicit False, not fall back to the ModelSpec default.
    assert wiz.use_pio_chk.value is True
    wiz.use_pio_chk.value = False
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["use_pio"] is False
    assert wiz.config.code.pio is None

    wiz.use_pio_chk.value = True
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["use_pio"] is True
    assert wiz.config.code.pio is not None


def test_auto_tiling_chk_toggles_dependent_widget_state():
    """Checking auto_tiling disables npx/npy, reveals n_cores, and force-locks
    use_pio on; unchecking reverses all of it but leaves use_pio's value as-is.
    """
    wiz = ForgeBlueprintWizard()
    wiz.use_pio_chk.value = False
    assert wiz.auto_tiling_chk.value is False
    assert wiz.npx.disabled is False
    assert wiz.npy.disabled is False
    assert "none" in (wiz.n_cores.layout.display or "none")
    assert wiz.use_pio_chk.disabled is False

    wiz.auto_tiling_chk.value = True
    assert wiz.npx.disabled is True
    assert wiz.npy.disabled is True
    assert (wiz.n_cores.layout.display or "") == ""
    assert wiz.use_pio_chk.value is True
    assert wiz.use_pio_chk.disabled is True

    wiz.auto_tiling_chk.value = False
    assert wiz.npx.disabled is False
    assert wiz.npy.disabled is False
    assert "none" in (wiz.n_cores.layout.display or "none")
    assert wiz.use_pio_chk.disabled is False
    assert wiz.use_pio_chk.value is True  # left as-is, not reset


def test_auto_tiling_toggle_seeds_n_cores_from_n_procs():
    """Checking the auto_tiling box seeds n_cores = npx * npy from the grid
    already entered (or loaded), so the user doesn't multiply by hand; the
    seed only happens on the off->on toggle, so a later manual n_cores edit
    survives unrelated _sync_auto_tiling() calls.
    """
    wiz = ForgeBlueprintWizard()
    wiz.npx.value = 4
    wiz.npy.value = 6

    wiz.auto_tiling_chk.value = True
    assert wiz.n_cores.value == 24

    # A manual n_cores edit is not clobbered by direct sync calls.
    wiz.n_cores.value = 30
    wiz._sync_auto_tiling()
    assert wiz.n_cores.value == 30

    # Re-toggling recomputes from the (re-enabled, possibly edited) grid.
    wiz.auto_tiling_chk.value = False
    wiz.npx.value = 2
    wiz.auto_tiling_chk.value = True
    assert wiz.n_cores.value == 12


def test_auto_tiling_gathers_n_cores_partitioning():
    """_gather() (and so the resolved config) swaps to auto_tiling/n_cores
    partitioning when checked, and back to n_procs_x/y when unchecked.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    wiz.auto_tiling_chk.value = True
    wiz.n_cores.value = 24
    wiz._rebuild()
    assert wiz.config is not None
    part = wiz.config.domain.partitioning
    assert part.auto_tiling is True
    assert part.n_cores == 24
    assert part.n_procs_x is None
    assert part.n_procs_y is None

    wiz.auto_tiling_chk.value = False
    wiz._rebuild()
    part = wiz.config.domain.partitioning
    assert part.auto_tiling is False
    assert part.n_procs_x == wiz.npx.value
    assert part.n_procs_y == wiz.npy.value


_CDR_SAMPLE_YAML = (
    Path(__file__).parent.parent
    / "applications"
    / "forge"
    / "fixtures"
    / "cdr_forcing_sample.yaml"
)


def _upload_change(content: bytes):
    """Build an ipywidgets FileUpload-style ``change`` dict for a single file."""
    return {"new": ({"name": "cdr.yaml", "content": content},)}


def test_cdr_upload_valid_yaml_gathers_into_config():
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.cdr_mode_dd.value = "yaml"

    wiz._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))

    assert wiz._cdr_forcing is not None
    assert "✓ CDR" in wiz.cdr_status.value
    assert wiz._gather()["cdr"]["cdr_forcing"] is wiz._cdr_forcing
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["cdr_forcing"] is True
    assert wiz.config.cdr.mode == "yaml"
    assert wiz.config.cdr.cdr_forcing["releases"]


def test_cdr_upload_invalid_yaml_surfaces_error_and_does_not_set_config():
    """Not a roms-tools CDRForcing document at all -- caught by
    ``read_cdr_forcing_yaml``'s own parse check, before roms-tools is ever imported.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    wiz._on_cdr_upload(_upload_change(b"---\nSomeOtherThing:\n  foo: bar\n"))

    assert wiz._cdr_forcing is None
    assert "invalid" in wiz.cdr_status.value.lower()
    # mode is still the default "none" (never touched by this test) -- no CDR
    # was gathered, and the rest of the config still resolves fine.
    assert wiz._gather()["cdr"] == {
        "mode": "none",
        "cdr_forcing": None,
        "cdr_forcing_file": None,
    }
    assert wiz.config is not None


def test_cdr_upload_semantically_broken_yaml_caught_by_eager_rt_construction():
    """A structurally-valid CDRForcing document (parses fine, has the right keys)
    but with start_time >= end_time. ``read_cdr_forcing_yaml`` has no opinion on
    this -- only the eager ``rt.CDRForcing(**parsed)`` construction in
    ``_on_cdr_upload`` catches it (roms-tools' own validator raises). This is the
    scenario the eager-validation design exists for: without it, this would embed
    silently into the blueprint and only fail much later, during blueprint processing.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    text = _CDR_SAMPLE_YAML.read_text()
    # swap start_time/end_time so parsing succeeds but roms-tools' own
    # `start_time < end_time` validator raises.
    swapped = text.replace(
        "start_time: '2012-01-01T00:00:00'\n  end_time: '2012-01-02T00:00:00'",
        "start_time: '2012-01-02T00:00:00'\n  end_time: '2012-01-01T00:00:00'",
    )
    assert swapped != text, "fixture format changed -- update this test's replace()"

    wiz._on_cdr_upload(_upload_change(swapped.encode("utf-8")))

    assert wiz._cdr_forcing is None
    assert "invalid" in wiz.cdr_status.value.lower()
    assert wiz._gather()["cdr"]["cdr_forcing"] is None


def test_cdr_clear_resets_state():
    wiz = ForgeBlueprintWizard()
    wiz.cdr_mode_dd.value = "yaml"
    wiz._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))
    assert wiz._cdr_forcing is not None

    wiz._on_cdr_clear(None)

    assert wiz._cdr_forcing is None
    assert wiz.cdr_status.value == ""
    assert wiz._gather()["cdr"]["cdr_forcing"] is None


def test_cdr_mode_yaml_with_nothing_uploaded_invalidates_the_blueprint():
    """Selecting "yaml" mode is itself a commitment to supplying a CDR forcing
    -- with nothing uploaded yet, CdrSpec's own validator rejects
    ``cdr_forcing=None``, and that (like any other resolver error) surfaces as
    an Invalid preview rather than silently falling back to "no CDR".
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    wiz.cdr_mode_dd.value = "yaml"

    assert wiz.config is None
    assert "Invalid" in wiz.derived.value


def test_cdr_forcing_round_trips_through_load(tmp_path):
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.cdr_mode_dd.value = "yaml"
    wiz._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))
    assert wiz.config is not None
    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    assert wiz2.cdr_mode_dd.value == "yaml"
    assert wiz2._cdr_forcing is not None

    # cdr_forcing is stored as a plain dict (no typed CDR model, by design -- see the
    # plan's "no typed CDR Pydantic model" note), so a bare YAML timestamp parses to
    # an in-memory datetime but re-serializes to an ISO string on the blueprint's own
    # to_yaml/from_yaml round trip. Compare with that normalized away.
    def _iso(t):
        return t.isoformat() if hasattr(t, "isoformat") else str(t)

    orig = [dict(r) for r in wiz._cdr_forcing["releases"]]
    back = [dict(r) for r in wiz2._cdr_forcing["releases"]]
    for r in (orig, back):
        for release in r:
            release["times"] = [_iso(t) for t in release["times"]]
    assert back == orig
    assert "✓ CDR loaded" in wiz2.cdr_status.value


# ===========================================================================
# CDR Overhaul WP5/WP6: mode dropdown, per-mode panels, plotting widget.
# ===========================================================================


def test_cdr_mode_switch_shows_and_hides_panels():
    wiz = ForgeBlueprintWizard()
    assert _display(wiz.cdr_simple_box) == "none"
    assert _display(wiz.cdr_yaml_box) == "none"
    assert _display(wiz.cdr_netcdf_box) == "none"
    assert _display(wiz.cdr_upscaled_box) == "none"
    assert _display(wiz.cdr_plot_box) == "none"

    wiz.cdr_mode_dd.value = "simple"
    assert _display(wiz.cdr_simple_box) == ""
    assert _display(wiz.cdr_yaml_box) == "none"
    assert _display(wiz.cdr_plot_box) == ""

    wiz.cdr_mode_dd.value = "yaml"
    assert _display(wiz.cdr_simple_box) == "none"
    assert _display(wiz.cdr_yaml_box) == ""
    assert _display(wiz.cdr_netcdf_box) == "none"
    assert _display(wiz.cdr_plot_box) == ""

    wiz.cdr_mode_dd.value = "netcdf"
    assert _display(wiz.cdr_yaml_box) == "none"
    assert _display(wiz.cdr_netcdf_box) == ""
    assert _display(wiz.cdr_plot_box) == ""

    wiz.cdr_mode_dd.value = "upscaled"
    assert _display(wiz.cdr_netcdf_box) == "none"
    assert _display(wiz.cdr_upscaled_box) == ""
    assert _display(wiz.cdr_plot_box) == "none"  # no plots for upscaled

    wiz.cdr_mode_dd.value = "none"
    assert _display(wiz.cdr_upscaled_box) == "none"
    assert _display(wiz.cdr_plot_box) == "none"


def test_cdr_yaml_and_netcdf_panels_link_to_docs():
    doc_url = "https://roms-tools.readthedocs.io/en/latest/cdr_forcing.html"
    wiz = ForgeBlueprintWizard()
    assert doc_url in wiz.cdr_yaml_box.children[0].value
    assert doc_url in wiz.cdr_netcdf_box.children[0].value


def test_cdr_simple_mode_seeds_defaults_on_activation():
    wiz = ForgeBlueprintWizard()
    wiz.grid_w["center_lat"].value = 12.5
    wiz.grid_w["center_lon"].value = -34.5
    wiz.start.value = date(2015, 3, 1)
    wiz.end.value = date(2015, 4, 1)

    wiz.cdr_mode_dd.value = "simple"

    assert wiz.cdr_simple_lat.value == 12.5
    assert wiz.cdr_simple_lon.value == -34.5
    assert wiz.cdr_simple_start.value == date(2015, 3, 1)
    assert wiz.cdr_simple_end.value == date(2015, 4, 1)
    assert wiz.cdr_simple_depth.value == 1.0
    assert wiz.cdr_simple_hsc.value == 10.0
    assert wiz.cdr_simple_vsc.value == 10.0
    assert wiz.cdr_simple_flux.value == 2 * 10**6


def test_cdr_simple_mode_compiles_flat_two_point_pulse():
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 3)
    wiz.cdr_mode_dd.value = "simple"
    wiz.cdr_simple_name.value = "my_release"
    wiz.cdr_simple_lat.value = 10.0
    wiz.cdr_simple_lon.value = 20.0
    wiz.cdr_simple_flux.value = 5.0

    cdr = wiz._gather()["cdr"]

    assert cdr["mode"] == "simple"
    forcing = cdr["cdr_forcing"]
    assert forcing["start_time"] == "2012-01-01T00:00:00"
    assert forcing["end_time"] == "2012-01-03T00:00:00"
    assert len(forcing["releases"]) == 1
    release = forcing["releases"][0]
    assert release["name"] == "my_release"
    assert release["lat"] == 10.0
    assert release["lon"] == 20.0
    assert release["times"] == ["2012-01-01T00:00:00", "2012-01-03T00:00:00"]
    assert release["tracer_fluxes"] == {"ALK": [5.0, 5.0]}
    assert release["release_type"] == "tracer_perturbation"
    assert isinstance(forcing["start_time"], str)  # ISO strings, not datetimes
    assert wiz.config is not None
    assert wiz.config.cdr.mode == "simple"
    assert wiz.config.cdr.cdr_forcing["releases"][0]["lat"] == 10.0


def test_cdr_simple_mode_round_trips_through_populate_from():
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.cdr_mode_dd.value = "simple"
    wiz.cdr_simple_name.value = "rt_release"
    wiz.cdr_simple_lat.value = 5.0
    wiz.cdr_simple_lon.value = 6.0
    wiz.cdr_simple_depth.value = 2.0
    wiz.cdr_simple_hsc.value = 15.0
    wiz.cdr_simple_vsc.value = 25.0
    wiz.cdr_simple_flux.value = 3.5e6
    assert wiz.config is not None

    wiz2 = ForgeBlueprintWizard()
    wiz2._populate_from(wiz.config)

    assert wiz2.cdr_mode_dd.value == "simple"
    assert wiz2.cdr_simple_name.value == "rt_release"
    assert wiz2.cdr_simple_lat.value == 5.0
    assert wiz2.cdr_simple_lon.value == 6.0
    assert wiz2.cdr_simple_depth.value == 2.0
    assert wiz2.cdr_simple_hsc.value == 15.0
    assert wiz2.cdr_simple_vsc.value == 25.0
    assert wiz2.cdr_simple_flux.value == 3.5e6
    assert wiz2.cdr_simple_start.value == date(2012, 1, 1)
    assert wiz2.cdr_simple_end.value == date(2012, 1, 2)
    assert wiz2.config.content_hash() == wiz.config.content_hash()


def test_cdr_upscaled_mode_round_trips_through_populate_from():
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.cdr_mode_dd.value = "upscaled"
    assert wiz.config is not None
    assert wiz.config.cdr.mode == "upscaled"

    wiz2 = ForgeBlueprintWizard()
    wiz2._populate_from(wiz.config)

    assert wiz2.cdr_mode_dd.value == "upscaled"
    assert wiz2._cdr_forcing is None
    assert wiz2._cdr_forcing_file is None
    assert wiz2.config.content_hash() == wiz.config.content_hash()


def test_composition_cdr_specref_provenance(tmp_path):
    import shutil

    from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog

    wiz = ForgeBlueprintWizard()
    assert wiz.config.composition.cdr.name == ""
    assert wiz.config.composition.cdr.origin == "custom"
    assert wiz.config.composition.cdr.modified is False

    root = tmp_path / "catalog"
    shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
    catalog = DomainCatalog(catalog_root=root)
    catalog.register_cdr(
        "prov-cdr",
        mode="yaml",
        cdr_forcing={
            "start_time": "2012-01-01T00:00:00",
            "end_time": "2012-01-02T00:00:00",
            "releases": [],
        },
    )
    wiz2 = ForgeBlueprintWizard(catalog=catalog)
    wiz2.cdr_dd.value = "prov-cdr"

    assert wiz2.config.composition.cdr.name == "prov-cdr"
    assert wiz2.config.composition.cdr.origin == "catalog"
    assert wiz2.config.composition.cdr.modified is False

    # The other seed path: _populate_from seeds composition.cdr.modified from
    # catalog.cdr_data(name) (minus "description"), while an authoring-time pick
    # (_on_cdr_spec, just exercised above) seeds it from _cdr_snapshot(). Those
    # must produce equal dicts for the same spec, or a save/load cycle would
    # disagree with the picker about whether the spec is modified -- exactly the
    # false-positive the forcing_seed comment in _populate_from exists to avoid.
    saved = tmp_path / "forge_blueprint.yaml"
    wiz2.config.to_yaml(saved)
    wiz3 = ForgeBlueprintWizard(catalog=catalog)
    wiz3.load_path.value = str(saved)
    wiz3._on_load_path(None)

    assert wiz3.cdr_dd.value == "prov-cdr"
    assert wiz3.config.composition.cdr.modified is False


def _fake_cdr_forcing_class(*, release_type="tracer_perturbation", names=("r1",)):
    """A stand-in for ``roms_tools.CDRForcing`` whose plot_* methods render a
    trivial figure (so plt.gcf()/savefig has something real to capture) without
    needing an actual grid/dataset. ``build_calls`` records each construction --
    tests assert on its length to prove the plot widget builds at most once per
    Generate click and never again on a plot-type/release switch.
    """
    import matplotlib.pyplot as plt

    build_calls: list[dict] = []

    class _FakeRelease:
        def __init__(self, name):
            self.name = name

    class _FakeCDRForcing:
        def __init__(self, **kwargs):
            build_calls.append(kwargs)
            self.release_type = release_type
            self.releases = [_FakeRelease(n) for n in names]
            self.release_tracers: dict[str, tuple[str, ...]] = {}  # all marbl

        def __getitem__(self, name):
            return next(r for r in self.releases if r.name == name)

        def plot_locations(self, *a, **k):
            plt.figure()

        def plot_distribution(self, release_name, *a, **k):
            plt.figure()

        def plot_tracer_flux(self, tracer_name, *a, **k):
            plt.figure()

    return _FakeCDRForcing, build_calls


def test_cdr_plot_generate_builds_once_and_switches_without_rebuild(monkeypatch):
    """WP6 cache: Generate builds the rt.CDRForcing once; switching plot type or
    release re-renders from the cached object/PNG WITHOUT rebuilding it (the
    plan's requirement) -- verified here by asserting ``build_calls`` never
    grows past 1 across two dropdown switches.
    """
    import roms_tools

    fake_cls, build_calls = _fake_cdr_forcing_class(names=("r1", "r2"))
    monkeypatch.setattr(roms_tools, "CDRForcing", fake_cls)

    wiz = ForgeBlueprintWizard()
    wiz.cdr_mode_dd.value = "yaml"
    wiz._cdr_forcing = {
        "start_time": "2012-01-01T00:00:00",
        "end_time": "2012-01-02T00:00:00",
        "releases": [],
    }
    wiz._rebuild()

    wiz._on_cdr_plot_generate(None)

    assert len(build_calls) == 1
    assert wiz._cdr_plot_object is not None
    # The "building…" marker must not stick around after a successful render.
    assert "building" not in wiz.cdr_plot_status.value
    assert "✓" in wiz.cdr_plot_status.value
    assert list(wiz.cdr_plot_release_dd.options) == ["r1", "r2"]
    assert ("Tracer flux (ALK)", "tracer_flux") in wiz.cdr_plot_type_dd.options
    assert _display(wiz.cdr_plot_release_dd) == ""  # >1 release -- shown
    assert wiz.cdr_plot_img.value

    wiz.cdr_plot_type_dd.value = "distribution"
    assert len(build_calls) == 1  # no rebuild
    assert wiz.cdr_plot_img.value

    wiz.cdr_plot_release_dd.value = "r2"
    assert len(build_calls) == 1  # still no rebuild
    assert wiz.cdr_plot_img.value


def test_cdr_plot_cache_invalidated_by_input_edit_and_mode_switch(monkeypatch):
    import roms_tools

    fake_cls, build_calls = _fake_cdr_forcing_class()
    monkeypatch.setattr(roms_tools, "CDRForcing", fake_cls)

    wiz = ForgeBlueprintWizard()
    wiz.cdr_mode_dd.value = "yaml"
    wiz._cdr_forcing = {
        "start_time": "2012-01-01T00:00:00",
        "end_time": "2012-01-02T00:00:00",
        "releases": [],
    }
    wiz._rebuild()
    wiz._on_cdr_plot_generate(None)
    assert len(build_calls) == 1
    assert wiz._cdr_plot_object is not None

    # An unrelated watched-widget edit goes through _rebuild() (the one hook
    # _invalidate_cdr_plot_cache is attached to), but the fingerprint check
    # sees no plot-affecting change -- the rendered plot SURVIVES.
    wiz.description.value = "something else"
    assert wiz._cdr_plot_object is not None
    assert wiz.cdr_plot_img.value != b""
    assert len(build_calls) == 1

    # A CDR-input edit changes the fingerprint and clears everything.
    wiz._cdr_forcing = {**wiz._cdr_forcing, "end_time": "2012-01-03T00:00:00"}
    wiz._rebuild()
    assert wiz._cdr_plot_object is None
    assert wiz._cdr_plot_cache == {}
    assert wiz.cdr_plot_img.value == b""

    wiz._on_cdr_plot_generate(None)
    assert len(build_calls) == 2

    # A grid-geometry edit also invalidates (the plot's grid is stale).
    wiz.grid_w["center_lat"].value = wiz.grid_w["center_lat"].value + 1.0
    assert wiz._cdr_plot_object is None
    assert wiz._cdr_plot_cache == {}

    wiz._on_cdr_plot_generate(None)
    assert len(build_calls) == 3

    # A mode switch also invalidates (leaving "yaml" clears _cdr_forcing too).
    wiz.cdr_mode_dd.value = "none"
    assert wiz._cdr_plot_object is None
    assert wiz._cdr_plot_cache == {}


def test_cdr_plot_hides_tracer_flux_for_volume_type_forcing(monkeypatch):
    import roms_tools

    fake_cls, _build_calls = _fake_cdr_forcing_class(release_type="volume")
    monkeypatch.setattr(roms_tools, "CDRForcing", fake_cls)

    wiz = ForgeBlueprintWizard()
    wiz.cdr_mode_dd.value = "yaml"
    wiz._cdr_forcing = {
        "start_time": "2012-01-01T00:00:00",
        "end_time": "2012-01-02T00:00:00",
        "releases": [],
    }
    wiz._rebuild()

    wiz._on_cdr_plot_generate(None)

    option_values = dict(wiz.cdr_plot_type_dd.options).values()
    assert "tracer_flux" not in option_values
    assert "volume-type" in wiz.cdr_plot_status.value


def test_cdr_forcing_from_netcdf_reconstructs_tracer_perturbation(tmp_path):
    """Integration test (real roms_tools, no mocking) for the module-level
    netcdf-reconstruction helper the plot widget's "netcdf" mode uses.
    """
    import numpy as np
    import roms_tools as rt
    import xarray as xr

    from cstar.wizard.wizard import _cdr_forcing_from_netcdf

    times = np.array(["2012-01-01", "2012-01-02", "2012-01-03"], dtype="datetime64[ns]")
    ds = xr.Dataset(
        {
            "cdr_time": ("time", [0.0, 1.0, 2.0]),
            "cdr_lon": ("ncdr", [0.5]),
            "cdr_lat": ("ncdr", [55.0]),
            "cdr_dep": ("ncdr", [5.0]),
            "cdr_hsc": ("ncdr", [15.0]),
            "cdr_vsc": ("ncdr", [25.0]),
            "cdr_trcflx": (
                ("time", "ntracers", "ncdr"),
                np.array([[[1.0], [10.0]], [[1.5], [11.0]], [[2.0], [12.0]]]),
            ),
        },
        coords={
            "time": times,
            "release_name": ("ncdr", ["r1"]),
            "tracer_name": ("ntracers", ["ALK", "DIC"]),
        },
    )
    path = tmp_path / "cdr.nc"
    ds.to_netcdf(path)
    grid = rt.Grid(
        nx=6, ny=2, size_x=500.0, size_y=1000.0, center_lon=0.0, center_lat=55.0, N=3
    )

    cdr = _cdr_forcing_from_netcdf(path, grid)

    assert cdr.release_type == "tracer_perturbation"
    assert [r.name for r in cdr.releases] == ["r1"]
    release = cdr.releases["r1"]
    assert release.lat == 55.0
    assert release.lon == 0.5
    assert release.depth == 5.0
    assert release.hsc == 15.0
    assert release.vsc == 25.0
    assert [v for v in release.tracer_fluxes["ALK"].values] == [1.0, 1.5, 2.0]


_CDR_TIMES = [datetime(2012, 1, 1), datetime(2012, 1, 2), datetime(2012, 1, 3)]


def _small_cdr_grid():
    import roms_tools as rt

    return rt.Grid(
        nx=6, ny=2, size_x=500.0, size_y=1000.0, center_lon=0.0, center_lat=55.0, N=3
    )


def _tracer_perturbation(name, lon, fluxes, tracer_set):
    import roms_tools as rt

    return rt.TracerPerturbation(
        name=name,
        lat=55.0,
        lon=lon,
        depth=5.0,
        hsc=15.0,
        vsc=25.0,
        times=_CDR_TIMES,
        tracer_set=tracer_set,
        tracer_fluxes=fluxes,
    )


def _save_cdr_forcing(tmp_path, releases, grid=None):
    """A real roms-tools ``CDRForcing`` of ``releases`` saved with roms-tools' own
    writer; returns it with the file's path.
    """
    import roms_tools as rt

    cdr = rt.CDRForcing(
        grid=grid if grid is not None else _small_cdr_grid(),
        start_time=_CDR_TIMES[0],
        end_time=_CDR_TIMES[-1],
        releases=releases,
    )
    [path] = cdr.save(tmp_path / "cdr.nc")
    return cdr, path


def _cdr_lite_releases():
    return [
        _tracer_perturbation(
            "oae",
            0.5,
            {"ALK": [1.0, 1.5, 2.0], "DIC": [10.0, 11.0, 12.0]},
            "cdr_lite",
        ),
        _tracer_perturbation("dor", 0.6, {"DIC": [-3.0, -4.0, -5.0]}, "cdr_lite"),
    ]


def _marbl_and_cdr_lite_releases():
    return [
        _tracer_perturbation("m", 0.4, {"ALK": [7.0, 8.0, 9.0]}, "marbl"),
        _tracer_perturbation("dor", 0.6, {"DIC": [-3.0, -4.0, -5.0]}, "cdr_lite"),
    ]


def _volume_release(name, lon, concentrations, tracer_set):
    import roms_tools as rt

    return rt.VolumeRelease(
        name=name,
        lat=55.0,
        lon=lon,
        depth=5.0,
        hsc=15.0,
        vsc=25.0,
        times=_CDR_TIMES,
        tracer_set=tracer_set,
        volume_fluxes=[1.0, 2.0, 3.0],
        tracer_concentrations=concentrations,
    )


def _fluxes(release):
    return {key: list(flux.values) for key, flux in release.tracer_fluxes.items()}


def _concentrations(release, *keys):
    return {key: list(release.tracer_concentrations[key].values) for key in keys}


def test_cdr_forcing_from_netcdf_reconstructs_cdr_lite_releases(tmp_path):
    """Regression: the tracer axis of a ``cdr_lite`` file (``CDR_OAE_ALK<k>``, ...)
    is not a MARBL tracer set, which roms-tools' ``TracerPerturbation`` defaults
    to -- the reconstruction named every column as a MARBL tracer and was rejected.
    """
    from cstar.wizard.wizard import _cdr_forcing_from_netcdf

    grid = _small_cdr_grid()
    original, path = _save_cdr_forcing(tmp_path, _cdr_lite_releases(), grid)

    cdr = _cdr_forcing_from_netcdf(path, grid)

    assert [r.name for r in cdr.releases] == ["oae", "dor"]
    assert cdr.tracer_set == "cdr_lite"
    oae, dor = cdr.releases["oae"], cdr.releases["dor"]
    assert (oae.is_oae, dor.is_dor) == (True, True)
    assert _fluxes(oae) == {"ALK": [1.0, 1.5, 2.0], "DIC": [10.0, 11.0, 12.0]}
    assert _fluxes(dor) == {"DIC": [-3.0, -4.0, -5.0]}
    assert (oae.lon, dor.lon, oae.depth, oae.hsc, oae.vsc) == (
        0.5,
        0.6,
        5.0,
        15.0,
        25.0,
    )
    assert cdr.release_tracers == original.release_tracers


@pytest.fixture
def agg_backend():
    """Headless matplotlib for the tests that draw real roms-tools plots."""
    import matplotlib.pyplot as plt

    previous = plt.get_backend()
    plt.switch_backend("Agg")
    yield
    plt.switch_backend(previous)


def test_cdr_forcing_from_netcdf_cdr_lite_forcing_plots_without_network(
    tmp_path, monkeypatch, agg_backend
):
    """The reconstructed forcing's plots draw, with no download (cartopy's own
    data directory is empty; the unit-test network guard is on).
    """
    import cartopy

    from cstar.wizard.wizard import _cdr_forcing_from_netcdf

    grid = _small_cdr_grid()
    _, path = _save_cdr_forcing(tmp_path, _cdr_lite_releases(), grid)
    cdr = _cdr_forcing_from_netcdf(path, grid)
    monkeypatch.setitem(cartopy.config, "data_dir", str(tmp_path / "cartopy"))

    cdr.plot_locations()
    for release, tracers in cdr.release_tracers.items():
        cdr.plot_tracer_flux(tracers[0], release_names=[release])


def test_cdr_forcing_from_netcdf_reconstructs_passive_releases(tmp_path):
    from cstar.wizard.wizard import _cdr_forcing_from_netcdf

    grid = _small_cdr_grid()
    original, path = _save_cdr_forcing(
        tmp_path,
        [
            _tracer_perturbation(
                "dye1", 0.5, {"passive_tracer": [1.0, 2.0, 3.0]}, "passive"
            ),
            _tracer_perturbation(
                "dye2", 0.6, {"passive_tracer": [4.0, 5.0, 6.0]}, "passive"
            ),
        ],
        grid,
    )

    cdr = _cdr_forcing_from_netcdf(path, grid)

    assert cdr.tracer_set == "passive"
    assert _fluxes(cdr.releases["dye1"]) == {"passive_tracer": [1.0, 2.0, 3.0]}
    assert _fluxes(cdr.releases["dye2"]) == {"passive_tracer": [4.0, 5.0, 6.0]}
    assert cdr.release_tracers == original.release_tracers


def test_cdr_forcing_from_netcdf_keeps_marbl_releases_beside_generated_tracers(
    tmp_path,
):
    """A MARBL release mixed in with a ``cdr_lite`` one owns no generated tracer
    (the file's ``tracer_release`` is empty for its rows): it is rebuilt as before,
    ALK-only, and the ``cdr_lite`` one from its own columns.
    """
    from cstar.wizard.wizard import _cdr_forcing_from_netcdf

    grid = _small_cdr_grid()
    original, path = _save_cdr_forcing(tmp_path, _marbl_and_cdr_lite_releases(), grid)

    cdr = _cdr_forcing_from_netcdf(path, grid)

    assert cdr.releases["m"].tracer_set == "marbl"
    assert _fluxes(cdr.releases["m"])["ALK"] == [7.0, 8.0, 9.0]
    assert cdr.releases["dor"].tracer_set == "cdr_lite"
    assert _fluxes(cdr.releases["dor"]) == {"DIC": [-3.0, -4.0, -5.0]}
    assert cdr.release_tracers == original.release_tracers


def test_cdr_forcing_from_netcdf_reconstructs_passive_volume_releases(tmp_path):
    """A passive volume release keeps its dye concentration under its own tracer
    set (``temp``/``salt`` are left to roms-tools' defaults).
    """
    from cstar.wizard.wizard import _cdr_forcing_from_netcdf

    grid = _small_cdr_grid()
    original, path = _save_cdr_forcing(
        tmp_path,
        [
            _volume_release(
                "dye1", 0.5, {"passive_tracer": [1.0, 2.0, 3.0]}, "passive"
            ),
            _volume_release(
                "dye2", 0.6, {"passive_tracer": [4.0, 5.0, 6.0]}, "passive"
            ),
        ],
        grid,
    )

    cdr = _cdr_forcing_from_netcdf(path, grid)

    assert cdr.release_type == "volume"
    assert cdr.tracer_set == "passive"
    assert _concentrations(cdr.releases["dye1"], "passive_tracer") == {
        "passive_tracer": [1.0, 2.0, 3.0]
    }
    assert _concentrations(cdr.releases["dye2"], "passive_tracer") == {
        "passive_tracer": [4.0, 5.0, 6.0]
    }
    assert list(cdr.releases["dye2"].volume_fluxes.values) == [1.0, 2.0, 3.0]
    assert cdr.release_tracers == original.release_tracers


def test_cdr_forcing_from_netcdf_keeps_a_marbl_volume_release_off_the_dye_columns(
    tmp_path,
):
    from cstar.wizard.wizard import _cdr_forcing_from_netcdf

    grid = _small_cdr_grid()
    original, path = _save_cdr_forcing(
        tmp_path,
        [
            _volume_release("m", 0.4, {"ALK": [7.0, 8.0, 9.0]}, "marbl"),
            _volume_release("dye", 0.5, {"passive_tracer": [1.0, 2.0, 3.0]}, "passive"),
        ],
        grid,
    )

    cdr = _cdr_forcing_from_netcdf(path, grid)

    marbl, dye = cdr.releases["m"], cdr.releases["dye"]
    assert (marbl.tracer_set, dye.tracer_set) == ("marbl", "passive")
    assert "passive_tracer1" not in marbl.tracer_concentrations
    assert _concentrations(marbl, "ALK") == {"ALK": [7.0, 8.0, 9.0]}
    assert _concentrations(dye, "passive_tracer") == {"passive_tracer": [1.0, 2.0, 3.0]}
    assert cdr.release_tracers == original.release_tracers


def test_cdr_forcing_from_netcdf_requires_tracer_release_for_generated_tracers(
    tmp_path,
):
    import xarray as xr

    from cstar.wizard.wizard import _cdr_forcing_from_netcdf

    grid = _small_cdr_grid()
    _, path = _save_cdr_forcing(tmp_path, _cdr_lite_releases(), grid)
    stripped = tmp_path / "stripped.nc"
    with xr.open_dataset(path) as ds:
        ds.drop_vars("tracer_release").to_netcdf(stripped)

    with pytest.raises(
        ValueError, match=r"could not reconstruct releases from .*tracer_release"
    ):
        _cdr_forcing_from_netcdf(stripped, grid)


def _spy_on_plot_tracer_flux(monkeypatch):
    """Record every ``(tracer_name, release_names)`` ``CDRForcing.plot_tracer_flux``
    is asked to plot, still drawing it.
    """
    import roms_tools as rt

    calls: list[tuple[str, list[str] | None]] = []
    real = rt.CDRForcing.plot_tracer_flux

    def spy(self, tracer_name, *args, release_names=None, **kwargs):
        calls.append((tracer_name, release_names))
        extra = {} if release_names is None else {"release_names": release_names}
        return real(self, tracer_name, *args, **extra, **kwargs)

    monkeypatch.setattr(rt.CDRForcing, "plot_tracer_flux", spy)
    return calls


def test_cdr_plot_of_an_attached_cdr_lite_file_draws_each_releases_own_tracer(
    tmp_path, monkeypatch, agg_backend
):
    """The reported failure end to end: attach a ``cdr_lite`` file in "netcdf"
    mode, Generate, and plot release locations and each release's tracer flux.
    """
    import cartopy

    grid = _small_cdr_grid()
    _, path = _save_cdr_forcing(tmp_path, _cdr_lite_releases(), grid)
    monkeypatch.setitem(cartopy.config, "data_dir", str(tmp_path / "cartopy"))
    flux_calls = _spy_on_plot_tracer_flux(monkeypatch)
    wiz = _cdr_lite_wizard()
    monkeypatch.setattr(wiz, "_build_grid_from_widgets", lambda: grid)
    wiz.cdr_mode_dd.value = "netcdf"
    wiz._attach_cdr_file_from_path(str(path))
    assert "no CDR_* tracers" not in wiz.cdr_file_status.value

    wiz._on_cdr_plot_generate(None)

    assert "✓" in wiz.cdr_plot_status.value, wiz.cdr_plot_status.value
    assert wiz.cdr_plot_img.value  # "Release locations" drew
    assert list(wiz.cdr_plot_release_dd.options) == ["oae", "dor"]
    assert ("Tracer flux", "tracer_flux") in wiz.cdr_plot_type_dd.options
    assert "Tracer flux (ALK)" not in dict(wiz.cdr_plot_type_dd.options)

    wiz.cdr_plot_type_dd.value = "tracer_flux"
    wiz.cdr_plot_release_dd.value = "dor"

    assert flux_calls == [
        ("CDR_OAE_ALK1", ["oae"]),
        ("CDR_DOR_DIC1", ["dor"]),
    ]
    assert {("tracer_flux", "oae"), ("tracer_flux", "dor")} <= set(wiz._cdr_plot_cache)


def test_cdr_plot_of_the_simple_release_follows_the_bgc_mode(
    tmp_path, monkeypatch, agg_backend
):
    """Under ``bgc_mode: cdr_lite`` the resolver gives a simple release the
    ``cdr_lite`` tracer set; the plot shows that forcing, not the MARBL default.
    """
    import cartopy

    monkeypatch.setitem(cartopy.config, "data_dir", str(tmp_path / "cartopy"))
    flux_calls = _spy_on_plot_tracer_flux(monkeypatch)
    grid = _small_cdr_grid()
    wiz = _cdr_lite_wizard()
    monkeypatch.setattr(wiz, "_build_grid_from_widgets", lambda: grid)
    wiz.cdr_simple_name.value = "pulse"
    wiz.cdr_simple_lat.value = 55.0
    wiz.cdr_simple_lon.value = 0.5  # off the coarse grid's coastal cells
    wiz.cdr_simple_depth.value = 5.0

    wiz._on_cdr_plot_generate(None)

    assert "✓" in wiz.cdr_plot_status.value, wiz.cdr_plot_status.value
    cdr = wiz._cdr_plot_object
    assert cdr.tracer_set == "cdr_lite"
    assert cdr.release_tracers == {"pulse": ("CDR_OAE_ALK1", "CDR_OAE_DIC1")}
    assert ("Tracer flux", "tracer_flux") in wiz.cdr_plot_type_dd.options

    wiz.cdr_plot_type_dd.value = "tracer_flux"

    assert flux_calls == [("CDR_OAE_ALK1", ["pulse"])]
    assert wiz.cdr_plot_img.value

    # A BGC-mode switch changes what the resolver would generate: drop the plot.
    wiz.bgc_dd.value = "marbl"
    assert wiz._cdr_plot_object is None
    wiz._on_cdr_plot_generate(None)
    assert wiz._cdr_plot_object.tracer_set == "marbl"
    assert ("Tracer flux (ALK)", "tracer_flux") in wiz.cdr_plot_type_dd.options


def test_cdr_plot_of_a_mixed_forcing_draws_only_the_selected_marbl_release(
    tmp_path, monkeypatch, agg_backend
):
    """A MARBL release beside a ``cdr_lite`` one plots its own ``ALK`` -- for that
    release only, not every release's MARBL ALK row (flat zeros for the others).
    """
    import cartopy

    grid = _small_cdr_grid()
    _, path = _save_cdr_forcing(tmp_path, _marbl_and_cdr_lite_releases(), grid)
    monkeypatch.setitem(cartopy.config, "data_dir", str(tmp_path / "cartopy"))
    flux_calls = _spy_on_plot_tracer_flux(monkeypatch)
    wiz = _cdr_lite_wizard()
    monkeypatch.setattr(wiz, "_build_grid_from_widgets", lambda: grid)
    wiz.cdr_mode_dd.value = "netcdf"
    wiz._attach_cdr_file_from_path(str(path))

    wiz._on_cdr_plot_generate(None)
    assert "✓" in wiz.cdr_plot_status.value, wiz.cdr_plot_status.value
    assert ("Tracer flux", "tracer_flux") in wiz.cdr_plot_type_dd.options

    wiz.cdr_plot_type_dd.value = "tracer_flux"  # the first release, "m"
    wiz.cdr_plot_release_dd.value = "dor"

    assert flux_calls == [("ALK", ["m"]), ("CDR_DOR_DIC1", ["dor"])]


def test_cdr_plot_of_an_attached_file_survives_a_bgc_mode_change(
    tmp_path, monkeypatch, agg_backend
):
    """Only a simple release's tracer set follows the BGC mode; a plot built from
    an attached file does not depend on it and is kept.
    """
    import cartopy

    grid = _small_cdr_grid()
    _, path = _save_cdr_forcing(tmp_path, _cdr_lite_releases(), grid)
    monkeypatch.setitem(cartopy.config, "data_dir", str(tmp_path / "cartopy"))
    wiz = _cdr_lite_wizard()
    monkeypatch.setattr(wiz, "_build_grid_from_widgets", lambda: grid)
    wiz.cdr_mode_dd.value = "netcdf"
    wiz._attach_cdr_file_from_path(str(path))
    wiz._on_cdr_plot_generate(None)
    built = wiz._cdr_plot_object
    assert built is not None

    wiz.bgc_dd.value = "marbl"

    assert wiz._cdr_plot_object is built
    assert wiz.cdr_plot_img.value


def test_cdr_plot_fingerprint_tolerates_a_selection_the_build_rejects(monkeypatch):
    """A simple-mode volume release is rejected for ``bgc_mode: cdr_lite``: the
    plot's cache check must not raise, and building the plot reports the error.
    """
    wiz = _cdr_lite_wizard()
    forcing = wiz._simple_cdr_forcing_dict()
    volume = {**forcing["releases"][0], "release_type": "volume"}
    monkeypatch.setattr(
        wiz, "_simple_cdr_forcing_dict", lambda: {**forcing, "releases": [volume]}
    )

    wiz._cdr_plot_inputs_fingerprint()
    wiz._on_cdr_plot_generate(None)

    assert "volume releases are not supported" in wiz.cdr_plot_status.value


def test_cdr_forcing_from_netcdf_wraps_errors_with_path(tmp_path):
    from cstar.wizard.wizard import _cdr_forcing_from_netcdf

    path = tmp_path / "not_cdr.nc"
    import xarray as xr

    xr.Dataset({"foo": ("x", [1, 2, 3])}).to_netcdf(path)

    with pytest.raises(ValueError, match="could not reconstruct releases"):
        _cdr_forcing_from_netcdf(path, grid=None)


def test_ntides_syncs_from_tidal_forcing_into_model_settings():
    """Item 6: the tidal forcing item's ntides drives model_settings['tides']['ntides'],
    not just the run-time-defaults placeholder (10).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    tidal_rows = wiz._forcing_editor._rows["tidal"]
    assert tidal_rows, "expected a default tidal row from the model's forcing inputs"
    # Default model forcing sets ntides=15 -> must already have synced, not left at 10.
    assert wiz.config.model_settings["tides"]["ntides"] == 15

    tidal_rows[0]["ntides"].value = 8
    wiz._rebuild()
    assert wiz.config.model_settings["tides"]["ntides"] == 8

    # A manual Advanced-settings override still wins over the tidal-forcing value.
    wiz._overrides[("tides", "ntides")] = 42
    wiz._rebuild()
    assert wiz.config.model_settings["tides"]["ntides"] == 42


def test_wizard_editor_rebuilds_across_roms_ref_schema_boundary():
    """Overriding the ``roms_ref`` box across the ucla-roms 0.5.0 line (with the
    same ModelSpec selected) must regenerate the Advanced-settings editor
    against the matching RunTimeSettings variant -- not just on a model switch.

    Explicitly sets a pre-0.5.0 override throughout (rather than relying on the
    default model's own pin, which is a "main" branch ref -- a non-semver ref
    resolves to the *latest* schema, so it can't stand in for "legacy" here).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.roms_ref.value = "0.2.0"
    wiz._rebuild()
    assert ("ocean_vars", "nrpf_rst") in wiz.editor._widgets

    wiz.roms_ref.value = "0.5.0"
    wiz._rebuild()
    assert ("ocean_vars", "nrpf_rst") not in wiz.editor._widgets

    wiz.roms_ref.value = "0.2.0"  # back across the boundary
    wiz._rebuild()
    assert ("ocean_vars", "nrpf_rst") in wiz.editor._widgets


def test_output_spec_defaults_to_daily_restarts():
    """The Output dropdown preselects the precheck-safe 'daily-restarts' spec
    (explicitly, not by sort position); 'standard' stays available for
    back-compat.
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.output_dd.value == "daily-restarts"
    assert "standard" in wiz.catalog.output_names


def test_default_model_uses_latest_settings_schema():
    """The default catalog model (``roms-marbl-0.16-default``) is pinned to
    ucla-roms ``0.16.0`` -- the newest registered schema boundary, which both
    the wizard and the executor (``write_roms_namelist`` ->
    ``run_time_settings_for_ref``) resolve to the *latest* known schema
    (currently ``RunTimeSettingsV0_16_0``), not the legacy one. This is an
    intentional behavior change from before this ref-awareness was added (the
    editor used to hardcode legacy ``RunTimeSettings``) -- it pins that the
    wizard now agrees with what the executor will actually write. (The tests in tests/test_forge_blueprint.py
    that exercise the ``nrpf_rst`` widget rules now pin a legacy ref
    explicitly.)
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz._editor_key.settings_cls is RunTimeSettingsV0_16_0
    assert ("ocean_vars", "nrpf_rst") not in wiz.editor._widgets


def test_advection_cppdefs_editable_only_for_models_that_declare_them():
    """``parabolic_splines``/``upstream_ts_land_curv`` (ucla-roms >= 0.8.0,
    PR #361) are opted into the "Physics & subgrid tuning" pane via
    ``_CPPDEFS_PANE_FIELDS``, like ``sponge_tune``. Unlike ``sponge_tune``
    (declared by every ModelSpec), these two keys are absent from ModelSpecs
    pinned below 0.8.0 -- the editor type-infers a widget from the composed
    dict, so a spec that omits the key must show no widget for it at all.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["parabolic_splines"] is False
    assert wiz.config.model_settings["cppdefs"]["upstream_ts_land_curv"] is False
    assert ("cppdefs", "parabolic_splines") in wiz.editor._widgets
    assert ("cppdefs", "upstream_ts_land_curv") in wiz.editor._widgets
    assert wiz.editor._widgets[("cppdefs", "parabolic_splines")][0].value is False
    assert wiz.editor._widgets[("cppdefs", "upstream_ts_land_curv")][0].value is False
    # cppdefs is never modeled by RomsNamelist, so the schema tooltip is empty;
    # the glossary hint (the "< 0.8.0 ignores this" caveat) must reach the widget.
    for key in ("parabolic_splines", "upstream_ts_land_curv"):
        assert "0.8.0" in wiz.editor._widgets[("cppdefs", key)][0].tooltip

    wiz.show_archived_models.value = True
    wiz.model_dd.value = "roms-marbl-0.7-default"
    wiz._rebuild()
    assert "parabolic_splines" not in wiz.config.model_settings["cppdefs"]
    assert "upstream_ts_land_curv" not in wiz.config.model_settings["cppdefs"]
    assert ("cppdefs", "parabolic_splines") not in wiz.editor._widgets
    assert ("cppdefs", "upstream_ts_land_curv") not in wiz.editor._widgets


def test_advection_cppdefs_editable_via_advanced_settings_accordion(tmp_path):
    """Mirrors ``test_sponge_tune_editable_via_advanced_settings_accordion``:
    toggling the ``parabolic_splines`` widget records an override (leaving the
    sibling ``upstream_ts_land_curv`` flag untouched), and the override
    survives a save/load round trip.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.composition.model.modified is False

    widget, _ = wiz.editor._widgets[("cppdefs", "parabolic_splines")]
    widget.value = True  # fires _on_editor_edit -> records the override
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["parabolic_splines"] is True
    assert wiz.config.model_settings["cppdefs"]["upstream_ts_land_curv"] is False
    assert wiz.config.composition.model.modified is True

    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    assert wiz2.config is not None
    assert wiz2.config.model_settings["cppdefs"]["parabolic_splines"] is True
    assert wiz2.config.model_settings["cppdefs"]["upstream_ts_land_curv"] is False
    assert wiz2.config.composition.model.modified is True
    assert wiz2.editor._widgets[("cppdefs", "parabolic_splines")][0].value is True


# The wizard's own default for the key in bgc_mode cdr_lite, as composition.overrides
# records it.
_INTERP_HOLD = {"cdr_lite": {_INTERP_KEY[1]: False}}


def test_0_12_model_declares_the_time_interpolation_key():
    """``roms-marbl-0.12-default`` carries the key at its default; the MARBL
    default mode builds no widget for it, and ``roms-marbl-0.11-default`` (a
    release that stops on the key) does not declare it.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.model_dd.value = "roms-marbl-0.12-default"
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz.config.model_settings["cdr_lite"] == {_INTERP_KEY[1]: True}
    assert _INTERP_KEY not in wiz.editor._widgets

    wiz.show_archived_models.value = True
    wiz.model_dd.value = "roms-marbl-0.11-default"
    wiz._rebuild()
    assert wiz.config is not None
    assert "cdr_lite" not in wiz.config.model_settings


class TestCarbonateSensitivityTimeInterpolation:
    """The time-interpolation switch sits in the Model card's carbonate
    sensitivity group, and the wizard starts it off in ``bgc_mode: cdr_lite``:
    UCLA-ROMS writes the sensitivities time-averaged by default.
    """

    @staticmethod
    def _wizard():
        wiz = ForgeBlueprintWizard()
        wiz.start.value = date(2012, 1, 1)
        wiz.end.value = date(2012, 1, 2)
        wiz.use_pio_chk.value = False
        wiz.cdr_mode_dd.value = "simple"  # cdr_lite needs a CDR forcing to resolve
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config is not None, wiz.derived.value
        return wiz

    @staticmethod
    def _checkbox(wiz):
        return wiz.editor._widgets[_INTERP_KEY][0]

    def test_cdr_lite_starts_with_the_hold_in_the_model_card(self):
        from cstar.wizard.wizard import _DEFAULT_MODEL

        wiz = self._wizard()
        assert wiz.model_dd.value == _DEFAULT_MODEL
        checkbox = self._checkbox(wiz)
        assert checkbox.value is False
        assert wiz.cs_interp_box.children == (wiz.editor.model_box, wiz.cs_interp_note)
        assert checkbox in _descendants(wiz.cs_group)
        assert checkbox not in _descendants(wiz.editor_box)
        assert "time-averaged by default" in wiz.cs_interp_note.value
        assert wiz.config.model_settings["cdr_lite"][_INTERP_KEY[1]] is False
        assert wiz.config.composition.overrides == _INTERP_HOLD
        # an honest deviation from the ModelSpec, but not an Advanced-pane edit
        assert wiz.config.composition.model.modified is True
        assert "All defaults" in wiz.card_chips["advanced"].value

    def test_marbl_keeps_the_model_spec_value_and_shows_no_switch(self):
        wiz = self._wizard()
        wiz.bgc_dd.value = "marbl"
        assert wiz.config is not None, wiz.derived.value
        assert wiz.config.model_settings["cdr_lite"] == {_INTERP_KEY[1]: True}
        assert wiz.config.composition.overrides == {}
        assert wiz.cs_interp_box.children == ()

    def test_a_user_choice_survives_mode_switches(self):
        wiz = self._wizard()
        self._checkbox(wiz).value = True
        assert wiz.config.model_settings["cdr_lite"][_INTERP_KEY[1]] is True
        wiz.bgc_dd.value = "marbl"
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config is not None, wiz.derived.value
        assert self._checkbox(wiz).value is True
        assert wiz.config.model_settings["cdr_lite"][_INTERP_KEY[1]] is True
        assert "1 modified" not in wiz.card_chips["advanced"].value

    def test_a_model_pick_seeds_the_default_again(self):
        wiz = self._wizard()
        self._checkbox(wiz).value = True
        wiz.show_archived_models.value = True
        wiz.model_dd.value = "roms-marbl-0.11-default"
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config is not None, wiz.derived.value
        assert _INTERP_KEY not in wiz.editor._widgets  # no such switch before 0.12
        assert _INTERP_KEY not in wiz._overrides
        assert wiz.cs_interp_box.children == ()

        wiz.model_dd.value = "roms-marbl-0.12-default"
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config is not None, wiz.derived.value
        assert self._checkbox(wiz).value is False

    def test_an_older_roms_ref_drops_the_default_instead_of_failing(self):
        wiz = self._wizard()
        wiz.roms_ref.value = "0.11.0"
        assert wiz.config is not None, wiz.derived.value
        assert _INTERP_KEY not in wiz._overrides
        wiz.roms_ref.value = ""
        assert wiz.config is not None, wiz.derived.value
        assert self._checkbox(wiz).value is False

    def test_the_switch_shows_before_the_blueprint_is_valid(self):
        """cdr_lite does not resolve until a CDR forcing is picked; the switch
        shows, at the wizard's default, as soon as the mode is picked, and a choice
        made then reaches the blueprint once it resolves. Advanced and Output wait
        for the valid blueprint.
        """
        wiz = ForgeBlueprintWizard()
        wiz.start.value = date(2012, 1, 1)
        wiz.end.value = date(2012, 1, 2)
        wiz.use_pio_chk.value = False
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config is None

        checkbox = self._checkbox(wiz)
        assert checkbox.value is False
        assert wiz.cs_interp_box.children == (wiz.editor.model_box, wiz.cs_interp_note)
        assert checkbox in _descendants(wiz.cs_group)
        for box in (wiz.editor_box, wiz.output_editor_box):
            (placeholder,) = box.children
            assert "once the blueprint is valid" in placeholder.value

        checkbox.value = True
        assert wiz.config is None
        assert self._checkbox(wiz) is checkbox  # same selection: not rebuilt
        assert wiz._overrides[_INTERP_KEY] is True

        wiz.cdr_mode_dd.value = "simple"
        assert wiz.config is not None, wiz.derived.value
        assert wiz._editor_key.resolved
        assert wiz.editor_box.children == (wiz.editor.accordion,)
        assert self._checkbox(wiz).value is True
        assert wiz.config.model_settings["cdr_lite"][_INTERP_KEY[1]] is True
        assert wiz.config.composition.overrides == {"cdr_lite": {_INTERP_KEY[1]: True}}

    @pytest.mark.parametrize("pick", ["model", "roms_ref"])
    def test_an_older_release_drops_the_switch_on_an_invalid_blueprint(self, pick):
        wiz = ForgeBlueprintWizard()
        wiz.start.value = date(2012, 1, 1)
        wiz.end.value = date(2012, 1, 2)
        wiz.use_pio_chk.value = False
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config is None
        assert wiz.cs_interp_box.children

        if pick == "model":
            wiz.show_archived_models.value = True
            wiz.model_dd.value = "roms-marbl-0.11-default"
            wiz.bgc_dd.value = "cdr_lite"
        else:
            wiz.roms_ref.value = "0.11.0"

        assert wiz.config is None
        assert wiz.cs_interp_box.children == ()
        assert _INTERP_KEY not in wiz._overrides

    def test_a_loaded_blueprint_keeps_its_own_value(self):
        wiz = self._wizard()
        self._checkbox(wiz).value = True

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)

        assert wiz2.config is not None, wiz2.derived.value
        assert self._checkbox(wiz2).value is True
        assert wiz2.config.model_settings["cdr_lite"][_INTERP_KEY[1]] is True
        assert _INTERP_KEY not in wiz2._overrides


def test_wizard_editor_cdr_output_streams_gated_by_model_spec_pin():
    """End-to-end sibling of the two tests above, driven through the wizard's
    model selector instead of ``_SettingsEditor`` directly: ``roms-marbl-0.6-
    default`` pins ucla-roms 0.6.0, so the resolver's
    ``prune_version_gated_sections`` drops ``cdr_lite_output``/
    ``cdr_gas_exch_output`` from ``model_settings`` entirely (they're
    OutputSpec-owned -- the default 'daily-restarts' OutputSpec carries them --
    but this pin's schema can't model them), and the editor never sees the
    section at all. Switching to ``roms-marbl-0.7-default`` (0.7.0) keeps both
    sections and their widgets.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    wiz.show_archived_models.value = True
    wiz.model_dd.value = "roms-marbl-0.6-default"
    wiz._rebuild()
    assert "cdr_lite_output" not in wiz.config.model_settings
    assert "cdr_gas_exch_output" not in wiz.config.model_settings
    assert ("cdr_lite_output", "do_cdr_lite_output") not in wiz.editor._widgets
    assert (
        "cdr_gas_exch_output",
        "do_cdr_gas_exch_output",
    ) not in wiz.editor._widgets

    wiz.model_dd.value = "roms-marbl-0.7-default"
    wiz._rebuild()
    assert "cdr_lite_output" in wiz.config.model_settings
    assert "cdr_gas_exch_output" in wiz.config.model_settings
    assert ("cdr_lite_output", "do_cdr_lite_output") in wiz.editor._widgets
    assert ("cdr_gas_exch_output", "do_cdr_gas_exch_output") in wiz.editor._widgets

    for model, cdr_lite in (
        ("roms-marbl-0.9-default", None),
        ("roms-marbl-0.10-default", None),
        ("roms-marbl-0.11-default", None),
        # From 0.12 the spec declares ``cdr_lite`` with only the time-interpolation
        # switch, which the default MARBL mode does not show.
        (
            "roms-marbl-0.12-default",
            {"cdr_carbonate_sensitivity_time_interpolation": True},
        ),
        (
            "roms-marbl-0.13-default",
            {"cdr_carbonate_sensitivity_time_interpolation": True},
        ),
        (
            "roms-marbl-0.14-default",
            {"cdr_carbonate_sensitivity_time_interpolation": True},
        ),
        (
            "roms-marbl-0.15-default",
            {"cdr_carbonate_sensitivity_time_interpolation": True},
        ),
        (
            "roms-marbl-0.16-default",
            {"cdr_carbonate_sensitivity_time_interpolation": True},
        ),
    ):
        wiz.model_dd.value = model
        wiz._rebuild()
        assert "cdr_lite_output" in wiz.config.model_settings
        assert ("cdr_lite_output", "do_cdr_lite_output") in wiz.editor._widgets
        assert ("cdr_lite_output", "wrt_gas_exchange") in wiz.editor._widgets
        # No bundled spec declares the ``cdr_lite`` online knob (CDR-lite tracers
        # need forcing Forge cannot generate), so there is no widget for it.
        assert wiz.config.model_settings.get("cdr_lite") == cdr_lite
        assert (
            "cdr_lite",
            "cdr_online_carbonate_sensitivity",
        ) not in wiz.editor._widgets


def test_cdr_tracer_counts_survive_wizard_save_and_load_round_trip(tmp_path):
    """``param.nt_cdr_oae``/``param.nt_cdr_dor`` (ucla-roms >= 0.4.0's passive
    CDR tracer counts) have no dedicated accordion widget: every bundled
    ModelSpec (including ``roms-marbl-0.7-default``) predates them, so its
    YAML's ``param`` dict never carries the keys and ``_SettingsEditor`` (which
    type-infers widgets from that dict, not from ``ParamCfgV0_4_0``'s field
    set) builds none. Same situation as a hand-edited override with no widget
    to show it -- see ``_SettingsEditor``'s and ``_diff_overrides``'s
    docstrings ("hiding the widget cannot drop or reset the value"). Recorded
    directly on the overrides layer (as a real UI edit would be, had a widget
    existed), both values must still flow into ``model_settings`` and survive
    a save/load round trip.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.show_archived_models.value = True
    wiz.model_dd.value = "roms-marbl-0.7-default"
    wiz._overrides[("param", "nt_cdr_oae")] = 2
    wiz._overrides[("param", "nt_cdr_dor")] = 1
    wiz._rebuild()
    assert ("param", "nt_cdr_oae") not in wiz.editor._widgets
    assert ("param", "nt_cdr_dor") not in wiz.editor._widgets
    assert wiz.config.model_settings["param"]["nt_cdr_oae"] == 2
    assert wiz.config.model_settings["param"]["nt_cdr_dor"] == 1

    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    assert wiz2.config is not None
    assert wiz2.config.model_settings["param"]["nt_cdr_oae"] == 2
    assert wiz2.config.model_settings["param"]["nt_cdr_dor"] == 1
    # ... and a rebuild after load (any later UI edit) re-emits them.
    wiz2._rebuild()
    assert wiz2.config.model_settings["param"]["nt_cdr_oae"] == 2
    assert wiz2.config.model_settings["param"]["nt_cdr_dor"] == 1


def test_constant_fe_widgets_built_for_the_0_11_model():
    """``marbl_bgc.use_constant_fe``/``constant_fe_value`` (ucla-roms >= 0.11.0,
    PR #391) get Advanced-settings widgets for ``roms-marbl-0.11-default`` (the
    default model, which declares both keys): the label comes from the
    labels file (symbol + unit appended), the tooltip from the C-Star field
    docstring.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.show_archived_models.value = True
    wiz.model_dd.value = "roms-marbl-0.11-default"
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz.config.model_settings["marbl_bgc"]["use_constant_fe"] is False

    use_fe, _ = wiz.editor._widgets[("marbl_bgc", "use_constant_fe")]
    fe_value, _ = wiz.editor._widgets[("marbl_bgc", "constant_fe_value")]
    assert use_fe.value is False
    assert fe_value.value == pytest.approx(3.0e-3)
    assert use_fe.description == "Hold iron constant"
    assert fe_value.description == "Constant iron concentration (mmol/m³)"
    assert use_fe.tooltip.startswith("use_constant_fe")
    assert "at constant_Fe_value everywhere" in use_fe.tooltip
    assert "Fe imposed when use_constant_Fe" in fe_value.tooltip


def test_constant_fe_widgets_absent_for_the_0_10_model():
    """``roms-marbl-0.10-default`` pins a ucla-roms that stops on the unknown
    keys: its ModelSpec does not declare them, so the editor builds neither
    widget.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.show_archived_models.value = True
    wiz.model_dd.value = "roms-marbl-0.10-default"
    wiz._rebuild()
    assert wiz.config is not None
    assert "use_constant_fe" not in wiz.config.model_settings["marbl_bgc"]
    assert ("marbl_bgc", "use_constant_fe") not in wiz.editor._widgets
    assert ("marbl_bgc", "constant_fe_value") not in wiz.editor._widgets


_V_SPONGE_TRC_KEY = ("v_sponge", "v_sponge_trc")


def test_v_sponge_trc_widget_built_for_the_0_13_model():
    """``v_sponge.v_sponge_trc`` (ucla-roms >= 0.13.0) gets an Advanced-settings
    widget in the Physics pane for ``roms-marbl-0.13-default``, which declares it:
    the label comes from the labels file (symbol + unit appended), the tooltip from
    the C-Star field docstring. ``v_sponge`` itself keeps its dedicated widget and
    gets no accordion one.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.model_dd.value = "roms-marbl-0.13-default"
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz._editor_key.settings_cls is RunTimeSettingsV0_13_0
    assert wiz.config.model_settings["v_sponge"]["v_sponge_trc"] == -1.0

    widget, _ = wiz.editor._widgets[_V_SPONGE_TRC_KEY]
    assert widget.value == -1.0
    assert widget.description == "Sponge tracer diffusivity (m²/s)"
    assert widget.tooltip.startswith("v_sponge_trc")
    assert "0 for a sponge on currents only" in widget.tooltip
    assert ("v_sponge", "v_sponge") not in wiz.editor._widgets
    assert "v_sponge" in wiz.editor._pane_sections["Physics & subgrid tuning"]


def test_v_sponge_trc_widget_absent_for_the_0_12_model():
    """``roms-marbl-0.12-default`` pins a ucla-roms that stops on the unknown key:
    its ModelSpec does not declare it, so the editor builds no widget (and the
    section, whose ``v_sponge`` has its own widget, no pane entry).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.model_dd.value = "roms-marbl-0.12-default"
    wiz._rebuild()
    assert wiz.config is not None
    assert "v_sponge_trc" not in wiz.config.model_settings["v_sponge"]
    assert _V_SPONGE_TRC_KEY not in wiz.editor._widgets
    assert "v_sponge" not in wiz.editor._pane_sections["Physics & subgrid tuning"]


def test_v_sponge_trc_edit_reaches_the_blueprint_beside_the_domain_v_sponge():
    """Editing the widget records an override that lands in ``model_settings`` next
    to the grid-derived ``v_sponge`` (which it neither replaces nor reads), and
    loading the saved blueprint rebuilds the same override.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.model_dd.value = "roms-marbl-0.13-default"
    wiz._rebuild()
    assert wiz.config is not None
    v_sponge = wiz.config.model_settings["v_sponge"]["v_sponge"]

    widget, _ = wiz.editor._widgets[_V_SPONGE_TRC_KEY]
    widget.value = 0.0
    assert wiz._overrides[_V_SPONGE_TRC_KEY] == 0.0
    assert wiz.config is not None
    assert wiz.config.model_settings["v_sponge"] == {
        "v_sponge": v_sponge,
        "v_sponge_trc": 0.0,
    }

    from cstar.wizard.wizard import _diff_overrides

    composed = wiz.config.model_settings
    spec_default = {
        **composed,
        "v_sponge": {"v_sponge": v_sponge, "v_sponge_trc": -1.0},
    }
    assert _diff_overrides(composed, spec_default) == {_V_SPONGE_TRC_KEY: 0.0}


def test_saving_a_model_spec_keeps_v_sponge_trc_and_drops_the_derived_v_sponge():
    from cstar.wizard.wizard import _model_owned_settings

    owned = _model_owned_settings(
        {"v_sponge": {"v_sponge": 4000.0, "v_sponge_trc": 0.0}}
    )
    assert owned["v_sponge"] == {"v_sponge_trc": 0.0}
    # a spec without the key (ucla-roms < 0.13.0) keeps no section at all
    assert "v_sponge" not in _model_owned_settings({"v_sponge": {"v_sponge": 4000.0}})


_AVG_TS_KEYS = [("ts_output", "wrt_avg_temp"), ("ts_output", "wrt_avg_salt")]


def test_avg_ts_widgets_built_in_the_averages_grid_for_the_0_14_model():
    """``ts_output.wrt_avg_temp``/``wrt_avg_salt`` (ucla-roms >= 0.14.0, PR #413)
    get checkboxes in the averages variable grid, right after ``wrt_avg_v``, for
    ``roms-marbl-0.14-default``: the bundled OutputSpec carries both keys, so the
    composed settings do.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.model_dd.value = "roms-marbl-0.14-default"
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz._editor_key.settings_cls is RunTimeSettingsV0_14_0
    editor = wiz.editor
    for (section, field), label in zip(
        _AVG_TS_KEYS, ("Temperature", "Salinity"), strict=True
    ):
        assert wiz.config.model_settings[section][field] is False
        widget, _ = editor._widgets[(section, field)]
        assert widget.value is False
        assert widget.description == label
        assert widget.tooltip.startswith(field)

    temp, salt = (editor._widgets[key][0] for key in _AVG_TS_KEYS)
    grid = next(
        n
        for n in _descendants(editor.output_box)
        if "forge-var-grid" in getattr(n, "_dom_classes", ()) and temp in n.children
    )
    cells = list(grid.children)
    assert (
        cells.index(temp)
        == cells.index(editor._widgets[("ocean_vars", "wrt_avg_v")][0]) + 1
    )
    assert cells.index(salt) == cells.index(temp) + 1


def test_avg_ts_widgets_absent_for_the_0_13_model():
    """``roms-marbl-0.13-default`` pins a ucla-roms that stops on the unknown keys:
    the tier does not model them, so the editor builds neither widget although the
    OutputSpec carries both (switched off).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.model_dd.value = "roms-marbl-0.13-default"
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz._editor_key.settings_cls is RunTimeSettingsV0_13_0
    for key in _AVG_TS_KEYS:
        assert key not in wiz.editor._widgets


def test_constant_fe_value_hidden_until_use_constant_fe_is_on():
    """``constant_fe_value`` is meaningless while ``use_constant_fe`` is off, so
    ``_apply_field_rules`` hides it -- display only: toggling never changes the
    value (neither widget's).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    use_fe, _ = wiz.editor._widgets[("marbl_bgc", "use_constant_fe")]
    fe_value, _ = wiz.editor._widgets[("marbl_bgc", "constant_fe_value")]

    assert use_fe.value is False
    assert fe_value.layout.display == "none"
    assert use_fe.layout.display != "none"

    use_fe.value = True
    assert fe_value.layout.display == ""
    assert fe_value.value == pytest.approx(3.0e-3)

    use_fe.value = False
    assert fe_value.layout.display == "none"
    assert fe_value.value == pytest.approx(3.0e-3)


_IMPLICIT_FS_FIELDS = ("fs_mode", "fs_nsub", "fs_rho_inf", "fs_tol", "fs_verbose")


def _implicit_fs_wizard(model: str) -> ForgeBlueprintWizard:
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.model_dd.value = model
    wiz._rebuild()
    return wiz


def test_implicit_fs_widgets_built_for_the_0_15_model():
    """``implicit_fs`` (ucla-roms >= 0.15.0) gets five widgets in the Physics pane
    for ``roms-marbl-0.15-default``; ``fs_mode`` is a dropdown of the schema's
    choices, defaulting to the older split-explicit behavior.
    """
    wiz = _implicit_fs_wizard("roms-marbl-0.15-default")
    assert wiz.config is not None
    assert wiz.config.model_settings["implicit_fs"]["fs_mode"] == "split"
    for field in _IMPLICIT_FS_FIELDS:
        assert ("implicit_fs", field) in wiz.editor._widgets
    assert "implicit_fs" in wiz.editor._pane_sections["Physics & subgrid tuning"]

    mode, base = wiz.editor._widgets[("implicit_fs", "fs_mode")]
    assert base is str
    assert mode.options == ("split", "implicit")
    assert mode.value == "split"
    assert mode.description == "Barotropic time stepping"


def test_implicit_fs_widgets_absent_for_the_0_14_model():
    """``roms-marbl-0.14-default`` pins a ucla-roms without the group: no widget."""
    wiz = _implicit_fs_wizard("roms-marbl-0.14-default")
    assert wiz.config is not None
    assert "implicit_fs" not in wiz.config.model_settings
    for field in _IMPLICIT_FS_FIELDS:
        assert ("implicit_fs", field) not in wiz.editor._widgets


def test_implicit_fs_solver_settings_hidden_until_implicit_is_chosen():
    """The solver settings read only in implicit mode, so they hide in split mode;
    ``fs_verbose`` also matters in split mode and stays. Display only: no value
    changes.
    """
    wiz = _implicit_fs_wizard("roms-marbl-0.15-default")
    widgets = {
        f: wiz.editor._widgets[("implicit_fs", f)][0] for f in _IMPLICIT_FS_FIELDS
    }
    solver = ("fs_nsub", "fs_rho_inf", "fs_tol")
    for field in solver:
        assert widgets[field].layout.display == "none"
    assert widgets["fs_mode"].layout.display != "none"
    assert widgets["fs_verbose"].layout.display != "none"

    widgets["fs_mode"].value = "implicit"
    for field in solver:
        assert widgets[field].layout.display == ""
    assert widgets["fs_nsub"].value == 2

    widgets["fs_mode"].value = "split"
    for field in solver:
        assert widgets[field].layout.display == "none"
    assert widgets["fs_verbose"].layout.display != "none"


def test_choosing_implicit_reaches_the_blueprint_and_split_is_the_default():
    wiz = _implicit_fs_wizard("roms-marbl-0.15-default")
    assert wiz.config.model_settings["implicit_fs"]["fs_mode"] == "split"
    assert ("implicit_fs", "fs_mode") not in wiz._overrides

    wiz.editor._widgets[("implicit_fs", "fs_mode")][0].value = "implicit"
    assert wiz._overrides[("implicit_fs", "fs_mode")] == "implicit"
    assert wiz.config is not None
    assert wiz.config.model_settings["implicit_fs"]["fs_mode"] == "implicit"


def test_implicit_choice_does_not_survive_a_switch_to_the_0_14_model():
    """A model switch clears the overrides, so the implicit choice leaves no error
    on a model that does not know the group.
    """
    wiz = _implicit_fs_wizard("roms-marbl-0.15-default")
    wiz.editor._widgets[("implicit_fs", "fs_mode")][0].value = "implicit"
    assert wiz.config.model_settings["implicit_fs"]["fs_mode"] == "implicit"

    wiz.model_dd.value = "roms-marbl-0.14-default"
    assert ("implicit_fs", "fs_mode") not in wiz._overrides
    assert wiz.config is not None
    assert "implicit_fs" not in wiz.config.model_settings
    assert ("implicit_fs", "fs_mode") not in wiz.editor._widgets


def test_implicit_choice_with_an_older_ucla_roms_version_is_reported():
    """Typing a ucla-roms version below 0.15.0 keeps the overrides, so the
    settings check must name the release implicit_fs needs.
    """
    wiz = _implicit_fs_wizard("roms-marbl-0.15-default")
    wiz.editor._widgets[("implicit_fs", "fs_mode")][0].value = "implicit"
    assert "Blueprint is valid" in wiz.validation.value

    wiz.roms_ref.value = "0.14.0"
    wiz._rebuild()
    assert "implicit_fs needs ucla-roms >= 0.15.0" in wiz.validation.value
    assert "Blueprint is valid" not in wiz.validation.value


def test_off_list_fs_mode_neither_crashes_the_build_nor_the_sync():
    """A hand-edited value outside the choices gets a text box when the editor is
    built, and a sync with it leaves the dropdown alone; the validation reports it.
    """
    wiz = _implicit_fs_wizard("roms-marbl-0.15-default")
    editor = wiz.editor
    settings = copy.deepcopy(wiz.config.model_settings)
    settings["implicit_fs"]["fs_mode"] = "Implicit"
    editor.sync(settings)  # the dropdown keeps "split"
    assert editor._widgets[("implicit_fs", "fs_mode")][0].value == "split"

    wiz._overrides[("implicit_fs", "fs_mode")] = "Implicit"
    wiz._rebuild()  # must not raise
    assert (
        wiz.config is None
        or wiz.config.model_settings["implicit_fs"]["fs_mode"] == "Implicit"
    )


def test_pio_direct_read_dropdown_built_for_the_0_16_model():
    """``pio_direct_read`` (ucla-roms >= 0.16.0) is a dropdown in the Parallel I/O
    section next to ``pio_stride``, defaulting to ``on``.
    """
    wiz = _implicit_fs_wizard("roms-marbl-0.16-default")
    assert wiz.config is not None
    assert wiz.config.model_settings["pio_settings"]["pio_direct_read"] == "on"
    assert ("pio_settings", "pio_stride") in wiz.editor._widgets
    widget, base = wiz.editor._widgets[("pio_settings", "pio_direct_read")]
    assert base is str
    assert widget.options == ("on", "off", "verify")
    assert widget.value == "on"
    assert "pio_settings" in wiz.editor._pane_sections["Physics & subgrid tuning"]


def test_pio_direct_read_absent_for_the_0_15_model():
    wiz = _implicit_fs_wizard("roms-marbl-0.15-default")
    assert wiz.config is not None
    assert "pio_direct_read" not in wiz.config.model_settings["pio_settings"]
    assert ("pio_settings", "pio_direct_read") not in wiz.editor._widgets


def test_cdr_lite_knob_override_survives_save_and_load(tmp_path):
    """The bundled ModelSpecs do not declare the ``cdr_lite`` online knob
    (CDR-lite tracers need forcing Forge cannot generate; the 0.12 spec declares
    only the time-interpolation switch), so the accordion has no widget for it.
    Set as an override (a hand-edit), it still lands in ``model_settings``,
    survives a save/load round trip, and can be turned back off.
    ``cppdefs.cdr_lite`` is resolver-/build-derived and has no widget
    (``configure_build`` recomputes it from the knob).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    knob = ("cdr_lite", "cdr_online_carbonate_sensitivity")
    wiz._overrides[("param", "nt_cdr_oae")] = 1
    wiz._rebuild()
    assert wiz.config is not None
    assert knob[1] not in wiz.config.model_settings.get("cdr_lite", {})
    assert knob not in wiz.editor._widgets
    assert ("cppdefs", "cdr_lite") not in wiz.editor._widgets

    wiz._overrides[knob] = True
    wiz._rebuild()
    assert wiz.config.model_settings["cdr_lite"]["cdr_online_carbonate_sensitivity"]

    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)
    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)
    assert wiz2.config is not None
    assert wiz2.config.model_settings["cdr_lite"]["cdr_online_carbonate_sensitivity"]

    wiz2._overrides[knob] = False
    wiz2._rebuild()
    assert wiz2.config is not None
    assert not wiz2.config.model_settings["cdr_lite"][
        "cdr_online_carbonate_sensitivity"
    ]


def test_saving_a_model_spec_does_not_bake_in_the_derived_cdr_lite_cppdef():
    from cstar.wizard.wizard import _model_owned_settings

    effective = {
        "cdr_lite": {"cdr_online_carbonate_sensitivity": True},
        "cdr_lite_output": {"do_cdr_lite_output": True},
        "cppdefs": {"cdr_lite": True, "cdr_forcing": False, "nhy_forcing": True},
    }
    owned = _model_owned_settings(effective)
    assert owned["cppdefs"] == {"nhy_forcing": True}
    assert owned["cdr_lite"] == {"cdr_online_carbonate_sensitivity": True}
    assert "cdr_lite_output" not in owned  # an OutputSpec section


def test_legacy_named_output_spec_shows_up_under_the_new_section_name(tmp_path, caplog):
    """A user OutputSpec still using ``cdr_tracer_output`` (before the rename)
    reaches the wizard through the catalog reader, which renames it: the composed
    settings and the editor carry ``cdr_lite_output`` only.
    """
    import logging
    import shutil

    from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog

    root = tmp_path / "catalog"
    shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
    text = (root / "specs" / "OutputSpec" / "daily-restarts.yaml").read_text()
    text = text.replace("cdr_lite_output:", "cdr_tracer_output:").replace(
        "do_cdr_lite_output", "do_cdr_tracer_output"
    )
    assert "cdr_tracer_output:" in text
    (root / "specs" / "OutputSpec" / "legacy-out.yaml").write_text(text)
    catalog = DomainCatalog(catalog_root=root)

    wiz = ForgeBlueprintWizard(catalog=catalog)
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    with caplog.at_level(logging.WARNING, logger="cstar.catalog.spec_schema"):
        wiz.output_dd.value = "legacy-out"
    wiz._rebuild()

    assert wiz.config is not None
    assert "cdr_tracer_output" not in wiz.config.model_settings
    assert wiz.config.model_settings["cdr_lite_output"]["do_cdr_lite_output"] is False
    assert ("cdr_lite_output", "do_cdr_lite_output") in wiz.editor._widgets
    assert "is schema 1.0.0" in caplog.text
    assert wiz.config.composition.output.modified is False


def test_domain_modified_reflects_deviation_from_catalog_pick():
    """composition.domain.modified follows "deviate" semantics: editing a
    domain-defining widget after a catalog Domain pick sets it True; reverting the
    edit exactly clears it back to False (audit follow-up: domain never used to
    track modification at all).
    """
    wiz = ForgeBlueprintWizard()
    wiz.domain_dd.value = ForgeBlueprintWizard._dd_values(wiz.domain_dd)[
        1
    ]  # first real catalog domain
    assert wiz.config.composition.domain.origin == "catalog"
    assert wiz.config.composition.domain.modified is False

    orig_npx = wiz.npx.value
    wiz.npx.value = orig_npx + 1
    assert wiz.config.composition.domain.modified is True
    assert wiz.config.composition.domain.origin == "catalog"  # never flips to custom

    wiz.npx.value = orig_npx  # revert exactly -> deviation clears
    assert wiz.config.composition.domain.modified is False


def test_domain_modified_false_when_hand_authored():
    """A from-scratch (non-catalog) domain has nothing to deviate from -> never
    modified, regardless of what its widgets hold.
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.domain_dd.value == "<custom>"
    wiz.npx.value = wiz.npx.value + 1
    assert wiz.config.composition.domain.origin == "custom"
    assert wiz.config.composition.domain.modified is False


def test_forcing_modified_reflects_deviation_from_catalog_pick():
    """composition.forcing.modified follows the same "deviate" semantics, and
    (post-unification) origin no longer flips to "custom" on edit.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.composition.forcing.modified is False
    assert wiz.config.composition.forcing.origin == "catalog"

    tidal_rows = wiz._forcing_editor._rows["tidal"]
    orig_ntides = tidal_rows[0]["ntides"].value
    tidal_rows[0]["ntides"].value = orig_ntides + 1
    wiz._rebuild()
    assert wiz.config.composition.forcing.modified is True
    assert wiz.config.composition.forcing.origin == "catalog"  # never flips to custom

    tidal_rows[0]["ntides"].value = orig_ntides  # revert exactly -> deviation clears
    wiz._rebuild()
    assert wiz.config.composition.forcing.modified is False


# ---------------------------------------------------------------------------
# Advanced settings: output-stream tables / per-variable checkbox grids
# (_SettingsEditor._build_section grouping -- see _OUTPUT_TABLES/_VARIABLE_GRIDS)
# ---------------------------------------------------------------------------


def _is_descendant_of_class(root, target, css_class: str) -> bool:
    """True if ``target`` sits under some node in ``root``'s tree carrying
    ``css_class`` (as added via ``add_class``).
    """
    for node in _iter_widget_tree(root):
        if css_class in getattr(node, "_dom_classes", ()) and any(
            t is target for t in _iter_widget_tree(node)
        ):
            return True
    return False


def test_ocean_vars_output_table_widgets_have_blank_description_and_live_in_table():
    """The ocean_vars Write/Period/Records-per-file widgets for each output
    stream row (see ``_OUTPUT_TABLES["ocean_vars"]``) render with a blank
    ``description`` (the table's own header carries the column labels) and
    sit inside a ``W.GridBox`` carrying the ``forge-out-table`` class.
    """
    wiz = ForgeBlueprintWizard()
    editor = wiz.editor
    row = next(
        r for r in _OUTPUT_TABLES["ocean_vars"] if r["label"] == "Instantaneous history"
    )
    for key in (row["write"], row["period"], row["records"]):
        widget, _base = editor._widgets[("ocean_vars", key)]
        assert widget.description == ""
        assert _is_descendant_of_class(editor.output_box, widget, "forge-out-table")


def test_ocean_vars_variable_checkbox_lives_in_grid_with_glossary_description():
    """``wrt_z`` (one of ``_VARIABLE_GRIDS["ocean_vars"]``'s history-file
    variables) renders inside a ``forge-var-grid`` GridBox and keeps its
    normal glossary-derived (non-blank) checkbox description -- only the
    output-stream table's write/period/records widgets get blanked.
    """
    wiz = ForgeBlueprintWizard()
    editor = wiz.editor
    widget, _base = editor._widgets[("ocean_vars", "wrt_z")]
    assert widget.description != ""
    assert _is_descendant_of_class(editor.output_box, widget, "forge-var-grid")


def test_ocean_vars_section_fields_unchanged_by_table_grid_layout():
    """The table/grid layout is display-only: the ocean_vars fields the editor
    records (which drive the section-title "N settings" count) must still list every
    field ``_build_section`` would have built as a plain flat list -- i.e.
    every ``ocean_vars`` field on the active settings_cls's sub-model, minus
    ``_ACCORDION_EXCLUDED_FIELDS``, in the same order.
    """
    wiz = ForgeBlueprintWizard()
    editor = wiz.editor
    sub = _section_submodel("ocean_vars", wiz._editor_key.settings_cls)
    excluded = _ACCORDION_EXCLUDED_FIELDS.get("ocean_vars", frozenset())
    expected = [
        key
        for key in wiz.config.model_settings["ocean_vars"]
        if key not in excluded and key in sub.model_fields
    ]
    assert expected  # sanity: the section isn't accidentally empty
    assert _built_fields(editor)["ocean_vars"] == expected


def test_ocean_vars_table_widget_read_reflects_edit():
    """A value set directly on a table-rendered widget (same object as in the
    ``_widgets`` registry -- the table only rearranges it) must read back via
    ``editor.read()`` exactly like any other advanced-settings field.
    """
    wiz = ForgeBlueprintWizard()
    editor = wiz.editor
    widget, _base = editor._widgets[("ocean_vars", "nrpf_his")]
    widget.value = 77
    assert editor.read("ocean_vars", "nrpf_his") == 77


def test_model_and_output_modified_from_accordion_overrides():
    """Model/output share the accordion overrides layer; modified is derived per-
    spec by whether a deviating override key belongs to OUTPUT_SECTIONS/
    PARTIAL_OUTPUT_SECTIONS (audit follow-up: these two specs never set `modified`
    at all before this fix).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.composition.model.modified is False
    assert wiz.config.composition.output.modified is False

    # A non-output section override -> model.modified only.
    wiz._overrides[("lateral_visc", "visc2")] = 99.0
    wiz._rebuild()
    assert wiz.config.composition.model.modified is True
    assert wiz.config.composition.output.modified is False

    # An OUTPUT_SECTIONS override -> output.modified only.
    del wiz._overrides[("lateral_visc", "visc2")]
    wiz._overrides[("ocean_vars", "wrt_z")] = False
    wiz._rebuild()
    assert wiz.config.composition.model.modified is False
    assert wiz.config.composition.output.modified is True


def test_composition_modified_survives_save_and_load_round_trip(tmp_path):
    """A saved deviation on model/output/domain/forcing must reload with the same
    `modified` flags (composition is meant to reliably answer "did the user touch
    this catalog spec" even after a save/load cycle).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.domain_dd.value = ForgeBlueprintWizard._dd_values(wiz.domain_dd)[1]
    wiz.npx.value = wiz.npx.value + 1  # deviate domain
    wiz._overrides[("lateral_visc", "visc2")] = 99.0  # deviate model
    wiz._rebuild()
    assert wiz.config.composition.domain.modified is True
    assert wiz.config.composition.model.modified is True

    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    # Domain always loads as origin="custom" (the file, not a catalog entry, is
    # authoritative) so domain.modified is moot on load; model.modified must survive
    # via the reconstructed overrides layer.
    assert wiz2.config.composition.domain.origin == "custom"
    assert wiz2.config.composition.model.modified is True


def test_composition_modified_all_false_on_pristine_save_and_load_round_trip(
    tmp_path,
):
    """A file saved with no edits must reload with every spec unmodified -- the
    forcing comparison in particular round-trips through `_sources_to_inputs` /
    `build_forge_blueprint` before being re-gathered, so a lossy resolve/reconstruct
    cycle (e.g. an omitted-vs-null field) could otherwise report a false positive
    for a file the user never touched.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.composition.forcing.modified is False
    assert wiz.config.composition.model.modified is False
    assert wiz.config.composition.output.modified is False

    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    assert wiz2.config.composition.forcing.modified is False
    assert wiz2.config.composition.model.modified is False
    assert wiz2.config.composition.output.modified is False


_BUNDLED_DOMAINS = sorted(
    p.stem for p in (BUNDLED_CATALOG_ROOT / "specs" / "DomainSpec").glob("*.yaml")
)
assert _BUNDLED_DOMAINS, "no bundled DomainSpec files: the glob is stale"


@pytest.mark.parametrize("name", _BUNDLED_DOMAINS)
def test_grid_previews_build_every_bundled_domain(monkeypatch, name):
    """Each bundled DomainSpec previews as this grid, as the nesting child and as
    the parent: every Grid the three Refresh preview buttons build gets only
    keywords ``roms_tools.Grid`` takes. GoA-10th_deg's and NEP-4th_deg's
    ``metadata`` block, kept from the loaded spec, once reached ``Grid(...)``.
    """
    import inspect

    import roms_tools

    grid_signature = inspect.signature(roms_tools.Grid)
    built: list[dict] = []

    class _SignatureGrid:
        def __init__(self, **kwargs):
            grid_signature.bind(**kwargs)  # the real Grid's TypeError
            built.append(kwargs)

        def plot(self):
            pass

    monkeypatch.setattr(roms_tools, "Grid", _SignatureGrid)
    monkeypatch.setattr(roms_tools, "plot_nesting", lambda *a, **k: None)

    wiz = ForgeBlueprintWizard()
    wiz.domain_dd.value = name
    wiz.nest_domain_dd.value = name
    wiz.parent_domain_dd.value = name
    wiz._on_plot(None)
    wiz._on_nest_plot(None)
    wiz._on_parent_plot(None)

    for status in (wiz.plot_status, wiz.nest_plot_status, wiz.parent_plot_status):
        assert "✓" in status.value, status.value
    assert len(built) >= 5  # this grid x3, the child and the parent


def test_parent_plot_is_always_grid_plot_only(monkeypatch):
    """The Grid section's plot is parent-only regardless of nesting state -- the
    parent+child overlay lives in its own Nesting-section plot (see _on_nest_plot).
    """
    import roms_tools

    calls = []

    class _FakeGrid:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def plot(self):
            calls.append("plot")

    monkeypatch.setattr(roms_tools, "Grid", _FakeGrid)

    wiz = ForgeBlueprintWizard()
    for nest_enabled in (False, True):
        calls.clear()
        wiz.nest_enable.value = nest_enabled
        wiz._on_plot(None)
        assert calls == ["plot"], (nest_enabled, wiz.plot_status.value)


def test_nest_plot_button_renders_parent_and_child_via_plot_nesting(monkeypatch):
    """Item 1 (revised): a dedicated 'Refresh plot' button in the Nesting section
    builds both grids and renders them via plot_nesting, independent of the parent
    Grid section's plot.
    """
    import roms_tools

    calls = []

    class _FakeGrid:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def plot(self):
            calls.append(("plot", self.kwargs))

    def _fake_plot_nesting(parent, child, **kw):
        calls.append(("plot_nesting", parent.kwargs, child.kwargs))

    monkeypatch.setattr(roms_tools, "Grid", _FakeGrid)
    monkeypatch.setattr(roms_tools, "plot_nesting", _fake_plot_nesting, raising=False)

    wiz = ForgeBlueprintWizard()
    wiz._on_nest_plot(None)

    assert any(c[0] == "plot_nesting" for c in calls), wiz.nest_plot_status.value
    assert not any(c[0] == "plot" for c in calls)  # doesn't touch the parent plot
    assert len(wiz.nest_plot_img.value) > 0
    # The parent plot/status are untouched by the nesting-section refresh
    # (plot_status still carries its initial empty-state hint).
    assert wiz.plot_status.value == "Click Refresh preview to draw the grid."
    assert wiz.plot_img.value == b""


def test_nest_plot_figure_survives_inline_backend_show(monkeypatch):
    """Regression: plot_nesting calls plt.show() internally (no way to suppress it,
    no return value). Under Jupyter's inline backend, show() renders-and-closes the
    current figure immediately, so a plain plt.gcf() call right after plot_nesting
    returns would grab a fresh *blank* figure instead of the one just drawn -- the
    bug report ("existing plot disappears"). _on_nest_plot must neutralize
    plt.show for the duration of the plot_nesting call so the real figure survives
    to be saved.
    """
    import matplotlib.pyplot as plt
    import roms_tools

    class _FakeGrid:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    def _fake_plot_nesting(parent, child, **kw):
        fig, ax = plt.subplots()
        ax.plot([0, 1], [0, 1])  # something identifiable on the "real" figure
        plt.show()  # exercises the real plt.show, patched below

    def _inline_backend_show(*a, **kw):
        # Mimic Jupyter's inline backend: render then close the current figure.
        plt.close(plt.gcf())

    captured = {}
    real_savefig = plt.Figure.savefig

    def _spy_savefig(self, *a, **kw):
        captured["axes"] = list(self.axes)
        return real_savefig(self, *a, **kw)

    monkeypatch.setattr(roms_tools, "Grid", _FakeGrid)
    monkeypatch.setattr(roms_tools, "plot_nesting", _fake_plot_nesting, raising=False)
    monkeypatch.setattr(plt, "show", _inline_backend_show)
    monkeypatch.setattr(plt.Figure, "savefig", _spy_savefig)

    wiz = ForgeBlueprintWizard()
    wiz._on_nest_plot(None)

    assert not wiz.nest_plot_status.value.startswith("<span class='forge-st-err'>"), (
        wiz.nest_plot_status.value
    )
    # A blank new figure (created after a premature close) would have zero axes;
    # the real drawn figure has one axis with the plotted line.
    assert captured.get("axes"), "expected the drawn figure's axes, got a blank figure"
    assert captured["axes"][0].lines, "the plotted line did not survive plt.show()"


def test_build_run_command_uses_cstar_blueprint_run_from_this_env():
    """The Run button invokes `cstar blueprint run <path>` -- the one command the docs
    give for both pipeline steps -- via the `cstar` script installed next to the
    interpreter already running the wizard's kernel, not a bare `cstar` from PATH or a
    `conda run` invocation (avoids conda/micromamba env-discovery issues). Not
    `cstar forge run`: the button exposes no per-run flags, so that passthrough's only
    advantage does not apply here.
    """
    import sys

    wiz = ForgeBlueprintWizard()
    cmd = wiz._build_run_command("/tmp/some_blueprint.yaml")
    cstar_exe = Path(sys.executable).with_name("cstar")
    if cstar_exe.exists():
        assert cmd == [str(cstar_exe), "blueprint", "run", "/tmp/some_blueprint.yaml"]
    else:
        assert cmd == [
            sys.executable,
            "-m",
            "cstar.cli.cli",
            "forge",
            "run",
            "/tmp/some_blueprint.yaml",
        ]


def test_on_run_guards_on_invalid_config(monkeypatch):
    """Clicking Run with no resolved config shows an error and spawns nothing."""
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz._boundaries_touched = True  # not exercising boundary derivation here
    wiz.config = None

    def _boom(*a, **kw):
        raise AssertionError("must not spawn a subprocess for an invalid config")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _boom)
    wiz._on_run(None)
    assert "invalid" in wiz.run_status.value
    assert wiz.run_output.outputs == ()


class _FakeStdout:
    """Minimal async stand-in for asyncio.StreamReader's ``read(n)``.

    Serves a flat byte buffer in small, deliberately line-*un*aligned chunks (7
    bytes by default), so a test exercises the wizard's chunk-to-line reassembly
    rather than getting one whole line per read (which would prove nothing). Returns
    ``b""`` at EOF, like the real reader.

    Deliberately implements only ``read(n)`` and no ``__aiter__``: the wizard must
    not go back to ``async for line in proc.stdout`` (StreamReader.readline), whose
    64 KiB line limit is the bug this change removed -- doing so would fail here.
    """

    def __init__(self, data, chunk_size=7):
        self._data = data if isinstance(data, bytes) else b"".join(data)
        self._pos = 0
        self._chunk_size = chunk_size

    async def read(self, n=-1):
        if self._pos >= len(self._data):
            return b""
        take = len(self._data) - self._pos if n < 0 else min(n, self._chunk_size)
        chunk = self._data[self._pos : self._pos + take]
        self._pos += len(chunk)
        return chunk


class _FakeProcess:
    def __init__(self, data, returncode=0, chunk_size=7):
        self.stdout = _FakeStdout(data, chunk_size=chunk_size)
        self._returncode = returncode

    async def wait(self):
        return self._returncode


def test_on_run_streams_subprocess_output_and_reports_success(monkeypatch, tmp_path):
    """Run auto-saves the current blueprint, launches the built command with
    stderr merged into stdout, and streams each line into run_output. There's no
    running event loop in a plain test function, so _schedule_coroutine's
    asyncio.run(...) fallback runs the whole thing to completion synchronously --
    no pytest.mark.asyncio needed.
    """
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True  # not exercising boundary derivation here

    captured_cmd = {}

    async def _fake_create_subprocess_exec(*args, **kwargs):
        captured_cmd["args"] = args
        captured_cmd["kwargs"] = kwargs
        return _FakeProcess([b"line one\n", b"line two\n"], returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

    wiz._on_run(None)

    assert (tmp_path / "bp.yaml").exists()  # auto-saved before running
    assert captured_cmd["kwargs"]["stderr"] == asyncio.subprocess.STDOUT
    text = "".join(o["text"] for o in wiz.run_output.outputs)
    assert "line one" in text
    assert "line two" in text
    assert "✓ finished" in wiz.run_status.value
    # ...and the success message hands the user their next command, since the
    # app-framework path prints no "run it with" trailer of its own
    assert "cstar blueprint run" in wiz.run_status.value
    assert wiz.run_btn.disabled is False


def test_on_run_reports_nonzero_exit_code(monkeypatch, tmp_path):
    """A failing subprocess is reported as an error status, not a silent success."""
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True  # not exercising boundary derivation here

    async def _fake_create_subprocess_exec(*args, **kwargs):
        return _FakeProcess([b"uh oh\n"], returncode=1)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

    wiz._on_run(None)

    assert "exited with code 1" in wiz.run_status.value
    assert wiz.run_btn.disabled is False


def _run_wiz_with_output(monkeypatch, tmp_path, data, *, returncode=0, chunk_size=7):
    """Drive _on_run with a fake process emitting ``data`` (bytes); return the wizard."""
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True  # not exercising boundary derivation here

    async def _fake(*args, **kwargs):
        return _FakeProcess(data, returncode=returncode, chunk_size=chunk_size)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake)
    wiz._on_run(None)
    return wiz


def test_on_run_survives_line_longer_than_stream_limit(monkeypatch, tmp_path):
    r"""A child that emits far more than 64 KiB with no newline (the classic
    \r-less/progress-less long line) must NOT raise the asyncio StreamReader
    "Separator is not found, and chunk exceed the limit" ValueError that the old
    ``async for line in proc.stdout`` loop did -- the content still lands, split
    across multiple appends by the memory-bounding flush.
    """
    from cstar.wizard.wizard import _STREAM_MAX_LINE

    giant = b"x" * (_STREAM_MAX_LINE * 3 + 17) + b"\ndone\n"
    # Read in chunks well below the flush threshold and unaligned to it, so the
    # buffer must *accumulate across many reads* before each flush -- the real
    # production path (asyncio read() returns whatever is buffered, usually far less
    # than the threshold), not one oversized read that drains immediately.
    wiz = _run_wiz_with_output(monkeypatch, tmp_path, giant, chunk_size=3000)

    assert "✓ finished" in wiz.run_status.value  # no exception surfaced
    text = "".join(o["text"] for o in wiz.run_output.outputs)
    assert text.count("x") == _STREAM_MAX_LINE * 3 + 17  # every byte preserved
    assert "done\n" in text
    # bounded memory => the giant run was flushed as several appends, not held whole
    assert len(wiz.run_output.outputs) > 3


def test_on_run_splits_carriage_return_progress_into_lines(monkeypatch, tmp_path):
    r"""\r-redrawn progress (git clone / tqdm) surfaces as successive log lines
    instead of one accumulating line.
    """
    wiz = _run_wiz_with_output(monkeypatch, tmp_path, b"10%\r20%\r30%\n")

    texts = [o["text"] for o in wiz.run_output.outputs]
    assert texts == ["10%\n", "20%\n", "30%\n"]


def test_on_run_error_status_names_the_command(monkeypatch, tmp_path):
    """An exception during the run is reported WITH the command that was running,
    not as a context-free error string.
    """
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True

    async def _boom(*args, **kwargs):
        raise OSError("no such executable")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _boom)
    wiz._on_run(None)

    assert "OSError: no such executable" in wiz.run_status.value
    assert "while running:" in wiz.run_status.value
    assert "bp.yaml" in wiz.run_status.value  # the built command is named
    assert wiz.run_btn.disabled is False


def test_drain_stream_buffer_crlf_split_across_reads():
    r"""A \r\n straddling a read boundary is one line break, not \r + blank line:
    the trailing \r is held back for the next chunk.
    """
    lines, rem = _drain_stream_buffer(b"abc\ndef\r")
    assert lines == ["abc\n"]
    assert rem == b"def\r"  # \r held, not emitted as a terminator yet
    lines2, rem2 = _drain_stream_buffer(rem + b"\nghi")
    assert lines2 == ["def\n"]  # the held \r + \n = a single CRLF break
    assert rem2 == b"ghi"


def test_drain_stream_buffer_mixed_terminators_and_eof():
    r"""\r, \n and \r\n all cut lines (normalised to \n); EOF flushes the
    unterminated remainder.
    """
    lines, rem = _drain_stream_buffer(b"a\rb\nc\r\nd")
    assert lines == ["a\n", "b\n", "c\n"]
    assert rem == b"d"
    flushed, rem2 = _drain_stream_buffer(rem, at_eof=True)
    assert flushed == ["d"]  # no trailing newline added at EOF
    assert rem2 == b""


# ===========================================================================
# User-provided-netCDF attach flows (grid / CDR forcing / river custom_file)
# ===========================================================================
#
# Real (tiny) netCDFs are used wherever ``hash_netcdf_contents`` runs for real
# (it opens the file with xarray) -- only ``roms_tools.Grid`` itself is stubbed
# (real Grid *generation* is broken in this env; ``Grid(filename=...)`` loading
# is also stubbed here for speed/determinism, mirroring
# test_parent_plot_is_always_grid_plot_only's monkeypatch pattern).


def _write_tiny_netcdf(path: Path) -> Path:
    """A minimal real netCDF, just for ``hash_netcdf_contents`` to hash."""
    import numpy as np
    import xarray as xr

    ds = xr.Dataset(
        {"temp": (["y", "x"], np.zeros((2, 2), dtype=np.float64))},
        attrs={"title": "tiny test file"},
    )
    ds.to_netcdf(path)
    return path


def _write_tiny_cdr_netcdf(path: Path, with_ncdr_dim: bool = True) -> Path:
    """A minimal real netCDF with (or without) the ``ncdr`` dimension the CDR
    attach flow's light validation checks for.
    """
    import numpy as np
    import xarray as xr

    dim = "ncdr" if with_ncdr_dim else "n_other"
    ds = xr.Dataset({"cdr_volume": ([dim], np.zeros(3, dtype=np.float64))})
    ds.to_netcdf(path)
    return path


class _FakeLoadedGrid:
    """Stands in for ``rt.Grid(filename=...)``'s return value: only the
    attributes the wizard/resolver actually read off a loaded grid file
    (nx/ny/N/center_lon/center_lat/rot/size_x/size_y/theta_s/theta_b/hc).

    Mirrors real roms-tools I/O behavior for a missing ``filename`` (raises)
    rather than silently succeeding -- needed so a test simulating a
    since-deleted grid_file also sees the resolver's own independent reload
    attempt (``build_forge_blueprint``'s ``rt.Grid(filename=grid_file_obj.location)``
    when no ``grid=`` is passed) fail the same way a real missing file would.
    """

    def __init__(self, **kwargs):
        filename = kwargs.get("filename")
        if filename is not None and not Path(filename).exists():
            raise FileNotFoundError(f"no such file: {filename}")
        self.kwargs = kwargs
        self.nx = 10
        self.ny = 8
        self.N = 5
        self.center_lon = 12.0
        self.center_lat = 34.0
        self.rot = 0.0
        self.size_x = 300.0
        self.size_y = 250.0
        self.theta_s = 6.0
        self.theta_b = 3.0
        self.hc = 200.0


@pytest.fixture
def fake_grid(monkeypatch):
    """Stub ``roms_tools.Grid`` for the duration of a test."""
    import roms_tools

    monkeypatch.setattr(roms_tools, "Grid", _FakeLoadedGrid)
    return _FakeLoadedGrid


def _new_wizard():
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    return wiz


class TestNestedTopographyWidgets:
    """Parent/child grids carry their own topography (see
    ForgeExecutor._nested_topography_pair); the wizard's parent/child pick copies
    the spec's topography, gather emits the keys, and load-back restores them.
    """

    _SPEC: ClassVar[dict] = {
        "grid_kwargs": {"nx": 30, "ny": 40, "size_x": 300.0, "size_y": 400.0},
        "topography_source": "SRTM15",
        "topography_path": "/hpc/data/srtm15_west.nc",
    }

    def _pick_parent(self, wiz, monkeypatch, spec):
        monkeypatch.setattr(wiz.catalog, "domain_data", lambda _name: dict(spec))
        monkeypatch.setattr(wiz, "_on_parent_plot", lambda _b: None)
        wiz.parent_domain_dd.value = wiz.parent_domain_dd.options[1]

    def test_parent_pick_copies_spec_topography_and_gather_emits_it(self, monkeypatch):
        wiz = _new_wizard()
        assert wiz.parent_topo_source.value == "(same as this grid)"
        self._pick_parent(wiz, monkeypatch, self._SPEC)

        assert wiz.parent_enable.value is True
        assert wiz.parent_w["nx"].value == 30
        assert wiz.parent_topo_source.value == "SRTM15"
        assert wiz.parent_topo_path.value == "/hpc/data/srtm15_west.nc"

        pk = wiz._gather()["grid_kwargs_parent"]
        assert pk["topography_source"] == "SRTM15"
        assert pk["topography_path"] == "/hpc/data/srtm15_west.nc"
        assert wiz.config is not None, wiz.derived.value
        assert wiz.config.domain.grid_kwargs_parent["topography_source"] == "SRTM15"
        assert "SRTM15" in wiz.config.datasets

    def test_parent_pick_with_default_topography_is_explicit_etopo5(self, monkeypatch):
        """A parent spec built with the default dataset says so explicitly: the
        parent must NOT silently inherit this grid's (e.g. EMOD) topography.
        """
        wiz = _new_wizard()
        wiz.topo_source.value = "EMOD"
        wiz.topo_path.value = "/hpc/data/EMODnet_C2.nc"
        spec = {"grid_kwargs": self._SPEC["grid_kwargs"]}  # no topography keys
        self._pick_parent(wiz, monkeypatch, spec)

        assert wiz.parent_topo_source.value == "ETOPO5"
        assert wiz.parent_topo_path.value == ""
        pk = wiz._gather()["grid_kwargs_parent"]
        assert pk["topography_source"] == "ETOPO5"
        assert "topography_path" not in pk

    def test_inherit_sentinel_emits_no_keys(self):
        wiz = _new_wizard()
        wiz.parent_enable.value = True
        wiz.nest_enable.value = True
        kw = wiz._gather()
        for key in ("grid_kwargs_parent", "grid_kwargs_child"):
            assert "topography_source" not in kw[key]
            assert "topography_path" not in kw[key]

    def test_round_trips_through_populate_from(self, monkeypatch):
        wiz = _new_wizard()
        self._pick_parent(wiz, monkeypatch, self._SPEC)
        wiz.nest_enable.value = True
        wiz.child_topo_path.value = "/hpc/data/child_tile.nc"  # path-only override
        wiz._rebuild()
        cfg = wiz.config
        assert cfg is not None, wiz.derived.value
        assert (
            cfg.domain.grid_kwargs_child["topography_path"] == "/hpc/data/child_tile.nc"
        )
        assert "topography_source" not in cfg.domain.grid_kwargs_child

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert wiz2.parent_topo_source.value == "SRTM15"
        assert wiz2.parent_topo_path.value == "/hpc/data/srtm15_west.nc"
        assert wiz2.child_topo_source.value == "(same as this grid)"
        assert wiz2.child_topo_path.value == "/hpc/data/child_tile.nc"
        assert (
            wiz2._gather()["grid_kwargs_parent"] == wiz._gather()["grid_kwargs_parent"]
        )

    def test_plot_topography_falls_back_when_file_not_local(self, tmp_path):
        from cstar.wizard.wizard import (
            _effective_nested_topo,
            _plot_topography_source,
        )

        assert _plot_topography_source("ETOPO5", "") == (None, "")
        topo, note = _plot_topography_source("EMOD", "/hpc/only/EMODnet_C2.nc")
        assert topo is None and "EMOD" in note and "not found here" in note
        topo, note = _plot_topography_source("EMOD", "")
        assert topo is None and "no local file" in note
        local = _write_tiny_netcdf(tmp_path / "topo.nc")
        assert _plot_topography_source("EMOD", str(local)) == (
            {"name": "EMOD", "path": str(local)},
            "",
        )
        # inherit: domain pair; source set: domain path is NOT inherited
        assert _effective_nested_topo("(same as this grid)", "", "EMOD", "/d/e.nc") == (
            "EMOD",
            "/d/e.nc",
        )
        assert _effective_nested_topo("SRTM15", "", "EMOD", "/d/e.nc") == ("SRTM15", "")
        assert _effective_nested_topo(
            "(same as this grid)", "/p.nc", "EMOD", "/d/e.nc"
        ) == (
            "EMOD",
            "/p.nc",
        )

    def test_parent_plot_builds_parent_with_its_own_topography(
        self, monkeypatch, tmp_path
    ):
        """The parent plot builds the parent grid with the parent's topography
        (when the file is available here) -- so a parent falling outside its
        dataset's coverage fails in the preview, not only at executor time.
        """
        import roms_tools

        calls: list[dict] = []

        class _G:
            def __init__(self, **kw):
                calls.append(kw)

        monkeypatch.setattr(roms_tools, "Grid", _G)
        monkeypatch.setattr(roms_tools, "plot_nesting", lambda *a, **k: None)
        wiz = _new_wizard()
        local = _write_tiny_netcdf(tmp_path / "parent_topo.nc")
        wiz.parent_enable.value = True
        wiz.parent_topo_source.value = "EMOD"
        wiz.parent_topo_path.value = str(local)
        wiz.topo_source.value = "SRTM15"  # this grid: no local file -> fallback

        wiz._on_parent_plot(None)

        parent_kw, this_kw = calls[0], calls[1]
        assert parent_kw["topography_source"] == {"name": "EMOD", "path": str(local)}
        assert "topography_source" not in this_kw
        assert "SRTM15" in wiz.parent_plot_status.value  # fallback note for this grid
        assert "EMOD" not in wiz.parent_plot_status.value


class TestComputeDt:
    """Compute dt (CFL) is arithmetic on the grid alone: it must not depend on
    the rest of the blueprint resolving.
    """

    @staticmethod
    def _resolver_dt(wiz) -> float:
        """What the resolver derives for the wizard's current grid."""
        cfg = build_forge_blueprint(**{**wiz._gather(), "dt": None})
        assert cfg.domain.dt is not None
        return float(cfg.domain.dt)

    def test_matches_the_resolver_for_the_same_grid(self):
        wiz = _new_wizard()
        expected = self._resolver_dt(wiz)
        wiz.dt.value = 1.0
        wiz._on_compute_dt(None)
        assert wiz.dt.value == expected != 1.0
        assert "(CFL)" in wiz.dt_status.value
        assert "forge-st-err" not in wiz.dt_status.value

    def test_does_not_run_the_resolver(self, monkeypatch):
        wiz = _new_wizard()

        def _fail(**_kw):
            raise AssertionError("Compute dt must not resolve the blueprint")

        monkeypatch.setattr("cstar.wizard.wizard.build_forge_blueprint", _fail)
        wiz._on_compute_dt(None)
        assert "forge-st-err" not in wiz.dt_status.value

    def test_works_while_the_blueprint_does_not_resolve(self):
        """cdr_lite demands a CDR forcing; without one the blueprint is invalid,
        but the grid's dt is still computable.
        """
        wiz = _new_wizard()
        expected = self._resolver_dt(wiz)
        _strip_bgc_forcing(wiz)
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.config is None
        wiz.dt.value = 1.0
        wiz._on_compute_dt(None)
        assert wiz.dt.value == expected
        assert "(CFL)" in wiz.dt_status.value

    def test_uses_the_attached_grid_files_grid(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        wiz.grid_file_path.value = str(_write_tiny_netcdf(tmp_path / "grid.nc"))
        wiz._on_grid_file_attach(None)
        wiz.dt.value = 1.0
        wiz._on_compute_dt(None)
        assert wiz.dt.value == cfl_dt({}, wiz._grid_file_grid) != 1.0
        assert "(CFL)" in wiz.dt_status.value

    def test_refuses_a_grid_file_that_failed_to_reload(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        p.unlink()
        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)
        assert wiz2._grid_file is not None
        assert wiz2._grid_file_grid is None
        wiz2.dt.value = 1.0
        wiz2._on_compute_dt(None)
        assert wiz2.dt.value == 1.0
        assert "failed to" in wiz2.dt_status.value
        assert "forge-st-err" in wiz2.dt_status.value


class TestGridFileAttach:
    def test_attach_locks_and_populates_widgets(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")

        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)

        assert wiz._grid_file == {
            "location": str(p),
            "content_hash": wiz._grid_file["content_hash"],
        }
        assert isinstance(wiz._grid_file_grid, _FakeLoadedGrid)
        assert wiz._grid_file_grid.kwargs == {
            "filename": str(p)
        }  # loaded from this path
        assert wiz.grid_w["nx"].value == 10
        assert wiz.grid_w["center_lon"].value == 12.0
        assert wiz.scoord_chk.value is True  # theta_s/theta_b/hc all present

        for w in (
            *wiz.grid_w.values(),
            wiz.scoord_chk,
            wiz.hmin,
            wiz.close_narrow_chk,
            wiz.mask_shapefile,
            wiz.topo_source,
            wiz.topo_path,
            wiz.nest_enable,
            wiz.parent_enable,
        ):
            assert w.disabled is True
        assert "attached" in wiz.grid_file_status.value
        assert "exact path" in wiz.grid_file_status.value  # persistent warning

    def test_upload_fallback_stages_and_attaches(
        self, fake_grid, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)  # forge_user_files/ lands under Path.cwd()
        wiz = _new_wizard()
        src = _write_tiny_netcdf(tmp_path / "uploaded.nc")
        change = {"new": ({"name": "uploaded.nc", "content": src.read_bytes()},)}

        wiz._on_grid_file_upload(change)

        staged = tmp_path / "forge_user_files" / "uploaded.nc"
        assert staged.exists()
        assert wiz._grid_file == {
            "location": str(staged),
            "content_hash": wiz._grid_file["content_hash"],
        }
        assert wiz.grid_w["nx"].disabled is True

    def test_attach_error_shown_in_status_not_raised(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        wiz.grid_file_path.value = str(tmp_path / "does-not-exist.nc")

        wiz._on_grid_file_attach(None)  # must not raise

        assert wiz._grid_file is None
        assert "FileNotFoundError" in wiz.grid_file_status.value

    def test_detach_restores_widgets(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        pre_attach_nx = wiz.grid_w["nx"].value
        pre_attach_center_lon = wiz.grid_w["center_lon"].value
        assert pre_attach_nx != 10  # sanity: differs from _FakeLoadedGrid's nx
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        assert wiz.grid_w["nx"].disabled is True
        assert wiz.grid_w["nx"].value == 10  # overwritten by the attached file

        wiz._on_grid_file_detach(None)

        assert wiz._grid_file is None
        assert wiz._grid_file_grid is None
        assert wiz.grid_file_status.value == ""
        assert wiz.grid_file_path.value == ""
        for w in (*wiz.grid_w.values(), wiz.scoord_chk, wiz.nest_enable):
            assert w.disabled is False
        # The user's own pre-attach geometry is restored, not left at the
        # detached file's (now meaningless) values.
        assert wiz.grid_w["nx"].value == pre_attach_nx
        assert wiz.grid_w["center_lon"].value == pre_attach_center_lon

    def test_reattach_and_detach_restores_original_pre_attach_geometry(
        self, fake_grid, tmp_path
    ):
        """Re-attaching a second file without detaching first must not clobber
        the snapshot with the first file's values -- Detach must still give
        back the ORIGINAL pre-any-attach geometry.
        """
        wiz = _new_wizard()
        pre_attach_nx = wiz.grid_w["nx"].value
        p1 = _write_tiny_netcdf(tmp_path / "grid1.nc")
        wiz.grid_file_path.value = str(p1)
        wiz._on_grid_file_attach(None)
        assert wiz.grid_w["nx"].value == 10

        p2 = _write_tiny_netcdf(tmp_path / "grid2.nc")
        wiz.grid_file_path.value = str(p2)
        wiz._on_grid_file_attach(None)  # re-attach without detaching
        assert wiz.grid_w["nx"].value == 10  # still the (only) fake grid's nx

        wiz._on_grid_file_detach(None)

        assert wiz.grid_w["nx"].value == pre_attach_nx

    def test_gather_emits_grid_file_and_empty_grid_kwargs(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)

        kw = wiz._gather()

        assert kw["grid_file"] == wiz._grid_file
        assert kw["grid"] is wiz._grid_file_grid
        assert kw["grid_kwargs"] == {}
        assert "grid_kwargs_child" not in kw
        assert "grid_kwargs_parent" not in kw

    def test_rebuild_does_not_rehash_after_attach(
        self, fake_grid, tmp_path, monkeypatch
    ):
        """The one hash computation happens at Attach time; every subsequent
        _rebuild() (triggered here by an unrelated widget edit) must reuse the
        cached dict, never recomputing the digest.
        """
        import cstar.applications.forge.user_files as user_files_mod
        import cstar.wizard.forcing_editor as forcing_editor_mod
        import cstar.wizard.wizard as wizard_mod

        calls = {"n": 0}
        real_hash = user_files_mod.hash_netcdf_contents

        def _counting_hash(path):
            calls["n"] += 1
            return real_hash(path)

        monkeypatch.setattr(wizard_mod, "hash_netcdf_contents", _counting_hash)
        monkeypatch.setattr(forcing_editor_mod, "hash_netcdf_contents", _counting_hash)
        monkeypatch.setattr(user_files_mod, "hash_netcdf_contents", _counting_hash)

        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)  # submit -> auto-attach (one hash)
        assert calls["n"] == 1
        assert wiz._grid_file is not None

        wiz.description.value = "edited after attach"  # triggers _rebuild()
        wiz._rebuild()

        assert calls["n"] == 1  # never rehashed
        assert wiz.config is not None

        # Re-submitting the same path is a dedupe no-op; an explicit Attach click
        # is the one deliberate re-hash (the file may have changed on disk).
        wiz._maybe_attach_grid_file()
        assert calls["n"] == 1
        wiz._on_grid_file_attach(None)
        assert calls["n"] == 2

    def test_path_submit_auto_attaches_without_attach_click(self, fake_grid, tmp_path):
        """Regression (same shape as the river CUSTOM_FILE report): a typed/pasted
        grid path with no Attach click used to leave the wizard silently
        building from the grid_kwargs widgets instead of the file.
        """
        wiz = _new_wizard()
        assert wiz.grid_file_path.continuous_update is False
        p = _write_tiny_netcdf(tmp_path / "grid.nc")

        wiz.grid_file_path.value = str(p)  # Enter / focus-out, no click

        assert wiz._grid_file == {
            "location": str(p),
            "content_hash": wiz._grid_file["content_hash"],
        }
        assert wiz.grid_w["nx"].disabled  # locked, exactly like a click-attach
        assert "attached" in wiz.grid_file_status.value.lower()
        assert wiz._gather()["grid_file"] == wiz._grid_file

    def test_config_round_trips_attached_and_locked_through_populate_from(
        self, fake_grid, tmp_path
    ):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        assert wiz.config is not None
        cfg = wiz.config

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert wiz2._grid_file is not None
        assert wiz2._grid_file["content_hash"] == wiz._grid_file["content_hash"]
        assert wiz2.grid_w["nx"].disabled is True
        assert wiz2.config is not None
        assert "attached" in wiz2.grid_file_status.value

    def test_reattach_failure_keeps_locked_and_surfaces_error(
        self, fake_grid, tmp_path
    ):
        """A missing file at reload time must not silently fall back to the
        default/generic grid_kwargs (which would gather a different blueprint) --
        the grid_file dict + locked widgets stay in place, and _gather()/
        _rebuild() surface the failure loudly (config goes Invalid).
        """
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        cfg = wiz.config

        p.unlink()  # the file is now missing at "reload" time

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert wiz2._grid_file is not None  # kept, not cleared
        assert wiz2._grid_file_grid is None
        assert wiz2.grid_w["nx"].disabled is True  # still locked
        assert "could not re-attach" in wiz2.grid_file_status.value
        assert wiz2.config is None  # surfaced loudly, not silently substituted

        # Plot/Derive/the Save-Run safety net must likewise refuse to silently
        # build a grid from the (locked, stale) grid_w values instead of the
        # missing file -- not just build_forge_blueprint().
        with pytest.raises(RuntimeError, match="failed to"):
            wiz2._build_grid_from_widgets()
        # _populate_from freezes a loaded file's boundaries as touched (a
        # deliberate, already-resolved choice -- see _populate_from), which
        # would short-circuit _ensure_boundaries_derived() before it ever
        # reaches _build_grid_from_widgets(); force the untouched path here to
        # actually exercise the safety net's own grid-build attempt.
        wiz2._boundaries_touched = False
        assert wiz2._ensure_boundaries_derived() is False
        assert "failed to" in wiz2.derive_status.value

    def test_domain_pick_detaches_attached_grid_file(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        assert wiz._grid_file is not None

        wiz.domain_dd.value = ForgeBlueprintWizard._dd_values(wiz.domain_dd)[
            1
        ]  # first real catalog domain

        assert wiz._grid_file is None
        assert wiz.grid_w["nx"].disabled is False

    def test_save_domain_guards_while_grid_file_attached(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        wiz.save_domain_name.value = "some-new-domain"

        wiz._on_save_domain(None)

        assert "Detach the grid file first" in wiz.save_domain_status.value


class TestCdrFileAttach:
    def test_attach_populates_and_clears_yaml_upload(self, tmp_path):
        wiz = _new_wizard()
        wiz._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))
        assert wiz._cdr_forcing is not None

        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)  # submit -> auto-attach, no click needed

        assert wiz._cdr_forcing_file == {
            "location": str(p),
            "content_hash": wiz._cdr_forcing_file["content_hash"],
        }
        assert wiz._cdr_forcing is None
        assert "cleared" in wiz.cdr_file_status.value.lower()
        assert "attached" in wiz.cdr_file_status.value.lower()

    def test_netcdf_mode_shows_hint_until_attached(self, tmp_path):
        """CdrSpec rejects mode='netcdf' without cdr_forcing_file; the status
        slot says so (and what to do) instead of staying blank, and Clear
        brings the hint back.
        """
        wiz = _new_wizard()
        assert wiz.cdr_file_status.value == ""

        wiz.cdr_mode_dd.value = "netcdf"
        assert "no file attached yet" in wiz.cdr_file_status.value.lower()
        assert wiz.config is None  # invalid until attached, as before

        wiz.cdr_file_path.value = str(_write_tiny_cdr_netcdf(tmp_path / "cdr.nc"))
        assert "attached" in wiz.cdr_file_status.value.lower()
        assert "no file attached yet" not in wiz.cdr_file_status.value.lower()
        assert wiz.config is not None, wiz.derived.value

        wiz._on_cdr_file_clear(None)
        assert "no file attached yet" in wiz.cdr_file_status.value.lower()

        wiz.cdr_mode_dd.value = "none"  # leaving netcdf mode clears the slot
        assert wiz.cdr_file_status.value == ""

    def test_upload_fallback_stages_and_attaches(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # forge_user_files/ lands under Path.cwd()
        wiz = _new_wizard()
        src = _write_tiny_cdr_netcdf(tmp_path / "uploaded_cdr.nc")
        change = {"new": ({"name": "uploaded_cdr.nc", "content": src.read_bytes()},)}

        wiz._on_cdr_file_upload(change)

        staged = tmp_path / "forge_user_files" / "uploaded_cdr.nc"
        assert staged.exists()
        assert wiz._cdr_forcing_file == {
            "location": str(staged),
            "content_hash": wiz._cdr_forcing_file["content_hash"],
        }

    def test_yaml_upload_clears_attached_cdr_file(self, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)
        assert wiz._cdr_forcing_file is not None

        wiz._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))

        assert wiz._cdr_forcing is not None
        assert wiz._cdr_forcing_file is None
        assert "cleared" in wiz.cdr_status.value.lower()

    def test_attach_warns_but_does_not_block_when_ncdr_dim_missing(self, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc", with_ncdr_dim=False)
        wiz.cdr_file_path.value = str(p)

        wiz._on_cdr_file_attach(None)

        assert wiz._cdr_forcing_file is not None  # not blocked
        assert "ncdr" in wiz.cdr_file_status.value.lower()

    def test_gather_emits_cdr_forcing_file(self, tmp_path):
        wiz = _new_wizard()
        wiz.cdr_mode_dd.value = "netcdf"
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)

        kw = wiz._gather()

        assert kw["cdr"]["cdr_forcing_file"] == wiz._cdr_forcing_file
        assert kw["cdr"]["mode"] == "netcdf"
        assert kw["cdr"]["cdr_forcing"] is None

    def test_clear_resets_state(self, tmp_path):
        wiz = _new_wizard()
        wiz.cdr_mode_dd.value = "netcdf"
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)

        wiz._on_cdr_file_clear(None)

        assert wiz._cdr_forcing_file is None
        assert wiz.cdr_file_path.value == ""
        # Still in netcdf mode with nothing attached -> the not-attached hint.
        assert "no file attached yet" in wiz.cdr_file_status.value.lower()
        assert wiz._gather()["cdr"]["cdr_forcing_file"] is None

    def test_round_trips_through_populate_from(self, tmp_path):
        wiz = _new_wizard()
        wiz.cdr_mode_dd.value = "netcdf"
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)
        assert wiz.config is not None
        cfg = wiz.config
        assert cfg.cdr.mode == "netcdf"

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert wiz2.cdr_mode_dd.value == "netcdf"
        assert wiz2._cdr_forcing_file == wiz._cdr_forcing_file
        assert "attached" in wiz2.cdr_file_status.value.lower()
        assert wiz2.config is not None

    def test_round_trips_through_populate_from_with_missing_file(self, tmp_path):
        """The missing-file warning: trusts the stored content_hash (never
        rehashes) and surfaces a warning instead of blocking the load.
        """
        wiz = _new_wizard()
        wiz.cdr_mode_dd.value = "netcdf"
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)
        assert wiz.config is not None
        cfg = wiz.config
        original_file_dict = dict(wiz._cdr_forcing_file)

        p.unlink()  # the file no longer exists at its recorded path

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert wiz2.cdr_mode_dd.value == "netcdf"
        # hash preserved from the blueprint, not recomputed (the file is gone --
        # recomputing would raise or silently produce a different hash).
        assert wiz2._cdr_forcing_file == original_file_dict
        assert "not found" in wiz2.cdr_file_status.value.lower()

    def test_mode_switch_away_from_netcdf_clears_attached_cdr_file(self, tmp_path):
        """Leaving "netcdf" mode clears the attached file (see _apply_cdr_mode's
        leaving-mode cleanup) -- the modern equivalent of the old
        "a ForcingSpec carrying CDR forcing clears an attached CDR file" behavior,
        which no longer applies now that CDR is its own independently composable
        spec (a ForcingSpec pick never touches it, except the legacy-embed path
        covered by test_legacy_forcing_spec_cdr_embed_routes_to_yaml_mode).
        """
        wiz = _new_wizard()
        wiz.cdr_mode_dd.value = "netcdf"
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)
        assert wiz._cdr_forcing_file is not None

        wiz.cdr_mode_dd.value = "none"

        assert wiz._cdr_forcing_file is None
        assert wiz.cdr_file_path.value == ""
        assert wiz.cdr_file_status.value == ""

    def test_legacy_forcing_spec_cdr_embed_routes_to_yaml_mode(
        self, tmp_path, monkeypatch
    ):
        """A ForcingSpec predating the CdrSpec split that still embeds a
        ``cdr_forcing`` block (see ``_split_forcing_data``) is routed into the
        CDR box as if uploaded as a YAML -- CDR is otherwise untouched by a
        ForcingSpec pick (it's an independently composable spec, see
        self.cdr_mode_dd).
        """
        wiz = _new_wizard()
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz.cdr_mode_dd.value = "netcdf"
        wiz._on_cdr_file_attach(None)
        assert wiz._cdr_forcing_file is not None

        fake_spec = dict(wiz.catalog.forcing_data(wiz.forcing_dd.value))
        fake_spec["cdr_forcing"] = {"releases": []}
        monkeypatch.setattr(wiz.catalog, "forcing_data", lambda _name: fake_spec)

        wiz._on_forcing_spec(None)

        assert wiz.cdr_mode_dd.value == "yaml"
        assert wiz._cdr_forcing == {"releases": []}
        assert wiz._cdr_forcing_file is None
        assert "legacy ForcingSpec" in wiz.cdr_status.value


class TestRiverCustomFileAttach:
    def test_selecting_custom_file_toggles_visibility(self, editor):
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        assert _display(w["custom_file_path"]) == "none"
        assert _display(w["climatology"]) == ""
        assert _display(w["path"]) == ""

        w["name"].value = "CUSTOM_FILE"

        assert _display(w["custom_file_path"]) == ""
        assert _display(w["custom_file_attach_btn"]) == ""
        assert _display(w["custom_file_upload"]) == ""
        assert _display(w["custom_file_status"]) == ""
        assert _display(w["climatology"]) == "none"
        assert _display(w["include_bgc"]) == "none"
        assert _display(w["convert_to_climatology"]) == "none"
        assert _display(w["coast_snap_buffer_km"]) == "none"
        assert _display(w["domain_edge_buffer"]) == "none"
        assert _display(w["bgc_source_name"]) == "none"
        assert _display(w["bgc_source_path"]) == "none"
        assert _display(w["path"]) == "none"

        w["name"].value = "DAI"
        assert _display(w["custom_file_path"]) == "none"
        assert _display(w["climatology"]) == ""
        assert _display(w["path"]) == ""

    def test_include_bgc_visibility_still_works_after_leaving_custom_file(self, editor):
        """Switching CUSTOM_FILE -> DAI must hand bgc-widget visibility back to
        include_bgc's own sync, not leave it stuck from the custom-file branch.
        """
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        w["name"].value = "DAI"
        assert _display(w["bgc_source_name"]) == "none"  # include_bgc still False

        w["include_bgc"].value = True
        assert _display(w["bgc_source_name"]) == ""

    def test_selecting_custom_file_hides_temperature_source_widgets(self, editor):
        """A custom-file river carries no surface_forcing_source either (see
        RiverForcingItem._custom_file_excludes_surface_forcing_source) --
        mirrors the bgc-widget assertions above.
        """
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        assert _display(w["surface_forcing_source_name"]) == "none"
        assert _display(w["surface_forcing_source_path"]) == "none"
        assert _display(w["river_temp_smoothing_window_days"]) == "none"

        w["name"].value = "DAI"
        assert _display(w["surface_forcing_source_name"]) == ""

    def test_attach_and_gather_emits_custom_file_omits_standard_fields(
        self, editor, tmp_path
    ):
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        w["include_bgc"].value = True  # would-be leftover state; must be ignored
        p = _write_tiny_netcdf(tmp_path / "river.nc")
        w["custom_file_path"].value = str(p)

        w["custom_file_attach_btn"].click()

        assert w["_custom_file"] == {
            "location": str(p),
            "content_hash": w["_custom_file"]["content_hash"],
        }
        assert "attached" in w["custom_file_status"].value.lower()

        item = editor._gather_item("river", w)

        assert item == {
            "source": {"name": "CUSTOM_FILE"},
            "custom_file": w["_custom_file"],
        }

    def test_attach_and_gather_emits_custom_file_omits_temperature_source(
        self, editor, tmp_path
    ):
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        w["surface_forcing_source_name"].value = "ERA5"  # leftover state; ignored
        p = _write_tiny_netcdf(tmp_path / "river.nc")
        w["custom_file_path"].value = str(p)

        w["custom_file_attach_btn"].click()

        item = editor._gather_item("river", w)

        assert item == {
            "source": {"name": "CUSTOM_FILE"},
            "custom_file": w["_custom_file"],
        }

    def test_upload_fallback_stages_and_attaches(self, editor, tmp_path, monkeypatch):
        """Unlike the grid/CDR upload-fallback tests (which call the wizard's
        ``_on_*_upload`` handler directly, bypassing the widget), the river
        row's upload handler is a closure with no externally-reachable name --
        this must go through the real ``FileUpload.value`` trait, so the
        change item needs every key ipywidgets' own (de)serializer requires
        (name/type/size/content/last_modified), not just the two the handler
        itself reads.
        """
        import datetime as dt

        monkeypatch.chdir(tmp_path)  # forge_user_files/ lands under Path.cwd()
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        src = _write_tiny_netcdf(tmp_path / "uploaded_river.nc")
        content = src.read_bytes()

        w["custom_file_upload"].value = (
            {
                "name": "uploaded_river.nc",
                "type": "application/x-netcdf",
                "size": len(content),
                "content": content,
                "last_modified": dt.datetime.now(dt.UTC),
            },
        )

        staged = tmp_path / "forge_user_files" / "uploaded_river.nc"
        assert staged.exists()
        assert w["_custom_file"] == {
            "location": str(staged),
            "content_hash": w["_custom_file"]["content_hash"],
        }

    def test_attach_error_shown_in_status(self, editor, tmp_path):
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        w["custom_file_path"].value = str(tmp_path / "missing.nc")

        w["custom_file_attach_btn"].click()  # must not raise

        assert w["_custom_file"] is None
        assert "FileNotFoundError" in w["custom_file_status"].value

    def test_gather_item_hints_when_nothing_attached_yet(self, editor):
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"

        item = editor._gather_item("river", w)

        assert item == {"source": {"name": "CUSTOM_FILE"}}  # no custom_file key

    def test_path_submit_auto_attaches_without_attach_click(self, editor, tmp_path):
        """Regression: a user who typed/pasted a path and moved on without
        clicking Attach got ``RiverForcingItem`` "custom_file is not set" from
        the blueprint build. Submitting the path Text (Enter / focus-out, i.e.
        a ``value`` change with ``continuous_update=False``) now attaches.
        """
        changes: list[int] = []
        editor.on_change = lambda: changes.append(1)
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        assert w["custom_file_path"].continuous_update is False
        p = _write_tiny_netcdf(tmp_path / "river.nc")
        changes.clear()

        w["custom_file_path"].value = str(p)  # no Attach click

        assert w["_custom_file"] == {
            "location": str(p),
            "content_hash": w["_custom_file"]["content_hash"],
        }
        assert "attached" in w["custom_file_status"].value.lower()
        assert changes == [1]  # attached exactly once, and notified the wizard
        assert editor._gather_item("river", w) == {
            "source": {"name": "CUSTOM_FILE"},
            "custom_file": w["_custom_file"],
        }

        # Re-submitting the same path is a no-op (no re-hash, no extra notify).
        w["_maybe_attach_custom_file"](w)
        assert changes == [1]

    def test_gather_item_attaches_typed_but_unsubmitted_path(self, editor, tmp_path):
        """Last-resort path: the box holds a valid path but nothing attached it
        yet (e.g. gather ran before the Text's submit event landed). gather
        must attach on the fly -- silently, without on_change() (it's already
        running inside the wizard's rebuild) -- rather than emit an item the
        schema rejects.
        """
        changes: list[int] = []
        editor.on_change = lambda: changes.append(1)
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        p = _write_tiny_netcdf(tmp_path / "river.nc")
        w["custom_file_path"].value = str(p)
        w["_custom_file"] = None  # simulate "typed, never submitted"
        changes.clear()

        item = editor._gather_item("river", w)

        assert item["custom_file"]["location"] == str(p)
        assert w["_custom_file"] is not None
        assert changes == []  # no on_change() from inside gather

        # A bad path still leaves custom_file unset so the schema error surfaces
        # (and the failure is shown in the row status).
        w["custom_file_path"].value = str(tmp_path / "missing.nc")
        w["_custom_file"] = None
        assert editor._gather_item("river", w) == {"source": {"name": "CUSTOM_FILE"}}
        assert "FileNotFoundError" in w["custom_file_status"].value

    def test_selecting_custom_file_shows_hint_until_attached(self, editor, tmp_path):
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        assert w["custom_file_status"].value == ""

        w["name"].value = "CUSTOM_FILE"
        assert "no file attached yet" in w["custom_file_status"].value.lower()

        w["custom_file_path"].value = str(_write_tiny_netcdf(tmp_path / "r.nc"))
        assert "attached" in w["custom_file_status"].value.lower()
        assert "no file attached yet" not in w["custom_file_status"].value.lower()

        # A row seeded with a custom_file (load-back) never shows the hint.
        w2 = editor._make_row(
            "river",
            {
                "source": {"name": "CUSTOM_FILE"},
                "custom_file": {"location": "/x/r.nc", "content_hash": "abc" * 22},
            },
        )
        assert "no file attached yet" not in w2["custom_file_status"].value.lower()

    def test_wizard_builds_valid_config_after_path_submit_only(self, tmp_path):
        """End-to-end shape of the bug report: pick CUSTOM_FILE, enter a path,
        never click Attach -- the wizard's blueprint must still validate.
        """
        wiz = _new_wizard()
        w = wiz._forcing_editor._rows["river"][0]
        w["name"].value = "CUSTOM_FILE"
        wiz._rebuild()
        assert wiz.config is None  # nothing attached yet -> invalid, as before
        assert "Invalid blueprint" in wiz.derived.value
        assert "CUSTOM_FILE" in wiz.card_errors["forcing"].value

        w["custom_file_path"].value = str(_write_tiny_netcdf(tmp_path / "river.nc"))
        wiz._rebuild()

        assert wiz.config is not None, wiz.derived.value
        (river,) = [it for it in wiz.config.forcing.river if it.custom_file]
        assert river.source.name == "CUSTOM_FILE"

    def test_custom_file_round_trips_through_populate_from(self, tmp_path):
        wiz = _new_wizard()
        fe = wiz._forcing_editor
        assert fe._rows["river"], "expected a default river row from ForcingSpec"
        w = fe._rows["river"][0]
        w["name"].value = "CUSTOM_FILE"
        p = _write_tiny_netcdf(tmp_path / "river.nc")
        w["custom_file_path"].value = str(p)
        w["custom_file_attach_btn"].click()
        wiz._rebuild()
        assert wiz.config is not None, wiz.derived.value
        assert any(it.source.name == "CUSTOM_FILE" for it in wiz.config.forcing.river)

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)  # must not raise ValueError

        assert wiz2.config is not None
        fe2 = wiz2._forcing_editor
        custom_rows = [
            ws for ws in fe2._rows["river"] if ws["name"].value == "CUSTOM_FILE"
        ]
        assert len(custom_rows) == 1
        assert custom_rows[0]["_custom_file"]["location"] == str(p)
        assert _display(custom_rows[0]["custom_file_path"]) == ""
        assert _display(custom_rows[0]["climatology"]) == "none"

    def test_generic_source_path_round_trips_through_populate_from(self):
        """Non-custom river categories already carry SourceSpec.path (a WP3 fix);
        this pins the wizard's load-back side: w["path"] must repopulate too.
        """
        wiz = _new_wizard()
        fe = wiz._forcing_editor
        w = fe._rows["river"][0]
        w["name"].value = "DAI"
        w["path"].value = "/custom/river/source.nc"
        wiz._rebuild()
        assert wiz.config is not None
        dai_item = next(
            it for it in wiz.config.forcing.river if it.source.name == "DAI"
        )
        assert dai_item.source.path == "/custom/river/source.nc"

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)

        fe2 = wiz2._forcing_editor
        w2 = next(ws for ws in fe2._rows["river"] if ws["name"].value == "DAI")
        assert w2["path"].value == "/custom/river/source.nc"


# ===========================================================================
# WP2 (page layout) redesign: cards, sticky bar, chips, banners.
# ===========================================================================


def test_widget_root_has_forge_wizard_class():
    """The redesigned root VBox carries the ``forge-wizard`` class (and
    ``forge-app``, so ``components.WIZARD_CSS`` also matches outside AppShell).
    """
    wiz = ForgeBlueprintWizard()
    assert "forge-wizard" in wiz.widget._dom_classes
    assert "forge-app" in wiz.widget._dom_classes


def test_every_card_key_is_findable():
    """Every one of the nine cards (start/model/grid/forcing/run/output/advanced/
    review/next) is reachable from the root via its ``forge_key``.
    """
    wiz = ForgeBlueprintWizard()
    root = wiz.widget
    for key in (
        "start",
        "model",
        "grid",
        "forcing",
        "run",
        "output",
        "advanced",
        "review",
        "next",
    ):
        assert _find_card(root, key) is not None, f"missing card {key!r}"


def test_sticky_bar_reflects_model_and_validity_after_rebuild():
    """After a rebuild, the sticky bar names the selected model and shows a
    "Valid" (not "Invalid") status chip for a config that resolves cleanly.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config is not None, wiz.derived.value
    assert wiz.model_dd.value in wiz.sticky_bar.value
    assert "● Valid" in wiz.sticky_bar.value


def test_download_html_default_caption_is_the_full_filename():
    """With no ``caption``, `_download_html` keeps the Review card's copy:
    "Download <code>fname</code>", no ``title=``.
    """
    wiz = ForgeBlueprintWizard()
    cfg = wiz.config
    assert cfg is not None, wiz.derived.value
    fname = f"{cfg.name}.forge_blueprint.yaml"
    html = ForgeBlueprintWizard._download_html(cfg)
    assert f"Download <code>{fname}</code>" in html
    assert "title=" not in html


def test_download_html_custom_caption_moves_filename_to_title():
    """A ``caption`` becomes the link text verbatim; the filename moves to
    ``title=`` instead (the sticky bar's compact "Download blueprint" copy).
    """
    wiz = ForgeBlueprintWizard()
    cfg = wiz.config
    assert cfg is not None, wiz.derived.value
    fname = f"{cfg.name}.forge_blueprint.yaml"
    html = ForgeBlueprintWizard._download_html(cfg, caption="x")
    assert ">x<" in html
    assert f'title="{fname}"' in html


# ---------------------------------------------------------------------------
# Output card: the output streams live in their own card (main streams + a
# collapsed "More output streams" accordion), not in the Advanced card.
# ---------------------------------------------------------------------------


def test_output_card_sits_between_run_and_advanced():
    wiz = ForgeBlueprintWizard()
    root = wiz.widget
    cards = [c for c in root.children if getattr(c, "forge_key", None)]
    assert [c.forge_key for c in cards] == ["start", *(k for k, _ in _STICKY_STEPS)]
    keys = [k for k, _ in _STICKY_STEPS]
    assert keys.index("run") + 1 == keys.index("output") == keys.index("advanced") - 1
    for number, key in enumerate(keys, start=1):
        card = _find_card(root, key)
        assert f"<span class='num'>{number}</span>" in card.forge_header.value
    output_card = _find_card(root, "output")
    assert output_card.children[0].value == "<a id='forge-sec-output'></a>"
    assert output_card.forge_title == "Output"
    assert "forge-sec-output'>5 Output</a>" in wiz.sticky_bar.value


def test_output_editor_follows_the_nesting_toggles():
    wiz = _new_wizard()
    wiz._rebuild()
    plain = wiz.editor
    assert wiz.output_editor_box.children == (plain.output_box,)
    assert wiz.editor_box.children == (plain.accordion,)
    assert "extract_data" not in _built_fields(plain)
    assert "upscale_output" in plain._group_fields[_OUTPUT_MORE_TITLE]

    wiz.nest_enable.value = True
    parent = wiz.editor
    assert parent is not plain
    assert wiz.output_editor_box.children == (parent.output_box,)
    assert parent._group_fields[_OUTPUT_CARD]["extract_data"] == [
        "nrpf",
        "extract_root_name",
    ]
    wiz.nest_enable.value = False
    assert "extract_data" not in _built_fields(wiz.editor)

    wiz.parent_enable.value = True
    child = wiz.editor
    assert wiz.config is not None, wiz.derived.value
    assert "upscale_output" in child._group_fields[_OUTPUT_CARD]
    assert _OUTPUT_MORE_TITLE in child._group_fields
    assert "upscale_output" not in child._group_fields[_OUTPUT_MORE_TITLE]
    # the nesting toggles that do not change the role keep the same editor
    wiz.nest_pressure_fluxes.value = True
    assert wiz.editor is child


def test_attached_grid_file_drops_the_nesting_streams(fake_grid, tmp_path):
    """An attached grid file locks the nesting toggles without clearing them, and
    the resolver ignores nesting then -- the Output card follows the resolver.
    """
    wiz = _new_wizard()
    wiz.nest_enable.value = True
    assert "extract_data" in _built_fields(wiz.editor)

    wiz.grid_file_path.value = str(_write_tiny_netcdf(tmp_path / "grid.nc"))
    wiz._on_grid_file_attach(None)

    assert wiz.nest_enable.value is True and wiz.nest_enable.disabled
    assert wiz.config is not None, wiz.derived.value
    assert wiz.config.domain.grid_kwargs_child is None
    assert "extract_data" not in _built_fields(wiz.editor)


def test_output_card_edit_records_an_override_like_any_advanced_edit():
    wiz = _new_wizard()
    wiz._rebuild()
    widget = wiz.editor._widgets[("ocean_vars", "nrpf_his")][0]
    assert _is_in_tree(wiz.output_editor_box, widget)
    widget.value = widget.value + 1
    assert wiz._overrides == {("ocean_vars", "nrpf_his"): widget.value}
    assert wiz.config.composition.output.modified is True
    assert wiz.config.composition.model.modified is False


@pytest.mark.parametrize(
    ("key", "is_output_key"),
    [
        (("particles", "floats"), False),
        (("calc_pflx", "calc_pflx"), False),
        (("bgc", "wrt_his"), True),
        (("diagnostics", "diag_avg"), True),
    ],
)
def test_output_card_placement_does_not_change_output_spec_ownership(
    key, is_output_key
):
    """``particles``/``calc_pflx`` sit in the Output card but stay ModelSpec-owned;
    the OutputSpec's own keys (including bgc's write-controls) keep marking
    composition.output.modified.
    """
    wiz = _new_wizard()
    wiz._rebuild()
    assert _is_output_key(*key) is is_output_key
    assert _in_output_card(*key)
    widget = wiz.editor._widgets[key][0]
    assert _is_in_tree(wiz.output_editor_box, widget)
    widget.value = (not widget.value) if isinstance(widget.value, bool) else 3
    assert key in wiz._overrides
    assert wiz.config.composition.output.modified is is_output_key
    assert wiz.config.composition.model.modified is (not is_output_key)


def test_output_and_advanced_chips_partition_the_overrides():
    wiz = _new_wizard()
    wiz._rebuild()
    assert "At preset defaults" in wiz.card_chips["output"].value
    assert "All defaults" in wiz.card_chips["advanced"].value

    output_keys = [
        ("ocean_vars", "nrpf_his"),  # main stream
        ("particles", "floats"),  # More, ModelSpec-owned
        ("bgc", "wrt_his"),  # bgc's output half
    ]
    advanced_keys = [("lateral_visc", "visc2")]
    forcing_keys = [("bgc", "xco2air_default")]  # the Forcing card's static CO2
    for key in output_keys + advanced_keys + forcing_keys:
        widget = wiz.editor._widgets[key][0]
        widget.value = (not widget.value) if isinstance(widget.value, bool) else 7
    assert set(wiz._overrides) == {*output_keys, *advanced_keys, *forcing_keys}
    assert "● 3 modified" in wiz.card_chips["output"].value
    assert "● 1 modified" in wiz.card_chips["advanced"].value
    assert "Modified" in wiz.card_chips["forcing"].value


def test_output_card_titles_carry_modified_counts():
    wiz = _new_wizard()
    wiz._rebuild()
    editor = wiz.editor
    header = editor._headers[(_OUTPUT_CARD, "ocean_vars")]
    assert "modified" not in header.value
    assert "all defaults" in editor.more_streams.get_title(0)

    wiz.editor._widgets[("ocean_vars", "nrpf_his")][0].value += 1
    wiz.editor._widgets[("particles", "floats")][0].value = True
    assert "● 1 modified" in header.value
    assert "modified" not in editor._headers[(_OUTPUT_CARD, "surf_flux")].value
    assert "1 modified" in editor.more_streams.get_title(0)
    assert "all defaults" in editor.accordion.get_title(0)


# ---------------------------------------------------------------------------
# Hosted fields: the T/S write flags draw inside the ocean_vars history grid and
# the diagnostics row, and the compile-time diagnostics switch heads that row --
# all still keyed by their real (section, field).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("key", "output_owned", "header"),
    [
        (("ts_output", "wrt_temp"), True, "ocean_vars"),
        (("ts_output", "wrt_salt_dia"), True, None),
        (("cppdefs", "diagnostics"), False, None),
    ],
)
def test_hosted_field_overrides_keep_ownership_and_count_in_the_output_card(
    key, output_owned, header
):
    """A hosted field counts under its host: the host's section header in the Output
    card, else the "More output streams" accordion title; ownership is unchanged.
    """
    wiz = _new_wizard()
    wiz._rebuild()
    editor = wiz.editor
    assert _is_output_key(*key) is output_owned
    widget = editor._widgets[key][0]
    assert _is_in_tree(wiz.output_editor_box, widget)
    widget.value = not widget.value

    assert wiz._overrides == {key: widget.value}
    assert wiz.config.composition.output.modified is output_owned
    assert wiz.config.composition.model.modified is (not output_owned)
    assert "● 1 modified" in wiz.card_chips["output"].value
    assert "All defaults" in wiz.card_chips["advanced"].value
    if header:
        assert "● 1 modified" in editor._headers[(_OUTPUT_CARD, header)].value
        assert "all defaults" in editor.more_streams.get_title(0)
    else:
        assert "1 modified" in editor.more_streams.get_title(0)
        assert "modified" not in editor._headers[(_OUTPUT_CARD, "ocean_vars")].value


def _nested_config_at_1800():
    wiz = _new_wizard()
    wiz.nest_enable.value = True
    wiz.nest_period.value = 1800.0
    wiz._rebuild()
    cfg = wiz.config
    assert cfg is not None, wiz.derived.value
    assert cfg.domain.metadata_child["period"] == 1800.0
    return cfg


def test_nest_period_loads_from_metadata_child_over_a_stale_snapshot():
    """A stored extract_period that differs from ``metadata_child.period`` (a
    workplan override of the domain's period, a hand edit) is a stale copy: the
    run writes metadata_child's period, so the wizard shows and keeps that.
    """
    cfg = _nested_config_at_1800()
    settings = copy.deepcopy(cfg.model_settings)
    settings["extract_data"]["extract_period"] = 3600.0
    wiz2 = _new_wizard()
    wiz2._populate_from(cfg.model_copy(update={"model_settings": settings}))

    assert wiz2.nest_period.value == 1800.0
    assert wiz2.config.model_settings["extract_data"]["extract_period"] == 1800.0
    assert wiz2._gather()["metadata_child"] == {"period": 1800.0}


def test_nest_period_loads_an_older_files_extract_period_override():
    """An extract_period override recorded before the field moved to the nesting
    options (Advanced) is the period that file ran with; loading it keeps that
    value, with nest_period its one editor, and the override is not kept.
    """
    cfg = _nested_config_at_1800()
    settings = copy.deepcopy(cfg.model_settings)
    settings["extract_data"]["extract_period"] = 3600.0
    composition = cfg.composition.model_copy(
        update={"overrides": {"extract_data": {"extract_period": 3600.0}}}
    )
    wiz2 = _new_wizard()
    wiz2._populate_from(
        cfg.model_copy(update={"model_settings": settings, "composition": composition})
    )

    assert wiz2.nest_period.value == 3600.0
    assert wiz2.config.model_settings["extract_data"]["extract_period"] == 3600.0
    assert wiz2._gather()["metadata_child"] == {"period": 3600.0}
    assert ("extract_data", "extract_period") not in wiz2._overrides


def test_nest_period_falls_back_to_metadata_child_without_an_extract_period():
    wiz = _new_wizard()
    wiz.nest_enable.value = True
    wiz.nest_period.value = 1800.0
    wiz._rebuild()
    cfg = wiz.config
    settings = copy.deepcopy(cfg.model_settings)
    del settings["extract_data"]["extract_period"]

    wiz2 = _new_wizard()
    wiz2._populate_from(cfg.model_copy(update={"model_settings": settings}))
    assert wiz2.nest_period.value == 1800.0
    assert wiz2.config.model_settings["extract_data"]["extract_period"] == 1800.0


def test_extract_data_nesting_fields_never_become_overrides():
    """The nesting-owned extract_data fields are excluded from the accordion layer:
    a hand-edited file cannot reintroduce one with no widget to show or clear it.
    """
    wiz = _new_wizard()
    wiz.nest_enable.value = True
    cfg = wiz.config
    settings = copy.deepcopy(cfg.model_settings)
    settings["extract_data"].update(n_chd=77, nrpf=48)
    wiz2 = _new_wizard()
    wiz2._populate_from(cfg.model_copy(update={"model_settings": settings}))
    assert ("extract_data", "n_chd") not in wiz2._overrides
    assert wiz2._overrides[("extract_data", "nrpf")] == 48


def test_the_specs_the_wizard_saves_have_only_known_keys():
    """A spec at a newer minor with a key this build does not know is refused, so
    every key a writer saves must be in ``known_spec_keys`` -- here for a nested
    domain with every optional key, and for the forcing the editor holds.
    """
    wiz = _new_wizard()
    wiz.nest_enable.value = True
    wiz.nest_pressure_fluxes.value = True
    wiz.parent_enable.value = True
    wiz._rebuild()
    assert wiz.config is not None, wiz.derived.value

    domain = wiz._domain_spec_data()
    assert {"grid_kwargs_parent", "grid_kwargs_child", "metadata_child"} <= set(domain)
    assert domain["nesting_include_pressure_fluxes"] is True
    assert set(domain) <= known_spec_keys("DomainSpec")
    forcing = {"description", *wiz._forcing_editor.gather()}
    assert forcing <= known_spec_keys("ForcingSpec")


# ---------------------------------------------------------------------------
# Provenance: the wizard stamps a blueprint when it writes one, and re-stamps it
# only when the content changed (see ForgeBlueprint.stamp_provenance).
# ---------------------------------------------------------------------------


def _saving_wizard(path):
    """A wizard holding the default valid config, set to save to ``path``."""
    wiz = _new_wizard()
    wiz._boundaries_touched = True  # not exercising boundary derivation here
    wiz._rebuild()
    assert wiz.config is not None, wiz.derived.value
    wiz.save_path.value = str(path)
    return wiz


def _save(wiz):
    """Press Save and return the provenance of the file it wrote."""
    wiz._on_save(None)
    assert "Saved" in wiz.save_status.value, wiz.save_status.value
    return ForgeBlueprint.from_yaml(wiz.save_path.value).provenance


def _downloaded_yaml(link_html):
    """The parsed YAML a download link carries in its data URI."""
    import base64
    import re

    payload = re.search(r"base64,([A-Za-z0-9+/=]+)", link_html).group(1)
    return yaml.safe_load(base64.b64decode(payload))


def test_save_stamps_the_blueprint_as_written_by_the_wizard(tmp_path):
    wiz = _saving_wizard(tmp_path / "bp.yaml")
    assert wiz._carried_provenance == ForgeProvenance()  # nothing recorded yet

    saved = _save(wiz)

    assert saved.generated_by.tool == WIZARD_TOOL
    assert saved.generated_at is not None
    assert saved.content_hash == wiz.config.content_hash()
    # the stamp is what the wizard now carries, and what its config holds
    assert wiz._carried_provenance == saved
    assert wiz.config.provenance == saved


def test_saving_a_loaded_blueprint_unchanged_keeps_its_stamp(tmp_path):
    first = _save(_saving_wizard(tmp_path / "first.yaml"))

    wiz = _new_wizard()
    wiz.load_path.value = str(tmp_path / "first.yaml")
    wiz._on_load_path(None)
    assert wiz._carried_provenance == first  # the load keeps what the file recorded
    wiz.save_path.value = str(tmp_path / "second.yaml")
    wiz._boundaries_touched = True
    second = _save(wiz)

    assert second.generated_by == first.generated_by
    assert second.generated_at == first.generated_at


def test_saving_a_never_stamped_blueprint_unchanged_records_the_wizard(tmp_path):
    """A blueprint written without a stamp (a script's plain ``to_yaml``, a catalog
    entry) records a hash that already matches its content: saved unchanged, it
    must still name the wizard as its producer.
    """
    writer = _new_wizard()
    writer._rebuild()
    assert writer.config is not None, writer.derived.value
    writer.config.to_yaml(tmp_path / "unstamped.yaml")  # the hash, and nothing else

    wiz = _new_wizard()
    wiz.load_path.value = str(tmp_path / "unstamped.yaml")
    wiz._on_load_path(None)
    assert wiz.config is not None, wiz.derived.value
    # no producer and a matching hash: only the missing stamp can trigger a stamp
    assert wiz.config.provenance.generated_by is None
    assert wiz.config.provenance.content_hash == wiz.config.content_hash()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True

    saved = _save(wiz)

    assert saved.generated_by is not None
    assert saved.generated_by.tool == WIZARD_TOOL
    assert saved.content_hash == wiz.config.content_hash()


def test_saving_after_an_edit_restamps_the_blueprint(tmp_path):
    wiz = _saving_wizard(tmp_path / "bp.yaml")
    first = _save(wiz)

    wiz.end.value = date(2012, 1, 3)  # a longer run: results-affecting
    second = _save(wiz)

    assert second.content_hash != first.content_hash
    assert second.generated_by.tool == WIZARD_TOOL
    assert second.generated_by.id != first.generated_by.id
    assert second.generated_at >= first.generated_at


def test_saving_twice_without_changes_keeps_the_stamp(tmp_path):
    wiz = _saving_wizard(tmp_path / "bp.yaml")
    first = _save(wiz)

    wiz._rebuild()  # a re-resolve in between must hand the stamp back
    assert wiz.config.provenance == first
    second = _save(wiz)

    assert second.generated_by == first.generated_by
    assert second.generated_at == first.generated_at


def test_editing_a_description_does_not_restamp(tmp_path):
    """The description is excluded from the content hash, so it is not content."""
    wiz = _saving_wizard(tmp_path / "bp.yaml")
    first = _save(wiz)

    wiz.description.value = "a different description"
    second = _save(wiz)

    assert second.generated_by == first.generated_by


def test_run_autosave_stamps_through_the_same_path(monkeypatch, tmp_path):
    wiz = _run_wiz_with_output(monkeypatch, tmp_path, b"ok\n")

    saved = ForgeBlueprint.from_yaml(tmp_path / "bp.yaml").provenance
    assert saved.generated_by.tool == WIZARD_TOOL
    assert wiz._carried_provenance == saved


def test_rebuild_preview_and_download_do_not_advance_the_carried_provenance(capsys):
    wiz = _new_wizard()
    wiz._boundaries_touched = True  # the links wait for real boundaries
    wiz._rebuild()
    carried = wiz._carried_provenance
    assert carried == ForgeProvenance()

    wiz.end.value = date(2012, 1, 3)  # content differs from anything recorded
    capsys.readouterr()  # drop what the earlier rebuilds printed
    wiz._rebuild()
    preview = capsys.readouterr().out

    assert wiz._carried_provenance is carried
    # the live preview and config stay unstamped: no id is minted per keystroke
    assert wiz.config.provenance == carried
    assert "generated_by: null" in preview
    assert f"tool: {WIZARD_TOOL}" not in preview
    # ... while what a download link carries is a stamped copy, the same in both links
    sticky = _downloaded_yaml(wiz.download_link.value)
    review = _downloaded_yaml(wiz.download_link_review.value)
    assert sticky["provenance"]["generated_by"]["tool"] == WIZARD_TOOL
    assert sticky["provenance"]["content_hash"] == wiz.config.content_hash()
    assert review == sticky


def test_a_download_does_not_count_as_a_save(tmp_path):
    """Saving after a rebuild stamps afresh: the download's stamp was never carried."""
    wiz = _saving_wizard(tmp_path / "bp.yaml")
    downloaded = _downloaded_yaml(wiz.download_link.value)["provenance"]["generated_by"]

    saved = _save(wiz)

    assert saved.generated_by.id != downloaded["id"]


def test_download_links_hand_out_the_file_just_saved(tmp_path):
    """Saving and then downloading with no edit between must not mint a second
    stamp: both links carry the very file that was written.
    """
    wiz = _saving_wizard(tmp_path / "bp.yaml")

    saved = _save(wiz)

    written = yaml.safe_load((tmp_path / "bp.yaml").read_text())
    for link in (wiz.download_link, wiz.download_link_review):
        downloaded = _downloaded_yaml(link.value)
        assert downloaded["provenance"]["generated_by"]["id"] == saved.generated_by.id
        assert downloaded == written


def test_grid_chip_shows_fields_to_check_initially():
    """A freshly-built wizard hasn't derived or touched boundaries/v_sponge
    yet, so the Grid card's chip reports outstanding fields.
    """
    wiz = ForgeBlueprintWizard()
    assert "fields to check" in wiz.card_chips["grid"].value


def test_forcing_accordion_first_title_contains_ic_source_name():
    """The forcing accordion's first pane (initial conditions) is retitled
    with a live summary that names the current IC source.
    """
    wiz = ForgeBlueprintWizard()
    title0 = wiz._forcing_editor._acc.get_title(0)
    assert wiz._forcing_editor.ic_name.value in title0


def test_set_grid_widgets_locked_toggles_grid_lock_banner():
    wiz = ForgeBlueprintWizard()
    assert wiz.grid_lock_banner.layout.display == "none"

    wiz._set_grid_widgets_locked(True)
    assert wiz.grid_lock_banner.layout.display == ""

    wiz._set_grid_widgets_locked(False)
    assert wiz.grid_lock_banner.layout.display == "none"


def test_wizard_module_keeps_the_public_names_that_moved():
    """Notebooks import these from ``cstar.wizard.wizard``; they now live elsewhere."""
    from cstar.wizard import app, fields, settings_editor, wizard

    assert wizard.ForgeBlueprintWizardApp is app.ForgeBlueprintWizardApp
    assert wizard.HELP_TEXT is fields.HELP_TEXT
    assert wizard.LABEL_TEXT is settings_editor.LABEL_TEXT


# ---------------------------------------------------------------------------
# bgc_mode "cdr_lite": the BGC dropdown option, the carbonate sensitivities
# group (cs_*), and the CDR panel's cdr_lite hint.
# ---------------------------------------------------------------------------


def _strip_bgc_forcing(wiz) -> None:
    """Remove every BGC signal from the bundled ForcingSpec's widgets, so
    bgc_mode "none"/"cdr_lite" (which reject BGC forcing) resolves.
    """
    fe = wiz._forcing_editor
    for ws in list(fe._rows["surface"]):
        if ws.get("type") is not None and ws["type"].value == "bgc":
            fe._remove("surface", ws)
    for section in ("boundary_bgc", "ic_bgc"):
        for ws in list(fe._rows[section]):
            fe._remove(section, ws)
    for ws in list(fe._rows["river"]):
        if "include_bgc" in ws:
            ws["include_bgc"].value = False


def _cdr_lite_wizard(catalog=None):
    """A wizard in bgc_mode "cdr_lite" with physics-only forcing and a simple CDR
    release (the mode requires a CDR forcing; "simple" targets the CDR-lite
    tracers by default).
    """
    wiz = ForgeBlueprintWizard() if catalog is None else ForgeBlueprintWizard(catalog)
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.use_pio_chk.value = False  # off: no CDF-5 conversion warning per rebuild
    _strip_bgc_forcing(wiz)
    wiz.bgc_dd.value = "cdr_lite"
    wiz.cdr_mode_dd.value = "simple"
    return wiz


def _write_cdrgas_dir(
    directory: Path,
    stamps=("20120101000000", "20120102000000"),
    fmt: str = "NETCDF4",
) -> Path:
    """A directory of joined ROMS ``_cdrgas`` files (real, tiny netCDF)."""
    import numpy as np
    import xarray as xr

    directory.mkdir(parents=True, exist_ok=True)
    for stamp in stamps:
        ds = xr.Dataset({"ddic_dco2": (["y", "x"], np.zeros((2, 2)))})
        ds.to_netcdf(directory / f"roms_cdrgas.{stamp}.nc", format=fmt)
    return directory


def _write_cdr_netcdf_with_tracers(path: Path, tracer_names) -> Path:
    import numpy as np
    import xarray as xr

    ds = xr.Dataset(
        {"cdr_volume": (["ncdr"], np.zeros(3))},
        coords={"tracer_name": ("ntracers", list(tracer_names))},
    )
    ds.to_netcdf(path)
    return path


def test_bgc_dd_offers_the_three_build_modes_in_order():
    from typing import get_args

    from cstar.applications.forge.namelist_model import BgcMode

    wiz = ForgeBlueprintWizard()
    assert list(wiz.bgc_dd.options) == ["marbl", "none", "cdr_lite"]
    assert list(wiz.bgc_dd.options) == list(get_args(BgcMode))
    assert "cdr_lite" in wiz.bgc_dd.tooltip


def test_marbl_ref_hidden_for_cdr_lite():
    wiz = ForgeBlueprintWizard()
    wiz.bgc_dd.value = "cdr_lite"
    assert _display(wiz.marbl_ref) == "none"


class TestCarbonateSensitivityGroup:
    def test_group_lives_in_the_model_card_and_is_hidden_by_default(self):
        wiz = ForgeBlueprintWizard()
        model_card = _find_card(wiz.widget, "model")
        assert wiz.cs_group in _descendants(model_card)
        assert wiz.bgc_dd.value == "marbl"
        assert _display(wiz.cs_group) == "none"

    def test_group_shows_only_for_cdr_lite(self):
        wiz = ForgeBlueprintWizard()
        wiz.bgc_dd.value = "cdr_lite"
        assert _display(wiz.cs_group) == ""
        wiz.bgc_dd.value = "none"
        assert _display(wiz.cs_group) == "none"
        wiz.bgc_dd.value = "marbl"
        assert _display(wiz.cs_group) == "none"

    def test_source_defaults_to_the_directive_and_hides_the_attach_row(self):
        wiz = ForgeBlueprintWizard()
        wiz.bgc_dd.value = "cdr_lite"
        assert wiz.cs_source_dd.value == "directive"
        assert [v for _, v in wiz.cs_source_dd.options] == ["directive", "path"]
        assert _display(wiz.cs_path_box) == "none"

        wiz.cs_source_dd.value = "path"
        assert _display(wiz.cs_path_box) == ""
        assert "no directory attached yet" in wiz.cs_status.value.lower()

        wiz.cs_source_dd.value = "directive"
        assert _display(wiz.cs_path_box) == "none"

    def test_default_cdr_lite_blueprint_has_no_carbonate_sensitivity(self):
        wiz = _cdr_lite_wizard()
        assert wiz.config is not None, wiz.derived.value
        assert wiz.config.carbonate_sensitivity is None
        assert wiz.config.model_settings["cppdefs"]["cdr_lite"] is True
        assert wiz.config.model_settings["cppdefs"]["marbl"] is False
        assert "carbonate_sensitivity" not in wiz._gather()

    def test_user_file_status_shows_hex_not_scheme_tag(self):
        from cstar.applications.forge.user_files import HASH_SCHEME
        from cstar.wizard.fields import _user_file_status_html

        status = _user_file_status_html(
            {"location": "/x/grid.nc", "content_hash": f"{HASH_SCHEME}:" + "ab" * 32}
        )
        assert "grid.nc (sha256 abababababab…)" in status

    def test_attach_stores_the_joined_files_and_reports_the_span(self, tmp_path):
        from cstar.applications.forge.user_files import HASH_SCHEME

        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz.cs_path.value = str(d)  # submit -> auto-attach, no click needed

        files = wiz._carbonate_sensitivity["files"]
        assert [Path(f["location"]).name for f in files] == [
            "roms_cdrgas.20120101000000.nc",
            "roms_cdrgas.20120102000000.nc",
        ]
        assert all(f["content_hash"].startswith(f"{HASH_SCHEME}:") for f in files)
        status = wiz.cs_status.value
        assert "attached 2 carbonate sensitivity file(s)" in status
        assert "2012-01-01 00:00 to 2012-01-02 00:00" in status
        assert "exact paths" in status
        assert wiz.config is not None, wiz.derived.value
        assert [f.location for f in wiz.config.carbonate_sensitivity.files] == [
            f["location"] for f in files
        ]

    def test_button_attach_and_rehash(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz.cs_path.value = str(d)
        wiz._carbonate_sensitivity = None
        wiz._on_cs_attach(None)
        assert len(wiz._carbonate_sensitivity["files"]) == 2

    def test_attach_without_a_path_asks_for_one(self):
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._on_cs_attach(None)
        assert "enter a path" in wiz.cs_status.value.lower()
        assert wiz._carbonate_sensitivity is None

    def test_per_rank_tiles_are_ignored(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        _write_cdrgas_dir(d, stamps=("20120101000000.0", "20120101000000.1"))
        assert (d / "roms_cdrgas.20120101000000.0.nc").exists()
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(d))
        names = [Path(f["location"]).name for f in wiz._carbonate_sensitivity["files"]]
        assert names == [
            "roms_cdrgas.20120101000000.nc",
            "roms_cdrgas.20120102000000.nc",
        ]
        assert "attached 2 " in wiz.cs_status.value

    def test_directory_of_only_tiles_is_an_error(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "tiles", stamps=("20120101000000.0",))
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(d))
        assert wiz._carbonate_sensitivity is None
        assert "1 per-rank tile" in wiz.cs_status.value
        assert "forge-st-err" in wiz.cs_status.value  # the error colour

    def test_empty_directory_is_an_error(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(d))
        assert wiz._carbonate_sensitivity is None
        assert "no joined carbonate sensitivity files" in wiz.cs_status.value
        assert wiz.config is None
        assert "attach a directory" in wiz.derived.value

    def test_missing_path_and_plain_file_are_errors(self, tmp_path):
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(tmp_path / "nope"))
        assert wiz._carbonate_sensitivity is None
        assert "not a directory" in wiz.cs_status.value

    def test_non_netcdf_file_is_an_error_and_nothing_is_stored(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        (d / "roms_cdrgas.20120103000000.nc").write_text("this is not netCDF")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(d))
        assert wiz._carbonate_sensitivity is None
        assert "not netCDF files: roms_cdrgas.20120103000000.nc" in wiz.cs_status.value

    def test_misnamed_cdrgas_file_is_an_error_with_a_legible_status(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        (d / "roms_cdrgas_old.nc").write_text("x")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(d))
        assert wiz._carbonate_sensitivity is None
        assert "naming convention" in wiz.cs_status.value

    def test_repeated_basenames_under_subdirectories_are_an_error(self, tmp_path):
        d = tmp_path / "runs"
        _write_cdrgas_dir(d / "run1", stamps=("20120101000000",))
        _write_cdrgas_dir(d / "run2", stamps=("20120101000000",))
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(d))
        assert wiz._carbonate_sensitivity is None
        assert "each must be distinct" in wiz.cs_status.value
        assert "✓" not in wiz.cs_status.value

    def test_failed_reattach_drops_the_earlier_attach(self, tmp_path):
        good = _write_cdrgas_dir(tmp_path / "good")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(good))
        assert wiz._carbonate_sensitivity is not None

        wiz._attach_carbonate_sensitivity_dir(str(tmp_path / "nope"))
        assert wiz._carbonate_sensitivity is None  # not quietly kept
        assert wiz.config is None

    def test_pio_hint_for_non_classic_files(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "hdf5", fmt="NETCDF4")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz.use_pio_chk.value = True
        wiz._attach_carbonate_sensitivity_dir(str(d))
        assert "CDF-5" in wiz.cs_status.value

    def test_no_pio_hint_without_pio_or_for_classic_files(self, tmp_path):
        hdf5 = _write_cdrgas_dir(tmp_path / "hdf5", fmt="NETCDF4")
        classic = _write_cdrgas_dir(tmp_path / "classic", fmt="NETCDF3_64BIT")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz.use_pio_chk.value = False
        wiz._attach_carbonate_sensitivity_dir(str(hdf5))
        assert "CDF-5" not in wiz.cs_status.value
        wiz.use_pio_chk.value = True
        wiz._attach_carbonate_sensitivity_dir(str(classic))
        assert "CDF-5" not in wiz.cs_status.value

    def test_clear_resets_state_and_brings_the_hint_back(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz.cs_path.value = str(d)
        assert wiz._carbonate_sensitivity is not None

        wiz._on_cs_clear(None)

        assert wiz._carbonate_sensitivity is None
        assert wiz.cs_path.value == ""
        assert "no directory attached yet" in wiz.cs_status.value.lower()
        assert wiz.config is None

    def test_gather_passes_the_attached_files_only_for_the_path_source(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(d))

        assert wiz._gather()["carbonate_sensitivity"] == wiz._carbonate_sensitivity

        wiz.cs_source_dd.value = "directive"  # attached state kept, but not passed
        assert wiz._carbonate_sensitivity is not None
        assert "carbonate_sensitivity" not in wiz._gather()
        assert wiz.config is not None, wiz.derived.value
        assert wiz.config.carbonate_sensitivity is None

    def test_gather_omits_attached_files_outside_cdr_lite(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(d))

        wiz.bgc_dd.value = "none"  # the resolver would reject the files here
        assert "carbonate_sensitivity" not in wiz._gather()

    def test_gather_fails_fast_for_the_path_source_with_nothing_attached(self):
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        with pytest.raises(ValueError, match="attach a directory"):
            wiz._gather()
        assert wiz.config is None
        assert "attach a directory" in wiz.derived.value

    def test_round_trips_through_populate_from(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(d))
        assert wiz.config is not None, wiz.derived.value

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)

        assert wiz2.bgc_dd.value == "cdr_lite"
        assert wiz2.cs_source_dd.value == "path"
        assert wiz2.cs_path.value == str(d.resolve())  # the files' common parent
        assert wiz2._carbonate_sensitivity == wiz._carbonate_sensitivity
        assert "attached 2 carbonate sensitivity file(s)" in wiz2.cs_status.value
        assert _display(wiz2.cs_group) == ""
        assert wiz2.config is not None, wiz2.derived.value
        assert wiz2.config.content_hash() == wiz.config.content_hash()

    def test_load_back_trusts_the_stored_hashes_and_warns_on_missing_files(
        self, tmp_path
    ):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.cs_source_dd.value = "path"
        wiz._attach_carbonate_sensitivity_dir(str(d))
        cfg = wiz.config
        stored = dict(wiz._carbonate_sensitivity)
        for f in d.glob("*.nc"):
            f.unlink()

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert wiz2._carbonate_sensitivity == stored  # hashes kept, not recomputed
        assert "2 file(s) not found" in wiz2.cs_status.value
        assert wiz2.config is not None

    def test_load_back_of_a_directive_blueprint_selects_the_directive(self):
        wiz = _cdr_lite_wizard()
        assert wiz.config is not None, wiz.derived.value
        assert wiz.config.carbonate_sensitivity is None

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)

        assert wiz2.bgc_dd.value == "cdr_lite"
        assert wiz2.cs_source_dd.value == "directive"
        assert wiz2._carbonate_sensitivity is None
        assert wiz2.cs_path.value == ""
        assert _display(wiz2.cs_group) == ""
        assert _display(wiz2.cs_path_box) == "none"
        assert wiz2.config is not None, wiz2.derived.value
        assert wiz2.config.content_hash() == wiz.config.content_hash()

    def test_load_back_of_a_directive_blueprint_resets_an_earlier_attach(
        self, tmp_path
    ):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz2 = _cdr_lite_wizard()
        wiz2.cs_source_dd.value = "path"
        wiz2._attach_carbonate_sensitivity_dir(str(d))

        wiz = _cdr_lite_wizard()
        wiz2._populate_from(wiz.config)

        assert wiz2.cs_source_dd.value == "directive"
        assert wiz2._carbonate_sensitivity is None
        assert wiz2.cs_status.value == ""

    @pytest.mark.parametrize("mode", ["marbl", "none"])
    def test_load_back_selects_the_other_bgc_modes_from_cppdefs(self, mode):
        wiz = ForgeBlueprintWizard()
        wiz.start.value = date(2012, 1, 1)
        wiz.end.value = date(2012, 1, 2)
        if mode == "none":
            _strip_bgc_forcing(wiz)
            wiz.bgc_dd.value = "none"
        assert wiz.config is not None, wiz.derived.value

        wiz2 = ForgeBlueprintWizard()
        wiz2.bgc_dd.value = "cdr_lite"  # a stale selection must be replaced
        wiz2._populate_from(wiz.config)

        assert wiz2.bgc_dd.value == mode
        assert _display(wiz2.cs_group) == "none"


class TestCdrLiteSavedModel:
    def test_saved_model_spec_round_trips_with_cdr_lite(self, tmp_path):
        import shutil

        from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog

        root = tmp_path / "catalog"
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        catalog = DomainCatalog(catalog_root=root)
        wiz = _cdr_lite_wizard(catalog)
        assert wiz.config is not None, wiz.derived.value
        assert wiz.config.model_settings["param"]["ntrc_bio"] == 0

        wiz.save_model_name.value = "my-cdr-lite-model"
        wiz._on_save_model(None)

        data = catalog.model_data("my-cdr-lite-model")
        assert data["bgc_mode"] == "cdr_lite"
        # The resolver zeroes ntrc_bio for a run without MARBL; the saved spec
        # keeps the base model's count so switching back to "marbl" still works.
        assert data["model_settings"]["param"]["ntrc_bio"] == 32
        # The wizard's hold on the time interpolation is not the model's: the spec
        # keeps the base value, so it still validates when bgc_mode is switched.
        assert data["model_settings"]["cdr_lite"][_INTERP_KEY[1]] is True
        assert "✓" in wiz.save_model_status.value
        assert "round-trip" not in wiz.save_model_status.value
        assert wiz.model_dd.value == "my-cdr-lite-model"
        # The run keeps the hold, which deviates from the saved spec exactly as it
        # does from the bundled one, so the status must not claim otherwise.
        assert wiz.config.model_settings["cdr_lite"][_INTERP_KEY[1]] is False
        assert wiz.editor._widgets[_INTERP_KEY][0].value is False
        assert wiz.config.composition.model.modified is True
        assert "unmodified" not in wiz.save_model_status.value

        wiz2 = ForgeBlueprintWizard(catalog)
        wiz2.start.value = date(2012, 1, 1)
        wiz2.end.value = date(2012, 1, 2)
        wiz2.use_pio_chk.value = False
        wiz2.model_dd.value = "my-cdr-lite-model"
        assert wiz2.bgc_dd.value == "cdr_lite"
        assert _display(wiz2.cs_group) == ""
        wiz2.bgc_dd.value = "marbl"
        assert wiz2.config is not None, wiz2.derived.value

    def test_a_hold_the_user_set_stays_out_of_the_saved_spec(self, tmp_path):
        """A hold the user chose (here toggled off and on again, so the wizard no
        longer owns it) is saved as the base value too, and the session keeps it.
        """
        import shutil

        from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog

        root = tmp_path / "catalog"
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        catalog = DomainCatalog(catalog_root=root)
        wiz = _cdr_lite_wizard(catalog)
        checkbox = wiz.editor._widgets[_INTERP_KEY][0]
        checkbox.value = True
        checkbox.value = False
        assert _INTERP_KEY not in wiz._wizard_default_keys

        wiz.save_model_name.value = "my-cdr-lite-model"
        wiz._on_save_model(None)

        data = catalog.model_data("my-cdr-lite-model")
        assert data["model_settings"]["cdr_lite"][_INTERP_KEY[1]] is True
        assert "round-trip" not in wiz.save_model_status.value
        assert wiz.config.model_settings["cdr_lite"][_INTERP_KEY[1]] is False
        assert wiz.editor._widgets[_INTERP_KEY][0].value is False


class TestCdrLiteCdrPanel:
    def test_hint_lives_in_the_run_card_and_shows_only_for_cdr_lite(self):
        wiz = ForgeBlueprintWizard()
        run_card = _find_card(wiz.widget, "run")
        assert wiz.cdr_lite_hint in _descendants(run_card)

        assert _display(wiz.cdr_lite_hint) == "none"
        wiz.bgc_dd.value = "cdr_lite"
        assert _display(wiz.cdr_lite_hint) == ""
        assert "tracer_set: cdr_lite" in wiz.cdr_lite_hint.value
        assert "required" in wiz.cdr_lite_hint.value
        wiz.bgc_dd.value = "none"
        assert _display(wiz.cdr_lite_hint) == "none"

    def test_netcdf_without_cdr_tracers_warns_in_cdr_lite_mode(self, tmp_path):
        p = _write_cdr_netcdf_with_tracers(tmp_path / "cdr.nc", ["temp", "salt", "ALK"])
        wiz = _new_wizard()
        wiz.bgc_dd.value = "cdr_lite"
        wiz.cdr_mode_dd.value = "netcdf"
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)

        assert wiz._cdr_forcing_file is not None  # a warning, not a block
        assert "no CDR_* tracers" in wiz.cdr_file_status.value

    def test_netcdf_with_cdr_tracers_does_not_warn(self, tmp_path):
        p = _write_cdr_netcdf_with_tracers(
            tmp_path / "cdr.nc", ["temp", "salt", "CDR_OAE_ALK1", "CDR_OAE_DIC1"]
        )
        wiz = _new_wizard()
        wiz.bgc_dd.value = "cdr_lite"
        wiz.cdr_mode_dd.value = "netcdf"
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)

        assert "no CDR_* tracers" not in wiz.cdr_file_status.value

    def test_netcdf_without_cdr_tracers_does_not_warn_outside_cdr_lite(self, tmp_path):
        p = _write_cdr_netcdf_with_tracers(tmp_path / "cdr.nc", ["temp", "salt", "ALK"])
        wiz = _new_wizard()
        wiz.cdr_mode_dd.value = "netcdf"
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)

        assert "no CDR_* tracers" not in wiz.cdr_file_status.value


# ===========================================================================
# Card 8: Next steps (saved-blueprint gate, workplan save, run pane)
# ===========================================================================


def _saved_wizard(tmp_path, *, hooks=None):
    """A valid wizard over a throwaway catalog, saved to ``tmp_path/bp.yaml``."""
    app = ForgeBlueprintWizardApp(str(tmp_path / "catalog"))
    wiz = app.inner
    assert wiz is not None
    assert Path(wiz.catalog.workplans_dir).is_relative_to(tmp_path)
    if hooks is not None:
        app.set_workplan_hooks(hooks)
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True
    return wiz


def _next_buttons(wiz):
    return (wiz.save_workplan_btn, wiz.open_workplan_btn, wiz.run_btn)


def test_saved_blueprint_survives_a_rebuild_and_not_an_edit(tmp_path):
    wiz = _saved_wizard(tmp_path)
    assert wiz._saved_path() is None
    wiz._on_save(None)
    assert wiz._saved_path() == (tmp_path / "bp.yaml").resolve()
    wiz._rebuild()
    assert wiz._saved_path() == (tmp_path / "bp.yaml").resolve()
    wiz.end.value = date(2012, 1, 3)
    assert wiz._saved_path() is None


def test_saved_blueprint_accepts_a_save_path_written_unnormalized(tmp_path):
    """A Save path that ``Path`` rewrites (a doubled slash) still counts as saved."""
    wiz = _saved_wizard(tmp_path)
    wiz.save_path.value = f"{tmp_path}//bp.yaml"
    wiz._on_save(None)
    assert wiz._saved_path() == (tmp_path / "bp.yaml").resolve()
    assert not wiz.save_workplan_btn.disabled


def test_card_seven_ends_at_save_and_card_eight_holds_the_rest():
    wiz = ForgeBlueprintWizard()
    root = wiz.widget
    review = {id(w) for w in _descendants(_find_card(root, "review"))}
    nxt = {id(w) for w in _descendants(_find_card(root, "next"))}
    for w in (wiz.run_btn, wiz.save_specs_accordion, wiz.run_forge_pane):
        assert id(w) not in review
        assert id(w) in nxt
    for w in (wiz.next_status, wiz.save_workplan_btn, wiz.open_workplan_btn):
        assert id(w) not in review
        assert id(w) in nxt
    texts = " ".join(
        getattr(w, "value", "")
        for w in _descendants(_find_card(root, "review"))
        if isinstance(getattr(w, "value", None), str)
    )
    assert "Workplan page" not in texts
    assert "Optional" in _find_card(root, "next").forge_header.value


def test_card_eight_panes_start_closed_with_their_summaries():
    wiz = ForgeBlueprintWizard()
    _ = wiz.widget
    for pane in (wiz.run_forge_pane, wiz.save_specs_accordion):
        assert pane.selected_index is None
    assert "inputs and UCLA-ROMS blueprint only" in wiz.run_forge_pane.get_title()
    assert "reuse this model" in wiz.save_specs_accordion.get_title()
    sub = _find_section(wiz.widget, "Run it as a workplan")
    assert sub is not None
    assert not hasattr(sub, "selected_index")


def test_next_steps_follow_the_saved_state(tmp_path):
    wiz = _saved_wizard(tmp_path)
    assert all(b.disabled for b in _next_buttons(wiz))
    assert "Save the blueprint above first" in wiz.next_status.value
    wiz._on_save(None)
    assert not any(b.disabled for b in _next_buttons(wiz))
    assert str((tmp_path / "bp.yaml").resolve()) in wiz.next_status.value
    wiz._rebuild()
    assert not any(b.disabled for b in _next_buttons(wiz))
    wiz.end.value = date(2012, 1, 3)
    assert all(b.disabled for b in _next_buttons(wiz))
    assert "has changed" in wiz.next_status.value
    wiz.end.value = date(2012, 1, 2)
    assert not any(b.disabled for b in _next_buttons(wiz))
    wiz.save_path.value = str(tmp_path / "other.yaml")
    assert all(b.disabled for b in _next_buttons(wiz))
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz.start.value = None
    wiz._rebuild()
    assert wiz.config is None
    assert all(b.disabled for b in _next_buttons(wiz))


def test_a_run_in_progress_keeps_run_disabled_through_a_rebuild(tmp_path):
    wiz = _saved_wizard(tmp_path)
    wiz._on_save(None)
    wiz._run_in_progress = True
    wiz._rebuild()
    assert wiz.run_btn.disabled
    wiz._run_in_progress = False
    wiz._rebuild()
    assert not wiz.run_btn.disabled


def _load_workplan(path):
    from cstar.orchestration.models import Workplan

    raw = yaml.safe_load(path.read_text())
    raw.pop("$schema", None)
    return Workplan.model_validate(raw)


def test_save_workplan_writes_the_forge_then_run_workplan(tmp_path):
    from cstar.base.utils import slugify
    from cstar.orchestration.models import DeferredBlueprintRef

    calls = []
    wiz = _saved_wizard(
        tmp_path,
        hooks=WorkplanHooks(
            open_steps=lambda p, n: None, saved=lambda: calls.append(1)
        ),
    )
    wiz._on_save(None)
    wiz._on_save_workplan(None)
    assert wiz.config is not None
    slug = slugify(wiz.config.name)
    target = Path(wiz.catalog.workplans_dir) / f"{slug}.yaml"
    assert target.exists(), wiz.workplan_status.value
    plan = _load_workplan(target)
    assert plan.name == slug
    forge, run = plan.steps
    assert [forge.name, run.name] == ["forge", "ucla_roms"]
    assert forge.application == "forge"
    assert Path(str(forge.blueprint_path)) == (tmp_path / "bp.yaml").resolve()
    assert not forge.compute_overrides
    assert run.depends_on == ["forge"]
    assert isinstance(run.blueprint_path, DeferredBlueprintRef)
    assert run.blueprint_path.from_step == "forge"
    assert run.compute_overrides["slurm"]["num_cpus"] == wiz.config.n_procs
    assert "download=" in wiz.workplan_download.value
    assert calls == [1]
    assert "cstar workplan submit" in wiz.workplan_status.value


def test_save_workplan_asks_before_replacing_a_different_file(tmp_path):
    wiz = _saved_wizard(tmp_path)
    wiz._on_save(None)
    wiz._on_save_workplan(None)
    target = next(Path(wiz.catalog.workplans_dir).glob("*.yaml"))
    good = target.read_text()
    target.write_text("different: true\n")
    wiz._on_save_workplan(None)
    assert wiz.save_workplan_btn.description == "Confirm overwrite"
    assert wiz.save_workplan_btn.button_style == "danger"
    assert target.read_text() == "different: true\n"
    wiz._on_save_workplan(None)
    assert target.read_text() == good
    assert wiz.save_workplan_btn.description == "Save workplan"
    assert wiz.save_workplan_btn.button_style == "primary"
    # identical text: no confirm
    wiz._on_save_workplan(None)
    assert wiz.save_workplan_btn.description == "Save workplan"
    # an edit disarms
    target.write_text("different: true\n")
    wiz._on_save_workplan(None)
    assert wiz._workplan_confirm == target
    wiz.end.value = date(2012, 1, 3)
    assert wiz._workplan_confirm is None
    assert wiz.save_workplan_btn.description == "Save workplan"


def test_save_workplan_without_a_current_save_is_refused(tmp_path):
    wiz = _saved_wizard(tmp_path)
    wiz._on_save_workplan(None)
    assert "Save the blueprint above first" in wiz.workplan_status.value
    assert not list(Path(wiz.catalog.workplans_dir).glob("*.yaml"))
    wiz._on_save(None)
    (tmp_path / "bp.yaml").unlink()
    wiz._on_save_workplan(None)
    assert "gone" in wiz.workplan_status.value


def test_open_in_builder_calls_the_hook(tmp_path):
    seen = []

    def open_steps(path, name):
        seen.append((path, name))
        if len(seen) > 1:
            raise ValueError("no good")

    wiz = _saved_wizard(
        tmp_path, hooks=WorkplanHooks(open_steps=open_steps, saved=lambda: None)
    )
    wiz._on_save(None)
    wiz._on_open_workplan(None)
    assert seen == [((tmp_path / "bp.yaml").resolve(), wiz.config.name)]
    assert "Opened the Workplan page" in wiz.workplan_status.value
    wiz._on_open_workplan(None)
    assert "no good" in wiz.workplan_status.value


def test_run_log_lands_while_the_pane_is_collapsed(monkeypatch, tmp_path):
    import asyncio

    wiz = _saved_wizard(tmp_path)
    _ = wiz.widget
    wiz.run_forge_pane.selected_index = None

    async def _fake(*args, **kwargs):
        return _FakeProcess([b"line one\n"], returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake)
    wiz._on_run(None)
    assert "line one" in "".join(o["text"] for o in wiz.run_output.outputs)


class TestChildNestingMetadata:
    """A child spec's ``metadata`` block (a DomainSpec's description of itself as
    a nesting child) feeds the nesting widgets; it is never kept as a second copy
    in ``grid_kwargs_child``.
    """

    _BLOCK: ClassVar[dict] = {"period": 1800.0, "include_pressure_fluxes": True}
    _CHILD_GRID: ClassVar[dict] = {
        "nx": 30,
        "ny": 40,
        "size_x": 300.0,
        "size_y": 400.0,
    }

    def _pick_child(self, wiz, monkeypatch, spec):
        monkeypatch.setattr(wiz.catalog, "domain_data", lambda _name: dict(spec))
        monkeypatch.setattr(wiz, "_on_nest_plot", lambda _b: None)
        wiz.nest_domain_dd.value = wiz.nest_domain_dd.options[1]

    def _child_spec(self, block):
        return {"grid_kwargs": {**self._CHILD_GRID, "metadata": dict(block)}}

    def test_bundled_child_preset_leaves_no_second_copy(self, monkeypatch):
        wiz = _new_wizard()
        spec = yaml.safe_load(
            (
                Path(cstar.__file__).parent
                / "catalog/bundled/specs/DomainSpec/NEP-4th_deg.yaml"
            ).read_text()
        )
        self._pick_child(wiz, monkeypatch, spec)
        wiz.nest_period.value = 1800.0
        wiz._rebuild()
        cfg = wiz.config
        assert cfg is not None, wiz.derived.value

        assert "metadata" not in cfg.domain.grid_kwargs_child
        assert cfg.domain.metadata_child == {"period": 1800.0}
        assert cfg.model_settings["extract_data"]["extract_period"] == 1800.0
        assert "metadata" not in wiz._domain_spec_data()["grid_kwargs_child"]

        plain = _new_wizard()
        plain.nest_enable.value = True
        for k, w in wiz.child_w.items():
            plain.child_w[k].value = w.value
        plain.child_topo_source.value = wiz.child_topo_source.value
        plain.nest_period.value = 1800.0
        plain._rebuild()
        assert plain.config is not None, plain.derived.value
        assert cfg.content_hash() == plain.config.content_hash()

    def test_child_preset_period_and_pressure_fluxes_fill_the_widgets(
        self, monkeypatch
    ):
        wiz = _new_wizard()
        assert wiz.nest_period.value == 3600.0
        self._pick_child(wiz, monkeypatch, self._child_spec(self._BLOCK))

        assert wiz.nest_period.value == 1800.0
        assert wiz.nest_pressure_fluxes.value is True
        wiz._rebuild()
        assert wiz.config is not None, wiz.derived.value
        assert wiz.config.model_settings["extract_data"]["extract_period"] == 1800.0

    def test_child_preset_without_a_period_leaves_nest_period(self, monkeypatch):
        wiz = _new_wizard()
        wiz.nest_period.value = 900.0
        self._pick_child(wiz, monkeypatch, self._child_spec({"include_bgc": True}))
        assert wiz.nest_period.value == 900.0

    def test_refused_child_block_reports_and_changes_nothing(self, monkeypatch):
        wiz = _new_wizard()
        wiz.nest_period.value = 900.0
        self._pick_child(wiz, monkeypatch, self._child_spec({"prefix": "a_b"}))

        assert "Invalid child domain" in wiz.derived.value
        assert wiz.nest_period.value == 900.0
        assert wiz.nest_enable.value is False

    def test_domain_spec_with_block_in_grid_kwargs_child_loads_its_period(
        self, monkeypatch
    ):
        wiz = _new_wizard()
        spec = {
            "grid_kwargs": {"nx": 20, "ny": 20, "size_x": 200.0, "size_y": 200.0},
            "grid_kwargs_child": {**self._CHILD_GRID, "metadata": {"period": 1800.0}},
        }
        monkeypatch.setattr(wiz.catalog, "domain_data", lambda _name: dict(spec))
        wiz.domain_dd.value = ForgeBlueprintWizard._dd_values(wiz.domain_dd)[1]

        assert wiz.nest_enable.value is True
        assert wiz.nest_period.value == 1800.0
        assert "metadata" not in wiz._loaded_grid_kwargs["child"]

    def test_domain_spec_with_a_refused_block_changes_nothing(self, monkeypatch):
        wiz = _new_wizard()
        wiz.nest_period.value = 900.0
        spec = {
            "grid_kwargs": {"nx": 20, "ny": 20, "size_x": 200.0, "size_y": 200.0},
            "grid_kwargs_child": {**self._CHILD_GRID, "metadata": {"prefix": "a_b"}},
        }
        monkeypatch.setattr(wiz.catalog, "domain_data", lambda _name: dict(spec))
        wiz.domain_dd.value = ForgeBlueprintWizard._dd_values(wiz.domain_dd)[1]

        assert "Invalid child domain" in wiz.derived.value
        assert wiz.nest_period.value == 900.0
        assert wiz.nest_enable.value is False
