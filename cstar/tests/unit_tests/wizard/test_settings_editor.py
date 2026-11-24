"""Tests for the settings editor and its placement tables (cstar.wizard.settings_editor).

Each builds a ``_SettingsEditor`` without a wizard, or calls a placement predicate,
the namelist tooltips or ``_applies_in`` directly.
"""

import pytest

from cstar.applications.forge.namelist_model import (
    RunTimeSettings,
    RunTimeSettingsV0_4_0,
    RunTimeSettingsV0_5_0,
    RunTimeSettingsV0_6_0,
    RunTimeSettingsV0_7_0,
    RunTimeSettingsV0_9_0,
    RunTimeSettingsV0_11_0,
    RunTimeSettingsV0_12_0,
    RunTimeSettingsV0_13_0,
    RunTimeSettingsV0_14_0,
    RunTimeSettingsV0_15_0,
    RunTimeSettingsV0_16_0,
)
from cstar.tests.unit_tests.wizard.helpers import (
    _INTERP_KEY,
    _LITE_STREAM,
    _built_fields,
    _descendants,
    _display,
    _is_in_tree,
)
from cstar.wizard.settings_editor import (
    _FORCING_CARD,
    _MODEL_CARD,
    _OUTPUT_CARD,
    _OUTPUT_MORE_TITLE,
    _applies_in,
    _in_forcing_card,
    _in_output_card,
    _namelist_tooltip,
    _output_placement,
    _SettingsEditor,
)


def test_param_pane_shows_only_nt_passive():
    """``param`` sits in the CDR pane for ``nt_passive`` alone: the grid/tiling
    dims have their own widgets, ``ntrc_bio`` is set from the BGC mode and the
    CDR tracer counts are read off the CDR forcing during generation.
    """
    import ipywidgets as W

    param = {
        "llm": 10,
        "mmm": 10,
        "n": 20,
        "np_xi": 2,
        "np_eta": 2,
        "nt_passive": 0,
        "ntrc_bio": 32,
        "nt_cdr_oae": 0,
        "nt_cdr_dor": 0,
    }
    editor = _SettingsEditor(W, {"param": param}, settings_cls=RunTimeSettingsV0_9_0)
    assert [key for (section, key) in editor._widgets if section == "param"] == [
        "nt_passive"
    ]
    assert "param" in editor._pane_sections["Carbon dioxide removal (CDR)"]
    assert "forcing file" in editor._widgets[("param", "nt_passive")][0].tooltip


def test_settings_editor_nrpf_rst_only_for_legacy_settings_cls():
    """``ocean_vars.nrpf_rst`` was removed from ucla-roms 0.5.0 -- the editor must
    generate a widget for it when introspecting the legacy ``RunTimeSettings``
    class, and must NOT when introspecting ``RunTimeSettingsV0_5_0``, even
    though the raw ``model_settings`` dict carries the key either way (e.g. a
    stale value passed through from a hand-edited/legacy-authored blueprint).
    """
    import ipywidgets as W

    model_settings = {"ocean_vars": {"nrpf_rst": 2, "wrt_file_rst": True}}

    legacy_editor = _SettingsEditor(W, model_settings, settings_cls=RunTimeSettings)
    assert ("ocean_vars", "nrpf_rst") in legacy_editor._widgets

    v0_5_0_editor = _SettingsEditor(
        W, model_settings, settings_cls=RunTimeSettingsV0_5_0
    )
    assert ("ocean_vars", "nrpf_rst") not in v0_5_0_editor._widgets
    # wrt_file_rst has no version-varying field set, so it's unaffected either way.
    assert ("ocean_vars", "wrt_file_rst") in v0_5_0_editor._widgets

    # Constructor default (no settings_cls passed) stays legacy, for back-compat.
    default_editor = _SettingsEditor(W, model_settings)
    assert ("ocean_vars", "nrpf_rst") in default_editor._widgets


def test_settings_editor_skips_version_gated_section_not_in_active_schema():
    """``pio_settings`` is version-gated: only ``RunTimeSettingsV0_6_0`` models
    it. A ``model_settings`` dict can still carry the key under an older
    ``settings_cls`` -- e.g. the ``pio-dev``/``roms-marbl-0.6-default``
    ModelSpecs always include it, but a user can override ``roms_ref`` down to
    "0.5.0" in the wizard while keeping that model selected. Regression: the
    editor used to fall back to type-inference (like it does for a *never*
    schema-modeled section, e.g. ``cppdefs``) and render an editable widget
    anyway; ``RunTimeSettings``/``RunTimeSettingsV0_5_0`` are ``extra="ignore"``
    at the top level, so any edit made there was silently discarded downstream
    instead of taking effect or raising. No widget must be built for
    ``pio_settings`` under a settings_cls that doesn't model it; a widget MUST
    still be built once the settings_cls does. ``cppdefs`` (never modeled by
    any tier) must keep rendering under both, guarding the distinction
    ``version_gated_section_names()`` draws between "version-gated" and
    "never schema-modeled".
    """
    import ipywidgets as W

    model_settings = {
        "pio_settings": {"pio_stride": 4},
        "cppdefs": {"sponge_tune": True},
    }

    v0_5_0_editor = _SettingsEditor(
        W, model_settings, settings_cls=RunTimeSettingsV0_5_0
    )
    assert ("pio_settings", "pio_stride") not in v0_5_0_editor._widgets
    assert "pio_settings" not in v0_5_0_editor._pane_sections.get(
        "Physics & subgrid tuning", []
    )
    assert ("cppdefs", "sponge_tune") in v0_5_0_editor._widgets

    v0_6_0_editor = _SettingsEditor(
        W, model_settings, settings_cls=RunTimeSettingsV0_6_0
    )
    assert ("pio_settings", "pio_stride") in v0_6_0_editor._widgets
    assert "pio_settings" in v0_6_0_editor._pane_sections.get(
        "Physics & subgrid tuning", []
    )
    assert ("cppdefs", "sponge_tune") in v0_6_0_editor._widgets

    # A section with NOTHING else built for its pane under an older settings_cls
    # must not leave a broken empty accordion pane (mirrors the "if not blocks:
    # continue" guard in _SettingsEditor.__init__).
    pio_only_editor = _SettingsEditor(
        W, {"pio_settings": {"pio_stride": 4}}, settings_cls=RunTimeSettingsV0_5_0
    )
    assert "Physics & subgrid tuning" not in pio_only_editor._pane_sections
    assert pio_only_editor._widgets == {}


def test_settings_editor_optional_pio_stride_is_a_blank_able_text_box():
    """``pio_stride`` is ``int | None`` (None: one I/O task per node at launch), so
    it gets a text box that shows ``None`` as blank and reads blank back as ``None``.
    """
    import ipywidgets as W

    editor = _SettingsEditor(
        W, {"pio_settings": {"pio_stride": None}}, settings_cls=RunTimeSettingsV0_6_0
    )
    widget, _base = editor._widgets[("pio_settings", "pio_stride")]
    assert isinstance(widget, W.Text)
    assert widget.value == ""
    assert editor.read("pio_settings", "pio_stride") is None

    widget.value = "  "
    assert editor.read("pio_settings", "pio_stride") is None
    widget.value = "128"
    assert editor.read("pio_settings", "pio_stride") == 128
    assert isinstance(editor.read("pio_settings", "pio_stride"), int)
    widget.value = "abc"
    assert editor.read("pio_settings", "pio_stride") == "abc"

    editor.sync({"pio_settings": {"pio_stride": 4}})
    assert widget.value == "4"
    editor.sync({"pio_settings": {"pio_stride": None}})
    assert widget.value == ""


def test_settings_editor_sync_preserves_in_progress_list_text():
    """Typing a comma into a list field (e.g. ``marbl_bgc.marbl_tracers_to_write``)
    must not be reverted. Every keystroke runs on_edit -> wizard._rebuild ->
    editor.sync(effective); sync used to unconditionally re-join the parsed
    list, so "a, b," was rewritten to "a, b" and a trailing comma could never
    be typed (the reported workaround was pasting a finished list). sync() now
    leaves the text alone when it already parses to the synced value, and
    still re-renders when the value genuinely differs.
    """
    import ipywidgets as W

    model_settings = {"marbl_bgc": {"marbl_tracers_to_write": ["ALK", "DIC"]}}
    edits: list[tuple[str, str]] = []
    editor = _SettingsEditor(
        W, model_settings, on_edit=lambda s, f: edits.append((s, f))
    )
    widget, base = editor._widgets[("marbl_bgc", "marbl_tracers_to_write")]
    assert base is list
    assert widget.value == "ALK, DIC"

    # Simulate the wizard's per-keystroke loop: the user types a trailing comma,
    # on_edit fires, the wizard records editor.read() and syncs it back.
    widget.value = "ALK, DIC,"
    assert edits == [("marbl_bgc", "marbl_tracers_to_write")]
    effective = editor.read("marbl_bgc", "marbl_tracers_to_write")
    assert effective == ["ALK", "DIC"]
    editor.sync({"marbl_bgc": {"marbl_tracers_to_write": effective}})
    assert widget.value == "ALK, DIC,"  # trailing comma survives

    widget.value = "ALK, DIC, "  # ...and so does a trailing separator + space
    editor.sync({"marbl_bgc": {"marbl_tracers_to_write": ["ALK", "DIC"]}})
    assert widget.value == "ALK, DIC, "

    # A genuine external change (model switch / load / override reset) still
    # re-renders the field.
    editor.sync({"marbl_bgc": {"marbl_tracers_to_write": ["PO4"]}})
    assert widget.value == "PO4"


@pytest.mark.parametrize(
    "settings_cls",
    [
        RunTimeSettings,
        RunTimeSettingsV0_4_0,
        RunTimeSettingsV0_5_0,
        RunTimeSettingsV0_6_0,
        RunTimeSettingsV0_7_0,
        RunTimeSettingsV0_9_0,
        RunTimeSettingsV0_11_0,
        RunTimeSettingsV0_12_0,
        RunTimeSettingsV0_13_0,
        RunTimeSettingsV0_14_0,
        RunTimeSettingsV0_15_0,
        RunTimeSettingsV0_16_0,
    ],
)
def test_namelist_tooltip_for_every_settings_field(settings_cls):
    """Every Advanced-settings field gets its namelist schema description, in
    every tier. Regression: the lookup used the forge section/field names as
    namelist group/key names, so only the few sections whose names coincide
    (``time_stepping``, ``s_coord``) had tooltips -- ``param`` (group
    ``param_settings``, ``n`` -> ``nz``) and nearly every other section had none.
    """
    for section, info in settings_cls.model_fields.items():
        cfg_fields = getattr(info.annotation, "model_fields", None)
        for field in cfg_fields or (section,):
            assert _namelist_tooltip(settings_cls, section, field), (section, field)


@pytest.mark.parametrize("field", ["np_xi", "n", "nt_cdr_oae"])
def test_namelist_tooltip_for_param_fields(field):
    assert _namelist_tooltip(RunTimeSettingsV0_9_0, "param", field)


def test_namelist_tooltip_empty_for_non_namelist_sections():
    assert _namelist_tooltip(RunTimeSettingsV0_9_0, "cppdefs", "marbl") == ""


def test_settings_editor_tooltips_prefer_the_glossary_hint():
    """A field with no glossary hint shows the schema description; a glossary
    hint (curated in the wizard's vocabulary) replaces it.
    """
    import ipywidgets as W

    model_settings = {"bottom_drag": {"rdrg2": 1e-3}, "bgc": {"xco2air_default": 280.0}}
    editor = _SettingsEditor(W, model_settings, settings_cls=RunTimeSettingsV0_9_0)

    rdrg2_tip = editor._widgets[("bottom_drag", "rdrg2")][0].tooltip
    assert _namelist_tooltip(RunTimeSettingsV0_9_0, "bottom_drag", "rdrg2") in (
        rdrg2_tip
    )
    from cstar.wizard.ui.labels import label_for

    xco2air_tip = editor._widgets[("bgc", "xco2air_default")][0].tooltip
    assert label_for("settings.bgc.xco2air_default").hint in xco2air_tip
    assert "PCO2AIR_FORCING" not in xco2air_tip


@pytest.mark.parametrize(
    "section,master_flag",
    [
        ("cdr_lite_output", "do_cdr_lite_output"),
        ("cdr_gas_exch_output", "do_cdr_gas_exch_output"),
    ],
)
def test_settings_editor_skips_cdr_output_streams_not_in_active_schema(
    section, master_flag
):
    """``cdr_lite_output``/``cdr_gas_exch_output`` (ucla-roms PR #351, >=
    0.7.0) are version-gated exactly like ``pio_settings`` (see
    ``test_settings_editor_skips_version_gated_section_not_in_active_schema``
    above), one schema tier later: only ``RunTimeSettingsV0_7_0`` models them.
    A ``model_settings`` dict can still carry the key under an older
    ``settings_cls`` (e.g. a bundled OutputSpec's defaults survive a
    ``roms_ref`` override down to "0.6.0" with the same ModelSpec selected),
    so the editor must build no widget for it under
    ``RunTimeSettingsV0_6_0`` and build one once the effective schema reaches
    ``RunTimeSettingsV0_7_0`` (or ``RunTimeSettingsV0_9_0``).
    """
    import ipywidgets as W

    model_settings = {section: {master_flag: False, "do_avg": True}}

    v0_6_0_editor = _SettingsEditor(
        W, model_settings, settings_cls=RunTimeSettingsV0_6_0
    )
    assert (section, master_flag) not in v0_6_0_editor._widgets
    assert section not in v0_6_0_editor._group_fields.get(_OUTPUT_CARD, {})

    v0_7_0_editor = _SettingsEditor(
        W, model_settings, settings_cls=RunTimeSettingsV0_7_0
    )
    assert (section, master_flag) in v0_7_0_editor._widgets
    assert section in v0_7_0_editor._group_fields[_OUTPUT_CARD]
    assert section not in v0_7_0_editor._pane_sections.get(
        "Carbon dioxide removal (CDR)", []
    )

    v0_9_0_editor = _SettingsEditor(
        W, model_settings, settings_cls=RunTimeSettingsV0_9_0
    )
    assert (section, master_flag) in v0_9_0_editor._widgets


def test_settings_editor_skips_cdr_lite_section_before_0_9_0():
    """The ``cdr_lite`` knob section only exists on ``RunTimeSettingsV0_9_0``:
    older tiers build no widget for it, 0.9 builds one in the CDR pane.
    """
    import ipywidgets as W

    model_settings = {"cdr_lite": {"cdr_online_carbonate_sensitivity": False}}
    key = ("cdr_lite", "cdr_online_carbonate_sensitivity")

    old = _SettingsEditor(W, model_settings, settings_cls=RunTimeSettingsV0_7_0)
    assert key not in old._widgets
    new = _SettingsEditor(W, model_settings, settings_cls=RunTimeSettingsV0_9_0)
    assert key in new._widgets
    assert "cdr_lite" in new._pane_sections["Carbon dioxide removal (CDR)"]


# One setting per kind of key the BGC-mode table names, plus two that apply in
# every mode.
_MARBL_ONLY_KEYS = [
    ("bgc", "wrt_his"),
    ("marbl_bgc", "marbl_config_file"),
    ("dic_alk_correction", "dcdt"),
    ("cdr_output", "do_cdr_output"),
    ("cdr_gas_exch_output", "do_cdr_gas_exch_output"),
    ("upscale_output", "do_upscale"),
    ("cppdefs", "nhy_forcing"),
    ("cppdefs", "nox_forcing"),
    ("cdr_lite", "cdr_online_carbonate_sensitivity"),
]


_EVERY_MODE_KEYS = [("ocean_vars", "wrt_file_his"), ("cppdefs", "sponge_tune")]


_BGC_MODE_SETTINGS_SAMPLE = {
    "bgc": {"wrt_his": True},
    "marbl_bgc": {"marbl_config_file": "marbl_in"},
    "dic_alk_correction": {"dcdt": 0.0},
    "cdr_output": {"do_cdr_output": False},
    "cdr_gas_exch_output": {"do_cdr_gas_exch_output": False},
    "upscale_output": {"do_upscale": False},
    "cdr_lite_output": {
        "do_cdr_lite_output": False,
        "output_period": 3600.0,
        "wrt_alk": True,
    },
    "cdr_lite": {"cdr_online_carbonate_sensitivity": False},
    "cppdefs": {"nhy_forcing": True, "nox_forcing": True, "sponge_tune": False},
    "ocean_vars": {"wrt_file_his": True},
}


@pytest.mark.parametrize(
    ("section", "field", "bgc_mode", "expected"),
    [
        ("bgc", "wrt_his", "marbl", True),
        ("bgc", "wrt_his", "none", False),
        ("bgc", None, "cdr_lite", False),
        ("cdr_lite_output", "do_cdr_lite_output", "marbl", True),
        ("cdr_lite_output", "do_cdr_lite_output", "cdr_lite", True),
        ("cdr_lite_output", "wrt_alk", "none", False),
        ("cppdefs", "nhy_forcing", "none", False),
        ("cppdefs", "nhy_forcing", "marbl", True),
        ("cppdefs", "sponge_tune", "none", True),
        ("cppdefs", None, "none", True),
        ("ocean_vars", "wrt_file_his", "cdr_lite", True),
        ("cdr_lite", "cdr_carbonate_sensitivity_time_interpolation", "cdr_lite", True),
        ("cdr_lite", "cdr_carbonate_sensitivity_time_interpolation", "marbl", False),
        ("cdr_lite", "cdr_carbonate_sensitivity_time_interpolation", "none", False),
    ],
)
def test_applies_in(section, field, bgc_mode, expected):
    assert _applies_in(section, field, bgc_mode) is expected


@pytest.mark.parametrize("bgc_mode", ["marbl", "none", "cdr_lite"])
def test_settings_editor_builds_only_the_settings_that_apply_in_the_bgc_mode(
    bgc_mode,
):
    import ipywidgets as W

    editor = _SettingsEditor(
        W,
        _BGC_MODE_SETTINGS_SAMPLE,
        settings_cls=RunTimeSettingsV0_9_0,
        bgc_mode=bgc_mode,
    )
    for key in _MARBL_ONLY_KEYS:
        assert (key in editor._widgets) is (bgc_mode == "marbl"), key
    assert (_LITE_STREAM in editor._widgets) is (bgc_mode != "none")
    for key in _EVERY_MODE_KEYS:
        assert key in editor._widgets, key


@pytest.mark.parametrize(
    ("settings_cls", "bgc_mode", "built"),
    [
        (RunTimeSettingsV0_12_0, "cdr_lite", True),
        (RunTimeSettingsV0_12_0, "marbl", False),
        (RunTimeSettingsV0_12_0, "none", False),
        (RunTimeSettingsV0_11_0, "cdr_lite", False),
    ],
)
def test_carbonate_sensitivity_time_interpolation_widget(settings_cls, bgc_mode, built):
    """ucla-roms >= 0.12.0 reads the switch only for the file-based carbonate
    sensitivities of ``bgc_mode: cdr_lite``, so the editor builds it there, for
    the Model card (``model_box``) rather than the CDR pane, and nowhere else; an
    older tier has no such field.
    """
    import ipywidgets as W

    model_settings = {"cdr_lite": {_INTERP_KEY[1]: True}}
    editor = _SettingsEditor(
        W, model_settings, settings_cls=settings_cls, bgc_mode=bgc_mode
    )
    assert (_INTERP_KEY in editor._widgets) is built
    if built:
        widget, _ = editor._widgets[_INTERP_KEY]
        assert widget.value is True
        assert widget.description == "Interpolate carbonate sensitivities in time"
        assert widget.tooltip.startswith(_INTERP_KEY[1])
        assert "held until the next one starts" in widget.tooltip
        assert editor._group_fields[_MODEL_CARD] == {"cdr_lite": [_INTERP_KEY[1]]}
        assert widget in _descendants(editor.model_box)
        assert widget not in _descendants(editor.accordion)
    else:
        assert not editor.model_box.children


def test_settings_editor_builds_no_pane_left_without_fields():
    """Without MARBL the Biogeochemistry pane has nothing left (its sections and
    its two cppdefs fields are all MARBL-only): no pane, no section header.
    """
    import ipywidgets as W

    def _titles(mode):
        editor = _SettingsEditor(
            W,
            _BGC_MODE_SETTINGS_SAMPLE,
            settings_cls=RunTimeSettingsV0_9_0,
            bgc_mode=mode,
        )
        return list(editor._pane_sections), _built_fields(editor)

    marbl_panes, marbl_fields = _titles("marbl")
    none_panes, none_fields = _titles("none")
    assert "Biogeochemistry (BGC / MARBL)" in marbl_panes
    assert "Biogeochemistry (BGC / MARBL)" not in none_panes
    assert "bgc" in marbl_fields
    assert not {"bgc", "marbl_bgc", "upscale_output", "cdr_output"} & set(none_fields)
    # a section that still has a field keeps its section
    assert none_fields["cppdefs"] == ["sponge_tune"]


def test_settings_editor_defaults_to_marbl():
    import ipywidgets as W

    editor = _SettingsEditor(
        W, _BGC_MODE_SETTINGS_SAMPLE, settings_cls=RunTimeSettingsV0_9_0
    )
    for key in _MARBL_ONLY_KEYS:
        assert key in editor._widgets, key


def test_cdr_lite_sections_show_glossary_titles_and_labels():
    import ipywidgets as W

    editor = _SettingsEditor(
        W,
        _BGC_MODE_SETTINGS_SAMPLE,
        settings_cls=RunTimeSettingsV0_9_0,
        bgc_mode="marbl",
    )
    headers = " ".join(
        w.value
        for container in (editor.accordion, editor.output_box)
        for w in _descendants(container)
        if isinstance(getattr(w, "value", None), str)
        and "forge-settings-sec" in w.value
    )
    for title in ("CDR-lite", "CDR-lite tracer output", "CDR gas-exchange output"):
        assert f"<span class='ttl'>{title}</span>" in headers

    alk = editor._widgets[("cdr_lite_output", "wrt_alk")][0]
    assert alk.description == "Write alkalinity tracers"
    assert alk.tooltip.startswith("wrt_alk")  # the raw name stays discoverable
    period = editor._widgets[("cdr_lite_output", "output_period")][0]
    assert period.description == "Output period (s)"
    online = editor._widgets[("cdr_lite", "cdr_online_carbonate_sensitivity")][0]
    assert online.description == "Compute carbonate sensitivities online"
    assert "Needs MARBL" in online.tooltip


def test_settings_editor_locks_the_cdr_lite_stream_on_under_cdr_lite():
    """``configure_build`` forces the CDR-lite stream on in ``cdr_lite`` mode, so
    the editor shows it checked and disabled with its dependents visible -- and
    never reports that as an edit, at construction or on a sync of the authored
    (off) value.
    """
    import ipywidgets as W

    edits: list[tuple] = []
    editor = _SettingsEditor(
        W,
        _BGC_MODE_SETTINGS_SAMPLE,
        on_edit=lambda s, f: edits.append((s, f)),
        settings_cls=RunTimeSettingsV0_9_0,
        bgc_mode="cdr_lite",
    )
    master = editor._widgets[_LITE_STREAM][0]
    period = editor._widgets[("cdr_lite_output", "output_period")][0]
    assert master.value is True
    assert master.disabled is True
    assert "bgc_mode cdr_lite" in master.tooltip
    assert _display(period) == ""

    editor.sync(_BGC_MODE_SETTINGS_SAMPLE)  # authored do_cdr_lite_output: False
    assert master.value is True
    assert _display(period) == ""
    assert edits == []
    assert editor.read(*_LITE_STREAM) is True


def test_settings_editor_leaves_the_cdr_lite_stream_free_under_marbl():
    import ipywidgets as W

    editor = _SettingsEditor(
        W,
        _BGC_MODE_SETTINGS_SAMPLE,
        settings_cls=RunTimeSettingsV0_9_0,
        bgc_mode="marbl",
    )
    master = editor._widgets[_LITE_STREAM][0]
    assert master.value is False
    assert master.disabled is False
    assert _display(editor._widgets[("cdr_lite_output", "output_period")][0]) == "none"
    master.value = True
    assert _display(editor._widgets[("cdr_lite_output", "output_period")][0]) == ""


@pytest.mark.parametrize(
    "section",
    ["cdr_output", "cdr_lite_output", "cdr_gas_exch_output"],
)
def test_cdr_output_stream_do_avg_and_monthly_render_as_dropdowns(section):
    """Every CDR output stream's ``do_avg``/``monthly_averages`` fields render
    as a two-option mode dropdown, not a checkbox -- see
    ``_BOOL_DROPDOWN_FIELDS``. ``cdr_output`` is the pre-existing stream;
    ``cdr_lite_output``/``cdr_gas_exch_output`` (ucla-roms >= 0.7.0) must
    behave identically.
    """
    import ipywidgets as W

    model_settings = {section: {"do_avg": True, "monthly_averages": False}}
    editor = _SettingsEditor(W, model_settings, settings_cls=RunTimeSettingsV0_7_0)

    do_avg_widget, _ = editor._widgets[(section, "do_avg")]
    assert isinstance(do_avg_widget, W.Dropdown)
    assert tuple(do_avg_widget.options) == ("averaged", "instantaneous")
    assert do_avg_widget.value == "averaged"

    monthly_widget, _ = editor._widgets[(section, "monthly_averages")]
    assert isinstance(monthly_widget, W.Dropdown)
    assert tuple(monthly_widget.options) == ("monthly", "periodic")
    assert monthly_widget.value == "periodic"


@pytest.mark.parametrize(
    "section,master_flag",
    [
        ("cdr_output", "do_cdr_output"),
        ("cdr_lite_output", "do_cdr_lite_output"),
        ("cdr_gas_exch_output", "do_cdr_gas_exch_output"),
    ],
)
def test_cdr_output_stream_field_rules_follow_master_switch(section, master_flag):
    """Each CDR output stream's ``do_avg``/``monthly_averages``/
    ``output_period``/``nrpf`` widgets are hidden while its master switch is
    off and shown once it's turned on -- exercises the
    ``_CDR_STREAM_MASTER_FLAGS``-driven loop in ``_apply_field_rules``/
    ``_register_field_rule_observers`` identically across all three streams
    (``cdr_output`` is the pre-existing behavior this generalization must
    keep byte-for-byte). ``cdr_lite_output`` additionally hides its six
    ``wrt_*`` field-group toggles (every non-master field follows) under the
    same condition, since they're meaningless while the stream itself is off.
    Also exercises the averaged/monthly cascade: ``monthly_averages`` stays
    hidden unless ``do_avg`` is "averaged", and ``output_period`` is disabled
    only once both ``do_avg`` is "averaged" and ``monthly_averages`` is
    "monthly" (a fixed monthly cadence makes the period moot).
    """
    import ipywidgets as W

    wrt_fields = (
        "wrt_tracers",
        "wrt_vertical_integrals",
        "wrt_thickness_weighted",
        "wrt_sources",
        "wrt_alk",
        "wrt_dic",
    )
    section_settings = {
        master_flag: False,
        "do_avg": True,
        "monthly_averages": False,
        "output_period": 3600.0,
        "nrpf": 4,
    }
    if section == "cdr_lite_output":
        section_settings.update(dict.fromkeys(wrt_fields, True))
    model_settings = {section: section_settings}

    editor = _SettingsEditor(W, model_settings, settings_cls=RunTimeSettingsV0_7_0)

    def _visible(field: str) -> bool:
        widget, _ = editor._widgets[(section, field)]
        return widget.layout.display != "none"

    # Master off: do_avg/monthly_averages/output_period/nrpf (and, for the
    # tracer stream, every wrt_* toggle) are hidden.
    assert not _visible("do_avg")
    assert not _visible("monthly_averages")
    assert not _visible("output_period")
    assert not _visible("nrpf")
    if section == "cdr_lite_output":
        for field in wrt_fields:
            assert not _visible(field)

    # Master on: they're shown again (do_avg starts "averaged" -> monthly_
    # averages is shown too; monthly_averages starts "periodic" -> output_
    # period stays enabled).
    editor._widgets[(section, master_flag)][0].value = True
    assert _visible("do_avg")
    assert _visible("monthly_averages")
    assert _visible("output_period")
    assert _visible("nrpf")
    if section == "cdr_lite_output":
        for field in wrt_fields:
            assert _visible(field)
    period_widget, _ = editor._widgets[(section, "output_period")]
    assert period_widget.disabled is False

    # do_avg -- Dropdown widgets, so set the mode label, not a bool -- see
    # _BOOL_DROPDOWN_FIELDS. "instantaneous": monthly_averages is meaningless
    # and hidden; output_period stays enabled (there's no fixed cadence to
    # make it moot).
    editor._widgets[(section, "do_avg")][0].value = "instantaneous"
    assert not _visible("monthly_averages")
    assert period_widget.disabled is False

    # "averaged" + monthly_averages "monthly": output_period is disabled --
    # the monthly cadence fixes it implicitly.
    editor._widgets[(section, "do_avg")][0].value = "averaged"
    editor._widgets[(section, "monthly_averages")][0].value = "monthly"
    assert _visible("monthly_averages")
    assert period_widget.disabled is True


# ---------------------------------------------------------------------------
# Output card: the output streams live in their own card (main streams + a
# collapsed "More output streams" accordion), not in the Advanced card.
# ---------------------------------------------------------------------------


_OUTPUT_CARD_SETTINGS = {
    **_BGC_MODE_SETTINGS_SAMPLE,
    "bgc": {"interp_frc": 0, "wrt_his": True, "xco2air_default": 284.7},
    "marbl_bgc": {"marbl_config_file": "marbl_in", "marbl_tracers_to_write": ["DIC"]},
    "surf_flux": {"output_period": 3600.0},
    "diagnostics": {"output_period": 3600.0},
    "particles": {"floats": False},
    "calc_pflx": {"calc_pflx": False},
    "extract_data": {
        "do_extract": True,
        "extract_file": "nesting.nc",
        "nrpf": 24,
        "n_chd": 30,
        "theta_s_chd": 5.0,
        "theta_b_chd": 2.0,
        "hc_chd": 250.0,
        "extract_period": 3600.0,
        "extract_root_name": "child",
    },
    "upscale_output": {"do_upscale": False},
}


def _output_editor(**kwargs):
    import ipywidgets as W

    return _SettingsEditor(
        W,
        _OUTPUT_CARD_SETTINGS,
        settings_cls=RunTimeSettingsV0_9_0,
        **kwargs,
    )


@pytest.mark.parametrize(
    ("is_parent", "is_child"),
    [(False, False), (True, False), (False, True), (True, True)],
)
def test_output_placement_follows_the_nesting_role(is_parent, is_child):
    main, more = _output_placement(is_parent=is_parent, is_child=is_child)
    assert main[:2] == ("ocean_vars", "surf_flux")
    assert ("extract_data" in main) is is_parent
    assert "extract_data" not in more
    assert ("upscale_output" in main) is is_child
    assert ("upscale_output" in more) is (not is_child)
    assert {"particles", "calc_pflx", "stdout_diag"} <= set(more)
    assert not set(main) & set(more)


def test_output_card_main_streams_and_more_accordion_membership():
    editor = _output_editor(is_parent=True, is_child=True)
    groups = editor._group_fields
    assert list(groups[_OUTPUT_CARD]) == [
        "ocean_vars",
        "surf_flux",
        "bgc",
        "marbl_bgc",
        "cdr_output",
        "cdr_lite_output",
        "cdr_gas_exch_output",
        "extract_data",
        "upscale_output",
    ]
    assert list(groups[_OUTPUT_MORE_TITLE]) == [
        "diagnostics",
        "particles",
        "calc_pflx",
    ]
    # More output streams is an accordion collapsed by default.
    assert editor.more_streams.panes[0].selected_index is None
    # The Advanced card keeps the physics/forcing/BGC/CDR panes only.
    assert list(editor._pane_sections) == [
        "Physics & subgrid tuning",
        "Biogeochemistry (BGC / MARBL)",
        "Carbon dioxide removal (CDR)",
    ]
    assert editor._pane_sections["Carbon dioxide removal (CDR)"] == ["cdr_lite"]
    in_advanced = {s for secs in editor._pane_sections.values() for s in secs}
    assert not in_advanced & {
        "cdr_output",
        "cdr_lite_output",
        "cdr_gas_exch_output",
        "ocean_vars",
        "extract_data",
        "upscale_output",
    }
    # Widgets sit in exactly one container, and the registry is shared.
    for (section, field), (widget, _base) in editor._widgets.items():
        in_output = _is_in_tree(editor.output_box, widget)
        in_forcing = _is_in_tree(editor.forcing_box, widget)
        assert in_output is _in_output_card(section, field), (section, field)
        assert in_forcing is _in_forcing_card(section, field), (section, field)
        assert _is_in_tree(editor.accordion, widget) is not (in_output or in_forcing)


def test_output_card_main_streams_are_stacked_under_glossary_headers():
    editor = _output_editor(is_parent=True, is_child=True)
    children = editor.output_box.children
    # headers and bodies are direct children (the divider CSS keys on :first-child),
    # and the collapsed More accordion comes last.
    assert children[-1] is editor.more_streams
    header = editor._headers[(_OUTPUT_CARD, "ocean_vars")]
    assert header in children
    assert "Physical state output" in header.value
    assert "forge-settings-sec-w" in header._dom_classes
    assert editor._headers[(_OUTPUT_CARD, "cdr_lite_output")] in children


def test_output_card_titles_the_split_bgc_sections_itself():
    editor = _output_editor()
    card = {
        s: editor._headers[(_OUTPUT_CARD, s)].value
        for s in ("bgc", "marbl_bgc", "ocean_vars")
    }
    assert "<span class='ttl'>BGC output</span>" in card["bgc"]
    assert (
        "<span class='ttl'>MARBL output (tracers &amp; diagnostics to write)</span>"
        in card["marbl_bgc"]
    )
    # sections the Output card does not split keep the glossary title
    assert "Physical state output" in card["ocean_vars"]
    # the Advanced BGC pane keeps the glossary titles, and the titles survive a
    # retitle with a modified count
    pane = "Biogeochemistry (BGC / MARBL)"
    assert "Biogeochemistry settings" in editor._headers[(pane, "bgc")].value
    assert (
        "<span class='ttl'>MARBL</span>" in editor._headers[(pane, "marbl_bgc")].value
    )
    assert "BGC output" in editor.header_html(_OUTPUT_CARD, "bgc", 2)
    assert "● 2 modified" in editor.header_html(_OUTPUT_CARD, "bgc", 2)


def test_output_card_splits_bgc_fields_along_the_output_seam():
    editor = _output_editor()
    pane = editor._group_fields["Biogeochemistry (BGC / MARBL)"]
    card = editor._group_fields[_OUTPUT_CARD]
    assert pane["bgc"] == ["interp_frc"]
    assert card["bgc"] == ["wrt_his"]
    assert editor._group_fields[_FORCING_CARD] == {"bgc": ["xco2air_default"]}
    assert pane["marbl_bgc"] == ["marbl_config_file"]
    assert card["marbl_bgc"] == ["marbl_tracers_to_write"]
    assert _built_fields(editor)["bgc"] == ["interp_frc", "wrt_his", "xco2air_default"]


def test_extract_data_is_built_only_for_a_parent_with_nesting_owned_fields_hidden():
    absent = _output_editor()
    assert not any(section == "extract_data" for section, _ in absent._widgets)
    assert "extract_data" not in _built_fields(absent)

    parent = _output_editor(is_parent=True)
    built = {field for section, field in parent._widgets if section == "extract_data"}
    assert built == {"nrpf", "extract_root_name"}
    assert parent._group_fields[_OUTPUT_CARD]["extract_data"] == [
        "nrpf",
        "extract_root_name",
    ]
    note = next(
        w.value
        for w in _descendants(parent.output_box)
        if isinstance(getattr(w, "value", None), str) and "nesting options" in w.value
    )
    assert "Grid card" in note


@pytest.mark.parametrize(
    ("is_child", "bgc_mode", "where"),
    [
        (True, "marbl", _OUTPUT_CARD),
        (False, "marbl", _OUTPUT_MORE_TITLE),
        (True, "none", None),
        (False, "cdr_lite", None),
    ],
)
def test_upscale_output_is_main_for_a_child_else_more_and_marbl_only(
    is_child, bgc_mode, where
):
    editor = _output_editor(is_child=is_child, bgc_mode=bgc_mode)
    homes = [
        group
        for group, sections in editor._group_fields.items()
        if "upscale_output" in sections
    ]
    assert homes == ([where] if where else [])


def test_cdr_streams_follow_the_bgc_mode_in_the_output_card():
    marbl = _output_editor(bgc_mode="marbl")
    assert {"cdr_output", "cdr_lite_output", "cdr_gas_exch_output"} <= set(
        marbl._group_fields[_OUTPUT_CARD]
    )
    cdr_lite = _output_editor(bgc_mode="cdr_lite")
    assert list(cdr_lite._group_fields[_OUTPUT_CARD]).count("cdr_lite_output") == 1
    assert "cdr_output" not in cdr_lite._group_fields[_OUTPUT_CARD]
    none = _output_editor(bgc_mode="none")
    assert "cdr_lite_output" not in none._group_fields[_OUTPUT_CARD]


def test_cdr_stream_field_rules_still_apply_inside_the_output_card():
    editor = _output_editor(bgc_mode="marbl")
    master = editor._widgets[("cdr_lite_output", "do_cdr_lite_output")][0]
    period = editor._widgets[("cdr_lite_output", "output_period")][0]
    assert _display(period) == "none"
    master.value = True
    assert _display(period) == ""


# ---------------------------------------------------------------------------
# Hosted fields: the T/S write flags draw inside the ocean_vars history grid and
# the diagnostics row, and the compile-time diagnostics switch heads that row --
# all still keyed by their real (section, field).
# ---------------------------------------------------------------------------


_TS_DIA_KEYS = [("ts_output", "wrt_temp_dia"), ("ts_output", "wrt_salt_dia")]


_DIAGNOSTICS_STACK_KEYS = [
    ("cppdefs", "diagnostics"),
    ("diagnostics", "diag_avg"),
    ("diagnostics", "diag_uv"),
    ("diagnostics", "diag_trc"),
    *_TS_DIA_KEYS,
]


def _diagnostics_editor(*, switch=True, diag_trc=False, on=False, on_edit=None):
    import ipywidgets as W

    settings = {
        "ocean_vars": {"wrt_file_his": True, "wrt_z": True, "wrt_u": True},
        "diagnostics": {
            "diag_avg": False,
            "output_period": 3600.0,
            "nrpf": 24,
            "diag_uv": False,
            "diag_trc": diag_trc,
        },
        "ts_output": {
            "wrt_temp": True,
            "wrt_salt": True,
            "wrt_temp_dia": False,
            "wrt_salt_dia": False,
        },
        "cppdefs": {"sponge_tune": False, **({"diagnostics": on} if switch else {})},
    }
    return _SettingsEditor(
        W, settings, on_edit=on_edit, settings_cls=RunTimeSettingsV0_9_0
    )


def test_ts_flags_draw_in_the_history_variable_grid_not_under_their_own_header():
    editor = _diagnostics_editor()
    grid = next(
        n
        for n in _descendants(editor.output_box)
        if "forge-var-grid" in getattr(n, "_dom_classes", ())
    )
    history_keys = [
        ("ocean_vars", "wrt_z"),
        ("ocean_vars", "wrt_u"),
        ("ts_output", "wrt_temp"),
        ("ts_output", "wrt_salt"),
    ]
    assert grid.children == tuple(editor._widgets[k][0] for k in history_keys)
    assert editor._widgets[("ts_output", "wrt_temp")][0].description == "Temperature"
    # no ts_output section header in either Output group
    assert not [key for key in editor._headers if key[1] == "ts_output"]
    assert not any(
        "<span class='sym'>ts_output</span>" in w.value
        for w in _descendants(editor.output_box)
        if isinstance(getattr(w, "value", None), str)
    )
    # each hosted field is listed under its own section, in its host's group
    assert editor._group_fields[_OUTPUT_CARD]["ts_output"] == ["wrt_temp", "wrt_salt"]
    assert editor._group_fields[_OUTPUT_MORE_TITLE]["ts_output"] == [
        "wrt_temp_dia",
        "wrt_salt_dia",
    ]
    assert editor._group_fields[_OUTPUT_MORE_TITLE]["cppdefs"] == ["diagnostics"]


def test_diagnostics_write_column_stacks_the_switch_first():
    editor = _diagnostics_editor()
    stack = next(
        n
        for n in _descendants(editor.output_box)
        if "forge-out-write-stack" in getattr(n, "_dom_classes", ())
    )
    assert stack.children == tuple(
        editor._widgets[k][0] for k in _DIAGNOSTICS_STACK_KEYS
    )


@pytest.mark.parametrize(
    ("on", "diag_trc", "diagnostics_shown", "ts_shown"),
    [
        (False, False, False, False),
        (False, True, False, False),
        (True, False, True, False),
        (True, True, True, True),
    ],
)
def test_diagnostics_switch_and_tracer_flag_set_what_is_shown(
    on, diag_trc, diagnostics_shown, ts_shown
):
    editor = _diagnostics_editor(on=on, diag_trc=diag_trc)
    assert _display(editor._widgets[("cppdefs", "diagnostics")][0]) == ""
    for field in ("diag_avg", "diag_uv", "diag_trc", "output_period", "nrpf"):
        shown = _display(editor._widgets[("diagnostics", field)][0]) == ""
        assert shown is diagnostics_shown, field
    for key in _TS_DIA_KEYS:
        assert (_display(editor._widgets[key][0]) == "") is ts_shown, key


def test_diagnostics_switch_and_tracer_flag_update_visibility_live_without_editing():
    editor = _diagnostics_editor()
    switch = editor._widgets[("cppdefs", "diagnostics")][0]
    trc = editor._widgets[("diagnostics", "diag_trc")][0]
    ts = editor._widgets[("ts_output", "wrt_temp_dia")][0]
    avg = editor._widgets[("diagnostics", "diag_avg")][0]
    assert (_display(avg), _display(ts)) == ("none", "none")

    switch.value = True
    assert (_display(avg), _display(ts)) == ("", "none")
    trc.value = True
    assert (_display(avg), _display(ts)) == ("", "")
    switch.value = False
    assert (_display(avg), _display(ts)) == ("none", "none")
    # hiding never mutates a value
    assert trc.value is True
    assert editor.read("diagnostics", "diag_trc") is True


def test_diagnostics_stay_visible_when_the_model_spec_declares_no_switch():
    editor = _diagnostics_editor(switch=False)
    assert ("cppdefs", "diagnostics") not in editor._widgets
    assert "cppdefs" not in editor._group_fields.get(_OUTPUT_MORE_TITLE, {})
    for key in [*_DIAGNOSTICS_STACK_KEYS[1:], ("diagnostics", "output_period")]:
        assert _display(editor._widgets[key][0]) == "", key


def test_diagnostics_switch_follows_sync():
    editor = _diagnostics_editor()
    avg = editor._widgets[("diagnostics", "diag_avg")][0]
    editor.sync({"cppdefs": {"diagnostics": True}})
    assert editor._widgets[("cppdefs", "diagnostics")][0].value is True
    assert _display(avg) == ""


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (("ts_output", "wrt_temp"), True),
        (("ts_output", "wrt_salt_dia"), True),
        (("cppdefs", "diagnostics"), True),
        (("cppdefs", "sponge_tune"), False),
        (("ts_output", "wrt_unknown"), False),
    ],
)
def test_hosted_fields_count_as_output_card_placement(key, expected):
    assert _in_output_card(*key) is expected


def test_hosted_field_edits_are_recorded_under_their_real_keys():
    edits: list[tuple] = []
    editor = _diagnostics_editor(on_edit=lambda s, f: edits.append((s, f)))
    for key in (("ts_output", "wrt_temp"), ("cppdefs", "diagnostics")):
        widget = editor._widgets[key][0]
        widget.value = not widget.value
    assert edits == [("ts_output", "wrt_temp"), ("cppdefs", "diagnostics")]
