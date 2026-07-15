"""Tests for the ipywidgets ForgeBlueprintWizard UI (cstar_forge.forge_blueprint_wizard).

These target the wizard-feedback fixes: conditional field visibility, forcing-row
option ordering, the ntides sync into model_settings, and the nesting-section
plot_nesting wiring. Widget construction is lightweight (no grid/network I/O — the
live preview resolves via ``dt``, never building a roms_tools.Grid), so these run
as fast unit tests.
"""

from datetime import date

import pytest

from cstar_forge.forge_blueprint_wizard import (
    ForgeBlueprintWizard,
    _ForcingEditor,
)


@pytest.fixture
def editor():
    import ipywidgets as W

    return _ForcingEditor(W, {}, on_change=lambda: None)


def _display(widget) -> str:
    return getattr(widget.layout, "display", "") or ""


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


def test_surface_row_visibility_by_type(editor):
    """Item 2/8: restore/corr_rad/wind_dropoff only show for their relevant type."""
    w = editor._make_row("surface", {"type": "physics", "source": {"name": "ERA5"}})
    assert _display(w["correct_radiation"]) == ""  # physics -> shown
    assert _display(w["wind_dropoff"]) == ""
    assert _display(w["restoring_forces"]) == "none"  # not restoring -> hidden

    w["type"].value = "restoring"
    assert _display(w["restoring_forces"]) == ""  # restoring -> shown
    assert _display(w["correct_radiation"]) == "none"  # no longer physics -> hidden
    assert _display(w["wind_dropoff"]) == "none"

    w["type"].value = "bgc"
    assert _display(w["restoring_forces"]) == "none"
    assert _display(w["correct_radiation"]) == "none"
    assert _display(w["wind_dropoff"]) == "none"


def test_boundary_row_layout_visibility_by_source_name(editor):
    """Item 7: glorys_layout only shows when the source name is GLORYS. Switching
    the boundary `type` to bgc resets `name` away from GLORYS (bgc boundary sources
    are UNIFIED/CESM_REGRIDDED) and must hide the layout box via the same cascade
    that resets the name dropdown's options (_on_type_change).
    """
    w = editor._make_row("boundary", {"type": "physics", "source": {"name": "GLORYS"}})
    assert w["name"].value == "GLORYS"
    assert _display(w["glorys_layout"]) == ""

    w["type"].value = "bgc"
    assert w["name"].value != "GLORYS"
    assert _display(w["glorys_layout"]) == "none"


def test_surface_row_never_shows_layout(editor):
    """Surface sources never include GLORYS, so the layout box is always hidden."""
    w = editor._make_row("surface", {"type": "physics", "source": {"name": "ERA5"}})
    assert _display(w["glorys_layout"]) == "none"


def test_ic_layout_visibility_initial_state():
    """Item 7 (IC side): glorys_layout is shown for the (currently sole) IC source,
    GLORYS. InitialConditionsSource has only one member today, so the hidden branch
    can't be exercised via the dropdown; this pins the visible/default case.
    """
    import ipywidgets as W

    ed = _ForcingEditor(
        W,
        {"initial_conditions": {"source": {"name": "GLORYS"}}},
        on_change=lambda: None,
    )
    assert ed.ic_name.value == "GLORYS"
    assert _display(ed.ic_layout) == ""


def test_row_box_puts_type_first(editor):
    """Item 3: `type` (when present) is the left-most widget in the row."""
    w = editor._make_row("surface", {"type": "bgc", "source": {"name": "ERA5"}})
    box = editor._row_box(w)
    assert box.children[0] is w["type"]


def test_row_box_without_type_unaffected(editor):
    """tidal/river rows have no `type`; ordering must not error or reorder oddly."""
    w = editor._make_row("tidal", {"ntides": 15, "source": {"name": "TPXO"}})
    box = editor._row_box(w)
    assert w["ntides"] in box.children


def test_wizard_smoke_assembles_widget():
    """Reordered sections (item 4b) and relocated dropdowns (item 5) assemble cleanly."""
    wiz = ForgeBlueprintWizard()
    root = wiz.widget  # must not raise
    # crude structural check: section titles appear in the expected relative order
    titles = []

    def walk(w):
        html = getattr(w, "value", None)
        if isinstance(html, str) and html.startswith("<b>"):
            titles.append(html)
        for c in getattr(w, "children", []):
            walk(c)

    walk(root)
    order = [t for t in titles]
    grid_i = next(i for i, t in enumerate(order) if "Grid" in t and "Nesting" not in t)
    obc_i = next(i for i, t in enumerate(order) if "Open boundaries" in t)
    nest_i = next(i for i, t in enumerate(order) if "Nesting" in t)
    assert grid_i < obc_i < nest_i


def test_pieces_section_has_forcing_and_output_dropdowns():
    """Item 5: Forcing/Output selectors live in the first 'Pieces' box."""
    wiz = ForgeBlueprintWizard()
    pieces_box = _find_section(wiz.widget, "<b>Pieces</b>")
    assert pieces_box is not None
    assert wiz.forcing_dd in pieces_box.children
    assert wiz.output_dd in pieces_box.children


def test_roms_ref_prefilled_and_placed_next_to_model_dropdown():
    """ucla-roms ref is prefilled from the selected model's pinned default (stays
    editable) and lives right next to the Model dropdown in the Pieces section.
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.roms_ref.value == wiz._model_default_roms_ref()
    assert wiz.roms_ref.value  # this model.yml pins a concrete commit

    pieces_box = _find_section(wiz.widget, "<b>Pieces</b>")
    assert pieces_box is not None
    model_row = next(
        c for c in pieces_box.children if wiz.model_dd in getattr(c, "children", [])
    )
    assert wiz.roms_ref in model_row.children  # same row as the Model dropdown


def test_roms_ref_repopulates_on_model_change(monkeypatch):
    """Switching models refreshes ucla-roms ref to the new model's pinned default."""
    wiz = ForgeBlueprintWizard()
    monkeypatch.setattr(wiz, "_model_default_roms_ref", lambda: "some-other-ref")
    wiz._on_model_change(None)
    assert wiz.roms_ref.value == "some-other-ref"


def test_co2_tvarying_is_not_user_editable():
    """co2_tvarying is controlled solely by the presence of an MBL_co2 bgc surface
    source; the wizard must not expose a checkbox letting the user override it --
    that derivation happens behind the scenes in the resolver/input_data.
    """
    wiz = ForgeBlueprintWizard()
    assert not hasattr(wiz, "co2_tvarying_chk")

    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    # The default model's forcing includes an MBL_co2 bgc surface source, so the
    # resolver still auto-derives co2_tvarying=True with no UI toggle involved.
    assert wiz.config.model_settings["cppdefs"]["co2_tvarying"] is True


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
    # The parent plot/status are untouched by the nesting-section refresh.
    assert wiz.plot_status.value == ""
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

    assert not wiz.nest_plot_status.value.startswith("<span style='color:#b00'>"), (
        wiz.nest_plot_status.value
    )
    # A blank new figure (created after a premature close) would have zero axes;
    # the real drawn figure has one axis with the plotted line.
    assert captured.get("axes"), "expected the drawn figure's axes, got a blank figure"
    assert captured["axes"][0].lines, "the plotted line did not survive plt.show()"
