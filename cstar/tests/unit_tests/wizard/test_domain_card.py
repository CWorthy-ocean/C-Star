"""Tests for the wizard's Domain and grid card: the grid and topography widgets,
the three grid previews, the nested grids' topography and extraction period, and
the lock a pre-made grid file puts on the grid fields.
"""

import copy
from typing import ClassVar

import pytest

from cstar.base.env import BUNDLED_CATALOG_ROOT
from cstar.tests.unit_tests.wizard.helpers import (
    _nested_config_at_1800,
    _new_wizard,
    _write_tiny_netcdf,
)
from cstar.wizard.wizard import ForgeBlueprintWizard


def test_topo_source_dropdown_includes_emod():
    """The topography-source dropdown must offer EMOD alongside ETOPO5/SRTM15."""
    wiz = ForgeBlueprintWizard()
    assert "EMOD" in wiz.domain_card.topo_source.options


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
    wiz.domain_card.domain_dd.value = name
    wiz.domain_card.nest_domain_dd.value = name
    wiz.domain_card.parent_domain_dd.value = name
    wiz.domain_card.on_plot(None)
    wiz.domain_card._on_nest_plot(None)
    wiz.domain_card.on_parent_plot(None)

    for status in (
        wiz.domain_card.plot_status,
        wiz.domain_card.nest_plot_status,
        wiz.domain_card.parent_plot_status,
    ):
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
        wiz.domain_card.nest_enable.value = nest_enabled
        wiz.domain_card.on_plot(None)
        assert calls == ["plot"], (nest_enabled, wiz.domain_card.plot_status.value)


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
    wiz.domain_card._on_nest_plot(None)

    assert any(c[0] == "plot_nesting" for c in calls), (
        wiz.domain_card.nest_plot_status.value
    )
    assert not any(c[0] == "plot" for c in calls)  # doesn't touch the parent plot
    assert len(wiz.domain_card.nest_plot_img.value) > 0
    # The parent plot/status are untouched by the nesting-section refresh
    # (plot_status still carries its initial empty-state hint).
    assert (
        wiz.domain_card.plot_status.value == "Click Refresh preview to draw the grid."
    )
    assert wiz.domain_card.plot_img.value == b""


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
    wiz.domain_card._on_nest_plot(None)

    assert not wiz.domain_card.nest_plot_status.value.startswith(
        "<span class='forge-st-err'>"
    ), wiz.domain_card.nest_plot_status.value
    # A blank new figure (created after a premature close) would have zero axes;
    # the real drawn figure has one axis with the plotted line.
    assert captured.get("axes"), "expected the drawn figure's axes, got a blank figure"
    assert captured["axes"][0].lines, "the plotted line did not survive plt.show()"


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
        monkeypatch.setattr(wiz.domain_card, "on_parent_plot", lambda _b: None)
        wiz.domain_card.parent_domain_dd.value = (
            wiz.domain_card.parent_domain_dd.options[1]
        )

    def test_parent_pick_copies_spec_topography_and_gather_emits_it(self, monkeypatch):
        wiz = _new_wizard()
        assert wiz.domain_card.parent_topo_source.value == "(same as this grid)"
        self._pick_parent(wiz, monkeypatch, self._SPEC)

        assert wiz.domain_card.parent_enable.value is True
        assert wiz.domain_card.parent_w["nx"].value == 30
        assert wiz.domain_card.parent_topo_source.value == "SRTM15"
        assert wiz.domain_card.parent_topo_path.value == "/hpc/data/srtm15_west.nc"

        pk = wiz._gather()["grid_kwargs_parent"]
        assert pk["topography_source"] == "SRTM15"
        assert pk["topography_path"] == "/hpc/data/srtm15_west.nc"
        assert wiz.config is not None, wiz.review_card.derived.value
        assert wiz.config.domain.grid_kwargs_parent["topography_source"] == "SRTM15"
        assert "SRTM15" in wiz.config.datasets

    def test_parent_pick_with_default_topography_is_explicit_etopo5(self, monkeypatch):
        """A parent spec built with the default dataset says so explicitly: the
        parent must NOT silently inherit this grid's (e.g. EMOD) topography.
        """
        wiz = _new_wizard()
        wiz.domain_card.topo_source.value = "EMOD"
        wiz.domain_card.topo_path.value = "/hpc/data/EMODnet_C2.nc"
        spec = {"grid_kwargs": self._SPEC["grid_kwargs"]}  # no topography keys
        self._pick_parent(wiz, monkeypatch, spec)

        assert wiz.domain_card.parent_topo_source.value == "ETOPO5"
        assert wiz.domain_card.parent_topo_path.value == ""
        pk = wiz._gather()["grid_kwargs_parent"]
        assert pk["topography_source"] == "ETOPO5"
        assert "topography_path" not in pk

    def test_inherit_sentinel_emits_no_keys(self):
        wiz = _new_wizard()
        wiz.domain_card.parent_enable.value = True
        wiz.domain_card.nest_enable.value = True
        kw = wiz._gather()
        for key in ("grid_kwargs_parent", "grid_kwargs_child"):
            assert "topography_source" not in kw[key]
            assert "topography_path" not in kw[key]

    def test_round_trips_through_populate_from(self, monkeypatch):
        wiz = _new_wizard()
        self._pick_parent(wiz, monkeypatch, self._SPEC)
        wiz.domain_card.nest_enable.value = True
        wiz.domain_card.child_topo_path.value = (
            "/hpc/data/child_tile.nc"  # path-only override
        )
        wiz._rebuild()
        cfg = wiz.config
        assert cfg is not None, wiz.review_card.derived.value
        assert (
            cfg.domain.grid_kwargs_child["topography_path"] == "/hpc/data/child_tile.nc"
        )
        assert "topography_source" not in cfg.domain.grid_kwargs_child

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert wiz2.domain_card.parent_topo_source.value == "SRTM15"
        assert wiz2.domain_card.parent_topo_path.value == "/hpc/data/srtm15_west.nc"
        assert wiz2.domain_card.child_topo_source.value == "(same as this grid)"
        assert wiz2.domain_card.child_topo_path.value == "/hpc/data/child_tile.nc"
        assert (
            wiz2._gather()["grid_kwargs_parent"] == wiz._gather()["grid_kwargs_parent"]
        )

    def test_plot_topography_falls_back_when_file_not_local(self, tmp_path):
        from cstar.wizard.domain_card import (
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
        wiz.domain_card.parent_enable.value = True
        wiz.domain_card.parent_topo_source.value = "EMOD"
        wiz.domain_card.parent_topo_path.value = str(local)
        wiz.domain_card.topo_source.value = (
            "SRTM15"  # this grid: no local file -> fallback
        )

        wiz.domain_card.on_parent_plot(None)

        parent_kw, this_kw = calls[0], calls[1]
        assert parent_kw["topography_source"] == {"name": "EMOD", "path": str(local)}
        assert "topography_source" not in this_kw
        assert (
            "SRTM15" in wiz.domain_card.parent_plot_status.value
        )  # fallback note for this grid
        assert "EMOD" not in wiz.domain_card.parent_plot_status.value


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

    assert wiz2.domain_card.nest_period.value == 1800.0
    assert wiz2.config.model_settings["extract_data"]["extract_period"] == 1800.0
    assert wiz2._gather()["metadata_child"] == {"period": 1800.0}


def test_nest_period_falls_back_to_metadata_child_without_an_extract_period():
    wiz = _new_wizard()
    wiz.domain_card.nest_enable.value = True
    wiz.domain_card.nest_period.value = 1800.0
    wiz._rebuild()
    cfg = wiz.config
    settings = copy.deepcopy(cfg.model_settings)
    del settings["extract_data"]["extract_period"]

    wiz2 = _new_wizard()
    wiz2._populate_from(cfg.model_copy(update={"model_settings": settings}))
    assert wiz2.domain_card.nest_period.value == 1800.0
    assert wiz2.config.model_settings["extract_data"]["extract_period"] == 1800.0


def test_set_grid_widgets_locked_toggles_grid_lock_banner():
    wiz = ForgeBlueprintWizard()
    assert wiz.domain_card.grid_lock_banner.layout.display == "none"

    wiz.domain_card._set_grid_widgets_locked(True)
    assert wiz.domain_card.grid_lock_banner.layout.display == ""

    wiz.domain_card._set_grid_widgets_locked(False)
    assert wiz.domain_card.grid_lock_banner.layout.display == "none"
