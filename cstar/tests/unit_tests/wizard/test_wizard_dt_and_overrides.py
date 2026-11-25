"""The wizard's time step follows the grid until the user sets one, and an
Advanced-settings edit can neither pin a value the resolver forces off nor turn
one back on.

Each test saves through the real Save handler and reads the file it wrote.
"""

import shutil
from datetime import date
from pathlib import Path

import pytest

from cstar.applications.forge.blueprint import ForgeBlueprint
from cstar.applications.forge.resolve import cfl_dt
from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog
from cstar.roms.precheck import marbl_step
from cstar.wizard.wizard import ForgeBlueprintWizard

_BRY_TIDES = ("tides", "bry_tides")


def _wizard() -> ForgeBlueprintWizard:
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    assert wiz.config is not None, wiz.review_card.derived.value
    return wiz


def _save(wiz: ForgeBlueprintWizard, path: Path) -> ForgeBlueprint:
    """Press Save and return the blueprint it wrote."""
    wiz.review_card.save_path.value = str(path)
    wiz.domain_card._boundaries_touched = (
        True  # not exercising boundary derivation here
    )
    wiz.review_card._on_save(None)
    assert "Saved" in wiz.review_card.save_status.value, (
        wiz.review_card.save_status.value
    )
    return ForgeBlueprint.from_yaml(path)


def _saved_dt(bp: ForgeBlueprint) -> float:
    assert bp.domain.dt == bp.model_settings["time_stepping"]["dt"]
    return float(bp.domain.dt)


def _grid_cfl_dt(wiz: ForgeBlueprintWizard) -> float:
    return float(cfl_dt(wiz.domain_card._base_grid_kwargs()))


class TestDerivedDt:
    def test_a_fresh_grid_saves_the_cfl_dt(self, tmp_path):
        wiz = _wizard()
        expected = _grid_cfl_dt(wiz)
        assert expected != 7200.0  # the old fixed default
        assert wiz.domain_card.dt.value == expected
        assert "derived" in wiz.domain_card.dt_status.value
        assert _saved_dt(_save(wiz, tmp_path / "bp.yaml")) == expected

    def test_a_grid_edit_re_derives_dt(self, tmp_path):
        wiz = _wizard()
        before = wiz.domain_card.dt.value
        wiz.domain_card.grid_w["nx"].value = wiz.domain_card.grid_w["nx"].value * 4
        after = _grid_cfl_dt(wiz)
        assert after < before
        assert wiz.domain_card.dt.value == after
        assert _saved_dt(_save(wiz, tmp_path / "bp.yaml")) == after

    def test_a_typed_dt_is_kept_through_grid_edits(self, tmp_path):
        wiz = _wizard()
        wiz.domain_card.dt.value = 300.0
        assert wiz.domain_card._dt_touched
        assert wiz.domain_card.dt_status.value == ""
        wiz.domain_card.grid_w["nx"].value = wiz.domain_card.grid_w["nx"].value * 4
        assert wiz.domain_card.dt.value == 300.0
        assert _saved_dt(_save(wiz, tmp_path / "bp.yaml")) == 300.0
        assert wiz._domain_spec_data()["dt"] == 300.0

    def test_use_cfl_value_goes_back_to_deriving(self, tmp_path):
        """The button clears a typed dt, and a later grid edit is followed rather
        than leaving a stale computed value (and status) behind.
        """
        wiz = _wizard()
        wiz.domain_card.dt.value = 300.0
        wiz.domain_card._on_compute_dt(None)
        assert not wiz.domain_card._dt_touched
        assert wiz.domain_card.dt.value == _grid_cfl_dt(wiz)
        wiz.domain_card.grid_w["nx"].value = wiz.domain_card.grid_w["nx"].value * 4
        after = _grid_cfl_dt(wiz)
        assert wiz.domain_card.dt.value == after
        assert "derived" in wiz.domain_card.dt_status.value
        assert _saved_dt(_save(wiz, tmp_path / "bp.yaml")) == after

    def test_an_untouched_dt_stays_out_of_a_saved_domain_spec(self):
        assert "dt" not in _wizard()._domain_spec_data()

    def test_a_loaded_blueprint_keeps_its_dt(self, tmp_path):
        w1 = _wizard()
        w1.domain_card.dt.value = 450.0
        path = tmp_path / "bp.yaml"
        _save(w1, path)

        w2 = ForgeBlueprintWizard()
        w2.load_path.value = str(path)
        w2._on_load_path(None)
        assert w2.config is not None, w2.review_card.derived.value
        assert w2.domain_card._dt_touched
        assert w2.domain_card.dt.value == 450.0
        w2.domain_card.grid_w["nx"].value = w2.domain_card.grid_w["nx"].value * 4
        assert w2.domain_card.dt.value == 450.0

    @pytest.mark.parametrize("domain", ["wio-toy", "gulf-guinea-toy"])
    def test_the_coarse_toys_get_a_legal_marbl_step(self, domain, tmp_path):
        """Their CFL estimates (about 31,000 s and 15,700 s) used to give 28,800 s
        and 14,400 s, beyond the 10,800 s MARBL step at which ucla-roms aborts.
        """
        wiz = _wizard()
        wiz.domain_card.domain_dd.value = domain
        assert wiz.model_card.bgc_dd.value == "marbl"
        assert not wiz.domain_card._dt_touched
        bp = _save(wiz, tmp_path / "bp.yaml")
        dt = _saved_dt(bp)
        assert dt == 3600.0
        marbl_timestep = bp.model_settings["marbl_bgc"]["marbl_timestep"]
        assert marbl_step(dt, marbl_timestep) <= 10800


class TestOverridesCannotDefeatTheTideRules:
    def test_an_edit_back_to_the_composed_value_is_not_an_override(self, tmp_path):
        """Flip bry_tides and back, then make the grid a child: the resolver turns
        boundary tides off, and nothing pins them on.
        """
        wiz = _wizard()
        assert wiz.config.model_settings["tides"]["bry_tides"] is True
        widget = wiz.editor._widgets[_BRY_TIDES][0]
        widget.value = False
        assert wiz._overrides[_BRY_TIDES] is False
        widget.value = True
        assert _BRY_TIDES not in wiz._overrides

        wiz.domain_card.parent_enable.value = True
        assert wiz.config is not None, wiz.review_card.derived.value
        bp = _save(wiz, tmp_path / "bp.yaml")
        assert bp.model_settings["tides"]["bry_tides"] is False

    def test_a_child_grid_cannot_turn_boundary_tides_on(self, tmp_path):
        wiz = _wizard()
        wiz.domain_card.parent_enable.value = True
        assert wiz.config is not None, wiz.review_card.derived.value
        widget = wiz.editor._widgets[_BRY_TIDES][0]
        assert widget.value is False
        widget.value = True
        assert widget.value is False
        bp = _save(wiz, tmp_path / "bp.yaml")
        assert bp.model_settings["tides"]["bry_tides"] is False
        assert bp.model_settings["sponge_tune"]["ub_tune"] is False

    def test_no_tidal_forcing_keeps_the_tide_switches_off(self, tmp_path):
        wiz = _wizard()
        wiz._forcing_editor.clear_category("tidal")
        assert wiz.config is not None, wiz.review_card.derived.value
        assert not wiz.config.forcing.tidal
        widget = wiz.editor._widgets[("tides", "pot_tides")][0]
        widget.value = True
        bp = _save(wiz, tmp_path / "bp.yaml")
        assert bp.model_settings["tides"]["pot_tides"] is False
        assert bp.model_settings["tides"]["bry_tides"] is False

    def test_a_forced_override_does_not_break_saving_a_spec(self, tmp_path):
        """Saving a spec ("Save as new spec") re-resolves and compares with the blueprint shown;
        both sides must force the tide switches off over the overrides.
        """
        root = tmp_path / "catalog"
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        catalog = DomainCatalog(catalog_root=root)
        wiz = ForgeBlueprintWizard(catalog=catalog)
        wiz.run_card.start.value = date(2012, 1, 1)
        wiz.run_card.end.value = date(2012, 1, 2)
        wiz.domain_card.parent_enable.value = True
        wiz.editor._widgets[_BRY_TIDES][0].value = True
        assert wiz._overrides[_BRY_TIDES] is True
        assert wiz.config.model_settings["tides"]["bry_tides"] is False

        wiz.save_domain_name.value = "child-domain"
        wiz._on_save_domain(None)
        assert "child-domain" in catalog.domain_names
        assert wiz.domain_card.domain_dd.value == "child-domain", (
            wiz.save_domain_status.value
        )
