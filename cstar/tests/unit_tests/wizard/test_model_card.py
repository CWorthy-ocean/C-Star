"""Tests for the wizard's Model card: the ModelSpec picker and its archived
filter, the BGC mode, the ucla-roms and MARBL refs, and the carbonate
sensitivities group shown for bgc_mode "cdr_lite".
"""

from datetime import date
from pathlib import Path

import pytest

from cstar.tests.unit_tests.wizard.helpers import (
    _cdr_lite_wizard,
    _descendants,
    _display,
    _find_card,
    _strip_bgc_forcing,
)
from cstar.wizard.fields import _dd_values
from cstar.wizard.wizard import ForgeBlueprintWizard


def test_roms_ref_prefilled_and_placed_next_to_model_dropdown():
    """ucla-roms ref is prefilled from the selected model's pinned default
    (stays editable) and lives in the Model card, in its own field row below
    the Model dropdown (the card/field_row redesign no longer puts it
    literally in the same row as Model -- see components.field_row).
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.model_card.roms_ref.value == wiz.model_card.default_roms_ref()
    assert wiz.model_card.roms_ref.value  # this model.yaml pins a concrete commit

    model_card = _find_card(wiz.widget, "model")
    assert model_card is not None
    assert wiz.model_card.roms_ref in _descendants(model_card)


def test_roms_ref_repopulates_on_model_change(monkeypatch):
    """Switching models refreshes ucla-roms ref to the new model's pinned default."""
    wiz = ForgeBlueprintWizard()
    monkeypatch.setattr(wiz.model_card, "default_roms_ref", lambda: "some-other-ref")
    wiz._on_model_change(None)
    assert wiz.model_card.roms_ref.value == "some-other-ref"


def test_marbl_ref_prefilled_and_placed_next_to_model_dropdown():
    """MARBL ref mirrors the ucla-roms ref: prefilled from the selected model's
    pinned default (stays editable) and lives in the Model card, after
    (in document order) BGC mode's own row -- MARBL ref only matters once BGC
    mode is "marbl".
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.model_card.marbl_ref.value == wiz.model_card.default_marbl_ref()
    assert wiz.model_card.marbl_ref.value  # this model.yaml pins a concrete MARBL tag

    model_card = _find_card(wiz.widget, "model")
    assert model_card is not None
    descendants = list(_descendants(model_card))
    assert wiz.model_card.marbl_ref in descendants
    assert wiz.model_card.bgc_dd in descendants
    assert descendants.index(wiz.model_card.marbl_ref) > descendants.index(
        wiz.model_card.bgc_dd
    )


def test_marbl_ref_hidden_unless_bgc_is_marbl():
    """The MARBL ref field only shows when BGC mode is "marbl". Its value is
    kept (not cleared) while hidden, so toggling BGC back restores the pin.
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.model_card.bgc_dd.value == "marbl"
    assert wiz.model_card.marbl_ref.layout.display != "none"
    kept = wiz.model_card.marbl_ref.value

    wiz.model_card.bgc_dd.value = "none"
    assert wiz.model_card.marbl_ref.layout.display == "none"
    assert wiz.model_card.marbl_ref.value == kept

    wiz.model_card.bgc_dd.value = "marbl"
    assert wiz.model_card.marbl_ref.layout.display != "none"


def test_marbl_ref_repopulates_on_model_change(monkeypatch):
    """Switching models refreshes the MARBL ref to the new model's pinned default."""
    wiz = ForgeBlueprintWizard()
    monkeypatch.setattr(wiz.model_card, "default_marbl_ref", lambda: "some-other-marbl")
    wiz._on_model_change(None)
    assert wiz.model_card.marbl_ref.value == "some-other-marbl"


def test_bgc_dd_default_and_placement():
    """BGC mode defaults to 'marbl' and lives in the Model card."""
    wiz = ForgeBlueprintWizard()
    assert wiz.model_card.bgc_dd.value == "marbl"
    assert set(wiz.model_card.bgc_dd.options) == {"marbl", "none", "cdr_lite"}

    model_card = _find_card(wiz.widget, "model")
    assert model_card is not None
    assert wiz.model_card.bgc_dd in _descendants(model_card)


def test_use_pio_chk_default_seeded_from_model_spec():
    """use_pio_chk mirrors bgc_dd: it is seeded from the selected ModelSpec's
    top-level use_pio (True for roms-marbl-0.16-default, the wizard's default
    model; False for cson_roms-marbl_v0.1), and reseeded on a model switch.
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.model_card.model_dd.value == "roms-marbl-0.16-default"
    assert wiz.run_card.use_pio_chk.value is True
    assert wiz.model_card.default_use_pio() is True

    wiz.model_card.show_archived_models.value = True
    wiz.model_card.model_dd.value = "cson_roms-marbl_v0.1"
    assert wiz.run_card.use_pio_chk.value is False
    assert wiz.model_card.default_use_pio() is False


def test_model_dd_hides_archived_specs_by_default():
    """The model dropdown lists only non-archived specs until "Show archived" is
    checked; checking it adds the archived ones, labelled, in catalog order.
    """
    wiz = ForgeBlueprintWizard()
    archived = wiz.catalog.archived_model_names
    assert archived  # the bundled layer ships archived specs
    assert wiz.model_card.show_archived_models.value is False
    assert set(_dd_values(wiz.model_card.model_dd)) == set(
        wiz.catalog.model_names
    ) - set(archived)
    assert wiz.model_card.model_dd.value == "roms-marbl-0.16-default"

    wiz.model_card.show_archived_models.value = True
    assert _dd_values(wiz.model_card.model_dd) == list(wiz.catalog.model_names)
    assert wiz.model_card.model_dd.value == "roms-marbl-0.16-default"
    labels = {value: label for label, value in wiz.model_card.model_dd.options}
    for name in archived:
        assert labels[name] == f"{name} (bundled, archived)"
    assert labels["roms-marbl-0.16-default"] == "roms-marbl-0.16-default (bundled)"

    wiz.model_card.show_archived_models.value = False
    assert set(_dd_values(wiz.model_card.model_dd)) == set(
        wiz.catalog.model_names
    ) - set(archived)


def test_loading_a_blueprint_with_an_archived_model_selects_it(tmp_path):
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz.model_card.show_archived_models.value = True
    wiz.model_card.model_dd.value = "roms-marbl-0.7-default"
    wiz._rebuild()
    assert wiz.config is not None, wiz.review_card.derived.value
    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    assert wiz2.model_card.show_archived_models.value is False
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    assert wiz2.model_card.show_archived_models.value is True
    assert wiz2.model_card.model_dd.value == "roms-marbl-0.7-default"
    assert "roms-marbl-0.7-default" in _dd_values(wiz2.model_card.model_dd)


# ---------------------------------------------------------------------------
# bgc_mode "cdr_lite": the BGC dropdown option, the carbonate sensitivities
# group (cs_*), and the CDR panel's cdr_lite hint.
# ---------------------------------------------------------------------------


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


def test_bgc_dd_offers_the_three_build_modes_in_order():
    from typing import get_args

    from cstar.applications.forge.namelist_model import BgcMode

    wiz = ForgeBlueprintWizard()
    assert list(wiz.model_card.bgc_dd.options) == ["marbl", "none", "cdr_lite"]
    assert list(wiz.model_card.bgc_dd.options) == list(get_args(BgcMode))
    assert "cdr_lite" in wiz.model_card.bgc_dd.tooltip


def test_marbl_ref_hidden_for_cdr_lite():
    wiz = ForgeBlueprintWizard()
    wiz.model_card.bgc_dd.value = "cdr_lite"
    assert _display(wiz.model_card.marbl_ref) == "none"


class TestCarbonateSensitivityGroup:
    def test_group_lives_in_the_model_card_and_is_hidden_by_default(self):
        wiz = ForgeBlueprintWizard()
        model_card = _find_card(wiz.widget, "model")
        assert wiz.model_card.cs_group in _descendants(model_card)
        assert wiz.model_card.bgc_dd.value == "marbl"
        assert _display(wiz.model_card.cs_group) == "none"

    def test_group_shows_only_for_cdr_lite(self):
        wiz = ForgeBlueprintWizard()
        wiz.model_card.bgc_dd.value = "cdr_lite"
        assert _display(wiz.model_card.cs_group) == ""
        wiz.model_card.bgc_dd.value = "none"
        assert _display(wiz.model_card.cs_group) == "none"
        wiz.model_card.bgc_dd.value = "marbl"
        assert _display(wiz.model_card.cs_group) == "none"

    def test_source_defaults_to_the_directive_and_hides_the_attach_row(self):
        wiz = ForgeBlueprintWizard()
        wiz.model_card.bgc_dd.value = "cdr_lite"
        assert wiz.model_card.cs_source_dd.value == "directive"
        assert [v for _, v in wiz.model_card.cs_source_dd.options] == [
            "directive",
            "path",
        ]
        assert _display(wiz.model_card.cs_path_box) == "none"

        wiz.model_card.cs_source_dd.value = "path"
        assert _display(wiz.model_card.cs_path_box) == ""
        assert "no directory attached yet" in wiz.model_card.cs_status.value.lower()

        wiz.model_card.cs_source_dd.value = "directive"
        assert _display(wiz.model_card.cs_path_box) == "none"

    def test_default_cdr_lite_blueprint_has_no_carbonate_sensitivity(self):
        wiz = _cdr_lite_wizard()
        assert wiz.config is not None, wiz.review_card.derived.value
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
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card.cs_path.value = str(d)  # submit -> auto-attach, no click needed

        files = wiz.model_card._carbonate_sensitivity["files"]
        assert [Path(f["location"]).name for f in files] == [
            "roms_cdrgas.20120101000000.nc",
            "roms_cdrgas.20120102000000.nc",
        ]
        assert all(f["content_hash"].startswith(f"{HASH_SCHEME}:") for f in files)
        status = wiz.model_card.cs_status.value
        assert "attached 2 carbonate sensitivity file(s)" in status
        assert "2012-01-01 00:00 to 2012-01-02 00:00" in status
        assert "exact paths" in status
        assert wiz.config is not None, wiz.review_card.derived.value
        assert [f.location for f in wiz.config.carbonate_sensitivity.files] == [
            f["location"] for f in files
        ]

    def test_button_attach_and_rehash(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card.cs_path.value = str(d)
        wiz.model_card._carbonate_sensitivity = None
        wiz.model_card._on_cs_attach(None)
        assert len(wiz.model_card._carbonate_sensitivity["files"]) == 2

    def test_attach_without_a_path_asks_for_one(self):
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._on_cs_attach(None)
        assert "enter a path" in wiz.model_card.cs_status.value.lower()
        assert wiz.model_card._carbonate_sensitivity is None

    def test_per_rank_tiles_are_ignored(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        _write_cdrgas_dir(d, stamps=("20120101000000.0", "20120101000000.1"))
        assert (d / "roms_cdrgas.20120101000000.0.nc").exists()
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))
        names = [
            Path(f["location"]).name
            for f in wiz.model_card._carbonate_sensitivity["files"]
        ]
        assert names == [
            "roms_cdrgas.20120101000000.nc",
            "roms_cdrgas.20120102000000.nc",
        ]
        assert "attached 2 " in wiz.model_card.cs_status.value

    def test_directory_of_only_tiles_is_an_error(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "tiles", stamps=("20120101000000.0",))
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))
        assert wiz.model_card._carbonate_sensitivity is None
        assert "1 per-rank tile" in wiz.model_card.cs_status.value
        assert "forge-st-err" in wiz.model_card.cs_status.value  # the error colour

    def test_empty_directory_is_an_error(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))
        assert wiz.model_card._carbonate_sensitivity is None
        assert "no joined carbonate sensitivity files" in wiz.model_card.cs_status.value
        assert wiz.config is None
        assert "attach a directory" in wiz.review_card.derived.value

    def test_missing_path_and_plain_file_are_errors(self, tmp_path):
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(tmp_path / "nope"))
        assert wiz.model_card._carbonate_sensitivity is None
        assert "not a directory" in wiz.model_card.cs_status.value

    def test_non_netcdf_file_is_an_error_and_nothing_is_stored(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        (d / "roms_cdrgas.20120103000000.nc").write_text("this is not netCDF")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))
        assert wiz.model_card._carbonate_sensitivity is None
        assert (
            "not netCDF files: roms_cdrgas.20120103000000.nc"
            in wiz.model_card.cs_status.value
        )

    def test_misnamed_cdrgas_file_is_an_error_with_a_legible_status(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        (d / "roms_cdrgas_old.nc").write_text("x")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))
        assert wiz.model_card._carbonate_sensitivity is None
        assert "naming convention" in wiz.model_card.cs_status.value

    def test_repeated_basenames_under_subdirectories_are_an_error(self, tmp_path):
        d = tmp_path / "runs"
        _write_cdrgas_dir(d / "run1", stamps=("20120101000000",))
        _write_cdrgas_dir(d / "run2", stamps=("20120101000000",))
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))
        assert wiz.model_card._carbonate_sensitivity is None
        assert "each must be distinct" in wiz.model_card.cs_status.value
        assert "✓" not in wiz.model_card.cs_status.value

    def test_failed_reattach_drops_the_earlier_attach(self, tmp_path):
        good = _write_cdrgas_dir(tmp_path / "good")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(good))
        assert wiz.model_card._carbonate_sensitivity is not None

        wiz.model_card._attach_carbonate_sensitivity_dir(str(tmp_path / "nope"))
        assert wiz.model_card._carbonate_sensitivity is None  # not quietly kept
        assert wiz.config is None

    def test_pio_hint_for_non_classic_files(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "hdf5", fmt="NETCDF4")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.run_card.use_pio_chk.value = True
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))
        assert "CDF-5" in wiz.model_card.cs_status.value

    def test_no_pio_hint_without_pio_or_for_classic_files(self, tmp_path):
        hdf5 = _write_cdrgas_dir(tmp_path / "hdf5", fmt="NETCDF4")
        classic = _write_cdrgas_dir(tmp_path / "classic", fmt="NETCDF3_64BIT")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.run_card.use_pio_chk.value = False
        wiz.model_card._attach_carbonate_sensitivity_dir(str(hdf5))
        assert "CDF-5" not in wiz.model_card.cs_status.value
        wiz.run_card.use_pio_chk.value = True
        wiz.model_card._attach_carbonate_sensitivity_dir(str(classic))
        assert "CDF-5" not in wiz.model_card.cs_status.value

    def test_clear_resets_state_and_brings_the_hint_back(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card.cs_path.value = str(d)
        assert wiz.model_card._carbonate_sensitivity is not None

        wiz.model_card._on_cs_clear(None)

        assert wiz.model_card._carbonate_sensitivity is None
        assert wiz.model_card.cs_path.value == ""
        assert "no directory attached yet" in wiz.model_card.cs_status.value.lower()
        assert wiz.config is None

    def test_gather_passes_the_attached_files_only_for_the_path_source(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))

        assert (
            wiz._gather()["carbonate_sensitivity"]
            == wiz.model_card._carbonate_sensitivity
        )

        wiz.model_card.cs_source_dd.value = (
            "directive"  # attached state kept, but not passed
        )
        assert wiz.model_card._carbonate_sensitivity is not None
        assert "carbonate_sensitivity" not in wiz._gather()
        assert wiz.config is not None, wiz.review_card.derived.value
        assert wiz.config.carbonate_sensitivity is None

    def test_gather_omits_attached_files_outside_cdr_lite(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))

        wiz.model_card.bgc_dd.value = "none"  # the resolver would reject the files here
        assert "carbonate_sensitivity" not in wiz._gather()

    def test_gather_fails_fast_for_the_path_source_with_nothing_attached(self):
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        with pytest.raises(ValueError, match="attach a directory"):
            wiz._gather()
        assert wiz.config is None
        assert "attach a directory" in wiz.review_card.derived.value

    def test_round_trips_through_populate_from(self, tmp_path):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))
        assert wiz.config is not None, wiz.review_card.derived.value

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)

        assert wiz2.model_card.bgc_dd.value == "cdr_lite"
        assert wiz2.model_card.cs_source_dd.value == "path"
        assert wiz2.model_card.cs_path.value == str(
            d.resolve()
        )  # the files' common parent
        assert (
            wiz2.model_card._carbonate_sensitivity
            == wiz.model_card._carbonate_sensitivity
        )
        assert (
            "attached 2 carbonate sensitivity file(s)"
            in wiz2.model_card.cs_status.value
        )
        assert _display(wiz2.model_card.cs_group) == ""
        assert wiz2.config is not None, wiz2.review_card.derived.value
        assert wiz2.config.content_hash() == wiz.config.content_hash()

    def test_load_back_trusts_the_stored_hashes_and_warns_on_missing_files(
        self, tmp_path
    ):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz = _cdr_lite_wizard()
        wiz.model_card.cs_source_dd.value = "path"
        wiz.model_card._attach_carbonate_sensitivity_dir(str(d))
        cfg = wiz.config
        stored = dict(wiz.model_card._carbonate_sensitivity)
        for f in d.glob("*.nc"):
            f.unlink()

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert (
            wiz2.model_card._carbonate_sensitivity == stored
        )  # hashes kept, not recomputed
        assert "2 file(s) not found" in wiz2.model_card.cs_status.value
        assert wiz2.config is not None

    def test_load_back_of_a_directive_blueprint_selects_the_directive(self):
        wiz = _cdr_lite_wizard()
        assert wiz.config is not None, wiz.review_card.derived.value
        assert wiz.config.carbonate_sensitivity is None

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)

        assert wiz2.model_card.bgc_dd.value == "cdr_lite"
        assert wiz2.model_card.cs_source_dd.value == "directive"
        assert wiz2.model_card._carbonate_sensitivity is None
        assert wiz2.model_card.cs_path.value == ""
        assert _display(wiz2.model_card.cs_group) == ""
        assert _display(wiz2.model_card.cs_path_box) == "none"
        assert wiz2.config is not None, wiz2.review_card.derived.value
        assert wiz2.config.content_hash() == wiz.config.content_hash()

    def test_load_back_of_a_directive_blueprint_resets_an_earlier_attach(
        self, tmp_path
    ):
        d = _write_cdrgas_dir(tmp_path / "cdrgas")
        wiz2 = _cdr_lite_wizard()
        wiz2.model_card.cs_source_dd.value = "path"
        wiz2.model_card._attach_carbonate_sensitivity_dir(str(d))

        wiz = _cdr_lite_wizard()
        wiz2._populate_from(wiz.config)

        assert wiz2.model_card.cs_source_dd.value == "directive"
        assert wiz2.model_card._carbonate_sensitivity is None
        assert wiz2.model_card.cs_status.value == ""

    @pytest.mark.parametrize("mode", ["marbl", "none"])
    def test_load_back_selects_the_other_bgc_modes_from_cppdefs(self, mode):
        wiz = ForgeBlueprintWizard()
        wiz.run_card.start.value = date(2012, 1, 1)
        wiz.run_card.end.value = date(2012, 1, 2)
        if mode == "none":
            _strip_bgc_forcing(wiz)
            wiz.model_card.bgc_dd.value = "none"
        assert wiz.config is not None, wiz.review_card.derived.value

        wiz2 = ForgeBlueprintWizard()
        wiz2.model_card.bgc_dd.value = "cdr_lite"  # a stale selection must be replaced
        wiz2._populate_from(wiz.config)

        assert wiz2.model_card.bgc_dd.value == mode
        assert _display(wiz2.model_card.cs_group) == "none"
