"""The Forge wizard keeps what it is given: a blueprint loaded and saved unchanged
writes back the same data, the Download link never hands out provisional open
boundaries, and a typo in a free-text box stops the blueprint instead of
vanishing from it.

Every load and save goes through the real Load and Save handlers.
"""

import base64
import copy
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import xarray as xr
import yaml

from cstar.applications.forge.blueprint import ForgeBlueprint
from cstar.wizard.fields import _dd_values
from cstar.wizard.forcing_editor import _parse_constants, _parse_options
from cstar.wizard.wizard import ForgeBlueprintWizard, _merge_loaded_kwargs

_REPO = Path(__file__).resolve().parents[4]
_WIO_RELEASE = _REPO / "docs/tutorials/forge_blueprint_wio_release.yaml"
_WIO_SPINUP = _REPO / "docs/tutorials/forge_blueprint_wio_spinup.yaml"
_NEST_TINY_CHILD = _REPO / "docs/tutorials/nesting_upscaling_tiny/forge_child.yaml"
_CCS_CHILD = _REPO / "docs/developers/nesting_upscaling_ccs/forge_child.yaml"
_WIO_TOY_EXAMPLE = _REPO / "cstar/catalog/bundled/blueprints/forge/wio-toy-example.yaml"

# Every Forge blueprint the project ships: the docs' and the bundled catalog's.
_SHIPPED_BLUEPRINTS = sorted(
    [
        *(_REPO / "docs").rglob("forge_*.yaml"),
        *(_REPO / "docs/applications/forge").glob("forge-blueprint-*.yaml"),
        *(_REPO / "cstar/catalog/bundled/blueprints/forge").glob("*.yaml"),
    ]
)

# Keys a newer ucla-roms namelist tier added to the ModelSpec after a shipped file
# was written. The wizard writes them at their default, which ROMS also gets when
# the key is absent (an unset cppdef is undefined; the namelist default is False).
# Nothing else may appear.
_ADDED_AT_DEFAULT = {
    "model_settings.cppdefs.diagnostics": False,
    "model_settings.ts_output.wrt_avg_temp": False,
    "model_settings.ts_output.wrt_avg_salt": False,
}


def _flat(d: Any, prefix: str = "") -> dict[str, Any]:
    """``d`` as ``{"a.b[0].c": leaf}``; an empty dict or list is a leaf."""
    out: dict[str, Any] = {}
    if isinstance(d, dict) and d:
        for k, v in d.items():
            out.update(_flat(v, f"{prefix}.{k}" if prefix else str(k)))
    elif isinstance(d, list) and d:
        for i, v in enumerate(d):
            out.update(_flat(v, f"{prefix}[{i}]"))
    else:
        out[prefix] = d
    return out


def _data_diff(before: dict, after: dict) -> list[str]:
    """Every leaf that differs between two blueprint dicts, outside ``provenance``
    and ``composition`` (a save restamps the one and records the wizard's picks in
    the other) and the keys in :data:`_ADDED_AT_DEFAULT`.
    """
    fa, fb = _flat(before), _flat(after)
    lines = []
    for key in sorted(set(fa) | set(fb)):
        if key.split(".")[0] in ("provenance", "composition"):
            continue
        a, b = fa.get(key, "<absent>"), fb.get(key, "<absent>")
        if a == b or (a == "<absent>" and _ADDED_AT_DEFAULT.get(key, a) == b):
            continue
        lines.append(f"{key}: {a!r} -> {b!r}")
    return lines


def _as_written(path: Path) -> dict:
    """The blueprint at ``path`` as a save writes its data (defaults filled in)."""
    return yaml.safe_load(ForgeBlueprint.from_yaml(str(path)).to_yaml_str())


def _load(path: Path) -> ForgeBlueprintWizard:
    """A fresh wizard with ``path`` loaded through the Load button's handler."""
    wiz = ForgeBlueprintWizard()
    wiz.load_path.value = str(path)
    wiz._on_load_path(None)
    assert "Loaded" in wiz.load_status.value, wiz.load_status.value
    assert wiz.config is not None, wiz.derived.value
    return wiz


def _save(wiz: ForgeBlueprintWizard, path: Path) -> dict:
    """Press Save, writing ``path``; return the file it wrote."""
    wiz.save_path.value = str(path)
    wiz._on_save(None)
    assert "Saved" in wiz.save_status.value, wiz.save_status.value
    return yaml.safe_load(path.read_text())


def _downloaded(link_html: str) -> dict | None:
    """The blueprint a download link hands out, or ``None`` when it has none."""
    found = re.search(r"base64,([A-Za-z0-9+/=]+)", link_html)
    return yaml.safe_load(base64.b64decode(found.group(1))) if found else None


def _variant(tmp_path: Path, source: Path, edit) -> Path:
    """A copy of ``source`` changed by ``edit(dict)``, without its provenance."""
    data = yaml.safe_load(source.read_text())
    data.pop("provenance", None)
    edit(data)
    out = tmp_path / f"variant_{source.name}"
    out.write_text(yaml.safe_dump(data, sort_keys=False))
    return out


class TestLoadSaveKeepsTheData:
    def test_the_shipped_blueprints_are_found(self):
        names = {p.relative_to(_REPO).as_posix() for p in _SHIPPED_BLUEPRINTS}
        assert {
            "docs/tutorials/nesting_upscaling_tiny/forge_child.yaml",
            "docs/developers/nesting_upscaling_ccs/forge_child.yaml",
            "cstar/catalog/bundled/blueprints/forge/wio-toy-example.yaml",
        } <= names

    @pytest.mark.parametrize(
        "path",
        _SHIPPED_BLUEPRINTS,
        ids=lambda p: p.relative_to(_REPO).as_posix(),
    )
    def test_a_shipped_blueprint_saves_back_unchanged(self, path, tmp_path):
        saved = _save(_load(path), tmp_path / "saved.yaml")

        assert _data_diff(_as_written(path), saved) == []

    def test_a_loaded_time_of_day_is_kept_and_shown(self, tmp_path):
        wiz = _load(_NEST_TINY_CHILD)

        assert wiz.config.run.start_date == datetime(2012, 1, 2, 6)
        assert "2012-01-02 06:00" in wiz.sticky_bar.value
        saved = _save(wiz, tmp_path / "saved.yaml")
        assert saved["run"]["start_date"] == "2012-01-02T06:00:00"

    def test_a_new_date_starts_at_midnight(self):
        wiz = _load(_NEST_TINY_CHILD)

        wiz.run_card.start.value = date(2012, 1, 1)

        gathered = wiz._gather()
        assert gathered["start_date"] == datetime(2012, 1, 1)
        assert gathered["end_date"] == datetime(2012, 1, 3, 6)  # untouched

    def test_a_parent_keeps_the_grid_kwargs_it_has_no_box_for(self, monkeypatch):
        wiz = _load(_CCS_CHILD)
        monkeypatch.setattr(wiz, "_on_parent_plot", lambda _: None)  # no grid build

        assert wiz.config.domain.grid_kwargs_parent["close_narrow_channels"] is True
        # ... but a parent picked from the catalog replaces the loaded one whole.
        preset = next(v for v in _dd_values(wiz.parent_domain_dd) if v != "<custom>")
        wiz.parent_domain_dd.value = preset
        spec_gk = wiz.catalog.domain_data(preset).get("grid_kwargs") or {}
        assert wiz._nested_grid_kwargs("parent").get(
            "close_narrow_channels"
        ) == spec_gk.get("close_narrow_channels")

    def test_a_grid_widget_edit_still_wins_over_the_loaded_value(self):
        wiz = _load(_NEST_TINY_CHILD)
        assert wiz.config.domain.grid_kwargs["hmin"] == 5.0  # written explicitly

        wiz.hmin.value = 7.5
        assert wiz.config.domain.grid_kwargs["hmin"] == 7.5
        wiz.close_narrow_chk.value = True
        assert wiz.config.domain.grid_kwargs["close_narrow_channels"] is True
        wiz.close_narrow_chk.value = False
        assert "close_narrow_channels" not in wiz.config.domain.grid_kwargs

    def test_prefill_and_extrap_kwargs_are_kept(self, tmp_path):
        def edit(d):
            forcing = d["forcing"]
            forcing["initial_conditions"]["prefill_kwargs"] = {"n_iter": 3}
            forcing["initial_conditions"]["extrap_kwargs"] = {"n_iter": 4}
            forcing["boundary"]["extrap_kwargs"] = {"n_iter": 5}
            forcing["surface"][0]["prefill_kwargs"] = {"n_iter": 6}
            forcing["tidal"][0]["extrap_kwargs"] = {"n_iter": 7}

        path = _variant(tmp_path, _WIO_TOY_EXAMPLE, edit)
        saved = _save(_load(path), tmp_path / "saved.yaml")

        assert _data_diff(_as_written(path), saved) == []
        assert saved["forcing"]["tidal"][0]["extrap_kwargs"] == {"n_iter": 7}

    def test_a_spec_name_missing_from_the_catalog_is_reported(self, tmp_path):
        def edit(d):
            d["composition"]["forcing"]["name"] = "no-such-forcing"

        wiz = _load(_variant(tmp_path, _WIO_SPINUP, edit))

        assert "forcing &#x27;no-such-forcing&#x27;" in wiz.load_status.value

    def test_a_file_that_names_no_spec_is_not_reported(self, tmp_path):
        def edit(d):
            for ref in d["composition"].values():
                if isinstance(ref, dict) and "name" in ref:
                    ref["name"] = ""

        wiz = _load(_variant(tmp_path, _WIO_SPINUP, edit))

        assert "not in this catalog" not in wiz.load_status.value

    def test_a_domain_spec_saved_from_a_loaded_run_keeps_its_times(self):
        data = _load(_NEST_TINY_CHILD)._domain_spec_data()

        assert data["start_time"] == "2012-01-02T06:00:00"
        assert data["end_time"] == "2012-01-03T06:00:00"


def _two_releases(d):
    """A second release beside the first, both with an ALK ramp and a DIC flux."""
    cdr = d["cdr"]["cdr_forcing"]
    first = cdr["releases"][0]
    first["tracer_fluxes"] = {"ALK": [1.0e6, 3.0e6], "DIC": [5.0e5, 5.0e5]}
    second = copy.deepcopy(first)
    second["name"] = "second"
    second["lat"] = first["lat"] + 0.5
    cdr["releases"].append(second)


def _alk_ramp(d):
    d["cdr"]["cdr_forcing"]["releases"][0]["tracer_fluxes"] = {"ALK": [1.0e6, 3.0e6]}


def _dic_only(d):
    d["cdr"]["cdr_forcing"]["releases"][0]["tracer_fluxes"] = {"DIC": [5.0e5, 5.0e5]}


def _release_at_noon(d):
    """The release starts at noon; the CDR window still covers the whole run."""
    d["cdr"]["cdr_forcing"]["releases"][0]["times"][0] = "2012-01-02T12:00:00"


class TestSimpleCdrTheFieldsCannotShow:
    @pytest.mark.parametrize(
        ("edit", "reason"),
        [
            (_two_releases, "2 releases"),
            (_alk_ramp, "a flux that changes in time"),
            (_dic_only, "a flux of DIC"),
            (_release_at_noon, "settings these fields do not set"),
        ],
    )
    def test_is_kept_as_loaded(self, edit, reason, tmp_path):
        path = _variant(tmp_path, _WIO_RELEASE, edit)

        wiz = _load(path)

        assert wiz.run_card.cdr_mode_dd.value == "simple"
        assert reason in wiz.run_card.cdr_simple_note.value
        assert all(w.disabled for w in wiz.run_card.cdr_simple_fields)
        saved = _save(wiz, tmp_path / "saved.yaml")
        assert _data_diff(_as_written(path), saved) == []

    def test_a_release_the_fields_can_show_stays_editable(self):
        wiz = _load(_WIO_RELEASE)

        assert wiz.run_card._cdr_simple_loaded is None
        assert wiz.run_card.cdr_simple_note.value == ""
        assert not any(w.disabled for w in wiz.run_card.cdr_simple_fields)

    def test_a_cdr_lite_release_the_wizard_saved_stays_editable(self, tmp_path):
        wiz = ForgeBlueprintWizard()
        wiz.run_card.start.value = date(2012, 1, 1)
        wiz.run_card.end.value = date(2012, 1, 2)
        wiz._boundaries_touched = True  # not exercising boundary derivation
        wiz.model_card.bgc_dd.value = "cdr_lite"
        wiz.run_card.cdr_mode_dd.value = "simple"
        saved = _save(wiz, tmp_path / "lite.yaml")
        # the resolver names the release's tracer set in the saved file
        assert saved["cdr"]["cdr_forcing"]["releases"][0]["tracer_set"] == "cdr_lite"

        loaded = _load(tmp_path / "lite.yaml")

        assert loaded.run_card._cdr_simple_loaded is None
        assert not any(w.disabled for w in loaded.run_card.cdr_simple_fields)

    def test_edit_as_one_release_replaces_it(self, tmp_path):
        wiz = _load(_variant(tmp_path, _WIO_RELEASE, _two_releases))

        wiz.run_card.cdr_simple_replace_btn.click()

        cdr = wiz.config.cdr.cdr_forcing
        assert [r["tracer_fluxes"] for r in cdr["releases"]] == [
            {"ALK": [1.0e6, 1.0e6]}
        ]
        assert not any(w.disabled for w in wiz.run_card.cdr_simple_fields)

    def test_leaving_simple_mode_drops_it(self, tmp_path):
        wiz = _load(_variant(tmp_path, _WIO_RELEASE, _two_releases))

        wiz.run_card.cdr_mode_dd.value = "none"
        wiz.run_card.cdr_mode_dd.value = "simple"

        assert wiz.run_card._cdr_simple_loaded is None
        assert len(wiz.config.cdr.cdr_forcing["releases"]) == 1


class TestSimpleCdrFollowsTheRun:
    def test_untouched_fields_follow_the_run_window_and_grid(self):
        wiz = ForgeBlueprintWizard()
        wiz.run_card.start.value = date(2012, 1, 1)
        wiz.run_card.end.value = date(2012, 1, 2)
        wiz.run_card.cdr_mode_dd.value = "simple"
        wiz.run_card.cdr_simple_lon.value = 61.25  # the user's own choice

        wiz.run_card.end.value = date(2012, 1, 5)
        wiz.grid_w["center_lat"].value = -3.0
        wiz.grid_w["center_lon"].value = 62.0

        assert wiz.run_card.cdr_simple_end.value == date(2012, 1, 5)
        assert wiz.run_card.cdr_simple_lat.value == -3.0
        assert wiz.run_card.cdr_simple_lon.value == 61.25
        release = wiz.config.cdr.cdr_forcing["releases"][0]
        assert release["times"][-1] == "2012-01-05T00:00:00"

    def test_a_loaded_release_stays_put(self):
        wiz = _load(_WIO_RELEASE)

        wiz.run_card.end.value = date(2012, 1, 9)

        assert wiz.run_card.cdr_simple_end.value == date(2012, 1, 6)


def _open_ocean_grid(nx: int = 20, ny: int = 20):
    """A stand-in for a built grid whose mask is ocean everywhere, so all four
    edges derive as open (the provisional checkboxes open east and north only).
    """
    mask = xr.DataArray(np.ones((ny + 2, nx + 2)), dims=("eta_rho", "xi_rho"))
    return type("Grid", (), {"ds": {"mask_rho": mask}, "size_x": 8000.0, "nx": nx})()


class TestDownloadWaitsForDerivedBoundaries:
    def test_a_fresh_custom_grid_offers_no_download(self):
        wiz = ForgeBlueprintWizard()
        wiz.run_card.start.value = date(2012, 1, 1)
        wiz.run_card.end.value = date(2012, 1, 2)

        assert wiz.config is not None, wiz.derived.value
        assert _downloaded(wiz.download_link.value) is None
        assert _downloaded(wiz.download_link_review.value) is None
        assert "Derive" in wiz.download_link.value

    def test_the_download_never_differs_from_what_save_writes(
        self, tmp_path, monkeypatch
    ):
        wiz = ForgeBlueprintWizard()
        wiz.run_card.start.value = date(2012, 1, 1)
        wiz.run_card.end.value = date(2012, 1, 2)
        monkeypatch.setattr(wiz, "_build_grid_from_widgets", _open_ocean_grid)

        before = _downloaded(wiz.download_link.value)
        saved = _save(wiz, tmp_path / "saved.yaml")

        assert all(saved["domain"]["open_boundaries"].values())  # derived on Save
        if before is not None:
            before.pop("provenance")
        assert before in (None, {k: v for k, v in saved.items() if k != "provenance"})
        assert _downloaded(wiz.download_link.value) == saved

    def test_the_download_after_deriving_is_what_save_writes(
        self, tmp_path, monkeypatch
    ):
        wiz = ForgeBlueprintWizard()
        wiz.run_card.start.value = date(2012, 1, 1)
        wiz.run_card.end.value = date(2012, 1, 2)
        provisional = {d: w.value for d, w in wiz.bnd.items()}
        monkeypatch.setattr(wiz, "_build_grid_from_widgets", _open_ocean_grid)

        wiz.derive_btn.click()
        downloaded = _downloaded(wiz.download_link.value)
        saved = _save(wiz, tmp_path / "saved.yaml")

        derived = dict.fromkeys(("north", "south", "east", "west"), True)
        assert provisional != derived
        assert downloaded["domain"]["open_boundaries"] == derived
        downloaded.pop("provenance")
        saved.pop("provenance")
        assert downloaded == saved


class TestFreeTextIsReadOrRefused:
    def test_constants_name_every_part_they_cannot_read(self):
        with pytest.raises(ValueError, match=r"'B=2x'") as info:
            _parse_constants("A=1, B=2x")
        assert "A=1" not in str(info.value)

        with pytest.raises(ValueError, match=r"'DIC=2100x', 'NO3 10'"):
            _parse_constants("ALK=2300, DIC=2100x, NO3 10")

    def test_constants_skip_blank_parts(self):
        assert _parse_constants("Fe=3e-3,\n, ALK=2300,") == {
            "Fe": 0.003,
            "ALK": 2300.0,
        }

    @pytest.mark.parametrize("text", ["{not json", "[1, 2]", '"a"'])
    def test_options_refuse_anything_but_a_json_object(self, text):
        with pytest.raises(ValueError):
            _parse_options(text)

    def test_options_read_an_object_or_nothing(self):
        assert _parse_options("  ") == {}
        assert _parse_options('{"chunks": {"time": 1}}') == {"chunks": {"time": 1}}

    def test_a_bad_constant_stops_the_blueprint(self, tmp_path):
        wiz = ForgeBlueprintWizard()
        wiz.run_card.start.value = date(2012, 1, 1)
        wiz.run_card.end.value = date(2012, 1, 2)
        row = wiz._forcing_editor._rows["ic_bgc"][0]
        row["name"].value = "constants"
        row["constants"].value = "ALK=2300, DIC=2100x"

        assert wiz.config is None
        assert "BGC initial conditions, row 1, Constant values" in wiz.derived.value
        assert "DIC=2100x" in wiz.derived.value
        wiz.save_path.value = str(tmp_path / "bp.yaml")
        wiz._on_save(None)
        assert not (tmp_path / "bp.yaml").exists()

    def test_a_bad_box_does_not_break_copying_rows_or_switching_bgc(self):
        wiz = ForgeBlueprintWizard()
        wiz.run_card.start.value = date(2012, 1, 1)
        wiz.run_card.end.value = date(2012, 1, 2)
        editor = wiz._forcing_editor
        before = list(editor._rows["boundary_bgc"])
        row = editor._rows["ic_bgc"][0]
        row["name"].value = "constants"
        row["constants"].value = "ALK=oops"

        editor._sync_bgc("ic_bgc", "boundary_bgc")  # "Copy IC BGC" button
        wiz.model_card.bgc_dd.value = "none"

        assert editor._rows["boundary_bgc"] == before  # nothing copied
        assert "ALK=oops" in wiz.derived.value

    def test_bad_options_json_stops_the_blueprint(self):
        wiz = ForgeBlueprintWizard()
        wiz.run_card.start.value = date(2012, 1, 1)
        wiz.run_card.end.value = date(2012, 1, 2)

        wiz._forcing_editor.ic_options.value = '{"chunks": '

        assert wiz.config is None
        assert "Initial conditions, Additional options: invalid JSON" in (
            wiz.derived.value
        )


class TestMergeLoadedKwargs:
    def test_keeps_keys_no_widget_owns_in_loaded_order(self):
        merged = _merge_loaded_kwargs(
            {"verbose": True, "nx": 10, "hmin": 9.0},
            {"nx": 12},
            owned={"nx"},
        )
        assert list(merged.items()) == [("verbose", True), ("nx", 12), ("hmin", 9.0)]

    def test_a_left_out_key_is_kept_only_while_the_widget_shows_it(self):
        left_out = {"hmin": 5.0}
        kept = _merge_loaded_kwargs({"hmin": 5.0}, {}, {"hmin"}, left_out)
        changed = _merge_loaded_kwargs({"hmin": 9.0}, {}, {"hmin"}, left_out)
        assert kept == {"hmin": 5.0}
        assert changed == {}
