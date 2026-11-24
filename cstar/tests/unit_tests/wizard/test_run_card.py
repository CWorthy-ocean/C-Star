"""Tests for the wizard's Run card: the run window, the processor layout (the MPI
tiling, auto tiling and ParallelIO) and the CDR section, with its per-mode panels,
its plot and the netCDF reconstruction behind the plot.
"""

from datetime import date, datetime
from pathlib import Path

import pytest

from cstar.tests.unit_tests.wizard.helpers import (
    _CDR_SAMPLE_YAML,
    _cdr_lite_wizard,
    _descendants,
    _display,
    _fake_cdr_forcing_class,
    _find_card,
    _new_wizard,
    _upload_change,
)
from cstar.wizard.wizard import (
    ForgeBlueprintWizard,
)


def test_use_pio_chk_emit_is_unconditional():
    """The wizard must send an explicit use_pio=False to the resolver when the
    checkbox is unchecked (not simply omit the kwarg) -- otherwise a ModelSpec
    that declares use_pio: true could never be turned off in the UI, since the
    resolver's None fallback re-reads the ModelSpec default.
    """
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    # roms-marbl-0.14-default (the default model) declares use_pio: true --
    # exactly the ModelSpec this test guards against: unchecking must emit an
    # explicit False, not fall back to the ModelSpec default.
    assert wiz.run_card.use_pio_chk.value is True
    wiz.run_card.use_pio_chk.value = False
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["use_pio"] is False
    assert wiz.config.code.pio is None

    wiz.run_card.use_pio_chk.value = True
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["use_pio"] is True
    assert wiz.config.code.pio is not None


def test_auto_tiling_chk_toggles_dependent_widget_state():
    """Checking auto_tiling disables npx/npy, reveals n_cores, and force-locks
    use_pio on; unchecking reverses all of it but leaves use_pio's value as-is.
    """
    wiz = ForgeBlueprintWizard()
    wiz.run_card.use_pio_chk.value = False
    assert wiz.run_card.auto_tiling_chk.value is False
    assert wiz.run_card.npx.disabled is False
    assert wiz.run_card.npy.disabled is False
    assert "none" in (wiz.run_card.n_cores.layout.display or "none")
    assert wiz.run_card.use_pio_chk.disabled is False

    wiz.run_card.auto_tiling_chk.value = True
    assert wiz.run_card.npx.disabled is True
    assert wiz.run_card.npy.disabled is True
    assert (wiz.run_card.n_cores.layout.display or "") == ""
    assert wiz.run_card.use_pio_chk.value is True
    assert wiz.run_card.use_pio_chk.disabled is True

    wiz.run_card.auto_tiling_chk.value = False
    assert wiz.run_card.npx.disabled is False
    assert wiz.run_card.npy.disabled is False
    assert "none" in (wiz.run_card.n_cores.layout.display or "none")
    assert wiz.run_card.use_pio_chk.disabled is False
    assert wiz.run_card.use_pio_chk.value is True  # left as-is, not reset


def test_auto_tiling_toggle_seeds_n_cores_from_n_procs():
    """Checking the auto_tiling box seeds n_cores = npx * npy from the grid
    already entered (or loaded), so the user doesn't multiply by hand; the
    seed only happens on the off->on toggle, so a later manual n_cores edit
    survives unrelated _sync_auto_tiling() calls.
    """
    wiz = ForgeBlueprintWizard()
    wiz.run_card.npx.value = 4
    wiz.run_card.npy.value = 6

    wiz.run_card.auto_tiling_chk.value = True
    assert wiz.run_card.n_cores.value == 24

    # A manual n_cores edit is not clobbered by direct sync calls.
    wiz.run_card.n_cores.value = 30
    wiz.run_card._sync_auto_tiling()
    assert wiz.run_card.n_cores.value == 30

    # Re-toggling recomputes from the (re-enabled, possibly edited) grid.
    wiz.run_card.auto_tiling_chk.value = False
    wiz.run_card.npx.value = 2
    wiz.run_card.auto_tiling_chk.value = True
    assert wiz.run_card.n_cores.value == 12


def test_auto_tiling_gathers_n_cores_partitioning():
    """_gather() (and so the resolved config) swaps to auto_tiling/n_cores
    partitioning when checked, and back to n_procs_x/y when unchecked.
    """
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)

    wiz.run_card.auto_tiling_chk.value = True
    wiz.run_card.n_cores.value = 24
    wiz._rebuild()
    assert wiz.config is not None
    part = wiz.config.domain.partitioning
    assert part.auto_tiling is True
    assert part.n_cores == 24
    assert part.n_procs_x is None
    assert part.n_procs_y is None

    wiz.run_card.auto_tiling_chk.value = False
    wiz._rebuild()
    part = wiz.config.domain.partitioning
    assert part.auto_tiling is False
    assert part.n_procs_x == wiz.run_card.npx.value
    assert part.n_procs_y == wiz.run_card.npy.value


def test_cdr_upload_valid_yaml_gathers_into_config():
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz.run_card.cdr_mode_dd.value = "yaml"

    wiz.run_card._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))

    assert wiz.run_card._cdr_forcing is not None
    assert "✓ CDR" in wiz.run_card.cdr_status.value
    assert wiz._gather()["cdr"]["cdr_forcing"] is wiz.run_card._cdr_forcing
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["cdr_forcing"] is True
    assert wiz.config.cdr.mode == "yaml"
    assert wiz.config.cdr.cdr_forcing["releases"]


def test_cdr_upload_invalid_yaml_surfaces_error_and_does_not_set_config():
    """Not a roms-tools CDRForcing document at all -- caught by
    ``read_cdr_forcing_yaml``'s own parse check, before roms-tools is ever imported.
    """
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)

    wiz.run_card._on_cdr_upload(_upload_change(b"---\nSomeOtherThing:\n  foo: bar\n"))

    assert wiz.run_card._cdr_forcing is None
    assert "invalid" in wiz.run_card.cdr_status.value.lower()
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
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)

    text = _CDR_SAMPLE_YAML.read_text()
    # swap start_time/end_time so parsing succeeds but roms-tools' own
    # `start_time < end_time` validator raises.
    swapped = text.replace(
        "start_time: '2012-01-01T00:00:00'\n  end_time: '2012-01-02T00:00:00'",
        "start_time: '2012-01-02T00:00:00'\n  end_time: '2012-01-01T00:00:00'",
    )
    assert swapped != text, "fixture format changed -- update this test's replace()"

    wiz.run_card._on_cdr_upload(_upload_change(swapped.encode("utf-8")))

    assert wiz.run_card._cdr_forcing is None
    assert "invalid" in wiz.run_card.cdr_status.value.lower()
    assert wiz._gather()["cdr"]["cdr_forcing"] is None


def test_cdr_clear_resets_state():
    wiz = ForgeBlueprintWizard()
    wiz.run_card.cdr_mode_dd.value = "yaml"
    wiz.run_card._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))
    assert wiz.run_card._cdr_forcing is not None

    wiz.run_card._on_cdr_clear(None)

    assert wiz.run_card._cdr_forcing is None
    assert wiz.run_card.cdr_status.value == ""
    assert wiz._gather()["cdr"]["cdr_forcing"] is None


def test_cdr_mode_yaml_with_nothing_uploaded_invalidates_the_blueprint():
    """Selecting "yaml" mode is itself a commitment to supplying a CDR forcing
    -- with nothing uploaded yet, CdrSpec's own validator rejects
    ``cdr_forcing=None``, and that (like any other resolver error) surfaces as
    an Invalid preview rather than silently falling back to "no CDR".
    """
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)

    wiz.run_card.cdr_mode_dd.value = "yaml"

    assert wiz.config is None
    assert "Invalid" in wiz.review_card.derived.value


def test_cdr_forcing_round_trips_through_load(tmp_path):
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz.run_card.cdr_mode_dd.value = "yaml"
    wiz.run_card._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))
    assert wiz.config is not None
    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    assert wiz2.run_card.cdr_mode_dd.value == "yaml"
    assert wiz2.run_card._cdr_forcing is not None

    # cdr_forcing is stored as a plain dict (no typed CDR model, by design -- see the
    # plan's "no typed CDR Pydantic model" note), so a bare YAML timestamp parses to
    # an in-memory datetime but re-serializes to an ISO string on the blueprint's own
    # to_yaml/from_yaml round trip. Compare with that normalized away.
    def _iso(t):
        return t.isoformat() if hasattr(t, "isoformat") else str(t)

    orig = [dict(r) for r in wiz.run_card._cdr_forcing["releases"]]
    back = [dict(r) for r in wiz2.run_card._cdr_forcing["releases"]]
    for r in (orig, back):
        for release in r:
            release["times"] = [_iso(t) for t in release["times"]]
    assert back == orig
    assert "✓ CDR loaded" in wiz2.run_card.cdr_status.value


# ===========================================================================
# CDR Overhaul WP5/WP6: mode dropdown, per-mode panels, plotting widget.
# ===========================================================================


def test_cdr_mode_switch_shows_and_hides_panels():
    wiz = ForgeBlueprintWizard()
    assert _display(wiz.run_card.cdr_simple_box) == "none"
    assert _display(wiz.run_card.cdr_yaml_box) == "none"
    assert _display(wiz.run_card.cdr_netcdf_box) == "none"
    assert _display(wiz.run_card.cdr_upscaled_box) == "none"
    assert _display(wiz.run_card.cdr_plot_box) == "none"

    wiz.run_card.cdr_mode_dd.value = "simple"
    assert _display(wiz.run_card.cdr_simple_box) == ""
    assert _display(wiz.run_card.cdr_yaml_box) == "none"
    assert _display(wiz.run_card.cdr_plot_box) == ""

    wiz.run_card.cdr_mode_dd.value = "yaml"
    assert _display(wiz.run_card.cdr_simple_box) == "none"
    assert _display(wiz.run_card.cdr_yaml_box) == ""
    assert _display(wiz.run_card.cdr_netcdf_box) == "none"
    assert _display(wiz.run_card.cdr_plot_box) == ""

    wiz.run_card.cdr_mode_dd.value = "netcdf"
    assert _display(wiz.run_card.cdr_yaml_box) == "none"
    assert _display(wiz.run_card.cdr_netcdf_box) == ""
    assert _display(wiz.run_card.cdr_plot_box) == ""

    wiz.run_card.cdr_mode_dd.value = "upscaled"
    assert _display(wiz.run_card.cdr_netcdf_box) == "none"
    assert _display(wiz.run_card.cdr_upscaled_box) == ""
    assert _display(wiz.run_card.cdr_plot_box) == "none"  # no plots for upscaled

    wiz.run_card.cdr_mode_dd.value = "none"
    assert _display(wiz.run_card.cdr_upscaled_box) == "none"
    assert _display(wiz.run_card.cdr_plot_box) == "none"


def test_cdr_yaml_and_netcdf_panels_link_to_docs():
    doc_url = "https://roms-tools.readthedocs.io/en/latest/cdr_forcing.html"
    wiz = ForgeBlueprintWizard()
    assert doc_url in wiz.run_card.cdr_yaml_box.children[0].value
    assert doc_url in wiz.run_card.cdr_netcdf_box.children[0].value


def test_cdr_simple_mode_seeds_defaults_on_activation():
    wiz = ForgeBlueprintWizard()
    wiz.domain_card.grid_w["center_lat"].value = 12.5
    wiz.domain_card.grid_w["center_lon"].value = -34.5
    wiz.run_card.start.value = date(2015, 3, 1)
    wiz.run_card.end.value = date(2015, 4, 1)

    wiz.run_card.cdr_mode_dd.value = "simple"

    assert wiz.run_card.cdr_simple_lat.value == 12.5
    assert wiz.run_card.cdr_simple_lon.value == -34.5
    assert wiz.run_card.cdr_simple_start.value == date(2015, 3, 1)
    assert wiz.run_card.cdr_simple_end.value == date(2015, 4, 1)
    assert wiz.run_card.cdr_simple_depth.value == 1.0
    assert wiz.run_card.cdr_simple_hsc.value == 10.0
    assert wiz.run_card.cdr_simple_vsc.value == 10.0
    assert wiz.run_card.cdr_simple_flux.value == 2 * 10**6


def test_cdr_simple_mode_compiles_flat_two_point_pulse():
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 3)
    wiz.run_card.cdr_mode_dd.value = "simple"
    wiz.run_card.cdr_simple_name.value = "my_release"
    wiz.run_card.cdr_simple_lat.value = 10.0
    wiz.run_card.cdr_simple_lon.value = 20.0
    wiz.run_card.cdr_simple_flux.value = 5.0

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
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz.run_card.cdr_mode_dd.value = "simple"
    wiz.run_card.cdr_simple_name.value = "rt_release"
    wiz.run_card.cdr_simple_lat.value = 5.0
    wiz.run_card.cdr_simple_lon.value = 6.0
    wiz.run_card.cdr_simple_depth.value = 2.0
    wiz.run_card.cdr_simple_hsc.value = 15.0
    wiz.run_card.cdr_simple_vsc.value = 25.0
    wiz.run_card.cdr_simple_flux.value = 3.5e6
    assert wiz.config is not None

    wiz2 = ForgeBlueprintWizard()
    wiz2._populate_from(wiz.config)

    assert wiz2.run_card.cdr_mode_dd.value == "simple"
    assert wiz2.run_card.cdr_simple_name.value == "rt_release"
    assert wiz2.run_card.cdr_simple_lat.value == 5.0
    assert wiz2.run_card.cdr_simple_lon.value == 6.0
    assert wiz2.run_card.cdr_simple_depth.value == 2.0
    assert wiz2.run_card.cdr_simple_hsc.value == 15.0
    assert wiz2.run_card.cdr_simple_vsc.value == 25.0
    assert wiz2.run_card.cdr_simple_flux.value == 3.5e6
    assert wiz2.run_card.cdr_simple_start.value == date(2012, 1, 1)
    assert wiz2.run_card.cdr_simple_end.value == date(2012, 1, 2)
    assert wiz2.config.content_hash() == wiz.config.content_hash()


def test_cdr_upscaled_mode_round_trips_through_populate_from():
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz.run_card.cdr_mode_dd.value = "upscaled"
    assert wiz.config is not None
    assert wiz.config.cdr.mode == "upscaled"

    wiz2 = ForgeBlueprintWizard()
    wiz2._populate_from(wiz.config)

    assert wiz2.run_card.cdr_mode_dd.value == "upscaled"
    assert wiz2.run_card._cdr_forcing is None
    assert wiz2.run_card._cdr_forcing_file is None
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
    wiz2.run_card.cdr_dd.value = "prov-cdr"

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

    assert wiz3.run_card.cdr_dd.value == "prov-cdr"
    assert wiz3.config.composition.cdr.modified is False


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
    wiz.run_card.cdr_mode_dd.value = "yaml"
    wiz.run_card._cdr_forcing = {
        "start_time": "2012-01-01T00:00:00",
        "end_time": "2012-01-02T00:00:00",
        "releases": [],
    }
    wiz._rebuild()

    wiz.run_card._on_cdr_plot_generate(None)

    assert len(build_calls) == 1
    assert wiz.run_card._cdr_plot_object is not None
    # The "building…" marker must not stick around after a successful render.
    assert "building" not in wiz.run_card.cdr_plot_status.value
    assert "✓" in wiz.run_card.cdr_plot_status.value
    assert list(wiz.run_card.cdr_plot_release_dd.options) == ["r1", "r2"]
    assert ("Tracer flux (ALK)", "tracer_flux") in wiz.run_card.cdr_plot_type_dd.options
    assert _display(wiz.run_card.cdr_plot_release_dd) == ""  # >1 release -- shown
    assert wiz.run_card.cdr_plot_img.value

    wiz.run_card.cdr_plot_type_dd.value = "distribution"
    assert len(build_calls) == 1  # no rebuild
    assert wiz.run_card.cdr_plot_img.value

    wiz.run_card.cdr_plot_release_dd.value = "r2"
    assert len(build_calls) == 1  # still no rebuild
    assert wiz.run_card.cdr_plot_img.value


def test_cdr_plot_hides_tracer_flux_for_volume_type_forcing(monkeypatch):
    import roms_tools

    fake_cls, _build_calls = _fake_cdr_forcing_class(release_type="volume")
    monkeypatch.setattr(roms_tools, "CDRForcing", fake_cls)

    wiz = ForgeBlueprintWizard()
    wiz.run_card.cdr_mode_dd.value = "yaml"
    wiz.run_card._cdr_forcing = {
        "start_time": "2012-01-01T00:00:00",
        "end_time": "2012-01-02T00:00:00",
        "releases": [],
    }
    wiz._rebuild()

    wiz.run_card._on_cdr_plot_generate(None)

    option_values = dict(wiz.run_card.cdr_plot_type_dd.options).values()
    assert "tracer_flux" not in option_values
    assert "volume-type" in wiz.run_card.cdr_plot_status.value


def test_cdr_forcing_from_netcdf_reconstructs_tracer_perturbation(tmp_path):
    """Integration test (real roms_tools, no mocking) for the module-level
    netcdf-reconstruction helper the plot widget's "netcdf" mode uses.
    """
    import numpy as np
    import roms_tools as rt
    import xarray as xr

    from cstar.wizard.run_card import _cdr_forcing_from_netcdf

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
    from cstar.wizard.run_card import _cdr_forcing_from_netcdf

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

    from cstar.wizard.run_card import _cdr_forcing_from_netcdf

    grid = _small_cdr_grid()
    _, path = _save_cdr_forcing(tmp_path, _cdr_lite_releases(), grid)
    cdr = _cdr_forcing_from_netcdf(path, grid)
    monkeypatch.setitem(cartopy.config, "data_dir", str(tmp_path / "cartopy"))

    cdr.plot_locations()
    for release, tracers in cdr.release_tracers.items():
        cdr.plot_tracer_flux(tracers[0], release_names=[release])


def test_cdr_forcing_from_netcdf_reconstructs_passive_releases(tmp_path):
    from cstar.wizard.run_card import _cdr_forcing_from_netcdf

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
    from cstar.wizard.run_card import _cdr_forcing_from_netcdf

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
    from cstar.wizard.run_card import _cdr_forcing_from_netcdf

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
    from cstar.wizard.run_card import _cdr_forcing_from_netcdf

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

    from cstar.wizard.run_card import _cdr_forcing_from_netcdf

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
    monkeypatch.setattr(wiz.domain_card, "build_grid_from_widgets", lambda: grid)
    wiz.run_card.cdr_mode_dd.value = "netcdf"
    wiz.run_card._attach_cdr_file_from_path(str(path))
    assert "no CDR_* tracers" not in wiz.run_card.cdr_file_status.value

    wiz.run_card._on_cdr_plot_generate(None)

    assert "✓" in wiz.run_card.cdr_plot_status.value, wiz.run_card.cdr_plot_status.value
    assert wiz.run_card.cdr_plot_img.value  # "Release locations" drew
    assert list(wiz.run_card.cdr_plot_release_dd.options) == ["oae", "dor"]
    assert ("Tracer flux", "tracer_flux") in wiz.run_card.cdr_plot_type_dd.options
    assert "Tracer flux (ALK)" not in dict(wiz.run_card.cdr_plot_type_dd.options)

    wiz.run_card.cdr_plot_type_dd.value = "tracer_flux"
    wiz.run_card.cdr_plot_release_dd.value = "dor"

    assert flux_calls == [
        ("CDR_OAE_ALK1", ["oae"]),
        ("CDR_DOR_DIC1", ["dor"]),
    ]
    assert {("tracer_flux", "oae"), ("tracer_flux", "dor")} <= set(
        wiz.run_card._cdr_plot_cache
    )


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
    monkeypatch.setattr(wiz.domain_card, "build_grid_from_widgets", lambda: grid)
    wiz.run_card.cdr_simple_name.value = "pulse"
    wiz.run_card.cdr_simple_lat.value = 55.0
    wiz.run_card.cdr_simple_lon.value = 0.5  # off the coarse grid's coastal cells
    wiz.run_card.cdr_simple_depth.value = 5.0

    wiz.run_card._on_cdr_plot_generate(None)

    assert "✓" in wiz.run_card.cdr_plot_status.value, wiz.run_card.cdr_plot_status.value
    cdr = wiz.run_card._cdr_plot_object
    assert cdr.tracer_set == "cdr_lite"
    assert cdr.release_tracers == {"pulse": ("CDR_OAE_ALK1", "CDR_OAE_DIC1")}
    assert ("Tracer flux", "tracer_flux") in wiz.run_card.cdr_plot_type_dd.options

    wiz.run_card.cdr_plot_type_dd.value = "tracer_flux"

    assert flux_calls == [("CDR_OAE_ALK1", ["pulse"])]
    assert wiz.run_card.cdr_plot_img.value

    # A BGC-mode switch changes what the resolver would generate: drop the plot.
    wiz.model_card.bgc_dd.value = "marbl"
    assert wiz.run_card._cdr_plot_object is None
    wiz.run_card._on_cdr_plot_generate(None)
    assert wiz.run_card._cdr_plot_object.tracer_set == "marbl"
    assert ("Tracer flux (ALK)", "tracer_flux") in wiz.run_card.cdr_plot_type_dd.options


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
    monkeypatch.setattr(wiz.domain_card, "build_grid_from_widgets", lambda: grid)
    wiz.run_card.cdr_mode_dd.value = "netcdf"
    wiz.run_card._attach_cdr_file_from_path(str(path))

    wiz.run_card._on_cdr_plot_generate(None)
    assert "✓" in wiz.run_card.cdr_plot_status.value, wiz.run_card.cdr_plot_status.value
    assert ("Tracer flux", "tracer_flux") in wiz.run_card.cdr_plot_type_dd.options

    wiz.run_card.cdr_plot_type_dd.value = "tracer_flux"  # the first release, "m"
    wiz.run_card.cdr_plot_release_dd.value = "dor"

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
    monkeypatch.setattr(wiz.domain_card, "build_grid_from_widgets", lambda: grid)
    wiz.run_card.cdr_mode_dd.value = "netcdf"
    wiz.run_card._attach_cdr_file_from_path(str(path))
    wiz.run_card._on_cdr_plot_generate(None)
    built = wiz.run_card._cdr_plot_object
    assert built is not None

    wiz.model_card.bgc_dd.value = "marbl"

    assert wiz.run_card._cdr_plot_object is built
    assert wiz.run_card.cdr_plot_img.value


def test_cdr_plot_fingerprint_tolerates_a_selection_the_build_rejects(monkeypatch):
    """A simple-mode volume release is rejected for ``bgc_mode: cdr_lite``: the
    plot's cache check must not raise, and building the plot reports the error.
    """
    wiz = _cdr_lite_wizard()
    forcing = wiz.run_card._simple_cdr_forcing_dict()
    volume = {**forcing["releases"][0], "release_type": "volume"}
    monkeypatch.setattr(
        wiz.run_card,
        "_simple_cdr_forcing_dict",
        lambda: {**forcing, "releases": [volume]},
    )

    wiz.run_card._cdr_plot_inputs_fingerprint()
    wiz.run_card._on_cdr_plot_generate(None)

    assert "volume releases are not supported" in wiz.run_card.cdr_plot_status.value


def test_cdr_forcing_from_netcdf_wraps_errors_with_path(tmp_path):
    from cstar.wizard.run_card import _cdr_forcing_from_netcdf

    path = tmp_path / "not_cdr.nc"
    import xarray as xr

    xr.Dataset({"foo": ("x", [1, 2, 3])}).to_netcdf(path)

    with pytest.raises(ValueError, match="could not reconstruct releases"):
        _cdr_forcing_from_netcdf(path, grid=None)


# ---------------------------------------------------------------------------
# bgc_mode "cdr_lite": the BGC dropdown option, the carbonate sensitivities
# group (cs_*), and the CDR panel's cdr_lite hint.
# ---------------------------------------------------------------------------


def _write_cdr_netcdf_with_tracers(path: Path, tracer_names) -> Path:
    import numpy as np
    import xarray as xr

    ds = xr.Dataset(
        {"cdr_volume": (["ncdr"], np.zeros(3))},
        coords={"tracer_name": ("ntracers", list(tracer_names))},
    )
    ds.to_netcdf(path)
    return path


class TestCdrLiteCdrPanel:
    def test_hint_lives_in_the_run_card_and_shows_only_for_cdr_lite(self):
        wiz = ForgeBlueprintWizard()
        run_card = _find_card(wiz.widget, "run")
        assert wiz.run_card.cdr_lite_hint in _descendants(run_card)

        assert _display(wiz.run_card.cdr_lite_hint) == "none"
        wiz.model_card.bgc_dd.value = "cdr_lite"
        assert _display(wiz.run_card.cdr_lite_hint) == ""
        assert "tracer_set: cdr_lite" in wiz.run_card.cdr_lite_hint.value
        assert "required" in wiz.run_card.cdr_lite_hint.value
        wiz.model_card.bgc_dd.value = "none"
        assert _display(wiz.run_card.cdr_lite_hint) == "none"

    def test_netcdf_without_cdr_tracers_warns_in_cdr_lite_mode(self, tmp_path):
        p = _write_cdr_netcdf_with_tracers(tmp_path / "cdr.nc", ["temp", "salt", "ALK"])
        wiz = _new_wizard()
        wiz.model_card.bgc_dd.value = "cdr_lite"
        wiz.run_card.cdr_mode_dd.value = "netcdf"
        wiz.run_card.cdr_file_path.value = str(p)
        wiz.run_card._on_cdr_file_attach(None)

        assert wiz.run_card._cdr_forcing_file is not None  # a warning, not a block
        assert "no CDR_* tracers" in wiz.run_card.cdr_file_status.value

    def test_netcdf_with_cdr_tracers_does_not_warn(self, tmp_path):
        p = _write_cdr_netcdf_with_tracers(
            tmp_path / "cdr.nc", ["temp", "salt", "CDR_OAE_ALK1", "CDR_OAE_DIC1"]
        )
        wiz = _new_wizard()
        wiz.model_card.bgc_dd.value = "cdr_lite"
        wiz.run_card.cdr_mode_dd.value = "netcdf"
        wiz.run_card.cdr_file_path.value = str(p)
        wiz.run_card._on_cdr_file_attach(None)

        assert "no CDR_* tracers" not in wiz.run_card.cdr_file_status.value

    def test_netcdf_without_cdr_tracers_does_not_warn_outside_cdr_lite(self, tmp_path):
        p = _write_cdr_netcdf_with_tracers(tmp_path / "cdr.nc", ["temp", "salt", "ALK"])
        wiz = _new_wizard()
        wiz.run_card.cdr_mode_dd.value = "netcdf"
        wiz.run_card.cdr_file_path.value = str(p)
        wiz.run_card._on_cdr_file_attach(None)

        assert "no CDR_* tracers" not in wiz.run_card.cdr_file_status.value
