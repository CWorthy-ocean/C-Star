"""
Tests for ``cstar_forge.forge.settings.write_roms_namelist`` (the ``namelist.nml``
writer) and its MARBL string-list helper / bounds guard.

These exercise the heart of the namelist refactor end-to-end: a populated flat
run-time settings dict is written to ``namelist.nml`` and read back with
``f90nml`` to assert the key renames, per-tracer array expansion, forcing-file
assembly, MARBL string-array emission, and the array-bounds warning.
"""

import warnings
from pathlib import Path

import f90nml
import pytest
import yaml
from cstar.roms.namelist import (
    MARBL_DIAGNOSTICS_TO_WRITE_MAX,
    MARBL_TRACERS_TO_WRITE_MAX,
    _namelist_str_list,
)

import cstar_forge
from cstar_forge.domain_catalog import default_catalog
from cstar_forge.forge.settings import write_roms_namelist
from cstar_forge.forge_blueprint_resolve import load_model_spec_data

_MODEL_DIR = (
    Path(cstar_forge.__file__).parent / "catalog" / "ModelSpec" / "cson_roms-marbl_v0.1"
)


def _base_settings():
    """A complete flat run-time settings dict: the ModelSpec's model_settings
    (physics/numerics defaults) deep-merged with the bundled 'standard' OutputSpec's
    output sections, plus the dynamic fields ``generate_inputs()`` /
    ``_init_settings_run_time`` would populate (title/s_coord/grid/initial/
    output_root_name/time_stepping/v_sponge, and a null-placeholder ``forcing``
    dict ready for the ``nml`` fixture to fill in) -- a complete dict ready for
    ``write_roms_namelist``.
    """
    model = load_model_spec_data(_MODEL_DIR)["model"]
    rt = yaml.safe_load(yaml.safe_dump(model["model_settings"]))  # deep copy
    output = default_catalog.output_data("standard")
    for k, v in output.items():
        if isinstance(v, dict) and isinstance(rt.get(k), dict):
            rt[k].update(v)
        else:
            rt[k] = v
    rt["title"] = {"casename": "test_case"}
    rt["output_root_name"] = {"output_root_name": "/run/out"}
    rt["reference_date_settings"] = {"reference_date": [2000, 1, 1]}
    rt["s_coord"] = {"theta_s": 5.0, "theta_b": 2.0, "tcline": 250.0}
    rt["grid"] = {"grid_file": "/in/grid.nc"}
    rt["initial"] = {"initial_file": "/in/init.nc"}
    rt["time_stepping"] = {"ntimes": 12, "dt": 7200, "ndtfast": 60, "ninfo": 1}
    rt["v_sponge"] = {"v_sponge": 8333.33}
    # param's grid/partitioning dims, tides.ntides, river_frc/cdr_frc, and
    # extract_data are likewise resolver-derived (from Domain/Forcing/a nesting
    # child domain, not a ModelSpec default) -- fill in representative values for
    # this direct-model fixture.
    rt["param"].update({"llm": 512, "mmm": 512, "n": 60, "np_xi": 16, "np_eta": 16})
    rt["tides"]["ntides"] = 15
    rt["extract_data"] = {
        "do_extract": False,
        "extract_file": "sample_edata.nc",
        "nrpf": 24,
        "n_chd": 90,
        "theta_s_chd": 5.0,
        "theta_b_chd": 2.0,
        "hc_chd": 250.0,
        "extract_period": 3600.0,
    }
    rt["river_frc"] = {
        "river_source": False,
        "analytical": False,
        "nriv": 0,
        "rvol_vname": "river_volume",
        "rvol_tname": "river_time",
        "rtrc_vname": "river_tracer",
        "rtrc_tname": "river_time",
    }
    rt["cdr_frc"] = {
        "cdr_source": False,
        "cdr_file": "cdr.nc",
        "ncdr_parm": 1,
        "forcing_depth_profiles": False,
        "forcing_3d": False,
        "forcing_parameterized": True,
        "time_interpolation": False,
        "relocate_to_wet_pts": True,
        "cdr_volume": False,
        "cdrvol_vname": "cdr_volume",
        "cdrvol_tname": "cdr_time",
        "cdrtrc_vname": "cdr_tracer",
        "cdrtrc_tname": "cdr_time",
        "cdrflx_vname": "cdr_trcflx",
        "cdrflx_tname": "cdr_time",
        "cdr_loc_lon": "cdr_lon",
        "cdr_loc_lat": "cdr_lat",
        "cdr_loc_dep": "cdr_dep",
        "cdr_scl_hor": "cdr_hsc",
        "cdr_scl_vrt": "cdr_vsc",
        "nz_chd": 50,
    }
    rt["forcing"] = {
        "surface_forcing_path": None,
        "surface_forcing_bgc_path": None,
        "boundary_forcing_path": None,
        "boundary_forcing_bgc_path": None,
        "tidal_forcing_path": None,
        "river_path": None,
    }
    return rt


def _write_and_read(tmp_path, rt, n_tracers=34):
    write_roms_namelist(settings_run_time=rt, output_dir=tmp_path, n_tracers=n_tracers)
    return f90nml.read(tmp_path / "namelist.nml")


@pytest.fixture
def nml(tmp_path):
    rt = _base_settings()
    rt["forcing"]["surface_forcing_path"] = "/in/surf.nc"
    rt["forcing"]["boundary_forcing_path"] = "/in/bry.nc"
    rt["forcing"]["river_path"] = "/in/river.nc"
    return _write_and_read(tmp_path, rt)


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------
def test_namelist_file_written(tmp_path):
    rt = _base_settings()
    write_roms_namelist(rt, tmp_path, n_tracers=34)
    assert (tmp_path / "namelist.nml").is_file()


def test_core_groups_present(nml):
    # The ``nml`` fixture writes with the default (legacy, pre-0.5.0) schema --
    # ``pio_settings`` is version-gated to ucla-roms >= 0.6.0 (see
    # test_pio_stride_defaults_when_omitted / test_pio_stride_override_is_written
    # below) and must NOT appear here.
    for group in (
        "simulation_name_settings",
        "time_stepping",
        "s_coord",
        "param_settings",
        "initial_conditions",
        "forcing_files",
        "bgc_settings",
        "marbl_biogeochemistry_settings",
    ):
        assert group in nml, f"missing &{group}"
    assert "pio_settings" not in nml


# ---------------------------------------------------------------------------
# Key renames (dict/YAML key -> namelist key)
# ---------------------------------------------------------------------------
def test_key_renames(nml):
    assert nml["s_coord"]["hc"] == 250.0  # tcline -> hc
    assert nml["grid_settings"]["grdname"] == "/in/grid.nc"  # grid_file -> grdname
    assert (
        nml["initial_conditions"]["inifile"] == "/in/init.nc"
    )  # initial_file -> inifile
    assert nml["simulation_name_settings"]["title"] == "test_case"  # casename -> title
    # blk_frc.interp_frc (0) -> interp_bulk_frc (logical False) in merged surf_frc_settings
    assert nml["surf_frc_settings"]["interp_bulk_frc"] is False
    # bgc.interp_frc (0) -> interp_bgc_frc (logical False); wrt_his -> wrt_bgc_his
    assert nml["bgc_settings"]["interp_bgc_frc"] is False
    assert nml["bgc_settings"]["wrt_bgc_his"] is False
    # river_frc.analytical -> river_analytical
    assert nml["river_frc_settings"]["river_analytical"] is False
    # cdr_output.do_cdr_output passes through unrenamed; do_avg -> wrt_cdr_avg
    assert nml["cdr_output_settings"]["do_cdr_output"] is False
    assert nml["cdr_output_settings"]["wrt_cdr_avg"] is True
    # param dims
    assert nml["param_settings"]["np_xi"] == 16
    assert nml["param_settings"]["llm"] == 512


def test_reference_date_round_trips(tmp_path):
    rt = _base_settings()
    rt["reference_date_settings"] = {"reference_date": [2012, 3, 15]}
    nml = _write_and_read(tmp_path, rt)
    assert nml["reference_date_settings"]["reference_date"] == [2012, 3, 15]


def test_code_check_mode_in_stdout_diag_not_diagnostics(tmp_path):
    rt = _base_settings()
    rt["stdout_diag"]["code_check_mode"] = True
    nml = _write_and_read(tmp_path, rt)
    # code_check_mode belongs to &stdout_diag_settings (matches ROMS), and must
    # NOT leak into &diagnostics_settings (ROMS would reject the unknown key).
    assert nml["stdout_diag_settings"]["code_check_mode"] is True
    assert "code_check_mode" not in nml["diagnostics_settings"]


def test_calc_pflx_from_section(nml):
    assert nml["calc_pflx_settings"]["calc_pflx"] is True
    assert nml["calc_pflx_settings"]["pflx_timescale"] == 86400.0


def test_extract_root_name_defaults_when_omitted(nml):
    # ``_base_settings()`` builds ``rt["extract_data"]`` without ``extract_root_name``
    # (representative of a pre-existing settings dict/blueprint) -- the
    # ``ExtractDataCfg`` Pydantic default must still land in the written namelist,
    # since the Fortran declaration has no initializer and requires the key.
    assert nml["extract_data_settings"]["extract_root_name"] == "child"


def test_pio_stride_defaults_when_omitted(tmp_path):
    # &PIO_SETTINGS is version-gated to ucla-roms >= 0.6.0 (RunTimeSettingsV0_6_0).
    # ``_base_settings()`` (built from the cson ModelSpec, which has no
    # ``pio_settings`` key at all -- it predates &PIO_SETTINGS) is representative
    # of a pre-existing settings dict/blueprint pinned forward to 0.6.0 -- the
    # ``PioSettingsCfg`` Pydantic default must still land in the written namelist,
    # since ucla-roms >= 0.6.0 requires the group.
    rt = _base_settings()
    assert "pio_settings" not in rt
    write_roms_namelist(
        settings_run_time=rt, output_dir=tmp_path, n_tracers=34, roms_ref="0.6.0"
    )
    nml = f90nml.read(tmp_path / "namelist.nml")
    assert nml["pio_settings"]["pio_stride"] == 1


def test_pio_stride_override_is_written(tmp_path):
    rt = _base_settings()
    rt["pio_settings"] = {"pio_stride": 4}
    write_roms_namelist(
        settings_run_time=rt, output_dir=tmp_path, n_tracers=34, roms_ref="0.6.0"
    )
    nml = f90nml.read(tmp_path / "namelist.nml")
    assert nml["pio_settings"]["pio_stride"] == 4


def test_pio_settings_ignored_before_0_6_0(tmp_path):
    # ``RunTimeSettings``/``RunTimeSettingsV0_5_0`` inherit ``_SettingsSection``'s
    # ``extra="ignore"`` (not "forbid") at the top level, same as any other
    # unmodeled key -- a settings dict that carries ``pio_settings`` under a
    # pre-0.6.0 pin is silently dropped, not rejected, and the written namelist
    # has no &pio_settings group (the pre-0.6.0 C-Star namelist schemas reject
    # the group outright, so it must never be emitted for them).
    rt = _base_settings()
    rt["pio_settings"] = {"pio_stride": 1}
    write_roms_namelist(
        settings_run_time=rt, output_dir=tmp_path, n_tracers=34, roms_ref="0.5.0"
    )
    nml = f90nml.read(tmp_path / "namelist.nml")
    assert "pio_settings" not in nml


# ---------------------------------------------------------------------------
# Per-tracer array expansion
# ---------------------------------------------------------------------------
def test_per_tracer_arrays_expand_to_n_tracers(tmp_path):
    rt = _base_settings()
    rt["tracer_diff2"]["tnu2_default"] = 1.5
    rt["vertical_mixing"]["akt_default"] = 2.5
    rt["vertical_mixing"]["akv"] = 9.0
    nml = _write_and_read(tmp_path, rt, n_tracers=5)
    assert nml["tracer_diff2"]["tnu2"] == [1.5] * 5
    assert nml["vertical_mixing_settings"]["akt_bak"] == [2.5] * 5
    assert nml["vertical_mixing_settings"]["akv_bak"] == 9.0  # scalar, not expanded


# ---------------------------------------------------------------------------
# Forcing file assembly
# ---------------------------------------------------------------------------
def test_frcfile_canonical_order_non_none(nml):
    # surface, boundary, river set (surface_bgc/boundary_bgc/tidal left None)
    assert nml["forcing_files"]["frcfiles"] == [
        "/in/surf.nc",
        "/in/bry.nc",
        "/in/river.nc",
    ]


def test_frcfile_omitted_when_all_none(tmp_path):
    rt = _base_settings()  # all forcing paths default to null
    nml = _write_and_read(tmp_path, rt)
    assert "frcfiles" not in nml["forcing_files"]


# ---------------------------------------------------------------------------
# MARBL string lists
# ---------------------------------------------------------------------------
def test_marbl_lists_emit_fortran_arrays(tmp_path):
    rt = _base_settings()
    rt["marbl_bgc"]["marbl_tracers_to_write"] = ["DIC", "ALK", "O2"]
    rt["marbl_bgc"]["marbl_diagnostics_to_write"] = ["PH", "FG_CO2"]
    nml = _write_and_read(tmp_path, rt)
    g = nml["marbl_biogeochemistry_settings"]
    assert g["marbl_tracers_to_write"] == ["DIC", "ALK", "O2"]
    assert g["marbl_diagnostics_to_write"] == ["PH", "FG_CO2"]


def test_marbl_empty_list_renders_as_empty_string(tmp_path):
    rt = _base_settings()
    rt["marbl_bgc"]["marbl_tracers_to_write"] = []
    nml = _write_and_read(tmp_path, rt)
    assert nml["marbl_biogeochemistry_settings"]["marbl_tracers_to_write"] == ""


def test_marbl_over_bounds_warns(tmp_path):
    rt = _base_settings()
    rt["marbl_bgc"]["marbl_tracers_to_write"] = [
        f"T{i}" for i in range(MARBL_TRACERS_TO_WRITE_MAX + 1)
    ]
    with pytest.warns(UserWarning, match="marbl_tracers_to_write.*overflow"):
        write_roms_namelist(rt, tmp_path, n_tracers=34)


def test_marbl_within_bounds_does_not_warn(tmp_path):
    rt = _base_settings()
    rt["marbl_bgc"]["marbl_diagnostics_to_write"] = [
        f"D{i}" for i in range(MARBL_DIAGNOSTICS_TO_WRITE_MAX)
    ]
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        write_roms_namelist(rt, tmp_path, n_tracers=34)  # must not raise


# ---------------------------------------------------------------------------
# _namelist_str_list helper (unit)
# ---------------------------------------------------------------------------
def test_str_list_passthrough_list():
    assert _namelist_str_list(["a", "b"]) == ["a", "b"]


def test_str_list_stringifies_elements():
    assert _namelist_str_list([1, 2]) == ["1", "2"]


def test_str_list_empty_and_none_become_empty_string():
    assert _namelist_str_list([]) == ""
    assert _namelist_str_list(None) == ""


def test_str_list_scalar_passthrough():
    assert _namelist_str_list("solo") == "solo"


def test_str_list_warns_over_max_len():
    with pytest.warns(UserWarning, match="overflow"):
        _namelist_str_list(["x"] * 3, max_len=2, name="field_x")


def test_str_list_no_warn_at_max_len(recwarn):
    _namelist_str_list(["x"] * 2, max_len=2, name="field_x")
    assert len(recwarn) == 0


# ---------------------------------------------------------------------------
# roms_ref -- versioned namelist schema selection
# ---------------------------------------------------------------------------
def test_write_roms_namelist_roms050_drops_nrpf_rst_and_renames_particles(tmp_path):
    rt = _base_settings()
    write_roms_namelist(rt, tmp_path, n_tracers=34, roms_ref="0.5.0")
    text = (tmp_path / "namelist.nml").read_text()
    assert "nrpf_rst" not in text
    assert "output_period_particles" in text
    assert "nrpf_particles" in text


def test_write_roms_namelist_none_ref_matches_legacy_ref(tmp_path):
    """``roms_ref=None`` preserves forge's historical (legacy, < 0.5.0) behavior
    -- it must write byte-identical output to an explicit pre-0.5.0 ref.
    """
    rt = _base_settings()
    none_dir = tmp_path / "none_ref"
    legacy_dir = tmp_path / "legacy_ref"
    none_dir.mkdir()
    legacy_dir.mkdir()
    write_roms_namelist(rt, none_dir, n_tracers=34, roms_ref=None)
    write_roms_namelist(rt, legacy_dir, n_tracers=34, roms_ref="0.4.1")
    assert (none_dir / "namelist.nml").read_text() == (
        legacy_dir / "namelist.nml"
    ).read_text()
