"""
Tests for ``cstar_forge.settings.write_roms_namelist`` (the ``namelist.nml``
writer) and its MARBL string-list helper / bounds guard.

These exercise the heart of the namelist refactor end-to-end: a populated flat
run-time settings dict is written to ``namelist.nml`` and read back with
``f90nml`` to assert the key renames, per-tracer array expansion, forcing-file
assembly, MARBL string-array emission, and the array-bounds warning.
"""
from pathlib import Path
import warnings

import pytest
import yaml
import f90nml

import cstar_forge
from cstar_forge.settings import (
    write_roms_namelist,
    _namelist_str_list,
    MARBL_TRACERS_TO_WRITE_MAX,
    MARBL_DIAGNOSTICS_TO_WRITE_MAX,
)

_TPL = (Path(cstar_forge.__file__).parent / "catalog" / "ModelSpec"
        / "cson_roms-marbl_v0.1" / "templates")


def _base_settings():
    """Load the real model defaults and fill the dynamic fields that
    ``generate_inputs()`` / ``_init_settings_run_time`` would populate, yielding
    a complete (compile_time, run_time) pair ready for ``write_roms_namelist``."""
    ct = yaml.safe_load((_TPL / "compile-time-defaults.yml").read_text())
    rt = yaml.safe_load((_TPL / "run-time-defaults.yml").read_text())
    rt["title"] = {"casename": "test_case"}
    rt["output_root_name"] = {"output_root_name": "/run/out"}
    rt["s_coord"] = {"theta_s": 5.0, "theta_b": 2.0, "tcline": 250.0}
    rt["grid"] = {"grid_file": "/in/grid.nc"}
    rt["initial"] = {"nrrec": 1, "initial_file": "/in/init.nc"}
    return ct, rt


def _write_and_read(tmp_path, ct, rt, n_tracers=34):
    write_roms_namelist(settings_compile_time=ct, settings_run_time=rt,
                        output_dir=tmp_path, n_tracers=n_tracers)
    return f90nml.read(tmp_path / "namelist.nml")


@pytest.fixture
def nml(tmp_path):
    ct, rt = _base_settings()
    rt["forcing"]["surface_forcing_path"] = "/in/surf.nc"
    rt["forcing"]["boundary_forcing_path"] = "/in/bry.nc"
    rt["forcing"]["river_path"] = "/in/river.nc"
    return _write_and_read(tmp_path, ct, rt)


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------
def test_namelist_file_written(tmp_path):
    ct, rt = _base_settings()
    write_roms_namelist(ct, rt, tmp_path, n_tracers=34)
    assert (tmp_path / "namelist.nml").is_file()


def test_core_groups_present(nml):
    for group in ("simulation_name_settings", "time_stepping", "s_coord",
                  "param_settings", "initial_conditions", "forcing_files",
                  "bgc_settings", "marbl_biogeochemistry_settings"):
        assert group in nml, f"missing &{group}"


# ---------------------------------------------------------------------------
# Key renames (dict/YAML key -> namelist key)
# ---------------------------------------------------------------------------
def test_key_renames(nml):
    assert nml["s_coord"]["hc"] == 250.0                       # tcline -> hc
    assert nml["grid_settings"]["grdname"] == "/in/grid.nc"    # grid_file -> grdname
    assert nml["initial_conditions"]["ininame"] == "/in/init.nc"  # initial_file -> ininame
    assert nml["simulation_name_settings"]["title"] == "test_case"  # casename -> title
    # blk_frc.interp_frc (0) -> interp_bulk_frc (logical False)
    assert nml["bulk_frc_settings"]["interp_bulk_frc"] is False
    # bgc.interp_frc (1) -> interp_bgc_frc (logical True); wrt_his -> wrt_bgc_his
    assert nml["bgc_settings"]["interp_bgc_frc"] is True
    assert nml["bgc_settings"]["wrt_bgc_his"] is False
    # river_frc.analytical -> river_analytical
    assert nml["river_frc_settings"]["river_analytical"] is False
    # cdr_output do_cdr/do_avg -> do_cdr_output/wrt_cdr_avg
    assert nml["cdr_output_settings"]["do_cdr_output"] is False
    assert nml["cdr_output_settings"]["wrt_cdr_avg"] is True
    # param case-folding (NP_XI -> np_xi, LLm -> llm)
    assert nml["param_settings"]["np_xi"] == 16
    assert nml["param_settings"]["llm"] == 512


def test_code_check_mode_sourced_from_ocean_vars(tmp_path):
    ct, rt = _base_settings()
    rt["ocean_vars"]["code_check"] = True
    nml = _write_and_read(tmp_path, ct, rt)
    # ocean_vars.code_check feeds diagnostics_settings.code_check_mode
    assert nml["diagnostics_settings"]["code_check_mode"] is True


def test_calc_pflx_from_section(nml):
    assert nml["calc_pflx_settings"]["calc_pflx"] is True
    assert nml["calc_pflx_settings"]["timescale"] == 86400.0


# ---------------------------------------------------------------------------
# Per-tracer array expansion
# ---------------------------------------------------------------------------
def test_per_tracer_arrays_expand_to_n_tracers(tmp_path):
    ct, rt = _base_settings()
    rt["tracer_diff2"]["tnu2_default"] = 1.5
    rt["vertical_mixing"]["akt_default"] = 2.5
    rt["vertical_mixing"]["akv"] = 9.0
    nml = _write_and_read(tmp_path, ct, rt, n_tracers=5)
    assert nml["tracer_diff2"]["tnu2"] == [1.5] * 5
    assert nml["vertical_mixing_settings"]["akt_bak"] == [2.5] * 5
    assert nml["vertical_mixing_settings"]["akv_bak"] == 9.0  # scalar, not expanded


# ---------------------------------------------------------------------------
# Forcing file assembly
# ---------------------------------------------------------------------------
def test_frcfile_canonical_order_non_none(nml):
    # surface, boundary, river set (surface_bgc/boundary_bgc/tidal left None)
    assert nml["forcing_files"]["frcfile"] == [
        "/in/surf.nc", "/in/bry.nc", "/in/river.nc"]


def test_frcfile_omitted_when_all_none(tmp_path):
    ct, rt = _base_settings()  # all forcing paths default to null
    nml = _write_and_read(tmp_path, ct, rt)
    assert "frcfile" not in nml["forcing_files"]


# ---------------------------------------------------------------------------
# MARBL string lists
# ---------------------------------------------------------------------------
def test_marbl_lists_emit_fortran_arrays(tmp_path):
    ct, rt = _base_settings()
    rt["marbl_bgc"]["marbl_tracers_to_write"] = ["DIC", "ALK", "O2"]
    rt["marbl_bgc"]["marbl_diagnostics_to_write"] = ["PH", "FG_CO2"]
    nml = _write_and_read(tmp_path, ct, rt)
    g = nml["marbl_biogeochemistry_settings"]
    assert g["marbl_tracers_to_write"] == ["DIC", "ALK", "O2"]
    assert g["marbl_diagnostics_to_write"] == ["PH", "FG_CO2"]


def test_marbl_empty_list_renders_as_empty_string(tmp_path):
    ct, rt = _base_settings()
    rt["marbl_bgc"]["marbl_tracers_to_write"] = []
    nml = _write_and_read(tmp_path, ct, rt)
    assert nml["marbl_biogeochemistry_settings"]["marbl_tracers_to_write"] == ""


def test_marbl_over_bounds_warns(tmp_path):
    ct, rt = _base_settings()
    rt["marbl_bgc"]["marbl_tracers_to_write"] = [f"T{i}" for i in range(MARBL_TRACERS_TO_WRITE_MAX + 1)]
    with pytest.warns(UserWarning, match="marbl_tracers_to_write.*overflow"):
        write_roms_namelist(ct, rt, tmp_path, n_tracers=34)


def test_marbl_within_bounds_does_not_warn(tmp_path):
    ct, rt = _base_settings()
    rt["marbl_bgc"]["marbl_diagnostics_to_write"] = [f"D{i}" for i in range(MARBL_DIAGNOSTICS_TO_WRITE_MAX)]
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        write_roms_namelist(ct, rt, tmp_path, n_tracers=34)  # must not raise


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
