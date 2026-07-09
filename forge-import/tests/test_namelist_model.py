"""
Tests for the Pydantic namelist/settings models (``cstar_forge.forge.namelist_model``):

* settings dict -> RunTimeSettings -> build_namelist -> write
* the read -> edit -> write round-trip (reusable by other repos)
* validation on bad/incomplete input
* defaults come from the YAML, not the models
"""

from pathlib import Path

import pytest
import yaml
from cstar.roms.namelist import RomsNamelist
from pydantic import ValidationError

import cstar_forge
from cstar_forge.forge.namelist_model import (
    RunTimeSettings,
    build_namelist,
    validate_run_time_sections,
)
from cstar_forge.forge.settings import write_roms_namelist

_TPL = (
    Path(cstar_forge.__file__).parent
    / "catalog"
    / "ModelSpec"
    / "cson_roms-marbl_v0.1"
    / "templates"
)


def _populated_rt_dict():
    rt = yaml.safe_load((_TPL / "run-time-defaults.yml").read_text())
    rt["title"] = {"casename": "spike_case"}
    rt["output_root_name"] = {"output_root_name": "/run/out"}
    rt["reference_date_settings"] = {"reference_date": [2000, 1, 1]}
    rt["s_coord"] = {"theta_s": 5.0, "theta_b": 2.0, "tcline": 250.0}
    rt["grid"] = {"grid_file": "/in/grid.nc"}
    rt["initial"] = {"initial_file": "/in/init.nc"}
    rt["forcing"]["surface_forcing_path"] = "/in/surf.nc"
    rt["forcing"]["boundary_forcing_path"] = "/in/bry.nc"
    rt["forcing"]["river_path"] = "/in/river.nc"
    return rt


def test_settings_dict_validates_into_model():
    rt = RunTimeSettings.model_validate(_populated_rt_dict())
    assert rt.param.ntrc_bio == 32  # coerced int
    assert rt.s_coord.tcline == 250.0
    assert rt.marbl_bgc.marbl_tracers_to_write[:2] == ["PO4", "NO3"]


def test_defaults_come_from_yaml_not_the_model():
    """A ModelSpec with different values yields those values — the model bakes
    in no defaults of its own.
    """
    d = _populated_rt_dict()
    d["param"]["ntrc_bio"] = 18  # a different ModelSpec's value
    d["ocean_vars"]["output_period_rst"] = 12345.0
    rt = RunTimeSettings.model_validate(d)
    assert rt.param.ntrc_bio == 18
    assert rt.ocean_vars.output_period_rst == 12345.0


def test_path_objects_coerced_to_str():
    """Input generation fills grid/initial/forcing with pathlib.Path objects;
    the model coerces them to str rather than rejecting them.
    """
    d = _populated_rt_dict()
    d["grid"]["grid_file"] = Path("/in/grid.nc")
    d["initial"]["initial_file"] = Path("/in/init.nc")
    d["forcing"]["surface_forcing_path"] = Path("/in/surf.nc")
    rt = RunTimeSettings.model_validate(d)
    assert rt.grid.grid_file == "/in/grid.nc" and isinstance(rt.grid.grid_file, str)
    assert rt.initial.initial_file == "/in/init.nc"
    assert rt.forcing.surface_forcing_path == "/in/surf.nc"
    nml = build_namelist(rt, n_tracers=34)
    assert nml.grid_settings.grdname == "/in/grid.nc"
    assert "/in/surf.nc" in nml.forcing_files.frcfiles


def test_incomplete_modelspec_fails_loudly():
    """A YAML missing a required section/key is rejected (no silent default)."""
    missing_section = _populated_rt_dict()
    del missing_section["tides"]
    with pytest.raises(ValidationError, match="tides"):
        RunTimeSettings.model_validate(missing_section)

    missing_key = _populated_rt_dict()
    del missing_key["param"]["ntrc_bio"]
    with pytest.raises(ValidationError, match="ntrc_bio"):
        RunTimeSettings.model_validate(missing_key)


def test_build_and_write_then_read_roundtrip(tmp_path):
    rt = RunTimeSettings.model_validate(_populated_rt_dict())
    nml = build_namelist(rt, n_tracers=34)
    nml.write(tmp_path / "namelist.nml")
    back = RomsNamelist.read(tmp_path / "namelist.nml")
    assert back == nml  # model survives a file round-trip


def test_transform_correctness():
    rt = RunTimeSettings.model_validate(_populated_rt_dict())
    nml = build_namelist(rt, n_tracers=5)
    assert nml.s_coord.hc == 250.0  # tcline -> hc
    assert nml.simulation_name_settings.title == "spike_case"  # casename -> title
    assert nml.param_settings.np_xi == 16  # lowercased in YAML + model
    assert (
        nml.river_frc_settings.river_analytical is False
    )  # analytical -> river_analytical
    assert nml.tracer_diff2.tnu2 == [0.0] * 5  # scalar -> array
    assert nml.forcing_files.frcfiles == ["/in/surf.nc", "/in/bry.nc", "/in/river.nc"]


def test_read_edit_write(tmp_path):
    """The other-repo use case: read a namelist, edit a field, write it back."""
    rt = RunTimeSettings.model_validate(_populated_rt_dict())
    build_namelist(rt, n_tracers=34).write(tmp_path / "namelist.nml")

    nml = RomsNamelist.read(tmp_path / "namelist.nml")
    nml.marbl_biogeochemistry_settings.marbl_tracers_to_write = ["DIC", "ALK", "O2"]
    nml.s_coord.hc = 300.0
    nml.write(tmp_path / "edited.nml")

    reread = RomsNamelist.read(tmp_path / "edited.nml")
    assert reread.marbl_biogeochemistry_settings.marbl_tracers_to_write == [
        "DIC",
        "ALK",
        "O2",
    ]
    assert reread.s_coord.hc == 300.0


def test_validation_rejects_bad_values():
    bad = _populated_rt_dict()
    bad["param"]["np_xi"] = "not-an-int"
    with pytest.raises(ValidationError):
        RunTimeSettings.model_validate(bad)


def test_edit_assignment_is_validated(tmp_path):
    rt = RunTimeSettings.model_validate(_populated_rt_dict())
    nml = build_namelist(rt, n_tracers=34)
    with pytest.raises(ValidationError):
        nml.param_settings.np_xi = "oops"  # validate_assignment catches it


def test_marbl_over_bounds_warns():
    rt = _populated_rt_dict()
    rt["marbl_bgc"]["marbl_tracers_to_write"] = [f"T{i}" for i in range(41)]
    model = RunTimeSettings.model_validate(rt)
    with pytest.warns(UserWarning, match="overflow"):
        build_namelist(model, n_tracers=34)


def test_model_reads_production_namelist(tmp_path):
    """RomsNamelist can ingest a real forge-produced namelist (strict schema)."""
    write_roms_namelist(
        settings_run_time=_populated_rt_dict(), output_dir=tmp_path, n_tracers=34
    )
    nml = RomsNamelist.read(
        tmp_path / "namelist.nml"
    )  # would raise if a group/key is unmodeled
    assert nml.param_settings.nt_bgc == 32
    assert nml.particles_settings.np == 50


# ---------------------------------------------------------------------------
# validate_run_time_sections — partial/per-section validation (fail-fast)
# ---------------------------------------------------------------------------
def test_validate_run_time_sections_accepts_good_partial():
    # only some sections present (as in ForgeBlueprint.model_settings) -> no error
    assert (
        validate_run_time_sections(
            {"time_stepping": {"ntimes": 12, "dt": 7200, "ndtfast": 60, "ninfo": 1}}
        )
        == []
    )
    # scalar fields validate too
    assert validate_run_time_sections({"gamma2": 1.0, "ubind": 0.1}) == []


def test_validate_run_time_sections_skips_non_runtime_keys():
    # cppdefs is a compile-time section, not part of RunTimeSettings -> skipped
    assert (
        validate_run_time_sections({"cppdefs": {"obc_west": True, "whatever": 9}}) == []
    )


def test_validate_run_time_sections_flags_bad_value():
    errs = validate_run_time_sections(
        {
            "param": {
                "np_xi": "not-an-int",
                "np_eta": 1,
                "llm": 6,
                "mmm": 2,
                "n": 3,
                "nsub_x": 1,
                "nsub_e": 1,
                "nt_passive": 0,
                "ntrc_bio": 32,
            }
        }
    )
    assert errs and any("np_xi" in e for e in errs)
