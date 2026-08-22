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
from cstar.roms.namelist import RomsNamelist, RomsNamelistV0_5_0
from pydantic import ValidationError

import cstar_forge
from cstar_forge.domain_catalog import default_catalog
from cstar_forge.forge.namelist_model import (
    RunTimeSettings,
    RunTimeSettingsV0_5_0,
    build_namelist,
    check_extract_divides_rst,
    run_time_settings_for_ref,
    validate_run_time_sections,
)
from cstar_forge.forge.settings import write_roms_namelist
from cstar_forge.forge_blueprint_resolve import load_model_spec_data

_MODEL_DIR = (
    Path(cstar_forge.__file__).parent / "catalog" / "ModelSpec" / "cson_roms-marbl_v0.1"
)


def _populated_rt_dict():
    """A complete flat run-time settings dict: the ModelSpec's model_settings
    (physics/numerics defaults) deep-merged with the bundled 'standard' OutputSpec's
    output sections, plus the "processing-filled" placeholder sections (title/
    s_coord/grid/forcing/initial/output_root_name) that generate_inputs() populates
    dynamically at generation time. These tests exercise RunTimeSettings/
    write_roms_namelist directly (bypassing the resolver), so they need the full
    namelist shape assembled by hand.
    """
    model = load_model_spec_data(_MODEL_DIR)["model"]
    rt = yaml.safe_load(yaml.safe_dump(model["model_settings"]))  # deep copy
    output = default_catalog.output_data("standard")
    for k, v in output.items():
        if isinstance(v, dict) and isinstance(rt.get(k), dict):
            rt[k].update(v)
        else:
            rt[k] = v
    rt["title"] = {"casename": "spike_case"}
    rt["output_root_name"] = {"output_root_name": "/run/out"}
    rt["reference_date_settings"] = {"reference_date": [2000, 1, 1]}
    rt["s_coord"] = {"theta_s": 5.0, "theta_b": 2.0, "tcline": 250.0}
    rt["grid"] = {"grid_file": "/in/grid.nc"}
    rt["initial"] = {"initial_file": "/in/init.nc"}
    # time_stepping/v_sponge are always resolver-derived (from dt/run-window and
    # grid spacing respectively), so they're intentionally absent from ModelSpec's
    # model_settings -- fill in representative values for these direct-model tests.
    rt["time_stepping"] = {"ntimes": 12, "dt": 7200, "ndtfast": 60, "ninfo": 1}
    rt["v_sponge"] = {"v_sponge": 8333.33}
    # param's grid/partitioning dims, tides.ntides, river_frc/cdr_frc, and
    # extract_data are likewise resolver-derived (from Domain/Forcing/a nesting
    # child domain, not a ModelSpec default) -- fill in representative values for
    # these direct-model tests.
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
        "surface_forcing_path": "/in/surf.nc",
        "surface_forcing_bgc_path": None,
        "boundary_forcing_path": "/in/bry.nc",
        "boundary_forcing_bgc_path": None,
        "tidal_forcing_path": None,
        "river_path": "/in/river.nc",
    }
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
    # Must stay an integer multiple of time_stepping.dt (7200) -- see
    # _rst_period_divisible_by_dt -- so this exercises "a different value" without
    # tripping the restart-period validator.
    d["ocean_vars"]["output_period_rst"] = 21600.0
    rt = RunTimeSettings.model_validate(d)
    assert rt.param.ntrc_bio == 18
    assert rt.ocean_vars.output_period_rst == 21600.0


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


def test_rst_period_not_divisible_by_dt_rejected():
    bad = _populated_rt_dict()
    bad["time_stepping"]["dt"] = 100.0
    bad["ocean_vars"]["output_period_rst"] = 150.0
    with pytest.raises(ValidationError, match="output_period_rst"):
        RunTimeSettings.model_validate(bad)


def test_rst_period_divisible_by_dt_accepted():
    good = _populated_rt_dict()
    good["time_stepping"]["dt"] = 100.0
    good["ocean_vars"]["output_period_rst"] = 200.0
    rt = RunTimeSettings.model_validate(good)
    assert rt.ocean_vars.output_period_rst == 200.0


def test_rst_period_not_divisible_accepted_with_monthly_restarts():
    d = _populated_rt_dict()
    d["time_stepping"]["dt"] = 100.0
    d["ocean_vars"]["output_period_rst"] = 150.0
    d["ocean_vars"]["monthly_restarts"] = True
    rt = RunTimeSettings.model_validate(d)
    assert rt.ocean_vars.output_period_rst == 150.0


def test_rst_period_not_divisible_accepted_with_rst_writing_off():
    d = _populated_rt_dict()
    d["time_stepping"]["dt"] = 100.0
    d["ocean_vars"]["output_period_rst"] = 150.0
    d["ocean_vars"]["wrt_file_rst"] = False
    rt = RunTimeSettings.model_validate(d)
    assert rt.ocean_vars.output_period_rst == 150.0


# --- check_extract_divides_rst (mirrors ucla-roms >= 0.5.0's precheck for the
#     nesting extract stream) ------------------------------------------------
_EXTRACT_OK = {"do_extract": True, "nrpf": 24, "extract_period": 3600.0}
_RST_ON = {"wrt_file_rst": True, "output_period_rst": 86400.0}


def test_extract_divides_rst_accepted():
    check_extract_divides_rst(_RST_ON, _EXTRACT_OK)  # 24 * 3600 == 86400


def test_extract_not_dividing_rst_rejected():
    bad = {**_EXTRACT_OK, "extract_period": 5000.0}  # 24 * 5000 = 120000
    with pytest.raises(ValueError, match="evenly divide"):
        check_extract_divides_rst(_RST_ON, bad)


def test_extract_nonpositive_frequency_rejected():
    with pytest.raises(ValueError, match="must be positive"):
        check_extract_divides_rst(_RST_ON, {**_EXTRACT_OK, "nrpf": 0})


def test_extract_check_skipped_when_extract_or_rst_off():
    bad = {**_EXTRACT_OK, "extract_period": 5000.0}
    check_extract_divides_rst({**_RST_ON, "wrt_file_rst": False}, bad)
    check_extract_divides_rst(_RST_ON, {**bad, "do_extract": False})


def test_extract_check_vacuous_for_zero_rst_period():
    """The monthly-restart convention (output_period_rst = 0) passes trivially,
    mirroring the Fortran ``mod(0, x) == 0``.
    """
    bad = {**_EXTRACT_OK, "extract_period": 5000.0}
    check_extract_divides_rst({**_RST_ON, "output_period_rst": 0.0}, bad)


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
    # extract_root_name has no Fortran initializer (mandatory in every emitted
    # namelist); _populated_rt_dict()'s extract_data omits it, so this exercises
    # the Forge-writes -> C-Star-reads default contract end to end.
    assert nml.extract_data_settings.extract_root_name == "child"


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


def _rst_period_sections(
    dt: float,
    output_period_rst: float,
    monthly_restarts: bool = False,
    wrt_file_rst: bool = True,
) -> dict:
    """Full, otherwise-valid time_stepping/ocean_vars sections (so the per-section
    TypeAdapter pass in validate_run_time_sections stays silent), with only the
    restart-period-relevant leaves overridden -- isolates the cross-section check.
    """
    rt = _populated_rt_dict()
    time_stepping = dict(rt["time_stepping"])
    time_stepping["dt"] = dt
    ocean_vars = dict(rt["ocean_vars"])
    ocean_vars["output_period_rst"] = output_period_rst
    ocean_vars["monthly_restarts"] = monthly_restarts
    ocean_vars["wrt_file_rst"] = wrt_file_rst
    return {"time_stepping": time_stepping, "ocean_vars": ocean_vars}


def test_validate_run_time_sections_flags_non_divisible_rst_period():
    sections = _rst_period_sections(dt=100.0, output_period_rst=150.0)
    errs = validate_run_time_sections(sections)
    assert errs and any("output_period_rst" in e for e in errs)


def test_validate_run_time_sections_accepts_divisible_rst_period():
    sections = _rst_period_sections(dt=100.0, output_period_rst=200.0)
    assert validate_run_time_sections(sections) == []


def test_validate_run_time_sections_ignores_rst_period_when_monthly():
    sections = _rst_period_sections(
        dt=100.0, output_period_rst=150.0, monthly_restarts=True
    )
    assert validate_run_time_sections(sections) == []


def test_validate_run_time_sections_skips_rst_period_check_when_section_missing():
    """Only one of the two sections present -> the cross-section check can't run
    (and must not crash), regardless of how invalid the missing pairing would be.
    """
    sections = _rst_period_sections(dt=100.0, output_period_rst=150.0)
    assert (
        validate_run_time_sections({"time_stepping": sections["time_stepping"]}) == []
    )
    assert validate_run_time_sections({"ocean_vars": sections["ocean_vars"]}) == []


# ---------------------------------------------------------------------------
# run_time_settings_for_ref -- schema-variant selection by ucla-roms ref
# ---------------------------------------------------------------------------
def test_run_time_settings_for_ref_none_and_pre_0_5_0_select_legacy():
    assert run_time_settings_for_ref(None) is RunTimeSettings
    for ref in ("0.4.1", "v0.4.9", "0.2.0"):
        assert run_time_settings_for_ref(ref) is RunTimeSettings


def test_run_time_settings_for_ref_0_5_0_and_later_select_v0_5_0():
    for ref in ("0.5.0", "v0.5.0", "0.7.3"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_5_0


def test_run_time_settings_for_ref_branch_warns_and_uses_latest():
    with pytest.warns(UserWarning, match="not a release tag"):
        cls = run_time_settings_for_ref("main")
    assert cls is RunTimeSettingsV0_5_0


def test_run_time_settings_for_ref_unresolvable_hash_warns_and_uses_latest():
    """A commit hash needs ``repo_path`` (not passed here) to resolve to a
    release tag, so it falls back the same way an unparseable/branch ref does.
    """
    with pytest.warns(UserWarning, match="not a release tag"):
        cls = run_time_settings_for_ref("a1b2c3d4")
    assert cls is RunTimeSettingsV0_5_0


def test_run_time_settings_for_ref_empty_string_selects_legacy():
    """`""` means "no ref" (callers pass ``commit or branch``, and a hand-edited
    blueprint can carry ``commit: null`` + ``branch: ""``) -- it must select the
    legacy schema like ``None``, not fall through to "latest" like an
    unparseable ref.
    """
    assert run_time_settings_for_ref("") is RunTimeSettings


def test_run_time_settings_for_ref_unknown_schema_raises_actionable_error(
    monkeypatch,
):
    """A C-Star (installed from its main branch) can grow a new namelist schema
    before forge maps it; the selector must fail with an actionable message,
    not a bare ``KeyError``.
    """

    class _FutureSchema:
        pass

    monkeypatch.setattr(
        "cstar_forge.forge.namelist_model.namelist_schema_for_ref",
        lambda ref: _FutureSchema,
    )
    with pytest.raises(ValueError, match="no matching\\s+run-time settings model"):
        run_time_settings_for_ref("9.9.9")


# ---------------------------------------------------------------------------
# build_namelist -- RunTimeSettingsV0_5_0 / RomsNamelistV0_5_0 dispatch
# ---------------------------------------------------------------------------
def test_build_namelist_v0_5_0_drops_nrpf_rst_and_renames_particles(tmp_path):
    d = _populated_rt_dict()
    rt = RunTimeSettingsV0_5_0.model_validate(d)
    nml = build_namelist(rt, n_tracers=34)
    assert type(nml) is RomsNamelistV0_5_0

    basic_output = nml.basic_output_settings.model_dump()
    assert "nrpf_rst" not in basic_output

    particles = nml.particles_settings.model_dump()
    assert "output_period" not in particles
    assert "nrpf" not in particles
    assert particles["output_period_particles"] == d["particles"]["output_period"]
    assert particles["nrpf_particles"] == d["particles"]["nrpf"]

    nml.write(tmp_path / "namelist.nml")
    text = (tmp_path / "namelist.nml").read_text()
    assert "nrpf_rst" not in text
    assert "output_period_particles" in text
    assert "nrpf_particles" in text


def test_build_namelist_legacy_keeps_nrpf_rst_and_particles_keys():
    """Regression: the same settings dict through the legacy schema still yields
    ``nrpf_rst`` and the un-renamed particles keys.
    """
    rt = RunTimeSettings.model_validate(_populated_rt_dict())
    nml = build_namelist(rt, n_tracers=34)
    assert type(nml) is RomsNamelist

    basic_output = nml.basic_output_settings.model_dump()
    assert "nrpf_rst" in basic_output

    particles = nml.particles_settings.model_dump()
    assert "output_period" in particles
    assert "nrpf" in particles
    assert "output_period_particles" not in particles
    assert "nrpf_particles" not in particles


# ---------------------------------------------------------------------------
# validate_run_time_sections -- roms_ref-selected schema variant
# ---------------------------------------------------------------------------
def test_validate_run_time_sections_roms050_ignores_stray_nrpf_rst():
    """``ocean_vars`` from a full settings dict carries ``nrpf_rst`` (the shared
    'standard' OutputSpec always sets it) -- against the >= 0.5.0 schema, that's
    an unmodeled key silently dropped by ``extra="ignore"``, not an error.
    """
    d = _populated_rt_dict()
    sections = {"time_stepping": d["time_stepping"], "ocean_vars": d["ocean_vars"]}
    assert validate_run_time_sections(sections, roms_ref="0.5.0") == []


def test_validate_run_time_sections_roms050_flags_bad_value():
    d = _populated_rt_dict()
    bad_ocean_vars = dict(d["ocean_vars"])
    bad_ocean_vars["wrt_file_rst"] = "notabool"
    errs = validate_run_time_sections({"ocean_vars": bad_ocean_vars}, roms_ref="0.5.0")
    assert errs and any("wrt_file_rst" in e for e in errs)


# ---------------------------------------------------------------------------
# _rst_period_divisible_by_dt -- enforced on both schema variants
# ---------------------------------------------------------------------------
def test_rst_period_not_divisible_by_dt_rejected_v0_5_0():
    bad = _populated_rt_dict()
    bad["time_stepping"]["dt"] = 100.0
    bad["ocean_vars"]["output_period_rst"] = 150.0
    with pytest.raises(ValidationError, match="output_period_rst"):
        RunTimeSettingsV0_5_0.model_validate(bad)
