"""
Tests for the Pydantic namelist/settings models (``cstar.applications.forge.namelist_model``):

* settings dict -> RunTimeSettings -> build_namelist -> write
* the read -> edit -> write round-trip (reusable by other repos)
* validation on bad/incomplete input
* defaults come from the YAML, not the models
"""

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

import cstar.catalog
from cstar.applications.forge.namelist_model import (
    _GATED_SECTION_SWITCHES,
    _PRECHECK_SECTION_MAP,
    CDR_LITE_MODE_MIN_ROMS,
    CdrGasExchOutputCfg,
    CdrLiteOutputCfg,
    CdrLiteOutputCfgV0_9_0,
    CdrTracerCounts,
    RunTimeSettings,
    RunTimeSettingsV0_4_0,
    RunTimeSettingsV0_5_0,
    RunTimeSettingsV0_6_0,
    RunTimeSettingsV0_7_0,
    RunTimeSettingsV0_9_0,
    bgc_mode_from_cppdefs,
    build_namelist,
    canonical_output_sections_for_precheck,
    cdr_tracer_counts,
    check_bgc_tracer_count,
    check_cdr_lite_mode_roms,
    check_cdr_lite_sections,
    check_cdr_output_sections,
    forge_field_for,
    n_tracers_from_param,
    normalize_legacy_sections,
    output_precheck_applies_to,
    prune_version_gated_sections,
    run_time_settings_for_ref,
    validate_run_time_sections,
)
from cstar.applications.forge.resolve import load_model_spec_data
from cstar.applications.forge.settings import write_roms_namelist
from cstar.catalog.domain_catalog import default_catalog
from cstar.roms.namelist import (
    RomsNamelist,
    RomsNamelistV0_5_0,
    RomsNamelistV0_7_0,
    RomsNamelistV0_9_0,
)

_MODEL_DIR = (
    Path(cstar.catalog.__file__).parent
    / "bundled"
    / "ModelSpec"
    / "cson_roms-marbl_v0.1"
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


# ---------------------------------------------------------------------------
# param.nt_cdr_oae / param.nt_cdr_dor -- gated to ucla-roms >= 0.4.0
# ---------------------------------------------------------------------------
def test_legacy_param_cfg_rejects_non_zero_cdr_tracer_counts():
    """`RunTimeSettings` (< 0.4.0)'s `param` rejects a non-zero CDR tracer
    count instead of silently dropping it (`extra="ignore"` would otherwise
    let it through uncounted by `n_tracers_from_param`).
    """
    d = _populated_rt_dict()
    d["param"]["nt_cdr_oae"] = 1
    with pytest.raises(ValidationError, match="ucla-roms >= 0.4.0"):
        RunTimeSettings.model_validate(d)

    d = _populated_rt_dict()
    d["param"]["nt_cdr_dor"] = 1
    with pytest.raises(ValidationError, match="ucla-roms >= 0.4.0"):
        RunTimeSettings.model_validate(d)


def test_legacy_param_cfg_accepts_zero_cdr_tracer_counts():
    """An explicit `0` for either key is accepted (it changes nothing)."""
    d = _populated_rt_dict()
    d["param"]["nt_cdr_oae"] = 0
    d["param"]["nt_cdr_dor"] = "0"  # a hand-edited string zero is still zero
    rt = RunTimeSettings.model_validate(d)
    assert rt.param.ntrc_bio == 32


def test_run_time_settings_v0_4_0_defaults_cdr_tracer_counts_to_zero():
    """`RunTimeSettingsV0_4_0.param` (`ParamCfgV0_4_0`) defaults both CDR
    tracer counts to 0 when the settings dict omits them (a blueprint saved
    before the keys existed bypasses the resolver and hits this directly).
    """
    rt = RunTimeSettingsV0_4_0.model_validate(_populated_rt_dict())
    assert rt.param.nt_cdr_oae == 0
    assert rt.param.nt_cdr_dor == 0


def test_run_time_settings_v0_4_0_accepts_non_zero_cdr_tracer_counts():
    d = _populated_rt_dict()
    d["param"]["nt_cdr_oae"] = 2
    d["param"]["nt_cdr_dor"] = 1
    rt = RunTimeSettingsV0_4_0.model_validate(d)
    assert rt.param.nt_cdr_oae == 2
    assert rt.param.nt_cdr_dor == 1


# ---------------------------------------------------------------------------
# n_tracers_from_param
# ---------------------------------------------------------------------------
def test_n_tracers_from_param_counts_all_tracer_kinds():
    # T + S (2) + ntrc_bio (32) + nt_passive (3) + 2*nt_cdr_oae (2*2) + nt_cdr_dor (1)
    param = {"ntrc_bio": 32, "nt_passive": 3, "nt_cdr_oae": 2, "nt_cdr_dor": 1}
    assert n_tracers_from_param(param) == 2 + 32 + 3 + 2 * 2 + 1


def test_n_tracers_from_param_missing_keys_count_as_zero():
    assert n_tracers_from_param({}) == 2
    assert n_tracers_from_param({"ntrc_bio": 32}) == 34


# ---------------------------------------------------------------------------
# check_bgc_tracer_count
# ---------------------------------------------------------------------------
def test_check_bgc_tracer_count_rejects_bgc_tracers_without_marbl():
    with pytest.raises(ValueError, match="param.ntrc_bio=32"):
        check_bgc_tracer_count({"ntrc_bio": 32}, bgc_mode_is_marbl=False)


@pytest.mark.parametrize(
    ("param", "bgc_mode_is_marbl"),
    [
        ({"ntrc_bio": 32}, True),
        ({"ntrc_bio": 0}, False),
        ({}, False),
    ],
)
def test_check_bgc_tracer_count_accepts_consistent_settings(param, bgc_mode_is_marbl):
    check_bgc_tracer_count(param, bgc_mode_is_marbl=bgc_mode_is_marbl)


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


def test_pio_settings_default_stride():
    # &PIO_SETTINGS is version-gated to ucla-roms >= 0.6.0 -- ``_populated_rt_dict()``
    # (built from the cson ModelSpec, which predates &PIO_SETTINGS) has no
    # ``pio_settings`` key at all, representative of a pre-existing settings
    # dict/blueprint pinned forward to 0.6.0.
    d = _populated_rt_dict()
    assert "pio_settings" not in d
    rt = RunTimeSettingsV0_6_0.model_validate(d)
    nml = build_namelist(rt, n_tracers=34)
    assert nml.pio_settings.pio_stride == 1


def test_cdr_lite_gas_exch_output_defaults_when_omitted():
    # &CDR_TRACER_OUTPUT_SETTINGS/&CDR_GAS_EXCH_OUTPUT_SETTINGS are version-gated
    # to ucla-roms >= 0.7.0 (PR #351). Unlike pio_settings (a ModelSpec physics
    # section), these two live in the shared "standard" OutputSpec that
    # ``_populated_rt_dict()`` merges in, so simulate a settings dict/blueprint
    # saved before OutputSpecs grew these sections by dropping them explicitly.
    d = _populated_rt_dict()
    del d["cdr_lite_output"]
    del d["cdr_gas_exch_output"]
    rt = RunTimeSettingsV0_7_0.model_validate(d)
    nml = build_namelist(rt, n_tracers=34)
    assert nml.cdr_tracer_output_settings.do_cdr_tracer_output is False
    assert nml.cdr_tracer_output_settings.nrpf_cdr_trc == 4
    assert nml.cdr_gas_exch_output_settings.do_cdr_gas_exch_output is False
    assert nml.cdr_gas_exch_output_settings.nrpf_cdr_gas == 4


def test_cdr_lite_output_cfg_defaults_and_aliases():
    # ucla-roms 0.7/0.8 tier: forge's do_cdr_lite_output & co. serialize to the
    # &CDR_TRACER_OUTPUT_SETTINGS names.
    dumped = CdrLiteOutputCfg().model_dump(by_alias=True)
    assert dumped == {
        "do_cdr_tracer_output": False,
        "wrt_cdr_trc_avg": True,
        "cdr_trc_monthly_averages": False,
        "output_period_cdr_trc": 3600.0,
        "nrpf_cdr_trc": 4,
        "wrt_tracers": True,
        "wrt_vertical_integrals": True,
        "wrt_thickness_weighted": True,
        "wrt_sources": True,
        "wrt_alk": True,
        "wrt_dic": True,
    }


def test_cdr_lite_output_cfg_v0_9_0_defaults_and_aliases():
    # ucla-roms >= 0.9.0 tier: the &CDR_LITE_OUTPUT_SETTINGS names, plus
    # wrt_gas_exchange.
    dumped = CdrLiteOutputCfgV0_9_0().model_dump(by_alias=True)
    assert dumped == {
        "do_cdr_lite_output": False,
        "wrt_cdr_lite_avg": True,
        "cdr_lite_monthly_averages": False,
        "output_period_cdr_lite": 3600.0,
        "nrpf_cdr_lite": 4,
        "wrt_tracers": True,
        "wrt_vertical_integrals": True,
        "wrt_thickness_weighted": True,
        "wrt_sources": True,
        "wrt_alk": True,
        "wrt_dic": True,
        "wrt_gas_exchange": False,
    }


def test_cdr_gas_exch_output_cfg_defaults_and_aliases():
    dumped = CdrGasExchOutputCfg().model_dump(by_alias=True)
    assert dumped == {
        "do_cdr_gas_exch_output": False,
        "wrt_cdr_gas_avg": True,
        "cdr_gas_monthly_averages": False,
        "output_period_cdr_gas": 3600.0,
        "nrpf_cdr_gas": 4,
    }


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


def test_pio_stride_zero_rejected():
    bad = _populated_rt_dict()
    bad["pio_settings"] = {"pio_stride": 0}
    with pytest.raises(ValidationError, match="pio_stride"):
        RunTimeSettingsV0_6_0.model_validate(bad)


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


_PARAM = {"llm": 20, "mmm": 20, "n": 10, "np_xi": 2, "np_eta": 5, "nt_passive": 0}


@pytest.mark.parametrize(
    ("ntrc_bio", "marbl", "rejected"),
    [(32, False, True), (0, False, False), (32, True, False)],
)
def test_validate_run_time_sections_checks_bgc_tracers_against_marbl(
    ntrc_bio, marbl, rejected
):
    """A stored blueprint with MARBL off but BGC tracers left in param is
    reported up front (engine/wizard), not first at configure_build.
    """
    errs = validate_run_time_sections(
        {"param": {**_PARAM, "ntrc_bio": ntrc_bio}, "cppdefs": {"marbl": marbl}}
    )
    assert any("param.ntrc_bio" in e for e in errs) is rejected


def test_validate_run_time_sections_skips_bgc_check_without_cppdefs():
    assert validate_run_time_sections({"param": {**_PARAM, "ntrc_bio": 32}}) == []


@pytest.mark.parametrize(
    ("cppdefs", "roms_ref", "rejected"),
    [
        ({"cdr_lite": True, "marbl": False}, "0.9.0", True),
        ({"cdr_lite": True, "marbl": False}, "0.9.1", False),
        ({"cdr_lite": True, "marbl": False}, None, False),
        # online sensitivities on a MARBL build are not the CDR-lite mode
        ({"cdr_lite": True, "marbl": True}, "0.9.0", False),
        ({"marbl": False}, "0.9.0", False),
    ],
)
def test_validate_run_time_sections_checks_cdr_lite_mode_roms_pin(
    cppdefs, roms_ref, rejected
):
    """A stored cdr_lite blueprint pinned below the minimum ucla-roms is reported
    up front (engine/wizard), not first at configure_build.
    """
    errs = validate_run_time_sections({"cppdefs": cppdefs}, roms_ref=roms_ref)
    pinned_too_old = [e for e in errs if "CDR-lite without MARBL needs" in e]
    assert bool(pinned_too_old) is rejected


# ---------------------------------------------------------------------------
# run_time_settings_for_ref -- schema-variant selection by ucla-roms ref
# ---------------------------------------------------------------------------
def test_run_time_settings_for_ref_none_and_pre_0_4_0_select_legacy():
    assert run_time_settings_for_ref(None) is RunTimeSettings
    for ref in ("0.2.0", "0.3.9"):
        assert run_time_settings_for_ref(ref) is RunTimeSettings


def test_run_time_settings_for_ref_0_4_0_up_to_0_5_0_selects_v0_4_0():
    for ref in ("0.4.0", "0.4.1", "v0.4.9"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_4_0


def test_run_time_settings_for_ref_0_5_0_up_to_0_6_0_selects_v0_5_0():
    # 0.5.0 <= ucla-roms < 0.6.0 selects RunTimeSettingsV0_5_0; 0.6.0 <=
    # ucla-roms < 0.7.0 selects RunTimeSettingsV0_6_0; 0.7.0 <= ucla-roms < 0.9.0
    # selects RunTimeSettingsV0_7_0 and 0.9.0 and later (including anything
    # beyond) RunTimeSettingsV0_9_0 -- see the tests below.
    for ref in ("0.5.0", "v0.5.0"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_5_0


def test_run_time_settings_for_ref_0_6_0_up_to_0_7_0_selects_v0_6_0():
    # 0.6.0 <= ucla-roms < 0.7.0 selects RunTimeSettingsV0_6_0; 0.7.0 and later
    # select RunTimeSettingsV0_7_0/V0_9_0 -- see the tests below.
    for ref in ("0.6.0", "v0.6.0"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_6_0


def test_run_time_settings_for_ref_0_7_0_up_to_0_9_0_selects_v0_7_0():
    for ref in ("0.7.0", "v0.7.0", "0.8.3"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_7_0


def test_run_time_settings_for_ref_0_9_0_and_later_selects_v0_9_0():
    for ref in ("0.9.0", "v0.9.0", "0.10.2", "1.0.0"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_9_0


def test_run_time_settings_for_ref_branch_warns_and_uses_latest():
    with pytest.warns(UserWarning, match="not a release tag"):
        cls = run_time_settings_for_ref("main")
    assert cls is RunTimeSettingsV0_9_0


def test_run_time_settings_for_ref_unresolvable_hash_warns_and_uses_latest():
    """A commit hash needs ``repo_path`` (not passed here) to resolve to a
    release tag, so it falls back the same way an unparseable/branch ref does.
    """
    with pytest.warns(UserWarning, match="not a release tag"):
        cls = run_time_settings_for_ref("a1b2c3d4")
    assert cls is RunTimeSettingsV0_9_0


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
        "cstar.applications.forge.namelist_model.namelist_schema_for_ref",
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


def test_build_namelist_v0_7_0_dispatches_to_most_specific_class(tmp_path):
    """A RunTimeSettingsV0_7_0 instance is also a RunTimeSettingsV0_6_0/
    RunTimeSettingsV0_5_0 instance (subclassing) -- proves ``build_namelist``'s
    most-specific-first ``isinstance`` chain selects ``RomsNamelistV0_7_0``
    (with ``&pio_settings`` AND the two new CDR output groups), not one of its
    superclasses' namelist schemas.
    """
    d = _populated_rt_dict()
    rt = RunTimeSettingsV0_7_0.model_validate(d)
    nml = build_namelist(rt, n_tracers=34)
    assert type(nml) is RomsNamelistV0_7_0
    assert nml.pio_settings.pio_stride == 1
    assert nml.cdr_tracer_output_settings.do_cdr_tracer_output is False
    assert nml.cdr_gas_exch_output_settings.do_cdr_gas_exch_output is False

    nml.write(tmp_path / "namelist.nml")
    text = (tmp_path / "namelist.nml").read_text()
    assert "&pio_settings" in text
    assert "&cdr_tracer_output_settings" in text
    assert "&cdr_gas_exch_output_settings" in text


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


# ---------------------------------------------------------------------------
# output_precheck_applies_to -- the >= 0.5.0 output-stream precheck gate,
# derived from a run-time settings class (inverts
# _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA rather than re-deriving the schema via
# a second namelist_schema_for_ref lookup -- see the function's docstring).
# ---------------------------------------------------------------------------
def test_output_precheck_applies_to_legacy_settings_cls_is_false():
    assert output_precheck_applies_to(RunTimeSettings) is False


def test_output_precheck_applies_to_v0_4_0_settings_cls_is_false():
    """`RunTimeSettingsV0_4_0` (< 0.5.0) is still below the >= 0.5.0 gate even
    though it adds the CDR tracer counts.
    """
    assert output_precheck_applies_to(RunTimeSettingsV0_4_0) is False


@pytest.mark.parametrize(
    "settings_cls",
    [RunTimeSettingsV0_5_0, RunTimeSettingsV0_6_0, RunTimeSettingsV0_7_0],
)
def test_output_precheck_applies_to_versioned_settings_cls_is_true(settings_cls):
    assert output_precheck_applies_to(settings_cls) is True


@pytest.mark.parametrize("roms_ref", [None, ""])
def test_output_precheck_applies_to_none_and_empty_ref_stays_legacy(roms_ref):
    """Pins the exact composition both resolve.py and executor.py use to gate
    the output-stream precheck: ``run_time_settings_for_ref(roms_ref)`` then
    ``output_precheck_applies_to(settings_cls)``. A blueprint with no pinned
    ucla-roms ref (``roms_ref`` None or "") must resolve to the legacy
    schema's gate (off), not the latest schema's -- the bug this composition
    was written to avoid: calling ``namelist_schema_for_ref(None)`` directly
    here instead would warn and return the *latest* schema, wrongly turning
    the precheck on for a no-ref blueprint.
    """
    settings_cls = run_time_settings_for_ref(roms_ref)
    assert settings_cls is RunTimeSettings
    assert output_precheck_applies_to(settings_cls) is False


# ---------------------------------------------------------------------------
# forge_field_for -- canonical (section, key) -> forge "section.field" lookup
# ---------------------------------------------------------------------------
def test_forge_field_for_renamed_field():
    # FrcOutputCfg.output_period has serialization_alias="output_period_frc".
    assert (
        forge_field_for("frc_output_settings", "output_period_frc")
        == "frc_output.output_period"
    )


def test_forge_field_for_unaliased_field():
    # OceanVarsCfgV0_5_0.output_period_rst has no serialization_alias -- the
    # forge field name already IS the canonical key.
    assert (
        forge_field_for("basic_output_settings", "output_period_rst")
        == "ocean_vars.output_period_rst"
    )


def test_forge_field_for_unknown_section_returns_none():
    assert forge_field_for("stdout_diag_settings", "code_check_mode") is None


def test_forge_field_for_unknown_key_returns_none():
    assert forge_field_for("frc_output_settings", "not_a_real_key") is None


# ---------------------------------------------------------------------------
# ucla-roms >= 0.9.0: RunTimeSettingsV0_9_0 (cdr_lite, renamed cdr_lite_output)
# ---------------------------------------------------------------------------
def test_build_namelist_v0_9_0_dispatches_to_its_own_class(tmp_path):
    """RunTimeSettingsV0_9_0 is a RunTimeSettingsV0_6_0 but NOT a V0_7_0 --
    ``build_namelist`` must pick ``RomsNamelistV0_9_0`` (the new CDR-lite groups,
    no ``cdr_tracer_output_settings``) and write the 0.9 names.
    """
    d = _populated_rt_dict()
    d["cdr_lite"] = {"cdr_online_carbonate_sensitivity": True}
    d["cdr_lite_output"].update(do_cdr_lite_output=True, wrt_gas_exchange=True)
    rt = RunTimeSettingsV0_9_0.model_validate(d)
    assert not isinstance(rt, RunTimeSettingsV0_7_0)
    nml = build_namelist(rt, n_tracers=34)
    assert type(nml) is RomsNamelistV0_9_0
    assert nml.cdr_lite_settings.cdr_online_carbonate_sensitivity is True
    assert nml.cdr_lite_output_settings.do_cdr_lite_output is True
    assert nml.cdr_lite_output_settings.wrt_gas_exchange is True
    assert nml.cdr_lite_output_settings.nrpf_cdr_lite == d["cdr_lite_output"]["nrpf"]

    nml.write(tmp_path / "namelist.nml")
    text = (tmp_path / "namelist.nml").read_text()
    assert "&cdr_lite_settings" in text
    assert "&cdr_lite_output_settings" in text
    assert "&cdr_gas_exch_output_settings" in text
    assert "cdr_tracer_output" not in text


def test_run_time_settings_v0_9_0_defaults_when_sections_omitted():
    d = _populated_rt_dict()
    for section in ("cdr_lite", "cdr_lite_output", "cdr_gas_exch_output"):
        d.pop(section, None)
    nml = build_namelist(RunTimeSettingsV0_9_0.model_validate(d), n_tracers=34)
    assert nml.cdr_lite_settings.cdr_online_carbonate_sensitivity is False
    assert nml.cdr_lite_output_settings.do_cdr_lite_output is False
    assert nml.cdr_lite_output_settings.wrt_gas_exchange is False


def test_v0_7_0_ignores_wrt_gas_exchange_and_writes_tracer_names(tmp_path):
    """The shared OutputSpec carries ``wrt_gas_exchange``; a 0.7/0.8 tier drops it
    (no such key in ``&CDR_TRACER_OUTPUT_SETTINGS``) and writes the old names.
    """
    d = _populated_rt_dict()
    d["cdr_lite_output"].update(do_cdr_lite_output=True, wrt_gas_exchange=True)
    nml = build_namelist(RunTimeSettingsV0_7_0.model_validate(d), n_tracers=34)
    assert nml.cdr_tracer_output_settings.do_cdr_tracer_output is True
    assert "wrt_gas_exchange" not in nml.cdr_tracer_output_settings.model_dump()


# ---------------------------------------------------------------------------
# check_cdr_lite_sections
# ---------------------------------------------------------------------------
_TRACERS = {"nt_cdr_oae": 1, "nt_cdr_dor": 0}


def _lite(online=False, stream=False, gas=False, param=None):
    return {
        "cdr_lite": {"cdr_online_carbonate_sensitivity": online},
        "cdr_lite_output": {"do_cdr_lite_output": stream, "wrt_gas_exchange": gas},
        "param": {**_PARAM, "ntrc_bio": 0, **(_TRACERS if param is None else param)},
    }


_V09 = RunTimeSettingsV0_9_0


@pytest.mark.parametrize(
    ("settings", "mode", "expected"),
    [
        (_lite(online=True), "marbl", True),
        (_lite(online=False, param={}), "marbl", False),
        (_lite(online=False, param={}), "none", False),
        ({}, "marbl", False),  # sections absent: nothing to read
        ({"param": _PARAM}, "none", False),
        # A disabled section never trips the tracer-count rule.
        (_lite(param={}), "marbl", False),
        (_lite(online=True, stream=True, gas=True), "marbl", True),
        # bgc_mode cdr_lite needs CDR_LITE without the online knob; the tracer
        # counts are generation-derived, so none are required before generation
        # (configure_build enforces them), and the stream/gas switches are fine.
        (_lite(param={}), "cdr_lite", True),
        (_lite(stream=True, param={}), "cdr_lite", True),
        (_lite(stream=True, gas=True, param={}), "cdr_lite", True),
        (_lite(stream=True), "cdr_lite", True),
    ],
)
def test_check_cdr_lite_sections_returns_whether_cdr_lite_is_needed(
    settings, mode, expected
):
    assert (
        check_cdr_lite_sections(settings, bgc_mode=mode, settings_cls=_V09) is expected
    )


@pytest.mark.parametrize(
    ("settings", "mode", "match"),
    [
        (_lite(online=True), "none", "MARBL"),
        (_lite(online=True), "cdr_lite", "MARBL"),
        (_lite(stream=True, gas=True), "marbl", "needs CDR_LITE.*cdr_online_carbonate"),
        (_lite(stream=True, gas=True, param={}), "none", 'bgc_mode "cdr_lite"'),
        (_lite(online=True, param={}), "marbl", "param.nt_cdr_oae"),
        (_lite(stream=True, param={}), "marbl", "cdr_lite_output.do_cdr_lite_output"),
        (
            _lite(online=True, param={"nt_cdr_oae": 0, "nt_cdr_dor": 0}),
            "marbl",
            "== 0",
        ),
        # CDR tracers need CDR_LITE (ROMS: "Forcing type not supported"): from
        # the bgc mode or the online knob, nothing else.
        (_lite(), "none", "Forcing type not supported"),
        (_lite(), "marbl", "Forcing type not supported"),
        (_lite(param={"nt_cdr_dor": 1}), "marbl", "Forcing type not supported"),
    ],
)
def test_check_cdr_lite_sections_rejects_what_roms_would_abort_on(
    settings, mode, match
):
    with pytest.raises(ValueError, match=match):
        check_cdr_lite_sections(settings, bgc_mode=mode, settings_cls=_V09)


def test_check_cdr_lite_sections_accepts_cdr_tracers_when_cdr_lite_is_compiled():
    assert check_cdr_lite_sections(_lite(), bgc_mode="cdr_lite", settings_cls=_V09)
    online = _lite(online=True)
    assert check_cdr_lite_sections(online, bgc_mode="marbl", settings_cls=_V09)


def test_check_cdr_lite_sections_cdr_tracers_without_cdr_lite_need_a_tier_that_has_it():
    """CDR tracers exist from ucla-roms 0.4.0, but the CDR_LITE cppkey (and the
    abort without it) only from 0.9.0: older tiers are not held to the rule.
    """
    for tier in (RunTimeSettingsV0_4_0, RunTimeSettingsV0_7_0):
        assert not check_cdr_lite_sections(_lite(), bgc_mode="none", settings_cls=tier)


def test_check_cdr_lite_sections_accepts_dor_tracers_alone():
    settings = _lite(online=True, param={"nt_cdr_oae": 0, "nt_cdr_dor": 2})
    assert check_cdr_lite_sections(settings, bgc_mode="marbl", settings_cls=_V09)


def test_check_cdr_lite_sections_treats_a_null_tracer_count_as_zero():
    """A YAML ``nt_cdr_oae:`` (null) is "no tracers", not a TypeError."""
    settings = _lite(stream=True, param={"nt_cdr_oae": None})
    with pytest.raises(ValueError, match="== 0"):
        check_cdr_lite_sections(settings, bgc_mode="marbl", settings_cls=_V09)


def test_check_cdr_lite_sections_reports_every_problem_together():
    settings = _lite(online=False, stream=True, gas=True, param={})
    with pytest.raises(ValueError) as exc:
        check_cdr_lite_sections(settings, bgc_mode="marbl", settings_cls=_V09)
    assert "needs CDR_LITE" in str(exc.value)
    assert "== 0" in str(exc.value)

    both = _lite(online=True, stream=True, param={"nt_cdr_dor": 1})
    with pytest.raises(ValueError) as exc:
        check_cdr_lite_sections(both, bgc_mode="none", settings_cls=_V09)
    assert "MARBL" in str(exc.value)


# ---------------------------------------------------------------------------
# bgc_mode_from_cppdefs
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("cppdefs", "expected"),
    [
        ({"marbl": True}, "marbl"),
        # MARBL with the online CDR-lite sensitivities is still a MARBL build.
        ({"marbl": True, "cdr_lite": True}, "marbl"),
        ({"marbl": False, "cdr_lite": True}, "cdr_lite"),
        ({"cdr_lite": True}, "cdr_lite"),
        ({"marbl": False}, "none"),
        ({"marbl": False, "cdr_lite": False}, "none"),
        ({}, "none"),
    ],
)
def test_bgc_mode_from_cppdefs(cppdefs, expected):
    assert bgc_mode_from_cppdefs(cppdefs) == expected


# ---------------------------------------------------------------------------
# check_cdr_lite_mode_roms
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("ref", ["0.9.1", "v0.9.1", "0.9.2", "0.10.0", "1.0.0"])
def test_check_cdr_lite_mode_roms_accepts_releases_from_the_minimum(ref):
    assert CDR_LITE_MODE_MIN_ROMS == (0, 9, 1)
    check_cdr_lite_mode_roms(ref)


@pytest.mark.parametrize("ref", ["main", "pio-dev", "abc1234", "", None])
def test_check_cdr_lite_mode_roms_treats_a_non_tag_ref_as_the_latest(ref):
    check_cdr_lite_mode_roms(ref)


@pytest.mark.parametrize("ref", ["0.9.0", "0.8.0", "v0.7.1"])
def test_check_cdr_lite_mode_roms_rejects_older_releases(ref):
    with pytest.raises(ValueError, match=r"ucla-roms >= 0\.9\.1"):
        check_cdr_lite_mode_roms(ref)


# ---------------------------------------------------------------------------
# cdr_tracer_counts
# ---------------------------------------------------------------------------
def test_cdr_tracer_counts_reads_every_generated_family():
    counts = cdr_tracer_counts(
        [
            "temp",
            "salt",
            "passive_tracer1",
            "passive_tracer2",
            "CDR_OAE_ALK1",
            "CDR_OAE_DIC1",
            "CDR_OAE_ALK2",
            "CDR_OAE_DIC2",
            "CDR_DOR_DIC1",
        ]
    )
    assert counts == CdrTracerCounts(n_passive=2, n_oae_pairs=2, n_dor=1, other=())


def test_cdr_tracer_counts_physics_only_axis_is_all_zero():
    assert cdr_tracer_counts(["temp", "salt"]) == CdrTracerCounts(0, 0, 0, ())
    assert cdr_tracer_counts([]) == CdrTracerCounts(0, 0, 0, ())


def test_cdr_tracer_counts_returns_marbl_names_in_axis_order():
    counts = cdr_tracer_counts(
        ["temp", "salt", "PO4", "CDR_OAE_ALK1", "CDR_OAE_DIC1", "NO3", "DIC"]
    )
    assert (counts.n_oae_pairs, counts.n_dor, counts.n_passive) == (1, 0, 0)
    assert counts.other == ("PO4", "NO3", "DIC")


def test_cdr_tracer_counts_accepts_numpy_strings():
    import numpy as np

    counts = cdr_tracer_counts(np.array(["temp", "salt", "CDR_DOR_DIC1"]))
    assert counts.n_dor == 1


@pytest.mark.parametrize(
    ("names", "match"),
    [
        (["CDR_OAE_ALK1"], r"do not pair up \(1 ALK, 0 DIC\)"),
        (["CDR_OAE_DIC1"], r"do not pair up \(0 ALK, 1 DIC\)"),
        (
            ["CDR_OAE_ALK1", "CDR_OAE_DIC1", "CDR_OAE_ALK2"],
            r"do not pair up \(2 ALK, 1 DIC\)",
        ),
        (["CDR_OAE_ALK2", "CDR_OAE_DIC2"], r"CDR_OAE_ALK<k> is numbered \[2\]"),
        (["CDR_DOR_DIC1", "CDR_DOR_DIC3"], r"CDR_DOR_DIC<k> is numbered \[1, 3\]"),
        (["passive_tracer2"], r"passive_tracer<k> is numbered \[2\]"),
        (["CDR_DOR_DIC1", "CDR_DOR_DIC1"], r"CDR_DOR_DIC<k> is numbered \[1, 1\]"),
    ],
)
def test_cdr_tracer_counts_rejects_malformed_axes(names, match):
    with pytest.raises(ValueError, match=match):
        cdr_tracer_counts(names)


def test_cdr_tracer_counts_reports_every_malformed_family_together():
    with pytest.raises(ValueError) as exc:
        cdr_tracer_counts(["passive_tracer2", "CDR_DOR_DIC2", "CDR_OAE_ALK1"])
    msg = str(exc.value)
    assert "passive_tracer<k>" in msg
    assert "CDR_DOR_DIC<k>" in msg
    assert "do not pair up" in msg


def test_validate_run_time_sections_reports_a_null_tracer_count_without_raising():
    errs = validate_run_time_sections(
        {**_lite(stream=True, param={"nt_cdr_oae": None}), "cppdefs": {"marbl": True}},
        roms_ref="0.9.0",
    )
    assert any("== 0" in e for e in errs)


def test_validate_run_time_sections_runs_the_cdr_lite_check():
    errs = validate_run_time_sections(
        {**_lite(online=True), "cppdefs": {"marbl": False}}, roms_ref="0.9.0"
    )
    assert any("MARBL" in e for e in errs)
    assert (
        validate_run_time_sections(
            {**_lite(online=True), "cppdefs": {"marbl": True}}, roms_ref="0.9.0"
        )
        == []
    )
    # Without cppdefs the MARBL input is missing: skipped, like the BGC check.
    assert validate_run_time_sections(_lite(online=True), roms_ref="0.9.0") == []


def test_validate_run_time_sections_reads_the_bgc_mode_from_cppdefs():
    """A stored cdr_lite blueprint has zero counts before generation (the CDR
    forcing supplies them), so the stream switch alone is not an error there; the
    same counts without ``CDR_LITE`` are, on a tier that has it.
    """
    stored = {**_lite(stream=True, param={}), "cppdefs": {"marbl": False}}
    assert any("== 0" in e for e in validate_run_time_sections(stored, "0.9.1"))
    stored["cppdefs"] = {"marbl": False, "cdr_lite": True}
    assert validate_run_time_sections(stored, "0.9.1") == []

    counted = {**_lite(), "cppdefs": {"marbl": False}}
    assert any(
        "Forcing type not supported" in e
        for e in validate_run_time_sections(counted, "0.9.1")
    )
    counted["cppdefs"] = {"marbl": False, "cdr_lite": True}
    assert validate_run_time_sections(counted, "0.9.1") == []
    # Older tiers have no CDR_LITE: the same counts are fine there.
    counted["cppdefs"] = {"marbl": False}
    assert validate_run_time_sections(counted, "0.7.0") == []


def test_validate_run_time_sections_skips_cdr_lite_check_on_a_tier_without_it():
    """The cross-section CDR-lite check reads only the sections the pinned tier
    models: on 0.8 ``cdr_lite`` is reported by ``prune_version_gated_sections``
    at resolve/configure_build, not advised on here with 0.9-only wording.
    """
    settings = {**_lite(online=True), "cppdefs": {"marbl": False}}
    assert any("MARBL" in e for e in validate_run_time_sections(settings, "0.9.0"))
    assert not any("MARBL" in e for e in validate_run_time_sections(settings, "0.8.0"))


# ---------------------------------------------------------------------------
# check_cdr_output_sections -- rows apply per tier
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("roms_ref", "forces_cdr_forcing"),
    [("0.7.0", True), ("0.8.0", True), ("0.9.0", False)],
)
def test_cdr_lite_output_forces_cdr_forcing_only_before_0_9_0(
    roms_ref, forces_cdr_forcing
):
    settings = {"cdr_lite_output": {"do_cdr_lite_output": True}}
    assert (
        check_cdr_output_sections(
            settings,
            bgc_mode_is_marbl=False,
            settings_cls=run_time_settings_for_ref(roms_ref),
        )
        is forces_cdr_forcing
    )


@pytest.mark.parametrize("roms_ref", ["0.7.0", "0.8.0", "0.9.0"])
def test_gas_exchange_output_keeps_its_marbl_rule_on_every_tier(roms_ref):
    settings = {"cdr_gas_exch_output": {"do_cdr_gas_exch_output": True}}
    cls = run_time_settings_for_ref(roms_ref)
    with pytest.raises(ValueError, match="gas-exchange"):
        check_cdr_output_sections(settings, bgc_mode_is_marbl=False, settings_cls=cls)
    assert check_cdr_output_sections(settings, bgc_mode_is_marbl=True, settings_cls=cls)


def test_cdr_output_sections_ignore_tiers_without_the_section():
    settings = {"cdr_lite_output": {"do_cdr_lite_output": True}}
    assert not check_cdr_output_sections(
        settings,
        bgc_mode_is_marbl=False,
        settings_cls=run_time_settings_for_ref("0.6.0"),
    )


# ---------------------------------------------------------------------------
# _PRECHECK_SECTION_MAP rows / forge_field_for
# ---------------------------------------------------------------------------
def test_precheck_translation_picks_the_group_by_tier():
    section = {"do_cdr_lite_output": True, "nrpf": 3, "output_period": 60.0}
    settings = {"cdr_lite_output": section}
    v07 = canonical_output_sections_for_precheck(
        settings, run_time_settings_for_ref("0.8.0")
    )
    assert v07 == {
        "cdr_tracer_output_settings": CdrLiteOutputCfg.model_validate(
            section
        ).model_dump(by_alias=True)
    }
    assert v07["cdr_tracer_output_settings"]["nrpf_cdr_trc"] == 3
    v09 = canonical_output_sections_for_precheck(
        settings, run_time_settings_for_ref("0.9.0")
    )
    assert list(v09) == ["cdr_lite_output_settings"]
    assert v09["cdr_lite_output_settings"]["nrpf_cdr_lite"] == 3
    assert v09["cdr_lite_output_settings"]["wrt_gas_exchange"] is False


@pytest.mark.parametrize("roms_ref", ["0.5.0", "0.6.0", "0.7.0", "0.8.0", "0.9.0"])
def test_every_precheck_row_that_applies_matches_the_tier_annotation(roms_ref):
    """Each section of the >= 0.5.0 tiers has exactly one applying row, so none is
    silently skipped by the row-matching rule (e.g. ``ocean_vars`` ->
    ``OceanVarsCfgV0_5_0``).
    """
    cls = run_time_settings_for_ref(roms_ref)
    assert output_precheck_applies_to(cls)
    sections = [section for section, _cfg, _group in _PRECHECK_SECTION_MAP]
    for section in set(sections):
        if section not in cls.model_fields:
            continue
        applying = [
            group
            for sec, cfg, group in _PRECHECK_SECTION_MAP
            if sec == section and cls.model_fields[sec].annotation is cfg
        ]
        assert len(applying) == 1, (roms_ref, section, applying)
    # ... and every section the tier models that the table knows is covered.
    assert {section for section in sections if section in cls.model_fields} == {
        sec
        for sec, cfg, _ in _PRECHECK_SECTION_MAP
        if sec in cls.model_fields and cls.model_fields[sec].annotation is cfg
    }


def test_forge_field_for_maps_both_cdr_lite_groups_to_the_forge_section():
    assert (
        forge_field_for("cdr_tracer_output_settings", "nrpf_cdr_trc")
        == "cdr_lite_output.nrpf"
    )
    assert (
        forge_field_for("cdr_lite_output_settings", "nrpf_cdr_lite")
        == "cdr_lite_output.nrpf"
    )
    assert (
        forge_field_for("cdr_tracer_output_settings", "do_cdr_tracer_output")
        == "cdr_lite_output.do_cdr_lite_output"
    )
    assert (
        forge_field_for("cdr_lite_output_settings", "wrt_gas_exchange")
        == "cdr_lite_output.wrt_gas_exchange"
    )


# ---------------------------------------------------------------------------
# prune_version_gated_sections -- an enabled gated section is rejected
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("section", "values"),
    [
        ("cdr_lite_output", {"do_cdr_lite_output": True}),
        ("cdr_gas_exch_output", {"do_cdr_gas_exch_output": True}),
    ],
)
def test_prune_rejects_an_enabled_section_the_tier_lacks(section, values):
    settings = {section: dict(values), "ocean_vars": {}}
    with pytest.raises(ValueError, match=rf"{section}\.{next(iter(values))}") as exc:
        prune_version_gated_sections(settings, RunTimeSettingsV0_6_0)
    assert "RunTimeSettingsV0_6_0" in str(exc.value)
    assert section in settings  # nothing was mutated


def test_prune_rejects_cdr_lite_knob_on_a_pre_0_9_0_tier():
    settings = {"cdr_lite": {"cdr_online_carbonate_sensitivity": True}}
    with pytest.raises(ValueError, match="cdr_lite.cdr_online_carbonate_sensitivity"):
        prune_version_gated_sections(settings, RunTimeSettingsV0_7_0)


def test_prune_stays_silent_for_switched_off_sections():
    settings = {
        "cdr_lite": {"cdr_online_carbonate_sensitivity": False},
        "cdr_lite_output": {"do_cdr_lite_output": False},
        "cdr_gas_exch_output": {"do_cdr_gas_exch_output": False},
        "ocean_vars": {},
    }
    assert prune_version_gated_sections(settings, RunTimeSettingsV0_6_0) == [
        "cdr_gas_exch_output",
        "cdr_lite",
        "cdr_lite_output",
    ]
    assert list(settings) == ["ocean_vars"]


def test_gated_section_switches_are_fields_of_their_0_9_0_sections():
    cls = RunTimeSettingsV0_9_0
    for section, flag in _GATED_SECTION_SWITCHES.items():
        assert flag in cls.model_fields[section].annotation.model_fields


# ---------------------------------------------------------------------------
# normalize_legacy_sections
# ---------------------------------------------------------------------------
def test_normalize_legacy_sections_renames_section_and_flag_keeping_order():
    settings = {
        "a": 1,
        "cdr_tracer_output": {"do_cdr_tracer_output": True, "nrpf": 8},
        "z": 2,
    }
    assert normalize_legacy_sections(settings) == {
        "cdr_tracer_output": "cdr_lite_output"
    }
    assert settings == {
        "a": 1,
        "cdr_lite_output": {"do_cdr_lite_output": True, "nrpf": 8},
        "z": 2,
    }
    assert list(settings) == ["a", "cdr_lite_output", "z"]


def test_normalize_legacy_sections_is_idempotent_and_leaves_new_keys_alone():
    settings = {"cdr_lite_output": {"do_cdr_lite_output": True}}
    assert normalize_legacy_sections(settings) == {}
    assert settings == {"cdr_lite_output": {"do_cdr_lite_output": True}}


def test_normalize_legacy_sections_rejects_a_legacy_flag_next_to_its_new_name():
    section = {"do_cdr_tracer_output": True, "do_cdr_lite_output": False}
    settings = {"cdr_tracer_output": section}
    with pytest.raises(ValueError, match="do_cdr_tracer_output.*do_cdr_lite_output"):
        normalize_legacy_sections(settings)
    assert settings == {"cdr_tracer_output": section}  # nothing renamed


def test_normalize_legacy_sections_rejects_both_names():
    with pytest.raises(ValueError, match="both"):
        normalize_legacy_sections({"cdr_tracer_output": {}, "cdr_lite_output": {}})


def test_normalize_legacy_sections_does_not_mutate_the_old_inner_dict():
    inner = {"do_cdr_tracer_output": True}
    normalize_legacy_sections({"cdr_tracer_output": inner})
    assert inner == {"do_cdr_tracer_output": True}
