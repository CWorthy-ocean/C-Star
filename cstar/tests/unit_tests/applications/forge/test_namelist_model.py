"""
Tests for the Pydantic namelist/settings models (``cstar.applications.forge.namelist_model``):

* settings dict -> RunTimeSettings -> build_namelist -> write
* the read -> edit -> write round-trip (reusable by other repos)
* validation on bad/incomplete input
* defaults come from the YAML, not the models
"""

import re
from pathlib import Path
from unittest import mock

import pytest
import yaml
from pydantic import TypeAdapter, ValidationError

import cstar.catalog
from cstar.applications.forge.namelist_model import (
    _GATED_SECTION_SWITCHES,
    _PRECHECK_SECTIONS,
    _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA,
    CDR_LITE_MODE_MIN_ROMS,
    CDR_NZ_CHD_FROM_FILE_MIN_ROMS,
    RIVER_TRACERS_WITHOUT_CDR_MIN_ROMS,
    UPSCALED_CDR_WITHOUT_MARBL_MIN_ROMS,
    CdrGasExchOutputCfg,
    CdrLiteCfg,
    CdrLiteCfgV0_12_0,
    CdrLiteOutputCfg,
    CdrLiteOutputCfgV0_9_0,
    CdrOutputCfg,
    CdrTracerCounts,
    MarblBgcCfg,
    MarblBgcCfgV0_11_0,
    RunTimeSettings,
    RunTimeSettingsV0_4_0,
    RunTimeSettingsV0_5_0,
    RunTimeSettingsV0_6_0,
    RunTimeSettingsV0_7_0,
    RunTimeSettingsV0_9_0,
    RunTimeSettingsV0_11_0,
    RunTimeSettingsV0_12_0,
    RunTimeSettingsV0_13_0,
    VSpongeCfg,
    VSpongeCfgV0_13_0,
    _settings_spellings,
    bgc_mode_from_cppdefs,
    build_namelist,
    canonical_output_sections_for_precheck,
    cdr_nz_chd_from_file,
    cdr_tracer_counts,
    cdr_tracer_role,
    check_bgc_tracer_count,
    check_cdr_forcing_mode,
    check_cdr_lite_mode_roms,
    check_cdr_lite_sections,
    check_cdr_output_sections,
    check_constant_fe,
    check_marbl_only_outputs,
    check_restoring_flux_output,
    check_river_tracer_counts,
    check_upscaled_cdr_lite_tracer_counts,
    n_tracers_from_param,
    namelist_field_for,
    namelist_schema_for,
    normalize_legacy_sections,
    output_precheck_applies_to,
    prune_version_gated_sections,
    river_tracer_floor,
    run_time_settings_for_ref,
    validate_run_time_sections,
)
from cstar.applications.forge.resolve import load_model_spec_data
from cstar.applications.forge.settings import write_roms_namelist
from cstar.catalog.domain_catalog import default_catalog
from cstar.roms.namelist import (
    NEWEST_KNOWN_UCLA_ROMS,
    RomsNamelist,
    RomsNamelistV0_5_0,
    RomsNamelistV0_7_0,
    RomsNamelistV0_9_0,
    RomsNamelistV0_11_0,
    RomsNamelistV0_12_0,
    RomsNamelistV0_13_0,
    format_roms_version,
    release_ref_for_remote,
)

# Every registered run-time settings tier, oldest first.
_ALL_SETTINGS_TIERS = (
    RunTimeSettings,
    RunTimeSettingsV0_4_0,
    RunTimeSettingsV0_5_0,
    RunTimeSettingsV0_6_0,
    RunTimeSettingsV0_7_0,
    RunTimeSettingsV0_9_0,
    RunTimeSettingsV0_11_0,
    RunTimeSettingsV0_12_0,
    RunTimeSettingsV0_13_0,
)


def _section_classes(settings_cls):
    """The settings section classes ``settings_cls`` types its sections as."""
    return {
        info.annotation
        for info in settings_cls.model_fields.values()
        if hasattr(info.annotation, "model_fields")
    }


_MODEL_PATH = (
    Path(cstar.catalog.__file__).parent
    / "bundled"
    / "specs"
    / "ModelSpec"
    / "cson_roms-marbl_v0.1.yaml"
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
    model = load_model_spec_data(_MODEL_PATH)["model"]
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
# Misspelled keys -- a key naming a field only up to case, or by its namelist
# key, is rejected instead of dropped by extra="ignore"
# ---------------------------------------------------------------------------
def _rename(section: dict, old: str, new: str) -> None:
    """Respell ``old`` as ``new`` in place, keeping the key order."""
    items = [(new if k == old else k, v) for k, v in section.items()]
    section.clear()
    section.update(items)


def test_mixed_case_key_is_rejected_with_its_field():
    """ROMS spells ``param``'s grid size ``LLm``; Forge's field is ``llm``."""
    d = _populated_rt_dict()
    _rename(d["param"], "llm", "LLm")
    with pytest.raises(ValidationError, match=r"LLm \(did you mean llm\?\)") as exc:
        RunTimeSettingsV0_9_0.model_validate(d)
    assert [err["loc"] for err in exc.value.errors()] == [("param",)]


def test_every_misspelled_key_is_reported_in_one_error():
    d = _populated_rt_dict()
    _rename(d["param"], "llm", "LLm")
    _rename(d["param"], "np_xi", "NP_XI")
    _rename(d["param"], "n", "nz")  # the namelist key (serialization_alias)
    _rename(d["ocean_vars"], "wrt_z", "wrt_Z")
    with pytest.raises(ValidationError) as exc:
        RunTimeSettingsV0_9_0.model_validate(d)
    by_loc = {err["loc"]: err["msg"] for err in exc.value.errors()}
    assert set(by_loc) == {("param",), ("ocean_vars",)}
    assert (
        "LLm (did you mean llm?), nz (did you mean n?), NP_XI (did you mean np_xi?)"
    ) in by_loc[("param",)]
    assert "wrt_Z (did you mean wrt_z?)" in by_loc[("ocean_vars",)]


@pytest.mark.parametrize("key", ["akv_bak", "Akv_bak", "AKV"])
def test_namelist_spelling_of_an_aliased_field_is_rejected(key):
    d = _populated_rt_dict()
    _rename(d["vertical_mixing"], "akv", key)
    with pytest.raises(ValidationError, match=rf"{key} \(did you mean akv\?\)"):
        RunTimeSettingsV0_9_0.model_validate(d)


def test_exact_and_metadata_keys_are_accepted():
    """Field names, a validation alias (``do_cdr``) and metadata keys no field
    names (``*_vname``) all pass; the metadata is dropped as before.
    """
    d = _populated_rt_dict()
    d["river_frc"]["rvol_vname"] = "river_volume"
    d["cdr_output"]["do_cdr"] = d["cdr_output"].pop("do_cdr_output")
    rt = RunTimeSettingsV0_9_0.model_validate(d)
    assert rt.param.llm == 512
    assert rt.cdr_output.do_cdr_output is d["cdr_output"]["do_cdr"]
    assert "rvol_vname" not in rt.river_frc.model_dump()


def test_misspelled_validation_alias_is_rejected():
    section = {
        "Do_Cdr": True,
        "do_avg": True,
        "monthly_averages": False,
        "output_period": 3600.0,
        "nrpf": 1,
    }
    with pytest.raises(
        ValidationError, match=r"Do_Cdr \(did you mean do_cdr_output\?\)"
    ):
        CdrOutputCfg.model_validate(section)


@pytest.mark.parametrize("value", [0, 2])
def test_legacy_param_cfg_rejects_a_misspelled_cdr_tracer_count(value):
    """``nt_cdr_oae`` is no field below ucla-roms 0.4.0, yet a case variant is
    still rejected, whatever its value: the version gate reads the exact key, so
    ``NT_CDR_OAE: 2`` would otherwise be dropped like a zero.
    """
    d = _populated_rt_dict()
    d["param"]["NT_CDR_OAE"] = value
    with pytest.raises(
        ValidationError, match=r"NT_CDR_OAE \(did you mean nt_cdr_oae\?\)"
    ):
        RunTimeSettings.model_validate(d)


def test_misspelled_section_is_rejected_even_on_a_tier_without_it():
    """A section name is checked like a key: ``CDR_LITE`` (a 0.9.0 section)
    fails on the 0.7.0 tier too, which would otherwise drop it unseen.
    """
    d = _populated_rt_dict()
    d["CDR_LITE"] = {"cdr_online_carbonate_sensitivity": True}
    _rename(d, "marbl_bgc", "Marbl_Bgc")
    with pytest.raises(
        ValidationError,
        match=r"Marbl_Bgc \(did you mean marbl_bgc\?\), CDR_LITE \(did you mean cdr_lite\?\)",
    ):
        RunTimeSettingsV0_7_0.model_validate(d)


@pytest.mark.parametrize("settings_cls", _ALL_SETTINGS_TIERS)
def test_every_settings_field_suggests_itself(settings_cls):
    """No alias in a section shadows another field's name in the spelling table,
    so each suggestion names the field the key would have set.
    """
    for cls in (settings_cls, *_section_classes(settings_cls)):
        spellings, accepted = _settings_spellings(cls)
        for name in cls.model_fields:
            assert name in accepted
            assert spellings[name.casefold()] == name, (cls.__name__, name)


def test_each_section_class_knows_the_keys_other_tiers_model():
    """Every key some tier models in a section is a field, or an
    ``_other_tier_keys()`` entry, of every class a tier types that section as, so
    its case variants fail on every tier (version gates read exact keys). A tier
    adding a key must list it in the older classes' ``_other_tier_keys()``.
    """
    classes_by_section: dict[str, set] = {}
    for settings_cls in _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA.values():
        for section, info in settings_cls.model_fields.items():
            if hasattr(info.annotation, "model_fields"):
                classes_by_section.setdefault(section, set()).add(info.annotation)
    missing = {
        (section, cls.__name__): sorted(unknown)
        for section, classes in classes_by_section.items()
        for cls in classes
        if (
            unknown := set().union(*(c.model_fields for c in classes))
            - set(cls.model_fields)
            - set(cls._other_tier_keys())
        )
    }
    assert not missing


def test_validate_run_time_sections_reports_misspelled_keys():
    errs = validate_run_time_sections(
        {
            "Marbl_Bgc": {},
            "cppdefs": {"MARBL": True},  # compile-time: not checked here
            "vertical_mixing": {"akv": 1.0e-4, "akt_default": 1.0e-6, "AKV": 2},
        },
        roms_ref="0.9.0",
    )
    assert len(errs) == 2
    assert "Marbl_Bgc (did you mean marbl_bgc?)" in errs[0]
    assert errs[1].startswith("vertical_mixing: ")
    assert "AKV (did you mean akv?)" in errs[1]


def test_prune_rejects_a_misspelled_switch_in_a_section_it_would_drop():
    """The switch is read by exact key, so a case variant would be pruned
    silently even when on; every such section is reported, with any enabled one.
    """
    settings = {
        "cdr_lite": {"cdr_online_carbonate_sensitivity": True},
        "cdr_lite_output": {"Do_Cdr_Lite_Output": True},
        "cdr_gas_exch_output": {"DO_CDR_GAS_EXCH_OUTPUT": False},
    }
    with pytest.raises(ValueError) as exc:
        prune_version_gated_sections(settings, RunTimeSettingsV0_6_0)
    message = str(exc.value)
    assert (
        "cdr_gas_exch_output.DO_CDR_GAS_EXCH_OUTPUT (did you mean "
        "do_cdr_gas_exch_output?), cdr_lite_output.Do_Cdr_Lite_Output (did you "
        "mean do_cdr_lite_output?)"
    ) in message
    assert "cdr_lite.cdr_online_carbonate_sensitivity enabled" in message
    assert set(settings) == {"cdr_lite", "cdr_lite_output", "cdr_gas_exch_output"}


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


# ---------------------------------------------------------------------------
# check_cdr_forcing_mode / cdr_nz_chd_from_file (ucla-roms #390)
# ---------------------------------------------------------------------------
def test_upscaled_cdr_constants_are_the_0_11_0_release():
    """Both rules come from ucla-roms #390, shipped in 0.11.0."""
    assert UPSCALED_CDR_WITHOUT_MARBL_MIN_ROMS == (0, 11, 0)
    assert CDR_NZ_CHD_FROM_FILE_MIN_ROMS == (0, 11, 0)


@pytest.mark.parametrize("roms_ref", ["0.10.1", "0.11.0", None])
@pytest.mark.parametrize("cdr_mode", ["simple", "netcdf", "upscaled"])
def test_check_cdr_forcing_mode_marbl_implies_cdr_output(cdr_mode, roms_ref):
    assert check_cdr_forcing_mode(cdr_mode, bgc_mode="marbl", roms_ref=roms_ref)


@pytest.mark.parametrize("bgc_mode", ["marbl", "none", "cdr_lite"])
def test_check_cdr_forcing_mode_none_is_inactive(bgc_mode):
    assert not check_cdr_forcing_mode("none", bgc_mode=bgc_mode, roms_ref="0.8.0")


@pytest.mark.parametrize("roms_ref", ["0.11.0", "0.11.3", "1.0.0", "main", None])
def test_check_cdr_forcing_mode_accepts_upscaled_cdr_lite_from_0_11_0(roms_ref):
    """From ucla-roms 0.11.0 depth-profile forcing feeds the CDR-lite tracers
    named in cdr_trc_name, so a CDR-lite build accepts "upscaled". cdr_output
    still needs MARBL, so it is not implied. A non-tag ref counts as latest.
    """
    assert not check_cdr_forcing_mode(
        "upscaled", bgc_mode="cdr_lite", roms_ref=roms_ref
    )


@pytest.mark.parametrize("roms_ref", ["0.10.1", "0.10.0", "0.9.1"])
def test_check_cdr_forcing_mode_rejects_upscaled_cdr_lite_before_0_11_0(roms_ref):
    with pytest.raises(
        ValueError,
        match=rf'"upscaled" with bgc_mode "cdr_lite" on ucla-roms {roms_ref}: '
        r".*>= 0\.11\.0",
    ):
        check_cdr_forcing_mode("upscaled", bgc_mode="cdr_lite", roms_ref=roms_ref)


@pytest.mark.parametrize("roms_ref", ["0.10.1", "0.11.0", "main"])
def test_check_cdr_forcing_mode_rejects_upscaled_physics_only(roms_ref):
    """A physics-only build has no tracer to receive the profiles on any release
    (ucla-roms aborts when no cdr_trc_name tracer exists in the run).
    """
    with pytest.raises(ValueError, match=r'"upscaled" with bgc_mode "none"'):
        check_cdr_forcing_mode("upscaled", bgc_mode="none", roms_ref=roms_ref)


@pytest.mark.parametrize("bgc_mode", ["none", "cdr_lite"])
def test_check_cdr_forcing_mode_parameterized_without_marbl_gate_unchanged(bgc_mode):
    assert not check_cdr_forcing_mode("simple", bgc_mode=bgc_mode, roms_ref="0.9.1")
    with pytest.raises(ValueError, match=r">= 0\.9\.1"):
        check_cdr_forcing_mode("simple", bgc_mode=bgc_mode, roms_ref="0.9.0")


@pytest.mark.parametrize(
    "param",
    [{}, {"nt_cdr_oae": 0, "nt_cdr_dor": 0}, {"nt_cdr_oae": None, "nt_cdr_dor": "0"}],
)
def test_check_upscaled_cdr_lite_tracer_counts_rejects_no_tracers(param):
    with pytest.raises(
        ValueError,
        match=r"needs the CDR-lite tracer counts.*CDR_OAE_ALK<n>/CDR_OAE_DIC<n> pair",
    ):
        check_upscaled_cdr_lite_tracer_counts(param)


@pytest.mark.parametrize(
    "param", [{"nt_cdr_oae": 1}, {"nt_cdr_dor": 2}, {"nt_cdr_oae": "1"}]
)
def test_check_upscaled_cdr_lite_tracer_counts_accepts_either_family(param):
    check_upscaled_cdr_lite_tracer_counts(param)


@pytest.mark.parametrize(
    ("roms_ref", "from_file"),
    [
        ("0.10.1", False),
        ("0.9.0", False),
        ("0.11.0", True),
        ("0.12.0", True),
        ("main", True),
        (None, True),
    ],
)
def test_cdr_nz_chd_from_file(roms_ref, from_file):
    assert cdr_nz_chd_from_file(roms_ref) is from_file


# ---------------------------------------------------------------------------
# check_constant_fe
# ---------------------------------------------------------------------------
def test_check_constant_fe_rejects_constant_iron_without_marbl():
    with pytest.raises(ValueError, match=r"marbl_bgc\.use_constant_fe.*MARBL is off"):
        check_constant_fe({"use_constant_fe": True}, bgc_mode_is_marbl=False)


@pytest.mark.parametrize(
    ("marbl_bgc", "bgc_mode_is_marbl"),
    [
        ({"use_constant_fe": True}, True),
        ({"use_constant_fe": False}, False),
        ({"use_constant_fe": "false"}, False),
        ({"use_constant_fe": 0}, False),
        ({"use_constant_fe": None}, False),
        ({}, False),
    ],
)
def test_check_constant_fe_accepts_consistent_settings(marbl_bgc, bgc_mode_is_marbl):
    check_constant_fe(marbl_bgc, bgc_mode_is_marbl=bgc_mode_is_marbl)


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
_MARBL_BGC = {
    "marbl_config_file": "marbl_in",
    "marbl_tracers_to_write": ["PO4"],
    "marbl_diagnostics_to_write": ["PH"],
    "marbl_timestep": 3600.0,
}


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


def test_validate_run_time_sections_checks_constant_fe_against_marbl():
    """A stored blueprint with MARBL off but constant iron requested is reported
    up front (engine/wizard), not first at configure_build.
    """
    marbl_bgc = {**_MARBL_BGC, "use_constant_fe": True}
    errs = validate_run_time_sections(
        {"marbl_bgc": marbl_bgc, "cppdefs": {"marbl": False}}, roms_ref="0.11.0"
    )
    assert len(errs) == 1
    assert "marbl_bgc.use_constant_fe" in errs[0]
    assert "MARBL is off" in errs[0]

    for settings in (
        {"marbl_bgc": marbl_bgc, "cppdefs": {"marbl": True}},
        {"marbl_bgc": marbl_bgc},  # no cppdefs: nothing to compare against
        {"marbl_bgc": _MARBL_BGC, "cppdefs": {"marbl": False}},  # not requested
    ):
        assert validate_run_time_sections(settings, roms_ref="0.11.0") == []


def test_validate_run_time_sections_checks_averages_against_the_marbl_step():
    """A stored blueprint whose ``_bgc_dia_avg`` window ends between MARBL steps
    is reported up front on ucla-roms >= 0.13.0, which stops on it at startup;
    0.12.0 runs it.
    """
    settings = {
        "cppdefs": {"marbl": True},
        "time_stepping": {"ntimes": 1440, "dt": 60.0, "ndtfast": 60, "ninfo": 1},
        "marbl_bgc": _MARBL_BGC,
        "bgc": {
            "interp_frc": False,
            "wrt_his": False,
            "output_period_his": 86400,
            "nrpf_his": 1,
            "wrt_avg": False,
            "output_period_avg": 86400,
            "nrpf_avg": 1,
            "wrt_his_dia": False,
            "output_period_his_dia": 86400,
            "nrpf_his_dia": 1,
            "wrt_avg_dia": True,
            "output_period_avg_dia": 1800,
            "nrpf_avg_dia": 1,
            "xco2air_default": 284.7,
        },
    }
    errs = validate_run_time_sections(settings, roms_ref="0.13.0")
    assert len(errs) == 1
    assert "bgc_settings.output_period_bgc_avg_dia (1800" in errs[0]
    assert "3600.0 s" in errs[0]
    assert validate_run_time_sections(settings, roms_ref="0.12.0") == []
    assert (
        validate_run_time_sections(
            {**settings, "marbl_bgc": {**_MARBL_BGC, "marbl_timestep": 1800.0}},
            roms_ref="0.13.0",
        )
        == []
    )


def test_validate_run_time_sections_reports_constant_fe_on_a_pre_0_11_0_pin():
    errs = validate_run_time_sections(
        {"marbl_bgc": {**_MARBL_BGC, "use_constant_fe": True}}, roms_ref="0.10.1"
    )
    assert len(errs) == 1
    assert errs[0].startswith("marbl_bgc: ")
    assert "marbl_bgc.use_constant_fe requires ucla-roms >= 0.11.0" in errs[0]


@pytest.mark.parametrize(
    ("cppdefs", "roms_ref", "rejected"),
    [
        ({"cdr_lite": True, "marbl": False}, "0.9.1", True),
        ({"cdr_lite": True, "marbl": False}, "0.10.0", False),
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


@pytest.mark.parametrize(
    "flag", ["cdr_output.do_cdr_output", "upscale_output.do_upscale"]
)
@pytest.mark.parametrize("marbl", [True, False])
def test_validate_run_time_sections_reports_marbl_only_outputs_without_marbl(
    flag, marbl
):
    """A stored or loaded blueprint with a MARBL-only output on but MARBL off is
    reported up front (engine/wizard), not first at configure_build.
    """
    section, name = flag.split(".")
    errs = validate_run_time_sections(
        {"param": dict(_PARAM), "cppdefs": {"marbl": marbl}, section: {name: True}}
    )
    assert any(f"{flag}=True" in e for e in errs) is (not marbl)


@pytest.mark.parametrize("marbl", [True, False])
def test_validate_run_time_sections_reports_gas_exchange_output_without_marbl(marbl):
    errs = validate_run_time_sections(
        {
            "param": dict(_PARAM),
            "cppdefs": {"marbl": marbl},
            "cdr_gas_exch_output": {"do_cdr_gas_exch_output": True},
        },
        roms_ref="0.9.0",
    )
    assert any("gas-exchange" in e for e in errs) is (not marbl)


# ---------------------------------------------------------------------------
# check_marbl_only_outputs
# ---------------------------------------------------------------------------
def test_marbl_only_outputs_pass_with_marbl_or_with_both_switches_off():
    on = {"cdr_output": {"do_cdr_output": True}, "upscale_output": {"do_upscale": True}}
    off = {
        "cdr_output": {"do_cdr_output": False},
        "upscale_output": {"do_upscale": False},
    }
    check_marbl_only_outputs(on, bgc_mode_is_marbl=True)
    check_marbl_only_outputs(off, bgc_mode_is_marbl=False)
    check_marbl_only_outputs({}, bgc_mode_is_marbl=False)


def test_marbl_only_outputs_name_every_switch_that_is_on_with_its_reason():
    settings = {
        "cdr_output": {"do_cdr_output": True},
        "upscale_output": {"do_upscale": True},
    }
    with pytest.raises(ValueError) as exc_info:
        check_marbl_only_outputs(settings, bgc_mode_is_marbl=False)
    message = str(exc_info.value)
    assert "cdr_output.do_cdr_output=True, upscale_output.do_upscale=True" in message
    assert "cdr_output.F90 only under MARBL && CDR_FORCING" in message
    assert "upscale_output.F90 only under MARBL && MARBL_DIAGS" in message


@pytest.mark.parametrize(
    ("section", "flag", "reason", "other_reason"),
    [
        (
            "cdr_output",
            "do_cdr_output",
            "cdr_output.F90 only under MARBL && CDR_FORCING",
            "upscale_output.F90",
        ),
        (
            "upscale_output",
            "do_upscale",
            "upscale_output.F90 only under MARBL && MARBL_DIAGS",
            "cdr_output.F90",
        ),
    ],
)
def test_marbl_only_outputs_name_only_the_switch_that_is_on(
    section, flag, reason, other_reason
):
    with pytest.raises(ValueError, match=rf"^{section}\.{flag}=True but") as exc_info:
        check_marbl_only_outputs({section: {flag: True}}, bgc_mode_is_marbl=False)
    message = exc_info.value.args[0]
    assert reason in message
    assert other_reason not in message
    assert message.count("=True") == 1


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
    # selects RunTimeSettingsV0_7_0 and 0.9.0 <= ucla-roms < 0.11.0
    # RunTimeSettingsV0_9_0 -- see the tests below.
    for ref in ("0.5.0", "v0.5.0"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_5_0


def test_run_time_settings_for_ref_0_6_0_up_to_0_7_0_selects_v0_6_0():
    # 0.6.0 <= ucla-roms < 0.7.0 selects RunTimeSettingsV0_6_0; 0.7.0 and later
    # select RunTimeSettingsV0_7_0/V0_9_0/V0_11_0 -- see the tests below.
    for ref in ("0.6.0", "v0.6.0"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_6_0


def test_run_time_settings_for_ref_0_7_0_up_to_0_9_0_selects_v0_7_0():
    for ref in ("0.7.0", "v0.7.0", "0.8.3"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_7_0


def test_run_time_settings_for_ref_0_9_0_up_to_0_11_0_selects_v0_9_0():
    for ref in ("0.9.0", "v0.9.0", "0.10.1", "0.10.2"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_9_0


def test_run_time_settings_for_ref_0_11_0_up_to_0_12_0_selects_v0_11_0():
    for ref in ("0.11.0", "v0.11.0", "0.11.3"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_11_0


def test_run_time_settings_for_ref_0_12_0_up_to_0_13_0_selects_v0_12_0(recwarn):
    for ref in ("0.12.0", "v0.12.0", "0.12.3"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_12_0
    assert len(recwarn) == 0


def test_run_time_settings_for_ref_0_13_0_and_later_selects_v0_13_0(recwarn):
    for ref in ("0.13.0", "v0.13.0"):
        assert run_time_settings_for_ref(ref) is RunTimeSettingsV0_13_0
    assert len(recwarn) == 0


_NEWER_THAN_KNOWN = "newer than the newest release this C-Star knows"
_NEWER_TAG = format_roms_version(
    (*NEWEST_KNOWN_UCLA_ROMS[:2], NEWEST_KNOWN_UCLA_ROMS[2] + 1)
)


@pytest.mark.parametrize(
    "ref",
    [
        _NEWER_TAG,
        format_roms_version((NEWEST_KNOWN_UCLA_ROMS[0] + 1, 0, 0)),
    ],
)
def test_run_time_settings_for_ref_newer_than_known_release_warns(ref):
    """A release above the newest one C-Star knows selects the latest tier
    with one warning naming the release, the schema and the newest known one.
    """
    with pytest.warns(UserWarning) as record:
        cls = run_time_settings_for_ref(ref)
    assert cls is RunTimeSettingsV0_13_0
    assert len(record) == 1
    message = str(record[0].message)
    assert f"ucla-roms release {ref} is {_NEWER_THAN_KNOWN}" in message
    assert f"({format_roms_version(NEWEST_KNOWN_UCLA_ROMS)})" in message
    assert RomsNamelistV0_13_0.__name__ in message


def test_run_time_settings_for_ref_hash_at_newer_than_known_release_warns():
    """Forge resolves a hash to its release tag without a clone; a tag newer
    than C-Star knows then warns when the tier is selected.
    """
    with mock.patch(
        "cstar.roms.namelist._describe_remote_nearest_tag",
        return_value=(_NEWER_TAG, 0),
    ):
        ref = release_ref_for_remote("/some/ucla-roms", "a" * 40)
    with pytest.warns(
        UserWarning, match=f"release {_NEWER_TAG} is {_NEWER_THAN_KNOWN}"
    ):
        cls = run_time_settings_for_ref(ref)
    assert cls is RunTimeSettingsV0_13_0


def test_write_roms_namelist_newer_than_known_release_warns(tmp_path):
    """The namelist Forge writes for a too-new release is the latest tier's,
    and writing it warns: the executor's tier probe silences warnings, the
    write does not.
    """
    with pytest.warns(UserWarning, match=_NEWER_THAN_KNOWN):
        write_roms_namelist(
            _populated_rt_dict(), tmp_path, n_tracers=34, roms_ref=_NEWER_TAG
        )
    RomsNamelistV0_13_0.read(tmp_path / "namelist.nml")


def test_run_time_settings_for_ref_branch_warns_and_uses_latest():
    with pytest.warns(UserWarning, match="not a release tag"):
        cls = run_time_settings_for_ref("main")
    assert cls is RunTimeSettingsV0_13_0


def test_run_time_settings_for_ref_unresolvable_hash_warns_and_uses_latest():
    """A commit hash needs ``repo_path`` (not passed here) to resolve to a
    release tag, so it falls back the same way an unparseable/branch ref does.
    """
    with pytest.warns(UserWarning, match="not a release tag"):
        cls = run_time_settings_for_ref("a1b2c3d4")
    assert cls is RunTimeSettingsV0_13_0


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
    RunTimeSettingsV0_5_0 instance (subclassing) -- proves ``build_namelist``
    selects ``RomsNamelistV0_7_0`` (with ``&pio_settings`` AND the two new CDR
    output groups), not one of its superclasses' namelist schemas.
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
    [
        RunTimeSettingsV0_5_0,
        RunTimeSettingsV0_6_0,
        RunTimeSettingsV0_7_0,
        RunTimeSettingsV0_9_0,
        RunTimeSettingsV0_11_0,
        RunTimeSettingsV0_12_0,
        RunTimeSettingsV0_13_0,
    ],
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
# ucla-roms >= 0.11.0: RunTimeSettingsV0_11_0 (marbl_bgc constant iron)
# ---------------------------------------------------------------------------
def test_build_namelist_v0_11_0_writes_constant_fe(tmp_path):
    """RunTimeSettingsV0_11_0 builds ``RomsNamelistV0_11_0`` and writes the
    constant-iron keys (lowercased, as f90nml does) with their values.
    """
    d = _populated_rt_dict()
    d["marbl_bgc"].update(use_constant_fe=True, constant_fe_value=5.0e-4)
    rt = RunTimeSettingsV0_11_0.model_validate(d)
    assert isinstance(rt, RunTimeSettingsV0_9_0)
    nml = build_namelist(rt, n_tracers=34)
    assert type(nml) is RomsNamelistV0_11_0
    assert nml.marbl_biogeochemistry_settings.use_constant_fe is True
    assert nml.marbl_biogeochemistry_settings.constant_fe_value == 5.0e-4

    nml.write(tmp_path / "namelist.nml")
    text = (tmp_path / "namelist.nml").read_text()
    assert "use_constant_fe = .true." in text
    assert "constant_fe_value = 0.0005" in text


def test_run_time_settings_v0_11_0_defaults_constant_fe_when_omitted(tmp_path):
    """A settings dict written before the keys existed validates to the ucla-roms
    reference values, and both keys are written regardless.
    """
    d = _populated_rt_dict()
    assert "use_constant_fe" not in d["marbl_bgc"]
    assert "constant_fe_value" not in d["marbl_bgc"]
    rt = RunTimeSettingsV0_11_0.model_validate(d)
    assert rt.marbl_bgc.use_constant_fe is False
    assert rt.marbl_bgc.constant_fe_value == 3.0e-3

    nml = build_namelist(rt, n_tracers=34)
    assert nml.marbl_biogeochemistry_settings.use_constant_fe is False
    assert nml.marbl_biogeochemistry_settings.constant_fe_value == 3.0e-3
    nml.write(tmp_path / "namelist.nml")
    text = (tmp_path / "namelist.nml").read_text()
    assert "use_constant_fe = .false." in text
    assert "constant_fe_value = 0.003" in text


def test_run_time_settings_v0_11_0_rejects_a_negative_constant_fe_value():
    d = _populated_rt_dict()
    d["marbl_bgc"]["constant_fe_value"] = -1.0e-3
    with pytest.raises(ValidationError, match="constant_fe_value"):
        RunTimeSettingsV0_11_0.model_validate(d)
    d["marbl_bgc"]["constant_fe_value"] = 0.0
    assert RunTimeSettingsV0_11_0.model_validate(d).marbl_bgc.constant_fe_value == 0.0


@pytest.mark.parametrize("settings_cls", [RunTimeSettings, RunTimeSettingsV0_9_0])
@pytest.mark.parametrize("requested", [True, "true", 1, "not-a-bool"])
def test_pre_0_11_0_tiers_reject_a_requested_constant_fe(settings_cls, requested):
    """ucla-roms < 0.11.0 has no constant-iron option: ``extra="ignore"`` would
    drop the request and the run would compute iron prognostically, so it is
    refused, naming the release that supports it. An unparseable value is not an
    "off" value either.
    """
    d = _populated_rt_dict()
    d["marbl_bgc"]["use_constant_fe"] = requested
    with pytest.raises(
        ValidationError,
        match=r"marbl_bgc\.use_constant_fe requires ucla-roms >= 0\.11\.0",
    ):
        settings_cls.model_validate(d)


@pytest.mark.parametrize("off", [False, "false", 0, None])
def test_pre_0_11_0_tiers_drop_a_switched_off_constant_fe(off):
    """A shared spec may carry the key switched off: it passes and is dropped,
    so the built namelist has no iron keys.
    """
    d = _populated_rt_dict()
    d["marbl_bgc"].update(use_constant_fe=off, constant_fe_value=5.0e-4)
    rt = RunTimeSettingsV0_9_0.model_validate(d)
    assert "use_constant_fe" not in rt.marbl_bgc.model_dump()
    assert "constant_fe_value" not in rt.marbl_bgc.model_dump()

    nml = build_namelist(rt, n_tracers=34)
    assert type(nml) is RomsNamelistV0_9_0
    written = nml.to_f90nml_dict()["marbl_biogeochemistry_settings"]
    assert "use_constant_fe" not in written
    assert "constant_fe_value" not in written


@pytest.mark.parametrize(
    "settings_cls", [RunTimeSettingsV0_9_0, RunTimeSettingsV0_11_0]
)
@pytest.mark.parametrize("value", [True, False])
def test_misspelled_constant_fe_is_rejected_on_every_tier(settings_cls, value):
    """ROMS spells the keys ``use_constant_Fe``/``constant_Fe_value``. Below
    0.11.0 the version gate reads only the exact key, so ``use_constant_Fe: true``
    would otherwise be dropped there and iron computed prognostically.
    """
    d = _populated_rt_dict()
    d["marbl_bgc"].update(use_constant_Fe=value, constant_Fe_value=5.0e-4)
    with pytest.raises(
        ValidationError,
        match=r"use_constant_Fe \(did you mean use_constant_fe\?\), "
        r"constant_Fe_value \(did you mean constant_fe_value\?\)",
    ):
        settings_cls.model_validate(d)


def test_marbl_bgc_cfg_tiers_share_the_four_marbl_fields():
    old = set(MarblBgcCfg.model_fields)
    new = set(MarblBgcCfgV0_11_0.model_fields)
    assert new - old == {"use_constant_fe", "constant_fe_value"}
    assert old <= new


def test_forge_writes_constant_fe_cstar_reads_it(tmp_path):
    """``write_roms_namelist`` for a 0.11.0 pin writes a namelist the C-Star
    schema for that release reads back, keys included.
    """
    d = _populated_rt_dict()
    d["marbl_bgc"].update(use_constant_fe=True, constant_fe_value=5.0e-4)
    write_roms_namelist(d, tmp_path, n_tracers=34, roms_ref="0.11.0")

    back = RomsNamelistV0_11_0.read(tmp_path / "namelist.nml")
    assert back.marbl_biogeochemistry_settings.use_constant_fe is True
    assert back.marbl_biogeochemistry_settings.constant_fe_value == 5.0e-4


# ---------------------------------------------------------------------------
# ucla-roms >= 0.12.0: RunTimeSettingsV0_12_0 (carbonate-sensitivity time
# interpolation in cdr_lite)
# ---------------------------------------------------------------------------
_INTERP = "cdr_carbonate_sensitivity_time_interpolation"


def test_build_namelist_v0_12_0_writes_carbonate_sensitivity_time_interpolation(
    tmp_path,
):
    """RunTimeSettingsV0_12_0 builds ``RomsNamelistV0_12_0`` and writes the key
    into ``&cdr_lite_settings``; the 0.11 constant-iron keys carry forward.
    """
    d = _populated_rt_dict()
    d["cdr_lite"] = {_INTERP: False}
    rt = RunTimeSettingsV0_12_0.model_validate(d)
    assert isinstance(rt, RunTimeSettingsV0_11_0)
    assert isinstance(rt.cdr_lite, CdrLiteCfgV0_12_0)
    nml = build_namelist(rt, n_tracers=34)
    assert type(nml) is RomsNamelistV0_12_0
    assert getattr(nml.cdr_lite_settings, _INTERP) is False
    assert nml.marbl_biogeochemistry_settings.use_constant_fe is False

    nml.write(tmp_path / "namelist.nml")
    assert f"{_INTERP} = .false." in (tmp_path / "namelist.nml").read_text()


def test_run_time_settings_v0_12_0_defaults_time_interpolation_when_omitted():
    """A settings dict without the key (or without ``cdr_lite`` at all) validates
    to interpolation on, the ucla-roms default and what earlier releases did.
    """
    d = _populated_rt_dict()
    d.pop("cdr_lite", None)
    rt = RunTimeSettingsV0_12_0.model_validate(d)
    assert getattr(rt.cdr_lite, _INTERP) is True
    nml = build_namelist(rt, n_tracers=34)
    assert nml.to_f90nml_dict()["cdr_lite_settings"][_INTERP] is True


@pytest.mark.parametrize(
    "settings_cls", [RunTimeSettingsV0_9_0, RunTimeSettingsV0_11_0]
)
@pytest.mark.parametrize("requested", [False, "false", 0, "not-a-bool"])
def test_pre_0_12_0_tiers_reject_a_requested_hold(settings_cls, requested):
    """ucla-roms < 0.12.0 always interpolates: ``extra="ignore"`` would drop a
    request to hold each record, so it is refused, naming the release that
    reads it. An unparseable value is not the default either.
    """
    d = _populated_rt_dict()
    d["cdr_lite"] = {_INTERP: requested}
    with pytest.raises(
        ValidationError,
        match=rf"cdr_lite\.{_INTERP}=False requires ucla-roms >= 0\.12\.0",
    ):
        settings_cls.model_validate(d)


@pytest.mark.parametrize("default", [True, "true", 1, None])
def test_pre_0_12_0_tiers_drop_interpolation_left_on(default):
    """A shared spec may carry the key at its default: it passes and is dropped,
    so the built namelist has no such key.
    """
    d = _populated_rt_dict()
    d["cdr_lite"] = {_INTERP: default}
    rt = RunTimeSettingsV0_11_0.model_validate(d)
    assert _INTERP not in rt.cdr_lite.model_dump()
    nml = build_namelist(rt, n_tracers=34)
    assert type(nml) is RomsNamelistV0_11_0
    assert _INTERP not in nml.to_f90nml_dict()["cdr_lite_settings"]


def test_cdr_lite_cfg_tiers_differ_by_the_time_interpolation_key():
    old = set(CdrLiteCfg.model_fields)
    assert set(CdrLiteCfgV0_12_0.model_fields) - old == {_INTERP}
    assert old <= set(CdrLiteCfgV0_12_0.model_fields)


def test_forge_writes_time_interpolation_cstar_reads_it(tmp_path):
    """``write_roms_namelist`` for a 0.12.0 pin writes a namelist the C-Star
    schema for that release reads back, key included.
    """
    d = _populated_rt_dict()
    d["cdr_lite"] = {_INTERP: False}
    write_roms_namelist(d, tmp_path, n_tracers=34, roms_ref="0.12.0")
    back = RomsNamelistV0_12_0.read(tmp_path / "namelist.nml")
    assert getattr(back.cdr_lite_settings, _INTERP) is False


# ---------------------------------------------------------------------------
# ucla-roms >= 0.13.0: RunTimeSettingsV0_13_0 (tracer diffusivity of the sponge)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("settings_cls", _ALL_SETTINGS_TIERS[:-1])
@pytest.mark.parametrize("requested", [0.0, 400.0, "0", "not-a-number", float("nan")])
def test_pre_0_13_0_tiers_reject_a_requested_v_sponge_trc(settings_cls, requested):
    """ucla-roms < 0.13.0 gives tracers ``v_sponge``: ``extra="ignore"`` would
    drop a request for a sponge of their own (0 = currents only, or a positive
    strength), so it is refused, naming the release that reads it. An
    unparseable value is not "same as v_sponge" either.
    """
    d = _populated_rt_dict()
    d["v_sponge"]["v_sponge_trc"] = requested
    with pytest.raises(
        ValidationError,
        match=r"v_sponge\.v_sponge_trc=.* requires ucla-roms >= 0\.13\.0",
    ):
        settings_cls.model_validate(d)


@pytest.mark.parametrize("absent", [-1.0, -0.5, "-1", None, "missing"])
def test_pre_0_13_0_tiers_drop_a_v_sponge_trc_meaning_same_as_v_sponge(absent):
    """Unset, or any negative value, means "same as v_sponge", which is what
    these releases do: it passes (a shared spec may carry it) and is dropped, so
    the built namelist has no such key.
    """
    d = _populated_rt_dict()
    if absent != "missing":
        d["v_sponge"]["v_sponge_trc"] = absent
    rt = RunTimeSettingsV0_12_0.model_validate(d)
    assert "v_sponge_trc" not in rt.v_sponge.model_dump()
    assert isinstance(rt.v_sponge, VSpongeCfg)

    nml = build_namelist(rt, n_tracers=34)
    assert type(nml) is RomsNamelistV0_12_0
    assert "v_sponge_trc" not in nml.to_f90nml_dict()["v_sponge_settings"]


def test_v_sponge_trc_rejection_names_the_version_from_the_constant():
    """The message formats ``UCLA_ROMS_0_13_0``, never a literal."""
    from cstar.roms.namelist import UCLA_ROMS_0_13_0

    version = ".".join(map(str, UCLA_ROMS_0_13_0))
    with pytest.raises(ValidationError, match=rf"requires ucla-roms >= {version}"):
        VSpongeCfg.model_validate({"v_sponge": 1.0, "v_sponge_trc": 0.0})


@pytest.mark.parametrize("value", [0.0, 400.0, -1.0, -5.0])
def test_build_namelist_v0_13_0_writes_v_sponge_trc(tmp_path, value):
    """RunTimeSettingsV0_13_0 builds ``RomsNamelistV0_13_0`` and writes the key
    into ``&v_sponge_settings``; negative values are valid (no bound).
    """
    d = _populated_rt_dict()
    d["v_sponge"]["v_sponge_trc"] = value
    rt = RunTimeSettingsV0_13_0.model_validate(d)
    assert isinstance(rt, RunTimeSettingsV0_12_0)
    assert isinstance(rt.v_sponge, VSpongeCfgV0_13_0)
    nml = build_namelist(rt, n_tracers=34)
    assert type(nml) is RomsNamelistV0_13_0
    assert nml.v_sponge_settings.v_sponge_trc == value
    assert nml.v_sponge_settings.v_sponge == 8333.33

    nml.write(tmp_path / "namelist.nml")
    assert f"v_sponge_trc = {value}" in (tmp_path / "namelist.nml").read_text()


def test_run_time_settings_v0_13_0_defaults_v_sponge_trc_when_omitted():
    """A settings dict without the key validates to -1 (tracers get ``v_sponge``),
    the ucla-roms default and what earlier releases did, and the namelist
    carries it.
    """
    d = _populated_rt_dict()
    d["v_sponge"].pop("v_sponge_trc", None)
    rt = RunTimeSettingsV0_13_0.model_validate(d)
    assert rt.v_sponge.v_sponge_trc == -1.0
    nml = build_namelist(rt, n_tracers=34)
    assert nml.to_f90nml_dict()["v_sponge_settings"]["v_sponge_trc"] == -1.0


def test_v_sponge_cfg_tiers_differ_by_the_tracer_diffusivity_key():
    old = set(VSpongeCfg.model_fields)
    assert set(VSpongeCfgV0_13_0.model_fields) - old == {"v_sponge_trc"}
    assert old <= set(VSpongeCfgV0_13_0.model_fields)


@pytest.mark.parametrize(
    "settings_cls", [RunTimeSettingsV0_12_0, RunTimeSettingsV0_13_0]
)
def test_misspelled_v_sponge_trc_is_rejected_on_every_tier(settings_cls):
    """Settings keys are lowercase; a case variant would be dropped by
    ``extra="ignore"`` on the old tier, so it fails there like a field's own.
    """
    d = _populated_rt_dict()
    d["v_sponge"]["V_Sponge_Trc"] = 0.0
    with pytest.raises(
        ValidationError, match=r"V_Sponge_Trc \(did you mean v_sponge_trc\?\)"
    ):
        settings_cls.model_validate(d)


def test_forge_writes_v_sponge_trc_cstar_reads_it(tmp_path):
    """``write_roms_namelist`` for a 0.13.0 pin writes a namelist the C-Star
    schema for that release reads back, key included.
    """
    d = _populated_rt_dict()
    d["v_sponge"]["v_sponge_trc"] = 0.0
    write_roms_namelist(d, tmp_path, n_tracers=34, roms_ref="0.13.0")
    back = RomsNamelistV0_13_0.read(tmp_path / "namelist.nml")
    assert back.v_sponge_settings.v_sponge_trc == 0.0


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


def _held(online=False, param=None):
    settings = _lite(online=online, param=param)
    settings["cdr_lite"][_INTERP] = False
    return settings


def test_check_cdr_lite_sections_accepts_a_hold_with_file_based_sensitivities():
    """``bgc_mode: cdr_lite`` with the online knob off reads ddic_dco2/ddic_dalk
    from forcing files, the one case where ROMS reads the switch.
    """
    assert check_cdr_lite_sections(
        _held(param={}), bgc_mode="cdr_lite", settings_cls=RunTimeSettingsV0_12_0
    )


@pytest.mark.parametrize(
    ("settings", "mode", "match"),
    [
        (_held(online=True), "marbl", "has no effect with cdr_lite.cdr_online"),
        (_held(param={}), "marbl", 'has no effect without CDR_LITE.*"cdr_lite" only'),
        (_held(param={}), "none", 'has no effect without CDR_LITE.*"cdr_lite" only'),
    ],
)
def test_check_cdr_lite_sections_rejects_a_hold_roms_never_reads(settings, mode, match):
    """Switched off where ROMS never reads it -- online sensitivities, or no
    CDR_LITE build -- the switch would silently do nothing.
    """
    with pytest.raises(ValueError, match=rf"cdr_lite\.{_INTERP}=False {match}"):
        check_cdr_lite_sections(
            settings, bgc_mode=mode, settings_cls=RunTimeSettingsV0_12_0
        )


def test_validate_run_time_sections_reports_a_hold_outside_cdr_lite_mode():
    """A stored 0.12 blueprint asking for a hold in a MARBL build is reported
    up front (engine/wizard), with the tier gate silent since the key is known.
    """
    settings = {
        **_held(param={}),
        "cppdefs": {"marbl": True, "cdr_lite": False},
    }
    errs = validate_run_time_sections(settings, roms_ref="0.12.0")
    assert any(f"cdr_lite.{_INTERP}=False has no effect" in e for e in errs), errs


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
@pytest.mark.parametrize("ref", ["0.10.0", "v0.10.0", "0.10.2", "0.11.0", "1.0.0"])
def test_check_cdr_lite_mode_roms_accepts_releases_from_the_minimum(ref):
    assert CDR_LITE_MODE_MIN_ROMS == (0, 10, 0)
    check_cdr_lite_mode_roms(ref)


@pytest.mark.parametrize("ref", ["main", "pio-dev", "abc1234", "", None])
def test_check_cdr_lite_mode_roms_treats_a_non_tag_ref_as_the_latest(ref):
    check_cdr_lite_mode_roms(ref)


@pytest.mark.parametrize("ref", ["0.9.1", "0.9.0", "0.8.0", "v0.7.1"])
def test_check_cdr_lite_mode_roms_rejects_older_releases(ref):
    with pytest.raises(ValueError, match=r"ucla-roms >= 0\.10\.0"):
        check_cdr_lite_mode_roms(ref)


# ---------------------------------------------------------------------------
# check_river_tracer_counts / river_tracer_floor (ucla-roms PR #369)
# ---------------------------------------------------------------------------
# T + S + 32 MARBL + 1 passive + 2*1 OAE + 1 DOR = 38 tracers; 35 without CDR.
_RIVER_PARAM = {"ntrc_bio": 32, "nt_passive": 1, "nt_cdr_oae": 1, "nt_cdr_dor": 1}
_RIVER_NT = n_tracers_from_param(_RIVER_PARAM)


def _check_river(ntracers, *, param=_RIVER_PARAM, roms_ref="0.10.0", **files):
    river = {"river.nc": ntracers, **files}
    check_river_tracer_counts(
        river,
        n_tracers=n_tracers_from_param(param),
        param=param,
        roms_ref=roms_ref,
    )


def test_river_tracer_floor_leaves_out_the_cdr_block():
    assert _RIVER_NT == 38
    assert river_tracer_floor(_RIVER_PARAM) == 35
    assert river_tracer_floor({}) == 2


def test_check_river_tracer_counts_gate_is_ucla_roms_0_9_0():
    assert RIVER_TRACERS_WITHOUT_CDR_MIN_ROMS == (0, 9, 0)


@pytest.mark.parametrize("ntracers", [38, 35])
def test_check_river_tracer_counts_accepts_full_and_without_cdr(ntracers):
    _check_river(ntracers)


@pytest.mark.parametrize("ntracers", [39, 37, 34, 2])
def test_check_river_tracer_counts_rejects_other_lengths(ntracers):
    with pytest.raises(ValueError) as exc:
        _check_river(ntracers)
    msg = str(exc.value)
    assert f"river forcing river.nc has {ntracers} tracers" in msg
    assert "accepts only 38 (every model tracer" in msg
    assert "or 35 (every tracer but the CDR block" in msg


def test_check_river_tracer_counts_rejects_a_bgc_file_for_a_physics_only_run():
    with pytest.raises(ValueError, match=r"has 34 tracers.*accepts only 2 \(") as exc:
        _check_river(34, param={})
    # No CDR block: only one length is allowed, so no "or ..." alternative.
    assert " or " not in str(exc.value)


def test_check_river_tracer_counts_needs_one_length_in_every_file():
    with pytest.raises(ValueError, match="same tracer count in every river"):
        _check_river(38, **{"river2.nc": 35})


@pytest.mark.parametrize("ref", ["main", "pio-dev", "", None, "0.9.0"])
def test_check_river_tracer_counts_is_strict_from_0_9_0_and_on_non_tags(ref):
    _check_river(35, roms_ref=ref)
    with pytest.raises(ValueError, match="accepts only"):
        _check_river(39, roms_ref=ref)


def test_check_river_tracer_counts_old_release_warns_on_extra_tracers():
    with pytest.warns(UserWarning, match=r"ucla-roms 0\.8\.0 reads the first 2"):
        _check_river(34, param={}, roms_ref="0.8.0")


@pytest.mark.parametrize("ntracers", [37, 35])
def test_check_river_tracer_counts_old_release_rejects_a_shorter_file(ntracers):
    """Before 0.9.0 ucla-roms reads all nt slots, CDR block included."""
    with pytest.raises(ValueError, match=r"ucla-roms 0\.8\.0 reads 38 from it"):
        _check_river(ntracers, roms_ref="0.8.0")


def test_check_river_tracer_counts_without_files_is_a_no_op():
    check_river_tracer_counts({}, n_tracers=38, param=_RIVER_PARAM, roms_ref=None)


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


# ---------------------------------------------------------------------------
# cdr_tracer_role
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("name", "role"),
    [
        ("CDR_OAE_ALK1", ("cdr_lite", "ALK")),
        ("CDR_OAE_DIC12", ("cdr_lite", "DIC")),
        ("CDR_DOR_DIC2", ("cdr_lite", "DIC")),
        ("passive_tracer3", ("passive", "passive_tracer")),
        ("temp", None),
        ("salt", None),
        ("ALK", None),
        ("DIC", None),
        ("CDR_OAE_ALK", None),
        ("CDR_DOR_ALK1", None),
        ("passive_tracer", None),
    ],
)
def test_cdr_tracer_role_maps_generated_names_to_set_and_flux_key(name, role):
    assert cdr_tracer_role(name) == role


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
    assert any("== 0" in e for e in validate_run_time_sections(stored, "0.10.0"))
    stored["cppdefs"] = {"marbl": False, "cdr_lite": True}
    assert validate_run_time_sections(stored, "0.10.0") == []

    counted = {**_lite(), "cppdefs": {"marbl": False}}
    assert any(
        "Forcing type not supported" in e
        for e in validate_run_time_sections(counted, "0.10.0")
    )
    counted["cppdefs"] = {"marbl": False, "cdr_lite": True}
    assert validate_run_time_sections(counted, "0.10.0") == []
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
# canonical_output_sections_for_precheck
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


# ---------------------------------------------------------------------------
# namelist_field_for -- forge settings (section, field) -> namelist (group, key),
# the vocabulary bridge build_namelist writes through (the wizard's tooltips
# read it too).
# ---------------------------------------------------------------------------
def _settings_fields(settings_cls):
    """Every forge settings (section, field) of ``settings_cls``; a scalar
    section (``gamma2``) is its own field.
    """
    for section, info in settings_cls.model_fields.items():
        cfg_fields = getattr(info.annotation, "model_fields", None)
        for field in cfg_fields or (section,):
            yield section, field


@pytest.mark.parametrize("settings_cls", _ALL_SETTINGS_TIERS)
def test_namelist_field_for_maps_every_settings_field(settings_cls):
    """Every settings field of every tier lands on a (group, key) the tier's
    namelist schema declares. A section added to RunTimeSettings without a
    _ONE_TO_ONE_GROUPS/_STRUCTURAL_GROUPS entry fails here.
    """
    schema = namelist_schema_for(settings_cls)
    for section, field in _settings_fields(settings_cls):
        target = namelist_field_for(settings_cls, section, field)
        assert target is not None, (section, field)
        group, key = target
        assert group in schema.model_fields, (section, field, group)
        assert key in schema.model_fields[group].annotation.model_fields, (
            section,
            field,
            group,
            key,
        )


@pytest.mark.parametrize("settings_cls", _ALL_SETTINGS_TIERS)
def test_namelist_field_for_agrees_with_build_namelist(settings_cls):
    """Each settings value is found in the built namelist at the (group, key)
    namelist_field_for names -- the structural sections' mapping is written
    beside build_namelist, not read by it, so this pins the two together.
    """
    rt = settings_cls.model_validate(_populated_rt_dict())
    n_tracers = 34
    nml = build_namelist(rt, n_tracers=n_tracers)
    for section, field in _settings_fields(settings_cls):
        section_value = getattr(rt, section)
        value = getattr(section_value, field, section_value)  # scalar: itself
        group, key = namelist_field_for(settings_cls, section, field)
        written = getattr(getattr(nml, group), key)
        if section == "forcing":  # every *_path is assembled into frcfiles
            paths = value if isinstance(value, list) else [value] * (value is not None)
            assert set(paths) <= set(written), (section, field)
        else:  # a per-tracer default is expanded to one entry per tracer
            assert written in (value, [value] * n_tracers), (section, field)


@pytest.mark.parametrize(
    ("section", "field", "expected"),
    [
        ("param", "n", ("param_settings", "nz")),
        ("param", "np_xi", ("param_settings", "np_xi")),
        ("param", "nt_cdr_oae", ("param_settings", "nt_cdr_oae")),
        ("title", "casename", ("simulation_name_settings", "title")),
        ("lateral_visc", "rho0", ("rho0_settings", "rho0")),
        ("vertical_mixing", "akt_default", ("vertical_mixing_settings", "akt_bak")),
        ("forcing", "river_path", ("forcing_files", "frcfiles")),
        ("gamma2", "gamma2", ("gamma2_settings", "gamma2")),
        (
            "extract_data",
            "extract_period",
            ("extract_data_settings", "output_period_extract"),
        ),
        ("cdr_lite_output", "nrpf", ("cdr_lite_output_settings", "nrpf_cdr_lite")),
    ],
)
def test_namelist_field_for_examples(section, field, expected):
    assert namelist_field_for(RunTimeSettingsV0_9_0, section, field) == expected


@pytest.mark.parametrize("field", ["use_constant_fe", "constant_fe_value"])
def test_namelist_field_for_constant_fe_is_a_0_11_0_field(field):
    assert namelist_field_for(RunTimeSettingsV0_11_0, "marbl_bgc", field) == (
        "marbl_biogeochemistry_settings",
        field,
    )
    assert namelist_field_for(RunTimeSettingsV0_9_0, "marbl_bgc", field) is None


def test_namelist_field_for_follows_the_tier_group_rename():
    """``cdr_lite_output`` is &CDR_TRACER_OUTPUT_SETTINGS on 0.7/0.8."""
    assert namelist_field_for(RunTimeSettingsV0_7_0, "cdr_lite_output", "nrpf") == (
        "cdr_tracer_output_settings",
        "nrpf_cdr_trc",
    )


@pytest.mark.parametrize(
    ("settings_cls", "section", "field"),
    [
        (RunTimeSettingsV0_9_0, "cppdefs", "marbl"),  # not a namelist section
        (RunTimeSettingsV0_9_0, "param", "no_such_key"),  # not a typed field
        (RunTimeSettingsV0_5_0, "pio_settings", "pio_stride"),  # other tier's section
    ],
)
def test_namelist_field_for_returns_none_off_the_namelist(settings_cls, section, field):
    assert namelist_field_for(settings_cls, section, field) is None


@pytest.mark.parametrize(
    "roms_ref", ["0.5.0", "0.6.0", "0.7.0", "0.8.0", "0.9.0", "0.11.0"]
)
def test_precheck_sections_match_what_build_namelist_writes(roms_ref):
    """The precheck sees every output-stream section the tier carries, under
    the group and keys build_namelist writes it with.
    """
    settings_cls = run_time_settings_for_ref(roms_ref)
    d = _populated_rt_dict()
    canonical = canonical_output_sections_for_precheck(d, settings_cls)
    nml = build_namelist(settings_cls.model_validate(d), n_tracers=34)
    present = [s for s in _PRECHECK_SECTIONS if s in settings_cls.model_fields]
    assert len(canonical) == len(present)
    for group, keys in canonical.items():
        written = getattr(nml, group).model_dump()
        assert keys == {k: written[k] for k in keys}, (roms_ref, group)


def _surf_flux(**switches):
    """The bundled ``daily-restarts`` surface-flux section with ``switches`` set."""
    return {**default_catalog.output_data("daily-restarts")["surf_flux"], **switches}


@pytest.mark.parametrize("settings_cls", _ALL_SETTINGS_TIERS[:-1])
def test_pre_0_13_0_tiers_reject_wrt_rstflx(settings_cls):
    """ucla-roms < 0.13.0 reads ``wrt_rstflx`` but writes no restoring flux."""
    annotation = settings_cls.model_fields["surf_flux"].annotation
    with pytest.raises(
        ValidationError, match=r"surf_flux\.wrt_rstflx requires ucla-roms >= 0\.13\.0"
    ):
        TypeAdapter(annotation).validate_python(_surf_flux(wrt_rstflx=True))
    TypeAdapter(annotation).validate_python(_surf_flux(wrt_rstflx=False))


def test_0_13_0_tier_writes_wrt_rstflx():
    d = _populated_rt_dict()
    d["surf_flux"] = _surf_flux(wrt_rstflx=True)
    nml = build_namelist(RunTimeSettingsV0_13_0.model_validate(d), 7)
    assert nml.surf_flx_output_settings.wrt_rstflx is True


@pytest.mark.parametrize(
    ("settings_cls", "sal_restore", "rejected"),
    [
        (RunTimeSettingsV0_13_0, False, True),
        (RunTimeSettingsV0_13_0, True, False),
        # The tier refuses the switch itself (test_pre_0_13_0_tiers_reject_wrt_rstflx).
        (RunTimeSettingsV0_12_0, False, False),
    ],
)
def test_check_restoring_flux_output_needs_a_restoring_term(
    settings_cls, sal_restore, rejected
):
    """ucla-roms >= 0.13.0 stops at startup on ``wrt_rstflx`` with no restoring term
    compiled in; SSS restoring (SFLX_CORR) is the one a Forge build can have.
    """
    surf_flux = _surf_flux(wrt_rstflx=True)
    if rejected:
        with pytest.raises(ValueError, match=r"wrt_rstflx .*restores nothing"):
            check_restoring_flux_output(
                surf_flux, sal_restore=sal_restore, settings_cls=settings_cls
            )
    else:
        check_restoring_flux_output(
            surf_flux, sal_restore=sal_restore, settings_cls=settings_cls
        )
    check_restoring_flux_output(
        _surf_flux(wrt_rstflx=False), sal_restore=False, settings_cls=settings_cls
    )


@pytest.mark.parametrize(
    ("roms_ref", "expected"),
    [
        ("0.13.0", r"wrt_rstflx .*restores nothing"),
        ("0.12.0", r"surf_flux\.wrt_rstflx requires ucla-roms >= 0\.13\.0"),
    ],
)
def test_validate_run_time_sections_reports_the_restoring_flux_switch(
    roms_ref, expected
):
    """Stored blueprints fail before generation, with one error naming the fix."""
    errors = validate_run_time_sections(
        {"surf_flux": _surf_flux(wrt_rstflx=True), "cppdefs": {"sal_restore": False}},
        roms_ref=roms_ref,
    )
    assert len(errors) == 1, errors
    assert re.search(expected, errors[0]), errors
    assert (
        validate_run_time_sections(
            {
                "surf_flux": _surf_flux(wrt_rstflx=True),
                "cppdefs": {"sal_restore": True},
            },
            roms_ref="0.13.0",
        )
        == []
    )
