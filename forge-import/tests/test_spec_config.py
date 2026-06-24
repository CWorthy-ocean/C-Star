"""
Tests for the SpecConfig schema (``cstar_forge.spec_config``) and the Phase-1
resolver (``cstar_forge.spec_config_resolve.build_spec_config``).

These validate that the resolver reproduces the known ``test-tiny`` demo values,
flattens settings, keeps naming/host values out of the stored config, resolves
sources from the ModelSpec, and round-trips through YAML.

NOTE: imports the in-package modules, so these run once the environment's editable
``cstar`` provides ``cstar.roms.namelist`` (i.e. on the namelist branch). The same
assertions were validated standalone during development.
"""
from datetime import datetime
from pathlib import Path

import pytest
import yaml

import cstar_forge
from cstar_forge.spec_config import SpecConfig
from cstar_forge.spec_config_resolve import build_spec_config

_MODEL_DIR = (Path(cstar_forge.__file__).parent / "catalog" / "ModelSpec"
              / "cson_roms-marbl_v0.1")
_GRID_KWARGS = dict(nx=6, ny=2, size_x=500, size_y=1000, center_lon=0, center_lat=55,
                    rot=10, N=3, theta_s=5.0, theta_b=2.0, hc=250.0)
_BOUNDARIES = {"south": False, "east": True, "north": True, "west": False}
_PART = {"n_procs_x": 1, "n_procs_y": 1}


def _build(**over):
    kw = dict(
        model_dir=_MODEL_DIR, grid_name="test-tiny", grid_kwargs=_GRID_KWARGS,
        open_boundaries=_BOUNDARIES, partitioning=_PART,
        start_date=datetime(2012, 1, 1), end_date=datetime(2012, 1, 2),
        description="Test tiny", dt=7200,  # pass dt -> stays dependency-light
    )
    kw.update(over)
    return build_spec_config(**kw)


def test_naming_is_derived_not_stored():
    cfg = _build()
    assert cfg.n_procs == 1
    assert cfg.name == "cson_roms-marbl_v0.1_test-tiny_1procs"
    assert cfg.casename == "cson_roms-marbl_v0.1_test-tiny_1procs_20120101-20120102"
    # output_root_name is host-derived from the scratch path
    assert cfg.output_root_name("/scratch").startswith("/scratch/cson_roms-marbl")


def test_timestepping_and_param_match_known_run():
    cfg = _build()
    assert cfg.model_settings["time_stepping"] == {
        "ntimes": 12, "dt": 7200, "ndtfast": 60, "ninfo": 1}
    p = cfg.model_settings["param"]
    assert (p["llm"], p["mmm"], p["n"]) == (6, 2, 3)        # from grid nx/ny/N
    assert (p["np_xi"], p["np_eta"]) == (1, 1)              # from partitioning
    assert (p["nsub_x"], p["nsub_e"]) == (1, 1)
    assert p["ntrc_bio"] == 32                              # from defaults


def test_cppdefs_obc_from_boundaries_and_cdr_flag():
    cfg = _build(cdr_forcing={"releases": []})
    c = cfg.model_settings["cppdefs"]
    assert c["obc_west"] is False and c["obc_east"] is True
    assert c["obc_north"] is True and c["obc_south"] is False
    assert c["cdr_forcing"] is True and c["marbl"] is True


def test_settings_is_flat_and_omits_processing_filled_sections():
    cfg = _build()
    ms = cfg.model_settings
    assert "cppdefs" in ms and "lateral_visc" in ms  # cppdefs flat alongside namelist
    for excluded in ("grid", "initial", "forcing", "s_coord", "title", "output_root_name"):
        assert excluded not in ms


def test_sources_resolved_from_modelspec():
    cfg = _build()
    s = cfg.sources
    assert s.initial_conditions.source.dataset_key == "GLORYS_REGIONAL"
    assert s.initial_conditions.bgc_source.dataset_key == "UNIFIED_BGC"
    assert [i.source.name for i in s.forcing.surface] == ["ERA5", "UNIFIED"]
    assert s.forcing.tidal[0].ntides == 15
    assert s.forcing.river[0].include_bgc is True
    assert s.resolved_datasets["GLORYS"].dataset_id == "cmems_mod_glo_phy_my_0.083deg_P1D-m"


def test_templates_are_repo_refs():
    cfg = _build()
    t = cfg.code.templates_compile_time
    assert t.location.endswith("cstar-forge.git")
    assert t.files == ["cppdefs.opt.j2"]
    assert cfg.code.templates_run_time.files == ["marbl_in"]
    assert cfg.code.roms.commit == "391de2798ca9d6e9b63ba5471b6e62e96043e177"


def test_no_host_or_machine_in_config():
    cfg = _build()
    d = cfg.model_dump()
    assert "machine" not in d and "execution" not in d and "paths" not in d
    assert "conventions" not in d


def test_overrides_take_precedence():
    cfg = _build(run_time_overrides={"v_sponge": {"v_sponge": 42.0},
                                     "time_stepping": {"ndtfast": 30}})
    assert cfg.model_settings["v_sponge"]["v_sponge"] == 42.0
    assert cfg.model_settings["time_stepping"]["ndtfast"] == 30


def test_composition_records_piece_provenance():
    cfg = _build()
    assert cfg.composition.model.origin == "catalog"
    assert cfg.composition.model.name == "cson_roms-marbl_v0.1"
    assert cfg.composition.domain.name == "test-tiny"


def test_yaml_round_trip(tmp_path):
    cfg = _build()
    p = cfg.to_yaml(tmp_path / "spec_config.yml")
    back = SpecConfig.from_yaml(p)
    assert back.casename == cfg.casename
    assert back.model_settings["time_stepping"] == cfg.model_settings["time_stepping"]


def test_committed_example_validates():
    """The checked-in example must remain a valid SpecConfig."""
    example = Path(cstar_forge.__file__).parents[1] / "docs" / "spec-config-example.test-tiny.yml"
    if not example.exists():
        pytest.skip("example file not present")
    cfg = SpecConfig.from_yaml(example)
    assert cfg.identity.model_name == "cson_roms-marbl_v0.1"
    assert cfg.composition.model.origin == "catalog"
