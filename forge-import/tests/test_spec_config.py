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


# ---------------------------------------------------------------------------
# Wizard (headless: ipywidgets value get/set/observe work without rendering)
# ---------------------------------------------------------------------------
class TestSpecConfigWizard:
    def _wizard(self):
        pytest.importorskip("ipywidgets")
        from cstar_forge.spec_config_wizard import SpecConfigWizard
        return SpecConfigWizard()

    def test_init_resolves_default_config(self):
        wiz = self._wizard()
        assert isinstance(wiz.config, SpecConfig)
        assert wiz.config.casename  # derived, non-empty

    def test_selecting_catalog_domain_prefills_and_resolves(self):
        wiz = self._wizard()
        if "gulf-guinea-toy" not in wiz.domain_dd.options:
            pytest.skip("gulf-guinea-toy domain not in catalog")
        wiz.domain_dd.value = "gulf-guinea-toy"  # triggers prefill + rebuild
        cfg = wiz.config
        assert cfg.identity.grid_name == "gulf-guinea-toy"
        assert cfg.domain.grid_kwargs["nx"] == 10 and cfg.domain.grid_kwargs["N"] == 5
        assert (cfg.domain.partitioning.n_procs_x, cfg.domain.partitioning.n_procs_y) == (2, 5)
        assert cfg.domain.open_boundaries.south is True
        # this domain doesn't specify s-coord -> not injected
        assert "theta_s" not in cfg.domain.grid_kwargs

    def test_editing_boundary_updates_cppdefs_live(self):
        wiz = self._wizard()
        wiz.bnd["west"].value = True
        assert wiz.config.model_settings["cppdefs"]["obc_west"] is True
        wiz.bnd["west"].value = False
        assert wiz.config.model_settings["cppdefs"]["obc_west"] is False

    def test_ensemble_id_feeds_derived_name(self):
        wiz = self._wizard()
        wiz.ensemble.value = "3"
        assert wiz.config.identity.ensemble_id == 3
        assert wiz.config.name.endswith("_003")

    def test_save_writes_valid_yaml(self, tmp_path):
        wiz = self._wizard()
        wiz.save_path.value = str(tmp_path / "spec_config.yml")
        wiz._on_save(None)
        cfg = SpecConfig.from_yaml(tmp_path / "spec_config.yml")
        assert cfg.casename == wiz.config.casename

    def test_download_link_encodes_the_config(self):
        """The browser-download link (used by Voilà) carries the resolved YAML."""
        import base64
        import re

        wiz = self._wizard()
        html = wiz.download_link.value
        assert 'download="' in html and "data:text/yaml;base64," in html
        b64 = re.search(r"base64,([A-Za-z0-9+/=]+)", html).group(1)
        text = base64.b64decode(b64).decode("utf-8")
        assert "spec_config_version" in text
        # casename is derived (not serialized) — it appears in the download filename
        assert wiz.config.casename in html


# ---------------------------------------------------------------------------
# Phase 2 engine (orchestration tested with an injected fake builder; the real
# pipeline downloads data + runs roms_tools and is out of scope for unit tests)
# ---------------------------------------------------------------------------
class _FakeBuilder:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []

    def ensure_source_data(self, **k):
        self.calls.append(("ensure", k))

    def generate_inputs(self, **k):
        self.calls.append(("generate", k))

    def configure_build(self, **k):
        self.calls.append(("configure", k))

    def path_blueprint(self, stage=None):
        return f"/bp/{stage}.yml"


class TestSpecConfigEngine:
    def _cfg(self):
        return _build()

    def test_builder_kwargs_carry_atomic_inputs_not_host(self):
        from cstar_forge.spec_config_engine import spec_config_to_builder_kwargs
        kw = spec_config_to_builder_kwargs(self._cfg())
        assert kw["model_name"] == "cson_roms-marbl_v0.1"
        assert kw["grid_name"] == "test-tiny"
        assert kw["partitioning"] == {"n_procs_x": 1, "n_procs_y": 1}
        assert kw["open_boundaries"]["east"] is True
        # host/machine/paths must NOT be passed (builder resolves them)
        assert not any(k in kw for k in ("machine", "paths", "scratch", "source_data"))

    def test_split_model_settings(self):
        from cstar_forge.spec_config_engine import split_model_settings, PROCESSING_FILLED_SECTIONS
        run_ov, comp_ov = split_model_settings(self._cfg())
        assert list(comp_ov) == ["cppdefs"] and "cppdefs" not in run_ov
        assert "time_stepping" in run_ov and "param" in run_ov
        for sec in PROCESSING_FILLED_SECTIONS:
            assert sec not in run_ov

    def test_process_orchestration_order_and_overlay(self):
        from cstar_forge.spec_config_engine import process_spec_config
        b = process_spec_config(self._cfg(), clobber=True, use_dask=False,
                                builder_factory=_FakeBuilder)
        assert [c[0] for c in b.calls] == ["ensure", "generate", "configure"]
        gen = dict(b.calls[1][1])
        assert gen["clobber"] is True and gen["use_dask"] is False
        cfgk = dict(b.calls[2][1])
        assert "cppdefs" in cfgk["compile_time_settings"]
        assert "time_stepping" in cfgk["run_time_settings"]
        assert "grid" not in cfgk["run_time_settings"]

    def test_process_skip_flags(self):
        from cstar_forge.spec_config_engine import process_spec_config
        b = process_spec_config(self._cfg(), ensure_data=False, generate=False,
                                builder_factory=_FakeBuilder)
        assert [c[0] for c in b.calls] == ["configure"]

    def test_resolve_host_reads_config_not_file(self):
        from cstar_forge.spec_config_engine import resolve_host
        h = resolve_host(self._cfg())
        assert h["system"]
        assert set(h["paths"]) == {"source_data", "input_data", "scratch", "catalog"}
        # host-derived run paths use the resolved scratch + derived casename
        assert h["casename"].endswith("20120101-20120102")
        assert h["run_output_dir"].endswith(h["casename"])
        assert h["output_root_name"].endswith(f"output/{h['casename']}")


# ---------------------------------------------------------------------------
# Step 3 (parity): the Phase-1 resolver and the live CstarSpecBuilder must agree
# on the derived values, so a reviewed config matches a from-scratch build.
#
# Compared at *construction* (no generate_inputs): the genuinely-computed numerics
# (dt/ntimes via CFL, v_sponge) and every shared default section. Sections that the
# two paths fill at different times are excluded: ``param`` and ``cppdefs`` (obc) are
# set by the builder's grid handler during generation, not at init; ``title`` /
# ``output_root_name`` / ``grid`` / ``initial`` / ``forcing`` / ``s_coord`` are
# host/artifact-derived (the resolver omits them by design).
# ---------------------------------------------------------------------------
_PARITY_DOMAINS = [
    ("test-tiny",
     dict(nx=6, ny=2, size_x=500, size_y=1000, center_lon=0, center_lat=55,
          rot=10, N=3, theta_s=5.0, theta_b=2.0, hc=250.0),
     {"south": False, "east": True, "north": True, "west": False},
     {"n_procs_x": 1, "n_procs_y": 1}),
    ("gulf-guinea-toy",
     dict(nx=10, ny=10, size_x=4000, size_y=2000, center_lon=4.0, center_lat=-1.0,
          rot=0, N=5),
     {"south": True, "east": True, "north": True, "west": True},
     {"n_procs_x": 2, "n_procs_y": 5}),
]

# Sections filled at different times / by different layers — not comparable at init.
_PARITY_SKIP = {"param", "cppdefs", "title", "output_root_name",
                "grid", "initial", "forcing", "s_coord"}


@pytest.mark.integration
class TestResolverBuilderParity:
    @pytest.mark.parametrize("grid_name,grid_kwargs,boundaries,partitioning",
                             _PARITY_DOMAINS)
    def test_resolver_matches_builder_derivation(
        self, grid_name, grid_kwargs, boundaries, partitioning, tmp_path
    ):
        pytest.importorskip("roms_tools")
        from datetime import datetime
        from cstar_forge._core import CstarSpecBuilder
        from cstar_forge.spec_config_resolve import build_spec_config

        start, end = datetime(2012, 1, 1), datetime(2012, 1, 2)

        # Real builder (no mocks): real ModelSpec defaults + real geometric grid;
        # persistence isolated to a temp catalog copied from the bundled one.
        builder = CstarSpecBuilder(
            description="parity", model_name="cson_roms-marbl_v0.1",
            grid_name=grid_name, grid_kwargs=grid_kwargs,
            open_boundaries=boundaries, partitioning=partitioning,
            start_time=start, end_time=end,
            catalog_root=str(tmp_path / "catalog"), initialize_catalog_from="local",
        )
        b_rt = builder._settings_run_time

        # Resolver with dt=None -> the same CFL path the builder uses.
        cfg = build_spec_config(
            model_dir=builder._get_catalog().model_dir("cson_roms-marbl_v0.1"),
            grid_name=grid_name, grid_kwargs=grid_kwargs,
            open_boundaries=boundaries, partitioning=partitioning,
            start_date=start, end_date=end,
        )
        r_ms = cfg.model_settings

        # the genuinely-computed numerics must match exactly
        assert r_ms["time_stepping"] == b_rt["time_stepping"]
        assert r_ms["v_sponge"] == b_rt["v_sponge"]

        # every shared default section must be identical between the two paths
        mismatches = {
            sec: (b_rt.get(sec), rval)
            for sec, rval in r_ms.items()
            if sec not in _PARITY_SKIP and b_rt.get(sec) != rval
        }
        assert not mismatches, f"resolver/builder drift: {mismatches}"
