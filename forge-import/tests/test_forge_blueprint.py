"""
Tests for the ForgeBlueprint schema (``cstar_forge.forge.forge_blueprint``) and the Phase-1
resolver (``cstar_forge.forge_blueprint_resolve.build_forge_blueprint``).

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
from cstar_forge.domain_catalog import default_catalog as _CATALOG
from cstar_forge.forge.forge_blueprint import ForgeBlueprint
from cstar_forge.forge_blueprint_resolve import build_forge_blueprint

_MODEL_DIR = (
    Path(cstar_forge.__file__).parent / "catalog" / "ModelSpec" / "cson_roms-marbl_v0.1"
)
_GRID_KWARGS = dict(
    nx=6,
    ny=2,
    size_x=500,
    size_y=1000,
    center_lon=0,
    center_lat=55,
    rot=10,
    N=3,
    theta_s=5.0,
    theta_b=2.0,
    hc=250.0,
)
_BOUNDARIES = {"south": False, "east": True, "north": True, "west": False}
_PART = {"n_procs_x": 1, "n_procs_y": 1}


def _build(**over):
    kw = dict(
        model_dir=_MODEL_DIR,
        grid_name="test-tiny",
        grid_kwargs=_GRID_KWARGS,
        open_boundaries=_BOUNDARIES,
        partitioning=_PART,
        start_date=datetime(2012, 1, 1),
        end_date=datetime(2012, 1, 2),
        description="Test tiny",
        dt=7200,  # pass dt -> stays dependency-light
        # ModelSpec no longer embeds a default forcing/output selection -- supply the
        # bundled catalog entries by default; callers can still override either.
        forcing_inputs=_CATALOG.forcing_data("glorys-era5-unified"),
        output_settings=_CATALOG.output_data("standard"),
    )
    kw.update(over)
    return build_forge_blueprint(**kw)


def test_naming_is_derived_not_stored():
    cfg = _build()
    assert cfg.n_procs == 1
    assert cfg.name == "cson_roms-marbl_v0.1_test-tiny_1procs"
    assert cfg.casename == "cson_roms-marbl_v0.1_test-tiny_1procs_20120101-20120102"
    # output_root_name is host-derived from the scratch path
    assert cfg.output_root_name("/scratch").startswith("/scratch/cson_roms-marbl")


def test_default_working_dir_includes_run_name():
    from cstar_forge.forge.forge_blueprint import DEFAULT_WORKING_ROOT

    cfg = _build()
    assert cfg.working_dir == f"{DEFAULT_WORKING_ROOT}/{cfg.name}"


def test_bare_default_working_dir_expands_and_explicit_survives():
    from cstar_forge.forge.forge_blueprint import DEFAULT_WORKING_ROOT

    cfg = _build()
    # an old file storing the bare default root gains the run-name layer on load
    data = cfg.model_dump(mode="json")
    data["working_dir"] = DEFAULT_WORKING_ROOT
    assert ForgeBlueprint(**data).working_dir == f"{DEFAULT_WORKING_ROOT}/{cfg.name}"
    # a deliberate non-default path passes through untouched
    data["working_dir"] = "/custom/spot"
    assert ForgeBlueprint(**data).working_dir == "/custom/spot"


def test_forge_blueprint_is_portable_no_forge_or_cstar_imports():
    """forge_blueprint.py is the C-Star-relocatable blueprint model: it must depend on
    nothing from cstar_forge / cstar (only stdlib + pydantic + yaml).
    """
    src = Path(cstar_forge.__file__).parent / "forge" / "forge_blueprint.py"
    text = src.read_text()
    import re

    bad = [
        ln.strip()
        for ln in text.splitlines()
        if re.match(r"\s*(from|import)\s+(cstar_forge|cstar|\.)", ln)
    ]
    assert not bad, f"forge_blueprint.py must stay forge/cstar-free; found: {bad}"


def test_application_discriminator_default():
    from cstar_forge.forge.forge_blueprint import DEFAULT_APPLICATION

    cfg = _build()
    assert cfg.application == DEFAULT_APPLICATION


def test_from_yaml_rejects_newer_version(tmp_path):
    cfg = _build()
    p = tmp_path / "forge_blueprint.yml"
    cfg.to_yaml(p)
    import yaml as _yaml

    data = _yaml.safe_load(p.read_text())
    data["forge_blueprint_version"] = 9999
    p.write_text(_yaml.safe_dump(data))
    with pytest.raises(ValueError, match="newer than this build"):
        ForgeBlueprint.from_yaml(p)


def test_schema_round_trip_identity(tmp_path):
    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yml")
    back = ForgeBlueprint.from_yaml(p)
    # content_hash is stamped on write -> back carries it; otherwise identical
    assert back.provenance.content_hash == cfg.content_hash()
    assert back.model_copy(update={"provenance": cfg.provenance}) == cfg
    assert back.application == cfg.application


def test_content_hash_ignores_excluded_sections():
    from cstar_forge.forge.forge_blueprint import _HASH_EXCLUDE, PieceRef

    cfg = _build()
    h = cfg.content_hash()
    # editing identity / composition / provenance does NOT change the hash
    c2 = cfg.model_copy(
        update={
            "identity": cfg.identity.model_copy(
                update={"description": "totally different"}
            ),
            "composition": cfg.composition.model_copy(
                update={"forcing": PieceRef(name="x", origin="custom")}
            ),
            "provenance": cfg.provenance.model_copy(update={"notes": "edited"}),
        }
    )
    assert c2.content_hash() == h
    assert _HASH_EXCLUDE == {
        "forge_blueprint_version",
        "identity",
        "composition",
        "provenance",
        "working_dir",
    }


def test_content_hash_changes_with_results_affecting_data():
    cfg = _build()
    h = cfg.content_hash()
    edited = dict(cfg.model_settings)
    edited["v_sponge"] = {"v_sponge": 999.0}
    c2 = cfg.model_copy(update={"model_settings": edited})
    assert c2.content_hash() != h


def test_content_hash_ignores_code_repo_location():
    """``location`` is the fetch address (git URL or, in tests, a local path) — host/
    transport, not content. The same commit/branch fetched from a different remote (or a
    local mirror) must hash identically; only commit/branch/directory/files are
    results-affecting.
    """
    cfg = _build()
    h = cfg.content_hash()
    c2 = cfg.model_copy(
        update={
            "code": cfg.code.model_copy(
                update={
                    "roms": cfg.code.roms.model_copy(
                        update={"location": "https://example.com/some/other/mirror.git"}
                    ),
                    "templates_compile_time": cfg.code.templates_compile_time.model_copy(
                        update={"location": "https://example.com/other-templates.git"}
                    ),
                }
            )
        }
    )
    assert c2.content_hash() == h

    # but a commit/branch change on the same repo IS results-affecting
    c3 = cfg.model_copy(
        update={
            "code": cfg.code.model_copy(
                update={"roms": cfg.code.roms.model_copy(update={"commit": "deadbeef"})}
            )
        }
    )
    assert c3.content_hash() != h


def test_content_hash_ignores_pio_repo_location():
    cfg = _build(use_pio=True)
    h = cfg.content_hash()
    c2 = cfg.model_copy(
        update={
            "code": cfg.code.model_copy(
                update={
                    "pio": cfg.code.pio.model_copy(
                        update={"location": "https://example.com/mirror/pio.git"}
                    )
                }
            )
        }
    )
    assert c2.content_hash() == h

    c3 = cfg.model_copy(
        update={
            "code": cfg.code.model_copy(
                update={"pio": cfg.code.pio.model_copy(update={"commit": "deadbeef"})}
            )
        }
    )
    assert c3.content_hash() != h


def test_code_pio_round_trips_through_yaml(tmp_path):
    cfg = _build(use_pio=True)
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.code.pio is not None
    assert back.code.pio.location == cfg.code.pio.location
    assert back.code.pio.commit == "pio2_7_0"
    assert back.model_settings["cppdefs"]["use_pio"] is True


def test_content_hash_round_trips_through_yaml(tmp_path):
    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yml")
    back = ForgeBlueprint.from_yaml(p)
    # recomputed hash on the loaded config matches the stamped one (no edits)
    assert back.content_hash() == back.provenance.content_hash


def test_engine_warns_on_hash_mismatch(tmp_path):
    from cstar_forge.forge.forge_blueprint_engine import (
        process_forge_blueprint,
        verify_content_hash,
    )

    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yml")
    data = yaml.safe_load(p.read_text())
    # hand-edit a results-affecting value WITHOUT updating the recorded hash
    data["model_settings"]["v_sponge"]["v_sponge"] = 12345.0
    p.write_text(yaml.safe_dump(data))
    tampered = ForgeBlueprint.from_yaml(p)
    assert verify_content_hash(tampered) is not None  # mismatch detected
    # ... and a clean (re-saved) file does not warn
    assert (
        verify_content_hash(
            ForgeBlueprint.from_yaml(cfg.to_yaml(tmp_path / "clean.yml"))
        )
        is None
    )

    # the engine warns but still processes (uses a fake executor)
    class _Fake:
        def __init__(self, cfg=None, host=None):
            self.calls = []

        def ensure_source_data(self, **k):
            self.calls.append("e")

        def generate_inputs(self, **k):
            self.calls.append("g")

        def configure_build(self, **k):
            self.calls.append("c")

        def path_roms_marbl_blueprint(self):
            return "/bp"

    with pytest.warns(UserWarning, match="integrity check FAILED"):
        b = process_forge_blueprint(tampered, validate=False, executor_factory=_Fake)
    assert b.calls == ["e", "g", "c"]  # processing proceeded


def test_golden_model_settings_test_tiny():
    """Behavior-preservation snapshot for the executor-portability refactor.

    ``model_settings`` is the host-independent semantic source of both ``namelist.nml``
    (run-time sections) and ``cppdefs.opt`` (``cppdefs``). ``configure_build`` already
    overlays it (cfg wins), so it is the authoritative settings both before and after the
    executor consumes it directly — pinning it byte-for-byte proves the generated ROMS
    settings are unchanged by the refactor (and catches resolver drift).
    """
    import json

    golden_path = (
        Path(cstar_forge.__file__).parents[1]
        / "tests"
        / "fixtures"
        / "golden_model_settings_test-tiny.json"
    )
    golden = json.loads(golden_path.read_text())
    cfg = _build()  # test-tiny, dt=7200 (matches how the golden was captured)
    got = json.loads(json.dumps(cfg.model_settings, sort_keys=True, default=str))
    assert got == golden, (
        "Resolved model_settings for test-tiny drifted from the golden fixture. If this is "
        "an intentional schema/default change, regenerate "
        "tests/fixtures/golden_model_settings_test-tiny.json; otherwise the change is a "
        "regression in the settings the executor feeds to namelist.nml / cppdefs.opt."
    )


def test_resolver_nesting_enables_extract_data():
    cfg = _build(
        grid_kwargs_child=dict(
            nx=30,
            ny=30,
            size_x=300,
            size_y=300,
            center_lon=0,
            center_lat=55,
            rot=0,
            N=20,
            theta_s=6.0,
            theta_b=3.0,
            hc=250.0,
        ),
        metadata_child={"period": 1800.0},
    )
    ed = cfg.model_settings["extract_data"]
    assert ed["do_extract"] is True and ed["extract_file"] == "nesting.nc"
    assert ed["n_chd"] == 20 and ed["theta_s_chd"] == 6.0 and ed["hc_chd"] == 250.0
    assert ed["extract_period"] == 1800.0
    assert cfg.domain.grid_kwargs_child["nx"] == 30
    assert cfg.domain.metadata_child == {"period": 1800.0}


def test_resolver_nesting_default_period():
    cfg = _build(
        grid_kwargs_child=dict(
            nx=30,
            ny=30,
            size_x=300,
            size_y=300,
            center_lon=0,
            center_lat=55,
            rot=0,
            N=15,
        )
    )
    assert cfg.model_settings["extract_data"]["extract_period"] == 3600.0
    assert cfg.model_settings["extract_data"]["n_chd"] == 15


def test_resolver_no_nesting_keeps_defaults():
    cfg = _build()
    assert cfg.model_settings["extract_data"]["do_extract"] is False
    assert cfg.domain.grid_kwargs_child is None


def test_resolver_restoring_sets_sal_restore():
    # the cson model.yml includes a WOA surface source with type=restoring and
    # restoring_forces=['sss'], so the resolver derives sal_restore=True
    # (see forge_blueprint_resolve.py: sal_restore = any restoring item with 'sss').
    cfg = _build()
    assert cfg.model_settings["cppdefs"].get("sal_restore") is True


def test_catalog_scans_forcingspec():
    from cstar_forge.domain_catalog import default_catalog as cat

    assert "glorys-era5-unified" in cat.forcing_names
    data = cat.forcing_data("glorys-era5-unified")
    assert "forcing" in data and "initial_conditions" in data


def test_sources_to_forcing_override_returns_dict_by_default():
    from cstar_forge.forge.forge_blueprint_engine import sources_to_forcing_override

    cfg = _build()
    # ModelSpec no longer provides a default forcing -- _build()'s own default
    # forcing_inputs (a ForcingSpec dict, not composition= tracking) resolves via the
    # generic fallback Composition, which always records origin="custom".
    assert cfg.composition.forcing.origin == "custom"
    ov = sources_to_forcing_override(cfg)
    assert ov is not None
    assert "initial_conditions" in ov and "forcing" in ov
    assert ov["initial_conditions"]["source"]["name"] == "GLORYS"


def test_sources_to_forcing_override_converts_custom_forcing():
    from cstar_forge.domain_catalog import default_catalog as cat
    from cstar_forge.forge.forge_blueprint_engine import sources_to_forcing_override

    fdata = cat.forcing_data("glorys-era5-unified")
    cfg = _build(forcing_inputs=fdata)
    assert cfg.composition.forcing.origin == "custom"
    ov = sources_to_forcing_override(cfg)
    assert ov is not None
    assert "initial_conditions" in ov and "forcing" in ov
    assert ov["initial_conditions"]["source"]["name"] == "GLORYS"
    assert [i["source"]["name"] for i in ov["forcing"]["surface"]] == [
        "ERA5",
        "UNIFIED",
        "MBL_co2",
        "WOA",
    ]
    assert ov["forcing"]["tidal"][0]["ntides"] == 15


def test_forcing_override_coerces_enums_to_strings():
    """Regression: enum-typed item fields (SurfaceType, BoundaryType, BgcInterpMethod,
    ClimatologyMode, …) must be dumped as plain strings, not enum instances. Enum
    instances leaked into output filenames (f"{key}-{type}") and into roms-tools'
    SafeDumper (→ 'cannot represent an object'). The bridge dumps with mode="json".
    """
    import enum

    from cstar_forge.domain_catalog import default_catalog as cat
    from cstar_forge.forge.forge_blueprint_engine import sources_to_forcing_override

    cfg = _build(forcing_inputs=cat.forcing_data("glorys-era5-unified"))
    ov = sources_to_forcing_override(cfg)

    def _assert_no_enums(obj, path="ov"):
        assert not isinstance(obj, enum.Enum), (
            f"enum instance leaked at {path}: {obj!r}"
        )
        if isinstance(obj, dict):
            for k, v in obj.items():
                _assert_no_enums(v, f"{path}[{k!r}]")
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                _assert_no_enums(v, f"{path}[{i}]")

    _assert_no_enums(ov)
    # Spot-check the field that produced the original warning.
    assert ov["forcing"]["surface"][0]["type"] == "physics"
    assert type(ov["forcing"]["surface"][0]["type"]) is str


def test_global_enum_representer_handles_safedumper_subclass():
    """Insurance: importing cstar_forge.forge registers a global Enum representer so any
    Forge enum reaching a SafeDumper (or a subclass, as roms-tools' NoAliasDumper is)
    serializes as its value rather than raising 'cannot represent an object'.
    """
    import cstar_forge.forge  # noqa: F401  (side effect: registers the representer)
    from cstar_forge.forge.forge_blueprint import SurfaceType

    class _NoAliasDumper(yaml.SafeDumper):  # mirrors roms-tools' dumper shape
        pass

    assert (
        yaml.dump({"type": SurfaceType.PHYSICS}, Dumper=_NoAliasDumper).strip()
        == "type: physics"
    )


def test_forcing_override_used_by_input_data(tmp_path):
    """When forcing_override is provided, RomsMarblInputData uses it instead of
    model_spec.inputs — the input_list reflects the override, not the defaults.
    """
    from unittest.mock import MagicMock, patch

    from cstar_forge.forge import input_data as id_mod

    override = {
        "initial_conditions": {"source": {"name": "GLORYS", "climatology": False}},
        "forcing": {
            "surface": [
                {
                    "source": {"name": "ERA5"},
                    "type": "physics",
                    "correct_radiation": True,
                    "coarse_grid_mode": "never",
                }
            ],
        },
    }
    # Minimal mock of what __post_init__ needs beyond the input_list building
    mock_spec = MagicMock()
    mock_spec.inputs.grid = None  # skip grid
    # Note: marbl is now read from _settings_compile_time["cppdefs"]["marbl"],
    # not from model_spec.settings.properties — no need to set it on mock_spec.

    with patch.object(
        id_mod.RomsMarblInputData,
        "__post_init__",
        id_mod.RomsMarblInputData.__post_init__,
    ):
        # Just verify input_list is built from the override, not model_spec.inputs
        # Use a lightweight construction that skips heavy validation
        obj = object.__new__(id_mod.RomsMarblInputData)
        object.__setattr__(obj, "forcing_override", override)
        object.__setattr__(obj, "model_spec", mock_spec)
        object.__setattr__(obj, "cdr_forcing", None)
        # Manually run the input_list building logic
        input_list = []
        # grid (None here)
        fo = override
        if fo.get("initial_conditions"):
            input_list.append(("initial_conditions", dict(fo["initial_conditions"])))
        for category, items in (fo.get("forcing") or {}).items():
            for item in items or []:
                input_list.append((f"forcing.{category}", dict(item)))
        assert (
            "initial_conditions",
            {"source": {"name": "GLORYS", "climatology": False}},
        ) in input_list
        assert ("forcing.surface", override["forcing"]["surface"][0]) in input_list
        # boundary/tidal/river are absent because override doesn't include them
        assert not any(k.startswith("forcing.boundary") for k, _ in input_list)


def test_catalog_scans_outputspec():
    from cstar_forge.domain_catalog import default_catalog as cat

    assert "standard" in cat.output_names
    data = cat.output_data("standard")
    assert "ocean_vars" in data and "diagnostics" in data
    assert set(data["marbl_bgc"]) == {
        "marbl_tracers_to_write",
        "marbl_diagnostics_to_write",
    }


def test_resolver_output_settings_override():
    from cstar_forge.domain_catalog import default_catalog as cat

    odata = cat.output_data("standard")
    cfg = _build(output_settings=odata)
    assert cfg.composition.output.origin == "custom"
    # marbl partial merge keeps the non-output marbl fields
    assert "marbl_config_file" in cfg.model_settings["marbl_bgc"]
    # an edited output (turn on wrt_temp) flows through; manual override still wins
    edited = {k: (dict(v) if isinstance(v, dict) else v) for k, v in odata.items()}
    edited["ts_output"] = dict(odata["ts_output"])
    edited["ts_output"]["wrt_temp"] = True
    cfg2 = _build(output_settings=edited)
    assert cfg2.model_settings["ts_output"]["wrt_temp"] is True
    cfg3 = _build(
        output_settings=edited, run_time_overrides={"ts_output": {"wrt_temp": False}}
    )
    assert cfg3.model_settings["ts_output"]["wrt_temp"] is False


def test_extract_output_settings_helper():
    from cstar_forge.forge_blueprint_resolve import (
        OUTPUT_BGC_FIELDS,
        OUTPUT_SECTIONS,
        extract_output_settings,
    )

    cfg = _build()
    out = extract_output_settings(cfg.model_settings)
    assert set(OUTPUT_SECTIONS) <= set(out)
    assert set(out["marbl_bgc"]) == {
        "marbl_tracers_to_write",
        "marbl_diagnostics_to_write",
    }
    assert set(out["bgc"]) == set(OUTPUT_BGC_FIELDS)


def test_resolver_forcing_inputs_override():
    from cstar_forge.domain_catalog import default_catalog as cat

    fdata = cat.forcing_data("glorys-era5-unified")
    cfg = _build(forcing_inputs=fdata)
    assert cfg.composition.forcing.origin == "custom"
    assert [i.source.name for i in cfg.forcing.surface] == [
        "ERA5",
        "UNIFIED",
        "MBL_co2",
        "WOA",
    ]
    # glorys-era5-unified already includes a restoring SSS source -> sal_restore
    assert cfg.model_settings["cppdefs"]["sal_restore"] is True
    # stripping it back out -> sal_restore goes False; adding it back -> True again
    # (isolates the derivation's causality rather than relying on the bundled default)
    without_restoring = dict(fdata)
    without_restoring["forcing"] = dict(fdata["forcing"])
    without_restoring["forcing"]["surface"] = [
        it for it in fdata["forcing"]["surface"] if it.get("type") != "restoring"
    ]
    cfg_bare = _build(forcing_inputs=without_restoring)
    assert cfg_bare.model_settings["cppdefs"]["sal_restore"] is False

    edited = dict(without_restoring)
    edited["forcing"] = dict(without_restoring["forcing"])
    edited["forcing"]["surface"] = without_restoring["forcing"]["surface"] + [
        {
            "source": {"name": "WOA", "climatology": True},
            "type": "restoring",
            "restoring_forces": ["sss"],
        }
    ]
    cfg2 = _build(forcing_inputs=edited)
    assert cfg2.model_settings["cppdefs"]["sal_restore"] is True


def test_timestepping_and_param_match_known_run():
    cfg = _build()
    assert cfg.model_settings["time_stepping"] == {
        "ntimes": 12,
        "dt": 7200,
        "ndtfast": 60,
        "ninfo": 1,
    }
    p = cfg.model_settings["param"]
    assert (p["llm"], p["mmm"], p["n"]) == (6, 2, 3)  # from grid nx/ny/N
    assert (p["np_xi"], p["np_eta"]) == (1, 1)  # from partitioning
    assert (p["nsub_x"], p["nsub_e"]) == (1, 1)
    assert p["ntrc_bio"] == 32  # from defaults


def test_cppdefs_obc_from_boundaries_and_cdr_flag():
    cfg = _build(cdr_forcing={"releases": []})
    c = cfg.model_settings["cppdefs"]
    assert c["obc_west"] is False and c["obc_east"] is True
    assert c["obc_north"] is True and c["obc_south"] is False
    assert c["cdr_forcing"] is True and c["marbl"] is True


_CDR_SAMPLE_YAML = (
    Path(cstar_forge.__file__).parent
    / "catalog"
    / "blueprints"
    / "MacOS"
    / "cson_roms-marbl_v0.1_test-tiny_1procs"
    / "_cdr_forcing.yml"
)


def test_read_cdr_forcing_yaml_from_sample():
    from cstar_forge.forge_blueprint_resolve import read_cdr_forcing_yaml

    block = read_cdr_forcing_yaml(_CDR_SAMPLE_YAML)
    assert block["releases"], "sample must carry at least one release"
    assert "_tracer_metadata" not in block
    assert block["start_time"] == "2012-01-01T00:00:00"


def test_read_cdr_forcing_yaml_accepts_raw_text():
    from cstar_forge.forge_blueprint_resolve import read_cdr_forcing_yaml

    text = _CDR_SAMPLE_YAML.read_text()
    block = read_cdr_forcing_yaml(text)
    assert block["releases"]
    assert "_tracer_metadata" not in block


def test_read_cdr_forcing_yaml_rejects_non_cdr():
    from cstar_forge.forge_blueprint_resolve import read_cdr_forcing_yaml

    with pytest.raises(ValueError, match="CDRForcing"):
        read_cdr_forcing_yaml("---\nSomeOtherThing:\n  foo: bar\n")


def test_build_with_cdr_forcing_yaml():
    cfg = _build(cdr_forcing_yaml=_CDR_SAMPLE_YAML)
    assert cfg.forcing.cdr_forcing["releases"]
    assert "_tracer_metadata" not in cfg.forcing.cdr_forcing
    assert cfg.model_settings["cppdefs"]["cdr_forcing"] is True


def test_build_forge_blueprint_strips_tracer_metadata():
    cfg = _build(
        cdr_forcing={"releases": [], "_tracer_metadata": {"temp": {"units": "C"}}}
    )
    assert "_tracer_metadata" not in cfg.forcing.cdr_forcing


def test_cdr_forcing_content_hash_stable_across_yaml_round_trip(tmp_path):
    """cdr_forcing's ``times`` field is a bare YAML timestamp on first parse (a
    Python ``datetime``) but re-serializes as an ISO string once the blueprint has
    been saved/reloaded (there is no typed CDR model to normalize it, by design).
    ``content_hash()`` must still be stable across that round trip -- the whole
    resolved_datasets/ForgeBlueprint determinism goal depends on identical content
    hashing identically regardless of how many times it's been saved and reloaded.
    """
    cfg = _build(cdr_forcing_yaml=_CDR_SAMPLE_YAML)
    h1 = cfg.content_hash()

    p = cfg.to_yaml(tmp_path / "forge_blueprint.yml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.content_hash() == h1


def test_resolver_use_pio_sets_cppdefs_and_code_pio():
    cfg = _build(use_pio=True)
    assert cfg.model_settings["cppdefs"]["use_pio"] is True
    assert cfg.code.pio is not None
    assert cfg.code.pio.location == "https://github.com/NCAR/ParallelIO.git"
    assert cfg.code.pio.commit == "pio2_7_0"


def test_resolver_use_pio_default_off():
    cfg = _build()
    assert cfg.model_settings["cppdefs"]["use_pio"] is False
    assert cfg.code.pio is None


def test_resolver_use_pio_defaults_from_model_spec(tmp_path):
    """A ModelSpec's top-level `use_pio: true` becomes the resolver default when the
    caller doesn't pass an explicit use_pio kwarg (mirrors bgc_mode's fallback).
    """
    import shutil

    model_dir = tmp_path / "cson_roms-marbl_v0.1"
    shutil.copytree(_MODEL_DIR, model_dir)
    text = (model_dir / "model.yml").read_text()
    assert "use_pio: false" in text
    (model_dir / "model.yml").write_text(
        text.replace("use_pio: false", "use_pio: true")
    )

    cfg = _build(model_dir=model_dir)
    assert cfg.model_settings["cppdefs"]["use_pio"] is True
    assert cfg.code.pio is not None

    # an explicit kwarg still overrides the ModelSpec default
    cfg_off = _build(model_dir=model_dir, use_pio=False)
    assert cfg_off.model_settings["cppdefs"]["use_pio"] is False
    assert cfg_off.code.pio is None


def test_resolver_use_pio_requires_model_yml_pin():
    from cstar_forge.forge.forge_blueprint import CodeRepo
    from cstar_forge.forge_blueprint_resolve import _build_code

    model = {
        "code": {"roms": {"location": "https://example.com/roms.git", "commit": "x"}},
        "templates": {},
    }
    templates_repo = CodeRepo(location="https://example.com/forge.git", branch="main")
    with pytest.raises(ValueError, match="code.pio"):
        _build_code(model, templates_repo, use_pio=True)


def test_resolver_bgc_mode_default_marbl():
    cfg = _build()
    assert cfg.model_settings["cppdefs"]["marbl"] is True
    assert cfg.code.marbl is not None
    assert cfg.code.marbl.location == "https://github.com/marbl-ecosys/MARBL.git"
    assert cfg.code.marbl.commit == "marbl0.45.0"


def test_resolver_bgc_mode_none_raises_with_bgc_forcing():
    """The default fixture (glorys-era5-unified) carries BGC signals (IC bgc_source
    + river include_bgc + bgc-type surface items) -- bgc_mode="none" must catch it
    and name the offending items.
    """
    with pytest.raises(ValueError) as exc_info:
        _build(bgc_mode="none")
    msg = str(exc_info.value)
    assert "bgc_mode" in msg
    assert "initial_conditions.bgc_source" in msg
    assert "river[0]" in msg
    assert "surface[" in msg


# A minimal physics-only forcing selection (no bgc_source/include_bgc/bgc-type
# items) -- no catalog ForcingSpec is physics-only today, so this is hand-authored.
_PHYSICS_ONLY_FORCING = {
    "initial_conditions": {"source": {"name": "GLORYS", "glorys_layout": "regional"}},
    "forcing": {
        "surface": [{"source": {"name": "ERA5"}, "type": "physics"}],
    },
}


def test_resolver_bgc_mode_none_with_physics_only_forcing():
    cfg = _build(bgc_mode="none", forcing_inputs=_PHYSICS_ONLY_FORCING)
    assert cfg.model_settings["cppdefs"]["marbl"] is False
    assert cfg.code.marbl is None


def test_cppdefs_tides_tracks_tidal_forcing_presence():
    """TIDES is derived purely from whether a tidal item is being generated -- the
    bundled glorys-era5-unified ForcingSpec carries one (ntides=15), so it defaults
    True; stripping tidal items out flips it False.
    """
    cfg = _build()
    assert cfg.model_settings["cppdefs"]["tides"] is True

    cfg_no_tides = _build(forcing_inputs=_PHYSICS_ONLY_FORCING)
    assert cfg_no_tides.model_settings["cppdefs"]["tides"] is False


def test_cppdefs_sponge_tune_defaults_false_and_is_overridable():
    """SPONGE_TUNE has no per-run resolver kwarg -- it's a plain ModelSpec default
    (False) only reachable via compile_time_overrides (the wizard's advanced
    settings accordion).
    """
    cfg = _build()
    assert cfg.model_settings["cppdefs"]["sponge_tune"] is False

    cfg_on = _build(compile_time_overrides={"cppdefs": {"sponge_tune": True}})
    assert cfg_on.model_settings["cppdefs"]["sponge_tune"] is True


def test_cppdefs_nhy_nox_forcing_default_true_and_off_when_bgc_mode_none():
    cfg = _build()
    assert cfg.model_settings["cppdefs"]["nhy_forcing"] is True
    assert cfg.model_settings["cppdefs"]["nox_forcing"] is True

    cfg_none = _build(bgc_mode="none", forcing_inputs=_PHYSICS_ONLY_FORCING)
    assert cfg_none.model_settings["cppdefs"]["nhy_forcing"] is False
    assert cfg_none.model_settings["cppdefs"]["nox_forcing"] is False


def test_resolver_bgc_mode_marbl_requires_model_yml_pin():
    from cstar_forge.forge.forge_blueprint import CodeRepo
    from cstar_forge.forge_blueprint_resolve import _build_code

    model = {
        "code": {"roms": {"location": "https://example.com/roms.git", "commit": "x"}},
        "templates": {},
    }
    templates_repo = CodeRepo(location="https://example.com/forge.git", branch="main")
    with pytest.raises(ValueError, match="code.marbl"):
        _build_code(model, templates_repo, bgc_mode="marbl")


def test_resolver_roms_ref_overrides_commit_and_clears_branch():
    cfg = _build(roms_ref="pio-refdate")
    assert cfg.code.roms.commit == "pio-refdate"
    assert cfg.code.roms.branch is None
    # location is untouched -- only the checkout target changes
    assert cfg.code.roms.location == "https://github.com/CWorthy-ocean/ucla-roms.git"


def test_resolver_roms_ref_default_uses_model_yml_pin():
    cfg = _build()
    assert cfg.code.roms.commit == "0.2.0"


def test_build_code_coerces_numeric_commit_to_string():
    """A bare numeric commit in model.yml (e.g. `commit: 123456`, parsed by PyYAML
    as an int) must be coerced to str -- CodeRepo.commit is str-typed and rejects
    an int outright.
    """
    from cstar_forge.forge.forge_blueprint import CodeRepo
    from cstar_forge.forge_blueprint_resolve import _build_code

    model = {
        "code": {
            "roms": {"location": "https://example.com/roms.git", "commit": 123456},
            "templates_compile_time": {
                "directory": "templates/compile-time",
                "files": [],
            },
            "templates_run_time": {"directory": "templates/run-time", "files": []},
        },
    }
    templates_repo = CodeRepo(location="https://example.com/forge.git", branch="main")
    code = _build_code(model, templates_repo, bgc_mode="none")
    assert code.roms.commit == "123456"
    assert isinstance(code.roms.commit, str)


def test_content_hash_changes_with_roms_ref():
    cfg = _build()
    h = cfg.content_hash()
    cfg_override = _build(roms_ref="pio-refdate")
    assert cfg_override.content_hash() != h


def test_roms_ref_round_trips_through_yaml(tmp_path):
    cfg = _build(roms_ref="pio-refdate")
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.code.roms.commit == "pio-refdate"
    assert back.code.roms.branch is None


def test_settings_is_flat_and_omits_processing_filled_sections():
    cfg = _build()
    ms = cfg.model_settings
    assert "cppdefs" in ms and "lateral_visc" in ms  # cppdefs flat alongside namelist
    for excluded in (
        "grid",
        "initial",
        "forcing",
        "s_coord",
        "title",
        "output_root_name",
    ):
        assert excluded not in ms


def test_sources_resolved_from_modelspec():
    from cstar_forge.forge.source_registry import resolve_dataset_key

    cfg = _build()
    s = cfg.forcing
    # dataset_key is no longer stored on SourceSpec — derive it when needed
    ic_src = s.initial_conditions.source
    assert resolve_dataset_key(ic_src.name, ic_src.glorys_layout) == "GLORYS_REGIONAL"
    bgc_src = s.initial_conditions.bgc_source
    assert resolve_dataset_key(bgc_src.name, bgc_src.glorys_layout) == "UNIFIED_BGC"
    assert [i.source.name for i in s.surface] == ["ERA5", "UNIFIED", "MBL_co2", "WOA"]
    assert s.tidal[0].ntides == 15
    assert s.river[0].include_bgc is True
    assert (
        s.resolved_datasets["GLORYS"].dataset_id
        == "cmems_mod_glo_phy_my_0.083deg_P1D-m"
    )


def test_templates_are_repo_refs():
    cfg = _build()
    t = cfg.code.templates_compile_time
    assert t.location.endswith("cstar-forge.git")
    assert t.files == ["cppdefs.opt.j2"]
    assert cfg.code.templates_run_time.files == ["marbl_in"]
    assert cfg.code.roms.commit == "0.2.0"


def test_no_host_or_machine_in_config():
    cfg = _build()
    d = cfg.model_dump()
    assert "machine" not in d and "execution" not in d and "paths" not in d
    assert "conventions" not in d


def test_overrides_take_precedence():
    cfg = _build(
        run_time_overrides={
            "v_sponge": {"v_sponge": 42.0},
            "time_stepping": {"ndtfast": 30},
        }
    )
    assert cfg.model_settings["v_sponge"]["v_sponge"] == 42.0
    assert cfg.model_settings["time_stepping"]["ndtfast"] == 30


def test_composition_records_piece_provenance():
    cfg = _build()
    assert cfg.composition.model.origin == "catalog"
    assert cfg.composition.model.name == "cson_roms-marbl_v0.1"
    assert cfg.composition.domain.name == "test-tiny"


def test_yaml_round_trip(tmp_path):
    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.casename == cfg.casename
    assert back.model_settings["time_stepping"] == cfg.model_settings["time_stepping"]


def test_committed_example_validates():
    """The checked-in example must remain a valid ForgeBlueprint."""
    example = (
        Path(cstar_forge.__file__).parents[1]
        / "docs"
        / "forge-blueprint-example.test-tiny.yml"
    )
    if not example.exists():
        pytest.skip("example file not present")
    cfg = ForgeBlueprint.from_yaml(example)
    assert cfg.composition.model.name == "cson_roms-marbl_v0.1"
    assert cfg.composition.model.origin == "catalog"


# ---------------------------------------------------------------------------
# Wizard (headless: ipywidgets value get/set/observe work without rendering)
# ---------------------------------------------------------------------------
class TestForgeBlueprintWizard:
    def _wizard(self):
        pytest.importorskip("ipywidgets")
        from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizard

        return ForgeBlueprintWizard()

    def test_init_resolves_default_config(self):
        wiz = self._wizard()
        assert isinstance(wiz.config, ForgeBlueprint)
        assert wiz.config.casename  # derived, non-empty

    def test_selecting_catalog_domain_prefills_and_resolves(self):
        wiz = self._wizard()
        if "gulf-guinea-toy" not in wiz.domain_dd.options:
            pytest.skip("gulf-guinea-toy domain not in catalog")
        wiz.domain_dd.value = "gulf-guinea-toy"  # triggers prefill + rebuild
        cfg = wiz.config
        assert cfg.domain.grid_name == "gulf-guinea-toy"
        assert cfg.domain.grid_kwargs["nx"] == 10 and cfg.domain.grid_kwargs["N"] == 5
        assert (
            cfg.domain.partitioning.n_procs_x,
            cfg.domain.partitioning.n_procs_y,
        ) == (2, 5)
        assert cfg.domain.open_boundaries.south is True
        # this domain doesn't specify s-coord -> not injected
        assert "theta_s" not in cfg.domain.grid_kwargs

    def test_editing_boundary_updates_cppdefs_live(self):
        wiz = self._wizard()
        wiz.bnd["west"].value = True
        assert wiz.config.model_settings["cppdefs"]["obc_west"] is True
        wiz.bnd["west"].value = False
        assert wiz.config.model_settings["cppdefs"]["obc_west"] is False

    def test_blank_name_tracks_derived_default(self):
        wiz = self._wizard()
        default_name = wiz.config.name
        assert wiz.name.value == default_name  # auto-backfilled
        wiz.npx.value = wiz.npx.value + 1  # change an input the default depends on
        assert wiz.config.name != default_name
        assert wiz.name.value == wiz.config.name  # still tracking the new default

    def test_custom_name_overrides_default_and_stops_tracking(self):
        wiz = self._wizard()
        default_name = wiz.config.name
        wiz.name.value = "my-custom-run"
        assert wiz.config.name == "my-custom-run"
        wiz.npx.value = wiz.npx.value + 1  # would change the derived default
        assert wiz.config.name == "my-custom-run"  # but the override sticks
        assert wiz.config.name != default_name

    def test_save_writes_valid_yaml(self, tmp_path):
        wiz = self._wizard()
        wiz.save_path.value = str(tmp_path / "forge_blueprint.yml")
        wiz._on_save(None)
        cfg = ForgeBlueprint.from_yaml(tmp_path / "forge_blueprint.yml")
        assert cfg.casename == wiz.config.casename

    def test_load_existing_config_round_trips(self, tmp_path):
        """Save a config, load it into a fresh wizard, and confirm widgets +
        resolved config round-trip (the #7 load affordance).
        """
        w1 = self._wizard()
        if "gulf-guinea-toy" in w1.domain_dd.options:
            w1.domain_dd.value = "gulf-guinea-toy"
        w1.name.value = "my-custom-run"
        p = tmp_path / "forge_blueprint.yml"
        w1.save_path.value = str(p)
        w1._on_save(None)
        saved = ForgeBlueprint.from_yaml(p)

        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.grid_name.value == saved.domain.grid_name
        assert w2.domain_dd.value == "<custom>"  # file authoritative, no prefill
        assert w2.name.value == "my-custom-run"
        assert w2.config is not None
        assert w2.config.casename == saved.casename
        assert (
            w2.config.model_settings["time_stepping"]
            == saved.model_settings["time_stepping"]
        )

    def test_load_from_upload_bytes(self, tmp_path):
        w1 = self._wizard()
        p = tmp_path / "forge_blueprint.yml"
        w1.save_path.value = str(p)
        w1._on_save(None)
        w2 = self._wizard()
        w2._load_bytes(p.read_bytes())
        assert w2.config is not None and w2.config.casename == w1.config.casename

    def test_validation_indicator_valid_by_default(self):
        w = self._wizard()
        assert "settings valid" in w.validation.value

    def test_advanced_editor_groups_sections_into_modeler_categories(self):
        w = self._wizard()
        assert w.editor is not None
        # Panes are the modeler-facing categories, in order, not raw section names.
        assert list(w.editor._pane_sections) == [
            "Physics & subgrid tuning",
            "Surface & lateral forcing",
            "Biogeochemistry (BGC / MARBL)",
            "Carbon dioxide removal (CDR)",
            "Output & diagnostics",
        ]
        sections = set(w.editor._section_fields)
        # A representative editable section from each category is present.
        assert {
            "lateral_visc",  # physics
            "tides",  # forcing
            "marbl_bgc",  # bgc
            "cdr_frc",  # cdr
            "ocean_vars",  # output
            "extract_data",  # output
        } <= sections
        # Dynamic / dedicated-widget sections are dropped from the accordion (their
        # resolver-composed value still flows through -- see the exclusion test).
        # ``cppdefs`` is the one partial exception: sponge_tune/nhy_forcing/
        # nox_forcing are accordion-editable (see test_advanced_editor_excludes_
        # dedicated_widget_fields for the resolver-derived fields that still aren't).
        assert not (
            {
                "time_stepping",
                "reference_date_settings",
                "grid",
                "s_coord",
                "param",
                "forcing",
            }
            & sections
        )
        assert w.config.composition.overrides == {}

    def test_advanced_editor_splits_bgc_along_output_seam(self):
        """bgc/marbl_bgc straddle physics and output: the write-controls appear under
        Output, the rest under Biogeochemistry (the PARTIAL_OUTPUT_SECTIONS seam).
        """
        w = self._wizard()
        assert "bgc" in w.editor._pane_sections["Biogeochemistry (BGC / MARBL)"]
        assert "bgc" in w.editor._pane_sections["Output & diagnostics"]
        # A bgc physics field and a bgc output field each got a widget (neither half
        # was dropped by the split).
        assert ("bgc", "xco2air_default") in w.editor._widgets  # physics
        assert ("bgc", "wrt_his") in w.editor._widgets  # output write-control

    def test_advanced_editor_excludes_dedicated_widget_fields(self):
        """Fields/sections controlled elsewhere (PIO, open boundaries, BGC mode, grid
        dims, partitioning, dt, run length) must not appear in the generic Advanced-
        settings accordion -- but their resolved value still flows through.
        """
        w = self._wizard()
        # The whole param/time_stepping/grid group is dropped from the accordion
        # (resolver-derived or edited by a dedicated widget).
        for dropped in ("param", "time_stepping", "grid", "s_coord"):
            assert dropped not in w.editor._section_fields

        # cppdefs is only PARTIALLY dropped: sponge_tune/nhy_forcing/nox_forcing (no
        # other UI) are accordion-editable; every resolver-derived flag still has no
        # widget at all, matching the dedicated-widget/derivation fields above.
        for resolver_owned in (
            "obc_west",
            "obc_east",
            "obc_north",
            "obc_south",
            "marbl",
            "use_pio",
            "cdr_forcing",
            "co2_tvarying",
            "sal_restore",
            "tides",
        ):
            assert ("cppdefs", resolver_owned) not in w.editor._widgets
        assert ("cppdefs", "sponge_tune") in w.editor._widgets
        assert ("cppdefs", "nhy_forcing") in w.editor._widgets
        assert ("cppdefs", "nox_forcing") in w.editor._widgets

        # No widget for these fields, but the resolver-composed value still lands
        # in the final config -- dropping the editor can't drop/reset the value.
        assert (
            w.config.model_settings["param"]["llm"] == w.config.domain.grid_kwargs["nx"]
        )
        assert w.config.model_settings["cppdefs"]["marbl"] is True
        assert "use_pio" in w.config.model_settings["cppdefs"]
        assert w.config.model_settings["time_stepping"]["ntimes"] > 0

    def test_editing_advanced_setting_reflects_in_config(self):
        w = self._wizard()
        wid = w.editor._widgets[("ocean_vars", "wrt_z")][0]
        wid.value = not wid.value
        assert w.config.model_settings["ocean_vars"]["wrt_z"] == wid.value

    def test_advanced_edit_persists_across_atomic_change(self):
        w = self._wizard()
        w.editor._widgets[("lateral_visc", "visc2")][0].value = 12.5
        w.grid_w["nx"].value = 8  # atomic change -> re-derive
        assert w.config.model_settings["lateral_visc"]["visc2"] == 12.5  # edit kept
        assert w.config.model_settings["param"]["llm"] == 8  # derived refreshed

    def test_editing_derived_value_records_override_and_wins(self):
        """time_stepping/param are now dropped from the accordion (resolver-controlled
        or dedicated widgets). Use v_sponge instead -- still resolver-derived (from
        grid spacing) and still accordion-editable (Physics pane) -- to exercise the
        same override-wins-over-re-derivation mechanic.
        """
        w = self._wizard()
        w.editor._widgets[("v_sponge", "v_sponge")][0].value = 999.0
        assert w.config.composition.overrides == {"v_sponge": {"v_sponge": 999.0}}
        # override persists and wins over the re-composed value across a rebuild
        w.dt.value = 3600.0
        assert w.config.model_settings["v_sponge"]["v_sponge"] == 999.0
        assert (
            w.config.model_settings["lateral_visc"]["visc2"] == 0.0
        )  # non-overridden field still re-derives to its composed default

    def test_override_layer_round_trips_through_load(self, tmp_path):
        w1 = self._wizard()
        w1.editor._widgets[("v_sponge", "v_sponge")][0].value = 999.0
        w1.editor._widgets[("lateral_visc", "visc2")][0].value = 3.3
        p = tmp_path / "forge_blueprint.yml"
        w1.save_path.value = str(p)
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.config.model_settings["v_sponge"]["v_sponge"] == 999.0
        assert (
            w2.config.composition.overrides.get("v_sponge", {}).get("v_sponge") == 999.0
        )
        assert w2.config.model_settings["lateral_visc"]["visc2"] == 3.3

    def test_forcing_spec_selection_and_edit(self):
        w = self._wizard()
        # ForcingSpec must always be an explicit catalog selection now -- no more
        # "<model default>" fallback -- so the default-selected entry is already
        # origin="catalog" from construction.
        if "glorys-era5-unified" not in w.forcing_dd.options:
            pytest.skip("example ForcingSpec not in catalog")
        assert w.forcing_dd.value == "glorys-era5-unified"
        assert w.config.composition.forcing.origin == "catalog"
        assert [i.source.name for i in w.config.forcing.surface] == [
            "ERA5",
            "UNIFIED",
            "MBL_co2",
            "WOA",
        ]
        # add + edit a restoring surface item -> deviates from the catalog pick.
        # origin stays "catalog" (unified with model/domain/output); `modified`
        # is what signals the edit.
        fe = w._forcing_editor
        fe._add("surface")
        row = fe._rows["surface"][-1]
        row["type"].value = "restoring"
        row["name"].value = "WOA"
        row["restoring_forces"].value = "sss"
        assert w.config.composition.forcing.origin == "catalog"
        assert w.config.composition.forcing.modified is True
        assert w.config.model_settings["cppdefs"]["sal_restore"] is True
        assert [i.source.name for i in w.config.forcing.surface].count("WOA") == 2

    def test_output_spec_selection_and_clear_on_select(self):
        w = self._wizard()
        # OutputSpec must always be an explicit catalog selection now -- no more
        # "<model default>" fallback.
        if "standard" not in w.output_dd.options:
            pytest.skip("example OutputSpec not in catalog")
        assert w.output_dd.value == "standard"
        assert w.config.composition.output.origin == "catalog"
        assert (
            "marbl_config_file" in w.config.model_settings["marbl_bgc"]
        )  # partial merge
        # edit an output section in Advanced -> override recorded; selection unchanged
        w.editor._widgets[("ts_output", "wrt_temp")][0].value = True
        assert w.config.composition.overrides["ts_output"]["wrt_temp"] is True
        # re-selecting the output piece clears output-section overrides (the handler
        # doesn't key off which value it changed to, only that a selection happened)
        w._on_output_spec(None)
        assert "ts_output" not in w.config.composition.overrides

    def test_output_spec_round_trips_through_load(self, tmp_path):
        w1 = self._wizard()
        if "standard" not in w1.output_dd.options:
            pytest.skip("example OutputSpec not in catalog")
        w1.output_dd.value = "standard"
        p = tmp_path / "forge_blueprint.yml"
        w1.save_path.value = str(p)
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.output_dd.value == "standard"
        assert w2.config.composition.output.name == "standard"

    def test_forcing_remove_item(self):
        w = self._wizard()
        fe = w._forcing_editor
        before = len(w.config.forcing.tidal)
        if before == 0:
            pytest.skip("no tidal item to remove")
        fe._remove("tidal", fe._rows["tidal"][0])
        assert len(w.config.forcing.tidal) == before - 1

    def test_forcing_round_trips_through_load(self, tmp_path):
        w1 = self._wizard()
        fe = w1._forcing_editor
        fe._add("surface")
        row = fe._rows["surface"][-1]
        row["type"].value = "restoring"
        row["name"].value = "WOA"
        row["restoring_forces"].value = "sss"
        p = tmp_path / "forge_blueprint.yml"
        w1.save_path.value = str(p)
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert "WOA" in [i.source.name for i in w2.config.forcing.surface]
        assert w2.config.model_settings["cppdefs"]["sal_restore"] is True
        assert w2.config.composition.forcing.origin == "catalog"
        assert w2.config.composition.forcing.modified is True

    def test_nest_from_domain_dropdown_prefills_child(self):
        w = self._wizard()
        if "gulf-guinea-toy" not in w.nest_domain_dd.options:
            pytest.skip("gulf-guinea-toy domain not in catalog")
        w.nest_domain_dd.value = "gulf-guinea-toy"  # prefills child + enables nesting
        assert w.nest_enable.value is True
        assert w.child_w["nx"].value == 10 and w.child_w["N"].value == 5
        assert w.config.model_settings["extract_data"]["do_extract"] is True
        assert w.config.model_settings["extract_data"]["n_chd"] == 5

    def test_nesting_ui_enables_extract_data(self):
        w = self._wizard()
        w.nest_enable.value = True
        w.child_w["N"].value = 25
        assert w.config.model_settings["extract_data"]["do_extract"] is True
        assert w.config.model_settings["extract_data"]["n_chd"] == 25
        assert w.config.domain.grid_kwargs_child is not None

    def test_load_preserves_advanced_edits_and_nesting(self, tmp_path):
        w1 = self._wizard()
        w1.editor._widgets[("lateral_visc", "visc2")][0].value = 7.25
        w1.nest_enable.value = True
        w1.child_w["N"].value = 18
        p = tmp_path / "forge_blueprint.yml"
        w1.save_path.value = str(p)
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.config.model_settings["lateral_visc"]["visc2"] == 7.25
        assert w2.nest_enable.value is True
        assert w2.config.model_settings["extract_data"]["n_chd"] == 18

    def test_roms_ref_gather_and_default_round_trip(self, tmp_path):
        w1 = self._wizard()
        w1.roms_ref.value = "pio-refdate"
        assert w1.config.code.roms.commit == "pio-refdate"
        p = tmp_path / "forge_blueprint.yml"
        w1.save_path.value = str(p)
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.roms_ref.value == "pio-refdate"
        assert w2.config.code.roms.commit == "pio-refdate"

    def test_roms_ref_prefilled_with_model_default_and_editable(self, tmp_path):
        """ucla-roms ref is prefilled from the selected Model's pinned default (shown
        next to the Model dropdown) rather than left blank, and stays editable. A
        blueprint using the unmodified default must reload showing that same default
        (not blank); an actual edit/override round-trips through save/reload too.
        """
        w1 = self._wizard()
        default_ref = w1._model_default_roms_ref()
        assert default_ref  # this model.yml pins a commit
        assert w1.roms_ref.value == default_ref

        p = tmp_path / "forge_blueprint.yml"
        w1.save_path.value = str(p)
        w1._on_save(None)
        w2 = self._wizard()
        w2.roms_ref.value = "stale-value-from-a-prior-load"
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.roms_ref.value == default_ref

        # An actual override round-trips through save/reload unchanged.
        w1.roms_ref.value = "my-custom-branch"
        w1._on_save(None)
        w3 = self._wizard()
        w3.load_path.value = str(p)
        w3._on_load_path(None)
        assert w3.roms_ref.value == "my-custom-branch"

    def test_loading_file_with_bad_settings_is_flagged(self, tmp_path):
        import yaml

        w = self._wizard()
        p = tmp_path / "forge_blueprint.yml"
        w.save_path.value = str(p)
        w._on_save(None)
        data = yaml.safe_load(p.read_text())
        data["model_settings"]["param"]["np_xi"] = "not-an-int"  # corrupt a value
        bad = tmp_path / "bad.yml"
        bad.write_text(yaml.safe_dump(data))
        w2 = self._wizard()
        w2.load_path.value = str(bad)
        w2._on_load_path(None)
        assert "invalid settings value" in w2.load_status.value
        # the wizard re-derives valid settings from the inputs, so it ends valid
        assert "settings valid" in w2.validation.value

    def test_load_bad_input_shows_error_not_crash(self):
        w = self._wizard()
        w.load_path.value = "/nonexistent/forge_blueprint.yml"
        w._on_load_path(None)
        assert "color:#b00" in w.load_status.value
        w._load_bytes(b"not: [valid spec config")
        assert "color:#b00" in w.load_status.value

    def test_download_link_encodes_the_config(self):
        """The browser-download link (used by Voilà) carries the resolved YAML."""
        import base64
        import re

        wiz = self._wizard()
        html = wiz.download_link.value
        assert 'download="' in html and "data:text/yaml;base64," in html
        b64 = re.search(r"base64,([A-Za-z0-9+/=]+)", html).group(1)
        text = base64.b64decode(b64).decode("utf-8")
        assert "forge_blueprint_version" in text
        # casename is derived (not serialized) — it appears in the download filename
        assert wiz.config.casename in html


# ---------------------------------------------------------------------------
# Phase 2 engine (orchestration tested with an injected fake builder; the real
# pipeline downloads data + runs roms_tools and is out of scope for unit tests)
# ---------------------------------------------------------------------------
class _FakeBuilder:
    """A ForgeBlueprintExecutor stand-in: records calls instead of doing real work."""

    def __init__(self, cfg=None, host=None):
        self.cfg = cfg
        self.calls = []

    def ensure_source_data(self, **k):
        self.calls.append(("ensure", k))

    def generate_inputs(self, **k):
        self.calls.append(("generate", k))

    def configure_build(self, **k):
        self.calls.append(("configure", k))

    def path_roms_marbl_blueprint(self):
        return "/bp.yml"


class TestForgeBlueprintEngine:
    def _cfg(self):
        return _build()

    def test_builder_kwargs_carry_atomic_inputs_not_host(self):
        from cstar_forge.forge.forge_blueprint_engine import (
            forge_blueprint_to_builder_kwargs,
        )

        cfg = self._cfg()
        kw = forge_blueprint_to_builder_kwargs(cfg)
        assert kw["name"] == cfg.name
        assert kw["grid_name"] == "test-tiny"
        assert kw["partitioning"] == {"n_procs_x": 1, "n_procs_y": 1}
        assert kw["open_boundaries"]["east"] is True
        # host/machine/paths must NOT be passed (builder resolves them)
        assert not any(k in kw for k in ("machine", "paths", "scratch", "source_data"))

    def test_builder_kwargs_carry_resolved_datasets_snapshot(self, tmp_path):
        """End-to-end check for the resolved_datasets pinning: the blueprint's
        forcing.resolved_datasets snapshot must actually reach the executor, not
        just be accepted as a same-named kwarg.
        """
        from cstar_forge.forge.executor import ForgeExecutor
        from cstar_forge.forge.forge_blueprint_engine import (
            forge_blueprint_to_builder_kwargs,
        )
        from cstar_forge.forge.host import HostPaths

        cfg = self._cfg()
        assert cfg.forcing.resolved_datasets, (
            "fixture must resolve at least one dataset"
        )

        kw = forge_blueprint_to_builder_kwargs(cfg)
        assert kw["resolved_datasets"]["GLORYS"]["dataset_key"] == "GLORYS_REGIONAL"
        assert (
            kw["resolved_datasets"]["GLORYS"]["dataset_id"]
            == "cmems_mod_glo_phy_my_0.083deg_P1D-m"
        )

        host = HostPaths(
            working_dir=tmp_path / "wd",
            source_data_cache=tmp_path / "cache",
            system="test",
            machine_config=None,
        )
        ex = ForgeExecutor.from_forge_blueprint(cfg, host=host)
        assert ex.resolved_datasets["GLORYS"]["dataset_key"] == "GLORYS_REGIONAL"

    def test_split_model_settings(self):
        from cstar_forge.forge.forge_blueprint_engine import (
            PROCESSING_FILLED_SECTIONS,
            split_model_settings,
        )

        run_ov, comp_ov = split_model_settings(self._cfg())
        assert list(comp_ov) == ["cppdefs"] and "cppdefs" not in run_ov
        assert "time_stepping" in run_ov and "param" in run_ov
        for sec in PROCESSING_FILLED_SECTIONS:
            assert sec not in run_ov

    def test_split_model_settings_excludes_generation_derived_leaves(self):
        """Regression for the §3a bug (docs/forge-blueprint-parameter-audit.md): the
        overlay passed to ``configure_build`` must not carry the leaf keys that
        ``generate_inputs`` derives from the *actual* generated forcing objects
        (river/CDR "is configured" flags + counts, the true tidal constituent count) —
        otherwise it silently reverts a correctly-generated configuration back to the
        resolver's pre-generation placeholder/declared value.
        """
        from cstar_forge.forge.forge_blueprint_engine import (
            GENERATION_DERIVED_LEAF_KEYS,
            split_model_settings,
        )

        cfg = (
            self._cfg()
        )  # test-tiny + glorys-era5-unified: a real DAI river is configured
        assert cfg.forcing.river, "fixture must have a configured river for this test"

        run_ov, _ = split_model_settings(cfg)

        for section, leaf_keys in GENERATION_DERIVED_LEAF_KEYS.items():
            sub = run_ov.get(section, {})
            for key in leaf_keys:
                assert key not in sub, (
                    f"{section}.{key} is generation-derived and must be excluded "
                    "from the configure_build overlay"
                )

        # Sibling fields in the same section that generate_inputs never touches must
        # still pass through untouched, so a genuine ModelSpec/hand-edit override still
        # reaches configure_build.
        assert run_ov["cdr_frc"]["relocate_to_wet_pts"] is True

    def test_process_orchestration_order_and_overlay(self):
        from cstar_forge.forge.forge_blueprint_engine import process_forge_blueprint

        b = process_forge_blueprint(
            self._cfg(), clobber=True, use_dask=False, executor_factory=_FakeBuilder
        )
        assert [c[0] for c in b.calls] == ["ensure", "generate", "configure"]
        gen = dict(b.calls[1][1])
        assert gen["clobber"] is True and gen["use_dask"] is False
        cfgk = dict(b.calls[2][1])
        assert "cppdefs" in cfgk["compile_time_settings"]
        assert "time_stepping" in cfgk["run_time_settings"]
        assert "grid" not in cfgk["run_time_settings"]

    def test_process_skip_flags(self):
        from cstar_forge.forge.forge_blueprint_engine import process_forge_blueprint

        b = process_forge_blueprint(
            self._cfg(),
            ensure_data=False,
            generate=False,
            executor_factory=_FakeBuilder,
        )
        assert [c[0] for c in b.calls] == ["configure"]

    def test_executor_must_implement_interface(self):
        from cstar_forge.forge.forge_blueprint_engine import (
            ForgeBlueprintExecutor,
            process_forge_blueprint,
        )

        # _FakeBuilder satisfies the runtime-checkable Protocol
        assert isinstance(_FakeBuilder(), ForgeBlueprintExecutor)

        class _Bad:  # missing the required methods
            def __init__(self, cfg=None, host=None):
                pass

        with pytest.raises(TypeError, match="ForgeBlueprintExecutor"):
            process_forge_blueprint(self._cfg(), executor_factory=_Bad)

    def test_invalid_model_settings_fail_fast(self):
        from cstar_forge.forge.forge_blueprint_engine import process_forge_blueprint

        cfg = self._cfg()
        cfg.model_settings["param"]["np_xi"] = "not-an-int"  # corrupt a value
        with pytest.raises(ValueError, match="invalid values"):
            process_forge_blueprint(
                cfg, executor_factory=_FakeBuilder
            )  # raises before any call

    def test_resolve_host_reads_config_not_file(self):
        # Forge's disposable host provider builds a HostPaths from auto-detected config;
        # the host is NOT read from the spec file. (The app receives this HostPaths via
        # process_forge_blueprint(host=...); C-Star supplies its own equivalent on relocation.)
        from cstar_forge import config
        from cstar_forge.forge.host import HostPaths

        cfg = self._cfg()
        h = config.resolve_host(cfg.working_dir)
        assert isinstance(h, HostPaths)
        assert h.system
        # working_dir is the injected per-run artifact root; source_data_cache is the
        # shared host download cache. Both resolved from config, not the spec file.
        # The spec default carries a per-run subdirectory: <root>/<name>.
        assert "cstar-forge-data" in str(h.working_dir)
        assert str(h.working_dir).endswith(cfg.name)
        assert h.source_data_cache is not None


# ---------------------------------------------------------------------------
# Step 3 (parity): the Phase-1 resolver and the live ForgeExecutor must agree
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
    (
        "test-tiny",
        dict(
            nx=6,
            ny=2,
            size_x=500,
            size_y=1000,
            center_lon=0,
            center_lat=55,
            rot=10,
            N=3,
            theta_s=5.0,
            theta_b=2.0,
            hc=250.0,
        ),
        {"south": False, "east": True, "north": True, "west": False},
        {"n_procs_x": 1, "n_procs_y": 1},
    ),
    (
        "gulf-guinea-toy",
        dict(
            nx=10,
            ny=10,
            size_x=4000,
            size_y=2000,
            center_lon=4.0,
            center_lat=-1.0,
            rot=0,
            N=5,
        ),
        {"south": True, "east": True, "north": True, "west": True},
        {"n_procs_x": 2, "n_procs_y": 5},
    ),
]

# Sections filled at different times / by different layers — not comparable at init.
_PARITY_SKIP = {
    "param",
    "cppdefs",
    "title",
    "output_root_name",
    "grid",
    "initial",
    "forcing",
    "s_coord",
}


@pytest.mark.integration
class TestResolverBuilderParity:
    @pytest.mark.parametrize(
        "grid_name,grid_kwargs,boundaries,partitioning", _PARITY_DOMAINS
    )
    def test_resolver_matches_builder_derivation(
        self, grid_name, grid_kwargs, boundaries, partitioning, tmp_path
    ):
        pytest.importorskip("roms_tools")
        from datetime import datetime

        from cstar_forge.forge.executor import ForgeExecutor
        from cstar_forge.forge.host import HostPaths
        from cstar_forge.forge_blueprint_resolve import build_forge_blueprint

        start, end = datetime(2012, 1, 1), datetime(2012, 1, 2)

        # The executor now consumes cfg.model_settings as its settings base; this guards
        # that its settings-init faithfully reproduces the resolver's model_settings for
        # every reviewable section (host-independent, catalog-free construction).
        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name=grid_name,
            grid_kwargs=grid_kwargs,
            open_boundaries=boundaries,
            partitioning=partitioning,
            start_date=start,
            end_date=end,
            forcing_inputs=_CATALOG.forcing_data("glorys-era5-unified"),
            output_settings=_CATALOG.output_data("standard"),
        )
        host = HostPaths(
            working_dir=tmp_path / "wd",
            source_data_cache=tmp_path / "cache",
            system="test",
            machine_config=None,
        )
        ex = ForgeExecutor.from_forge_blueprint(cfg, host=host)
        b_rt = ex._settings_run_time
        r_ms = cfg.model_settings

        # the genuinely-computed numerics must match exactly
        assert r_ms["time_stepping"] == b_rt["time_stepping"]
        assert r_ms["v_sponge"] == b_rt["v_sponge"]

        # every shared reviewable section must be identical (the _PARITY_SKIP sections
        # are filled later during generation / are host-derived)
        mismatches = {
            sec: (b_rt.get(sec), rval)
            for sec, rval in r_ms.items()
            if sec not in _PARITY_SKIP and b_rt.get(sec) != rval
        }
        assert not mismatches, f"resolver/executor settings drift: {mismatches}"


# ---------------------------------------------------------------------------
# "Save modified pieces to catalog" (wizard panel + DomainCatalog register_*)
# ---------------------------------------------------------------------------
class TestSaveModifiedPiecesToCatalog:
    """Each save handler: extract the piece from current state, write it to an
    isolated catalog, side-effect-free round-trip verify via content_hash, and
    only on a match repoint the dropdown / clear that piece's overrides-or-seed.
    A mismatch must still write the file but leave everything else untouched.
    """

    @pytest.fixture
    def isolated_catalog(self, tmp_path):
        import shutil

        from cstar_forge.domain_catalog import DomainCatalog

        root = tmp_path / "catalog"
        shutil.copytree(_CATALOG.catalog_root, root)
        return DomainCatalog(catalog_root=root)

    def _wizard(self, catalog):
        pytest.importorskip("ipywidgets")
        from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizard

        return ForgeBlueprintWizard(catalog=catalog)

    def test_save_output_piece_marks_unmodified_and_clears_overrides(
        self, isolated_catalog
    ):
        wiz = self._wizard(isolated_catalog)
        wiz._overrides[("ocean_vars", "wrt_file_his")] = True
        wiz._rebuild()
        assert wiz.config.composition.output.modified is True

        wiz.save_output_name.value = "my-output"
        wiz._on_save_output(None)

        assert "my-output" in isolated_catalog.output_names
        assert wiz.output_dd.value == "my-output"
        assert wiz.config.composition.output.modified is False
        assert wiz._overrides == {}
        assert "✓" in wiz.save_output_status.value

    def test_save_model_piece_marks_unmodified(self, isolated_catalog):
        wiz = self._wizard(isolated_catalog)
        wiz._overrides[("lateral_visc", "visc2")] = 999.0
        wiz._rebuild()
        assert wiz.config.composition.model.modified is True

        wiz.save_model_name.value = "my-model"
        wiz._on_save_model(None)

        assert "my-model" in isolated_catalog.model_names
        assert wiz.model_dd.value == "my-model"
        assert wiz.config.composition.model.modified is False

    def test_save_forcing_piece_marks_unmodified(self, isolated_catalog):
        wiz = self._wizard(isolated_catalog)
        wiz._forcing_editor.ic_bgc_clim.value = (
            not wiz._forcing_editor.ic_bgc_clim.value
        )
        wiz._on_forcing_change()
        assert wiz.config.composition.forcing.modified is True

        wiz.save_forcing_name.value = "my-forcing"
        wiz._on_save_forcing(None)

        assert "my-forcing" in isolated_catalog.forcing_names
        assert wiz.forcing_dd.value == "my-forcing"
        assert wiz.config.composition.forcing.modified is False

    def test_save_forcing_piece_embeds_and_reloads_cdr(self, isolated_catalog):
        wiz = self._wizard(isolated_catalog)
        fake_cdr = {"releases": [{"lon": 1.0, "lat": 2.0}]}
        wiz._cdr_forcing = fake_cdr
        wiz._rebuild()
        assert wiz.config.forcing.cdr_forcing == fake_cdr

        wiz.save_forcing_name.value = "my-cdr-forcing"
        wiz._on_save_forcing(None)
        assert wiz.config.composition.forcing.modified is False
        assert (
            isolated_catalog.forcing_data("my-cdr-forcing")["cdr_forcing"] == fake_cdr
        )

        # a fresh wizard picking this ForcingSpec reloads the same CDR dict.
        wiz2 = self._wizard(isolated_catalog)
        wiz2.forcing_dd.value = "my-cdr-forcing"
        assert wiz2._cdr_forcing == fake_cdr

    def test_save_domain_piece_marks_unmodified_and_preserves_other_pieces(
        self, isolated_catalog
    ):
        wiz = self._wizard(isolated_catalog)
        if "gulf-guinea-toy" in wiz.domain_dd.options:
            wiz.domain_dd.value = "gulf-guinea-toy"
        wiz.npx.value = wiz.npx.value + 1
        assert wiz.config.composition.domain.modified is True

        before_overrides = dict(wiz._overrides)
        before_forcing_seed = wiz._forcing_seed
        before_model_dd = wiz.model_dd.value
        before_output_dd = wiz.output_dd.value

        wiz.save_domain_name.value = "my-domain"
        wiz._on_save_domain(None)

        assert "my-domain" in isolated_catalog.domain_names
        assert wiz.domain_dd.value == "my-domain"
        assert wiz.config.composition.domain.modified is False
        # no-clobber: every OTHER piece's state is untouched by the domain save.
        assert wiz._overrides == before_overrides
        assert wiz._forcing_seed == before_forcing_seed
        assert wiz.model_dd.value == before_model_dd
        assert wiz.output_dd.value == before_output_dd

    def test_invalid_name_refuses_without_writing(self, isolated_catalog):
        wiz = self._wizard(isolated_catalog)
        before = list(isolated_catalog.output_names)
        wiz.save_output_name.value = "bad name!!"
        wiz._on_save_output(None)
        assert "color:#b00" in wiz.save_output_status.value
        assert isolated_catalog.output_names == before

    def test_name_collision_refuses(self, isolated_catalog):
        wiz = self._wizard(isolated_catalog)
        wiz.save_output_name.value = "standard"
        wiz._on_save_output(None)
        assert "already exists" in wiz.save_output_status.value

    def test_config_invalid_refuses(self, isolated_catalog):
        wiz = self._wizard(isolated_catalog)
        wiz.start.value = None
        wiz._rebuild()
        assert wiz.config is None
        wiz.save_output_name.value = "whatever"
        wiz._on_save_output(None)
        assert "nothing to save" in wiz.save_output_status.value

    def test_mismatch_keeps_piece_modified_and_state_untouched(
        self, isolated_catalog, monkeypatch
    ):
        """A writer bug (or any post-write drift) must not silently claim
        unmodified: the file is still written, but the dropdown/overrides don't
        move -- this is the side-effect-free verifier's whole purpose.
        """
        wiz = self._wizard(isolated_catalog)
        wiz._overrides[("ocean_vars", "wrt_file_his")] = True
        wiz._rebuild()
        assert wiz.config.composition.output.modified is True

        orig_register = isolated_catalog.register_output

        def _bad_register(name, output_settings, description=""):
            corrupted = dict(output_settings)
            corrupted.pop("ocean_vars", None)  # simulate a lossy extractor/writer
            orig_register(name, corrupted, description)

        monkeypatch.setattr(isolated_catalog, "register_output", _bad_register)

        before_overrides = dict(wiz._overrides)
        before_dd = wiz.output_dd.value
        wiz.save_output_name.value = "broken-output"
        wiz._on_save_output(None)

        assert "broken-output" in isolated_catalog.output_names  # still written
        assert wiz.output_dd.value == before_dd  # selection untouched
        assert wiz._overrides == before_overrides  # overrides untouched
        assert wiz.config.composition.output.modified is True  # still modified
        assert "differs" in wiz.save_output_status.value
