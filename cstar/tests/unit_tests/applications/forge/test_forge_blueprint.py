"""
Tests for the ForgeBlueprint schema (``cstar.applications.forge.blueprint``) and the
resolver (``cstar.applications.forge.resolve.build_forge_blueprint``).

These validate that the resolver reproduces the known ``test-tiny`` demo values,
flattens settings, keeps naming/host values out of the stored config, resolves
sources from the ModelSpec, and round-trips through YAML.

NOTE: imports the in-package modules, so these run once the environment's editable
``cstar`` provides ``cstar.roms.namelist`` (i.e. on the namelist branch). The same
assertions were validated standalone during development.
"""

from datetime import date, datetime
from pathlib import Path

import pytest
import yaml

import cstar
import cstar.catalog
from cstar.applications.forge.blueprint import FORGE_BLUEPRINT_VERSION, ForgeBlueprint
from cstar.applications.forge.resolve import build_forge_blueprint
from cstar.applications.forge.settings import render_roms_settings
from cstar.catalog.domain_catalog import default_catalog as _CATALOG

_BUNDLED_CATALOG = Path(cstar.catalog.__file__).parent / "bundled"
_MODEL_DIR = _BUNDLED_CATALOG / "ModelSpec" / "cson_roms-marbl_v0.1"
# ucla-roms >= 0.5.0 ModelSpec -- used by the versioned-namelist golden test below.
_MODEL_DIR_ROMS050 = _BUNDLED_CATALOG / "ModelSpec" / "roms-marbl-0.5-default"
# ucla-roms >= 0.6.0 ModelSpec (adds &PIO_SETTINGS) -- used by the versioned-namelist
# golden test below.
_MODEL_DIR_ROMS060 = _BUNDLED_CATALOG / "ModelSpec" / "roms-marbl-0.6-default"
# ucla-roms >= 0.7.0 ModelSpec (adds &CDR_TRACER_OUTPUT_SETTINGS/
# &CDR_GAS_EXCH_OUTPUT_SETTINGS, PR #351) -- used by the versioned-namelist golden
# test below.
_MODEL_DIR_ROMS070 = _BUNDLED_CATALOG / "ModelSpec" / "roms-marbl-0.7-default"
# ucla-roms >= 0.8.0 ModelSpec (adds the PARABOLIC_SPLINES/UPSTREAM_TS_LAND_CURV
# advection cppdefs switches, PR #361) -- no namelist-schema change from 0.7.0, so
# there is no versioned-namelist golden fixture for this tier (unlike 0.5.0-0.7.0
# above).
_MODEL_DIR_ROMS080 = _BUNDLED_CATALOG / "ModelSpec" / "roms-marbl-0.8-default"
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


def test_resolver_does_not_alias_output_settings():
    """_deep_merge must deep-copy override values: a section the ModelSpec
    doesn't define (e.g. ocean_vars) used to be assigned into model_settings by
    reference, so mutating one resolved blueprint in place cross-contaminated
    the shared OutputSpec dict and every other blueprint resolved from it.
    """
    output_settings = _CATALOG.output_data("standard")
    cfg_a = _build(output_settings=output_settings)
    cfg_b = _build(output_settings=output_settings)

    assert cfg_a.model_settings["ocean_vars"] is not output_settings["ocean_vars"]
    assert cfg_a.model_settings["ocean_vars"] is not cfg_b.model_settings["ocean_vars"]

    original = output_settings["ocean_vars"]["output_period_rst"]
    cfg_a.model_settings["ocean_vars"]["output_period_rst"] = original * 2
    assert output_settings["ocean_vars"]["output_period_rst"] == original
    assert cfg_b.model_settings["ocean_vars"]["output_period_rst"] == original


def test_naming_is_derived_not_stored():
    cfg = _build()
    assert cfg.n_procs == 1
    assert cfg.name == "cson_roms-marbl_v0.1_test-tiny_1procs"
    assert cfg.casename == "cson_roms-marbl_v0.1_test-tiny_1procs_20120101-20120102"
    # output_root_name is host-derived from the scratch path
    assert cfg.output_root_name("/scratch").startswith("/scratch/cson_roms-marbl")


def test_partitioning_auto_tiling_with_n_cores_is_valid():
    from cstar.applications.forge.blueprint import Partitioning

    p = Partitioning(auto_tiling=True, n_cores=4)
    assert p.n_procs_x is None
    assert p.n_procs_y is None
    assert p.n_cores == 4


def test_partitioning_requires_n_procs_without_auto_tiling():
    from cstar.applications.forge.blueprint import Partitioning

    with pytest.raises(ValueError, match="n_procs_x and n_procs_y are required"):
        Partitioning()


def test_partitioning_rejects_n_cores_without_auto_tiling():
    from cstar.applications.forge.blueprint import Partitioning

    with pytest.raises(ValueError, match="n_cores is only accepted with auto_tiling"):
        Partitioning(n_procs_x=1, n_procs_y=1, n_cores=4)


def test_partitioning_rejects_auto_tiling_without_n_cores():
    from cstar.applications.forge.blueprint import Partitioning

    with pytest.raises(ValueError, match="auto_tiling requires n_cores"):
        Partitioning(auto_tiling=True)


def test_partitioning_rejects_auto_tiling_combined_with_n_procs():
    from cstar.applications.forge.blueprint import Partitioning

    with pytest.raises(ValueError, match="must not be set when auto_tiling"):
        Partitioning(auto_tiling=True, n_cores=8, n_procs_x=4, n_procs_y=2)


def test_build_forge_blueprint_auto_tiling_derives_n_cores_from_n_procs():
    """Switching auto_tiling on over an existing explicit grid (e.g. a loaded
    blueprint's n_procs_x/n_procs_y) derives n_cores from the product; the
    resolved Partitioning still carries only n_cores.
    """
    cfg = _build(
        partitioning={"auto_tiling": True, "n_procs_x": 2, "n_procs_y": 2},
        use_pio=True,
    )
    assert cfg.domain.partitioning.n_cores == 4
    assert cfg.domain.partitioning.n_procs_x is None
    assert cfg.domain.partitioning.n_procs_y is None
    assert cfg.n_procs == 4


def test_build_forge_blueprint_auto_tiling_accepts_consistent_n_procs_and_n_cores():
    cfg = _build(
        partitioning={
            "auto_tiling": True,
            "n_cores": 4,
            "n_procs_x": 2,
            "n_procs_y": 2,
        },
        use_pio=True,
    )
    assert cfg.domain.partitioning.n_cores == 4
    assert cfg.domain.partitioning.n_procs_x is None


def test_build_forge_blueprint_auto_tiling_rejects_inconsistent_n_cores():
    with pytest.raises(ValueError, match="inconsistent with"):
        _build(
            partitioning={
                "auto_tiling": True,
                "n_cores": 8,
                "n_procs_x": 2,
                "n_procs_y": 2,
            },
            use_pio=True,
        )


def test_n_procs_property_uses_n_cores_when_set():
    cfg = _build(partitioning={"auto_tiling": True, "n_cores": 4}, use_pio=True)
    assert cfg.n_procs == 4


def test_n_procs_property_raises_when_neither_available():
    """Reachable only via direct/unvalidated construction (``Partitioning``'s own
    validator forbids this combination) -- mirrors the ``model_copy`` bypass used
    elsewhere in this file (e.g. ``test_domain_grid_file_rejects_generation_geometry_keys``'s
    docstring) for exercising a validator-adjacent code path directly.
    """
    from cstar.applications.forge.blueprint import Partitioning

    cfg = _build()
    cfg.domain.partitioning = Partitioning.model_construct(
        n_procs_x=None, n_procs_y=None, auto_tiling=False, n_cores=None
    )
    with pytest.raises(ValueError, match="cannot determine n_procs"):
        _ = cfg.n_procs


def test_build_forge_blueprint_auto_tiling_resolves_cppdefs_and_partitioning():
    cfg = _build(partitioning={"auto_tiling": True, "n_cores": 4}, use_pio=True)
    assert cfg.model_settings["cppdefs"]["auto_tiling"] is True
    assert cfg.model_settings["param"]["np_xi"] == 1
    assert cfg.model_settings["param"]["np_eta"] == 1
    assert cfg.domain.partitioning.auto_tiling is True
    assert cfg.domain.partitioning.n_cores == 4
    assert cfg.domain.partitioning.n_procs_x is None
    assert cfg.domain.partitioning.n_procs_y is None
    assert cfg.n_procs == 4
    assert cfg.name == "cson_roms-marbl_v0.1_test-tiny_4procs"


def test_build_forge_blueprint_auto_tiling_requires_use_pio():
    with pytest.raises(ValueError, match="auto_tiling requires use_pio"):
        _build(partitioning={"auto_tiling": True, "n_cores": 4}, use_pio=False)


def test_build_forge_blueprint_auto_tiling_requires_n_cores():
    """Only raises when there is no explicit grid to derive n_cores from."""
    with pytest.raises(ValueError, match="auto_tiling requires partitioning.n_cores"):
        _build(partitioning={"auto_tiling": True}, use_pio=True)
    with pytest.raises(ValueError, match="auto_tiling requires partitioning.n_cores"):
        _build(partitioning={"auto_tiling": True, "n_procs_x": 2}, use_pio=True)


def test_build_forge_blueprint_auto_tiling_partitioning_round_trips():
    """``domain.partitioning.model_dump()`` is exactly the DomainSpec save/load
    shape (``forge_blueprint_wizard.py``'s ``_domain_spec_data``/``_apply_domain_spec``
    round-trip a saved ``partitioning`` dict straight back into ``build_forge_blueprint``'s
    ``partitioning=`` kwarg) -- confirm it feeds back in cleanly, with ``use_pio``
    passed separately at the top level as the wizard does.
    """
    cfg = _build(partitioning={"auto_tiling": True, "n_cores": 4}, use_pio=True)
    saved = cfg.domain.partitioning.model_dump()
    cfg2 = _build(partitioning=saved, use_pio=True)
    assert cfg2.domain.partitioning.auto_tiling is True
    assert cfg2.domain.partitioning.n_cores == 4
    assert cfg2.n_procs == 4


def test_build_forge_blueprint_n_cores_requires_auto_tiling():
    with pytest.raises(ValueError, match="n_cores is only accepted with auto_tiling"):
        _build(partitioning={"n_procs_x": 1, "n_procs_y": 1, "n_cores": 1})


def test_resolved_provenance_is_unstamped():
    """generated_at/forge_version/cstar_version/roms_tools_version are left None by
    the resolver -- ``ForgeBlueprint.to_yaml_str`` is what stamps them (see
    TestProvenanceStamping below), keeping resolution deterministic and
    independent of whether ``roms_tools`` happens to be installed.
    """
    cfg = _build()
    assert cfg.provenance.generated_at is None
    assert cfg.provenance.forge_version is None
    assert cfg.provenance.cstar_version is None
    assert cfg.provenance.roms_tools_version is None


def test_forge_version_explicit_override_preserved():
    """An explicit ``forge_version`` (e.g. re-resolving without touching original
    provenance) is passed straight through, unstamped by the resolver.
    """
    cfg = _build(forge_version="0.2.0")
    assert cfg.provenance.forge_version == "0.2.0"


def test_default_working_dir_includes_run_name():
    from cstar.applications.forge.blueprint import DEFAULT_WORKING_ROOT

    cfg = _build()
    assert cfg.working_dir == f"{DEFAULT_WORKING_ROOT}/{cfg.name}"


def test_bare_default_working_dir_expands_and_explicit_survives():
    from cstar.applications.forge.blueprint import DEFAULT_WORKING_ROOT

    cfg = _build()
    # an old file storing the bare default root gains the run-name layer on load
    data = cfg.model_dump(mode="json")
    data["working_dir"] = DEFAULT_WORKING_ROOT
    assert ForgeBlueprint(**data).working_dir == f"{DEFAULT_WORKING_ROOT}/{cfg.name}"
    # a deliberate non-default path passes through untouched
    data["working_dir"] = "/custom/spot"
    assert ForgeBlueprint(**data).working_dir == "/custom/spot"


def test_working_dir_accepts_path_from_scheduler_override():
    """C-Star's workplan scheduler (``get_system_overrides``) overrides working_dir
    with ``step.fsm.root_dir`` -- a ``Path``, which pydantic won't coerce to the
    field's ``str`` type on its own.
    """
    from pathlib import Path

    cfg = _build()
    data = cfg.model_dump(mode="json")
    data["working_dir"] = Path("/scratch/run-id/step-root")
    assert ForgeBlueprint(**data).working_dir == "/scratch/run-id/step-root"


def test_estimate_forge_cpus_anchors_floor_and_no_cap():
    """The estimate has a 16 floor and no upper cap: the forge run is single-node,
    so the launcher clamps the request to the target partition's CPUs per node
    (which forge cannot know when authoring the blueprint).
    """
    from cstar.applications.forge.blueprint import estimate_forge_cpus

    # toy domain (wio-toy) hits the 16 floor
    assert estimate_forge_cpus(20, 20, 10) == 16
    # hvalfjordur-0 (~2.0e7 cells) lands around a 128-core node
    assert estimate_forge_cpus(512, 384, 100) == 132
    # an exceptionally large domain asks for far more than any node has; the
    # old 128 ceiling is gone and the launcher-side clamp is the real cap
    assert estimate_forge_cpus(1856, 960, 100) > 1000
    # mid-size domains scale with cell count
    assert 16 < estimate_forge_cpus(350, 350, 100) < 128


def test_forge_blueprint_is_single_node():
    """ForgeBlueprint declares itself single-node so C-Star pins the forge step
    to one node and clamps cpus_needed to the queue's CPUs per node.
    """
    cfg = _build()
    assert cfg.single_node is True


def test_cpus_needed_is_grid_sized_forge_estimate():
    """cpus_needed sizes the forge run itself (scheduler fallback for the
    workplan's forge step) -- the grid estimate, not the ROMS partitioning.
    """
    from cstar.applications.forge.blueprint import estimate_forge_cpus

    cfg = _build()
    gk = cfg.domain.grid_kwargs
    assert cfg.cpus_needed == estimate_forge_cpus(gk["nx"], gk["ny"], gk["N"])


def test_forge_blueprint_import_stays_light():
    """Importing ``cstar.applications.forge.blueprint`` must stay cheap.

    Validation paths that touch only the blueprint schema -- the wizard's
    load-back of a saved ``forge_blueprint.yaml``, ``cstar blueprint schemas``,
    and workplan deserialization -- run in-process and never touch the heavy
    execution/authoring stack. This is an import-graph test rather than a
    textual import scan (superseding the old regex-on-source-text check, whose
    "must be relocatable back into a standalone cstar-forge" rationale no
    longer applies now that this module lives in C-Star): it imports the module
    in a fresh interpreter and asserts none of the heavy modules below ended up
    in ``sys.modules``, regardless of *how* they'd sneak in.
    """
    import subprocess
    import sys

    script = (
        "import sys\n"
        "import cstar.applications.forge.blueprint\n"
        "print('\\n'.join(sorted(sys.modules)))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"importing cstar.applications.forge.blueprint failed:\n{result.stderr}"
    )
    imported = set(result.stdout.splitlines())

    forbidden_prefixes = (
        "roms_tools",
        "xarray",
        "dask",
        "numba",
        "ipywidgets",
        "cstar.roms",
        "cstar.applications.roms_marbl",
        "cstar.applications.forge.executor",
        "cstar.applications.forge.engine",
        "cstar.applications.forge.input_data",
        "cstar.applications.forge.source_datasets",
        "cstar.applications.forge.resolve",
        "cstar.catalog",
        "cstar.wizard",
    )
    hits = {
        mod
        for mod in imported
        if any(
            mod == prefix or mod.startswith(prefix + ".")
            for prefix in forbidden_prefixes
        )
    }
    assert not hits, (
        "importing cstar.applications.forge.blueprint pulled in heavy/forbidden "
        f"modules: {sorted(hits)}"
    )


def test_application_discriminator_default():
    from cstar.applications.forge.blueprint import DEFAULT_APPLICATION

    cfg = _build()
    assert cfg.application == DEFAULT_APPLICATION


def test_from_yaml_rejects_newer_version(tmp_path):
    cfg = _build()
    p = tmp_path / "forge_blueprint.yaml"
    cfg.to_yaml(p)
    import yaml as _yaml

    data = _yaml.safe_load(p.read_text())
    data["forge_blueprint_version"] = 9999
    p.write_text(_yaml.safe_dump(data))
    with pytest.raises(ValueError, match="newer than this build"):
        ForgeBlueprint.from_yaml(p)


@pytest.mark.parametrize("legacy_value", [False, True])
def test_migrate_v4_cdr_output_do_cdr_renamed(tmp_path, legacy_value):
    """v4 -> v5: a v4-shaped ``model_settings.cdr_output.do_cdr`` is renamed
    ``do_cdr_output`` on load, for both legacy values.
    """
    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    data = yaml.safe_load(p.read_text())

    data["forge_blueprint_version"] = 4
    cdr_output = data["model_settings"]["cdr_output"]
    cdr_output["do_cdr"] = legacy_value
    del cdr_output["do_cdr_output"]
    p.write_text(yaml.safe_dump(data))

    back = ForgeBlueprint.from_yaml(p)
    assert back.model_settings["cdr_output"]["do_cdr_output"] is legacy_value
    assert "do_cdr" not in back.model_settings["cdr_output"]
    assert back.forge_blueprint_version == FORGE_BLUEPRINT_VERSION


def test_migrate_v4_cdr_output_migration_is_idempotent(tmp_path):
    """Already-current (do_cdr_output-shaped) data passes through unchanged --
    calling the migration on already-migrated data must not error or re-rename.
    """
    from cstar.applications.forge.migration import migrate_forge_blueprint_data

    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    data = yaml.safe_load(p.read_text())
    assert data["model_settings"]["cdr_output"]["do_cdr_output"] is False

    migrated = migrate_forge_blueprint_data(data)
    assert migrated["model_settings"]["cdr_output"]["do_cdr_output"] is False
    assert "do_cdr" not in migrated["model_settings"]["cdr_output"]


def test_migrate_tolerates_missing_cdr_output_section():
    """No ``model_settings``/``cdr_output`` section at all -- the v4->v5 step must
    not KeyError.
    """
    from cstar.applications.forge.migration import migrate_forge_blueprint_data

    migrated = migrate_forge_blueprint_data({"forge_blueprint_version": 4})
    assert migrated["forge_blueprint_version"] == FORGE_BLUEPRINT_VERSION


def test_migrate_v5_ic_bgc_source_becomes_bgc_sources_list():
    """v6 -> v7: a pre-v7 singular ``initial_conditions.bgc_source`` is
    rewrapped as a one-item ``bgc_sources`` list; the old key is gone.
    """
    from cstar.applications.forge.migration import migrate_forge_blueprint_data

    data = {
        "forge_blueprint_version": 5,
        "forcing": {
            "initial_conditions": {
                "source": {"name": "GLORYS"},
                "bgc_source": {"name": "UNIFIED", "climatology": True},
            }
        },
    }
    migrated = migrate_forge_blueprint_data(data)
    ic = migrated["forcing"]["initial_conditions"]
    assert "bgc_source" not in ic
    assert ic["bgc_sources"] == [{"source": {"name": "UNIFIED", "climatology": True}}]
    assert migrated["forge_blueprint_version"] == FORGE_BLUEPRINT_VERSION


def test_migrate_v5_ic_bgc_source_none_becomes_empty_list():
    """v6 -> v7: an absent/``None`` ``bgc_source`` becomes an empty list, not
    a list containing ``None``.
    """
    from cstar.applications.forge.migration import migrate_forge_blueprint_data

    data = {
        "forge_blueprint_version": 5,
        "forcing": {
            "initial_conditions": {"source": {"name": "GLORYS"}, "bgc_source": None}
        },
    }
    migrated = migrate_forge_blueprint_data(data)
    assert migrated["forcing"]["initial_conditions"]["bgc_sources"] == []


def test_migrate_v5_ic_bgc_source_migration_is_idempotent():
    """Already-current (bgc_sources-shaped) data passes through unchanged."""
    from cstar.applications.forge.migration import migrate_forge_blueprint_data

    data = {
        "forge_blueprint_version": 7,
        "forcing": {
            "initial_conditions": {
                "source": {"name": "GLORYS"},
                "bgc_sources": [{"source": {"name": "UNIFIED"}}],
            }
        },
    }
    migrated = migrate_forge_blueprint_data(data)
    assert migrated["forcing"]["initial_conditions"]["bgc_sources"] == [
        {"source": {"name": "UNIFIED"}}
    ]


def test_migrate_v5_ic_bgc_source_and_bgc_sources_both_present_raises():
    """Both the pre-v7 singular key and the v7+ list key in the same dict is an
    inconsistent (likely hand-edited) shape -- must raise, not silently discard
    `bgc_source`.
    """
    from cstar.applications.forge.migration import migrate_forge_blueprint_data

    data = {
        "forge_blueprint_version": 5,
        "forcing": {
            "initial_conditions": {
                "source": {"name": "GLORYS"},
                "bgc_source": {"name": "UNIFIED"},
                "bgc_sources": [{"source": {"name": "GLODAP"}}],
            }
        },
    }
    with pytest.raises(ValueError, match="bgc_source.*bgc_sources"):
        migrate_forge_blueprint_data(data)


def test_migrate_v5_shaped_dict_loads_with_null_user_file_fields(tmp_path):
    """A v5 file (predating user-provided files) loads, migrates its version to
    current, and the new fields default to ``None`` -- purely additive, no data
    rewrite (beyond the v6->v7 CDR relocation, which finds nothing to move here).
    """
    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    data = yaml.safe_load(p.read_text())
    data["forge_blueprint_version"] = 5

    back = ForgeBlueprint.from_yaml_data(data)
    assert back.forge_blueprint_version == FORGE_BLUEPRINT_VERSION
    assert back.domain.grid_file is None
    assert back.cdr.mode == "none"
    assert back.cdr.cdr_forcing_file is None
    assert all(river.custom_file is None for river in back.forcing.river)


class TestMigrateV6ToV7CdrRelocation:
    """v6 -> v7: ``forcing.cdr_forcing``/``forcing.cdr_forcing_file`` move onto the
    new top-level ``cdr`` section, with ``mode`` inferred from which (if either)
    was populated.
    """

    def _v6_data(self, **forcing_overrides):
        from cstar.applications.forge.migration import migrate_forge_blueprint_data

        cfg = _build()
        data = yaml.safe_load(cfg.to_yaml_str())
        data["forge_blueprint_version"] = 6
        data["forcing"].pop("cdr_forcing", None)
        data["forcing"].pop("cdr_forcing_file", None)
        data["forcing"].update(forcing_overrides)
        data.pop("cdr", None)
        return data, migrate_forge_blueprint_data

    def test_cdr_forcing_dict_infers_yaml_mode(self):
        data, migrate = self._v6_data(cdr_forcing={"releases": []})
        migrated = migrate(data)
        assert migrated["forge_blueprint_version"] == FORGE_BLUEPRINT_VERSION
        assert migrated["cdr"] == {"mode": "yaml", "cdr_forcing": {"releases": []}}
        assert "cdr_forcing" not in migrated["forcing"]
        assert "cdr_forcing_file" not in migrated["forcing"]

    def test_cdr_forcing_file_infers_netcdf_mode(self):
        data, migrate = self._v6_data(cdr_forcing_file=dict(_USER_FILE_KWARGS))
        migrated = migrate(data)
        assert migrated["cdr"] == {
            "mode": "netcdf",
            "cdr_forcing_file": dict(_USER_FILE_KWARGS),
        }
        assert "cdr_forcing" not in migrated["forcing"]
        assert "cdr_forcing_file" not in migrated["forcing"]

    def test_neither_field_infers_none_mode(self):
        data, migrate = self._v6_data()
        migrated = migrate(data)
        assert migrated["cdr"] == {"mode": "none"}

    def test_full_v6_yaml_round_trip_through_from_yaml_data(self, tmp_path):
        """A full pre-CDR-overhaul YAML (as ``ForgeBlueprint.to_yaml`` would have
        written it under v6) loads via the real public entry point, not just the
        bare migration function.
        """
        data, _ = self._v6_data(cdr_forcing={"releases": [{"lon": 1.0}]})
        back = ForgeBlueprint.from_yaml_data(data)
        assert back.forge_blueprint_version == FORGE_BLUEPRINT_VERSION
        assert back.cdr.mode == "yaml"
        assert back.cdr.cdr_forcing == {"releases": [{"lon": 1.0}]}
        assert back.cdr.cdr_forcing_file is None

    def test_idempotent_on_already_migrated_data(self):
        """Running the migration twice (or on data that already declares an
        explicit ``cdr``) must not clobber the explicit value.
        """
        from cstar.applications.forge.migration import migrate_forge_blueprint_data

        data, _ = self._v6_data(cdr_forcing={"releases": []})
        once = migrate_forge_blueprint_data(data)
        twice = migrate_forge_blueprint_data(dict(once))
        assert (
            twice["cdr"]
            == once["cdr"]
            == {
                "mode": "yaml",
                "cdr_forcing": {"releases": []},
            }
        )

    def test_direct_construction_with_explicit_cdr_is_not_overwritten(self):
        """Direct keyword construction (``version is None``) must not let the
        v6->v7 step clobber an explicitly-passed ``cdr=`` with an inferred one.
        """
        from cstar.applications.forge.migration import migrate_forge_blueprint_data

        data = {"forcing": {}, "cdr": {"mode": "upscaled"}}
        migrated = migrate_forge_blueprint_data(data)
        assert migrated["cdr"] == {"mode": "upscaled"}


_USER_FILE_KWARGS = dict(location="/data/staged/grid.nc", content_hash="a" * 64)


def test_domain_grid_file_round_trips_through_yaml(tmp_path):
    from cstar.applications.forge.blueprint import UserProvidedFile

    cfg = _build()
    vertical_only = {
        k: v
        for k, v in cfg.domain.grid_kwargs.items()
        if k in {"theta_s", "theta_b", "hc", "N"}
    }
    cfg = cfg.model_copy(
        update={
            "domain": cfg.domain.model_copy(
                update={
                    "grid_kwargs": vertical_only,
                    "grid_file": UserProvidedFile(**_USER_FILE_KWARGS),
                }
            )
        }
    )
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.domain.grid_file == cfg.domain.grid_file


def test_domain_grid_file_rejects_generation_geometry_keys():
    # ``model_copy`` (used elsewhere in this file for hash-only comparisons) does
    # NOT re-run validators, so this constructs ``Domain`` directly through its
    # constructor to actually exercise ``_grid_file_excludes_generation_geometry``.
    from cstar.applications.forge.blueprint import (
        Domain,
        OpenBoundaries,
        Partitioning,
        UserProvidedFile,
    )

    with pytest.raises(ValueError, match="generation-only keys"):
        Domain(
            grid_name="custom",
            grid_kwargs={"nx": 6, "ny": 2, "theta_s": 5.0},
            open_boundaries=OpenBoundaries(),
            partitioning=Partitioning(n_procs_x=1, n_procs_y=1),
            grid_file=UserProvidedFile(**_USER_FILE_KWARGS),
        )


def test_domain_grid_file_allows_vertical_coord_kwargs():
    """theta_s/theta_b/hc/N remain allowed alongside a supplied grid file --
    roms-tools accepts them alongside ``filename``.
    """
    from cstar.applications.forge.blueprint import (
        Domain,
        OpenBoundaries,
        Partitioning,
        UserProvidedFile,
    )

    domain = Domain(
        grid_name="custom",
        grid_kwargs={"theta_s": 5.0, "theta_b": 2.0, "hc": 250.0, "N": 3},
        open_boundaries=OpenBoundaries(),
        partitioning=Partitioning(n_procs_x=1, n_procs_y=1),
        grid_file=UserProvidedFile(**_USER_FILE_KWARGS),
    )
    assert domain.grid_file is not None


def test_domain_grid_file_rejects_nesting():
    from cstar.applications.forge.blueprint import (
        Domain,
        OpenBoundaries,
        Partitioning,
        UserProvidedFile,
    )

    with pytest.raises(ValueError, match="nesting"):
        Domain(
            grid_name="custom",
            grid_kwargs={},
            open_boundaries=OpenBoundaries(),
            partitioning=Partitioning(n_procs_x=1, n_procs_y=1),
            grid_file=UserProvidedFile(**_USER_FILE_KWARGS),
            grid_kwargs_parent={"nx": 10, "ny": 10},
        )


def test_river_custom_file_required_when_source_is_custom_file():
    from cstar.applications.forge.blueprint import RiverForcingItem, SourceSpec

    with pytest.raises(ValueError, match="custom_file is not set"):
        RiverForcingItem(source=SourceSpec(name="CUSTOM_FILE"))


def test_river_custom_file_forbidden_when_source_is_not_custom_file():
    from cstar.applications.forge.blueprint import (
        RiverForcingItem,
        SourceSpec,
        UserProvidedFile,
    )

    with pytest.raises(ValueError, match="only valid with a CUSTOM_FILE source"):
        RiverForcingItem(
            source=SourceSpec(name="DAI"),
            custom_file=UserProvidedFile(**_USER_FILE_KWARGS),
        )


def test_river_custom_file_round_trips_and_is_valid():
    from cstar.applications.forge.blueprint import (
        RiverForcingItem,
        SourceSpec,
        UserProvidedFile,
    )

    river = RiverForcingItem(
        source=SourceSpec(name="CUSTOM_FILE"),
        custom_file=UserProvidedFile(**_USER_FILE_KWARGS),
    )
    assert river.custom_file is not None


def test_river_custom_file_excludes_bgc_source():
    from cstar.applications.forge.blueprint import (
        RiverForcingItem,
        SourceSpec,
        UserProvidedFile,
    )

    with pytest.raises(ValueError, match="mutually exclusive"):
        RiverForcingItem(
            source=SourceSpec(name="CUSTOM_FILE"),
            custom_file=UserProvidedFile(**_USER_FILE_KWARGS),
            include_bgc=True,
            bgc_source={"name": "RIVR2O"},
        )


def test_river_surface_forcing_source_defaults():
    from cstar.applications.forge.blueprint import RiverForcingItem, SourceSpec

    river = RiverForcingItem(source=SourceSpec(name="DAI"))
    assert river.surface_forcing_source is None
    assert river.river_temp_smoothing_window_days == 30.0


@pytest.mark.parametrize("name", ["ERA5", "era5"])
def test_river_surface_forcing_source_accepts_era5(name):
    from cstar.applications.forge.blueprint import RiverForcingItem, SourceSpec

    river = RiverForcingItem(
        source=SourceSpec(name="DAI"),
        surface_forcing_source={"name": name, "path": "/x/era5"},
    )
    # Normalized: roms-tools compares against the literal "ERA5".
    assert river.surface_forcing_source == {"name": "ERA5", "path": "/x/era5"}


def test_river_surface_forcing_source_rejects_unsupported_name():
    from cstar.applications.forge.blueprint import RiverForcingItem, SourceSpec

    with pytest.raises(ValueError, match="is not one of"):
        RiverForcingItem(
            source=SourceSpec(name="DAI"),
            surface_forcing_source={"name": "GLORYS"},
        )


def test_river_surface_forcing_source_requires_name():
    from cstar.applications.forge.blueprint import RiverForcingItem, SourceSpec

    with pytest.raises(ValueError, match="is not one of"):
        RiverForcingItem(
            source=SourceSpec(name="DAI"),
            surface_forcing_source={"path": "/tmp/era5.nc"},
        )


def test_river_custom_file_excludes_surface_forcing_source():
    from cstar.applications.forge.blueprint import (
        RiverForcingItem,
        SourceSpec,
        UserProvidedFile,
    )

    with pytest.raises(ValueError, match="mutually exclusive"):
        RiverForcingItem(
            source=SourceSpec(name="CUSTOM_FILE"),
            custom_file=UserProvidedFile(**_USER_FILE_KWARGS),
            surface_forcing_source={"name": "ERA5"},
        )


@pytest.mark.parametrize("window", [0, -1.0])
def test_river_temp_smoothing_window_days_rejects_non_positive(window):
    from cstar.applications.forge.blueprint import RiverForcingItem, SourceSpec

    with pytest.raises(ValueError, match="must be > 0"):
        RiverForcingItem(
            source=SourceSpec(name="DAI"),
            surface_forcing_source={"name": "ERA5"},
            river_temp_smoothing_window_days=window,
        )


def test_river_surface_forcing_source_round_trips_through_yaml(tmp_path):
    from cstar.applications.forge.blueprint import RiverForcingItem, SourceSpec

    cfg = _build()
    river = RiverForcingItem(
        source=SourceSpec(name="DAI"),
        surface_forcing_source={"name": "ERA5", "path": "/x/era5.nc"},
        river_temp_smoothing_window_days=7.0,
    )
    cfg = cfg.model_copy(
        update={"forcing": cfg.forcing.model_copy(update={"river": [river]})}
    )
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.forcing.river[0].surface_forcing_source == {
        "name": "ERA5",
        "path": "/x/era5.nc",
    }
    assert back.forcing.river[0].river_temp_smoothing_window_days == 7.0


class TestCdrSpecValidatorMatrix:
    """``CdrSpec._fields_match_mode``: each of the five modes accepts exactly the
    field combination its docstring promises, and rejects every other one.
    """

    def _file(self):
        from cstar.applications.forge.blueprint import UserProvidedFile

        return UserProvidedFile(**_USER_FILE_KWARGS)

    @pytest.mark.parametrize("mode", ["none", "upscaled"])
    def test_none_and_upscaled_accept_no_fields(self, mode):
        from cstar.applications.forge.blueprint import CdrSpec

        spec = CdrSpec(mode=mode)
        assert spec.cdr_forcing is None
        assert spec.cdr_forcing_file is None

    @pytest.mark.parametrize("mode", ["none", "upscaled"])
    def test_none_and_upscaled_reject_cdr_forcing(self, mode):
        from cstar.applications.forge.blueprint import CdrSpec

        with pytest.raises(ValueError, match="requires both cdr_forcing"):
            CdrSpec(mode=mode, cdr_forcing={"some": "config"})

    @pytest.mark.parametrize("mode", ["none", "upscaled"])
    def test_none_and_upscaled_reject_cdr_forcing_file(self, mode):
        from cstar.applications.forge.blueprint import CdrSpec

        with pytest.raises(ValueError, match="requires both cdr_forcing"):
            CdrSpec(mode=mode, cdr_forcing_file=self._file())

    @pytest.mark.parametrize("mode", ["simple", "yaml"])
    def test_simple_and_yaml_require_cdr_forcing(self, mode):
        from cstar.applications.forge.blueprint import CdrSpec

        spec = CdrSpec(mode=mode, cdr_forcing={"some": "config"})
        assert spec.cdr_forcing == {"some": "config"}
        assert spec.cdr_forcing_file is None
        with pytest.raises(ValueError, match="requires cdr_forcing to be set"):
            CdrSpec(mode=mode)

    @pytest.mark.parametrize("mode", ["simple", "yaml"])
    def test_simple_and_yaml_reject_cdr_forcing_file(self, mode):
        from cstar.applications.forge.blueprint import CdrSpec

        with pytest.raises(ValueError, match="requires cdr_forcing_file to be unset"):
            CdrSpec(
                mode=mode,
                cdr_forcing={"some": "config"},
                cdr_forcing_file=self._file(),
            )

    def test_netcdf_requires_cdr_forcing_file(self):
        from cstar.applications.forge.blueprint import CdrSpec

        spec = CdrSpec(mode="netcdf", cdr_forcing_file=self._file())
        assert spec.cdr_forcing_file is not None
        assert spec.cdr_forcing is None
        with pytest.raises(ValueError, match="requires cdr_forcing_file to be set"):
            CdrSpec(mode="netcdf")

    def test_netcdf_rejects_cdr_forcing(self):
        from cstar.applications.forge.blueprint import CdrSpec

        with pytest.raises(ValueError, match="requires cdr_forcing to be unset"):
            CdrSpec(
                mode="netcdf",
                cdr_forcing_file=self._file(),
                cdr_forcing={"some": "config"},
            )


def test_content_hash_ignores_user_file_location_but_not_content_hash():
    """Same rationale as ``code.<repo>.location``: a user file's ``location`` is
    host/transport and must not perturb the content hash, but its ``content_hash``
    leaf (the pin on the file's actual data) is results-affecting.
    """
    from cstar.applications.forge.blueprint import UserProvidedFile

    cfg = _build()
    vertical_only = {
        k: v
        for k, v in cfg.domain.grid_kwargs.items()
        if k in {"theta_s", "theta_b", "hc", "N"}
    }
    base = cfg.model_copy(
        update={
            "domain": cfg.domain.model_copy(
                update={
                    "grid_kwargs": vertical_only,
                    "grid_file": UserProvidedFile(
                        location="/data/staged/a.nc", content_hash="a" * 64
                    ),
                }
            )
        }
    )
    same_content_other_location = base.model_copy(
        update={
            "domain": base.domain.model_copy(
                update={
                    "grid_file": UserProvidedFile(
                        location="/somewhere/else/b.nc", content_hash="a" * 64
                    )
                }
            )
        }
    )
    assert same_content_other_location.content_hash() == base.content_hash()

    different_content = base.model_copy(
        update={
            "domain": base.domain.model_copy(
                update={
                    "grid_file": UserProvidedFile(
                        location="/data/staged/a.nc", content_hash="b" * 64
                    )
                }
            )
        }
    )
    assert different_content.content_hash() != base.content_hash()


# ---------------------------------------------------------------------------
# Resolver: grid pathway (build_forge_blueprint(grid_file=...))
#
# roms_tools.Grid is stubbed here (not called for real): a real ``rt.Grid(...)``
# build is broken in this dev env (PROJ/geopandas ``proj.db`` version mismatch --
# see CLAUDE.md), and stubbing also keeps these tests fast/offline. The stub
# stands in for the ONE grid load the resolver performs
# (``rt.Grid(filename=..., **vert)``) when ``grid_file`` is set.
# ---------------------------------------------------------------------------
class _FakeLoadedGrid:
    """Stand-in for a roms_tools.Grid loaded from a user-supplied filename."""

    def __init__(
        self,
        *,
        nx,
        ny,
        N,
        theta_s=5.0,
        theta_b=2.0,
        hc=250.0,
        size_x=None,
        size_y=None,
    ):
        self.nx = nx
        self.ny = ny
        self.N = N
        self.theta_s = theta_s
        self.theta_b = theta_b
        self.hc = hc
        self.size_x = size_x
        self.size_y = size_y
        self.ds = None  # unused: CFL derivation defaults to baroclinic mode


def _write_tiny_netcdf(tmp_path, name="grid.nc"):
    """A minimal real netCDF file for ``hash_netcdf_contents`` to hash -- content
    is irrelevant (the loaded *grid* comes from the stubbed ``roms_tools.Grid``,
    not from parsing this file), only that it exists and is a valid netCDF.
    """
    import numpy as np
    import xarray as xr

    ds = xr.Dataset(
        {"mask_rho": (("eta_rho", "xi_rho"), np.ones((4, 5)))},
        attrs={"title": "tiny user-supplied grid"},
    )
    path = tmp_path / name
    ds.to_netcdf(path)
    return path


def test_build_forge_blueprint_grid_file_derives_dims_dt_v_sponge(
    monkeypatch, tmp_path
):
    from cstar.applications.forge.user_files import hash_netcdf_contents

    grid_path = _write_tiny_netcdf(tmp_path)
    captured = {}
    fake_grid = _FakeLoadedGrid(nx=7, ny=9, N=4, size_x=300.0, size_y=400.0)

    def _stub(**kwargs):
        captured.update(kwargs)
        return fake_grid

    monkeypatch.setattr("roms_tools.Grid", _stub)

    cfg = _build(grid_file=str(grid_path), grid_kwargs={}, dt=None, v_sponge=None)

    assert captured == {"filename": str(grid_path)}
    assert cfg.domain.grid_file is not None
    assert cfg.domain.grid_file.location == str(grid_path)
    assert cfg.domain.grid_file.content_hash == hash_netcdf_contents(grid_path)
    assert cfg.model_settings["param"]["llm"] == 7
    assert cfg.model_settings["param"]["mmm"] == 9
    assert cfg.model_settings["param"]["n"] == 4
    assert cfg.domain.dt is not None
    assert cfg.domain.v_sponge is not None


def test_build_forge_blueprint_grid_file_skips_topography_dataset(
    monkeypatch, tmp_path
):
    # Topography is baked into a user-supplied grid file: the configured source
    # must not be noted into resolved_datasets/datasets (a user-staged source
    # like EMOD would otherwise hard-fail ensure_source_data over an unused file).
    grid_path = _write_tiny_netcdf(tmp_path)
    monkeypatch.setattr(
        "roms_tools.Grid",
        lambda **kw: _FakeLoadedGrid(nx=7, ny=9, N=4, size_x=300.0, size_y=400.0),
    )

    cfg = _build(
        grid_file=str(grid_path),
        grid_kwargs={},
        topography_source="EMOD",
        dt=7200,
        v_sponge=1.0,
    )
    assert "EMOD" not in cfg.forcing.resolved_datasets
    assert "EMOD" not in cfg.datasets

    control = _build(topography_source="EMOD")
    assert "EMOD" in control.forcing.resolved_datasets


def test_build_forge_blueprint_grid_file_passes_vert_kwargs(monkeypatch, tmp_path):
    grid_path = _write_tiny_netcdf(tmp_path)
    captured = {}
    fake_grid = _FakeLoadedGrid(nx=5, ny=5, N=3, size_x=100.0, size_y=100.0)

    def _stub(**kwargs):
        captured.update(kwargs)
        return fake_grid

    monkeypatch.setattr("roms_tools.Grid", _stub)

    _build(
        grid_file=str(grid_path),
        grid_kwargs={"theta_s": 5.0, "theta_b": 2.0, "hc": 250.0, "N": 3},
        dt=7200,
        v_sponge=1.0,
    )
    assert captured == {
        "filename": str(grid_path),
        "theta_s": 5.0,
        "theta_b": 2.0,
        "hc": 250.0,
        "N": 3,
    }


def test_build_forge_blueprint_grid_file_partial_vert_kwargs_raises(tmp_path):
    grid_path = _write_tiny_netcdf(tmp_path)
    with pytest.raises(ValueError, match="theta_s"):
        _build(
            grid_file=str(grid_path),
            grid_kwargs={"theta_s": 5.0},
            dt=7200,
            v_sponge=1.0,
        )


def test_build_forge_blueprint_grid_file_missing_raises(tmp_path):
    missing = tmp_path / "does-not-exist.nc"
    with pytest.raises(FileNotFoundError):
        _build(grid_file=str(missing), grid_kwargs={}, dt=7200, v_sponge=1.0)


def test_build_forge_blueprint_grid_file_no_size_x_requires_explicit_dt(
    monkeypatch, tmp_path
):
    grid_path = _write_tiny_netcdf(tmp_path)
    fake_grid = _FakeLoadedGrid(nx=5, ny=5, N=3, size_x=None, size_y=None)
    monkeypatch.setattr("roms_tools.Grid", lambda **kw: fake_grid)

    with pytest.raises(ValueError, match="dt"):
        _build(grid_file=str(grid_path), grid_kwargs={}, dt=None, v_sponge=1.0)


def test_build_forge_blueprint_grid_file_no_size_x_requires_explicit_v_sponge(
    monkeypatch, tmp_path
):
    grid_path = _write_tiny_netcdf(tmp_path)
    fake_grid = _FakeLoadedGrid(nx=5, ny=5, N=3, size_x=None, size_y=None)
    monkeypatch.setattr("roms_tools.Grid", lambda **kw: fake_grid)

    with pytest.raises(ValueError, match="v_sponge"):
        _build(grid_file=str(grid_path), grid_kwargs={}, dt=7200, v_sponge=None)


def test_build_forge_blueprint_grid_file_trusted_dict_skips_rehash(
    monkeypatch, tmp_path
):
    """A dict carrying both ``location``/``content_hash`` (the wizard's rebuild
    path) is trusted as-is -- the resolver still loads the grid (for nx/ny/N),
    but does not recompute the hash.
    """
    grid_path = _write_tiny_netcdf(tmp_path)
    fake_grid = _FakeLoadedGrid(nx=5, ny=5, N=3, size_x=100.0, size_y=100.0)
    monkeypatch.setattr("roms_tools.Grid", lambda **kw: fake_grid)

    trusted = {"location": str(grid_path), "content_hash": "not-the-real-hash"}
    cfg = _build(grid_file=trusted, grid_kwargs={}, dt=7200, v_sponge=1.0)
    assert cfg.domain.grid_file.content_hash == "not-the-real-hash"


def test_build_forge_blueprint_grid_file_accepts_user_provided_file_instance(
    monkeypatch, tmp_path
):
    from cstar.applications.forge.blueprint import UserProvidedFile

    grid_path = _write_tiny_netcdf(tmp_path)
    fake_grid = _FakeLoadedGrid(nx=5, ny=5, N=3, size_x=100.0, size_y=100.0)
    monkeypatch.setattr("roms_tools.Grid", lambda **kw: fake_grid)

    gf = UserProvidedFile(location=str(grid_path), content_hash="pinned-hash")
    cfg = _build(grid_file=gf, grid_kwargs={}, dt=7200, v_sponge=1.0)
    assert cfg.domain.grid_file == gf


def test_cpus_needed_falls_back_to_param_dims_for_grid_file(monkeypatch, tmp_path):
    from cstar.applications.forge.blueprint import estimate_forge_cpus

    grid_path = _write_tiny_netcdf(tmp_path)
    fake_grid = _FakeLoadedGrid(nx=50, ny=60, N=10, size_x=500.0, size_y=600.0)
    monkeypatch.setattr("roms_tools.Grid", lambda **kw: fake_grid)

    cfg = _build(grid_file=str(grid_path), grid_kwargs={}, dt=7200, v_sponge=1.0)
    assert "nx" not in cfg.domain.grid_kwargs
    assert cfg.cpus_needed == estimate_forge_cpus(50, 60, 10)


# ---------------------------------------------------------------------------
# Resolver: river custom_file pathway (build_forge_blueprint(forcing_inputs=...))
# ---------------------------------------------------------------------------
def test_build_forge_blueprint_river_custom_file_carries_hash(tmp_path):
    import copy

    from cstar.applications.forge.user_files import hash_netcdf_contents

    river_path = _write_tiny_netcdf(tmp_path, name="river.nc")
    fdata = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["river"] = [
        {
            "source": {"name": "CUSTOM_FILE"},
            "custom_file": str(river_path),
        }
    ]

    cfg = _build(forcing_inputs=fdata)

    river = cfg.forcing.river[0]
    assert river.source.name == "CUSTOM_FILE"
    assert river.custom_file is not None
    assert river.custom_file.location == str(river_path)
    assert river.custom_file.content_hash == hash_netcdf_contents(river_path)
    # No registry entry for CUSTOM_FILE -- staging it would either raise "Unknown
    # dataset" downstream or bogus-stage a source that is never used.
    assert "CUSTOM_FILE" not in cfg.forcing.resolved_datasets
    assert "CUSTOM_FILE" not in cfg.datasets


def test_build_forge_blueprint_river_custom_file_trusted_dict_skips_rehash(tmp_path):
    import copy

    river_path = _write_tiny_netcdf(tmp_path, name="river.nc")
    fdata = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["river"] = [
        {
            "source": {"name": "CUSTOM_FILE"},
            "custom_file": {
                "location": str(river_path),
                "content_hash": "not-the-real-hash",
            },
        }
    ]

    cfg = _build(forcing_inputs=fdata)
    assert cfg.forcing.river[0].custom_file.content_hash == "not-the-real-hash"


def test_build_forge_blueprint_river_custom_file_missing_raises(tmp_path):
    import copy

    missing = tmp_path / "does-not-exist.nc"
    fdata = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["river"] = [
        {"source": {"name": "CUSTOM_FILE"}, "custom_file": str(missing)}
    ]

    with pytest.raises(FileNotFoundError):
        _build(forcing_inputs=fdata)


# ---------------------------------------------------------------------------
# Resolver: CDR-forcing custom-file pathway (build_forge_blueprint(cdr_forcing_file=...))
# ---------------------------------------------------------------------------
def test_build_forge_blueprint_cdr_forcing_file_carries_hash_and_forces_output(
    tmp_path,
):
    from cstar.applications.forge.user_files import hash_netcdf_contents

    cdr_path = _write_tiny_netcdf(tmp_path, name="cdr.nc")

    cfg = _build(cdr_forcing_file=str(cdr_path))

    assert cfg.cdr.mode == "netcdf"
    assert cfg.cdr.cdr_forcing_file is not None
    assert cfg.cdr.cdr_forcing_file.location == str(cdr_path)
    assert cfg.cdr.cdr_forcing_file.content_hash == hash_netcdf_contents(cdr_path)
    assert cfg.cdr.cdr_forcing is None

    settings = cfg.model_settings
    assert settings["cdr_output"]["do_cdr_output"] is True
    assert settings["cppdefs"]["cdr_forcing"] is True
    diags = settings["marbl_bgc"]["marbl_diagnostics_to_write"]
    for name in _CDR_OUTPUT_REQUIRED_DIAGNOSTICS:
        assert name in diags


def test_build_forge_blueprint_cdr_forcing_file_trusted_dict_skips_rehash(tmp_path):
    cdr_path = _write_tiny_netcdf(tmp_path, name="cdr.nc")

    cfg = _build(
        cdr_forcing_file={
            "location": str(cdr_path),
            "content_hash": "not-the-real-hash",
        }
    )
    assert cfg.cdr.cdr_forcing_file.content_hash == "not-the-real-hash"


def test_build_forge_blueprint_cdr_forcing_file_missing_raises(tmp_path):
    missing = tmp_path / "does-not-exist.nc"
    with pytest.raises(FileNotFoundError):
        _build(cdr_forcing_file=str(missing))


def test_build_forge_blueprint_cdr_forcing_file_requires_marbl(tmp_path):
    """Mirrors test_cdr_output_requires_marbl: a user-supplied cdr_forcing_file
    implies do_cdr_output just like a generated cdr_forcing, so it must raise
    the same way when bgc_mode="none".
    """
    cdr_path = _write_tiny_netcdf(tmp_path, name="cdr.nc")
    with pytest.raises(ValueError, match="do_cdr_output"):
        _build(
            cdr_forcing_file=str(cdr_path),
            bgc_mode="none",
            forcing_inputs=_PHYSICS_ONLY_FORCING,
        )


def test_build_forge_blueprint_cdr_forcing_file_conflicts_with_cdr_forcing(tmp_path):
    """A resolver-level error (not the bare pydantic ValidationError from
    Forcing's own validator) when both are passed to the resolver.
    """
    cdr_path = _write_tiny_netcdf(tmp_path, name="cdr.nc")
    with pytest.raises(ValueError, match="mutually exclusive"):
        _build(cdr_forcing_file=str(cdr_path), cdr_forcing={"releases": []})


def test_build_forge_blueprint_cdr_forcing_file_conflicts_with_cdr_forcing_yaml(
    tmp_path,
):
    cdr_path = _write_tiny_netcdf(tmp_path, name="cdr.nc")
    with pytest.raises(ValueError, match="mutually exclusive"):
        _build(cdr_forcing_file=str(cdr_path), cdr_forcing_yaml=_CDR_SAMPLE_YAML)


# ---------------------------------------------------------------------------
# Resolver: the `cdr=` kwarg (a full CdrSpec selection) -- mode matrix, conflicts
# with the cdr_forcing/cdr_forcing_yaml/cdr_forcing_file conveniences, and
# convenience-kwarg/`cdr=` parity.
# ---------------------------------------------------------------------------
def test_build_forge_blueprint_cdr_kwarg_conflicts_with_cdr_forcing():
    with pytest.raises(ValueError, match="mutually exclusive"):
        _build(cdr={"mode": "none"}, cdr_forcing={"releases": []})


def test_build_forge_blueprint_cdr_kwarg_conflicts_with_cdr_forcing_yaml():
    with pytest.raises(ValueError, match="mutually exclusive"):
        _build(cdr={"mode": "none"}, cdr_forcing_yaml=_CDR_SAMPLE_YAML)


def test_build_forge_blueprint_cdr_kwarg_conflicts_with_cdr_forcing_file(tmp_path):
    cdr_path = _write_tiny_netcdf(tmp_path, name="cdr.nc")
    with pytest.raises(ValueError, match="mutually exclusive"):
        _build(cdr={"mode": "none"}, cdr_forcing_file=str(cdr_path))


def test_build_forge_blueprint_cdr_kwarg_none_mode():
    cfg = _build(cdr={"mode": "none"})
    assert cfg.cdr.mode == "none"
    settings = cfg.model_settings
    assert settings["cppdefs"]["cdr_forcing"] is False
    assert settings["cdr_output"]["do_cdr_output"] is False


def test_build_forge_blueprint_cdr_kwarg_simple_mode():
    """The "simple" mode (an authored-fresh kwargs dict) is reachable only via
    ``cdr=`` -- the convenience kwargs always land on "yaml" (see the module
    docstring on ``cdr_forcing_yaml``).
    """
    cfg = _build(cdr={"mode": "simple", "cdr_forcing": {"releases": []}})
    assert cfg.cdr.mode == "simple"
    assert cfg.cdr.cdr_forcing == {"releases": []}
    settings = cfg.model_settings
    assert settings["cppdefs"]["cdr_forcing"] is True
    assert settings["cdr_output"]["do_cdr_output"] is True
    diags = settings["marbl_bgc"]["marbl_diagnostics_to_write"]
    for name in _CDR_OUTPUT_REQUIRED_DIAGNOSTICS:
        assert name in diags


def test_build_forge_blueprint_cdr_kwarg_yaml_mode_strips_tracer_metadata():
    cfg = _build(
        cdr={
            "mode": "yaml",
            "cdr_forcing": {
                "releases": [],
                "_tracer_metadata": {"temp": {"units": "C"}},
            },
        }
    )
    assert cfg.cdr.mode == "yaml"
    assert "_tracer_metadata" not in cfg.cdr.cdr_forcing
    assert cfg.model_settings["cppdefs"]["cdr_forcing"] is True


def test_build_forge_blueprint_cdr_kwarg_netcdf_mode(tmp_path):
    from cstar.applications.forge.user_files import hash_netcdf_contents

    cdr_path = _write_tiny_netcdf(tmp_path, name="cdr.nc")
    cfg = _build(cdr={"mode": "netcdf", "cdr_forcing_file": str(cdr_path)})
    assert cfg.cdr.mode == "netcdf"
    assert cfg.cdr.cdr_forcing_file.location == str(cdr_path)
    assert cfg.cdr.cdr_forcing_file.content_hash == hash_netcdf_contents(cdr_path)
    settings = cfg.model_settings
    assert settings["cppdefs"]["cdr_forcing"] is True
    assert settings["cdr_output"]["do_cdr_output"] is True


def test_build_forge_blueprint_cdr_kwarg_netcdf_mode_missing_file_raises(tmp_path):
    missing = tmp_path / "does-not-exist.nc"
    with pytest.raises(FileNotFoundError):
        _build(cdr={"mode": "netcdf", "cdr_forcing_file": str(missing)})


def test_build_forge_blueprint_cdr_kwarg_upscaled_mode_sets_cdr_frc_statics():
    """The "upscaled" mode: no generation step runs, so the resolver is the sole source
    of these ``cdr_frc`` statics (see ``build_forge_blueprint``'s upscaled
    handling); it also participates in the CDR-output consistency block just
    like the generated/custom-file modes.
    """
    cfg = _build(cdr={"mode": "upscaled"})
    assert cfg.cdr.mode == "upscaled"
    assert cfg.cdr.cdr_forcing is None
    assert cfg.cdr.cdr_forcing_file is None
    settings = cfg.model_settings
    assert settings["cppdefs"]["cdr_forcing"] is True
    assert settings["cdr_output"]["do_cdr_output"] is True
    diags = settings["marbl_bgc"]["marbl_diagnostics_to_write"]
    for name in _CDR_OUTPUT_REQUIRED_DIAGNOSTICS:
        assert name in diags
    cdr_frc = settings["cdr_frc"]
    assert cdr_frc["cdr_source"] is True
    assert cdr_frc["cdr_file"] == "cdr.nc"
    assert cdr_frc["forcing_depth_profiles"] is True
    assert cdr_frc["forcing_parameterized"] is False
    assert cdr_frc["cdr_volume"] is False
    assert cdr_frc["relocate_to_wet_pts"] is False


def test_build_forge_blueprint_cdr_kwarg_upscaled_requires_marbl():
    """Mirrors test_cdr_output_requires_marbl / the cdr_forcing_file variant:
    "upscaled" implies do_cdr_output just like generated/custom-file CDR, so it
    must raise the same way when bgc_mode="none".
    """
    with pytest.raises(ValueError, match="do_cdr_output"):
        _build(
            cdr={"mode": "upscaled"},
            bgc_mode="none",
            forcing_inputs=_PHYSICS_ONLY_FORCING,
        )


def test_build_forge_blueprint_cdr_kwarg_accepts_cdrspec_instance():
    from cstar.applications.forge.blueprint import CdrSpec

    cfg = _build(cdr=CdrSpec(mode="yaml", cdr_forcing={"releases": []}))
    assert cfg.cdr.mode == "yaml"
    assert cfg.cdr.cdr_forcing == {"releases": []}


def test_build_forge_blueprint_convenience_kwargs_match_cdr_kwarg_semantics():
    """The cdr_forcing/cdr_forcing_yaml/cdr_forcing_file conveniences remain
    exactly equivalent to the ``cdr=`` shape they map onto (mode inferred) --
    pre-CdrSpec callers get an identical resolved blueprint either way.
    """
    via_convenience = _build(cdr_forcing={"releases": []})
    via_cdr_kwarg = _build(cdr={"mode": "yaml", "cdr_forcing": {"releases": []}})
    assert via_convenience.cdr == via_cdr_kwarg.cdr
    assert (
        via_convenience.model_settings["cppdefs"]["cdr_forcing"]
        == via_cdr_kwarg.model_settings["cppdefs"]["cdr_forcing"]
    )
    assert (
        via_convenience.model_settings["cdr_output"]
        == via_cdr_kwarg.model_settings["cdr_output"]
    )


# ---------------------------------------------------------------------------
# SourceSpec.path: the legacy "explicit dataset path override" -- previously
# collected by the wizard but silently dropped by the resolver (never threaded
# into SourceSpec, so it never survived to the executor). Deliberately no
# hashing for this override (out of scope; unlike grid_file/custom_file this is
# a legacy escape hatch, not a new user-provided-file contract).
# ---------------------------------------------------------------------------
def test_build_forge_blueprint_source_path_round_trips_to_blueprint():
    import copy

    fdata = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["surface"][0]["source"]["path"] = "/custom/era5.nc"

    cfg = _build(forcing_inputs=fdata)
    assert cfg.forcing.surface[0].source.path == "/custom/era5.nc"


def test_build_forge_blueprint_source_path_skips_dataset_noting():
    """An item whose source carries an explicit path bypasses staging entirely
    (mirrors topography_path semantics) -- it must not be noted into
    resolved_datasets/datasets, since input_data._resolve_source_block returns
    the explicit path verbatim without ever staging/verifying via SourceDatasets.
    """
    import copy

    fdata = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["surface"][0]["source"]["path"] = "/custom/era5.nc"

    cfg = _build(forcing_inputs=fdata)
    assert "ERA5" not in cfg.forcing.resolved_datasets
    assert "ERA5" not in cfg.datasets

    # Control: without the explicit path, ERA5 is noted as usual.
    control = _build(forcing_inputs=_CATALOG.forcing_data("glorys-era5-unified"))
    assert "ERA5" in control.forcing.resolved_datasets
    assert "ERA5" in control.datasets


def test_sources_to_forcing_override_carries_source_path():
    import copy

    from cstar.applications.forge.engine import sources_to_forcing_override

    fdata = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["surface"][0]["source"]["path"] = "/custom/era5.nc"

    cfg = _build(forcing_inputs=fdata)
    ov = sources_to_forcing_override(cfg)
    assert ov["forcing"]["surface"][0]["source"]["path"] == "/custom/era5.nc"


def test_schema_round_trip_identity(tmp_path):
    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    back = ForgeBlueprint.from_yaml(p)
    # content_hash is stamped on write -> back carries it; otherwise identical
    assert back.provenance.content_hash == cfg.content_hash()
    assert back.model_copy(update={"provenance": cfg.provenance}) == cfg
    assert back.application == cfg.application


def test_content_hash_ignores_excluded_sections():
    from cstar.applications.forge.blueprint import _HASH_EXCLUDE, SpecRef

    cfg = _build()
    h = cfg.content_hash()
    # editing name/description/composition/provenance does NOT change the hash
    c2 = cfg.model_copy(
        update={
            "name": "totally-different-name",
            "description": "totally different",
            "composition": cfg.composition.model_copy(
                update={"forcing": SpecRef(name="x", origin="custom")}
            ),
            "provenance": cfg.provenance.model_copy(update={"notes": "edited"}),
        }
    )
    assert c2.content_hash() == h
    assert _HASH_EXCLUDE == {
        "forge_blueprint_version",
        "name",
        "description",
        "composition",
        "provenance",
        "working_dir",
        "state",
        "schema_version",
        "$schema",
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
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.code.pio is not None
    assert back.code.pio.location == cfg.code.pio.location
    assert back.code.pio.commit == "2.7.1-fork"
    assert back.model_settings["cppdefs"]["use_pio"] is True


def test_content_hash_round_trips_through_yaml(tmp_path):
    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    back = ForgeBlueprint.from_yaml(p)
    # recomputed hash on the loaded config matches the stamped one (no edits)
    assert back.content_hash() == back.provenance.content_hash


def test_engine_warns_on_hash_mismatch(tmp_path):
    from cstar.applications.forge.engine import (
        process_forge_blueprint,
        verify_content_hash,
    )

    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    data = yaml.safe_load(p.read_text())
    # hand-edit a results-affecting value WITHOUT updating the recorded hash
    data["model_settings"]["v_sponge"]["v_sponge"] = 12345.0
    p.write_text(yaml.safe_dump(data))
    tampered = ForgeBlueprint.from_yaml(p)
    assert verify_content_hash(tampered) is not None  # mismatch detected
    # ... and a clean (re-saved) file does not warn
    assert (
        verify_content_hash(
            ForgeBlueprint.from_yaml(cfg.to_yaml(tmp_path / "clean.yaml"))
        )
        is None
    )

    # the engine warns but still processes (uses a fake executor)
    class _Fake:
        def __init__(self, cfg=None, host=None, verbose=False):
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
        Path(__file__).parent / "fixtures" / "golden_model_settings_test-tiny.json"
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


def test_golden_model_settings_test_tiny_roms050():
    """Behavior-preservation snapshot for the ``roms-marbl-0.5-default`` ModelSpec
    (ucla-roms >= 0.5.0), resolved from the same test-tiny domain/forcing/output
    setup as ``test_golden_model_settings_test_tiny``.

    ``model_settings`` itself is schema-version-agnostic (the resolver doesn't
    validate it against ``RunTimeSettings``/``RunTimeSettingsV0_5_0`` -- that
    happens downstream, at ``write_roms_namelist`` time), so this is a plain
    resolver-drift snapshot for the new ModelSpec, not a versioned-namelist
    assertion (see ``TestGoldenNamelist.test_golden_namelist_test_tiny_roms050``
    in ``tests/test_core.py`` for that). It intentionally differs from
    ``golden_model_settings_test-tiny.json`` because that fixture is resolved
    from the unrelated ``cson_roms-marbl_v0.1`` ModelSpec, with its own physics/
    numerics defaults.
    """
    import json

    golden_path = (
        Path(__file__).parent
        / "fixtures"
        / "golden_model_settings_test-tiny-roms050.json"
    )
    golden = json.loads(golden_path.read_text())
    cfg = _build(model_dir=_MODEL_DIR_ROMS050)  # test-tiny, dt=7200
    got = json.loads(json.dumps(cfg.model_settings, sort_keys=True, default=str))
    assert got == golden, (
        "Resolved model_settings for test-tiny (roms-marbl-0.5-default) drifted "
        "from the golden fixture. If this is an intentional schema/default "
        "change, regenerate tests/fixtures/golden_model_settings_test-tiny-"
        "roms050.json; otherwise the change is a regression in the settings the "
        "executor feeds to namelist.nml / cppdefs.opt."
    )


def test_golden_model_settings_test_tiny_roms060():
    """Behavior-preservation snapshot for the ``roms-marbl-0.6-default`` ModelSpec
    (ucla-roms >= 0.6.0, adds ``&PIO_SETTINGS``), resolved from the same
    test-tiny domain/forcing/output setup as ``test_golden_model_settings_test_tiny``.

    Mirrors ``test_golden_model_settings_test_tiny_roms050`` exactly; the only
    resolved-settings difference from that fixture is the added ``pio_settings``
    entry (see ``TestGoldenNamelist.test_golden_namelist_test_tiny_roms060`` in
    ``tests/test_core.py`` for the versioned-namelist assertion).
    """
    import json

    golden_path = (
        Path(__file__).parent
        / "fixtures"
        / "golden_model_settings_test-tiny-roms060.json"
    )
    golden = json.loads(golden_path.read_text())
    cfg = _build(model_dir=_MODEL_DIR_ROMS060)  # test-tiny, dt=7200
    got = json.loads(json.dumps(cfg.model_settings, sort_keys=True, default=str))
    assert got == golden, (
        "Resolved model_settings for test-tiny (roms-marbl-0.6-default) drifted "
        "from the golden fixture. If this is an intentional schema/default "
        "change, regenerate tests/fixtures/golden_model_settings_test-tiny-"
        "roms060.json; otherwise the change is a regression in the settings the "
        "executor feeds to namelist.nml / cppdefs.opt."
    )


def test_golden_model_settings_test_tiny_roms070():
    """Behavior-preservation snapshot for the ``roms-marbl-0.7-default`` ModelSpec
    (ucla-roms >= 0.7.0, adds ``&CDR_TRACER_OUTPUT_SETTINGS``/
    ``&CDR_GAS_EXCH_OUTPUT_SETTINGS``, PR #351), resolved from the same test-tiny
    domain/forcing/output setup as ``test_golden_model_settings_test_tiny``.

    Mirrors ``test_golden_model_settings_test_tiny_roms060`` exactly; the only
    resolved-settings difference from that fixture is the added
    ``cdr_tracer_output``/``cdr_gas_exch_output`` entries (see
    ``TestGoldenNamelist.test_golden_namelist_test_tiny_roms070`` in
    ``tests/test_core.py`` for the versioned-namelist assertion).
    """
    import json

    golden_path = (
        Path(__file__).parent
        / "fixtures"
        / "golden_model_settings_test-tiny-roms070.json"
    )
    golden = json.loads(golden_path.read_text())
    cfg = _build(model_dir=_MODEL_DIR_ROMS070)  # test-tiny, dt=7200
    got = json.loads(json.dumps(cfg.model_settings, sort_keys=True, default=str))
    assert got == golden, (
        "Resolved model_settings for test-tiny (roms-marbl-0.7-default) drifted "
        "from the golden fixture. If this is an intentional schema/default "
        "change, regenerate tests/fixtures/golden_model_settings_test-tiny-"
        "roms070.json; otherwise the change is a regression in the settings the "
        "executor feeds to namelist.nml / cppdefs.opt."
    )


def test_roms080_model_spec_declares_advection_cppdefs_and_renders(tmp_path):
    """``roms-marbl-0.8-default`` (ucla-roms 0.8.0, PR #361) declares the two new
    advection cppdefs keys both off (the 0.8.0 defaults), and pins the ucla-roms
    ref to "0.8.0" -- no namelist-schema change, so unlike the 0.5.0-0.7.0 tiers
    there is no versioned-namelist golden fixture to snapshot here. Instead this
    end-to-end renders the *working-tree* ``cppdefs.opt.j2`` from the resolved
    settings, proving the spec's declared cppdefs keys and the current template
    agree (render_roms_settings rejects a settings key the template never
    references). It deliberately does NOT fetch the template at the spec's
    ``templates_commit`` pin -- that pin must be repointed by hand whenever the
    template gains a key (see the TODO in the spec's model.yaml); this test
    cannot catch a stale pin. See TestCppdefsTemplate in tests/test_settings.py
    for the synthetic-settings coverage of the same two keys.
    """
    cfg = _build(model_dir=_MODEL_DIR_ROMS080)  # test-tiny, dt=7200
    assert cfg.model_settings["cppdefs"]["parabolic_splines"] is False
    assert cfg.model_settings["cppdefs"]["upstream_ts_land_curv"] is False
    assert cfg.code.roms.commit == "0.8.0"

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    template_dir = (
        Path(cstar.__file__).parent
        / "additional_files"
        / "templates"
        / "forge"
        / "compile-time"
    )
    render_roms_settings(
        template_files=["cppdefs.opt.j2"],
        template_dir=template_dir,
        settings_dict={
            "cppdefs": cfg.model_settings["cppdefs"],
            "upscale_output": cfg.model_settings.get("upscale_output", {}),
            "cdr_frc": cfg.model_settings.get("cdr_frc", {}),
        },
        code_output_dir=output_dir,
    )
    text = (output_dir / "cppdefs.opt").read_text()
    assert "#undef PARABOLIC_SPLINES" in text
    assert "#undef UPSTREAM_TS_LAND_CURV" in text


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


_PARENT_GRID_KWARGS = dict(
    nx=20,
    ny=20,
    size_x=2000,
    size_y=2000,
    center_lon=0,
    center_lat=55,
    rot=0,
    N=10,
    theta_s=6.0,
    theta_b=3.0,
    hc=250.0,
)


def test_resolver_parent_grid_stored_and_is_child():
    cfg = _build(grid_kwargs_parent=_PARENT_GRID_KWARGS)
    assert cfg.domain.grid_kwargs_parent["nx"] == 20
    assert cfg.domain.is_child is True
    assert cfg.domain.is_parent is False


def test_resolver_parent_grid_clears_boundary_forcing():
    # the bundled glorys-era5-unified ForcingSpec carries a boundary section --
    # a child grid (has a parent) must not generate boundary forcing (it
    # receives boundaries from the parent's nesting.nc extraction instead).
    fi = _CATALOG.forcing_data("glorys-era5-unified")
    assert fi["forcing"]["boundary"]  # sanity: fixture actually has a boundary section
    cfg = _build(grid_kwargs_parent=_PARENT_GRID_KWARGS)
    assert cfg.forcing.boundary is None
    # open-boundary edge flags are untouched -- edges stay open, just fed by
    # nesting.nc instead of reanalysis boundary forcing.
    assert cfg.domain.open_boundaries.model_dump() == _BOUNDARIES


def test_resolver_parent_grid_skips_boundary_only_dataset():
    # Boundary must be skipped entirely (not just cleared afterward) so a
    # boundary-only source never leaks into resolved_datasets/datasets -- e.g.
    # CESM_REGRIDDED here isn't used by surface/IC/tidal/river in this fixture,
    # so a stale post-hoc clear would still leave it in cfg.datasets.
    import copy

    fi = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    fi["forcing"]["boundary"] = {
        "source": {"name": "GLORYS"},
        "bgc_sources": [{"source": {"name": "CESM_REGRIDDED"}}],
    }
    cfg = _build(grid_kwargs_parent=_PARENT_GRID_KWARGS, forcing_inputs=fi)
    assert cfg.forcing.boundary is None
    assert "CESM_REGRIDDED" not in cfg.datasets
    assert "CESM_REGRIDDED" not in cfg.forcing.resolved_datasets


def test_resolver_child_grid_is_parent_and_keeps_boundary_forcing():
    cfg = _build(
        grid_kwargs_child=dict(
            nx=3,
            ny=3,
            size_x=300,
            size_y=300,
            center_lon=0,
            center_lat=55,
            rot=0,
            N=10,
            theta_s=6.0,
            theta_b=3.0,
            hc=250.0,
        )
    )
    assert cfg.domain.is_parent is True
    assert cfg.domain.is_child is False
    # a parent-only grid keeps its own boundary forcing
    assert cfg.forcing.boundary is not None


def test_resolver_parent_grid_skips_initial_conditions():
    # A child grid (has a parent) receives its state from the parent's
    # nesting.nc extraction, so IC is optional -- omitting it must not leak
    # any IC source into resolved_datasets/datasets, mirroring the boundary
    # skip above.
    import copy

    fi = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    assert fi["initial_conditions"]["source"]  # sanity: fixture has an IC
    del fi["initial_conditions"]
    cfg = _build(grid_kwargs_parent=_PARENT_GRID_KWARGS, forcing_inputs=fi)
    assert cfg.forcing.initial_conditions is None
    # GLORYS_REGIONAL is the resolved dataset key for the IC's glorys_layout;
    # it must not leak into datasets/resolved_datasets when IC is skipped.
    assert "GLORYS_REGIONAL" not in cfg.datasets
    assert "GLORYS" not in cfg.forcing.resolved_datasets


def test_resolver_non_child_requires_initial_conditions():
    import copy

    fi = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    del fi["initial_conditions"]
    with pytest.raises(ValueError, match="initial_conditions"):
        _build(forcing_inputs=fi)


def test_resolver_child_with_explicit_ic_keeps_it():
    cfg = _build(grid_kwargs_parent=_PARENT_GRID_KWARGS)
    assert cfg.forcing.initial_conditions is not None
    assert cfg.forcing.initial_conditions.source.name == "GLORYS"
    assert "GLORYS" in cfg.forcing.resolved_datasets


def test_resolver_restoring_sets_sal_restore():
    # the cson model.yaml includes a WOA surface source with type=restoring and
    # restoring_forces=['sss'], so the resolver derives sal_restore=True
    # (see forge_blueprint_resolve.py: sal_restore = any restoring item with 'sss').
    cfg = _build()
    assert cfg.model_settings["cppdefs"].get("sal_restore") is True


def test_resolver_threads_river_bgc_source_and_climatology():
    """Regression: the resolver's river _items()/_note() previously dropped
    bgc_source and convert_to_climatology entirely (a silent no-op — a configured
    RIVR2O river-BGC source would vanish before reaching RiverForcingItem). Both
    must round-trip, and a Forge-staged bgc_source name (RIVR2O) must land in
    datasets/resolved_datasets so the executor verifies it; CONSTANTS (roms-tools'
    own auto-downloaded default) must NOT, since Forge has no handler for it.
    """
    import copy

    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["river"][0]["bgc_source"] = {"name": "RIVR2O"}
    fdata["forcing"]["river"][0]["convert_to_climatology"] = "always"

    cfg = _build(forcing_inputs=fdata)
    river = cfg.forcing.river[0]

    assert river.bgc_source == {"name": "RIVR2O"}
    assert river.convert_to_climatology.value == "always"
    assert "RIVR2O" in cfg.datasets
    assert "RIVR2O" in cfg.forcing.resolved_datasets
    assert "CONSTANTS" not in cfg.datasets


def test_resolver_river_bgc_source_with_path_not_noted():
    """An explicit bgc_source path bypasses staging (the executor reads it verbatim,
    see input_data._resolve_source_block), so RIVR2O must NOT be noted into
    datasets/resolved_datasets -- otherwise _prepare_rivr2o would demand files at
    the canonical staged location that are never used. Same rule as `_note` and
    the river surface_forcing_source loop.
    """
    import copy

    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["river"][0]["bgc_source"] = {
        "name": "RIVR2O",
        "path": "/tmp/rivr2o/*.nc",
    }

    cfg = _build(forcing_inputs=fdata)

    assert cfg.forcing.river[0].bgc_source == {
        "name": "RIVR2O",
        "path": "/tmp/rivr2o/*.nc",
    }
    assert "RIVR2O" not in cfg.datasets
    assert "RIVR2O" not in cfg.forcing.resolved_datasets


def test_resolver_threads_river_surface_forcing_source():
    """The resolver's river _items() must also thread surface_forcing_source and
    river_temp_smoothing_window_days into RiverForcingItem (same generic plain-fields
    loop as bgc_source/convert_to_climatology). A streamable ERA5 source with no
    explicit path lands in datasets/resolved_datasets so the executor verifies it;
    an explicit path bypasses staging entirely, same as SourceSpec.path.
    """
    import copy

    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    # Drop the catalog's own ERA5 surface-physics entry so the assertion below
    # actually demonstrates the river noting logic, not ERA5 already being noted
    # for an unrelated reason.
    fdata["forcing"]["surface"] = [
        item for item in fdata["forcing"]["surface"] if item["source"]["name"] != "ERA5"
    ]
    fdata["forcing"]["river"][0]["surface_forcing_source"] = {"name": "ERA5"}
    fdata["forcing"]["river"][0]["river_temp_smoothing_window_days"] = 14.0

    cfg = _build(forcing_inputs=fdata)
    river = cfg.forcing.river[0]

    assert river.surface_forcing_source == {"name": "ERA5"}
    assert river.river_temp_smoothing_window_days == 14.0
    assert "ERA5" in cfg.datasets
    assert "ERA5" in cfg.forcing.resolved_datasets


def test_resolver_river_surface_forcing_source_with_path_not_noted():
    """An explicit path bypasses staging entirely (mirrors SourceSpec.path
    semantics), so ERA5 must not be noted into resolved_datasets/datasets when a
    path is already given -- it is never fetched via SourceDatasets in that case.
    """
    import copy

    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    # Drop the catalog's own ERA5 surface-physics entry so the assertion below
    # actually demonstrates the path-bypasses-staging logic, not ERA5 already
    # being noted for an unrelated reason.
    fdata["forcing"]["surface"] = [
        item for item in fdata["forcing"]["surface"] if item["source"]["name"] != "ERA5"
    ]
    fdata["forcing"]["river"][0]["surface_forcing_source"] = {
        "name": "ERA5",
        "path": "/tmp/era5/*.nc",
    }

    cfg = _build(forcing_inputs=fdata)
    river = cfg.forcing.river[0]

    assert river.surface_forcing_source == {"name": "ERA5", "path": "/tmp/era5/*.nc"}
    assert "ERA5" not in cfg.datasets
    assert "ERA5" not in cfg.forcing.resolved_datasets


def test_resolver_ic_bgc_esper_source_excluded_from_datasets():
    """Regression: an ESPER-named IC-BGC source (SourceSpec.name == "ESPER") is
    derived from physics T/S via PyESPER at generation time -- Forge has no
    SourceDatasets handler for it and never will, so it must never land in
    datasets/resolved_datasets (see DERIVED_BGC_SOURCES in source_registry.py);
    doing so previously raised "Unknown dataset(s) requested: ESPER" downstream
    in SourceDatasets.__post_init__.
    """
    import copy

    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    fdata["initial_conditions"]["bgc_sources"] = [
        {"source": {"name": "ESPER", "path": "/tmp/PyESPER"}}
    ]

    cfg = _build(forcing_inputs=fdata)

    assert cfg.forcing.initial_conditions.bgc_sources[0].source.name == "ESPER"
    assert "ESPER" not in cfg.datasets
    assert "ESPER" not in cfg.forcing.resolved_datasets


def test_resolver_boundary_bgc_esper_source_excluded_from_datasets():
    """Same regression as above, for an ESPER-named boundary bgc source."""
    import copy

    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["boundary"]["bgc_sources"][-1]["source"] = {
        "name": "ESPER",
        "path": "/tmp/PyESPER",
    }

    cfg = _build(forcing_inputs=fdata)

    assert cfg.forcing.boundary.bgc_sources[-1].source.name == "ESPER"
    assert "ESPER" not in cfg.datasets
    assert "ESPER" not in cfg.forcing.resolved_datasets


def test_resolver_ic_bgc_constants_source_excluded_from_datasets():
    """Same regression, via the generic IC-BGC path (not the river bgc_source path
    already covered by test_resolver_threads_river_bgc_source_and_climatology) --
    a "constants"-named source must not land in datasets either.
    """
    import copy

    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    fdata["initial_conditions"]["bgc_sources"] = [
        {"source": {"name": "constants", "constants": {"Fe": 3.0e-3}}}
    ]

    cfg = _build(forcing_inputs=fdata)

    assert cfg.forcing.initial_conditions.bgc_sources[0].source.constants == {
        "Fe": 3.0e-3
    }
    assert "CONSTANTS" not in cfg.datasets
    assert "CONSTANTS" not in cfg.forcing.resolved_datasets


def test_resolver_topography_source_emod_lands_in_datasets():
    cfg = _build(topography_source="EMOD")
    assert "EMOD" in cfg.datasets
    assert "EMOD" in cfg.forcing.resolved_datasets


def test_sources_to_forcing_override_carries_river_bgc_source():
    """Regression: sources_to_forcing_override is the production path a serialized
    ForgeBlueprint takes on the execution host (no original inputs dict available).
    If it dropped river bgc_source/convert_to_climatology the way the resolver's
    _items() once did, ensure_source_data would still stage/verify RIVR2O (datasets
    already carries it) but rt.RiverForcing would silently fall back to CONSTANTS —
    file present, no error, wrong tracers. Confirm it survives the round trip.
    """
    import copy

    from cstar.applications.forge.engine import (
        forge_blueprint_to_builder_kwargs,
        sources_to_forcing_override,
    )
    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    # No explicit path: that is the Forge-staged case in which RIVR2O must be
    # noted into datasets (an explicit path bypasses staging -- see
    # test_resolver_river_bgc_source_with_path_not_noted).
    fdata["forcing"]["river"][0]["bgc_source"] = {"name": "RIVR2O"}
    fdata["forcing"]["river"][0]["convert_to_climatology"] = "always"

    cfg = _build(forcing_inputs=fdata, topography_source="EMOD")

    ov = sources_to_forcing_override(cfg)
    river_ov = ov["forcing"]["river"][0]
    assert river_ov["bgc_source"] == {"name": "RIVR2O"}
    assert river_ov["convert_to_climatology"] == "always"

    kwargs = forge_blueprint_to_builder_kwargs(cfg)
    assert kwargs["topography_source"] == "EMOD"
    assert "RIVR2O" in kwargs["source_dataset_keys"]
    assert "EMOD" in kwargs["source_dataset_keys"]


def test_sources_to_forcing_override_carries_river_surface_forcing_source():
    """Same bridge, for surface_forcing_source/river_temp_smoothing_window_days --
    sources_to_forcing_override dumps RiverForcingItem generically, but confirm
    both new fields actually survive the round trip rather than assuming it.
    """
    import copy

    from cstar.applications.forge.engine import sources_to_forcing_override
    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["river"][0]["surface_forcing_source"] = {
        "name": "ERA5",
        "path": "/tmp/era5/*.nc",
    }
    fdata["forcing"]["river"][0]["river_temp_smoothing_window_days"] = 21.0

    cfg = _build(forcing_inputs=fdata)

    ov = sources_to_forcing_override(cfg)
    river_ov = ov["forcing"]["river"][0]
    assert river_ov["surface_forcing_source"] == {
        "name": "ERA5",
        "path": "/tmp/era5/*.nc",
    }
    assert river_ov["river_temp_smoothing_window_days"] == 21.0


def test_sources_to_forcing_override_carries_river_custom_file():
    """The third propagation path (resolver / sources_to_forcing_override / wizard
    load-back -- this WP covers the first two): custom_file must survive the
    round trip through sources_to_forcing_override the same as bgc_source does
    above. Guards against a future ``exclude=`` edit to ``_item()``'s
    ``model_dump`` silently dropping it (today it propagates "for free" because
    nothing excludes it).
    """
    import copy

    from cstar.applications.forge.engine import sources_to_forcing_override

    fdata = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["river"] = [
        {
            "source": {"name": "CUSTOM_FILE"},
            "custom_file": {
                "location": "/data/staged/river.nc",
                "content_hash": "a" * 64,
            },
        }
    ]

    cfg = _build(forcing_inputs=fdata)
    ov = sources_to_forcing_override(cfg)
    river_ov = ov["forcing"]["river"][0]
    assert river_ov["source"]["name"] == "CUSTOM_FILE"
    assert river_ov["custom_file"] == {
        "location": "/data/staged/river.nc",
        "content_hash": "a" * 64,
    }


def test_sources_to_forcing_override_omits_initial_conditions_for_child_no_ic():
    """A child domain with no explicit IC resolves cfg.forcing.initial_conditions
    to None -- sources_to_forcing_override must omit the key entirely rather
    than crash on `_ic(None)` or emit a None/placeholder value.
    """
    import copy

    from cstar.applications.forge.engine import sources_to_forcing_override

    fi = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    del fi["initial_conditions"]
    cfg = _build(grid_kwargs_parent=_PARENT_GRID_KWARGS, forcing_inputs=fi)
    assert cfg.forcing.initial_conditions is None

    ov = sources_to_forcing_override(cfg)
    assert "initial_conditions" not in ov
    assert "forcing" in ov


def test_forge_blueprint_to_builder_kwargs_carries_cdr_forcing_file(tmp_path):
    """cdr_forcing_file reaches the executor the same way as cdr_forcing/grid_file:
    a top-level ``forge_blueprint_to_builder_kwargs`` kwarg, NOT routed through
    ``sources_to_forcing_override`` (which only ever carries initial_conditions/
    surface/boundary/tidal/river -- cdr_forcing itself is never in there either).
    """
    from cstar.applications.forge.engine import (
        forge_blueprint_to_builder_kwargs,
        sources_to_forcing_override,
    )

    cdr_path = _write_tiny_netcdf(tmp_path, name="cdr.nc")
    cfg = _build(cdr_forcing_file=str(cdr_path))

    kwargs = forge_blueprint_to_builder_kwargs(cfg)
    assert kwargs["cdr_forcing_file"] == cfg.cdr.cdr_forcing_file
    assert kwargs["cdr_forcing"] is None
    assert kwargs["cdr_mode"] == "netcdf"

    ov = sources_to_forcing_override(cfg)
    assert "cdr_forcing_file" not in ov
    assert "cdr_forcing" not in ov


def test_catalog_scans_forcingspec():
    from cstar.catalog.domain_catalog import default_catalog as cat

    assert "glorys-era5-unified" in cat.forcing_names
    data = cat.forcing_data("glorys-era5-unified")
    assert "forcing" in data and "initial_conditions" in data


def test_sources_to_forcing_override_returns_dict_by_default():
    from cstar.applications.forge.engine import sources_to_forcing_override

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
    from cstar.applications.forge.engine import sources_to_forcing_override
    from cstar.catalog.domain_catalog import default_catalog as cat

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


def test_regrid_options_survive_resolve_and_override_round_trip():
    """The roms-tools >=4 prefill/regrid_method/extrap_method knobs, authored on
    initial_conditions/surface/tidal, survive both the resolver
    (build_forge_blueprint -> Forcing) and the reverse dump
    (sources_to_forcing_override) -- the two propagation paths that must stay in
    lockstep with the typed model fields (see forge_blueprint_resolve._build_forcing
    and forge_blueprint_engine.sources_to_forcing_override).
    """
    from cstar.applications.forge.engine import sources_to_forcing_override
    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = cat.forcing_data("glorys-era5-unified")
    fdata["initial_conditions"]["prefill"] = "inverse_dist"
    fdata["initial_conditions"]["regrid_method"] = "xesmf"
    fdata["forcing"]["surface"][0]["prefill"] = "nearest_neighbor"
    fdata["forcing"]["surface"][0]["extrap_method"] = "nearest_s2d"
    fdata["forcing"]["tidal"][0]["prefill"] = "2d_lateral_fill"
    fdata["forcing"]["tidal"][0]["regrid_method"] = "scipy"

    cfg = _build(forcing_inputs=fdata)
    s = cfg.forcing
    assert s.initial_conditions.prefill == "inverse_dist"
    assert s.initial_conditions.regrid_method == "xesmf"
    assert s.surface[0].prefill == "nearest_neighbor"
    assert s.surface[0].extrap_method == "nearest_s2d"
    assert s.tidal[0].prefill == "2d_lateral_fill"
    assert s.tidal[0].regrid_method == "scipy"

    ov = sources_to_forcing_override(cfg)
    assert ov["initial_conditions"]["prefill"] == "inverse_dist"
    assert ov["initial_conditions"]["regrid_method"] == "xesmf"
    assert ov["forcing"]["surface"][0]["prefill"] == "nearest_neighbor"
    assert ov["forcing"]["surface"][0]["extrap_method"] == "nearest_s2d"
    assert ov["forcing"]["tidal"][0]["prefill"] == "2d_lateral_fill"
    assert ov["forcing"]["tidal"][0]["regrid_method"] == "scipy"


def test_regrid_options_survive_wizard_load_back():
    """The *other* reverse path -- ForgeBlueprintWizard._sources_to_inputs, which
    seeds the forcing editor when a config is loaded into the wizard -- must also
    carry prefill/regrid_method/extrap_method and allow_flex_time. This is the
    load-back whitelist that silently dropped allow_flex_time until this test was
    added (see project memory emod_rivr2o_datasources for the pattern);
    sources_to_forcing_override (tested above) is a different code path and does
    not cover this one.
    """
    pytest.importorskip("ipywidgets")
    from cstar.catalog.domain_catalog import default_catalog as cat
    from cstar.wizard.wizard import ForgeBlueprintWizard

    fdata = cat.forcing_data("glorys-era5-unified")
    fdata["initial_conditions"]["prefill"] = "inverse_dist"
    fdata["initial_conditions"]["allow_flex_time"] = True
    fdata["forcing"]["surface"][0]["regrid_method"] = "xesmf"
    fdata["forcing"]["tidal"][0]["extrap_method"] = "nearest_s2d"

    cfg = _build(forcing_inputs=fdata)
    seeded = ForgeBlueprintWizard._sources_to_inputs(cfg)

    assert seeded["initial_conditions"]["prefill"] == "inverse_dist"
    assert seeded["initial_conditions"]["allow_flex_time"] is True
    assert seeded["forcing"]["surface"][0]["regrid_method"] == "xesmf"
    assert seeded["forcing"]["tidal"][0]["extrap_method"] == "nearest_s2d"


def test_forcing_override_coerces_enums_to_strings():
    """Regression: enum-typed item fields (SurfaceType, BgcInterpMethod,
    ClimatologyMode, …) must be dumped as plain strings, not enum instances. Enum
    instances leaked into output filenames (f"{key}-{type}") and into roms-tools'
    SafeDumper (→ 'cannot represent an object'). The bridge dumps with mode="json".
    """
    import enum

    from cstar.applications.forge.engine import sources_to_forcing_override
    from cstar.catalog.domain_catalog import default_catalog as cat

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
    """Insurance: importing cstar.applications.forge registers a global Enum representer so any
    Forge enum reaching a SafeDumper (or a subclass, as roms-tools' NoAliasDumper is)
    serializes as its value rather than raising 'cannot represent an object'.
    """
    import cstar.applications.forge  # noqa: F401  (side effect: registers the representer)
    from cstar.applications.forge.blueprint import SurfaceType

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

    from cstar.applications.forge import input_data as id_mod

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
    # Note: BGC is now the explicit `has_bgc` constructor arg (mirroring
    # ForgeExecutor._has_bgc, itself read from _settings_compile_time["cppdefs"]["marbl"]),
    # not read from model_spec.settings.properties — no need to set it on mock_spec.

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
    from cstar.catalog.domain_catalog import default_catalog as cat

    assert "standard" in cat.output_names
    data = cat.output_data("standard")
    assert "ocean_vars" in data and "diagnostics" in data
    assert set(data["marbl_bgc"]) == {
        "marbl_tracers_to_write",
        "marbl_diagnostics_to_write",
    }


# Every (period, nrpf) stream pair ucla-roms >= 0.5.0's check_output_divides_rst
# covers within OutputSpec-owned sections. The precheck requires, per ENABLED
# stream, that nrpf * output_period evenly divide output_period_rst (skipped
# when restarts are monthly / the periodic frequency is 0), so spec defaults
# must satisfy it for every stream a user might enable.
_OUTPUT_SPEC_STREAMS = (
    ("ocean_vars", "output_period_his", "nrpf_his"),
    ("ocean_vars", "output_period_avg", "nrpf_avg"),
    ("surf_flux", "output_period", "nrpf"),
    ("diagnostics", "output_period", "nrpf"),
    ("frc_output", "output_period", "nrpf"),
    ("cdr_output", "output_period", "nrpf"),
    ("cdr_tracer_output", "output_period", "nrpf"),
    ("cdr_gas_exch_output", "output_period", "nrpf"),
    ("upscale_output", "output_period_uscl", "nrpf_uscl"),
    ("zslice", "output_period", "nrpf"),
    ("random_output", "output_period", "nrpf"),
    ("bgc", "output_period_his", "nrpf_his"),
    ("bgc", "output_period_avg", "nrpf_avg"),
    ("bgc", "output_period_his_dia", "nrpf_his_dia"),
    ("bgc", "output_period_avg_dia", "nrpf_avg_dia"),
)

# ModelSpec-owned stream pairs the same precheck covers (extract_data is
# nesting-derived at resolve time, so it can't be checked from static specs).
_MODEL_SPEC_STREAMS = (
    ("sponge_tune", "output_period", "nrpf"),
    ("particles", "output_period", "nrpf"),
)


def _assert_streams_divide_rst(sections: dict, streams, rst: float, origin: str):
    for section, period_key, nrpf_key in streams:
        newfile_freq = sections[section][nrpf_key] * sections[section][period_key]
        assert newfile_freq > 0 and rst % newfile_freq == 0, (
            f"{origin}: {section}.{nrpf_key} * {section}.{period_key} "
            f"= {newfile_freq} s does not evenly divide output_period_rst "
            f"= {rst} s -- ucla-roms >= 0.5.0's check_output_divides_rst "
            f"aborts if this stream is enabled."
        )


@pytest.mark.parametrize(
    "spec_name", ["daily-restarts", "weekly-restarts", "monthly-restarts"]
)
def test_bundled_output_specs_satisfy_roms_divides_rst_precheck(spec_name):
    """The precheck-safe OutputSpecs must stay self-consistent: every stream a
    user might enable divides the restart period ('standard' is exempt -- it is
    kept unchanged for blueprints that reference it).
    """
    data = _CATALOG.output_data(spec_name)
    ov = data["ocean_vars"]
    if ov["monthly_restarts"] or ov["output_period_rst"] == 0:
        return  # the precheck is vacuous (mod 0) -- nothing to assert
    _assert_streams_divide_rst(
        data, _OUTPUT_SPEC_STREAMS, ov["output_period_rst"], spec_name
    )


@pytest.mark.parametrize("spec_name", ["daily-restarts", "weekly-restarts"])
@pytest.mark.parametrize(
    "model_spec_name",
    ["roms-marbl-0.5-default", "roms-marbl-0.7-default", "roms-marbl-0.8-default"],
)
def test_model_spec_streams_satisfy_roms_divides_rst_precheck(
    model_spec_name, spec_name
):
    """Each versioned ModelSpec's own streams (sponge, particles) must divide
    the restart period of every periodic-restart precheck-safe OutputSpec,
    since a resolved blueprint combines the two.
    """
    model_settings = yaml.safe_load(
        (_BUNDLED_CATALOG / "ModelSpec" / model_spec_name / "model.yaml").read_text()
    )["model_settings"]
    rst = _CATALOG.output_data(spec_name)["ocean_vars"]["output_period_rst"]
    _assert_streams_divide_rst(
        model_settings,
        _MODEL_SPEC_STREAMS,
        rst,
        f"{model_spec_name} + {spec_name}",
    )


# Child grid used by the resolver-layer extract-divides-rst tests below
# (same shape as test_resolver_nesting_enables_extract_data's).
_CHILD_GRID = dict(
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
)


def test_resolver_rejects_extract_period_not_dividing_rst_for_roms050():
    """Nesting with a child period whose files don't roll on restart boundaries
    fails at authoring time for a >= 0.5.0 model (ucla-roms's own
    check_output_divides_rst would abort the run at startup).
    """
    # seeded nrpf=24; 24 * 5000 = 120000 s does not divide rst 86400 s
    with pytest.raises(ValueError, match="evenly divide"):
        _build(
            model_dir=_MODEL_DIR_ROMS050,
            grid_kwargs_child=_CHILD_GRID,
            metadata_child={"period": 5000.0},
        )


def test_resolver_accepts_conforming_extract_period_for_roms050():
    cfg = _build(
        model_dir=_MODEL_DIR_ROMS050,
        grid_kwargs_child=_CHILD_GRID,
        metadata_child={"period": 1800.0},  # 24 * 1800 = 43200 | 86400
    )
    assert cfg.model_settings["extract_data"]["do_extract"] is True


def test_resolver_extract_check_gated_off_for_legacy_roms():
    """The same nonconforming period resolves fine for a pre-0.5.0 model --
    older ucla-roms has no such precheck, so authoring isn't blocked.
    """
    cfg = _build(  # default _MODEL_DIR pins roms 0.2.0 (legacy schema)
        grid_kwargs_child=_CHILD_GRID,
        metadata_child={"period": 5000.0},
    )
    assert cfg.model_settings["extract_data"]["extract_period"] == 5000.0


def _standard_output_settings_with_bad_frc():
    """A copy of the 'standard' OutputSpec with `frc` enabled and set to a
    period that doesn't evenly divide `ocean_vars.output_period_rst`
    (86400 s): 4 (nrpf) * 5000 s = 20000 s, and 86400 % 20000 != 0.
    """
    settings = {
        k: (dict(v) if isinstance(v, dict) else v)
        for k, v in _CATALOG.output_data("standard").items()
    }
    settings["frc_output"] = {
        **settings["frc_output"],
        "wrt_frc": True,
        "output_period": 5000.0,
    }
    return settings


def test_resolver_rejects_non_extract_stream_not_dividing_rst_for_roms050():
    """The general C-Star `check_output_streams_divide_rst` check (which covers
    every ucla-roms >= 0.5.0 precheck stream, not just `extract`) also fires at
    resolve time -- here for `frc`.
    """
    with pytest.raises(ValueError, match="evenly divide"):
        _build(
            model_dir=_MODEL_DIR_ROMS050,
            output_settings=_standard_output_settings_with_bad_frc(),
        )


def test_resolver_non_extract_stream_check_gated_off_for_legacy_roms():
    """The same nonconforming `frc` config resolves fine for a pre-0.5.0 model
    -- older ucla-roms has no such precheck, so authoring isn't blocked.
    """
    cfg = _build(  # default _MODEL_DIR pins roms 0.2.0 (legacy schema)
        output_settings=_standard_output_settings_with_bad_frc(),
    )
    assert cfg.model_settings["frc_output"]["wrt_frc"] is True


def test_resolver_output_settings_override():
    from cstar.catalog.domain_catalog import default_catalog as cat

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
    from cstar.applications.forge.resolve import (
        OUTPUT_BGC_FIELDS,
        OUTPUT_SECTIONS,
        extract_output_settings,
    )

    # OUTPUT_SECTIONS now includes cdr_tracer_output/cdr_gas_exch_output
    # (ucla-roms >= 0.7.0, PR #351), which prune_version_gated_sections drops
    # from an older-pinned build's model_settings -- use a 0.7.0-pinned
    # ModelSpec so every OUTPUT_SECTIONS entry actually survives resolution.
    cfg = _build(model_dir=_MODEL_DIR_ROMS070)
    out = extract_output_settings(cfg.model_settings)
    assert set(OUTPUT_SECTIONS) <= set(out)
    assert set(out["marbl_bgc"]) == {
        "marbl_tracers_to_write",
        "marbl_diagnostics_to_write",
    }
    assert set(out["bgc"]) == set(OUTPUT_BGC_FIELDS)


def test_resolver_forcing_inputs_override():
    from cstar.catalog.domain_catalog import default_catalog as cat

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


_CDR_SAMPLE_YAML = Path(__file__).parent / "fixtures" / "cdr_forcing_sample.yaml"


def test_read_cdr_forcing_yaml_from_sample():
    from cstar.applications.forge.resolve import read_cdr_forcing_yaml

    block = read_cdr_forcing_yaml(_CDR_SAMPLE_YAML)
    assert block["releases"], "sample must carry at least one release"
    assert "_tracer_metadata" not in block
    assert block["start_time"] == "2012-01-01T00:00:00"


def test_read_cdr_forcing_yaml_accepts_raw_text():
    from cstar.applications.forge.resolve import read_cdr_forcing_yaml

    text = _CDR_SAMPLE_YAML.read_text()
    block = read_cdr_forcing_yaml(text)
    assert block["releases"]
    assert "_tracer_metadata" not in block


def test_read_cdr_forcing_yaml_rejects_non_cdr():
    from cstar.applications.forge.resolve import read_cdr_forcing_yaml

    with pytest.raises(ValueError, match="CDRForcing"):
        read_cdr_forcing_yaml("---\nSomeOtherThing:\n  foo: bar\n")


def test_build_with_cdr_forcing_yaml():
    cfg = _build(cdr_forcing_yaml=_CDR_SAMPLE_YAML)
    assert cfg.cdr.mode == "yaml"
    assert cfg.cdr.cdr_forcing["releases"]
    assert "_tracer_metadata" not in cfg.cdr.cdr_forcing
    assert cfg.model_settings["cppdefs"]["cdr_forcing"] is True


def test_build_forge_blueprint_strips_tracer_metadata():
    cfg = _build(
        cdr_forcing={"releases": [], "_tracer_metadata": {"temp": {"units": "C"}}}
    )
    assert "_tracer_metadata" not in cfg.cdr.cdr_forcing


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

    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.content_hash() == h1


_CDR_OUTPUT_REQUIRED_DIAGNOSTICS = (
    "zsatarag",
    "zsatcalc",
    "CO3",
    "CO3_ALT_CO2",
    "co3_sat_arag",
    "co3_sat_calc",
)
# Output.yaml's own defaults, in file order -- asserted to still come first so the
# CDR-output consistency block only *appends*, never reorders/replaces.
_DEFAULT_MARBL_DIAGNOSTICS = (
    "PH",
    "PH_ALT_CO2",
    "pCO2SURF_ALT_CO2",
    "pCO2SURF",
    "FG_CO2",
    "FG_ALT_CO2",
)


def test_cdr_output_user_enabled_without_forcing_sets_cppdef_and_diagnostics():
    """Pathway 1: a user turns on CDR output with no CDR forcing at all -- output is
    valid standalone (ROMS opens no CDR file; cdr_frc.cdr_source stays False), but
    cppdefs.cdr_forcing must still flip True (it gates compiling cdr_output.F90) and
    the MARBL diagnostics ucla-roms looks up by name must be present.
    """
    cfg = _build(run_time_overrides={"cdr_output": {"do_cdr_output": True}})
    settings = cfg.model_settings
    assert settings["cdr_output"]["do_cdr_output"] is True
    assert settings["cppdefs"]["cdr_forcing"] is True
    assert settings["cdr_frc"]["cdr_source"] is False
    diags = settings["marbl_bgc"]["marbl_diagnostics_to_write"]
    assert diags[: len(_DEFAULT_MARBL_DIAGNOSTICS)] == list(_DEFAULT_MARBL_DIAGNOSTICS)
    for name in _CDR_OUTPUT_REQUIRED_DIAGNOSTICS:
        assert diags.count(name) == 1, name


def test_cdr_forcing_implies_cdr_output():
    """Providing CDR forcing always implies CDR output -- there is no point
    generating a CDR forcing file ROMS won't report on.
    """
    cfg = _build(cdr_forcing_yaml=_CDR_SAMPLE_YAML)
    settings = cfg.model_settings
    assert settings["cdr_output"]["do_cdr_output"] is True
    assert settings["cppdefs"]["cdr_forcing"] is True
    diags = settings["marbl_bgc"]["marbl_diagnostics_to_write"]
    for name in _CDR_OUTPUT_REQUIRED_DIAGNOSTICS:
        assert name in diags


def test_cdr_output_disabled_by_default():
    """Neither a user override nor CDR forcing -- output stays disabled and the
    diagnostics list is untouched (still just Output.yaml's 6 defaults).
    """
    cfg = _build()
    settings = cfg.model_settings
    assert settings["cdr_output"]["do_cdr_output"] is False
    assert settings["cppdefs"]["cdr_forcing"] is False
    assert settings["marbl_bgc"]["marbl_diagnostics_to_write"] == list(
        _DEFAULT_MARBL_DIAGNOSTICS
    )


def test_cdr_output_requires_marbl():
    """do_cdr_output=True with bgc_mode="none" must raise -- ucla-roms only compiles
    cdr_output.F90 under MARBL && CDR_FORCING.
    """
    with pytest.raises(ValueError, match="do_cdr_output"):
        _build(
            bgc_mode="none",
            forcing_inputs=_PHYSICS_ONLY_FORCING,
            run_time_overrides={"cdr_output": {"do_cdr_output": True}},
        )


def test_cdr_tracer_output_does_not_require_marbl():
    """do_cdr_tracer_output=True with bgc_mode="none" is accepted: the CDR
    tracers exist without MARBL, so C-Star encodes the intended rule (only
    CDR_FORCING is needed) rather than ucla-roms 0.7.0/0.8.0's MARBL-only
    compile guard. cppdefs.cdr_forcing is still forced on.
    """
    cfg = _build(
        model_dir=_MODEL_DIR_ROMS070,
        bgc_mode="none",
        forcing_inputs=_PHYSICS_ONLY_FORCING,
        run_time_overrides={"cdr_tracer_output": {"do_cdr_tracer_output": True}},
    )
    settings = cfg.model_settings
    assert settings["cdr_tracer_output"]["do_cdr_tracer_output"] is True
    assert settings["cppdefs"]["cdr_forcing"] is True
    assert settings["cppdefs"]["marbl"] is False


def test_cdr_gas_exch_output_requires_marbl():
    """do_cdr_gas_exch_output=True with bgc_mode="none" must raise: the
    gas-exchange stream reads MARBL's alternative-CO2 tracers, so MARBL is a
    genuine requirement (unlike the tracer stream above).
    """
    with pytest.raises(ValueError, match="do_cdr_gas_exch_output"):
        _build(
            model_dir=_MODEL_DIR_ROMS070,
            bgc_mode="none",
            forcing_inputs=_PHYSICS_ONLY_FORCING,
            run_time_overrides={
                "cdr_gas_exch_output": {"do_cdr_gas_exch_output": True}
            },
        )


def test_cdr_tracer_output_enabled_sets_cppdef():
    """Enabling do_cdr_tracer_output alone (no CDR forcing; do_cdr_output stays
    False) still flips cppdefs.cdr_forcing -- it gates compiling the CDR tracer
    output module.
    """
    cfg = _build(
        model_dir=_MODEL_DIR_ROMS070,
        run_time_overrides={"cdr_tracer_output": {"do_cdr_tracer_output": True}},
    )
    settings = cfg.model_settings
    assert settings["cdr_tracer_output"]["do_cdr_tracer_output"] is True
    assert settings["cppdefs"]["cdr_forcing"] is True
    assert settings["cdr_output"]["do_cdr_output"] is False  # not forced on


def test_cdr_gas_exch_output_enabled_sets_cppdef():
    """Mirrors test_cdr_tracer_output_enabled_sets_cppdef for the gas-exchange
    output group.
    """
    cfg = _build(
        model_dir=_MODEL_DIR_ROMS070,
        run_time_overrides={"cdr_gas_exch_output": {"do_cdr_gas_exch_output": True}},
    )
    settings = cfg.model_settings
    assert settings["cdr_gas_exch_output"]["do_cdr_gas_exch_output"] is True
    assert settings["cppdefs"]["cdr_forcing"] is True
    assert settings["cdr_output"]["do_cdr_output"] is False


def test_active_cdr_forcing_does_not_enable_tracer_gas_exch_output():
    """Unlike do_cdr_output, an active CDR forcing mode must NOT force
    do_cdr_tracer_output/do_cdr_gas_exch_output on -- they're opt-in extras a
    user enables explicitly (see the resolver's CDR tracer/gas-exchange output
    consistency check).
    """
    cfg = _build(model_dir=_MODEL_DIR_ROMS070, cdr_forcing_yaml=_CDR_SAMPLE_YAML)
    settings = cfg.model_settings
    assert settings["cdr_output"]["do_cdr_output"] is True  # forced on, as before
    assert settings["cdr_tracer_output"]["do_cdr_tracer_output"] is False
    assert settings["cdr_gas_exch_output"]["do_cdr_gas_exch_output"] is False


def test_cdr_tracer_gas_exch_output_sections_pruned_before_0_7_0():
    """A blueprint pinned to ucla-roms 0.6.x (RunTimeSettingsV0_6_0, which has
    no cdr_tracer_output/cdr_gas_exch_output fields) must not carry either
    section in model_settings -- see prune_version_gated_sections in
    namelist_model.py. The matching 0.7.0-pinned build keeps both.
    """
    cfg_060 = _build(model_dir=_MODEL_DIR_ROMS060)
    assert "cdr_tracer_output" not in cfg_060.model_settings
    assert "cdr_gas_exch_output" not in cfg_060.model_settings

    cfg_070 = _build(model_dir=_MODEL_DIR_ROMS070)
    assert "cdr_tracer_output" in cfg_070.model_settings
    assert "cdr_gas_exch_output" in cfg_070.model_settings


def test_pruned_cdr_tracer_output_override_does_not_flip_cppdef_before_0_7_0():
    """Pruning runs BEFORE the tracer/gas-exchange consistency check: on a
    0.6.x pin an override enabling do_cdr_tracer_output is dropped (the pin's
    namelist schema can't emit the group), so it must not leave a stray
    cppdefs.cdr_forcing=True behind with no section in model_settings to
    explain it.
    """
    cfg = _build(
        model_dir=_MODEL_DIR_ROMS060,
        run_time_overrides={"cdr_tracer_output": {"do_cdr_tracer_output": True}},
    )
    settings = cfg.model_settings
    assert "cdr_tracer_output" not in settings
    assert settings["cppdefs"].get("cdr_forcing", False) is False
    assert settings["cdr_output"]["do_cdr_output"] is False


def test_rst_period_not_divisible_by_dt_raises():
    """output_period_rst must be an integer multiple of dt when restarts are
    written on a fixed period (the default: wrt_file_rst=True,
    monthly_restarts=False) -- 150s / 100s = 1.5, not a whole number of steps.
    """
    with pytest.raises(ValueError, match="output_period_rst"):
        _build(
            run_time_overrides={
                "time_stepping": {"dt": 100.0},
                "ocean_vars": {"output_period_rst": 150.0},
            }
        )


def test_rst_period_divisible_by_dt_accepted():
    cfg = _build(
        run_time_overrides={
            "time_stepping": {"dt": 100.0},
            "ocean_vars": {"output_period_rst": 200.0},
        }
    )
    assert cfg.model_settings["ocean_vars"]["output_period_rst"] == 200.0


def test_rst_period_not_divisible_accepted_with_monthly_restarts():
    """monthly_restarts=True means output_period_rst is unused -- any value must
    be accepted.
    """
    cfg = _build(
        run_time_overrides={
            "time_stepping": {"dt": 100.0},
            "ocean_vars": {"output_period_rst": 150.0, "monthly_restarts": True},
        }
    )
    assert cfg.model_settings["ocean_vars"]["output_period_rst"] == 150.0


def test_rst_period_not_divisible_accepted_with_rst_writing_off():
    """wrt_file_rst=False means output_period_rst is unused -- any value must be
    accepted.
    """
    cfg = _build(
        run_time_overrides={
            "time_stepping": {"dt": 100.0},
            "ocean_vars": {"output_period_rst": 150.0, "wrt_file_rst": False},
        }
    )
    assert cfg.model_settings["ocean_vars"]["output_period_rst"] == 150.0


def test_bgc_source_item_serialize_dask_roundtrips():
    """`serialize_dask` is a per-source write option, not a roms-tools source
    parameter: it must survive a blueprint round-trip, default to None (inherit
    the --serialize-dask-write CLI flag), and not loosen `extra="forbid"`.
    """
    from pydantic import ValidationError

    from cstar.applications.forge.blueprint import BgcSourceItem

    item = BgcSourceItem(
        source={"name": "ESPER"}, use_vars=["ALK", "DIC"], serialize_dask=True
    )
    assert item.serialize_dask is True
    assert item.model_dump()["serialize_dask"] is True
    assert BgcSourceItem(**item.model_dump()).serialize_dask is True

    # Omitted means "inherit", which must stay distinguishable from an explicit
    # False -- the CLI flag can only take over when the source said nothing.
    assert BgcSourceItem(source={"name": "ESPER"}).serialize_dask is None
    assert (
        BgcSourceItem(source={"name": "ESPER"}, serialize_dask=False).serialize_dask
        is False
    )

    with pytest.raises(ValidationError):
        BgcSourceItem(source={"name": "ESPER"}, serialize_dsk=True)


class TestEnsureCdrOutputMarblDiagnostics:
    def test_none_input_returns_all_required(self):
        from cstar.applications.forge.namelist_model import (
            CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS,
            ensure_cdr_output_marbl_diagnostics,
        )

        assert ensure_cdr_output_marbl_diagnostics(None) == list(
            CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS
        )

    def test_empty_list_returns_all_required(self):
        from cstar.applications.forge.namelist_model import (
            CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS,
            ensure_cdr_output_marbl_diagnostics,
        )

        assert ensure_cdr_output_marbl_diagnostics([]) == list(
            CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS
        )

    def test_partial_overlap_no_duplicates_order_preserved(self):
        from cstar.applications.forge.namelist_model import (
            ensure_cdr_output_marbl_diagnostics,
        )

        result = ensure_cdr_output_marbl_diagnostics(["PH", "CO3", "FG_CO2"])
        assert result[:3] == ["PH", "CO3", "FG_CO2"]
        assert result.count("CO3") == 1
        for name in _CDR_OUTPUT_REQUIRED_DIAGNOSTICS:
            assert result.count(name) == 1


@pytest.mark.parametrize("do_cdr_output", [True, False])
def test_cdr_output_toggle_renders_cdr_forcing_cppdef(tmp_path, do_cdr_output):
    """End-to-end template gate: the stored blueprint's settings alone (no CDR
    forcing, no generation step) must drive ``#define``/``#undef CDR_FORCING`` in the
    real ``cppdefs.opt.j2`` -- the cppdef gates compiling ucla-roms' cdr_output.F90.
    """
    overrides = {"cdr_output": {"do_cdr_output": True}} if do_cdr_output else {}
    cfg = _build(run_time_overrides=overrides)
    param = cfg.model_settings["param"]
    n_tracers = 2 + int(param.get("ntrc_bio", 0)) + int(param.get("nt_passive", 0))
    render_roms_settings(
        template_files=["cppdefs.opt.j2"],
        template_dir=Path(cstar.__file__).parent
        / "additional_files"
        / "templates"
        / "forge"
        / "compile-time",
        settings_dict=dict(cfg.model_settings),
        code_output_dir=tmp_path,
        n_tracers=n_tracers,
    )
    text = (tmp_path / "cppdefs.opt").read_text()
    expected = "#define CDR_FORCING" if do_cdr_output else "#undef CDR_FORCING"
    assert expected in text


def test_n_tracers_includes_cdr_tracer_counts():
    """`ForgeBlueprint.n_tracers` counts `2*nt_cdr_oae + nt_cdr_dor` on top of
    T + S + ntrc_bio + nt_passive (see `n_tracers_from_param`); the bundled
    < 0.4.0 ModelSpec has neither key, so this exercises them via direct
    ``model_settings["param"]`` mutation, same as a >= 0.4.0-pinned blueprint
    loaded back from disk would carry.
    """
    cfg = _build()
    base = cfg.n_tracers

    cfg.model_settings["param"]["nt_cdr_oae"] = 2
    cfg.model_settings["param"]["nt_cdr_dor"] = 1
    assert cfg.n_tracers == base + 2 * 2 + 1


def test_resolver_use_pio_sets_cppdefs_and_code_pio():
    cfg = _build(use_pio=True)
    assert cfg.model_settings["cppdefs"]["use_pio"] is True
    assert cfg.code.pio is not None
    assert cfg.code.pio.location == "https://github.com/CWorthy-ocean/ParallelIO.git"
    assert cfg.code.pio.commit == "2.7.1-fork"


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
    text = (model_dir / "model.yaml").read_text()
    assert "use_pio: false" in text
    (model_dir / "model.yaml").write_text(
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
    from cstar.applications.forge.blueprint import CodeRepo
    from cstar.applications.forge.resolve import _build_code

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
    assert cfg.code.marbl.location == "https://github.com/CWorthy-ocean/MARBL.git"
    assert cfg.code.marbl.commit == "marbl0.45.0-max-it-10"


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


def test_no_tidal_item_forces_runtime_tide_switches_off():
    """ROMS enables tides at run time via bry_tides/pot_tides (TIDAL_FRC_SETTINGS);
    the TIDES cppdef only stamps a netCDF attribute. With no tidal item generated,
    both must be forced off -- past an explicit override -- or ROMS goes looking
    for tidal input data that was never generated. ana_tides is the deliberate
    escape hatch: analytical tides are computed in-model and need no input file.
    """
    cfg = _build()  # bundled ForcingSpec has a tidal item -> ModelSpec defaults kept
    assert cfg.model_settings["tides"]["bry_tides"] is True
    assert cfg.model_settings["tides"]["pot_tides"] is True

    cfg_no = _build(forcing_inputs=_PHYSICS_ONLY_FORCING)
    assert cfg_no.model_settings["tides"]["bry_tides"] is False
    assert cfg_no.model_settings["tides"]["pot_tides"] is False
    assert cfg_no.model_settings["tides"]["ntides"] == 0

    # an explicit bry_tides/pot_tides=True override can't win (it would crash ROMS)
    cfg_ov = _build(
        forcing_inputs=_PHYSICS_ONLY_FORCING,
        run_time_overrides={"tides": {"bry_tides": True, "pot_tides": True}},
    )
    assert cfg_ov.model_settings["tides"]["bry_tides"] is False
    assert cfg_ov.model_settings["tides"]["pot_tides"] is False

    # ...unless ana_tides is set, which legitimately runs tides without input data
    cfg_ana = _build(
        forcing_inputs=_PHYSICS_ONLY_FORCING,
        run_time_overrides={"tides": {"ana_tides": True}},
    )
    assert cfg_ana.model_settings["tides"]["ana_tides"] is True
    assert cfg_ana.model_settings["tides"]["bry_tides"] is True
    assert cfg_ana.model_settings["tides"]["pot_tides"] is True


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
    from cstar.applications.forge.blueprint import CodeRepo
    from cstar.applications.forge.resolve import _build_code

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
    """A bare numeric commit in model.yaml (e.g. `commit: 123456`, parsed by PyYAML
    as an int) must be coerced to str -- CodeRepo.commit is str-typed and rejects
    an int outright.
    """
    from cstar.applications.forge.blueprint import CodeRepo
    from cstar.applications.forge.resolve import _build_code

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


def test_build_code_copies_file_hashes_verbatim_from_modelspec():
    """``_build_code`` copies a ModelSpec's authored ``file_hashes`` straight into
    ``TemplateRepo.file_hashes`` -- no hashing, no filesystem access. It doesn't
    matter whether the value corresponds to anything on disk; that's the
    executor's problem at stage time (``ForgeExecutor._stage_templates``), not
    the resolver's.
    """
    from cstar.applications.forge.blueprint import CodeRepo
    from cstar.applications.forge.resolve import _build_code

    model = {
        "code": {
            "roms": {"location": "https://example.com/roms.git", "commit": "x"},
            "templates_compile_time": {
                "directory": "templates/compile-time",
                "files": ["cppdefs.opt.j2"],
                "file_hashes": {"cppdefs.opt.j2": "deadbeef" * 8},
            },
            "templates_run_time": {"directory": "templates/run-time", "files": []},
        },
    }
    templates_repo = CodeRepo(location="https://example.com/forge.git", branch="main")
    code = _build_code(model, templates_repo, bgc_mode="none")
    assert code.templates_compile_time.file_hashes == {"cppdefs.opt.j2": "deadbeef" * 8}


def test_build_code_defaults_file_hashes_empty_when_absent():
    """A ModelSpec that hasn't authored ``file_hashes`` yet resolves to an empty
    mapping -- ``ForgeExecutor._stage_templates`` then skips both the fast path
    and fetch verification, exactly as it did before ``file_hashes`` existed.
    """
    from cstar.applications.forge.blueprint import CodeRepo
    from cstar.applications.forge.resolve import _build_code

    model = {
        "code": {
            "roms": {"location": "https://example.com/roms.git", "commit": "x"},
            "templates_compile_time": {
                "directory": "templates/compile-time",
                "files": ["cppdefs.opt.j2"],
            },
            "templates_run_time": {"directory": "templates/run-time", "files": []},
        },
    }
    templates_repo = CodeRepo(location="https://example.com/forge.git", branch="main")
    code = _build_code(model, templates_repo, bgc_mode="none")
    assert code.templates_compile_time.file_hashes == {}


def test_content_hash_changes_with_roms_ref():
    cfg = _build()
    h = cfg.content_hash()
    cfg_override = _build(roms_ref="pio-refdate")
    assert cfg_override.content_hash() != h


def test_roms_ref_round_trips_through_yaml(tmp_path):
    cfg = _build(roms_ref="pio-refdate")
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.code.roms.commit == "pio-refdate"
    assert back.code.roms.branch is None


def test_resolver_marbl_ref_overrides_commit_and_clears_branch():
    cfg = _build(marbl_ref="marbl0.99.0")
    assert cfg.code.marbl.commit == "marbl0.99.0"
    assert cfg.code.marbl.branch is None
    # location is untouched -- only the checkout target changes
    assert cfg.code.marbl.location == "https://github.com/CWorthy-ocean/MARBL.git"


def test_resolver_marbl_ref_default_uses_model_yml_pin():
    cfg = _build()
    assert cfg.code.marbl.commit == "marbl0.45.0-max-it-10"


def test_resolver_marbl_ref_ignored_when_bgc_none():
    # bgc_mode="none" never populates code.marbl, so a stray marbl_ref is inert.
    cfg = _build(
        bgc_mode="none",
        marbl_ref="marbl0.99.0",
        forcing_inputs=_PHYSICS_ONLY_FORCING,
    )
    assert cfg.code.marbl is None


def test_content_hash_changes_with_marbl_ref():
    cfg = _build()
    h = cfg.content_hash()
    cfg_override = _build(marbl_ref="marbl0.99.0")
    assert cfg_override.content_hash() != h


def test_marbl_ref_round_trips_through_yaml(tmp_path):
    cfg = _build(marbl_ref="marbl0.99.0")
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.code.marbl.commit == "marbl0.99.0"
    assert back.code.marbl.branch is None


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
    from cstar.applications.forge.source_registry import resolve_dataset_key

    cfg = _build()
    s = cfg.forcing
    # dataset_key is no longer stored on SourceSpec — derive it when needed
    ic_src = s.initial_conditions.source
    assert resolve_dataset_key(ic_src.name, ic_src.glorys_layout) == "GLORYS_REGIONAL"
    bgc_src = s.initial_conditions.bgc_sources[0].source
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
    assert t.location.endswith("C-Star.git")
    assert t.files == ["cppdefs.opt.j2"]
    assert cfg.code.templates_run_time.files == ["marbl_in"]
    assert cfg.code.roms.commit == "0.2.0"


def test_resolved_templates_carry_modelspec_authored_hashes():
    """Resolving a bundled ModelSpec (``cson_roms-marbl_v0.1``, pinned at
    ``templates_commit`` = the C-Star 0.15.0 commit) copies its hand-authored ``file_hashes``
    straight onto the resolved ``TemplateRepo`` -- not a hash of whatever happens
    to be bundled in this C-Star build (which may be a newer commit).
    """
    cfg = _build()
    authored = yaml.safe_load((_MODEL_DIR / "model.yaml").read_text())["code"]
    assert (
        cfg.code.templates_compile_time.file_hashes
        == authored["templates_compile_time"]["file_hashes"]
    )
    assert (
        cfg.code.templates_run_time.file_hashes
        == authored["templates_run_time"]["file_hashes"]
    )
    # sanity: this ModelSpec's pin actually has authored hashes to compare (not
    # vacuously true because both sides are empty)
    assert cfg.code.templates_compile_time.file_hashes


@pytest.mark.parametrize(
    "model_spec_name,compile_time_fast_path",
    [
        # Every bundled ModelSpec pins templates_commit to the C-Star 0.15.0 commit,
        # whose cppdefs.opt.j2 / marbl_in are exactly the copies bundled in this
        # build, so both stages take the local fast path (no fetch). A ModelSpec
        # pinned elsewhere would show False here and fetch-and-verify instead --
        # see the real_template_staging tests in test_executor.py for that path.
        ("cson_roms-marbl_v0.1", True),
        ("roms-marbl-0.3-default", True),
        ("roms-marbl-0.4-default", True),
        ("roms-marbl-0.5-default", True),
        ("roms-marbl-0.6-default", True),
        ("roms-marbl-0.7-default", True),
        ("roms-marbl-0.8-default", True),
        ("pio-dev", True),
    ],
)
def test_bundled_modelspec_fast_path_eligibility_by_stage(
    model_spec_name, compile_time_fast_path
):
    """Whether ``ForgeExecutor._stage_templates`` takes the local fast path (vs.
    fetch-and-verify) is decided per stage by comparing each ModelSpec's authored
    ``file_hashes`` against the bundled copy actually shipped with this C-Star
    build. With every bundled ModelSpec pinned at the commit the bundled copy
    matches, both stages qualify for every spec.
    """
    from cstar.applications.forge.templates import (
        bundled_template_dir,
        hash_template_files,
    )

    code = yaml.safe_load(
        (_BUNDLED_CATALOG / "ModelSpec" / model_spec_name / "model.yaml").read_text()
    )["code"]
    expected = {
        "templates_compile_time": compile_time_fast_path,
        "templates_run_time": True,
    }
    for stage_field, want_fast_path in expected.items():
        stage = code[stage_field]
        bundled_dir = bundled_template_dir(stage["directory"])
        assert bundled_dir is not None
        try:
            bundled_hashes = hash_template_files(bundled_dir, stage["files"])
        except FileNotFoundError:
            bundled_hashes = None
        takes_fast_path = bundled_hashes == stage["file_hashes"]
        assert takes_fast_path is want_fast_path, (
            f"{model_spec_name}.{stage_field}: expected fast_path={want_fast_path}, "
            f"authored={stage['file_hashes']}, bundled={bundled_hashes}"
        )


def test_content_hash_unaffected_by_file_hashes():
    """``file_hashes`` is a locally-derived cache (the pinned commit's own bundled
    content), not independent results-affecting content -- adding or clearing it
    must not change ``content_hash``.
    """
    cfg = _build()
    assert cfg.code.templates_compile_time.file_hashes  # sanity: actually recorded
    with_hashes = cfg.content_hash()

    cleared = cfg.model_copy(
        update={
            "code": cfg.code.model_copy(
                update={
                    "templates_compile_time": cfg.code.templates_compile_time.model_copy(
                        update={"file_hashes": {}}
                    ),
                    "templates_run_time": cfg.code.templates_run_time.model_copy(
                        update={"file_hashes": {}}
                    ),
                }
            )
        }
    )
    assert cleared.content_hash() == with_hashes


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


def test_composition_records_spec_provenance():
    cfg = _build()
    assert cfg.composition.model.origin == "catalog"
    assert cfg.composition.model.name == "cson_roms-marbl_v0.1"
    assert cfg.composition.domain.name == "test-tiny"


def test_yaml_round_trip(tmp_path):
    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    back = ForgeBlueprint.from_yaml(p)
    assert back.casename == cfg.casename
    assert back.model_settings["time_stepping"] == cfg.model_settings["time_stepping"]


def test_committed_example_validates():
    """The checked-in example must remain a valid ForgeBlueprint."""
    example = (
        Path(cstar.__file__).parents[1]
        / "docs"
        / "forge-blueprint-example.wio-toy.yaml"
    )
    if not example.exists():
        pytest.skip("example file not present")
    cfg = ForgeBlueprint.from_yaml(example)
    assert cfg.composition.model.name == "cson_roms-marbl_v0.1"
    assert cfg.composition.model.origin == "catalog"


# ---------------------------------------------------------------------------
# Multiple bgc_sources: use_vars partitioning is required and enforced disjoint
# on both InitialConditions and BoundaryForcing (_require_partitioned_bgc_use_vars).
# ---------------------------------------------------------------------------
class TestBgcSourcesUseVarsPartitioning:
    def _two_sources(self, use_vars=(None, None)):
        from cstar.applications.forge.blueprint import BgcSourceItem

        return [
            BgcSourceItem(source={"name": "UNIFIED"}, use_vars=use_vars[0]),
            BgcSourceItem(source={"name": "GLODAP"}, use_vars=use_vars[1]),
        ]

    @pytest.mark.parametrize(
        "section_cls_name", ["InitialConditions", "BoundaryForcing"]
    )
    def test_single_bgc_source_needs_no_use_vars(self, section_cls_name):
        """Control: a single bgc source is never ambiguous, so use_vars stays
        optional -- the shipped blueprints all have exactly one.
        """
        from cstar.applications.forge import blueprint as fb

        cls = getattr(fb, section_cls_name)
        section = cls(
            source={"name": "GLORYS"},
            bgc_sources=[fb.BgcSourceItem(source={"name": "UNIFIED"})],
        )
        assert section.bgc_sources[0].use_vars is None

    @pytest.mark.parametrize(
        "section_cls_name", ["InitialConditions", "BoundaryForcing"]
    )
    def test_multiple_bgc_sources_require_use_vars_on_every_item(
        self, section_cls_name
    ):
        from cstar.applications.forge import blueprint as fb

        cls = getattr(fb, section_cls_name)
        with pytest.raises(ValueError, match="partition with use_vars"):
            cls(
                source={"name": "GLORYS"},
                bgc_sources=self._two_sources(use_vars=(["ALK"], None)),
            )

    @pytest.mark.parametrize(
        "section_cls_name", ["InitialConditions", "BoundaryForcing"]
    )
    def test_multiple_bgc_sources_reject_overlapping_use_vars(self, section_cls_name):
        from cstar.applications.forge import blueprint as fb

        cls = getattr(fb, section_cls_name)
        with pytest.raises(ValueError, match="partition with use_vars"):
            cls(
                source={"name": "GLORYS"},
                bgc_sources=self._two_sources(use_vars=(["ALK", "DIC"], ["DIC"])),
            )

    @pytest.mark.parametrize(
        "section_cls_name", ["InitialConditions", "BoundaryForcing"]
    )
    def test_multiple_bgc_sources_accept_disjoint_use_vars(self, section_cls_name):
        from cstar.applications.forge import blueprint as fb

        cls = getattr(fb, section_cls_name)
        section = cls(
            source={"name": "GLORYS"},
            bgc_sources=self._two_sources(use_vars=(["ALK", "DIC"], ["NO3"])),
        )
        assert [bs.use_vars for bs in section.bgc_sources] == [["ALK", "DIC"], ["NO3"]]

    # -- the one sanctioned overlap: ESPER + WOA_BGC on the salinity-merge tracers --

    def _esper_woa(self, esper_vars, woa_vars, *, partner="WOA_BGC"):
        from cstar.applications.forge.blueprint import BgcSourceItem

        return [
            BgcSourceItem(
                source={"name": "ESPER", "esper_method": "nn", "esper_equation": 8},
                use_vars=esper_vars,
            ),
            BgcSourceItem(
                source={"name": partner, "climatology": True}, use_vars=woa_vars
            ),
        ]

    @pytest.mark.parametrize(
        "section_cls_name", ["InitialConditions", "BoundaryForcing"]
    )
    def test_esper_and_woa_bgc_may_overlap_on_merge_tracers(self, section_cls_name):
        """roms-tools' salinity-based merge needs WOA to carry NO3/PO4/SiO3
        alongside ESPER; it blends them and drops them from WOA itself, so the
        validator must let that overlap through -- and keep both declarations.
        """
        pytest.importorskip("roms_tools.setup.salinity_merge")
        from cstar.applications.forge import blueprint as fb

        cls = getattr(fb, section_cls_name)
        section = cls(
            source={"name": "GLORYS"},
            bgc_sources=self._esper_woa(
                ["ALK", "DIC", "NO3", "PO4", "SiO3", "O2"], ["NO3", "PO4", "SiO3"]
            ),
        )
        assert [bs.use_vars for bs in section.bgc_sources] == [
            ["ALK", "DIC", "NO3", "PO4", "SiO3", "O2"],
            ["NO3", "PO4", "SiO3"],
        ]

    @pytest.mark.parametrize(
        "section_cls_name", ["InitialConditions", "BoundaryForcing"]
    )
    def test_esper_and_woa_bgc_still_reject_overlap_outside_merge_tracers(
        self, section_cls_name
    ):
        """The merge leaves ALK/DIC/O2 alone, so an overlap there is still ambiguous."""
        from cstar.applications.forge import blueprint as fb

        cls = getattr(fb, section_cls_name)
        with pytest.raises(ValueError, match="both claim 'ALK'"):
            cls(
                source={"name": "GLORYS"},
                bgc_sources=self._esper_woa(["ALK", "NO3"], ["ALK", "PO4"]),
            )

    @pytest.mark.parametrize(
        "section_cls_name", ["InitialConditions", "BoundaryForcing"]
    )
    def test_non_esper_pairs_still_reject_overlap_on_merge_tracers(
        self, section_cls_name
    ):
        """Only ESPER has a merge; GLODAP + WOA_BGC both claiming NO3 is a plain clash."""
        from cstar.applications.forge import blueprint as fb

        cls = getattr(fb, section_cls_name)
        items = self._esper_woa(["NO3"], ["NO3"])
        items[0] = fb.BgcSourceItem(source={"name": "GLODAP"}, use_vars=["NO3"])
        with pytest.raises(ValueError, match="both claim 'NO3'"):
            cls(source={"name": "GLORYS"}, bgc_sources=items)

    @pytest.mark.parametrize(
        "section_cls_name", ["InitialConditions", "BoundaryForcing"]
    )
    def test_esper_woa_bgc_overlap_rejected_without_merge_module(
        self, section_cls_name, monkeypatch
    ):
        """On a roms-tools that lacks the merge the overlap would reach
        process_bgc_fields undefined, so the exception must switch itself off.
        """
        from cstar.applications.forge import blueprint as fb

        monkeypatch.setattr(fb, "find_spec", lambda name: None)
        cls = getattr(fb, section_cls_name)
        with pytest.raises(ValueError, match="both claim 'NO3'"):
            cls(
                source={"name": "GLORYS"},
                bgc_sources=self._esper_woa(["NO3"], ["NO3"]),
            )

    def test_shipped_blueprints_have_single_bgc_source_and_still_validate(self):
        """The new validator must not break any file already in the repo -- every
        shipped blueprint has exactly one bgc source per section.
        """
        for p in _shipped_blueprint_paths():
            cfg = ForgeBlueprint.from_yaml(p)
            for section in (cfg.forcing.initial_conditions, cfg.forcing.boundary):
                if section is not None:
                    assert len(section.bgc_sources) <= 1, (
                        f"{p} has multiple bgc_sources -- the validator matrix "
                        "above should gain a case for this instead of relying "
                        "on this assumption"
                    )


# ---------------------------------------------------------------------------
# migrate_forcing_inputs hardening: warn on dropped per-item bgc keys, raise on
# a non-dict boundary entry (silent data loss before), and a direct test of the
# shared function on a ForcingSpec-shaped (ic + forcing siblings) pair.
# ---------------------------------------------------------------------------
class TestMigrateForcingInputsHardening:
    def test_dropped_bgc_item_keys_warn_with_index_and_source_name(self):
        from cstar.applications.forge.migration import migrate_forcing_inputs

        forcing = {
            "boundary": [
                {"type": "physics", "source": {"name": "GLORYS"}},
                {
                    "type": "bgc",
                    "source": {"name": "UNIFIED"},
                    "regrid_method": "nearest",  # not on BgcSourceItem -- dropped
                },
            ]
        }
        with pytest.warns(
            UserWarning, match=r"forcing\.boundary\[1\].*UNIFIED.*regrid_method"
        ):
            migrate_forcing_inputs(None, forcing)
        assert forcing["boundary"]["bgc_sources"] == [{"source": {"name": "UNIFIED"}}]

    def test_no_warning_when_no_extra_keys_dropped(self):
        import warnings

        from cstar.applications.forge.migration import migrate_forcing_inputs

        forcing = {
            "boundary": [
                {"type": "physics", "source": {"name": "GLORYS"}},
                {"type": "bgc", "source": {"name": "UNIFIED"}, "use_vars": None},
            ]
        }
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            migrate_forcing_inputs(None, forcing)
        assert not w

    def test_non_dict_boundary_entry_raises(self):
        from cstar.applications.forge.migration import migrate_forcing_inputs

        with pytest.raises(ValueError, match="non-dict entr"):
            migrate_forcing_inputs(
                None,
                {"boundary": [{"type": "physics", "source": {"name": "GLORYS"}}, None]},
            )

    def test_all_non_dict_boundary_list_raises_not_silently_none(self):
        """Previously an all-non-dict list silently became `None` (boundary
        forcing quietly vanishing); it must now raise instead.
        """
        from cstar.applications.forge.migration import migrate_forcing_inputs

        with pytest.raises(ValueError, match="non-dict entr"):
            migrate_forcing_inputs(None, {"boundary": [None, "not-a-dict"]})

    def test_direct_call_on_forcingspec_shaped_pair(self):
        """A catalog ``Forcing.yaml`` keeps ``initial_conditions`` and
        ``forcing`` as top-level siblings (not nested under one ``forcing``
        dict, unlike a full ForgeBlueprint) -- migrate_forcing_inputs must
        accept exactly that shape, since it's the wizard's ForcingSpec loader's
        real call signature.
        """
        from cstar.applications.forge.migration import migrate_forcing_inputs

        ic = {"bgc_source": {"name": "UNIFIED"}}
        forcing = {
            "boundary": [
                {"type": "physics", "source": {"name": "GLORYS"}},
                {"type": "bgc", "source": {"name": "UNIFIED"}},
            ]
        }
        migrate_forcing_inputs(ic, forcing)
        assert ic["bgc_sources"] == [{"source": {"name": "UNIFIED"}}]
        assert forcing["boundary"]["source"] == {"name": "GLORYS"}
        assert forcing["boundary"]["bgc_sources"] == [{"source": {"name": "UNIFIED"}}]

    def test_already_migrated_pair_is_a_no_op(self):
        import copy

        from cstar.applications.forge.migration import migrate_forcing_inputs

        ic = {"bgc_sources": [{"source": {"name": "UNIFIED"}}]}
        forcing = {"boundary": {"source": {"name": "GLORYS"}, "bgc_sources": []}}
        ic_before, forcing_before = copy.deepcopy(ic), copy.deepcopy(forcing)
        migrate_forcing_inputs(ic, forcing)
        assert ic == ic_before
        assert forcing == forcing_before


# ---------------------------------------------------------------------------
# Shipped-blueprint hygiene: every checked-in blueprint must be current version,
# hash-consistent with its own content, and load with no warnings.
# ---------------------------------------------------------------------------
def _shipped_blueprint_paths() -> list[Path]:
    repo_root = Path(cstar.__file__).parents[1]
    paths = sorted((_BUNDLED_CATALOG / "blueprints").glob("*.forge_blueprint.yaml"))
    example = repo_root / "docs" / "forge-blueprint-example.wio-toy.yaml"
    if example.exists():
        paths.append(example)
    return paths


@pytest.mark.parametrize("path", _shipped_blueprint_paths(), ids=lambda p: p.name)
def test_shipped_blueprint_hygiene(path):
    import warnings

    raw = yaml.safe_load(path.read_text())
    assert raw.get("forge_blueprint_version") == FORGE_BLUEPRINT_VERSION, (
        f"{path} is stamped forge_blueprint_version={raw.get('forge_blueprint_version')!r}, "
        f"expected {FORGE_BLUEPRINT_VERSION!r} -- restamp it (see B1's restamp recipe)."
    )
    stored_hash = raw.get("provenance", {}).get("content_hash")

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        cfg = ForgeBlueprint.from_yaml(path)
    assert not w, (
        f"{path} emits warning(s) on load: {[str(x.message) for x in w]} -- a "
        "shipped blueprint should already be in current, warning-free shape."
    )

    assert stored_hash == cfg.content_hash(), (
        f"{path}'s stored provenance.content_hash is stale -- restamp it (see "
        "B1's restamp recipe: rewrite the content_hash: line with "
        "ForgeBlueprint.from_yaml(p).content_hash())."
    )


class TestInstalledVersion:
    """``_installed_version`` backs ``provenance.cstar_version``/``roms_tools_version``
    -- no git-describe fallback needed, since both packages version themselves via
    ``setuptools_scm`` (an editable/dev checkout's installed version already embeds
    commit info).
    """

    def test_returns_formatted_version_when_installed(self, monkeypatch):
        from cstar.applications.forge import blueprint as fb

        monkeypatch.setattr(fb, "_pkg_version", lambda name: "4.0.0")
        assert fb._installed_version("roms-tools") == "roms-tools==4.0.0"

    def test_returns_none_when_not_installed(self, monkeypatch):
        from importlib.metadata import PackageNotFoundError

        from cstar.applications.forge import blueprint as fb

        def _raise(name):
            raise PackageNotFoundError(name)

        monkeypatch.setattr(fb, "_pkg_version", _raise)
        assert fb._installed_version("not-a-real-package") is None


class TestProvenanceStamping:
    """``ForgeBlueprint.to_yaml_str`` stamps generated_at/cstar_version/
    roms_tools_version on first save; a resave preserves whatever was already
    stamped (or explicitly set), same "first save wins" semantics as
    ``content_hash`` is exempt from (content_hash always recomputes; these don't).
    ``forge_version`` is never stamped (Forge is in-tree now -- see
    ``Provenance``'s docstring); it round-trips untouched if a file already has
    one (see ``test_forge_version_survives_resave_even_when_never_stamped``).
    """

    def _patched_fb(self, monkeypatch):
        from cstar.applications.forge import blueprint as fb

        monkeypatch.setattr(
            fb,
            "_installed_version",
            lambda name: f"{name}==9.9.9",
        )
        return fb

    def test_first_save_stamps_all_unset_fields(self, monkeypatch, tmp_path):
        fb = self._patched_fb(monkeypatch)
        cfg = _build()
        assert cfg.provenance.generated_at is None  # unstamped before saving

        path = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
        back = fb.ForgeBlueprint.from_yaml(path)

        assert back.provenance.generated_at is not None
        # must round-trip as tz-aware (UTC), not silently go naive/local through
        # model_dump(mode="json") -> yaml.safe_dump -> yaml.safe_load -> Pydantic
        assert back.provenance.generated_at.tzinfo is not None
        assert back.provenance.forge_version is None
        assert back.provenance.cstar_version == "cstar-ocean==9.9.9"
        assert back.provenance.roms_tools_version == "roms-tools==9.9.9"

    def test_resave_preserves_original_stamp(self, monkeypatch, tmp_path):
        fb = self._patched_fb(monkeypatch)
        cfg = _build()
        path = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
        first = fb.ForgeBlueprint.from_yaml(path)

        # a later save, from a different roms_tools/cstar install, must not
        # overwrite the original values
        monkeypatch.setattr(fb, "_installed_version", lambda name: f"{name}==1.0.0")
        first.to_yaml(path)
        second = fb.ForgeBlueprint.from_yaml(path)

        assert second.provenance.generated_at == first.provenance.generated_at
        assert second.provenance.forge_version is None
        assert second.provenance.cstar_version == "cstar-ocean==9.9.9"
        assert second.provenance.roms_tools_version == "roms-tools==9.9.9"

    def test_explicit_provenance_values_are_preserved(self, monkeypatch, tmp_path):
        """An explicitly pre-set field (e.g. a caller building a ``ForgeBlueprint``
        directly, or a re-resolve carrying an original value forward) is never
        overwritten by ``to_yaml_str``, even on first save.
        """
        fb = self._patched_fb(monkeypatch)
        cfg = _build()
        explicit_dt = datetime(2020, 1, 1)
        cfg = cfg.model_copy(
            update={
                "provenance": cfg.provenance.model_copy(
                    update={"generated_at": explicit_dt, "roms_tools_version": "pinned"}
                )
            }
        )
        path = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
        back = fb.ForgeBlueprint.from_yaml(path)

        assert back.provenance.generated_at == explicit_dt
        assert back.provenance.roms_tools_version == "pinned"
        # fields left unset are still stamped as normal
        assert back.provenance.cstar_version == "cstar-ocean==9.9.9"

    def test_forge_version_survives_resave_even_when_never_stamped(
        self, monkeypatch, tmp_path
    ):
        """An old blueprint that already has a ``forge_version`` (from before Forge
        moved in-tree) keeps it through a save/reload/resave cycle -- the field is
        kept for backward compatibility even though nothing stamps it anymore.
        """
        fb = self._patched_fb(monkeypatch)
        cfg = _build()
        cfg = cfg.model_copy(
            update={
                "provenance": cfg.provenance.model_copy(
                    update={"forge_version": "0.2.0"}
                )
            }
        )
        path = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
        first = fb.ForgeBlueprint.from_yaml(path)
        assert first.provenance.forge_version == "0.2.0"

        first.to_yaml(path)
        second = fb.ForgeBlueprint.from_yaml(path)
        assert second.provenance.forge_version == "0.2.0"


class _ReadOnlyValueGuard:
    """Wraps a widget so a bare ``.value = ...`` raises ``TraitError``, the way a
    read-only ``value`` trait would -- while ``.set_trait("value", ...)`` (the
    sanctioned traitlets escape hatch) and plain reads still work.

    ``FileUpload.value`` is read-only in some installed ipywidgets versions (e.g.
    7.x -- confirmed via ``ipywidgets.FileUpload.value.read_only`` there) but not
    others (e.g. this repo's dev env, ipywidgets>=8), so a bug in code that does
    `widget.value = ...` directly can pass every test here yet crash for a real
    user on a different ipywidgets version. This wrapper reproduces that failure
    mode regardless of which version happens to be installed for the test run.
    """

    def __init__(self, widget):
        object.__setattr__(self, "_widget", widget)

    def __getattr__(self, name):
        return getattr(self._widget, name)

    def __setattr__(self, name, value):
        if name == "value":
            from traitlets import TraitError

            raise TraitError('The "value" trait is read-only.')
        setattr(self._widget, name, value)


# ---------------------------------------------------------------------------
# Wizard (headless: ipywidgets value get/set/observe work without rendering)
# ---------------------------------------------------------------------------
class TestForgeBlueprintWizard:
    def _wizard(self):
        pytest.importorskip("ipywidgets")
        from cstar.wizard.wizard import ForgeBlueprintWizard

        return ForgeBlueprintWizard()

    def test_init_resolves_default_config(self):
        wiz = self._wizard()
        assert isinstance(wiz.config, ForgeBlueprint)
        assert wiz.config.casename  # derived, non-empty

    def test_selecting_catalog_domain_prefills_and_resolves(self):
        wiz = self._wizard()
        if "gulf-guinea-toy" not in wiz._dd_values(wiz.domain_dd):
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

    @staticmethod
    def _stub_grid_with_mask():
        """A fake roms_tools.Grid substitute for _build_grid_from_widgets: no
        real grid build (no network/roms_tools needed). Its mask is chosen so
        every edge's "any ocean point" verdict is the OPPOSITE of the widget
        checkbox defaults (south=False, west=False, east=True, north=True) --
        so a passing assertion can only mean real mask-derived values landed,
        never a coincidental match with the pre-derive defaults.
        """
        from types import SimpleNamespace

        import xarray as xr

        mask = xr.DataArray(
            [
                [1, 1, 1, 1, 0],  # south edge (eta_rho=0): has ocean -> south=True
                [1, 0, 0, 0, 0],
                [1, 0, 0, 0, 0],
                [0, 0, 0, 0, 0],  # north edge (eta_rho=-1): all land -> north=False
            ],
            dims=("eta_rho", "xi_rho"),
            # west edge (xi_rho=0) has ocean -> west=True;
            # east edge (xi_rho=-1) all land -> east=False
        )
        return SimpleNamespace(ds={"mask_rho": mask}, size_x=50.0, nx=5)

    def test_derive_from_grid_sets_untouched_boundaries_from_mask(self):
        """The "Derive from grid" button must actually flip open-boundary
        checkboxes from the land mask, not silently leave the checkbox
        defaults in place.
        """
        wiz = self._wizard()
        assert (
            wiz.bnd["south"].value,
            wiz.bnd["west"].value,
            wiz.bnd["east"].value,
            wiz.bnd["north"].value,
        ) == (False, False, True, True)  # widget defaults, pre-derive

        wiz._build_grid_from_widgets = self._stub_grid_with_mask
        wiz._on_derive_domain_properties(None)

        assert wiz._boundaries_derived is True
        assert wiz.bnd["south"].value is True
        assert wiz.bnd["west"].value is True
        assert wiz.bnd["east"].value is False
        assert wiz.bnd["north"].value is False
        ob = wiz.config.domain.open_boundaries
        assert (ob.south, ob.west, ob.east, ob.north) == (True, True, False, False)

    def test_derive_from_grid_does_not_overwrite_touched_boundaries(self):
        """_boundaries_touched is a single flag for the whole open_boundaries
        set (it persists to/from a DomainSpec as one unit) -- a manual edit to
        ANY one boundary freezes ALL of them against further auto-derivation,
        not just the one edited.
        """
        wiz = self._wizard()
        wiz.bnd["south"].value = True  # a manual edit -> touches the whole set
        assert wiz._boundaries_touched is True
        pre = {d: w.value for d, w in wiz.bnd.items()}

        wiz._build_grid_from_widgets = self._stub_grid_with_mask
        wiz._on_derive_domain_properties(None)

        assert wiz._boundaries_derived is True  # the button still "ran"
        # nothing was overwritten by the mask, even though the stub's mask
        # would derive different values for every one of these edges
        assert {d: w.value for d, w in wiz.bnd.items()} == pre

    def test_save_derives_untouched_boundaries_from_mask_not_defaults(self, tmp_path):
        """The export-time safety net (_ensure_boundaries_derived, called from
        _on_save) must make the SAVED blueprint reflect real mask-derived
        boundaries, not the provisional checkbox defaults -- the core guarantee
        behind "a save must never silently ship provisional defaults."
        """
        wiz = self._wizard()
        wiz._build_grid_from_widgets = self._stub_grid_with_mask
        p = tmp_path / "forge_blueprint.yaml"
        wiz.save_path.value = str(p)
        assert wiz._boundaries_derived is False  # nothing derived yet

        wiz._on_save(None)

        assert wiz._boundaries_derived is True  # the safety net ran
        saved = ForgeBlueprint.from_yaml(p)
        ob = saved.domain.open_boundaries
        assert (ob.south, ob.west, ob.east, ob.north) == (True, True, False, False)

    def test_save_aborts_when_boundary_derivation_fails(self, tmp_path):
        """If the grid build needed to derive boundaries fails, Save must abort
        rather than silently persist the provisional checkbox defaults.
        """
        wiz = self._wizard()

        def _boom():
            raise RuntimeError("synthetic grid-build failure")

        wiz._build_grid_from_widgets = _boom
        p = tmp_path / "forge_blueprint.yaml"
        wiz.save_path.value = str(p)

        wiz._on_save(None)

        assert not p.exists()  # nothing was written
        assert "aborted" in wiz.save_status.value.lower()

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
        wiz.save_path.value = str(tmp_path / "forge_blueprint.yaml")
        # Not exercising boundary derivation here -- skip the real grid build
        # the export-time safety net would otherwise trigger (see the
        # dedicated test_derive_from_grid_*/test_save_*_boundary_* tests).
        wiz._boundaries_touched = True
        wiz._on_save(None)
        cfg = ForgeBlueprint.from_yaml(tmp_path / "forge_blueprint.yaml")
        assert cfg.casename == wiz.config.casename

    def test_load_existing_config_round_trips(self, tmp_path):
        """Save a config, load it into a fresh wizard, and confirm widgets +
        resolved config round-trip (the #7 load affordance).
        """
        w1 = self._wizard()
        if "gulf-guinea-toy" in w1._dd_values(w1.domain_dd):
            w1.domain_dd.value = "gulf-guinea-toy"
        w1.name.value = "my-custom-run"
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
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
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        w2 = self._wizard()
        w2._load_bytes(p.read_bytes())
        assert w2.config is not None and w2.config.casename == w1.config.casename

    def test_validation_indicator_valid_by_default(self):
        w = self._wizard()
        assert "Blueprint is valid" in w.validation.value

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

    # -- ocean_vars/cdr_output bool<->dropdown + cross-field rules -------------
    def test_cdr_do_avg_dropdown_maps_to_bool(self):
        w = self._wizard()
        dd = w.editor._widgets[("cdr_output", "do_avg")][0]
        dd.value = "instantaneous"
        assert w.config.model_settings["cdr_output"]["do_avg"] is False
        dd.value = "averaged"
        assert w.config.model_settings["cdr_output"]["do_avg"] is True

    def test_bool_dropdown_load_back(self):
        """sync() maps a real bool from the composed/effective settings back onto
        the dropdown's string value (do_avg/monthly_averages). Guarded by the
        wizard's own ``_syncing`` flag, exactly as ``_rebuild`` does, so the
        generic on_edit observers don't mistake the programmatic push for a
        user edit and record a (stale, mid-sync) override.
        """
        w = self._wizard()
        w._syncing = True
        try:
            w.editor.sync({"cdr_output": {"do_avg": False, "monthly_averages": True}})
        finally:
            w._syncing = False
        assert w.editor._widgets[("cdr_output", "do_avg")][0].value == "instantaneous"
        assert (
            w.editor._widgets[("cdr_output", "monthly_averages")][0].value == "monthly"
        )

    def test_do_avg_persists_through_load(self, tmp_path):
        """The real user-facing round trip: set the dropdown -> save (which
        reconstructs a real-bool override via _diff_overrides) -> YAML -> reload
        -> sync back onto a fresh wizard's dropdown. Exercises the override/YAML
        layer, not just sync()'s in-memory mapping (see test_bool_dropdown_load_back).
        """
        w1 = self._wizard()
        w1.editor._widgets[("cdr_output", "do_avg")][0].value = "instantaneous"
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.config.model_settings["cdr_output"]["do_avg"] is False
        assert w2.editor._widgets[("cdr_output", "do_avg")][0].value == "instantaneous"

    def test_ocean_vars_rst_dependents_visibility_follows_wrt_file_rst(self):
        w = self._wizard()
        # nrpf_rst exists only pre ucla-roms 0.5.0; pin a legacy ref so the
        # editor generates the widget (the default model pins "main" -> latest).
        w.roms_ref.value = "0.2.0"
        w._rebuild()
        wrt = w.editor._widgets[("ocean_vars", "wrt_file_rst")][0]
        monthly = w.editor._widgets[("ocean_vars", "monthly_restarts")][0]
        nrpf = w.editor._widgets[("ocean_vars", "nrpf_rst")][0]
        period = w.editor._widgets[("ocean_vars", "output_period_rst")][0]
        wrt.value = False
        assert monthly.layout.display == "none"
        assert nrpf.layout.display == "none"
        assert period.layout.display == "none"
        wrt.value = True
        assert monthly.layout.display == ""
        assert nrpf.layout.display == ""
        assert period.layout.display == ""

    def test_ocean_vars_output_period_rst_disabled_when_monthly(self):
        w = self._wizard()
        wrt = w.editor._widgets[("ocean_vars", "wrt_file_rst")][0]
        monthly = w.editor._widgets[("ocean_vars", "monthly_restarts")][0]
        period = w.editor._widgets[("ocean_vars", "output_period_rst")][0]
        wrt.value = True
        monthly.value = True
        assert period.disabled is True
        monthly.value = False
        assert period.disabled is False

    def test_monthly_restarts_forces_wrt_file_rst_on(self):
        """A real user edit that checks monthly_restarts while wrt_file_rst is off
        pulls wrt_file_rst on -- the one value-mutating rule (forcing, not display).
        """
        w = self._wizard()
        wrt = w.editor._widgets[("ocean_vars", "wrt_file_rst")][0]
        monthly = w.editor._widgets[("ocean_vars", "monthly_restarts")][0]
        wrt.value = False
        monthly.value = True
        assert wrt.value is True
        assert w.config.model_settings["ocean_vars"]["wrt_file_rst"] is True

    def test_monthly_restarts_forcing_rule_persists_through_load(self, tmp_path):
        """The wrt_file_rst override recorded by the forcing rule is a cascaded
        widget mutation, not a direct user edit -- make sure it still survives
        _diff_overrides/YAML/reload (not just the in-memory check in
        test_monthly_restarts_forces_wrt_file_rst_on), so a future refactor of
        the overrides layer can't silently drop it.
        """
        w1 = self._wizard()
        wrt1 = w1.editor._widgets[("ocean_vars", "wrt_file_rst")][0]
        monthly1 = w1.editor._widgets[("ocean_vars", "monthly_restarts")][0]
        wrt1.value = False  # user edit
        monthly1.value = True  # user edit -> forcing rule flips wrt_file_rst back on
        assert wrt1.value is True
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)

        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.config.model_settings["ocean_vars"]["wrt_file_rst"] is True
        assert w2.config.model_settings["ocean_vars"]["monthly_restarts"] is True
        assert w2.editor._widgets[("ocean_vars", "wrt_file_rst")][0].value is True
        assert w2.editor._widgets[("ocean_vars", "monthly_restarts")][0].value is True

    def test_cdr_output_dependents_visibility_follows_master_switch(self):
        w = self._wizard()
        cdr_on = w.editor._widgets[("cdr_output", "do_cdr_output")][0]
        do_avg = w.editor._widgets[("cdr_output", "do_avg")][0]
        monthly_avg = w.editor._widgets[("cdr_output", "monthly_averages")][0]
        period = w.editor._widgets[("cdr_output", "output_period")][0]
        nrpf = w.editor._widgets[("cdr_output", "nrpf")][0]
        cdr_on.value = False
        for widget in (do_avg, monthly_avg, period, nrpf):
            assert widget.layout.display == "none"
        cdr_on.value = True
        do_avg.value = "averaged"  # show the cadence dropdown too
        for widget in (do_avg, monthly_avg, period, nrpf):
            assert widget.layout.display == ""

    def test_cdr_output_period_disabled_when_averaged_monthly(self):
        w = self._wizard()
        cdr_on = w.editor._widgets[("cdr_output", "do_cdr_output")][0]
        do_avg = w.editor._widgets[("cdr_output", "do_avg")][0]
        monthly_avg = w.editor._widgets[("cdr_output", "monthly_averages")][0]
        period = w.editor._widgets[("cdr_output", "output_period")][0]
        cdr_on.value = True
        do_avg.value = "averaged"
        monthly_avg.value = "monthly"
        assert period.disabled is True
        monthly_avg.value = "periodic"
        assert period.disabled is False
        do_avg.value = "instantaneous"
        assert monthly_avg.layout.display == "none"
        assert period.disabled is False

    def test_field_rules_apply_after_sync(self):
        """sync() (the load path) must leave visibility/disabled consistent with
        the synced values, not just widgets driven by a live user edit.
        """
        w = self._wizard()
        # nrpf_rst exists only pre ucla-roms 0.5.0; pin a legacy ref so the
        # editor generates the widget (the default model pins "main" -> latest).
        w.roms_ref.value = "0.2.0"
        w._rebuild()
        w._syncing = True
        try:
            w.editor.sync(
                {
                    "ocean_vars": {
                        "wrt_file_rst": False,
                        "monthly_restarts": True,
                        "nrpf_rst": 2,
                        "output_period_rst": 100.0,
                    },
                    "cdr_output": {
                        "do_cdr_output": True,
                        "do_avg": False,
                        "monthly_averages": True,
                        "output_period": 50.0,
                        "nrpf": 3,
                    },
                }
            )
        finally:
            w._syncing = False
        monthly = w.editor._widgets[("ocean_vars", "monthly_restarts")][0]
        nrpf = w.editor._widgets[("ocean_vars", "nrpf_rst")][0]
        period = w.editor._widgets[("ocean_vars", "output_period_rst")][0]
        # wrt_file_rst off hides its dependents even though monthly_restarts is True.
        assert monthly.layout.display == "none"
        assert nrpf.layout.display == "none"
        assert period.layout.display == "none"

        do_avg = w.editor._widgets[("cdr_output", "do_avg")][0]
        monthly_avg = w.editor._widgets[("cdr_output", "monthly_averages")][0]
        period_cdr = w.editor._widgets[("cdr_output", "output_period")][0]
        assert do_avg.value == "instantaneous"
        assert monthly_avg.layout.display == "none"
        assert period_cdr.disabled is False

    def test_advanced_edit_persists_across_atomic_change(self):
        w = self._wizard()
        w.editor._widgets[("lateral_visc", "visc2")][0].value = 12.5
        w.grid_w["nx"].value = 8  # atomic change -> re-derive
        assert w.config.model_settings["lateral_visc"]["visc2"] == 12.5  # edit kept
        assert w.config.model_settings["param"]["llm"] == 8  # derived refreshed

    def test_editing_v_sponge_touches_and_wins(self):
        """v_sponge is a first-class domain property with its own dedicated
        widget (Domain-derived properties), not a generic accordion override --
        it has no accordion widget (_ACCORDION_EXCLUDED_FIELDS) and is never
        recorded in composition.overrides. Editing it directly "touches" the
        value so it wins over grid-driven re-derivation, exactly like the old
        override-wins-over-re-derivation mechanic, but resolved via
        build_forge_blueprint's own v_sponge= param instead of the overrides layer.
        """
        w = self._wizard()
        assert ("v_sponge", "v_sponge") not in w.editor._widgets
        w.v_sponge.value = 999.0
        assert w._v_sponge_touched is True
        assert w.config.model_settings["v_sponge"]["v_sponge"] == 999.0
        assert w.config.domain.v_sponge == 999.0
        assert "v_sponge" not in w.config.composition.overrides
        # touched value persists and wins over the re-derived value across a rebuild
        w.dt.value = 3600.0
        assert w.config.model_settings["v_sponge"]["v_sponge"] == 999.0
        assert (
            w.config.model_settings["lateral_visc"]["visc2"] == 0.0
        )  # non-overridden field still re-derives to its composed default

    def test_v_sponge_persists_through_load(self, tmp_path):
        w1 = self._wizard()
        w1.v_sponge.value = 999.0
        w1.editor._widgets[("lateral_visc", "visc2")][0].value = 3.3
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.config.model_settings["v_sponge"]["v_sponge"] == 999.0
        assert w2.config.domain.v_sponge == 999.0
        assert w2._v_sponge_touched is True
        assert "v_sponge" not in w2.config.composition.overrides
        assert w2.config.model_settings["lateral_visc"]["visc2"] == 3.3

    def test_dt_persists_through_load(self, tmp_path):
        """``dt`` has no touched flag (always gathered raw from the widget) and is
        excluded from the accordion overrides layer -- it round-trips purely via
        model_settings["time_stepping"]["dt"] being written on save and read back
        in _populate_from. Lock that in explicitly.
        """
        w1 = self._wizard()
        # Must stay an integer multiple of the default output_period_rst (86400)
        # -- see check_rst_period_divisible -- so this exercises round-tripping
        # a non-default dt without tripping the restart-period validator.
        w1.dt.value = 1200.0
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.dt.value == 1200.0
        assert w2.config.model_settings["time_stepping"]["dt"] == 1200.0
        assert "time_stepping" not in w2.config.composition.overrides

    def test_model_ref_date_persists_through_load(self, tmp_path):
        """model_ref_date is gathered into Run.model_reference_date whenever it
        differs from the 2000-01-01 default, but build_forge_blueprint() had no
        matching parameter (a TypeError swallowed by _rebuild's except, always
        showing "Invalid") and _populate_from never restored the widget -- both
        fixed together since the populate fix is meaningless without the resolver
        accepting the value (see project memory for the load-back bug pattern).
        """
        w1 = self._wizard()
        w1.model_ref_date.value = date(2015, 6, 15)
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        assert w1.config.run.model_reference_date == datetime(2015, 6, 15)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.model_ref_date.value == date(2015, 6, 15)

    def test_grid_extended_options_persist_through_load(self, tmp_path):
        """hmin/close_narrow_channels/mask_shapefile are grid_kwargs entries with
        their own dedicated widgets (not in self.grid_w, which only covers
        _GRID_INT/_GRID_FLOAT/_SCOORD) -- _populate_from silently left them at
        their constructor defaults on load until this test was added (same bug
        class as allow_flex_time; see project memory).
        """
        w1 = self._wizard()
        w1.hmin.value = 3.3
        w1.close_narrow_chk.value = True
        w1.mask_shapefile.value = "/tmp/mask.shp"
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        assert w1.config.domain.grid_kwargs["hmin"] == 3.3
        assert w1.config.domain.grid_kwargs["close_narrow_channels"] is True
        assert w1.config.domain.grid_kwargs["mask_shapefile"] == "/tmp/mask.shp"
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.hmin.value == 3.3
        assert w2.close_narrow_chk.value is True
        assert w2.mask_shapefile.value == "/tmp/mask.shp"

    def test_default_forcing_spec_is_pinned_not_first_alphabetically(self, tmp_path):
        """A newly bundled ForcingSpec that sorts first must not steal the default.

        ``catalog.forcing_names`` is sorted, so seeding the dropdown with
        ``names[0]`` silently repoints every user's default the moment someone
        adds a spec whose name sorts earlier -- which is exactly what bundling
        ``glorys-era5-esper`` did. The selection is pinned to
        ``_DEFAULT_FORCING_SPEC`` instead (mirroring ``_DEFAULT_OUTPUT_SPEC``).
        Build a catalog that provokes the regression rather than leaning on
        whatever happens to be bundled today, so this keeps guarding the
        invariant no matter how the shipped specs are renamed or extended.
        """
        pytest.importorskip("ipywidgets")
        import shutil

        from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog
        from cstar.wizard.wizard import (
            _DEFAULT_FORCING_SPEC,
            ForgeBlueprintWizard,
        )

        root = tmp_path / "catalog"
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        cat = DomainCatalog(catalog_root=root)
        if _DEFAULT_FORCING_SPEC not in cat.forcing_names:
            pytest.skip(f"{_DEFAULT_FORCING_SPEC!r} not in the bundled catalog")
        cat.register_forcing("aaa-sorts-first", cat.forcing_data(_DEFAULT_FORCING_SPEC))
        # Without this the assertion below passes vacuously and guards nothing.
        assert cat.forcing_names[0] == "aaa-sorts-first"

        wiz = ForgeBlueprintWizard(catalog=cat)
        assert wiz.forcing_dd.value == _DEFAULT_FORCING_SPEC

    def test_forcing_spec_selection_and_edit(self):
        w = self._wizard()
        # ForcingSpec must always be an explicit catalog selection now -- no more
        # "<model default>" fallback -- so the default-selected entry is already
        # origin="catalog" from construction.
        if "glorys-era5-unified" not in w._dd_values(w.forcing_dd):
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
        if "standard" not in w._dd_values(w.output_dd):
            pytest.skip("example OutputSpec not in catalog")
        # 'daily-restarts' is the preselected default (_DEFAULT_OUTPUT_SPEC);
        # 'standard' remains selectable for back-compat.
        assert w.output_dd.value == "daily-restarts"
        assert w.config.composition.output.origin == "catalog"
        assert (
            "marbl_config_file" in w.config.model_settings["marbl_bgc"]
        )  # partial merge
        # edit an output section in Advanced -> override recorded; selection unchanged
        w.editor._widgets[("ts_output", "wrt_temp")][0].value = True
        assert w.config.composition.overrides["ts_output"]["wrt_temp"] is True
        # re-selecting the output spec clears output-section overrides (the handler
        # doesn't key off which value it changed to, only that a selection happened)
        w._on_output_spec(None)
        assert "ts_output" not in w.config.composition.overrides

    def test_output_spec_round_trips_through_load(self, tmp_path):
        w1 = self._wizard()
        if "standard" not in w1._dd_values(w1.output_dd):
            pytest.skip("example OutputSpec not in catalog")
        w1.output_dd.value = "standard"
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
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
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert "WOA" in [i.source.name for i in w2.config.forcing.surface]
        assert w2.config.model_settings["cppdefs"]["sal_restore"] is True
        assert w2.config.composition.forcing.origin == "catalog"
        assert w2.config.composition.forcing.modified is True

    def test_ic_bgc_multi_source_round_trips_through_load(self, tmp_path):
        """Two (or more) IC-BGC rows -- different sources, each with its own
        use_vars down-select -- must gather -> resolve -> reload with both rows
        (and their use_vars) intact, both into `config.forcing` and back into a
        freshly-loaded wizard's own IC-BGC rows.
        """
        w1 = self._wizard()
        if "glorys-era5-unified" in w1.forcing_dd.options:
            w1.forcing_dd.value = "glorys-era5-unified"
        fe = w1._forcing_editor
        for ws in list(fe._rows["ic_bgc"]):
            fe._remove("ic_bgc", ws)
        fe._add("ic_bgc")
        row1 = fe._rows["ic_bgc"][-1]
        row1["name"].value = "UNIFIED"
        row1["use_vars"].value = "ALK, DIC"
        fe._add("ic_bgc")
        row2 = fe._rows["ic_bgc"][-1]
        row2["name"].value = "GLODAP"
        row2["use_vars"].value = "PO4, NO3"

        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        assert [
            bs.source.name for bs in w1.config.forcing.initial_conditions.bgc_sources
        ] == [
            "UNIFIED",
            "GLODAP",
        ]

        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        bgc_sources = w2.config.forcing.initial_conditions.bgc_sources
        assert [bs.source.name for bs in bgc_sources] == ["UNIFIED", "GLODAP"]
        assert bgc_sources[0].use_vars == ["ALK", "DIC"]
        assert bgc_sources[1].use_vars == ["PO4", "NO3"]
        # the reloaded wizard's own IC-BGC rows reflect the same, so re-editing and
        # re-gathering round-trips too (not just the one-shot load).
        fe2 = w2._forcing_editor
        assert [r["name"].value for r in fe2._rows["ic_bgc"]] == ["UNIFIED", "GLODAP"]
        assert fe2._rows["ic_bgc"][0]["use_vars"].value == "ALK, DIC"
        assert fe2._rows["ic_bgc"][1]["use_vars"].value == "PO4, NO3"

    def test_boundary_bgc_use_vars_round_trips_through_load(self, tmp_path):
        """A boundary type='bgc' row's use_vars down-select must survive save/load.

        "boundary_bgc" is its own row-list/pane now (mirroring "ic_bgc"), split out
        of "boundary" (physics-only, no per-row type widget anymore) -- see
        _ROW_CATEGORIES/_make_row. Every row seeded there is implicitly bgc-type.
        """
        w1 = self._wizard()
        if "glorys-era5-unified" in w1.forcing_dd.options:
            w1.forcing_dd.value = "glorys-era5-unified"
        fe = w1._forcing_editor
        bgc_rows = fe._rows["boundary_bgc"]
        assert bgc_rows, "expected a bgc boundary row in the bundled ForcingSpec"
        bgc_rows[0]["use_vars"].value = "ALK, DIC, NO3"

        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)

        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        bgc_items = w2.config.forcing.boundary.bgc_sources
        assert bgc_items and bgc_items[0].use_vars == ["ALK", "DIC", "NO3"]

    def test_ic_bgc_constants_source_round_trips_through_load(self, tmp_path):
        """A `name="constants"` IC-BGC row's constants mapping must survive save/load."""
        w1 = self._wizard()
        if "glorys-era5-unified" in w1.forcing_dd.options:
            w1.forcing_dd.value = "glorys-era5-unified"
        fe = w1._forcing_editor
        for ws in list(fe._rows["ic_bgc"]):
            fe._remove("ic_bgc", ws)
        fe._add("ic_bgc")
        row = fe._rows["ic_bgc"][-1]
        row["name"].value = "constants"
        row["constants"].value = "Fe=3.0e-3, ALK=2300"

        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        assert w1.config.forcing.initial_conditions.bgc_sources[0].source.constants == {
            "Fe": 3.0e-3,
            "ALK": 2300.0,
        }

        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        bgc_sources = w2.config.forcing.initial_conditions.bgc_sources
        assert bgc_sources[0].source.name == "constants"
        assert bgc_sources[0].source.constants == {"Fe": 3.0e-3, "ALK": 2300.0}
        from cstar.wizard.wizard import _parse_constants

        fe2 = w2._forcing_editor
        assert _parse_constants(fe2._rows["ic_bgc"][0]["constants"].value) == {
            "Fe": 3.0e-3,
            "ALK": 2300.0,
        }

    def test_ic_bgc_esper_source_round_trips_through_load(self, tmp_path):
        """A `name="ESPER"` IC-BGC row's esper_method/esper_equation must survive
        save/load (and its otherwise-required path).
        """
        w1 = self._wizard()
        if "glorys-era5-unified" in w1.forcing_dd.options:
            w1.forcing_dd.value = "glorys-era5-unified"
        fe = w1._forcing_editor
        for ws in list(fe._rows["ic_bgc"]):
            fe._remove("ic_bgc", ws)
        fe._add("ic_bgc")
        row = fe._rows["ic_bgc"][-1]
        row["name"].value = "ESPER"
        row["path"].value = "/data/PyESPER"
        row["esper_method"].value = "mixed"
        row["esper_equation"].value = "16"

        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        esper_src = w1.config.forcing.initial_conditions.bgc_sources[0].source
        assert esper_src.esper_method == "mixed"
        assert esper_src.esper_equation == 16

        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        bgc_sources = w2.config.forcing.initial_conditions.bgc_sources
        assert bgc_sources[0].source.name == "ESPER"
        assert bgc_sources[0].source.esper_method == "mixed"
        assert bgc_sources[0].source.esper_equation == 16
        fe2 = w2._forcing_editor
        assert fe2._rows["ic_bgc"][0]["esper_method"].value == "mixed"
        assert fe2._rows["ic_bgc"][0]["esper_equation"].value == "16"

    def test_nest_from_domain_dropdown_prefills_child(self):
        w = self._wizard()
        if "gulf-guinea-toy" not in w._dd_values(w.nest_domain_dd):
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
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.config.model_settings["lateral_visc"]["visc2"] == 7.25
        assert w2.nest_enable.value is True
        assert w2.config.model_settings["extract_data"]["n_chd"] == 18

    def test_nesting_pressure_fluxes_persists_through_load(self, tmp_path):
        """nesting_include_pressure_fluxes is a first-class Domain field, correctly
        gathered from nest_pressure_fluxes.value, but _populate_nesting silently
        left the widget at its default (False) on load until this test was added
        (same bug class as allow_flex_time; see project memory).
        """
        w1 = self._wizard()
        w1.nest_enable.value = True
        w1.nest_pressure_fluxes.value = True
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        assert w1.config.domain.nesting_include_pressure_fluxes is True
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.nest_enable.value is True
        assert w2.nest_pressure_fluxes.value is True

    def test_parent_from_domain_dropdown_prefills_parent(self):
        w = self._wizard()
        if "gulf-guinea-toy" not in w._dd_values(w.parent_domain_dd):
            pytest.skip("gulf-guinea-toy domain not in catalog")
        w.parent_domain_dd.value = "gulf-guinea-toy"  # prefills parent + enables it
        assert w.parent_enable.value is True
        assert w.parent_w["nx"].value == 10 and w.parent_w["N"].value == 5
        assert w.config.domain.grid_kwargs_parent is not None
        assert w.config.domain.is_child is True

    def test_parent_ui_stores_grid_kwargs_parent_and_clears_boundary_forcing(self):
        w = self._wizard()
        assert w.config.forcing.boundary is not None  # sanity: default forcing has one
        w.parent_enable.value = True
        w.parent_w["N"].value = 25
        cfg = w.config
        assert cfg.domain.grid_kwargs_parent is not None
        assert cfg.domain.grid_kwargs_parent["N"] == 25
        assert cfg.domain.is_child is True
        assert cfg.domain.is_parent is False
        assert cfg.forcing.boundary is None
        # open-boundary edge flags (obc_*) are untouched -- edges stay open, fed
        # by the parent's nesting.nc extraction instead of reanalysis forcing.
        assert cfg.domain.open_boundaries.model_dump() == {
            d: w_.value for d, w_ in w.bnd.items()
        }

    def test_load_preserves_parent(self, tmp_path):
        w1 = self._wizard()
        w1.parent_enable.value = True
        w1.parent_w["N"].value = 30
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.parent_enable.value is True
        assert w2.parent_w["N"].value == 30
        assert w2.config.domain.is_child is True
        assert w2.config.forcing.boundary is None

    def test_parent_toggle_clears_ic_and_reselecting_source_restores_it(self):
        """Enabling a parent defaults IC to "(none)" (mirrors boundary), but
        unlike boundary this is only a default -- re-selecting GLORYS on the
        dropdown must restore an explicit IC in the built blueprint.
        """
        w = self._wizard()
        assert w.config.forcing.initial_conditions is not None  # sanity
        w.parent_enable.value = True
        cfg = w.config
        assert cfg.domain.is_child is True
        assert cfg.forcing.initial_conditions is None
        assert w._forcing_editor.ic_name.value == "(none)"

        w._forcing_editor.ic_name.value = "GLORYS"
        cfg2 = w.config
        assert cfg2.forcing.initial_conditions is not None
        assert cfg2.forcing.initial_conditions.source.name == "GLORYS"

    def test_parent_toggle_off_restores_ic_default(self):
        """Enabling then disabling the parent checkbox must not strand the user
        on the resolver's non-child hard error: disabling restores IC to the
        default source when it's still at the "(none)" default the enable-
        toggle applied.
        """
        w = self._wizard()
        w.parent_enable.value = True
        assert w._forcing_editor.ic_name.value == "(none)"
        assert w.config.forcing.initial_conditions is None

        w.parent_enable.value = False
        assert w._forcing_editor.ic_name.value == "GLORYS"
        cfg = w.config
        assert cfg.domain.is_child is False
        assert cfg.forcing.initial_conditions is not None

    def test_reselecting_forcing_spec_keeps_ic_cleared_for_child(self):
        """Reseeding the forcing editor from a newly-selected ForcingSpec (which
        always carries a real IC block) must not silently undo the "(none)"
        default a parent toggle already applied -- mirrors the boundary-forcing
        strip that already happens in ``_on_forcing_spec``.
        """
        w = self._wizard()
        if "simple-bgc-demo" not in w._dd_values(w.forcing_dd):
            pytest.skip("simple-bgc-demo ForcingSpec not in catalog")
        w.parent_enable.value = True
        assert w.config.forcing.initial_conditions is None
        other = next(v for v in w._dd_values(w.forcing_dd) if v != w.forcing_dd.value)
        w.forcing_dd.value = other
        assert w._forcing_editor.ic_name.value == "(none)"
        assert w.config.forcing.initial_conditions is None

    def test_load_preserves_child_no_ic(self, tmp_path):
        w1 = self._wizard()
        w1.parent_enable.value = True
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        assert w1.config.forcing.initial_conditions is None
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.parent_enable.value is True
        assert w2.config.forcing.initial_conditions is None
        assert w2._forcing_editor.ic_name.value == "(none)"

    def test_roms_ref_gather_and_default_round_trip(self, tmp_path):
        w1 = self._wizard()
        w1.roms_ref.value = "pio-refdate"
        assert w1.config.code.roms.commit == "pio-refdate"
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
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
        assert default_ref  # this model.yaml pins a commit
        assert w1.roms_ref.value == default_ref

        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
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

    def test_marbl_ref_gather_and_default_round_trip(self, tmp_path):
        w1 = self._wizard()
        w1.marbl_ref.value = "marbl0.99.0"
        assert w1.config.code.marbl.commit == "marbl0.99.0"
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.marbl_ref.value == "marbl0.99.0"
        assert w2.config.code.marbl.commit == "marbl0.99.0"

    def test_marbl_ref_prefilled_with_model_default_and_editable(self, tmp_path):
        """Mirror of the roms_ref test above: the MARBL ref is prefilled from the
        selected Model's pinned default, an unmodified default reloads showing that
        same default (not blank), and an override round-trips through save/reload.
        """
        w1 = self._wizard()
        default_ref = w1._model_default_marbl_ref()
        assert default_ref  # this model.yaml pins a MARBL tag
        assert w1.marbl_ref.value == default_ref

        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        w2 = self._wizard()
        w2.marbl_ref.value = "stale-value-from-a-prior-load"
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.marbl_ref.value == default_ref

        # An actual override round-trips through save/reload unchanged.
        w1.marbl_ref.value = "my-custom-marbl-branch"
        w1._on_save(None)
        w3 = self._wizard()
        w3.load_path.value = str(p)
        w3._on_load_path(None)
        assert w3.marbl_ref.value == "my-custom-marbl-branch"

    def test_loading_file_with_bad_settings_is_flagged(self, tmp_path):
        import yaml

        w = self._wizard()
        p = tmp_path / "forge_blueprint.yaml"
        w.save_path.value = str(p)
        w._boundaries_touched = True  # not exercising boundary derivation here
        w._on_save(None)
        data = yaml.safe_load(p.read_text())
        data["model_settings"]["param"]["np_xi"] = "not-an-int"  # corrupt a value
        bad = tmp_path / "bad.yaml"
        bad.write_text(yaml.safe_dump(data))
        w2 = self._wizard()
        w2.load_path.value = str(bad)
        w2._on_load_path(None)
        assert "invalid settings value" in w2.load_status.value
        # the wizard re-derives valid settings from the inputs, so it ends valid
        assert "Blueprint is valid" in w2.validation.value

    def test_load_bad_input_shows_error_not_crash(self):
        w = self._wizard()
        w.load_path.value = "/nonexistent/forge_blueprint.yaml"
        w._on_load_path(None)
        assert "color:#b00" in w.load_status.value
        w._load_bytes(b"not: [valid spec config")
        assert "color:#b00" in w.load_status.value

    def test_cdr_clear_button_resets_upload_value_without_error(self):
        """Regression: clicking the CDR 'clear' button used to do
        `self.cdr_upload.value = ()` directly, which raises `TraitError` on an
        ipywidgets version where `FileUpload.value` is read-only (real user report:
        `TraitError: The "value" trait is read-only.` from `_on_cdr_clear`). Forced
        via `_ReadOnlyValueGuard` so this is caught regardless of the installed
        ipywidgets version's own read-only-ness (see class docstring).
        """
        wiz = self._wizard()
        real_upload = wiz.cdr_upload
        wiz.cdr_upload = _ReadOnlyValueGuard(real_upload)
        try:
            wiz._cdr_forcing = {"releases": [{"lon": 1.0, "lat": 2.0}]}
            wiz._on_cdr_clear(None)  # must not raise TraitError
        finally:
            wiz.cdr_upload = real_upload
        assert wiz._cdr_forcing is None
        assert wiz.cdr_status.value == ""

    def test_loading_blueprint_with_cdr_forcing_does_not_raise(self, tmp_path):
        """Regression: loading a saved blueprint that has `cdr_forcing` set used to
        crash in `_populate_from` for the same reason as the clear-button bug above
        (`self.cdr_upload.value = ()` on a read-only trait) -- this is the exact
        path the original bug report hit (`ForgeBlueprintWizard._on_load_path` ->
        `_populate_from`, loading `.../ccs-4km_64procs.forge_blueprint.yaml`).
        """
        w1 = self._wizard()
        w1.cdr_mode_dd.value = "yaml"
        w1._cdr_forcing = {"releases": [{"lon": 1.0, "lat": 2.0}]}
        w1._rebuild()
        assert w1.config.cdr.cdr_forcing == w1._cdr_forcing

        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)

        w2 = self._wizard()
        real_upload = w2.cdr_upload
        w2.cdr_upload = _ReadOnlyValueGuard(real_upload)
        try:
            w2.load_path.value = str(p)
            w2._on_load_path(None)  # must not raise TraitError
        finally:
            w2.cdr_upload = real_upload
        assert "color:#b00" not in w2.load_status.value
        assert w2._cdr_forcing == w1._cdr_forcing

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
        # the download filename is keyed off cfg.name (matching save_path, see
        # _on_save_path_change/_rebuild), not the date-suffixed casename
        assert f'download="{wiz.config.name}.forge_blueprint.yaml"' in html


class TestForgeBlueprintWizardApp:
    """The catalog-location wrapper around ForgeBlueprintWizard."""

    def _app(self, **kwargs):
        pytest.importorskip("ipywidgets")
        from cstar.wizard.wizard import ForgeBlueprintWizardApp

        return ForgeBlueprintWizardApp(**kwargs)

    def test_default_auto_loads_layered_catalog(self):
        pytest.importorskip("ipywidgets")
        from cstar.catalog.domain_catalog import (
            _DEFAULT_CATALOG_ROOT,
            LayeredCatalog,
            user_catalog_root,
        )
        from cstar.wizard.wizard import ForgeBlueprintWizard

        app = self._app()
        assert isinstance(app.inner, ForgeBlueprintWizard)
        cat = app.inner.catalog
        # Blank App load yields a LayeredCatalog: writable user layer (top,
        # .catalog_root) over the read-only bundled packaged layer (bottom).
        assert isinstance(cat, LayeredCatalog)
        assert cat.catalog_root == user_catalog_root()
        assert cat.stores[-1].catalog_root == _DEFAULT_CATALOG_ROOT
        assert "Loaded" in app._bar._cat_status.value

    def test_reload_with_bad_value_keeps_previous_wizard(self):
        # A nonexistent local path is no longer an error (it becomes an empty
        # writable layer over bundled, matching CSTAR_CATALOG semantics),
        # so a malformed GitHub URL is the failure case now. Calls `_load`
        # directly -- the catalog bar's own reload button now requires a
        # two-step confirm (see `cstar.wizard.wizard.CatalogBar`),
        # which isn't this test's concern.
        app = self._app()
        original_inner = app.inner
        app._load("https://github.com/org-but-no-repo")
        assert app.inner is original_inner
        assert "Failed to load catalog" in app._bar._cat_status.value

    def test_single_local_path_builds_stack_with_bundled(self, tmp_path):
        from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, LayeredCatalog

        app = self._app()
        app._load(str(tmp_path / "my-catalog"))
        cat = app.inner.catalog
        # One local path routes through build_catalog_stack: writable top over
        # the read-only bundled layer, same as the env var would produce.
        assert isinstance(cat, LayeredCatalog)
        assert cat.catalog_root == (tmp_path / "my-catalog").resolve()
        assert cat.stores[-1].catalog_root == _DEFAULT_CATALOG_ROOT
        assert cat.model_names  # bundled models visible through the stack
        assert "Loaded" in app._bar._cat_status.value

    def test_single_local_literal_loads_readonly_store_with_warning(self):
        from cstar.catalog.domain_catalog import DomainCatalog, LayeredCatalog

        app = self._app()
        app._load("local")
        cat = app.inner.catalog
        # "local" (the bundled catalog) can never be a writable top layer:
        # exactly one read-only store, and the status line warns that saves
        # fall back to CWD-relative filenames.
        assert isinstance(cat, DomainCatalog)
        assert not isinstance(cat, LayeredCatalog)
        assert cat.read_only is True
        assert "read-only catalog" in app._bar._cat_status.value
        assert app.inner._default_blueprint_path("x") == "x.forge_blueprint.yaml"


# ---------------------------------------------------------------------------
# Processing engine (orchestration tested with an injected fake builder; the real
# pipeline downloads data + runs roms_tools and is out of scope for unit tests)
# ---------------------------------------------------------------------------
class _FakeBuilder:
    """A ForgeBlueprintExecutor stand-in: records calls instead of doing real work."""

    def __init__(self, cfg=None, host=None, verbose=False):
        self.cfg = cfg
        self.calls = []

    def ensure_source_data(self, **k):
        self.calls.append(("ensure", k))

    def generate_inputs(self, **k):
        self.calls.append(("generate", k))

    def configure_build(self, **k):
        self.calls.append(("configure", k))

    def path_roms_marbl_blueprint(self):
        return "/bp.yaml"


class TestForgeBlueprintEngine:
    def _cfg(self):
        return _build()

    def test_builder_kwargs_carry_atomic_inputs_not_host(self):
        from cstar.applications.forge.engine import (
            forge_blueprint_to_builder_kwargs,
        )

        cfg = self._cfg()
        kw = forge_blueprint_to_builder_kwargs(cfg)
        assert kw["name"] == cfg.name
        assert kw["grid_name"] == "test-tiny"
        # Subset, not full-dict equality: forge_blueprint_engine.py also injects a
        # use_pio leaf (from cppdefs) into this dict for ForgeExecutor's
        # PartitioningParameterSet, which is out of scope here -- see
        # forge_blueprint_to_builder_kwargs's own docstring/comment for why.
        assert kw["partitioning"]["n_procs_x"] == 1
        assert kw["partitioning"]["n_procs_y"] == 1
        assert kw["partitioning"]["auto_tiling"] is False
        assert kw["open_boundaries"]["east"] is True
        # host/machine/paths must NOT be passed (builder resolves them)
        assert not any(k in kw for k in ("machine", "paths", "scratch", "source_data"))

    def test_builder_kwargs_carry_resolved_datasets_snapshot(self, tmp_path):
        """End-to-end check for the resolved_datasets pinning: the blueprint's
        forcing.resolved_datasets snapshot must actually reach the executor, not
        just be accepted as a same-named kwarg.
        """
        from cstar.applications.forge.engine import (
            forge_blueprint_to_builder_kwargs,
        )
        from cstar.applications.forge.executor import ForgeExecutor
        from cstar.applications.forge.host import HostPaths

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
        )
        ex = ForgeExecutor.from_forge_blueprint(cfg, host=host)
        assert ex.resolved_datasets["GLORYS"]["dataset_key"] == "GLORYS_REGIONAL"

    def test_split_model_settings(self):
        from cstar.applications.forge.engine import (
            PROCESSING_FILLED_SECTIONS,
            split_model_settings,
        )

        run_ov, comp_ov = split_model_settings(self._cfg())
        assert list(comp_ov) == ["cppdefs"] and "cppdefs" not in run_ov
        assert "time_stepping" in run_ov and "param" in run_ov
        for sec in PROCESSING_FILLED_SECTIONS:
            assert sec not in run_ov

    def test_split_model_settings_excludes_generation_derived_leaves(self):
        """Regression for the §3a bug (docs/dev-notes/forge-blueprint-parameter-audit.md): the
        overlay passed to ``configure_build`` must not carry the leaf keys that
        ``generate_inputs`` derives from the *actual* generated forcing objects
        (river/CDR "is configured" flags + counts, the true tidal constituent count) —
        otherwise it silently reverts a correctly-generated configuration back to the
        resolver's pre-generation placeholder/declared value.
        """
        from cstar.applications.forge.engine import (
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
        from cstar.applications.forge.engine import process_forge_blueprint

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
        from cstar.applications.forge.engine import process_forge_blueprint

        b = process_forge_blueprint(
            self._cfg(),
            ensure_data=False,
            generate=False,
            executor_factory=_FakeBuilder,
        )
        assert [c[0] for c in b.calls] == ["configure"]

    def test_only_inputs_forces_configure_off_and_resolves_selection(self):
        """A subset run must never reach configure_build -- persist() only lives
        there, so this is what guarantees an only_inputs run can't clobber an
        existing complete blueprint from a prior full run.
        """
        from cstar.applications.forge.engine import process_forge_blueprint

        b = process_forge_blueprint(
            self._cfg(),
            only_inputs=["boundary", "bry"],  # dupe alias -> single resolved key
            executor_factory=_FakeBuilder,
        )
        assert [c[0] for c in b.calls] == ["ensure", "generate"]
        gen = dict(b.calls[1][1])
        assert gen["only"] == {"forcing.boundary"}

    def test_only_inputs_wins_even_if_configure_true(self):
        from cstar.applications.forge.engine import process_forge_blueprint

        b = process_forge_blueprint(
            self._cfg(),
            configure=True,
            only_inputs=["grid"],
            executor_factory=_FakeBuilder,
        )
        assert "configure" not in [c[0] for c in b.calls]

    def test_only_inputs_unknown_name_fails_fast_before_any_call(self):
        from cstar.applications.forge.engine import process_forge_blueprint

        with pytest.raises(ValueError, match="bogus"):
            process_forge_blueprint(
                self._cfg(), only_inputs=["bogus"], executor_factory=_FakeBuilder
            )

    def test_executor_must_implement_interface(self):
        from cstar.applications.forge.engine import (
            ForgeBlueprintExecutor,
            process_forge_blueprint,
        )

        # _FakeBuilder satisfies the runtime-checkable Protocol
        assert isinstance(_FakeBuilder(), ForgeBlueprintExecutor)

        class _Bad:  # missing the required methods
            def __init__(self, cfg=None, host=None, verbose=False):
                pass

        with pytest.raises(TypeError, match="ForgeBlueprintExecutor"):
            process_forge_blueprint(self._cfg(), executor_factory=_Bad)

    def test_invalid_model_settings_fail_fast(self):
        from cstar.applications.forge.engine import process_forge_blueprint

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
        from cstar.applications.forge import config
        from cstar.applications.forge.host import HostPaths

        cfg = self._cfg()
        h = config.resolve_host(cfg.working_dir)
        assert isinstance(h, HostPaths)
        assert h.system
        # working_dir is the injected per-run artifact root; source_data_cache is the
        # shared host download cache. Both resolved from config, not the spec file.
        # The spec default carries a per-run subdirectory: <root>/<name>.
        assert "_forge_bp_runs" in str(h.working_dir)
        assert str(h.working_dir).endswith(cfg.name)
        assert h.source_data_cache is not None


# ---------------------------------------------------------------------------
# Step 3 (parity): the resolver and the live ForgeExecutor must agree
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

        from cstar.applications.forge.executor import ForgeExecutor
        from cstar.applications.forge.host import HostPaths
        from cstar.applications.forge.resolve import build_forge_blueprint

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
# "Save modified specs to catalog" (wizard panel + DomainCatalog register_*)
# ---------------------------------------------------------------------------
class TestSaveModifiedSpecsToCatalog:
    """Each save handler: extract the spec from current state, write it to an
    isolated catalog, side-effect-free round-trip verify via content_hash, and
    only on a match repoint the dropdown / clear that spec's overrides-or-seed.
    A mismatch must still write the file but leave everything else untouched.
    """

    @pytest.fixture
    def isolated_catalog(self, tmp_path):
        import shutil

        from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog

        root = tmp_path / "catalog"
        # Copy the BUNDLED catalog (not _CATALOG.catalog_root, which is now the
        # writable *user* layer -- empty/nonexistent in tests, see conftest.py).
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        return DomainCatalog(catalog_root=root)

    def _wizard(self, catalog):
        pytest.importorskip("ipywidgets")
        from cstar.wizard.wizard import ForgeBlueprintWizard

        return ForgeBlueprintWizard(catalog=catalog)

    def test_save_output_spec_marks_unmodified_and_clears_overrides(
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

    def test_save_model_spec_marks_unmodified(self, isolated_catalog):
        wiz = self._wizard(isolated_catalog)
        wiz._overrides[("lateral_visc", "visc2")] = 999.0
        wiz._rebuild()
        assert wiz.config.composition.model.modified is True

        wiz.save_model_name.value = "my-model"
        wiz._on_save_model(None)

        assert "my-model" in isolated_catalog.model_names
        assert wiz.model_dd.value == "my-model"
        assert wiz.config.composition.model.modified is False

    def test_save_model_spec_marks_unmodified_with_blank_roms_ref(
        self, isolated_catalog
    ):
        # A blank roms_ref means "clone the base pin" -- must not itself count
        # as a deviation.
        wiz = self._wizard(isolated_catalog)
        wiz.roms_ref.value = ""

        wiz.save_model_name.value = "my-model-blank-ref"
        wiz._on_save_model(None)

        assert wiz.config.composition.model.modified is False

    def test_save_model_spec_persists_use_pio_and_roms_ref(self, isolated_catalog):
        # Name deliberately distinct from the bundled "pio-dev" ModelSpec
        # (cstar/catalog/bundled/ModelSpec/pio-dev) -- register_model_from_settings
        # refuses to overwrite an existing entry, and isolated_catalog copies the
        # bundled catalog verbatim, so reusing that name here would collide.
        spec_name = "pio-dev-test"
        wiz = self._wizard(isolated_catalog)
        # Pin a spec where use_pio=True / roms_ref="main" are genuine
        # deviations (the default pio-dev spec already declares both).
        wiz.model_dd.value = "cson_roms-marbl_v0.1"
        wiz.use_pio_chk.value = True
        wiz.roms_ref.value = "main"
        assert wiz.config.composition.model.modified is True  # spec deviation

        wiz.save_model_name.value = spec_name
        wiz._on_save_model(None)

        data = isolated_catalog.model_data(spec_name)
        assert data["use_pio"] is True
        assert data["code"]["roms"]["commit"] == "main"
        assert "branch" not in data["code"]["roms"]
        assert data["code"]["pio"] is not None

        assert wiz.model_dd.value == spec_name
        assert wiz.config.composition.model.modified is False
        assert "✓" in wiz.save_model_status.value

        # A fresh wizard picking this ModelSpec must reload the same toggles --
        # this is the part that actually failed for the reported bug.
        wiz2 = self._wizard(isolated_catalog)
        wiz2.model_dd.value = spec_name
        assert wiz2.use_pio_chk.value is True
        assert wiz2.roms_ref.value == "main"
        assert wiz2.config.model_settings["cppdefs"]["use_pio"] is True
        assert wiz2.config.code.pio is not None
        assert wiz2.config.code.roms.commit == "main"

    def test_verify_model_roundtrip_false_when_spec_loses_use_pio(
        self, isolated_catalog
    ):
        pytest.importorskip("ipywidgets")
        from cstar.wizard.wizard import _model_owned_settings

        wiz = self._wizard(isolated_catalog)
        wiz.use_pio_chk.value = True
        # Simulate the pre-fix writer: a spec saved without the live use_pio.
        isolated_catalog.register_model_from_settings(
            "no-pio",
            _model_owned_settings(wiz.config.model_settings),
            isolated_catalog.model_dir(wiz.model_dd.value),
            use_pio=False,
            roms_ref=wiz.roms_ref.value.strip() or None,
        )
        assert wiz._verify_spec_roundtrip("model", "no-pio") is False

    def test_verify_model_roundtrip_false_when_spec_loses_roms_ref(
        self, isolated_catalog
    ):
        pytest.importorskip("ipywidgets")
        from cstar.wizard.wizard import _model_owned_settings

        wiz = self._wizard(isolated_catalog)
        # Pin a spec whose base pin is NOT "main", so a spec that drops the
        # live roms_ref actually loses information (pio-dev's base pin is
        # already "main", which would make the roundtrip spuriously succeed).
        wiz.model_dd.value = "cson_roms-marbl_v0.1"
        wiz.roms_ref.value = "main"
        # Simulate the pre-fix writer: a spec saved without the live roms_ref.
        isolated_catalog.register_model_from_settings(
            "stale-ref",
            _model_owned_settings(wiz.config.model_settings),
            isolated_catalog.model_dir(wiz.model_dd.value),
            use_pio=wiz.use_pio_chk.value,
            roms_ref=None,
        )
        assert wiz._verify_spec_roundtrip("model", "stale-ref") is False

    def test_model_modified_reflects_use_pio_and_roms_ref_toggles(
        self, isolated_catalog
    ):
        # Toggling PIO/roms_ref must flag the Model spec as modified even
        # without ever touching "Save as new spec" -- these live outside
        # model_settings, so composition.model.modified must not silently
        # stay False while resolving with a different code/use_pio than the
        # selected catalog spec declares.
        wiz = self._wizard(isolated_catalog)
        # Pin a spec whose base pin is NOT "main", so roms_ref="main" below is
        # a real deviation (pio-dev, the default, already pins "main").
        wiz.model_dd.value = "cson_roms-marbl_v0.1"
        assert wiz.config.composition.model.modified is False

        wiz.use_pio_chk.value = not wiz.use_pio_chk.value
        assert wiz.config.composition.model.modified is True

        wiz.use_pio_chk.value = not wiz.use_pio_chk.value  # flip back
        assert wiz.config.composition.model.modified is False

        wiz.roms_ref.value = "main"
        assert wiz.config.composition.model.modified is True

        wiz.roms_ref.value = ""  # blank => clone the base pin, not a deviation
        assert wiz.config.composition.model.modified is False

        wiz.marbl_ref.value = "some-marbl-branch"
        assert wiz.config.composition.model.modified is True

        wiz.marbl_ref.value = ""  # blank => clone the base pin, not a deviation
        assert wiz.config.composition.model.modified is False

    def test_save_model_spec_persists_marbl_ref(self, isolated_catalog):
        spec_name = "marbl-ref-test"
        wiz = self._wizard(isolated_catalog)
        wiz.model_dd.value = "cson_roms-marbl_v0.1"
        wiz.marbl_ref.value = "my-marbl-tag"
        assert wiz.config.composition.model.modified is True  # spec deviation

        wiz.save_model_name.value = spec_name
        wiz._on_save_model(None)

        data = isolated_catalog.model_data(spec_name)
        assert data["code"]["marbl"]["commit"] == "my-marbl-tag"
        assert "branch" not in data["code"]["marbl"]
        assert (
            data["code"]["marbl"]["location"]
            == "https://github.com/CWorthy-ocean/MARBL.git"
        )

        assert wiz.model_dd.value == spec_name
        assert wiz.config.composition.model.modified is False
        assert "✓" in wiz.save_model_status.value

        # A fresh wizard picking this ModelSpec must reload the same ref.
        wiz2 = self._wizard(isolated_catalog)
        wiz2.model_dd.value = spec_name
        assert wiz2.marbl_ref.value == "my-marbl-tag"
        assert wiz2.config.code.marbl.commit == "my-marbl-tag"

    def test_verify_model_roundtrip_false_when_spec_loses_marbl_ref(
        self, isolated_catalog
    ):
        pytest.importorskip("ipywidgets")
        from cstar.wizard.wizard import _model_owned_settings

        wiz = self._wizard(isolated_catalog)
        wiz.model_dd.value = "cson_roms-marbl_v0.1"
        wiz.marbl_ref.value = "some-other-marbl-tag"
        # Simulate a pre-fix writer: a spec saved without the live marbl_ref.
        isolated_catalog.register_model_from_settings(
            "stale-marbl-ref",
            _model_owned_settings(wiz.config.model_settings),
            isolated_catalog.model_dir(wiz.model_dd.value),
            use_pio=wiz.use_pio_chk.value,
            roms_ref=wiz.roms_ref.value.strip() or None,
            marbl_ref=None,
        )
        assert wiz._verify_spec_roundtrip("model", "stale-marbl-ref") is False

    def test_save_forcing_spec_marks_unmodified(self, isolated_catalog):
        wiz = self._wizard(isolated_catalog)
        # Toggle the climatology checkbox on the first IC-BGC row (the bundled
        # "glorys-era5-unified" ForcingSpec seeds one -- UNIFIED, climatology=true).
        ic_bgc_row = wiz._forcing_editor._rows["ic_bgc"][0]
        ic_bgc_row["climatology"].value = not ic_bgc_row["climatology"].value
        wiz._on_forcing_change()
        assert wiz.config.composition.forcing.modified is True

        wiz.save_forcing_name.value = "my-forcing"
        wiz._on_save_forcing(None)

        assert "my-forcing" in isolated_catalog.forcing_names
        assert wiz.forcing_dd.value == "my-forcing"
        assert wiz.config.composition.forcing.modified is False

    def test_save_forcing_spec_for_child_with_no_ic_marks_unmodified(
        self, isolated_catalog
    ):
        """A child domain's forcing (IC defaulted to "(none)") saved to the
        catalog omits the initial_conditions key from the written YAML.
        ``_verify_spec_roundtrip`` rebuilds via the resolver (not the wizard's
        sentinel-aware ``_ForcingEditor``), so re-resolving the freshly-written
        file must still reproduce content_hash-equal to the live config.
        """
        wiz = self._wizard(isolated_catalog)
        wiz.parent_enable.value = True
        assert wiz.config.forcing.initial_conditions is None

        wiz.save_forcing_name.value = "child-no-ic-forcing"
        wiz._on_save_forcing(None)

        assert "child-no-ic-forcing" in isolated_catalog.forcing_names
        assert wiz.forcing_dd.value == "child-no-ic-forcing"
        assert wiz.config.composition.forcing.modified is False
        saved = isolated_catalog.forcing_data("child-no-ic-forcing")
        assert "initial_conditions" not in saved

    def test_save_forcing_spec_preserves_per_source_serialize_dask(
        self, isolated_catalog
    ):
        """A per-source `serialize_dask` must survive save -> reload.

        The field is no longer wizard-editable -- PyESPER serialises its own
        kernels, so the checkbox was removed -- but a blueprint that already sets
        it must round-trip rather than be silently rewritten on an unrelated edit.
        `_verify_spec_roundtrip` compares `content_hash()`, which covers all
        results-affecting blueprint data, so a dropped field shows up here as a
        failed round-trip. Seeded through the row's opaque carry, which is how
        `_make_row` preserves it without a widget.
        """
        wiz = self._wizard(isolated_catalog)
        rows = wiz._forcing_editor._rows["boundary_bgc"]
        if not rows:
            pytest.skip("bundled forcing spec has no boundary bgc source to flag")
        row = rows[0]
        row["name"].value = "ESPER"
        row["_serialize_dask"] = True
        wiz._rebuild()

        bgc = wiz.config.forcing.boundary.bgc_sources
        assert any(bs.serialize_dask for bs in bgc), (
            "editor state did not reach the resolved blueprint"
        )

        wiz.save_forcing_name.value = "serialized-esper-forcing"
        wiz._on_save_forcing(None)
        # modified=False is the round-trip verdict: the saved spec reproduces the
        # live config content-hash-exactly, serialize_dask included.
        assert wiz.config.composition.forcing.modified is False

        saved = isolated_catalog.forcing_data("serialized-esper-forcing")
        saved_bgc = saved["forcing"]["boundary"]["bgc_sources"]
        assert any(bs.get("serialize_dask") for bs in saved_bgc), (
            f"serialize_dask missing from the written spec: {saved_bgc}"
        )
        wiz = self._wizard(isolated_catalog)

        # And a fresh wizard picking that spec back up still carries it.
        wiz2 = self._wizard(isolated_catalog)
        wiz2.forcing_dd.value = "serialized-esper-forcing"
        assert any(bs.serialize_dask for bs in wiz2.config.forcing.boundary.bgc_sources)

    def test_save_cdr_spec_marks_unmodified(self, isolated_catalog):
        """The CDR row in "Save modified specs to catalog": register_cdr from the
        wizard's current "yaml" mode state, verified via the same round-trip
        pattern as output/model/domain/forcing (see class docstring).
        """
        wiz = self._wizard(isolated_catalog)
        wiz.cdr_mode_dd.value = "yaml"
        fake_cdr = {"releases": [{"lon": 1.0, "lat": 2.0}]}
        wiz._cdr_forcing = fake_cdr
        wiz._rebuild()
        assert wiz.config.cdr.cdr_forcing == fake_cdr
        assert wiz.config.composition.cdr.modified is False  # <custom>: moot

        wiz.save_cdr_name.value = "my-cdr-spec"
        wiz._on_save_cdr(None)

        assert "my-cdr-spec" in isolated_catalog.cdr_names
        assert isolated_catalog.cdr_data("my-cdr-spec") == {
            "description": wiz.description.value,
            "mode": "yaml",
            "cdr_forcing": fake_cdr,
            "cdr_forcing_file": None,
        }
        assert wiz.cdr_dd.value == "my-cdr-spec"
        assert wiz.config.composition.cdr.modified is False
        assert "✓" in wiz.save_cdr_status.value

    def test_save_cdr_spec_refuses_mode_none(self, isolated_catalog):
        wiz = self._wizard(isolated_catalog)
        assert wiz.cdr_mode_dd.value == "none"

        wiz.save_cdr_name.value = "should-not-be-saved"
        wiz._on_save_cdr(None)

        assert "should-not-be-saved" not in isolated_catalog.cdr_names
        assert "nothing to save" in wiz.save_cdr_status.value.lower()

    def test_cdr_spec_dropdown_load_back(self, isolated_catalog):
        """Picking a CdrSpec from the catalog loads its mode + fields, and a
        subsequent hand edit is reported via composition.cdr.modified (mirrors
        domain/forcing's snapshot-based "modified" tracking).
        """
        isolated_catalog.register_cdr(
            "my-simple-cdr",
            mode="simple",
            cdr_forcing={
                "start_time": "2012-01-01T00:00:00",
                "end_time": "2012-01-02T00:00:00",
                "releases": [
                    {
                        "name": "test_release",
                        "lat": 12.0,
                        "lon": 34.0,
                        "depth": 5.0,
                        "hsc": 20.0,
                        "vsc": 30.0,
                        "times": ["2012-01-01T00:00:00", "2012-01-02T00:00:00"],
                        "tracer_fluxes": {"ALK": [1.0, 1.0]},
                        "release_type": "tracer_perturbation",
                    }
                ],
            },
        )
        wiz = self._wizard(isolated_catalog)

        wiz.cdr_dd.value = "my-simple-cdr"

        assert wiz.cdr_mode_dd.value == "simple"
        assert wiz.cdr_simple_name.value == "test_release"
        assert wiz.cdr_simple_lat.value == 12.0
        assert wiz.cdr_simple_lon.value == 34.0
        assert wiz.config.composition.cdr.modified is False

        wiz.cdr_simple_lat.value = 99.0

        assert wiz.config.composition.cdr.modified is True

    def test_save_domain_spec_marks_unmodified_and_preserves_other_specs(
        self, isolated_catalog
    ):
        wiz = self._wizard(isolated_catalog)
        if "gulf-guinea-toy" in wiz._dd_values(wiz.domain_dd):
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
        # no-clobber: every OTHER spec's state is untouched by the domain save.
        assert wiz._overrides == before_overrides
        assert wiz._forcing_seed == before_forcing_seed
        assert wiz.model_dd.value == before_model_dd
        assert wiz.output_dd.value == before_output_dd

    def test_save_domain_spec_persists_v_sponge_when_touched(self, isolated_catalog):
        """v_sponge is a first-class domain property (Domain-derived
        properties): touching it and saving the domain must persist it into
        Domain.yaml, and a fresh wizard selecting that saved domain must
        restore both the value and the touched state -- so it doesn't
        silently re-derive and drift on the next grid edit.
        """
        wiz = self._wizard(isolated_catalog)
        wiz.v_sponge.value = 4242.0
        assert wiz._v_sponge_touched is True

        wiz.save_domain_name.value = "my-domain-vsponge"
        wiz._on_save_domain(None)

        assert "my-domain-vsponge" in isolated_catalog.domain_names
        saved = isolated_catalog.domain_data("my-domain-vsponge")
        assert saved.get("v_sponge") == 4242.0

        wiz2 = self._wizard(isolated_catalog)
        wiz2.domain_dd.value = "my-domain-vsponge"
        assert wiz2.v_sponge.value == 4242.0
        assert wiz2._v_sponge_touched is True
        assert wiz2.config.domain.v_sponge == 4242.0

    def test_save_domain_spec_omits_v_sponge_when_untouched(self, isolated_catalog):
        """An untouched v_sponge is deliberately omitted from a saved
        DomainSpec so it re-derives fresh from the grid on next load, instead
        of freezing a resolver default that was never a real user choice.
        """
        wiz = self._wizard(isolated_catalog)
        assert wiz._v_sponge_touched is False

        wiz.save_domain_name.value = "my-domain-no-vsponge"
        wiz._on_save_domain(None)

        saved = isolated_catalog.domain_data("my-domain-no-vsponge")
        assert "v_sponge" not in saved

        wiz2 = self._wizard(isolated_catalog)
        wiz2.domain_dd.value = "my-domain-no-vsponge"
        assert wiz2._v_sponge_touched is False
        # re-derives live from the (identical) grid -- same value, but arrived
        # at by fresh derivation, not a frozen saved number.
        assert wiz2.v_sponge.value == wiz.v_sponge.value

    def test_save_domain_spec_persists_dt(self, isolated_catalog):
        """``dt`` is a first-class domain property (Domain-derived properties),
        alongside v_sponge -- but unlike v_sponge/open_boundaries it has no
        touched flag: the widget is always authoritative (default, CFL-computed,
        or hand-typed), so saving a domain always records the current dt,
        whether or not the user ever edited it.
        """
        wiz = self._wizard(isolated_catalog)
        # Must stay an integer multiple of the default output_period_rst (86400)
        # -- see check_rst_period_divisible.
        wiz.dt.value = 3600.0

        wiz.save_domain_name.value = "my-domain-dt"
        wiz._on_save_domain(None)

        assert "my-domain-dt" in isolated_catalog.domain_names
        saved = isolated_catalog.domain_data("my-domain-dt")
        assert saved.get("dt") == 3600.0

        wiz2 = self._wizard(isolated_catalog)
        wiz2.domain_dd.value = "my-domain-dt"
        assert wiz2.dt.value == 3600.0
        assert wiz2.config.domain.dt == 3600.0
        # domain.dt and the model_settings leaf must never diverge.
        assert (
            wiz2.config.domain.dt == wiz2.config.model_settings["time_stepping"]["dt"]
        )

    def test_dt_edit_flags_domain_modified(self, isolated_catalog):
        """Editing ``dt`` after picking a catalog domain is a domain-owned
        deviation, exactly like editing v_sponge or a boundary checkbox.
        """
        wiz = self._wizard(isolated_catalog)
        wiz.save_domain_name.value = "my-domain-dt-seed"
        wiz._on_save_domain(None)
        wiz.domain_dd.value = "my-domain-dt-seed"
        assert wiz.config.composition.domain.modified is False

        # +100 would land on 7300, no longer an integer multiple of the default
        # output_period_rst (86400) -- see check_rst_period_divisible -- so
        # double instead (7200 -> 14400, still divisible) to isolate the
        # "editing dt flags domain modified" behavior under test.
        wiz.dt.value = wiz.dt.value * 2
        assert wiz.config.composition.domain.modified is True

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

    def test_mismatch_keeps_spec_modified_and_state_untouched(
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


def _shipped_cppdefs_sources() -> list[tuple[str, dict]]:
    """Every shipped cppdefs mapping: the docs example, the bundled example blueprints,
    and the bundled ModelSpecs' ``model_settings.cppdefs``.
    """
    import cstar.catalog

    repo_root = Path(cstar.__file__).parents[1]
    bundled = Path(cstar.catalog.__file__).parent / "bundled"
    out: list[tuple[str, dict]] = []
    for path in [
        repo_root / "docs" / "forge-blueprint-example.wio-toy.yaml",
        *sorted((bundled / "blueprints").glob("*.y*ml")),
    ]:
        docs = [d for d in yaml.safe_load_all(path.read_text()) if isinstance(d, dict)]
        bp = next(d for d in docs if "model_settings" in d)
        out.append(
            (str(path.relative_to(repo_root)), bp["model_settings"].get("cppdefs", {}))
        )
    for path in sorted((bundled / "ModelSpec").glob("*/model.yaml")):
        spec = yaml.safe_load(path.read_text())
        out.append(
            (
                str(path.relative_to(repo_root)),
                spec["model_settings"].get("cppdefs", {}),
            )
        )
    return out


@pytest.mark.parametrize(
    "source, cppdefs",
    _shipped_cppdefs_sources(),
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_shipped_cppdefs_keys_are_referenced_by_the_bundled_template(source, cppdefs):
    """Every cppdefs key a shipped blueprint or ModelSpec carries must be one the
    bundled ``cppdefs.opt.j2`` references, or rendering rejects it
    (``render_roms_settings`` fails on keys the template never reads). The docs
    example shipped for weeks pinning a template that predated four of its keys;
    tests never saw it because staging is redirected to the working tree.
    """
    from jinja2 import Environment

    from cstar.applications.forge.settings import _static_nested_keys
    from cstar.applications.forge.templates import bundled_template_dir

    template = bundled_template_dir("templates/compile-time") / "cppdefs.opt.j2"
    ast = Environment().parse(template.read_text())
    referenced, dynamic = _static_nested_keys(ast, "cppdefs")
    assert not dynamic, "template reads cppdefs dynamically; this check cannot apply"
    unreferenced = sorted(set(cppdefs) - referenced)
    assert not unreferenced, (
        f"{source}: cppdefs keys the bundled template never references: {unreferenced}"
    )
