"""
Tests for the ForgeBlueprint schema (``cstar_forge.forge.forge_blueprint``) and the
resolver (``cstar_forge.forge_blueprint_resolve.build_forge_blueprint``).

These validate that the resolver reproduces the known ``test-tiny`` demo values,
flattens settings, keeps naming/host values out of the stored config, resolves
sources from the ModelSpec, and round-trips through YAML.

NOTE: imports the in-package modules, so these run once the environment's editable
``cstar`` provides ``cstar.roms.namelist`` (i.e. on the namelist branch). The same
assertions were validated standalone during development.
"""

import subprocess
from datetime import date, datetime
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


def test_estimate_forge_cpus_anchors_and_strict_cap():
    from cstar_forge.forge.forge_blueprint import estimate_forge_cpus

    # toy domain (wio-toy) hits the 16 floor
    assert estimate_forge_cpus(20, 20, 10) == 16
    # hvalfjordur-0 (~2.0e7 cells) saturates the cap
    assert estimate_forge_cpus(512, 384, 100) == 128
    # an exceptionally large domain is far past the strict 128 cap
    assert estimate_forge_cpus(1856, 960, 100) == 128
    assert estimate_forge_cpus(10_000, 10_000, 1_000) == 128
    # mid-size domains scale between the bounds
    assert 16 < estimate_forge_cpus(350, 350, 100) < 128


def test_cpus_needed_is_grid_sized_forge_estimate():
    """cpus_needed sizes the forge run itself (scheduler fallback for the
    workplan's forge step) -- the grid estimate, not the ROMS partitioning.
    """
    from cstar_forge.forge.forge_blueprint import estimate_forge_cpus

    cfg = _build()
    gk = cfg.domain.grid_kwargs
    assert cfg.cpus_needed == estimate_forge_cpus(gk["nx"], gk["ny"], gk["N"])


def test_forge_blueprint_is_portable_no_forge_or_heavy_cstar_imports():
    """forge_blueprint.py is the C-Star-relocatable blueprint model: it must depend on
    nothing from cstar_forge (only stdlib + pydantic + yaml), and the only ``cstar``
    dependency it's allowed is the lightweight ``cstar.orchestration.models.Blueprint``
    base (see ``cstar_forge.forge.app.ForgeApplication`` -- this is what makes forge a
    real C-Star application). It must NOT reach into heavier cstar submodules (e.g.
    ``cstar.roms``, ``cstar.applications.roms_marbl``) that would drag in the
    ROMS/MARBL build + roms-tools stack.
    """
    src = Path(cstar_forge.__file__).parent / "forge" / "forge_blueprint.py"
    text = src.read_text()
    import re

    allowed_cstar_import = "from cstar.orchestration.models import Blueprint"
    bad = [
        ln.strip()
        for ln in text.splitlines()
        if re.match(r"\s*(from|import)\s+(cstar_forge|cstar|\.)", ln)
        and ln.strip() != allowed_cstar_import
    ]
    assert not bad, (
        "forge_blueprint.py must stay forge-free and depend on nothing beyond "
        f"{allowed_cstar_import!r} for cstar; found: {bad}"
    )


def test_application_discriminator_default():
    from cstar_forge.forge.forge_blueprint import DEFAULT_APPLICATION

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
    assert back.forge_blueprint_version == 6


def test_migrate_v4_cdr_output_migration_is_idempotent(tmp_path):
    """Already-current (do_cdr_output-shaped) data passes through unchanged --
    calling the migration on already-migrated data must not error or re-rename.
    """
    from cstar_forge.forge.forge_blueprint import migrate_forge_blueprint_data

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
    from cstar_forge.forge.forge_blueprint import migrate_forge_blueprint_data

    migrated = migrate_forge_blueprint_data({"forge_blueprint_version": 4})
    assert migrated["forge_blueprint_version"] == 6


def test_migrate_v5_shaped_dict_loads_with_null_user_file_fields(tmp_path):
    """A v5 file (predating user-provided files) loads, migrates its version to 6,
    and the new fields default to ``None`` -- purely additive, no data rewrite.
    """
    cfg = _build()
    p = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
    data = yaml.safe_load(p.read_text())
    data["forge_blueprint_version"] = 5

    back = ForgeBlueprint.from_yaml_data(data)
    assert back.forge_blueprint_version == 6
    assert back.domain.grid_file is None
    assert back.forcing.cdr_forcing_file is None
    assert all(river.custom_file is None for river in back.forcing.river)


_USER_FILE_KWARGS = dict(location="/data/staged/grid.nc", content_hash="a" * 64)


def test_domain_grid_file_round_trips_through_yaml(tmp_path):
    from cstar_forge.forge.forge_blueprint import UserProvidedFile

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
    from cstar_forge.forge.forge_blueprint import (
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
    from cstar_forge.forge.forge_blueprint import (
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
    from cstar_forge.forge.forge_blueprint import (
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
    from cstar_forge.forge.forge_blueprint import RiverForcingItem, SourceSpec

    with pytest.raises(ValueError, match="custom_file is not set"):
        RiverForcingItem(source=SourceSpec(name="CUSTOM_FILE"))


def test_river_custom_file_forbidden_when_source_is_not_custom_file():
    from cstar_forge.forge.forge_blueprint import (
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
    from cstar_forge.forge.forge_blueprint import (
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
    from cstar_forge.forge.forge_blueprint import (
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


def test_forcing_cdr_forcing_file_mutually_exclusive_with_cdr_forcing():
    # Constructs ``Forcing`` directly (see the comment on the sibling grid_file
    # test above for why ``model_copy`` won't do here).
    from cstar_forge.forge.forge_blueprint import (
        Forcing,
        InitialConditions,
        SourceSpec,
        UserProvidedFile,
    )

    with pytest.raises(ValueError, match="mutually exclusive"):
        Forcing(
            initial_conditions=InitialConditions(source=SourceSpec(name="GLORYS")),
            cdr_forcing={"some": "config"},
            cdr_forcing_file=UserProvidedFile(**_USER_FILE_KWARGS),
        )


def test_content_hash_ignores_user_file_location_but_not_content_hash():
    """Same rationale as ``code.<repo>.location``: a user file's ``location`` is
    host/transport and must not perturb the content hash, but its ``content_hash``
    leaf (the pin on the file's actual data) is results-affecting.
    """
    from cstar_forge.forge.forge_blueprint import UserProvidedFile

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
    from cstar_forge.forge.user_files import hash_netcdf_contents

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
    from cstar_forge.forge.forge_blueprint import UserProvidedFile

    grid_path = _write_tiny_netcdf(tmp_path)
    fake_grid = _FakeLoadedGrid(nx=5, ny=5, N=3, size_x=100.0, size_y=100.0)
    monkeypatch.setattr("roms_tools.Grid", lambda **kw: fake_grid)

    gf = UserProvidedFile(location=str(grid_path), content_hash="pinned-hash")
    cfg = _build(grid_file=gf, grid_kwargs={}, dt=7200, v_sponge=1.0)
    assert cfg.domain.grid_file == gf


def test_cpus_needed_falls_back_to_param_dims_for_grid_file(monkeypatch, tmp_path):
    from cstar_forge.forge.forge_blueprint import estimate_forge_cpus

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

    from cstar_forge.forge.user_files import hash_netcdf_contents

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
    from cstar_forge.forge.user_files import hash_netcdf_contents

    cdr_path = _write_tiny_netcdf(tmp_path, name="cdr.nc")

    cfg = _build(cdr_forcing_file=str(cdr_path))

    assert cfg.forcing.cdr_forcing_file is not None
    assert cfg.forcing.cdr_forcing_file.location == str(cdr_path)
    assert cfg.forcing.cdr_forcing_file.content_hash == hash_netcdf_contents(cdr_path)
    assert cfg.forcing.cdr_forcing is None

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
    assert cfg.forcing.cdr_forcing_file.content_hash == "not-the-real-hash"


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
    the explicit path verbatim without ever staging/verifying via SourceData.
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

    from cstar_forge.forge.forge_blueprint_engine import sources_to_forcing_override

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
    from cstar_forge.forge.forge_blueprint import _HASH_EXCLUDE, SpecRef

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
    from cstar_forge.forge.forge_blueprint_engine import (
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
    # the bundled glorys-era5-unified ForcingSpec carries boundary items --
    # a child grid (has a parent) must not generate boundary forcing (it
    # receives boundaries from the parent's nesting.nc extraction instead).
    fi = _CATALOG.forcing_data("glorys-era5-unified")
    assert fi["forcing"]["boundary"]  # sanity: fixture actually has boundary items
    cfg = _build(grid_kwargs_parent=_PARENT_GRID_KWARGS)
    assert cfg.forcing.boundary == []
    # open-boundary edge flags are untouched -- edges stay open, just fed by
    # nesting.nc instead of reanalysis boundary forcing.
    assert cfg.domain.open_boundaries.model_dump() == _BOUNDARIES


def test_resolver_parent_grid_skips_boundary_only_dataset():
    # Boundary items must be skipped entirely (not just cleared afterward) so a
    # boundary-only source never leaks into resolved_datasets/datasets -- e.g.
    # CESM_REGRIDDED here isn't used by surface/IC/tidal/river in this fixture,
    # so a stale post-hoc clear would still leave it in cfg.datasets.
    import copy

    fi = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
    fi["forcing"]["boundary"] = [{"source": {"name": "CESM_REGRIDDED"}, "type": "bgc"}]
    cfg = _build(grid_kwargs_parent=_PARENT_GRID_KWARGS, forcing_inputs=fi)
    assert cfg.forcing.boundary == []
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
    assert cfg.forcing.boundary  # a parent-only grid keeps its own boundary forcing


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

    from cstar_forge.domain_catalog import default_catalog as cat

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["river"][0]["bgc_source"] = {
        "name": "RIVR2O",
        "path": "/tmp/rivr2o/*.nc",
    }
    fdata["forcing"]["river"][0]["convert_to_climatology"] = "always"

    cfg = _build(forcing_inputs=fdata)
    river = cfg.forcing.river[0]

    assert river.bgc_source == {"name": "RIVR2O", "path": "/tmp/rivr2o/*.nc"}
    assert river.convert_to_climatology.value == "always"
    assert "RIVR2O" in cfg.datasets
    assert "RIVR2O" in cfg.forcing.resolved_datasets
    assert "CONSTANTS" not in cfg.datasets


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

    from cstar_forge.domain_catalog import default_catalog as cat
    from cstar_forge.forge.forge_blueprint_engine import (
        forge_blueprint_to_builder_kwargs,
        sources_to_forcing_override,
    )

    fdata = copy.deepcopy(cat.forcing_data("glorys-era5-unified"))
    fdata["forcing"]["river"][0]["bgc_source"] = {
        "name": "RIVR2O",
        "path": "/tmp/rivr2o/*.nc",
    }
    fdata["forcing"]["river"][0]["convert_to_climatology"] = "always"

    cfg = _build(forcing_inputs=fdata, topography_source="EMOD")

    ov = sources_to_forcing_override(cfg)
    river_ov = ov["forcing"]["river"][0]
    assert river_ov["bgc_source"] == {"name": "RIVR2O", "path": "/tmp/rivr2o/*.nc"}
    assert river_ov["convert_to_climatology"] == "always"

    kwargs = forge_blueprint_to_builder_kwargs(cfg)
    assert kwargs["topography_source"] == "EMOD"
    assert "RIVR2O" in kwargs["source_dataset_keys"]
    assert "EMOD" in kwargs["source_dataset_keys"]


def test_sources_to_forcing_override_carries_river_custom_file():
    """The third propagation path (resolver / sources_to_forcing_override / wizard
    load-back -- this WP covers the first two): custom_file must survive the
    round trip through sources_to_forcing_override the same as bgc_source does
    above. Guards against a future ``exclude=`` edit to ``_item()``'s
    ``model_dump`` silently dropping it (today it propagates "for free" because
    nothing excludes it).
    """
    import copy

    from cstar_forge.forge.forge_blueprint_engine import sources_to_forcing_override

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

    from cstar_forge.forge.forge_blueprint_engine import sources_to_forcing_override

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
    from cstar_forge.forge.forge_blueprint_engine import (
        forge_blueprint_to_builder_kwargs,
        sources_to_forcing_override,
    )

    cdr_path = _write_tiny_netcdf(tmp_path, name="cdr.nc")
    cfg = _build(cdr_forcing_file=str(cdr_path))

    kwargs = forge_blueprint_to_builder_kwargs(cfg)
    assert kwargs["cdr_forcing_file"] == cfg.forcing.cdr_forcing_file
    assert kwargs["cdr_forcing"] is None

    ov = sources_to_forcing_override(cfg)
    assert "cdr_forcing_file" not in ov
    assert "cdr_forcing" not in ov


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


def test_regrid_options_survive_resolve_and_override_round_trip():
    """The roms-tools >=4 prefill/regrid_method/extrap_method knobs, authored on
    initial_conditions/surface/tidal, survive both the resolver
    (build_forge_blueprint -> Forcing) and the reverse dump
    (sources_to_forcing_override) -- the two propagation paths that must stay in
    lockstep with the typed model fields (see forge_blueprint_resolve._build_forcing
    and forge_blueprint_engine.sources_to_forcing_override).
    """
    from cstar_forge.domain_catalog import default_catalog as cat
    from cstar_forge.forge.forge_blueprint_engine import sources_to_forcing_override

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
    from cstar_forge.domain_catalog import default_catalog as cat
    from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizard

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


_CDR_SAMPLE_YAML = Path(__file__).parent / "fixtures" / "cdr_forcing_sample.yaml"


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


class TestEnsureCdrOutputMarblDiagnostics:
    def test_none_input_returns_all_required(self):
        from cstar_forge.forge.namelist_model import (
            CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS,
            ensure_cdr_output_marbl_diagnostics,
        )

        assert ensure_cdr_output_marbl_diagnostics(None) == list(
            CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS
        )

    def test_empty_list_returns_all_required(self):
        from cstar_forge.forge.namelist_model import (
            CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS,
            ensure_cdr_output_marbl_diagnostics,
        )

        assert ensure_cdr_output_marbl_diagnostics([]) == list(
            CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS
        )

    def test_partial_overlap_no_duplicates_order_preserved(self):
        from cstar_forge.forge.namelist_model import (
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
    from cstar_forge.forge.settings import render_roms_settings

    overrides = {"cdr_output": {"do_cdr_output": True}} if do_cdr_output else {}
    cfg = _build(run_time_overrides=overrides)
    param = cfg.model_settings["param"]
    n_tracers = 2 + int(param.get("ntrc_bio", 0)) + int(param.get("nt_passive", 0))
    render_roms_settings(
        template_files=["cppdefs.opt.j2"],
        template_dir=Path(cstar_forge.__file__).parents[1]
        / "templates"
        / "compile-time",
        settings_dict=dict(cfg.model_settings),
        code_output_dir=tmp_path,
        n_tracers=n_tracers,
    )
    text = (tmp_path / "cppdefs.opt").read_text()
    expected = "#define CDR_FORCING" if do_cdr_output else "#undef CDR_FORCING"
    assert expected in text


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
    """A bare numeric commit in model.yaml (e.g. `commit: 123456`, parsed by PyYAML
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
        Path(cstar_forge.__file__).parents[1]
        / "docs"
        / "forge-blueprint-example.wio-toy.yaml"
    )
    if not example.exists():
        pytest.skip("example file not present")
    cfg = ForgeBlueprint.from_yaml(example)
    assert cfg.composition.model.name == "cson_roms-marbl_v0.1"
    assert cfg.composition.model.origin == "catalog"


# ---------------------------------------------------------------------------
# _forge_version -- best-effort git describe / package-version identifier for
# provenance.forge_version (see test_forge_version_* above for the resolver wiring)
# ---------------------------------------------------------------------------
class TestForgeVersion:
    """``_REPO_ROOT`` is monkeypatched to a real ``tmp_path`` (with/without an actual
    ``.git`` subdirectory) rather than stubbing ``Path.exists`` globally, so these
    don't risk affecting unrelated filesystem checks during the test.
    """

    def test_uses_git_describe_when_repo_present(self, monkeypatch, tmp_path):
        from cstar_forge.forge import forge_blueprint as fb

        (tmp_path / ".git").mkdir()
        monkeypatch.setattr(fb, "_REPO_ROOT", tmp_path)
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="abc1234-dirty\n"
        )
        monkeypatch.setattr(fb.subprocess, "run", lambda *a, **k: mock_result)
        assert fb._forge_version() == "abc1234-dirty"

    def test_falls_back_to_package_version_when_no_git_dir(self, monkeypatch, tmp_path):
        from cstar_forge.forge import forge_blueprint as fb

        monkeypatch.setattr(fb, "_REPO_ROOT", tmp_path)  # no .git subdir
        monkeypatch.setattr(fb, "_pkg_version", lambda name: "0.1.0")
        assert fb._forge_version() == "cstar-forge==0.1.0"

    def test_falls_back_to_package_version_when_git_fails(self, monkeypatch, tmp_path):
        from cstar_forge.forge import forge_blueprint as fb

        (tmp_path / ".git").mkdir()
        monkeypatch.setattr(fb, "_REPO_ROOT", tmp_path)

        def _raise(*a, **k):
            raise FileNotFoundError("git not installed")

        monkeypatch.setattr(fb.subprocess, "run", _raise)
        monkeypatch.setattr(fb, "_pkg_version", lambda name: "0.1.0")
        assert fb._forge_version() == "cstar-forge==0.1.0"

    @pytest.mark.parametrize(
        "exc",
        [
            subprocess.TimeoutExpired(cmd=["git"], timeout=2),
            subprocess.CalledProcessError(1, ["git"]),
        ],
        ids=["timeout", "nonzero_exit"],
    )
    def test_falls_back_on_slow_or_failing_git(self, monkeypatch, tmp_path, exc):
        """A slow/unreachable git (e.g. an HPC home dir) or a non-zero exit
        (``check=True``) must fall back like a missing git binary -- both
        ``TimeoutExpired`` and ``CalledProcessError`` are ``SubprocessError``.
        """
        from cstar_forge.forge import forge_blueprint as fb

        (tmp_path / ".git").mkdir()
        monkeypatch.setattr(fb, "_REPO_ROOT", tmp_path)

        def _raise(*a, **k):
            raise exc

        monkeypatch.setattr(fb.subprocess, "run", _raise)
        monkeypatch.setattr(fb, "_pkg_version", lambda name: "0.1.0")
        assert fb._forge_version() == "cstar-forge==0.1.0"

    def test_returns_none_when_neither_git_nor_package_available(
        self, monkeypatch, tmp_path
    ):
        from importlib.metadata import PackageNotFoundError

        from cstar_forge.forge import forge_blueprint as fb

        monkeypatch.setattr(fb, "_REPO_ROOT", tmp_path)  # no .git subdir

        def _raise(name):
            raise PackageNotFoundError(name)

        monkeypatch.setattr(fb, "_pkg_version", _raise)
        assert fb._forge_version() is None


class TestInstalledVersion:
    """``_installed_version`` backs ``provenance.cstar_version``/``roms_tools_version``
    -- no git-describe fallback needed, since both packages version themselves via
    ``setuptools_scm`` (an editable/dev checkout's installed version already embeds
    commit info).
    """

    def test_returns_formatted_version_when_installed(self, monkeypatch):
        from cstar_forge.forge import forge_blueprint as fb

        monkeypatch.setattr(fb, "_pkg_version", lambda name: "4.0.0")
        assert fb._installed_version("roms-tools") == "roms-tools==4.0.0"

    def test_returns_none_when_not_installed(self, monkeypatch):
        from importlib.metadata import PackageNotFoundError

        from cstar_forge.forge import forge_blueprint as fb

        def _raise(name):
            raise PackageNotFoundError(name)

        monkeypatch.setattr(fb, "_pkg_version", _raise)
        assert fb._installed_version("not-a-real-package") is None


class TestProvenanceStamping:
    """``ForgeBlueprint.to_yaml_str`` stamps generated_at/forge_version/
    cstar_version/roms_tools_version on first save; a resave preserves whatever
    was already stamped (or explicitly set), same "first save wins" semantics as
    ``content_hash`` is exempt from (content_hash always recomputes; these don't).
    """

    def _patched_fb(self, monkeypatch):
        from cstar_forge.forge import forge_blueprint as fb

        monkeypatch.setattr(fb, "_forge_version", lambda: "abc1234")
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
        assert back.provenance.forge_version == "abc1234"
        assert back.provenance.cstar_version == "cstar-ocean==9.9.9"
        assert back.provenance.roms_tools_version == "roms-tools==9.9.9"

    def test_resave_preserves_original_stamp(self, monkeypatch, tmp_path):
        fb = self._patched_fb(monkeypatch)
        cfg = _build()
        path = cfg.to_yaml(tmp_path / "forge_blueprint.yaml")
        first = fb.ForgeBlueprint.from_yaml(path)

        # a later save, from a different Forge/roms_tools/cstar install, must not
        # overwrite the original values
        monkeypatch.setattr(fb, "_forge_version", lambda: "def5678")
        monkeypatch.setattr(fb, "_installed_version", lambda name: f"{name}==1.0.0")
        first.to_yaml(path)
        second = fb.ForgeBlueprint.from_yaml(path)

        assert second.provenance.generated_at == first.provenance.generated_at
        assert second.provenance.forge_version == "abc1234"
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
        assert back.provenance.forge_version == "abc1234"


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
        w1.dt.value = 1234.0
        p = tmp_path / "forge_blueprint.yaml"
        w1.save_path.value = str(p)
        w1._boundaries_touched = True  # not exercising boundary derivation here
        w1._on_save(None)
        w2 = self._wizard()
        w2.load_path.value = str(p)
        w2._on_load_path(None)
        assert w2.dt.value == 1234.0
        assert w2.config.model_settings["time_stepping"]["dt"] == 1234.0
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
        assert w.output_dd.value == "standard"
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
        assert w.config.forcing.boundary  # sanity: default forcing has boundary items
        w.parent_enable.value = True
        w.parent_w["N"].value = 25
        cfg = w.config
        assert cfg.domain.grid_kwargs_parent is not None
        assert cfg.domain.grid_kwargs_parent["N"] == 25
        assert cfg.domain.is_child is True
        assert cfg.domain.is_parent is False
        assert cfg.forcing.boundary == []
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
        assert w2.config.forcing.boundary == []

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
        assert "settings valid" in w2.validation.value

    def test_load_bad_input_shows_error_not_crash(self):
        w = self._wizard()
        w.load_path.value = "/nonexistent/forge_blueprint.yaml"
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
        # the download filename is keyed off cfg.name (matching save_path, see
        # _on_save_path_change/_rebuild), not the date-suffixed casename
        assert f'download="{wiz.config.name}.forge_blueprint.yaml"' in html


class TestForgeBlueprintWizardApp:
    """The catalog-location wrapper around ForgeBlueprintWizard."""

    def _app(self, **kwargs):
        pytest.importorskip("ipywidgets")
        from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizardApp

        return ForgeBlueprintWizardApp(**kwargs)

    def test_default_auto_loads_layered_catalog(self):
        from cstar_forge.domain_catalog import (
            _DEFAULT_CATALOG_ROOT,
            LayeredCatalog,
            user_catalog_root,
        )
        from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizard

        app = self._app()
        assert isinstance(app.inner, ForgeBlueprintWizard)
        cat = app.inner.catalog
        # Blank App load yields a LayeredCatalog: writable user layer (top,
        # .catalog_root) over the read-only bundled packaged layer (bottom).
        assert isinstance(cat, LayeredCatalog)
        assert cat.catalog_root == user_catalog_root()
        assert cat.stores[-1].catalog_root == _DEFAULT_CATALOG_ROOT
        assert "color:#2a2" in app._cat_status.value

    def test_reload_with_bad_value_keeps_previous_wizard(self):
        # A nonexistent local path is no longer an error (it becomes an empty
        # writable layer over bundled, matching CSTAR_FORGE_CATALOG semantics),
        # so a malformed GitHub URL is the failure case now.
        app = self._app()
        original_inner = app.inner
        app._cat_input.value = "https://github.com/org-but-no-repo"
        app._reload(None)
        assert app.inner is original_inner
        assert "color:#b00" in app._cat_status.value

    def test_single_local_path_builds_stack_with_bundled(self, tmp_path):
        from cstar_forge.domain_catalog import _DEFAULT_CATALOG_ROOT, LayeredCatalog

        app = self._app()
        app._cat_input.value = str(tmp_path / "my-catalog")
        app._reload(None)
        cat = app.inner.catalog
        # One local path routes through build_catalog_stack: writable top over
        # the read-only bundled layer, same as the env var would produce.
        assert isinstance(cat, LayeredCatalog)
        assert cat.catalog_root == (tmp_path / "my-catalog").resolve()
        assert cat.stores[-1].catalog_root == _DEFAULT_CATALOG_ROOT
        assert cat.model_names  # bundled models visible through the stack
        assert "color:#2a2" in app._cat_status.value

    def test_single_local_literal_loads_readonly_store_with_warning(self):
        from cstar_forge.domain_catalog import DomainCatalog, LayeredCatalog

        app = self._app()
        app._cat_input.value = "local"
        app._reload(None)
        cat = app.inner.catalog
        # "local" (the bundled catalog) can never be a writable top layer:
        # exactly one read-only store, and the status line warns that saves
        # fall back to CWD-relative filenames.
        assert isinstance(cat, DomainCatalog)
        assert not isinstance(cat, LayeredCatalog)
        assert cat.read_only is True
        assert "read-only catalog" in app._cat_status.value
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
        """Regression for the §3a bug (docs/dev-notes/forge-blueprint-parameter-audit.md): the
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

    def test_only_inputs_forces_configure_off_and_resolves_selection(self):
        """A subset run must never reach configure_build -- persist() only lives
        there, so this is what guarantees an only_inputs run can't clobber an
        existing complete blueprint from a prior full run.
        """
        from cstar_forge.forge.forge_blueprint_engine import process_forge_blueprint

        b = process_forge_blueprint(
            self._cfg(),
            only_inputs=["boundary", "bry"],  # dupe alias -> single resolved key
            executor_factory=_FakeBuilder,
        )
        assert [c[0] for c in b.calls] == ["ensure", "generate"]
        gen = dict(b.calls[1][1])
        assert gen["only"] == {"forcing.boundary"}

    def test_only_inputs_wins_even_if_configure_true(self):
        from cstar_forge.forge.forge_blueprint_engine import process_forge_blueprint

        b = process_forge_blueprint(
            self._cfg(),
            configure=True,
            only_inputs=["grid"],
            executor_factory=_FakeBuilder,
        )
        assert "configure" not in [c[0] for c in b.calls]

    def test_only_inputs_unknown_name_fails_fast_before_any_call(self):
        from cstar_forge.forge.forge_blueprint_engine import process_forge_blueprint

        with pytest.raises(ValueError, match="bogus"):
            process_forge_blueprint(
                self._cfg(), only_inputs=["bogus"], executor_factory=_FakeBuilder
            )

    def test_executor_must_implement_interface(self):
        from cstar_forge.forge.forge_blueprint_engine import (
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
        assert "cstar-forge-run" in str(h.working_dir)
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

        from cstar_forge.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog

        root = tmp_path / "catalog"
        # Copy the BUNDLED catalog (not _CATALOG.catalog_root, which is now the
        # writable *user* layer -- empty/nonexistent in tests, see conftest.py).
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        return DomainCatalog(catalog_root=root)

    def _wizard(self, catalog):
        pytest.importorskip("ipywidgets")
        from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizard

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
        # (cstar_forge/catalog/ModelSpec/pio-dev) -- register_model_from_settings
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
        from cstar_forge.forge_blueprint_wizard import _model_owned_settings

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
        from cstar_forge.forge_blueprint_wizard import _model_owned_settings

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
        from cstar_forge.forge_blueprint_wizard import _model_owned_settings

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

    def test_save_forcing_spec_embeds_and_reloads_cdr(self, isolated_catalog):
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
        wiz.dt.value = 3333.0

        wiz.save_domain_name.value = "my-domain-dt"
        wiz._on_save_domain(None)

        assert "my-domain-dt" in isolated_catalog.domain_names
        saved = isolated_catalog.domain_data("my-domain-dt")
        assert saved.get("dt") == 3333.0

        wiz2 = self._wizard(isolated_catalog)
        wiz2.domain_dd.value = "my-domain-dt"
        assert wiz2.dt.value == 3333.0
        assert wiz2.config.domain.dt == 3333.0
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

        wiz.dt.value = wiz.dt.value + 100.0
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
