"""The Forge resolver on a ``ucla_roms_gpu`` ModelSpec (experimental).

The bundled GPU spec resolves, records the derived switches, and keys its tier on the
ucla-roms release roms-marbl-gpu reads; what roms-marbl-gpu cannot take is refused at
resolve time, and the switches it only warns about reach ``resolver_warnings``.
"""

from __future__ import annotations

import typing as t
from datetime import datetime
from pathlib import Path

import pytest

import cstar.catalog
from cstar.applications.forge.namelist_model import (
    _ONE_TO_ONE_GROUPS,
    GPU_PRECHECK_SECTIONS,
    namelist_schema_for,
    run_time_settings_for_ref,
    validate_run_time_sections,
)
from cstar.applications.forge.resolve import (
    GPU_FORCING_SPEC,
    build_forge_blueprint,
    resolver_warnings,
)
from cstar.catalog.domain_catalog import default_catalog
from cstar.roms import precheck
from cstar.roms.precheck import NamelistConsistencyError

if t.TYPE_CHECKING:
    from cstar.applications.forge.blueprint import ForgeBlueprint

_SPECS = Path(cstar.catalog.__file__).parent / "bundled" / "specs" / "ModelSpec"
_GPU_MODEL = "roms-marbl-gpu-0.1.3-default"
_CPU_MODEL = "roms-marbl-0.15-default"
_GRID_KWARGS = {
    "nx": 8,
    "ny": 8,
    "size_x": 500,
    "size_y": 500,
    "center_lon": 0,
    "center_lat": 55,
    "rot": 10,
    "N": 3,
    "theta_s": 5.0,
    "theta_b": 2.0,
    "hc": 250.0,
}


def _resolve(
    model: str = _GPU_MODEL, forcing: str = GPU_FORCING_SPEC, **over: t.Any
) -> ForgeBlueprint:
    kw: dict[str, t.Any] = {
        "model_path": _SPECS / f"{model}.yaml",
        "grid_name": "test-tiny",
        "grid_kwargs": _GRID_KWARGS,
        "open_boundaries": {"south": False, "east": True, "north": True, "west": False},
        "partitioning": {"n_procs_x": 2, "n_procs_y": 2},
        "start_date": datetime(2012, 1, 1),
        "end_date": datetime(2012, 1, 2),
        "description": "gpu resolver",
        "dt": 7200,
        "v_sponge": 100.0,
        "forcing_inputs": default_catalog.forcing_data(forcing),
        "output_settings": default_catalog.output_data("standard"),
    }
    kw.update(over)
    return build_forge_blueprint(**kw)


@pytest.fixture(scope="module")
def gpu_bp() -> ForgeBlueprint:
    return _resolve()


def test_nodes_and_writers_reach_the_tiling() -> None:
    partitioning = {"n_procs_x": 2, "n_procs_y": 2, "nodes": 2, "writers_per_node": 3}
    bp = _resolve(partitioning=partitioning)
    assert bp.domain.partitioning.nodes == 2
    assert bp.domain.partitioning.writers_per_node == 3
    assert bp.tiling is not None
    assert (bp.tiling.nodes, bp.tiling.writers_per_node) == (2, 3)


def test_nodes_are_refused_for_a_cpu_spec() -> None:
    with pytest.raises(ValueError, match="only for ucla_roms_gpu"):
        _resolve(
            model=_CPU_MODEL,
            forcing="glorys-era5-unified",
            partitioning={"n_procs_x": 2, "n_procs_y": 2, "nodes": 2},
        )


class TestResolves:
    def test_blueprint_carries_the_application(self, gpu_bp: ForgeBlueprint) -> None:
        assert gpu_bp.emitted_application == "ucla_roms_gpu"
        assert gpu_bp.code.marbl is None
        assert gpu_bp.code.pio is None
        assert gpu_bp.code.templates_compile_time.files == []
        assert gpu_bp.model_settings["cppdefs"]["marbl"] is True
        assert gpu_bp.model_settings["cppdefs"]["use_pio"] is False

    def test_settings_validate_on_the_0_11_tier(self, gpu_bp: ForgeBlueprint) -> None:
        assert (
            validate_run_time_sections(gpu_bp.model_settings, roms_ref="0.11.0") == []
        )

    def test_cpu_spec_keeps_the_default_application(self) -> None:
        bp = _resolve(_CPU_MODEL, forcing="glorys-era5-unified")
        assert bp.emitted_application == "ucla_roms"
        assert bp.code.marbl is not None


class TestRefusals:
    def test_restoring_forcing_names_the_fix(self) -> None:
        with pytest.raises(ValueError, match=GPU_FORCING_SPEC) as exc:
            _resolve(forcing="glorys-era5-unified")
        assert "roms-marbl-gpu" in str(exc.value)
        assert "experimental" in str(exc.value)

    def test_compile_time_overrides(self) -> None:
        with pytest.raises(ValueError, match="no compile-time switches"):
            _resolve(compile_time_overrides={"cppdefs": {"diagnostics": True}})

    def test_marbl_ref(self) -> None:
        with pytest.raises(ValueError, match="marbl_ref"):
            _resolve(marbl_ref="abc1234")

    def test_every_problem_in_one_error(self) -> None:
        with pytest.raises(ValueError) as exc:
            _resolve(
                forcing="glorys-era5-unified",
                compile_time_overrides={"cppdefs": {"diagnostics": True}},
            )
        assert GPU_FORCING_SPEC in str(exc.value)
        assert "no compile-time switches" in str(exc.value)

    def test_rule_f_stop_switch(self) -> None:
        with pytest.raises(NamelistConsistencyError, match="z-slice output"):
            _resolve(run_time_overrides={"zslice": {"do_zslice": True}})

    def test_rule_f2_ignored_value(self) -> None:
        with pytest.raises(NamelistConsistencyError, match="xco2air_default"):
            _resolve(run_time_overrides={"bgc": {"xco2air_default": 400.0}})

    def test_nesting_child_is_the_blueprint_validators(self) -> None:
        with pytest.raises(ValueError, match="nest"):
            _resolve(grid_kwargs_parent=dict(_GRID_KWARGS, size_x=1000))


class TestWarnings:
    def test_warn_switch_reaches_resolver_warnings(self) -> None:
        bp = _resolve(run_time_overrides={"sponge_tune": {"ub_tune": True}})
        messages = resolver_warnings(bp, roms_ref="0.11.0")
        assert any("roms-marbl-gpu" in m and "ub_tune" in m for m in messages)

    def test_default_gpu_blueprint_has_none(self, gpu_bp: ForgeBlueprint) -> None:
        assert not any(
            "roms-marbl-gpu" in m for m in resolver_warnings(gpu_bp, roms_ref="0.11.0")
        )

    def test_cpu_blueprint_never_has_them(self) -> None:
        bp = _resolve(
            _CPU_MODEL,
            forcing="glorys-era5-unified",
            run_time_overrides={"sponge_tune": {"ub_tune": True}},
        )
        assert not any(
            "roms-marbl-gpu" in m for m in resolver_warnings(bp, roms_ref="0.15.0")
        )


def test_gpu_precheck_sections_cover_every_group_the_rules_read() -> None:
    tier = run_time_settings_for_ref("0.11.0")
    # A full section for each Forge section, so the translation produces every group.

    produced = {
        group
        for section in GPU_PRECHECK_SECTIONS
        for group in _ONE_TO_ONE_GROUPS[section]
        if group in tier_groups(tier)
    }
    read = {
        s.group for s in (*precheck._GPU_STOP_SWITCHES, *precheck._GPU_WARN_SWITCHES)
    }
    read |= {"bgc_settings"}
    absent_from_tier = {"cdr_tracer_output_settings", "tides_settings"}
    assert read - absent_from_tier <= produced, sorted(
        read - absent_from_tier - produced
    )


def tier_groups(tier: t.Any) -> set[str]:
    return set(namelist_schema_for(tier).model_fields)
