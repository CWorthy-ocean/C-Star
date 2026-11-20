"""Generated values win over the stored ``model_settings`` snapshot, and an explicit
override that contradicts one is refused (``GENERATION_DERIVED_LEAF_KEYS``).
"""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

import cstar.catalog
from cstar.applications.forge.blueprint import CoarseGridMode, ForgeBlueprint
from cstar.applications.forge.engine import (
    GENERATION_DERIVED_LEAF_KEYS,
    known_derived_values,
    process_forge_blueprint,
    split_model_settings,
)
from cstar.applications.forge.executor import ForgeExecutor
from cstar.applications.forge.host import HostPaths
from cstar.applications.forge.resolve import build_forge_blueprint
from cstar.catalog.domain_catalog import default_catalog as _CATALOG

_MODEL_PATH = (
    Path(cstar.catalog.__file__).parent
    / "bundled"
    / "specs"
    / "ModelSpec"
    / "cson_roms-marbl_v0.1.yaml"
)
_GRID_KWARGS = {
    "nx": 3,
    "ny": 4,
    "size_x": 500,
    "size_y": 1000,
    "center_lon": 0,
    "center_lat": 55,
    "rot": 10,
    "N": 3,
    "theta_s": 5.0,
    "theta_b": 2.0,
    "hc": 250.0,
}
_CHILD_GRID_KWARGS = {
    "nx": 2,
    "ny": 2,
    "size_x": 100,
    "size_y": 100,
    "center_lon": 0,
    "center_lat": 55,
    "rot": 10,
    "N": 4,
    "theta_s": 6.0,
    "theta_b": 3.0,
    "hc": 200.0,
}
_BOUNDARIES = {"south": True, "east": True, "north": True, "west": True}
_PARTITIONING = {"n_procs_x": 2, "n_procs_y": 2}


def _build(**over: Any) -> ForgeBlueprint:
    kwargs: dict[str, Any] = dict(
        model_path=_MODEL_PATH,
        grid_name="test-grid",
        grid_kwargs=_GRID_KWARGS,
        open_boundaries=_BOUNDARIES,
        partitioning=_PARTITIONING,
        start_date=datetime(2012, 1, 1),
        end_date=datetime(2012, 1, 2),
        dt=7200,
        forcing_inputs=_CATALOG.forcing_data("glorys-era5-unified"),
        output_settings=_CATALOG.output_data("standard"),
    )
    kwargs.update(over)
    return build_forge_blueprint(**kwargs)


def _with_coarse_grid_mode(cfg: ForgeBlueprint, mode: CoarseGridMode) -> None:
    cfg.forcing.surface = [
        item.model_copy(update={"coarse_grid_mode": mode})
        for item in cfg.forcing.surface
    ]


class TestSplitModelSettings:
    def test_overlays_leave_out_every_derived_leaf(self) -> None:
        """Neither overlay carries a derived leaf, ``cppdefs.obc_*`` included, so
        the stored snapshot cannot revert what generation wrote.
        """
        run_ov, compile_ov = split_model_settings(_build())
        overlays = {**run_ov, **compile_ov}
        for section, leaves in GENERATION_DERIVED_LEAF_KEYS.items():
            for leaf in leaves:
                assert leaf not in overlays.get(section, {}), f"{section}.{leaf}"
        # Siblings of a derived leaf still reach configure_build.
        assert "nt_passive" in run_ov["param"]
        assert "marbl" in compile_ov["cppdefs"]


class TestResolverRefusesConflicts:
    def test_np_xi_override_that_contradicts_the_partitioning(self) -> None:
        with pytest.raises(ValueError, match="param.np_xi") as excinfo:
            _build(run_time_overrides={"param": {"np_xi": 3}})
        message = str(excinfo.value)
        assert "3" in message
        assert "2" in message
        assert "domain.partitioning.n_procs_x" in message

    def test_obc_west_override_that_contradicts_open_boundaries(self) -> None:
        with pytest.raises(ValueError, match="cppdefs.obc_west") as excinfo:
            _build(compile_time_overrides={"cppdefs": {"obc_west": False}})
        assert "domain.open_boundaries.west" in str(excinfo.value)

    def test_interp_frc_override_under_coarse_grid_mode_never(self) -> None:
        """Every bundled surface item says ``never``, so generation writes
        ``interp_frc = 0`` and an override to true is refused.
        """
        with pytest.raises(ValueError, match="blk_frc.interp_frc"):
            _build(run_time_overrides={"blk_frc": {"interp_frc": True}})

    def test_child_level_count_override(self) -> None:
        with pytest.raises(ValueError, match="extract_data.n_chd"):
            _build(
                grid_kwargs_child=_CHILD_GRID_KWARGS,
                metadata_child={"period": 3600.0},
                run_time_overrides={"extract_data": {"n_chd": 90}},
            )

    def test_override_equal_to_the_derived_value_is_kept(self) -> None:
        """Re-typing the derived value records an override (the wizard does this),
        which must not be refused. The resolver records it in the composition.
        """
        cfg = _build(
            run_time_overrides={"param": {"np_xi": 2}},
            compile_time_overrides={"cppdefs": {"obc_west": True}},
        )
        assert cfg.model_settings["param"]["np_xi"] == 2
        assert cfg.composition.overrides == {
            "param": {"np_xi": 2},
            "cppdefs": {"obc_west": True},
        }

    def test_upscaled_cdr_counts_stay_user_stated(self) -> None:
        """``param.nt_cdr_*`` are derived from a generated CDR forcing but stated
        by hand for ``"upscaled"``, so an override of them is never refused.
        """
        cfg = _build(run_time_overrides={"param": {"nt_cdr_oae": 2}})
        assert cfg.model_settings["param"]["nt_cdr_oae"] == 2


class TestChildVerticalCoordinates:
    @pytest.mark.parametrize("missing", ["N", "theta_s", "theta_b", "hc"])
    def test_resolver_requires_them(self, missing: str) -> None:
        child = {k: v for k, v in _CHILD_GRID_KWARGS.items() if k != missing}
        with pytest.raises(ValueError, match=f"grid_kwargs_child is missing {missing}"):
            _build(grid_kwargs_child=child, metadata_child={"period": 3600.0})

    def test_resolver_copies_them_into_extract_data(self) -> None:
        cfg = _build(
            grid_kwargs_child=_CHILD_GRID_KWARGS, metadata_child={"period": 3600.0}
        )
        extract = cfg.model_settings["extract_data"]
        assert (
            extract["n_chd"],
            extract["theta_s_chd"],
            extract["theta_b_chd"],
            extract["hc_chd"],
        ) == (4, 6.0, 3.0, 200.0)


class TestKnownDerivedValues:
    def test_auto_coarse_grid_mode_leaves_interp_frc_open(self) -> None:
        cfg = _build()
        _with_coarse_grid_mode(cfg, CoarseGridMode.AUTO)
        known = known_derived_values(cfg)
        assert "interp_frc" not in known.get("blk_frc", {})
        assert "interp_frc" not in known.get("bgc", {})

    def test_always_fixes_both_sections(self) -> None:
        cfg = _build()
        _with_coarse_grid_mode(cfg, CoarseGridMode.ALWAYS)
        known = known_derived_values(cfg)
        assert known["blk_frc"]["interp_frc"] is True
        assert known["bgc"]["interp_frc"] is True

    def test_auto_tiling_leaves_tiles_open(self) -> None:
        cfg = _build(partitioning={"auto_tiling": True, "n_cores": 4}, use_pio=True)
        known = known_derived_values(cfg)
        assert "np_xi" not in known["param"]
        assert "np_eta" not in known["param"]


class _Refused(Exception):
    """Raised by the executor factory: processing got past the pre-generation check."""


def _refusing_factory(*_args: Any) -> Any:
    raise _Refused


class TestProcessRefusesBeforeGeneration:
    def test_wizard_override_that_contradicts_coarse_grid_mode(self) -> None:
        """The wizard records its overrides in ``composition.overrides`` after the
        resolver ran; processing refuses the conflict before building an executor.
        """
        cfg = _build()
        _with_coarse_grid_mode(cfg, CoarseGridMode.ALWAYS)
        cfg.composition.overrides = {"blk_frc": {"interp_frc": False}}
        with pytest.raises(ValueError, match="forcing.surface\\[\\].coarse_grid_mode"):
            process_forge_blueprint(
                cfg, ensure_data=False, executor_factory=_refusing_factory
            )

    def test_stale_snapshot_without_overrides_is_not_refused(self) -> None:
        """Every saved blueprint stores the pre-generation values; without an
        override they are replaced, not refused.
        """
        cfg = _build()
        _with_coarse_grid_mode(cfg, CoarseGridMode.ALWAYS)
        assert cfg.model_settings["blk_frc"]["interp_frc"] in (0, False)
        with pytest.raises(_Refused):
            process_forge_blueprint(
                cfg, ensure_data=False, executor_factory=_refusing_factory
            )


class TestConfigureBuildUsesGeneratedValues:
    """``configure_build`` after a simulated ``generate_inputs`` (the same call
    chain ``process_forge_blueprint`` drives).
    """

    @staticmethod
    def _executor(cfg: ForgeBlueprint) -> ForgeExecutor:
        tmp = Path(tempfile.mkdtemp(prefix="forge-test-derived-"))
        host = HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")
        return ForgeExecutor.from_forge_blueprint(cfg, host=host)

    @staticmethod
    def _simulate_generation(executor: ForgeExecutor) -> None:
        # What the grid and surface steps write in input_generator.py: the values
        # read off the grids roms-tools built and the files it wrote.
        executor._update_settings_run_time(
            {
                "blk_frc": {"interp_frc": 1},
                "bgc": {"interp_frc": 1},
                "param": {"llm": 3, "mmm": 4, "n": 3, "np_xi": 2, "np_eta": 2},
                "extract_data": {
                    "n_chd": 100,
                    "theta_s_chd": 5.0,
                    "theta_b_chd": 2.0,
                    "hc_chd": 300.0,
                },
            }
        )
        executor._settings_compile_time["cppdefs"].update(
            obc_west=True, obc_east=True, obc_north=True, obc_south=True
        )

    @staticmethod
    def _configure(executor: ForgeExecutor, cfg: ForgeBlueprint) -> None:
        run_ov, compile_ov = split_model_settings(cfg)
        with (
            patch(
                "cstar.applications.forge.executor.render_roms_settings"
            ) as mock_render,
            patch("cstar.applications.forge.executor.write_roms_namelist"),
        ):
            mock_render.return_value = {
                "location": str(executor.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }
            executor.configure_build(
                compile_time_settings=compile_ov,
                run_time_settings=run_ov,
                explicit_overrides=cfg.composition.overrides,
            )

    def _stale_cfg(self) -> ForgeBlueprint:
        """A stored parent whose snapshot holds stale values for every kind of
        derived leaf: the resolver's old child fallbacks, a coarse-grid flag from
        the ModelSpec, a hand-edited tile count and boundary switch.
        """
        cfg = _build(
            grid_kwargs_child=_CHILD_GRID_KWARGS, metadata_child={"period": 3600.0}
        )
        settings = cfg.model_settings
        settings["extract_data"].update(n_chd=90, hc_chd=250.0)
        settings["blk_frc"]["interp_frc"] = False
        settings["bgc"]["interp_frc"] = False
        settings["param"]["np_xi"] = 1
        settings["cppdefs"]["obc_west"] = False
        return cfg

    def test_stale_snapshot_is_replaced(self) -> None:
        cfg = self._stale_cfg()
        executor = self._executor(cfg)
        self._simulate_generation(executor)
        self._configure(executor, cfg)

        run_time = executor._settings_run_time
        assert run_time["blk_frc"]["interp_frc"] == 1
        assert run_time["bgc"]["interp_frc"] == 1
        assert run_time["param"]["np_xi"] == 2
        assert run_time["extract_data"]["n_chd"] == 100
        assert run_time["extract_data"]["hc_chd"] == 300.0
        assert executor._settings_compile_time["cppdefs"]["obc_west"] is True

    def test_override_that_contradicts_a_generated_value(self) -> None:
        """``coarse_grid_mode: auto`` is only decided by roms-tools, so the
        conflict surfaces after generation.
        """
        cfg = self._stale_cfg()
        _with_coarse_grid_mode(cfg, CoarseGridMode.AUTO)
        cfg.composition.overrides = {"bgc": {"interp_frc": False}}
        executor = self._executor(cfg)
        self._simulate_generation(executor)
        with pytest.raises(ValueError, match="bgc.interp_frc is overridden to False"):
            self._configure(executor, cfg)
