"""Forge's output-period, MARBL-step and output-in-window checks
(`check_output_cadence`) at the resolver and `validate_run_time_sections`; the
`configure_build` net is tested in `test_executor.py`
(`TestConfigureBuildOutputCadence`).
"""

import logging
from datetime import datetime

import pytest

from cstar.applications.forge.namelist_model import validate_run_time_sections
from cstar.applications.forge.resolve import build_forge_blueprint
from cstar.catalog.domain_catalog import default_catalog as _CATALOG
from cstar.roms.precheck import NamelistConsistencyError

_MODEL = "roms-marbl-0.15-default"
_RESOLVE_LOGGER = "cstar.applications.forge.resolve"
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
_HOURLY_HISTORY = {
    "ocean_vars": {"wrt_file_his": True, "output_period_his": 3600.0, "nrpf_his": 24}
}


def _build(**over):
    kw = dict(
        model_path=_CATALOG.model_path(_MODEL),
        grid_name="test-tiny",
        grid_kwargs=_GRID_KWARGS,
        open_boundaries={"south": False, "east": True, "north": True, "west": False},
        partitioning={"n_procs_x": 1, "n_procs_y": 1},
        start_date=datetime(2012, 1, 1),
        end_date=datetime(2012, 1, 2),
        dt=960,
        forcing_inputs=_CATALOG.forcing_data("glorys-era5-unified"),
        output_settings=_CATALOG.output_data("standard"),
    )
    kw.update(over)
    return build_forge_blueprint(**kw)


def _resolver_warnings(caplog):
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and r.name == _RESOLVE_LOGGER
    ]


class TestResolverOutputCadence:
    def test_hourly_history_with_dt_960_is_refused(self):
        with pytest.raises(
            NamelistConsistencyError,
            match=r"output_period_his \(3600\.0 s\).*time_stepping\.dt \(960",
        ):
            _build(run_time_overrides=_HOURLY_HISTORY)

    def test_hourly_history_with_dt_900_resolves(self):
        cfg = _build(dt=900, run_time_overrides=_HOURLY_HISTORY)
        assert cfg.model_settings["ocean_vars"]["output_period_his"] == 3600.0

    def test_dt_28800_with_marbl_is_refused(self):
        with pytest.raises(NamelistConsistencyError, match="MARBL step would be 28800"):
            _build(dt=28800)

    def test_dt_28800_without_marbl_resolves(self):
        cfg = _build(
            dt=28800,
            bgc_mode="none",
            forcing_inputs=_CATALOG.forcing_data("glorys-era5-physics"),
        )
        assert cfg.model_settings["time_stepping"]["dt"] == 28800

    def test_wio_toy_cfl_dt_with_marbl_is_refused(self):
        """The CFL dt of the bundled wio-toy grid is 28,800 s, too long for MARBL."""
        domain = _CATALOG.domain_data("wio-toy")
        with pytest.raises(NamelistConsistencyError, match="MARBL step"):
            _build(
                grid_name=domain["grid_name"],
                grid_kwargs=domain["grid_kwargs"],
                open_boundaries=domain["open_boundaries"],
                partitioning=domain["partitioning"],
                dt=None,
            )

    def test_hourly_cdr_gas_with_dt_960_warns_3840(self, caplog):
        with caplog.at_level(logging.WARNING, logger=_RESOLVE_LOGGER):
            _build(
                run_time_overrides={
                    "cdr_gas_exch_output": {"do_cdr_gas_exch_output": True}
                }
            )
        (message,) = [m for m in _resolver_warnings(caplog) if "cdr_gas" in m]
        assert "output_period_cdr_gas (3600.0 s)" in message
        assert "every 3840 s" in message

    @pytest.mark.parametrize("output", ["weekly-restarts", "monthly-restarts"])
    def test_one_day_window_writing_nothing_warns(self, caplog, output):
        with caplog.at_level(logging.WARNING, logger=_RESOLVE_LOGGER):
            cfg = _build(dt=7200, output_settings=_CATALOG.output_data(output))
        assert cfg is not None
        assert any(
            m.startswith("No output stream writes") for m in _resolver_warnings(caplog)
        )

    def test_one_day_window_with_daily_restarts_does_not_warn(self, caplog):
        with caplog.at_level(logging.WARNING, logger=_RESOLVE_LOGGER):
            _build(dt=7200, output_settings=_CATALOG.output_data("daily-restarts"))
        assert not any(
            m.startswith("No output stream writes") for m in _resolver_warnings(caplog)
        )


class TestValidateRunTimeSectionsOutputCadence:
    def test_hourly_history_with_dt_960_is_reported(self):
        settings = {
            **_build(dt=900).model_settings,
            "time_stepping": {"dt": 960.0, "ntimes": 90},
        }
        settings["ocean_vars"] = {
            **settings["ocean_vars"],
            "wrt_file_his": True,
            "output_period_his": 3600.0,
            "nrpf_his": 24,
        }
        problems = validate_run_time_sections(settings, roms_ref="0.15.0")
        assert any("output_period_his (3600.0 s)" in p for p in problems)

    def test_dt_28800_with_marbl_is_reported(self):
        settings = {
            **_build(dt=900).model_settings,
            "time_stepping": {"dt": 28800.0, "ntimes": 3},
        }
        problems = validate_run_time_sections(settings, roms_ref="0.15.0")
        assert any("MARBL step would be 28800" in p for p in problems)

    def test_clean_settings_have_no_problems(self):
        settings = _build(dt=900).model_settings
        assert validate_run_time_sections(settings, roms_ref="0.15.0") == []
