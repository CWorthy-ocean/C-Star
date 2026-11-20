"""Tests for the output-period, MARBL-step and output-in-window rules of
`cstar.roms.precheck` (Rules H, I and K).
"""

from datetime import datetime
from pathlib import Path

import pytest

from cstar.roms.namelist import RomsNamelistV0_15_0
from cstar.roms.precheck import (
    _STREAM_CHECKS,
    MAX_MARBL_TIMESTEP,
    NamelistConsistencyError,
    check_marbl_step_ceiling,
    check_output_periods_divisible_by_dt,
    check_output_written_in_window,
    output_period_drift_warnings,
)

V0_15_0_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist_v0_15_0.nml"

MARBL = {"marbl": True, "marbl_diags": True, "cdr_forcing": True}
START = datetime(2012, 1, 1)


def _settings(*, dt=960.0, ntimes=90, **groups):
    """Restarts every day, every other stream off, unless ``groups`` says otherwise
    (each entry is merged into the group of that name).
    """
    settings = {
        "time_stepping": {"dt": dt, "ntimes": ntimes},
        "basic_output_settings": {
            "wrt_file_rst": True,
            "output_period_rst": 86400.0,
            "monthly_restarts": False,
            "wrt_file_his": False,
            "output_period_his": 86400.0,
            "nrpf_his": 1,
            "wrt_file_avg": False,
            "output_period_avg": 86400.0,
            "nrpf_avg": 1,
        },
        "bgc_settings": {
            "wrt_bgc_his": False,
            "output_period_bgc_his": 86400.0,
            "nrpf_bgc_his": 1,
            "wrt_bgc_avg": False,
            "output_period_bgc_avg": 86400.0,
            "nrpf_bgc_avg": 1,
            "wrt_bgc_dia_his": False,
            "output_period_bgc_his_dia": 86400.0,
            "nrpf_bgc_his_dia": 1,
            "wrt_bgc_dia_avg": False,
            "output_period_bgc_avg_dia": 86400.0,
            "nrpf_bgc_avg_dia": 1,
        },
        "cdr_gas_exch_output_settings": {
            "do_cdr_gas_exch_output": False,
            "output_period_cdr_gas": 3600.0,
            "nrpf_cdr_gas": 24,
            "cdr_gas_monthly_averages": False,
        },
        "cdr_lite_output_settings": {
            "do_cdr_lite_output": False,
            "output_period_cdr_lite": 3600.0,
            "nrpf_cdr_lite": 24,
            "cdr_lite_monthly_averages": False,
        },
        "upscale_settings": {
            "do_upscale": False,
            "output_period_uscl": 3600.0,
            "nrpf_uscl": 24,
        },
        "marbl_biogeochemistry_settings": {"marbl_timestep": 3600.0},
    }
    for name, values in groups.items():
        settings[name] = {**settings.get(name, {}), **values}
    return settings


class TestOutputPeriodsDivisibleByDt:
    """Rule H: ucla-roms stops at start-up on a his/avg/BGC period off dt."""

    def test_hourly_history_with_dt_960_raises(self):
        settings = _settings(
            basic_output_settings={"wrt_file_his": True, "output_period_his": 3600.0}
        )
        with pytest.raises(NamelistConsistencyError) as exc_info:
            check_output_periods_divisible_by_dt(settings)
        exc = exc_info.value
        assert exc.rule == "output_periods_divisible_by_dt"
        assert exc.section == "basic_output_settings"
        assert exc.keys == ("output_period_his",)
        assert "3600.0 s" in str(exc) and "960.0 s" in str(exc)

    def test_hourly_history_with_dt_7200_raises(self):
        settings = _settings(
            dt=7200.0,
            basic_output_settings={"wrt_file_his": True, "output_period_his": 3600.0},
        )
        with pytest.raises(NamelistConsistencyError, match="output_period_his"):
            check_output_periods_divisible_by_dt(settings)

    def test_average_raises(self):
        settings = _settings(
            basic_output_settings={"wrt_file_avg": True, "output_period_avg": 1000.0}
        )
        with pytest.raises(NamelistConsistencyError, match="output_period_avg"):
            check_output_periods_divisible_by_dt(settings)

    def test_stream_switched_off_passes(self):
        settings = _settings(basic_output_settings={"output_period_his": 3600.0})
        check_output_periods_divisible_by_dt(settings)

    def test_multiple_of_dt_passes(self):
        settings = _settings(
            dt=900.0,
            basic_output_settings={"wrt_file_his": True, "output_period_his": 3600.0},
        )
        check_output_periods_divisible_by_dt(settings)

    @pytest.mark.parametrize(
        "switch, key",
        [
            ("wrt_bgc_his", "output_period_bgc_his"),
            ("wrt_bgc_avg", "output_period_bgc_avg"),
            ("wrt_bgc_dia_his", "output_period_bgc_his_dia"),
            ("wrt_bgc_dia_avg", "output_period_bgc_avg_dia"),
        ],
    )
    def test_bgc_streams_raise_in_a_marbl_build(self, switch, key):
        settings = _settings(bgc_settings={switch: True, key: 3600.0})
        with pytest.raises(NamelistConsistencyError, match=key):
            check_output_periods_divisible_by_dt(settings, MARBL)
        # not compiled without MARBL, so not checked
        check_output_periods_divisible_by_dt(settings, {})

    def test_bgc_diagnostics_need_marbl_diags(self):
        """`wrt_bgc_diags` checks its periods only in a MARBL_DIAGS build."""
        settings = _settings(
            bgc_settings={"wrt_bgc_dia_his": True, "output_period_bgc_his_dia": 3600.0}
        )
        check_output_periods_divisible_by_dt(settings, {"marbl": True})

    def test_other_streams_are_not_refused(self):
        settings = _settings(
            cdr_gas_exch_output_settings={"do_cdr_gas_exch_output": True}
        )
        check_output_periods_divisible_by_dt(settings, MARBL)

    def test_live_namelist(self):
        nml = RomsNamelistV0_15_0.read(V0_15_0_NAMELIST)
        nml.time_stepping.dt = 960.0
        nml.basic_output_settings.wrt_file_his = True
        nml.basic_output_settings.output_period_his = 3600.0
        with pytest.raises(NamelistConsistencyError, match="output_period_his"):
            check_output_periods_divisible_by_dt(nml)


def test_rule_h_rows_mirror_the_fortran_checks():
    """The rows ROMS checks against dt, by their (unique) period key, with the
    extra cppdefs the check needs: `init_avg_arrays` (his, avg), `wrt_bgc_tracers`
    and, in a MARBL_DIAGS build, `wrt_bgc_diags`.
    """
    rows = {
        row.period_field: row.divides_dt_cppdefs
        for row in _STREAM_CHECKS
        if row.period_divides_dt
    }
    assert rows == {
        "output_period_his": (),
        "output_period_avg": (),
        "output_period_bgc_his": (),
        "output_period_bgc_avg": (),
        "output_period_bgc_his_dia": ("marbl_diags",),
        "output_period_bgc_avg_dia": ("marbl_diags",),
    }


class TestOutputPeriodDriftWarnings:
    """The streams Rule H does not refuse drift to ceil(period / dt) * dt."""

    def test_hourly_cdr_gas_with_dt_960_warns_3840(self):
        settings = _settings(
            cdr_gas_exch_output_settings={"do_cdr_gas_exch_output": True}
        )
        (message,) = output_period_drift_warnings(settings, MARBL)
        assert "output_period_cdr_gas (3600.0 s)" in message
        assert "every 3840 s" in message

    def test_cdr_lite_is_unguarded(self):
        settings = _settings(cdr_lite_output_settings={"do_cdr_lite_output": True})
        (message,) = output_period_drift_warnings(settings)
        assert "every 3840 s" in message

    def test_monthly_averages_ignore_the_period(self):
        settings = _settings(
            cdr_lite_output_settings={
                "do_cdr_lite_output": True,
                "cdr_lite_monthly_averages": True,
            }
        )
        assert output_period_drift_warnings(settings) == []

    def test_refused_streams_and_multiples_do_not_warn(self):
        settings = _settings(
            dt=900.0,
            basic_output_settings={"wrt_file_his": True, "output_period_his": 1000.0},
            cdr_lite_output_settings={"do_cdr_lite_output": True},
        )
        assert output_period_drift_warnings(settings) == []

    def test_uncompiled_stream_does_not_warn(self):
        settings = _settings(
            cdr_gas_exch_output_settings={"do_cdr_gas_exch_output": True}
        )
        assert output_period_drift_warnings(settings, {}) == []


class TestMarblStepCeiling:
    """Rule I: ucla-roms stops when the MARBL step exceeds 10,800 s."""

    def test_dt_28800_with_marbl_raises(self):
        settings = _settings(dt=28800.0)
        with pytest.raises(NamelistConsistencyError) as exc_info:
            check_marbl_step_ceiling(settings, MARBL)
        exc = exc_info.value
        assert exc.rule == "marbl_step_ceiling"
        assert (exc.section, exc.keys) == ("time_stepping", ("dt",))
        assert "28800 s" in str(exc) and "10800 s" in str(exc)

    def test_dt_14400_with_marbl_raises(self):
        with pytest.raises(NamelistConsistencyError, match="14400"):
            check_marbl_step_ceiling(_settings(dt=14400.0), MARBL)

    def test_long_marbl_timestep_raises(self):
        settings = _settings(
            dt=3600.0, marbl_biogeochemistry_settings={"marbl_timestep": 14400.0}
        )
        with pytest.raises(NamelistConsistencyError) as exc_info:
            check_marbl_step_ceiling(settings, MARBL)
        assert exc_info.value.keys == ("marbl_timestep",)

    def test_marbl_timestep_rounded_down_to_the_limit_passes(self):
        """10,801 s at dt 3600 rounds down to 10,800 s, which ROMS accepts."""
        settings = _settings(
            dt=3600.0, marbl_biogeochemistry_settings={"marbl_timestep": 10801.0}
        )
        check_marbl_step_ceiling(settings, MARBL)

    def test_limit_itself_passes(self):
        check_marbl_step_ceiling(_settings(dt=MAX_MARBL_TIMESTEP), MARBL)

    def test_without_marbl_passes(self):
        check_marbl_step_ceiling(_settings(dt=28800.0), {})
        check_marbl_step_ceiling(_settings(dt=28800.0))

    def test_without_marbl_timestep_dt_alone_decides(self):
        settings = _settings(dt=28800.0)
        del settings["marbl_biogeochemistry_settings"]
        with pytest.raises(NamelistConsistencyError):
            check_marbl_step_ceiling(settings, MARBL)


class TestOutputWrittenInWindow:
    """Rule K: a run in which no stream writes loses its compute."""

    def test_one_day_with_weekly_restarts_raises(self):
        settings = _settings(
            dt=7200.0,
            ntimes=12,
            basic_output_settings={"output_period_rst": 604800.0},
        )
        with pytest.raises(NamelistConsistencyError) as exc_info:
            check_output_written_in_window(settings, MARBL, start_date=START)
        exc = exc_info.value
        assert exc.rule == "output_written_in_window"
        assert "restarts (every 604800 s)" in str(exc)

    @pytest.mark.parametrize(
        ("restarts", "keys", "advice"),
        [
            (
                {"output_period_rst": 604800.0},
                ("output_period_rst",),
                "Set basic_output_settings.output_period_rst to at most the run "
                "length (86400 s)",
            ),
            (
                {"monthly_restarts": True, "output_period_rst": 0},
                ("monthly_restarts", "output_period_rst"),
                "turn off basic_output_settings.monthly_restarts",
            ),
            (
                {"wrt_file_rst": False},
                ("wrt_file_rst", "output_period_rst"),
                "Turn on restarts (basic_output_settings.wrt_file_rst)",
            ),
        ],
        ids=["periodic", "monthly", "off"],
    )
    def test_advice_names_the_restart_setting_to_change(self, restarts, keys, advice):
        """The error points at what the user would change: the restart period, the
        monthly switch, or the restart switch itself.
        """
        settings = _settings(dt=7200.0, ntimes=12, basic_output_settings=restarts)
        with pytest.raises(NamelistConsistencyError) as exc_info:
            check_output_written_in_window(settings, MARBL, start_date=START)
        exc = exc_info.value
        assert exc.keys == keys
        assert advice in str(exc)
        # CDR gas exchange and upscaling do write at the end of a run
        assert "end-of-run" not in str(exc)

    def test_restart_inside_the_window_passes(self):
        check_output_written_in_window(
            _settings(dt=7200.0, ntimes=12), MARBL, start_date=START
        )

    def test_restart_one_step_after_the_end_passes(self):
        """An EXACT_RESTART build writes the step before the period ends."""
        settings = _settings(
            dt=7200.0, ntimes=11, basic_output_settings={"output_period_rst": 86400.0}
        )
        check_output_written_in_window(settings, MARBL, start_date=START)

    def test_monthly_restarts_inside_a_month_raise(self):
        settings = _settings(
            dt=7200.0,
            ntimes=12,
            basic_output_settings={"monthly_restarts": True, "output_period_rst": 0},
        )
        with pytest.raises(NamelistConsistencyError, match="restarts \\(monthly\\)"):
            check_output_written_in_window(settings, MARBL, start_date=START)

    def test_monthly_restarts_across_a_month_pass(self):
        settings = _settings(
            dt=7200.0,
            ntimes=12,
            basic_output_settings={"monthly_restarts": True, "output_period_rst": 0},
        )
        check_output_written_in_window(
            settings, MARBL, start_date=datetime(2012, 1, 31, 6)
        )

    def test_history_writes_the_initial_state(self):
        settings = _settings(
            dt=7200.0,
            ntimes=12,
            basic_output_settings={
                "output_period_rst": 604800.0,
                "wrt_file_his": True,
                "output_period_his": 604800.0,
            },
        )
        check_output_written_in_window(settings, MARBL, start_date=START)

    def test_average_that_never_ends_raises(self):
        settings = _settings(
            dt=7200.0,
            ntimes=12,
            basic_output_settings={
                "wrt_file_rst": False,
                "wrt_file_avg": True,
                "output_period_avg": 172800.0,
            },
        )
        with pytest.raises(
            NamelistConsistencyError, match="'avg' \\(every 172800 s\\)"
        ):
            check_output_written_in_window(settings, MARBL, start_date=START)

    @pytest.mark.parametrize(
        "group, switch",
        [
            ("cdr_gas_exch_output_settings", "do_cdr_gas_exch_output"),
            ("upscale_settings", "do_upscale"),
        ],
    )
    def test_end_of_run_writers_count_when_compiled(self, group, switch):
        settings = _settings(
            dt=7200.0,
            ntimes=12,
            basic_output_settings={"output_period_rst": 604800.0},
            **{group: {switch: True}},
        )
        check_output_written_in_window(
            settings, {**MARBL, "upscaling": True}, start_date=START
        )
        with pytest.raises(NamelistConsistencyError):
            check_output_written_in_window(settings, {}, start_date=START)

    def test_unknown_build_counts_gated_streams(self):
        settings = _settings(
            dt=7200.0,
            ntimes=12,
            basic_output_settings={"output_period_rst": 604800.0},
            cdr_gas_exch_output_settings={"do_cdr_gas_exch_output": True},
        )
        check_output_written_in_window(settings, None, start_date=START)

    def test_monthly_average_across_a_month_passes(self):
        settings = _settings(
            dt=7200.0,
            ntimes=12,
            basic_output_settings={"output_period_rst": 604800.0},
            cdr_lite_output_settings={
                "do_cdr_lite_output": True,
                "cdr_lite_monthly_averages": True,
            },
        )
        with pytest.raises(NamelistConsistencyError, match="'cdrtrc' \\(monthly\\)"):
            check_output_written_in_window(settings, {}, start_date=START)
        check_output_written_in_window(
            settings, {}, start_date=datetime(2012, 1, 31, 6)
        )

    def test_without_ntimes_skips(self):
        settings = _settings(
            dt=7200.0, ntimes=None, basic_output_settings={"output_period_rst": 1e9}
        )
        check_output_written_in_window(settings, {}, start_date=START)
