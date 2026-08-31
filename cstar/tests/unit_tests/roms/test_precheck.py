"""Tests for `cstar.roms.precheck.check_output_streams_divide_rst`.

Mirrors ucla-roms' `src/precheck.F90::do_precheck`/`check_output_divides_rst`:
for every *enabled* output stream, `nrpf * output_period` must be positive and
must evenly divide `basic_output_settings.output_period_rst`, when restarts
are on (`wrt_file_rst`). Four stream groups are additionally gated on a
compile-time cppdef.

Operates on C-Star's canonical namelist vocabulary (RomsNamelistBase group
field names + real Fortran namelist keys), NOT forge's settings-dict
vocabulary -- every settings dict built here uses e.g.
`frc_output_settings.output_period_frc`, not forge's `frc_output.output_period`.
"""

import pytest

from cstar.roms.precheck import _STREAM_CHECKS, check_output_streams_divide_rst


def _base_settings(**overrides):
    """A minimal, conforming settings mapping: restarts on, daily period,
    every stream disabled (so the per-stream gates all skip) except what a
    test overrides.
    """
    settings = {
        "basic_output_settings": {
            "wrt_file_rst": True,
            "output_period_rst": 86400,
            "wrt_file_his": False,
            "output_period_his": 86400,
            "nrpf_his": 1,
            "wrt_file_avg": False,
            "output_period_avg": 86400,
            "nrpf_avg": 1,
        },
        "frc_output_settings": {
            "wrt_frc": False,
            "output_period_frc": 3600,
            "nrpf_frc": 4,
        },
        "extract_data_settings": {
            "do_extract": False,
            "output_period_extract": 3600,
            "nrpf_extract": 24,
        },
        "cdr_output_settings": {
            "do_cdr_output": False,
            "output_period_cdr": 3600,
            "nrpf_cdr": 24,
        },
        "upscale_settings": {
            "do_upscale": False,
            "output_period_uscl": 3600,
            "nrpf_uscl": 24,
        },
        "bgc_settings": {
            "wrt_bgc_his": False,
            "output_period_bgc_his": 86400,
            "nrpf_bgc_his": 1,
            "wrt_bgc_avg": False,
            "output_period_bgc_avg": 86400,
            "nrpf_bgc_avg": 1,
            "wrt_bgc_dia_his": False,
            "output_period_bgc_his_dia": 86400,
            "nrpf_bgc_his_dia": 1,
            "wrt_bgc_dia_avg": False,
            "output_period_bgc_avg_dia": 86400,
            "nrpf_bgc_avg_dia": 1,
        },
    }
    settings.update(overrides)
    return settings


def test_conforming_config_passes():
    """A stream whose `nrpf * output_period` evenly divides `output_period_rst`
    raises nothing.
    """
    settings = _base_settings()
    settings["frc_output_settings"] = {
        "wrt_frc": True,
        "output_period_frc": 3600,
        "nrpf_frc": 4,
    }
    # 4 * 3600 = 14400 s; 86400 / 14400 = 6 -> evenly divides.
    check_output_streams_divide_rst(settings, cppdefs={})


def test_cppdef_inactive_skips_even_when_it_would_fail():
    """A cppdef-gated stream that would otherwise violate the rule is skipped
    entirely when its guarding cppdef is not active -- ucla-roms never
    compiles that stream's write calls in that build.
    """
    settings = _base_settings()
    # 24 * 3600 = 86400 s doesn't matter here -- pick a genuinely non-dividing
    # combination to prove it's really skipped, not accidentally conforming.
    settings["cdr_output_settings"] = {
        "do_cdr_output": True,
        "output_period_cdr": 1000,
        "nrpf_cdr": 3,
    }
    # cppdefs omits "cdr_forcing" (and "marbl_diags") -> guard unsatisfied.
    check_output_streams_divide_rst(settings, cppdefs={"marbl": True})


def test_enabled_cppdef_active_non_dividing_raises():
    """An enabled, cppdef-active stream whose frequency doesn't evenly divide
    the restart period raises.
    """
    settings = _base_settings()
    settings["cdr_output_settings"] = {
        "do_cdr_output": True,
        "output_period_cdr": 1000,
        "nrpf_cdr": 3,
    }
    with pytest.raises(ValueError, match="cdr_output_settings"):
        check_output_streams_divide_rst(
            settings,
            cppdefs={"marbl": True, "marbl_diags": True, "cdr_forcing": True},
        )


def test_non_positive_newfile_freq_raises():
    """`newfile_freq <= 0` (e.g. `nrpf == 0`) raises even though `0` divides
    everything arithmetically -- ucla-roms treats it as a distinct violation
    (a stream that never rolls a file at all).
    """
    settings = _base_settings()
    settings["frc_output_settings"] = {
        "wrt_frc": True,
        "output_period_frc": 3600,
        "nrpf_frc": 0,
    }
    with pytest.raises(ValueError, match="frc_output_settings"):
        check_output_streams_divide_rst(settings, cppdefs={})


def test_wrt_file_rst_false_never_raises():
    """The global gate: `wrt_file_rst` false means nothing is checked, no
    matter how badly a stream would otherwise violate the rule.
    """
    settings = _base_settings()
    settings["basic_output_settings"]["wrt_file_rst"] = False
    settings["frc_output_settings"] = {
        "wrt_frc": True,
        "output_period_frc": 1000,
        "nrpf_frc": 3,
    }
    check_output_streams_divide_rst(settings, cppdefs={})


def test_output_period_rst_zero_passes_trivially():
    """`output_period_rst == 0` (the monthly-restart convention) passes
    trivially for every stream, since `mod(0, x) == 0`.
    """
    settings = _base_settings()
    settings["basic_output_settings"]["output_period_rst"] = 0
    settings["frc_output_settings"] = {
        "wrt_frc": True,
        "output_period_frc": 1000,
        "nrpf_frc": 3,
    }
    check_output_streams_divide_rst(settings, cppdefs={})


def test_bgc_stream_gated_on_marbl_or_bec2():
    """The four `bgc_*` streams are gated on `MARBL || BIOLOGY_BEC2`, not on
    `marbl_diags` (unlike `cdr`/`upscale`).
    """
    settings = _base_settings()
    settings["bgc_settings"]["wrt_bgc_his"] = True
    settings["bgc_settings"]["output_period_bgc_his"] = 1000
    settings["bgc_settings"]["nrpf_bgc_his"] = 3
    # Not active under either MARBL or BIOLOGY_BEC2 -> skipped.
    check_output_streams_divide_rst(settings, cppdefs={})
    # Active under BIOLOGY_BEC2 alone -> now checked, and it violates.
    with pytest.raises(ValueError, match="bgc_settings"):
        check_output_streams_divide_rst(settings, cppdefs={"biology_bec2": True})


def test_missing_section_is_skipped_not_an_error():
    """A settings mapping that omits a stream's section entirely (a partial
    dict) is skipped for that stream rather than raising an unrelated error --
    field/section presence is owned by schema validation upstream.
    """
    settings = {
        "basic_output_settings": {
            "wrt_file_rst": True,
            "output_period_rst": 86400,
        }
    }
    check_output_streams_divide_rst(settings, cppdefs={"marbl": True})


def test_sflx_or_gate_fires_when_only_one_field_present():
    """Regression: a missing OR-partner gate field must not skip the check.
    `sflx`'s gate is `wrt_smflx .or. wrt_stflx`; only `wrt_smflx` is present
    (True) here, `wrt_stflx` is entirely absent -- the stream must still be
    treated as enabled (a missing field reads as False, not "unknown").
    """
    settings = {
        "basic_output_settings": {"wrt_file_rst": True, "output_period_rst": 86400},
        "surf_flx_output_settings": {
            "wrt_smflx": True,
            "output_period_sflx": 1000,
            "nrpf_sflx": 3,
        },
    }
    # 3 * 1000 = 3000 s; 86400 % 3000 = 2400 != 0 -> non-dividing.
    with pytest.raises(ValueError, match="surf_flx_output_settings"):
        check_output_streams_divide_rst(settings, cppdefs={})


def test_diagnostics_or_gate_fires_when_only_one_field_present():
    """Same OR-gate regression as `sflx`, for `diagnostics` (`diag_uv .or.
    diag_trc`) -- also confirms the `diagnostics` cppdef guard is satisfiable.
    """
    settings = {
        "basic_output_settings": {"wrt_file_rst": True, "output_period_rst": 86400},
        "diagnostics_settings": {
            "diag_trc": True,
            "output_period_diag": 1000,
            "nrpf_diag": 3,
        },
    }
    with pytest.raises(ValueError, match="diagnostics_settings"):
        check_output_streams_divide_rst(settings, cppdefs={"diagnostics": True})


# --- Table-driven coverage of every `_STREAM_CHECKS` row -------------------
#
# `_get` silently returns None on a wrong/renamed field name, and the row
# then just `continue`s (skipped) -- so a field-name/alias drift in an
# untested row would otherwise disable that row's check with no test
# failure. These two parametrized tests exercise all 16 rows directly from
# the table itself, proving every row's section/gate/period/nrpf field names
# actually resolve against a real settings dict.

_DIVIDING_NRPF, _DIVIDING_PERIOD = 24, 3600  # 24 * 3600 = 86400 -> divides evenly
_NON_DIVIDING_NRPF, _NON_DIVIDING_PERIOD = 3, 1000  # 3000 -> does not divide 86400
_RST_PERIOD = 86400


def _settings_for_row(row, *, nrpf, period):
    """Minimal settings dict enabling exactly `row`'s stream, with its
    section holding every gate field True plus the given nrpf/period.
    """
    settings = {
        "basic_output_settings": {
            "wrt_file_rst": True,
            "output_period_rst": _RST_PERIOD,
        }
    }
    section = settings.setdefault(row.section, {})
    for field in row.gate_fields:
        section[field] = True
    section[row.nrpf_field] = nrpf
    section[row.period_field] = period
    return settings


def _cppdefs_for_row(row):
    """A cppdefs mapping that satisfies `row`'s cppdef guard (empty if none)."""
    cppdefs = {}
    for name in row.cppdef_guard:
        # "marbl_or_bec2" is the synthetic name for `MARBL || BIOLOGY_BEC2`
        # (see `_cppdef_guard_satisfied`) -- satisfy it via `marbl`.
        cppdefs["marbl" if name == "marbl_or_bec2" else name] = True
    return cppdefs


@pytest.mark.parametrize("row", _STREAM_CHECKS, ids=lambda r: r.label)
def test_every_stream_row_raises_when_enabled_active_and_non_dividing(row):
    settings = _settings_for_row(
        row, nrpf=_NON_DIVIDING_NRPF, period=_NON_DIVIDING_PERIOD
    )
    with pytest.raises(ValueError, match=row.label):
        check_output_streams_divide_rst(settings, cppdefs=_cppdefs_for_row(row))


@pytest.mark.parametrize("row", _STREAM_CHECKS, ids=lambda r: r.label)
def test_every_stream_row_passes_when_enabled_active_and_dividing(row):
    settings = _settings_for_row(row, nrpf=_DIVIDING_NRPF, period=_DIVIDING_PERIOD)
    check_output_streams_divide_rst(settings, cppdefs=_cppdefs_for_row(row))


@pytest.mark.parametrize(
    "row", [r for r in _STREAM_CHECKS if r.cppdef_guard], ids=lambda r: r.label
)
def test_every_guarded_row_skipped_when_cppdef_inactive(row):
    """Every cppdef-gated row (diagnostics, cdr, upscale, the four bgc_*
    streams) is skipped -- even with a genuinely non-dividing config -- when
    its guard is not satisfied.
    """
    settings = _settings_for_row(
        row, nrpf=_NON_DIVIDING_NRPF, period=_NON_DIVIDING_PERIOD
    )
    check_output_streams_divide_rst(settings, cppdefs={})
