"""Tests for Rules E and F, `cstar.roms.precheck.check_gpu_features` and
`check_gpu_array_sizes`.

Mirrors the GPU model's start-up guard (gpu-ucla-roms ``src/host/feature_guard.F90``
at v0.1.2-beta): fifteen switches stop the run, five more only warn. The tables
below restate upstream's calls as literals, so that a drift in the module's data
shows up as a failing test.
"""

import warnings
from pathlib import Path

import pytest

from cstar.roms.namelist import RomsNamelistV0_5_0, RomsNamelistV0_11_0
from cstar.roms.precheck import (
    _GPU_ARRAY_SIZES,
    _GPU_STOP_SWITCHES,
    _GPU_WARN_SWITCHES,
    NamelistConsistencyError,
    check_gpu_array_sizes,
    check_gpu_features,
)

FIXTURES = Path(__file__).parent / "fixtures"
V0_5_0_NAMELIST = FIXTURES / "example_namelist_v0_5_0.nml"
V0_11_0_NAMELIST = FIXTURES / "example_namelist_v0_11_0.nml"

# (group, key, what, gate) for each `need_off` call, in upstream's order. The gate is
# the `(group, key)` of the `is_true` around the call, or None.
STOP = [
    ("river_frc_settings", "river_analytical", "analytical river forcing",
     ("river_frc_settings", "river_source")),
    ("pipe_frc_settings", "pipe_source", "pipe (point source) forcing", None),
    ("cdr_output_settings", "cdr_monthly_averages", "monthly CDR averages",
     ("cdr_output_settings", "do_cdr_output")),
    ("cdr_tracer_output_settings", "do_cdr_tracer_output", "CDR tracer output", None),
    ("cdr_gas_exch_output_settings", "cdr_gas_monthly_averages",
     "monthly CDR gas-exchange averages",
     ("cdr_gas_exch_output_settings", "do_cdr_gas_exch_output")),
    ("cdr_lite_output_settings", "cdr_lite_monthly_averages",
     "monthly CDR_LITE averages", ("cdr_lite_output_settings", "do_cdr_lite_output")),
    ("zslice_settings", "do_zslice", "z-slice output", None),
    ("particles_settings", "floats", "particles (floats)", None),
    ("frc_output_settings", "wrt_frc", "forcing output", None),
    ("surf_flx_output_settings", "wrt_smflx", "surface momentum flux output", None),
    ("surf_flx_output_settings", "wrt_stflx", "surface tracer flux output", None),
    ("basic_output_settings", "monthly_restarts", "monthly restarts", None),
    ("bgc_settings", "wrt_bgc_dia_avg", "BGC diagnostic averages output", None),
    ("tidal_frc_settings", "ana_tides", "analytical tides", None),
    ("tides_settings", "ana_tides", "analytical tides", None),
]  # fmt: skip

# (group, key, the model's warning sentence) for each `warn_on` call, in order.
WARN = [
    ("diagnostics_settings", "diag_uv",
     "momentum budget diagnostics (DIAGNOSTICS cpp key on the CPU) are not on the GPU"),
    ("diagnostics_settings", "diag_trc",
     "tracer budget diagnostics (DIAGNOSTICS cpp key on the CPU) are not on the GPU"),
    ("sponge_tune_settings", "ub_tune",
     "sponge tuning (SPONGE_TUNE cpp key on the CPU) is not on the GPU"),
    ("sponge_tune_settings", "wrt_sponge",
     "sponge tuning output (SPONGE_TUNE cpp key on the CPU) is not on the GPU"),
    ("calc_pflx_settings", "calc_pflx",
     "pressure-flux fields for nesting are not computed; restarts will not hold them"),
]  # fmt: skip


def _on(*switches):
    """A settings mapping with each ``(group, key)`` switched on."""
    settings: dict[str, dict[str, bool]] = {}
    for group, key in switches:
        settings.setdefault(group, {})[key] = True
    return settings


def _with_gate(group, key, gate):
    """The switches to turn on for ``group.key`` to count: it, and its gate."""
    return [(group, key), *([gate] if gate else [])]


def _no_warnings(settings):
    """Run the check, failing the test on any warning."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        check_gpu_features(settings)


@pytest.fixture
def v0_11_0_namelist() -> RomsNamelistV0_11_0:
    """The 0.11.0 fixture with the one warn switch it has on (``calc_pflx``) off."""
    nml = RomsNamelistV0_11_0.read(V0_11_0_NAMELIST)
    nml.calc_pflx_settings.calc_pflx = False
    return nml


def test_tables_restate_upstream():
    assert [(s.group, s.key, s.what, s.when) for s in _GPU_STOP_SWITCHES] == STOP
    assert [(s.group, s.key, s.what, s.when) for s in _GPU_WARN_SWITCHES] == [
        (g, k, what, None) for g, k, what in WARN
    ]


@pytest.mark.parametrize(("group", "key", "what", "gate"), STOP)
def test_each_stop_switch_alone_raises(group, key, what, gate):
    with pytest.raises(NamelistConsistencyError) as raised:
        check_gpu_features(_on(*_with_gate(group, key, gate)))
    assert f"&{group} {key} = .true.: {what} is not available on the GPU model." in str(
        raised.value
    )
    assert raised.value.rule == "gpu_features"
    assert raised.value.section == group
    assert raised.value.keys == (key,)


@pytest.mark.parametrize(
    ("group", "key", "what", "gate"), [row for row in STOP if row[3]]
)
def test_a_gated_switch_counts_only_when_its_gate_is_on(group, key, what, gate):
    _no_warnings(_on((group, key)))  # gate absent
    _no_warnings({gate[0]: {gate[1]: False, key: True}})


def test_analytical_rivers_pass_while_river_source_is_off():
    _no_warnings(
        {"river_frc_settings": {"river_source": False, "river_analytical": True}}
    )


def test_a_gate_alone_is_not_a_violation():
    _no_warnings(
        _on(
            ("river_frc_settings", "river_source"),
            ("cdr_output_settings", "do_cdr_output"),
            ("cdr_gas_exch_output_settings", "do_cdr_gas_exch_output"),
            ("cdr_lite_output_settings", "do_cdr_lite_output"),
        )
    )


def test_every_stop_switch_on_at_once_is_listed_in_one_error():
    everything = [
        pair for group, key, _, gate in STOP for pair in _with_gate(group, key, gate)
    ]
    with pytest.raises(NamelistConsistencyError) as raised:
        check_gpu_features(_on(*everything))
    message = str(raised.value)
    lines = [ln for ln in message.splitlines() if ln.startswith("  &")]
    assert lines == [
        f"  &{g} {k} = .true.: {what} is not available on the GPU model."
        for g, k, what, _ in STOP
    ]
    assert message.endswith("Set them to .false. to run on the GPU.")
    # the first offender, in the model's order
    assert (raised.value.section, raised.value.keys) == (
        "river_frc_settings",
        ("river_analytical",),
    )


def test_two_offenders_are_both_reported_in_upstream_order():
    with pytest.raises(NamelistConsistencyError) as raised:
        check_gpu_features(
            _on(("zslice_settings", "do_zslice"), ("pipe_frc_settings", "pipe_source"))
        )
    message = str(raised.value)
    assert message.index("&pipe_frc_settings") < message.index("&zslice_settings")
    assert raised.value.section == "pipe_frc_settings"


def test_a_switch_is_on_only_when_it_is_true():
    _no_warnings(
        {
            "pipe_frc_settings": {"pipe_source": False},
            "zslice_settings": {"do_zslice": None},
            "particles_settings": {"floats": 1},  # not the boolean True
        }
    )


@pytest.mark.parametrize(
    "settings", [{}, {"unrelated": {"x": True}}, {"zslice_settings": {}}]
)
def test_absent_groups_and_keys_are_off(settings):
    _no_warnings(settings)


@pytest.mark.parametrize(("group", "key", "sentence"), WARN)
def test_each_warn_switch_warns_without_raising(group, key, sentence):
    with pytest.warns(UserWarning) as record:
        check_gpu_features(_on((group, key)))
    assert len(record) == 1
    assert f"&{group} {key} = .true.: {sentence}" in str(record[0].message)
    assert record[0].filename == __file__


def test_all_warn_switches_make_one_warning():
    with pytest.warns(UserWarning) as record:
        check_gpu_features(_on(*[(g, k) for g, k, _ in WARN]))
    assert len(record) == 1
    for group, key, sentence in WARN:
        assert f"&{group} {key} = .true.: {sentence}" in str(record[0].message)


def test_warnings_are_emitted_before_the_error_when_both_apply():
    with (
        pytest.warns(UserWarning, match="calc_pflx"),
        pytest.raises(NamelistConsistencyError, match="do_zslice"),
    ):
        check_gpu_features(
            _on(("zslice_settings", "do_zslice"), ("calc_pflx_settings", "calc_pflx"))
        )


def test_live_namelist_passes(v0_11_0_namelist):
    _no_warnings(v0_11_0_namelist)


def test_live_namelist_with_calc_pflx_on_warns():
    nml = RomsNamelistV0_11_0.read(V0_11_0_NAMELIST)  # the fixture has it on
    with pytest.warns(UserWarning, match="calc_pflx"):
        check_gpu_features(nml)


def test_live_namelist_with_stop_switches_on_raises(v0_11_0_namelist):
    v0_11_0_namelist.pipe_frc_settings.pipe_source = True
    v0_11_0_namelist.river_frc_settings.river_source = True
    v0_11_0_namelist.river_frc_settings.river_analytical = True
    v0_11_0_namelist.cdr_lite_output_settings.do_cdr_lite_output = True
    v0_11_0_namelist.cdr_lite_output_settings.cdr_lite_monthly_averages = True
    v0_11_0_namelist.basic_output_settings.monthly_restarts = True
    with pytest.raises(NamelistConsistencyError) as raised:
        check_gpu_features(v0_11_0_namelist)
    listed = [ln.split(" = ")[0] for ln in str(raised.value).splitlines() if "&" in ln]
    assert listed == [
        "  &river_frc_settings river_analytical",
        "  &pipe_frc_settings pipe_source",
        "  &cdr_lite_output_settings cdr_lite_monthly_averages",
        "  &basic_output_settings monthly_restarts",
    ]


def test_live_namelist_of_a_schema_without_the_group_is_fine():
    """The 0.5.0 schema has no ``cdr_lite_output_settings`` or ``pio_settings``."""
    nml = RomsNamelistV0_5_0.read(V0_5_0_NAMELIST)
    nml.calc_pflx_settings.calc_pflx = False
    _no_warnings(nml)


# Rule G: (group, key, size) of the arrays the GPU model declares smaller than
# ucla-roms, restated from upstream (`akt_bak_in(2)`, src/host/case_io.F90:96 at
# v0.1.2-beta).
ARRAY_SIZES = [("vertical_mixing_settings", "akt_bak", 2)]


def test_array_size_table_matches_upstream():
    assert list(_GPU_ARRAY_SIZES) == ARRAY_SIZES


@pytest.mark.parametrize("values", [[0.0], [1e-5, 1e-6]])
def test_akt_bak_within_the_gpu_size_passes(values):
    check_gpu_array_sizes({"vertical_mixing_settings": {"akt_bak": values}})


@pytest.mark.parametrize(
    "settings",
    [
        {"vertical_mixing_settings": {"akt_bak": 0.0}},
        {"vertical_mixing_settings": {"akv_bak": 0.0}},
        {},
    ],
)
def test_a_scalar_or_missing_akt_bak_passes(settings):
    check_gpu_array_sizes(settings)


def test_more_akt_bak_values_than_the_gpu_reads_raise():
    with pytest.raises(NamelistConsistencyError) as raised:
        check_gpu_array_sizes({"vertical_mixing_settings": {"akt_bak": [0.0] * 3}})
    assert raised.value.rule == "gpu_array_sizes"
    assert raised.value.section == "vertical_mixing_settings"
    assert raised.value.keys == ("akt_bak",)
    assert "&vertical_mixing_settings akt_bak: 3 values; the GPU model reads 2" in str(
        raised.value
    )
    assert "namelist_overrides" in str(raised.value)


def test_a_live_cpu_namelist_is_refused(v0_11_0_namelist):
    """A CPU 0.11.0 namelist gives one `akt_bak` per tracer."""
    assert len(v0_11_0_namelist.vertical_mixing_settings.akt_bak) > 2
    with pytest.raises(NamelistConsistencyError, match="akt_bak"):
        check_gpu_array_sizes(v0_11_0_namelist)


def test_a_live_namelist_with_two_values_passes(v0_11_0_namelist):
    v0_11_0_namelist.vertical_mixing_settings.akt_bak = [0.0, 0.0]
    check_gpu_array_sizes(v0_11_0_namelist)
