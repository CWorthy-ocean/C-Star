"""Tests for trimming forcing files to a run window and checking coverage."""

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from cstar.roms.forcing_window import (
    FileTimes,
    UncoveredSpan,
    files_in_window,
    read_file_times,
    uncovered_variables,
)

REF = datetime(2000, 1, 1)
D0 = datetime(2020, 1, 1)


def d(n: float) -> datetime:
    """Return the datetime `n` days after 2020-01-01."""
    return D0 + timedelta(days=n)


def ft(first: float, last: float, *variables: str, cyclic: bool = False) -> FileTimes:
    """Build a FileTimes spanning day `first` to day `last`."""
    return FileTimes(
        frozenset(variables or ("a",)), {"time": (d(first), d(last))}, cyclic
    )


def _abs(first: datetime, last: datetime) -> FileTimes:
    return FileTimes(frozenset("a"), {"time": (first, last)}, False)


def monthly(n: int = 6, *variables: str) -> list[FileTimes]:
    """Build `n` files of 10 days each, day 0-9, 10-19, ... (daily records)."""
    return [ft(10 * i, 10 * i + 9, *variables) for i in range(n)]


class TestFilesInWindowSingleStream:
    """Trimming one stream of files around the window."""

    def test_start_between_files_keeps_earlier_file(self) -> None:
        """The last record before the start lies in the earlier file."""
        keep = files_in_window(monthly(), d(19.5), d(31))
        assert keep == [False, True, True, True, False, False]

    def test_start_on_first_record_keeps_previous_file(self) -> None:
        """The start bound is strict, so the previous file stays."""
        keep = files_in_window(monthly(), d(20), d(31))
        assert keep == [False, True, True, True, False, False]

    def test_start_inside_file_drops_earlier_files(self) -> None:
        """Only the file holding the start and later ones are needed."""
        keep = files_in_window(monthly(), d(25), d(31))
        assert keep == [False, False, True, True, False, False]

    def test_end_on_last_record_keeps_next_file(self) -> None:
        """The end bound is strict, so the next file stays."""
        keep = files_in_window(monthly(), d(21), d(29))
        assert keep == [False, False, True, True, False, False]

    def test_end_between_files(self) -> None:
        """The first record after the end lies in the next file."""
        keep = files_in_window(monthly(), d(21), d(29.5))
        assert keep == [False, False, True, True, False, False]

    def test_window_inside_one_file(self) -> None:
        """A window inside one file keeps only that file."""
        keep = files_in_window(monthly(), d(32), d(35))
        assert keep == [False, False, False, True, False, False]

    def test_end_none_keeps_everything_after_start(self) -> None:
        """No end means no trimming on that side."""
        keep = files_in_window(monthly(), d(25), None)
        assert keep == [False, False, True, True, True, True]

    def test_all_before_keeps_last_only(self) -> None:
        """A window after every file keeps the last file."""
        keep = files_in_window(monthly(), d(500), d(600))
        assert keep == [False] * 5 + [True]

    def test_all_after_keeps_first_only(self) -> None:
        """A window before every file keeps the first file."""
        keep = files_in_window(monthly(), d(-50), d(-40))
        assert keep == [True] + [False] * 5

    def test_empty_list(self) -> None:
        """No files gives no flags."""
        assert files_in_window([], d(0), d(1)) == []

    def test_single_holder_kept(self) -> None:
        """A variable held by one file keeps that file wherever the window is."""
        assert files_in_window([ft(0, 9, "solo")], d(100), d(110)) == [True]

    def test_absolute_time_comparison(self) -> None:
        """Spans from different reference dates compare as absolute datetimes."""
        a = _abs(datetime(2020, 1, 1), datetime(2020, 1, 10))
        b = _abs(datetime(2020, 1, 11), datetime(2020, 1, 20))
        c = _abs(datetime(2020, 1, 21), datetime(2020, 1, 30))
        assert files_in_window(
            [a, b, c], datetime(2020, 1, 15), datetime(2020, 1, 16)
        ) == [
            False,
            True,
            False,
        ]


class TestFilesInWindowStreams:
    """Variables from different streams are bracketed independently."""

    def test_streams_with_different_cadence(self) -> None:
        """A BGC stream of one record per file keeps its own bracket."""
        phys = monthly(6, "uwnd")
        bgc = [ft(10 * i, 10 * i, "dic") for i in range(6)]
        keep = files_in_window(phys + bgc, d(25), d(31))
        assert keep[:6] == [False, False, True, True, False, False]
        # records at 20, 30, 40: last before start is 20, first after end is 40
        assert keep[6:] == [False, False, True, True, True, False]

    def test_bgc_not_dropped_by_merged_physics_bracket(self) -> None:
        """Sparse files stay although dense files of another stream sit between."""
        phys = [ft(i, i, "uwnd") for i in range(0, 60)]
        bgc = [ft(0, 0, "dic"), ft(30, 30, "dic"), ft(60, 60, "dic")]
        keep = files_in_window(phys + bgc, d(40), d(45))
        assert keep[60:] == [False, True, True]

    def test_variable_gains_sibling_midway(self) -> None:
        """Files 1-3 hold {a}, files 4-6 hold {a, b}; nothing needed is dropped."""
        times = [ft(10 * i, 10 * i + 9, "a") for i in range(3)] + [
            ft(10 * i, 10 * i + 9, "a", "b") for i in range(3, 6)
        ]
        keep = files_in_window(times, d(45), d(52))
        assert keep == [False, False, False, False, True, True]
        # b has a single holder before the window: b's holders are 3, 4, 5
        keep = files_in_window(times, d(5), d(12))
        assert keep[:2] == [True, True]
        assert keep[2] is False

    def test_cross_kind_variable_keeps_bracket(self) -> None:
        """A variable in files whose other variables differ keeps its bracket."""
        times = [
            ft(0, 9, "a", "x"),
            ft(10, 19, "a", "x"),
            ft(20, 29, "a", "y"),
            ft(30, 39, "a", "y"),
        ]
        keep = files_in_window(times, d(15), d(25))
        assert keep == [False, True, True, False]

    def test_none_always_kept(self) -> None:
        """An unreadable file is kept."""
        times: list[FileTimes | None] = [None, *monthly(3), None]
        keep = files_in_window(times, d(25), d(26))
        assert keep == [True, False, False, True, True]

    def test_cyclic_kept_and_variable_never_trimmed(self) -> None:
        """A cyclic file is kept and its variables are not trimmed elsewhere."""
        clim = ft(0, 364, "a", cyclic=True)
        times = [ft(0, 9, "a"), ft(10, 19, "a"), clim, ft(20, 29, "a")]
        assert files_in_window(times, d(25), d(26)) == [True, True, True, True]

    def test_file_without_variables_kept(self) -> None:
        """A file with no forcing variables is kept."""
        empty = FileTimes(frozenset(), {"time": (d(0), d(1))}, False)
        assert files_in_window([empty], d(100), d(101)) == [True]


class TestFilesInWindowTimeVariables:
    """Each time variable of a file is bracketed on its own."""

    def test_second_time_variable_does_not_hide_bracket(self) -> None:
        """A record on another time variable must not drop the bracketing file."""
        f0 = FileTimes(frozenset({"v"}), {"tv": (d(0), d(12))}, False)
        f1 = FileTimes(
            frozenset({"v"}), {"tv": (d(30), d(40)), "tw": (d(15), d(35))}, False
        )
        assert files_in_window([f0, f1], d(22), d(35)) == [True, True]

    def test_time_and_rad_time_offset(self) -> None:
        """A rad_time 30 min before time keeps the previous month at a boundary."""
        half = timedelta(minutes=30)

        def month(a: float, b: float) -> FileTimes:
            return FileTimes(
                frozenset({"uwnd"}),
                {"time": (d(a), d(b)), "rad_time": (d(a) - half, d(b) - half)},
                False,
            )

        times = [month(0, 30), month(31, 60), month(61, 90)]
        # segment starts exactly on the second month's first record
        assert files_in_window(times, d(31), d(45)) == [True, True, False]
        assert files_in_window(times, d(31) - half, d(45)) == [True, True, False]


class TestUncoveredVariables:
    """Detecting variables whose files miss the window."""

    def test_one_covering_time_variable_is_enough(self) -> None:
        """`time` covers the window although `rad_time` ends 30 min early."""
        half = timedelta(minutes=30)
        f = FileTimes(
            frozenset({"v"}),
            {"time": (d(0), d(20)), "rad_time": (d(0) - half, d(20) - half)},
            False,
        )
        assert uncovered_variables([f], ["s"], d(0), d(20)) == []

    def test_mixed_shortfall_is_refused_without_flags(self) -> None:
        """Each time variable falls short on a different side: refuse, no side named."""
        f = FileTimes(
            frozenset({"v"}),
            {"a": (d(5), d(30)), "b": (d(0), d(10))},
            False,
        )
        got = uncovered_variables([f], ["s"], d(0), d(20))
        assert got == [UncoveredSpan((0,), ("v",), d(0), d(30), False, False)]

    def test_refused_at_start(self) -> None:
        """First record after the start is reported."""
        got = uncovered_variables([ft(5, 30, "a")], ["s"], d(0), d(20))
        assert got == [UncoveredSpan((0,), ("a",), d(5), d(30), True, False)]

    def test_refused_at_end(self) -> None:
        """Last record before the end is reported."""
        got = uncovered_variables([ft(0, 10, "a")], ["s"], d(0), d(20))
        assert got == [UncoveredSpan((0,), ("a",), d(0), d(10), False, True)]

    def test_exact_edges_pass(self) -> None:
        """Records on the window edges, or within one second, pass."""
        assert uncovered_variables([ft(0, 20, "a")], ["s"], d(0), d(20)) == []
        edge = _abs(d(0) + timedelta(seconds=0.5), d(20) - timedelta(seconds=0.5))
        assert uncovered_variables([edge], ["s"], d(0), d(20)) == []

    def test_cyclic_ignored(self) -> None:
        """A variable held by a cyclic file is not checked."""
        times = [ft(5, 6, "a"), ft(0, 364, "a", cyclic=True)]
        assert uncovered_variables(times, ["s", "s"], d(0), d(20)) == []

    def test_ignore_honoured(self) -> None:
        """Ignored variables are not checked."""
        assert uncovered_variables([ft(5, 6, "a")], ["s"], d(0), d(20), ["a"]) == []

    def test_kind_with_unreadable_file_skipped(self) -> None:
        """An unreadable file of the same kind might hold the variable."""
        times: list[FileTimes | None] = [ft(5, 6, "a"), None, ft(5, 6, "b")]
        got = uncovered_variables(
            times, ["surface", "surface", "boundary"], d(0), d(20)
        )
        assert [g.variables for g in got] == [("b",)]

    def test_grouping(self) -> None:
        """Variables sharing files and span are one entry."""
        got = uncovered_variables([ft(5, 6, "b", "a")], ["s"], d(0), d(20))
        assert got == [UncoveredSpan((0,), ("a", "b"), d(5), d(6), True, True)]

    def test_distinct_spans_not_grouped_and_sorted(self) -> None:
        """Different files give separate entries ordered by first file."""
        times = [ft(5, 6, "a"), ft(7, 8, "b")]
        got = uncovered_variables(times, ["s", "s"], d(0), d(20))
        assert [g.variables for g in got] == [("a",), ("b",)]

    def test_lone_stale_file_reported(self) -> None:
        """A single file ending before the window is caught."""
        times = [ft(0, 30, "a"), ft(0, 3, "stale")]
        got = uncovered_variables(times, ["s", "s"], d(0), d(20))
        assert [g.variables for g in got] == [("stale",)]

    def test_combined_holders_cover(self) -> None:
        """Two files together covering the window pass."""
        times = [ft(0, 10, "a"), ft(10, 25, "a")]
        assert uncovered_variables(times, ["s", "s"], d(0), d(20)) == []


def _days(start: float, stop: float, n: int) -> np.ndarray:
    return np.linspace(start, stop, n)


def _write(path: Path, ds: xr.Dataset) -> Path:
    ds.to_netcdf(path)
    return path


def _ref_days(date: datetime) -> float:
    return (date - REF).total_seconds() / 86400


class TestReadFileTimes:
    """Reading a file's variables and span from tiny netCDF files."""

    def test_roms_tools_surface(self, tmp_path: Path) -> None:
        """Only `time`/`rad_time` give the span; abs_time and time vars are not variables."""
        t = _days(100, 102, 3)
        ds = xr.Dataset(
            {
                "time": (
                    "time",
                    t,
                    {"long_name": "relative time: days since 2000-01-01 00:00:00"},
                ),
                "rad_time": (
                    "time",
                    t + 0.5,
                    {"long_name": "relative time: days since 2000-01-01 00:00:00"},
                ),
                "abs_time": ("time", t * 24 + 1e6, {"units": "hours since 1900-01-01"}),
                "uwnd": (("time", "eta_rho", "xi_rho"), np.zeros((3, 2, 2))),
            },
            attrs={"model_reference_date": "2000-01-01 00:00:00"},
        )
        got = read_file_times(_write(tmp_path / "s.nc", ds))
        assert got == FileTimes(
            frozenset({"uwnd"}),
            {
                "time": (REF + timedelta(days=100), REF + timedelta(days=102)),
                "rad_time": (REF + timedelta(days=100.5), REF + timedelta(days=102.5)),
            },
            False,
        )
        assert got.first == REF + timedelta(days=100)
        assert got.last == REF + timedelta(days=102.5)

    def test_cdrgas_like(self, tmp_path: Path) -> None:
        """ocean_time is ignored for values but supplies the reference date."""
        ds = xr.Dataset(
            {
                "ocean_time": (
                    "time",
                    [1e9, 2e9],
                    {"long_name": "Time since 2012/01/01"},
                ),
                "ddic_dco2_time": ("time", [10.0, 20.0]),
                "ddic_dco2": (("time", "eta_rho", "xi_rho"), np.zeros((2, 2, 2))),
            }
        )
        got = read_file_times(_write(tmp_path / "c.nc", ds))
        assert got == FileTimes(
            frozenset({"ddic_dco2"}),
            {"ddic_dco2_time": (datetime(2012, 1, 11), datetime(2012, 1, 21))},
            False,
        )

    def test_boundary_like(self, tmp_path: Path) -> None:
        """A `bry_time` dimension carries its variables."""
        ds = xr.Dataset(
            {
                "bry_time": (
                    "bry_time",
                    [1.0, 2.0],
                    {"long_name": "Time since 2012/01/01"},
                ),
                "temp_south": (("bry_time", "s_rho", "xi_rho"), np.zeros((2, 2, 2))),
                "zeta_south": (("bry_time", "xi_rho"), np.zeros((2, 2))),
                "static": (("xi_rho",), np.zeros(2)),
            }
        )
        got = read_file_times(_write(tmp_path / "b.nc", ds))
        assert got is not None
        assert got.variables == {"temp_south", "zeta_south"}
        assert got.first == datetime(2012, 1, 2)

    def test_only_endpoints_read(self, tmp_path: Path) -> None:
        """An interior record beyond the last one does not move `last`."""
        ds = xr.Dataset(
            {
                "river_time": (
                    "river_time",
                    [1.0, 99.0, 3.0],
                    {"long_name": "relative time: days since 2012-01-01"},
                ),
                "river_volume": (("river_time", "nriver"), np.zeros((3, 1))),
            }
        )
        got = read_file_times(_write(tmp_path / "r.nc", ds))
        assert got is not None
        assert got.last == datetime(2012, 1, 4)
        assert got.first == datetime(2012, 1, 2)

    def test_non_finite_endpoint_falls_back_to_whole_variable(
        self, tmp_path: Path
    ) -> None:
        """A NaN last record is dropped and the last finite one is used."""
        ds = xr.Dataset(
            {
                "river_time": (
                    "river_time",
                    [1.0, 5.0, np.nan],
                    {"long_name": "relative time: days since 2012-01-01"},
                ),
                "river_volume": (("river_time", "nriver"), np.zeros((3, 1))),
            }
        )
        got = read_file_times(_write(tmp_path / "r.nc", ds))
        assert got is not None
        assert got.last == datetime(2012, 1, 6)

    def test_no_reference_date(self, tmp_path: Path) -> None:
        """Without a recorded reference date the file cannot be placed."""
        ds = xr.Dataset(
            {"time": ("time", [1.0]), "a": (("time", "x"), np.zeros((1, 1)))}
        )
        assert read_file_times(_write(tmp_path / "n.nc", ds)) is None

    def test_no_time_variable(self, tmp_path: Path) -> None:
        """A tidal-like file has no forcing time."""
        ds = xr.Dataset(
            {
                "tide_period": ("ntides", [12.0, 24.0]),
                "tide_Ephase": (("ntides", "x"), np.zeros((2, 1))),
            },
            attrs={"model_reference_date": "2000-01-01 00:00:00"},
        )
        assert read_file_times(_write(tmp_path / "t.nc", ds)) is None

    def test_not_netcdf(self, tmp_path: Path) -> None:
        """A file that is not netCDF is None, as is a missing one."""
        bad = tmp_path / "bad.nc"
        bad.write_bytes(b"this is not netcdf")
        assert read_file_times(bad) is None
        assert read_file_times(tmp_path / "missing.nc") is None

    @pytest.mark.parametrize("how", ["cycle_length", "climatology"])
    def test_climatology_is_cyclic(self, tmp_path: Path, how: str) -> None:
        """Either marker of a climatology sets `cyclic`."""
        attrs = {"model_reference_date": "2000-01-01 00:00:00"}
        dust_attrs: dict[str, object] = {}
        if how == "cycle_length":
            dust_attrs["cycle_length"] = 365.25
        else:
            attrs["climatology"] = "True"
        ds = xr.Dataset(
            {
                "dust_time": ("dust_time", [15.0, 45.0], dust_attrs),
                "dust": (("dust_time", "x"), np.zeros((2, 1))),
            },
            attrs=attrs,
        )
        got = read_file_times(_write(tmp_path / "d.nc", ds))
        assert got is not None
        assert got.cyclic is True
        assert got.variables == {"dust"}
