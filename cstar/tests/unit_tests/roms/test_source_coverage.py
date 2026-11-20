"""Tests for the forcing source coverage check and the directives that use it."""

import types
import typing as t
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from cstar.applications.ucla_roms.transforms import (
    CarbonateSensitivityDirective,
    NestingDirective,
)
from cstar.base.env import ENV_CSTAR_RUNID
from cstar.base.exceptions import CstarError
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.transforms import DirectiveConfig, OverrideDirective
from cstar.roms import source_coverage
from cstar.roms.outputs import BoundaryFile, CarbonateSensitivityFile
from cstar.roms.source_coverage import check_source_coverage

REF = datetime(2012, 1, 1)
LONG_NAME = "Time since 2012/01/01"
BRY = BoundaryFile.TIME_VARIABLE


def day(n: float) -> datetime:
    """Return the datetime `n` days after the reference date.

    Parameters
    ----------
    n : float
        Days after 2012-01-01.

    Returns
    -------
    datetime
    """
    return REF + timedelta(days=n)


def write_nc(
    path: Path,
    values: Sequence[float] | None,
    variable: str = BRY,
    long_name: str = LONG_NAME,
    attrs: dict[str, t.Any] | None = None,
) -> Path:
    """Write a tiny netCDF file with one time variable.

    Parameters
    ----------
    path : Path
        The file to write.
    values : Sequence[float] | None
        Record times in days; None writes a file without the variable.
    variable : str
        The time variable name.
    long_name : str
        The variable's `long_name`.
    attrs : dict[str, Any] | None
        Extra variable attributes.

    Returns
    -------
    Path
        `path`.
    """
    if values is None:
        ds = xr.Dataset({"other": ("x", np.zeros(2))})
    else:
        var_attrs = {"long_name": long_name, **(attrs or {})}
        ds = xr.Dataset(
            {variable: ("t", np.asarray(values, dtype=float), var_attrs)},
        )
    ds.to_netcdf(path)
    return path


def bry(
    tmp_path: Path, name: str, values: Sequence[float] | None, **kw: t.Any
) -> BoundaryFile:
    """Write a boundary file and return its reference.

    Parameters
    ----------
    tmp_path : Path
        Directory to write into.
    name : str
        File name, e.g. `child_bry.20120101000000.nc`.
    values : Sequence[float] | None
        Record times in days.
    **kw : Any
        Passed to `write_nc`.

    Returns
    -------
    BoundaryFile
    """
    return BoundaryFile(path=write_nc(tmp_path / name, values, **kw))


def halves(start: float, stop: float) -> list[float]:
    """Return half-day records from `start` to `stop` inclusive.

    Parameters
    ----------
    start : float
        First record.
    stop : float
        Last record.

    Returns
    -------
    list[float]
    """
    return [float(v) for v in np.arange(start, stop + 0.25, 0.5)]


def check(files: Sequence[BoundaryFile], start: float, end: float, **kw: t.Any) -> None:
    """Run the check on boundary files for a window in days.

    Parameters
    ----------
    files : Sequence[BoundaryFile]
        The files.
    start : float
        Window start in days.
    end : float
        Window end in days.
    **kw : Any
        Passed to `check_source_coverage`.
    """
    check_source_coverage(files, BRY, day(start), day(end), source="the source", **kw)


class TestCheckSourceCoverage:
    """Tests of `check_source_coverage`."""

    def test_gap_refused(self, tmp_path: Path) -> None:
        """A missing segment is refused, naming both files and both gap ends."""
        a = bry(tmp_path, "child_bry.20120101000000.nc", halves(0, 10))
        b = bry(tmp_path, "child_bry.20120121000000.nc", halves(20, 30))
        with pytest.raises(CstarError) as err:
            check([a, b], 0, 30)
        msg = str(err.value)
        assert str(a.path) in msg
        assert str(b.path) in msg
        assert "2012-01-11" in msg
        assert "2012-01-21" in msg

    def test_contiguous_passes(self, tmp_path: Path) -> None:
        """Contiguous files pass for a window inside them."""
        a = bry(tmp_path, "child_bry.20120101000000.nc", halves(0, 10))
        b = bry(tmp_path, "child_bry.20120111000000.nc", halves(10.5, 20))
        check([a, b], 1, 19)

    def test_ends_before_window(self, tmp_path: Path) -> None:
        """A source ending before the window end is refused."""
        a = bry(tmp_path, "child_bry.20120101000000.nc", halves(0, 10))
        with pytest.raises(CstarError) as err:
            check([a], 0, 15)
        msg = str(err.value)
        assert "end_date" in msg
        assert "2012-01-11" in msg

    def test_starts_after_window(self, tmp_path: Path) -> None:
        """A source starting after the window start is refused."""
        a = bry(tmp_path, "child_bry.20120103000000.nc", halves(2, 10))
        with pytest.raises(CstarError, match="start_date"):
            check([a], 0, 8)

    def test_duplicate_record_at_boundary(self, tmp_path: Path) -> None:
        """Both files holding the shared record is not a gap."""
        a = bry(tmp_path, "child_bry.20120101000000.nc", halves(0, 10))
        b = bry(tmp_path, "child_bry.20120111000000.nc", halves(10, 20))
        check([a, b], 0, 20)

    def test_cadence_change_passes(self, tmp_path: Path) -> None:
        """An hourly file followed by multi-record daily files passes."""
        hourly = [i / 24 for i in range(49)]
        files = [bry(tmp_path, "child_bry.20120101000000.nc", hourly)]
        for k, start in enumerate(range(3, 20, 4)):
            vals = [float(start + i) for i in range(4)]
            files.append(bry(tmp_path, f"child_bry.2012010{k + 3}000000.nc", vals))
        check(files, 0, 20)

    def test_cdrgas_pattern(self, tmp_path: Path) -> None:
        """`_cdrgas` files with edge records half a period from neighbours pass."""
        var = CarbonateSensitivityFile.TIME_VARIABLE
        sec = 86400.0

        def make(name: str, days: list[float]) -> CarbonateSensitivityFile:
            path = tmp_path / name
            ds = xr.Dataset(
                {
                    "ocean_time": (
                        "o",
                        np.asarray(days) * sec,
                        {"long_name": LONG_NAME},
                    ),
                    var: (
                        "t",
                        np.asarray(days),
                        {
                            "long_name": "time of ddic_dco2 record, days since reference date"
                        },
                    ),
                }
            )
            ds.to_netcdf(path)
            return CarbonateSensitivityFile(path=path)

        c1 = make(
            "output_cdrgas.20120101000000.nc", [0, 0.5, *np.arange(1.5, 10, 1.0), 10]
        )
        c2 = make(
            "output_cdrgas.20120111000000.nc", [10, 10.5, *np.arange(11.5, 20, 1.0), 20]
        )
        check_source_coverage([c1, c2], var, day(0), day(20), source="the source")

    def test_partitioned_source(self, tmp_path: Path) -> None:
        """Pieces without the variable are skipped for one with records."""
        files: list[BoundaryFile] = []
        for ts, vals in (
            ("20120101000000", halves(0, 10)),
            ("20120111000000", halves(10, 20)),
        ):
            files.append(bry(tmp_path, f"child_bry.{ts}.0.nc", None))
            files.append(bry(tmp_path, f"child_bry.{ts}.1.nc", vals))
        check(files, 0, 20)

    def test_partitioned_reads_one_piece(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Three whole files of four pieces open one piece each."""
        files: list[BoundaryFile] = []
        for n, ts in enumerate(("20120101000000", "20120111000000", "20120121000000")):
            vals = halves(10 * n + (0.5 if n else 0), 10 * n + 10)
            for piece in range(4):
                files.append(bry(tmp_path, f"child_bry.{ts}.{piece}.nc", vals))
        calls: list[Path] = []
        real = source_coverage.read_time_axis

        def counting(path: Path, variable: str) -> t.Any:
            calls.append(path)
            return real(path, variable)

        monkeypatch.setattr(source_coverage, "read_time_axis", counting)
        check(files, 0, 30)
        assert len(calls) == 3

    def test_no_reference_warns(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without a reference date the span is unchecked, with a warning."""
        warnings: list[str] = []
        monkeypatch.setattr(
            source_coverage.log,
            "warning",
            lambda msg, *a, **k: warnings.append(msg % a),
        )
        a = bry(
            tmp_path,
            "child_bry.20120101000000.nc",
            halves(0, 10),
            long_name="Time since 2000",
        )
        b = bry(
            tmp_path,
            "child_bry.20120111000000.nc",
            halves(10.5, 20),
            long_name="Time since 2000",
        )
        check([a, b], 0, 40)
        assert any("no reference date" in w for w in warnings)

    def test_no_reference_gap_refused(self, tmp_path: Path) -> None:
        """A gap is refused even when no file records a reference date."""
        a = bry(
            tmp_path,
            "child_bry.20120101000000.nc",
            halves(0, 10),
            long_name="Time since 2000",
        )
        b = bry(
            tmp_path,
            "child_bry.20120121000000.nc",
            halves(20, 30),
            long_name="Time since 2000",
        )
        with pytest.raises(CstarError, match="no record between"):
            check([a, b], 0, 30)

    @pytest.mark.parametrize(("start", "end"), [(0, 5), (22, 28)])
    def test_gap_outside_window_passes(
        self, tmp_path: Path, start: float, end: float
    ) -> None:
        """A gap wholly before or after the run window is not a problem.

        Parameters
        ----------
        start : float
            Window start in days.
        end : float
            Window end in days.
        """
        a = bry(tmp_path, "child_bry.20120101000000.nc", halves(0, 10))
        b = bry(tmp_path, "child_bry.20120121000000.nc", halves(20, 30))
        check([a, b], start, end)

    def test_missing_variable(self, tmp_path: Path) -> None:
        """A whole file lacking the variable is refused, with the hint."""
        a = bry(tmp_path, "child_bry.20120101000000.nc", None)
        with pytest.raises(CstarError) as err:
            check([a], 0, 1, missing_hint="HINT")
        msg = str(err.value)
        assert str(a.path) in msg
        assert BRY in msg
        assert "HINT" in msg

    def test_unreadable_file(self, tmp_path: Path) -> None:
        """A file that is not netCDF is refused, naming it."""
        path = tmp_path / "child_bry.20120101000000.nc"
        path.write_text("mock")
        with pytest.raises(CstarError, match="child_bry.20120101000000.nc"):
            check([BoundaryFile(path=path)], 0, 1)

    def test_cyclic_skipped(self, tmp_path: Path) -> None:
        """Cyclic files are not checked."""
        attrs = {"cycle_length": 365.0}
        a = bry(tmp_path, "child_bry.20120101000000.nc", halves(0, 10), attrs=attrs)
        b = bry(tmp_path, "child_bry.20120121000000.nc", halves(20, 30), attrs=attrs)
        check([a, b], 0, 100)

    def test_out_of_order_files(self, tmp_path: Path) -> None:
        """Files given later-segment-first are sorted by their records."""
        a = bry(tmp_path, "child_bry.20120101000000.nc", halves(0, 10))
        b = bry(tmp_path, "child_bry.20120111000000.nc", halves(10.5, 20))
        check([b, a], 0, 20)


def make_step(start: float, end: float) -> t.Any:
    """Build a minimal step exposing the run window.

    Parameters
    ----------
    start : float
        Window start in days.
    end : float
        Window end in days.

    Returns
    -------
    Any
    """
    runtime = types.SimpleNamespace(start_date=day(start), end_date=day(end))
    return types.SimpleNamespace(
        blueprint=types.SimpleNamespace(runtime_params=runtime)
    )


class TestDirectiveVerify:
    """Tests of `verify` on the directives that collect forcing files."""

    def _segments(
        self, root: Path, name: str, spans: dict[str, tuple[float, float]]
    ) -> list[Path]:
        """Write one directory per segment.

        Parameters
        ----------
        root : Path
            Parent directory.
        name : str
            File name template with `{ts}`.
        spans : dict[str, tuple[float, float]]
            Timestamp -> record span in days.

        Returns
        -------
        list[Path]
        """
        dirs = []
        for ts, (lo, hi) in spans.items():
            d = root / ts
            d.mkdir()
            variable = BRY if "_bry" in name else CarbonateSensitivityFile.TIME_VARIABLE
            write_nc(d / name.format(ts=ts), halves(lo, hi), variable=variable)
            dirs.append(d)
        return dirs

    def test_nesting(self, tmp_path: Path) -> None:
        """`NestingDirective.verify` refuses a gap and accepts contiguous files."""
        d1, d2, d3 = self._segments(
            tmp_path,
            "child_bry.{ts}.nc",
            {
                "20120101000000": (0, 10),
                "20120111000000": (10.5, 20),
                "20120121000000": (20.5, 30),
            },
        )
        step = make_step(0, 30)
        with pytest.raises(CstarError, match="`nest-from` boundary files"):
            NestingDirective({"path": f"{d1};{d3}"}).verify(step)
        NestingDirective({"path": f"{d1};{d2}"}).verify(make_step(0, 20))

    def test_carbonate_sensitivity(self, tmp_path: Path) -> None:
        """`CarbonateSensitivityDirective.verify` refuses a gap and accepts contiguous files."""
        d1, d2, d3 = self._segments(
            tmp_path,
            "output_cdrgas.{ts}.nc",
            {
                "20120101000000": (0, 10),
                "20120111000000": (10.5, 20),
                "20120121000000": (20.5, 30),
            },
        )
        with pytest.raises(CstarError, match="carbonate sensitivity files"):
            CarbonateSensitivityDirective({"path": f"{d1};{d3}"}).verify(
                make_step(0, 30)
            )
        CarbonateSensitivityDirective({"path": f"{d1};{d2}"}).verify(make_step(0, 20))


class TestApplyDirectivesVerify:
    """Tests that `apply_directives` verifies after applying every directive."""

    def _setup(self, tmp_path: Path) -> tuple[Path, Path]:
        """Write a hello_world blueprint.

        Parameters
        ----------
        tmp_path : Path
            Directory to write into.

        Returns
        -------
        tuple[Path, Path]
            The blueprint path and the directives path.
        """
        bp = tmp_path / "bp.yaml"
        bp.write_text(
            f"""\
name: verify
description: verify test
application: hello_world
state: draft
target: 'before'
schema_version: '1.0.0'
working_dir: {(tmp_path / "wd").as_posix()}
"""
        )
        return bp, tmp_path / "directives.yaml"

    def test_verify_runs_after_all_directives(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A directive's verify sees overrides of directives listed after it."""
        seen: list[str] = []

        class DirectiveA(OverrideDirective):
            """Records the target it verifies."""

            @classmethod
            def key(cls) -> str:
                return "verify-a"

            def verify(self, step: LiveStep) -> None:
                seen.append(t.cast("t.Any", step.blueprint).target)

        class DirectiveB(OverrideDirective):
            """Overrides the target."""

            verified = 0

            @classmethod
            def key(cls) -> str:
                return "verify-b"

            def _generate_overrides(self) -> dict[str, t.Any]:
                return {"target": "after"}

            def verify(self, step: LiveStep) -> None:
                type(self).verified += 1

        bp, directives = self._setup(tmp_path)
        directives.write_text(
            "directives:\n  verify-a:\n    enabled: true\n  verify-b:\n    enabled: true\n"
        )
        monkeypatch.delenv(ENV_CSTAR_RUNID, raising=False)
        DirectiveConfig.register(DirectiveA.key(), DirectiveA)
        DirectiveConfig.register(DirectiveB.key(), DirectiveB)
        try:
            DirectiveConfig.apply_directives(str(directives), str(bp))
        finally:
            DirectiveConfig.directive_map.pop(DirectiveA.key(), None)
            DirectiveConfig.directive_map.pop(DirectiveB.key(), None)

        assert seen == ["after"]
        assert DirectiveB.verified == 1

    def test_verify_error_propagates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A CstarError raised by verify escapes `apply_directives`."""

        class FailingDirective(OverrideDirective):
            """Refuses the step."""

            @classmethod
            def key(cls) -> str:
                return "verify-fails"

            def verify(self, step: LiveStep) -> None:
                raise CstarError("refused by verify")

        bp, directives = self._setup(tmp_path)
        directives.write_text("directives:\n  verify-fails:\n    enabled: true\n")
        monkeypatch.delenv(ENV_CSTAR_RUNID, raising=False)
        DirectiveConfig.register(FailingDirective.key(), FailingDirective)
        try:
            with pytest.raises(CstarError, match="refused by verify"):
                DirectiveConfig.apply_directives(str(directives), str(bp))
        finally:
            DirectiveConfig.directive_map.pop(FailingDirective.key(), None)
