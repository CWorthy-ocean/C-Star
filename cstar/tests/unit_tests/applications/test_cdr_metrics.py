import logging
import textwrap
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from unittest import mock

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from pydantic import ValidationError

from cstar.applications import cdr_metrics
from cstar.applications.cdr_metrics import (
    APP_NAME,
    FIGURE_FILENAME,
    METRICS_FILENAME,
    CdrMetricsApplication,
    CdrMetricsBlueprint,
    CdrMetricsRunner,
    CdrOutputFile,
    PathLocation,
    check_contiguous,
    find_cdr_output,
    load_cdr_output,
    metric_series,
)
from cstar.applications.core import RunnerRequest, get_application
from cstar.applications.roms_marbl.transforms import CarbonateSensitivityFile
from cstar.entrypoint.config import JobConfig, ServiceConfiguration
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.serialization import load_blueprint

WINDOW = 3600.0
"""The averaging window length (seconds) of the synthetic CDR output."""


def _touch(path: Path) -> Path:
    """Create an empty file, and its parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    return path


def _domain(grid: Path, *sources: Path) -> dict[str, Any]:
    """Build the raw mapping of one domain."""
    return {
        "grid": {"location": str(grid)},
        "sources": [{"location": str(source)} for source in sources],
    }


def _locations(*paths: Path) -> list[PathLocation]:
    return [PathLocation(location=path) for path in paths]


def _window_ds(begin: Sequence[float], end: Sequence[float]) -> xr.Dataset:
    """Build a dataset holding only the averaging windows, one record per window."""
    times = pd.date_range("2012-01-01 00:30", periods=len(begin), freq="h")
    return xr.Dataset(
        {
            "avg_begin_time": ("time", np.asarray(begin, dtype=float)),
            "avg_end_time": ("time", np.asarray(end, dtype=float)),
        },
        coords={"time": times},
    )


def _cdr_output_ds(n: int = 4) -> xr.Dataset:
    """Build a dask-backed CDR output with `n` contiguous records, as `ROMSOutput` yields.

    One cell releases alkalinity (and removes DIC), and the air-sea flux and DIC
    inventory differ from the control (`*_ALT_CO2`) by an amount growing in time.
    """
    nz, ny, nx = 2, 3, 4
    begin = WINDOW * np.arange(n)
    growth = np.arange(1, n + 1, dtype=float)

    alk = np.zeros((n, nz, ny, nx))
    alk[:, 0, 0, 0] = 1.0
    dic = np.zeros((n, nz, ny, nx))
    dic[:, 0, 0, 0] = -1.0
    flux = np.broadcast_to(growth[:, None, None], (n, ny, nx))
    inventory = np.broadcast_to(growth[:, None, None, None], (n, nz, ny, nx))

    dims4 = ("time", "s_rho", "eta_rho", "xi_rho")
    dims3 = ("time", "eta_rho", "xi_rho")
    ds = xr.Dataset(
        {
            "avg_begin_time": ("time", begin),
            "avg_end_time": ("time", begin + WINDOW),
            "ALK_source": (dims4, alk),
            "DIC_source": (dims4, dic),
            "FG_CO2": (dims3, 2e-3 * flux, {"units": "mmol/m^2/s"}),
            "FG_ALT_CO2": (dims3, 1e-3 * flux, {"units": "mmol/m^2/s"}),
            "hDIC": (dims4, 10.0 + inventory, {"units": "mmol/m^2"}),
            "hDIC_ALT_CO2": (dims4, np.full((n, nz, ny, nx), 10.0)),
        },
        coords={
            "time": pd.date_range("2012-01-01 00:30", periods=n, freq="h"),
            "lat_rho": (("eta_rho", "xi_rho"), np.zeros((ny, nx))),
            "lon_rho": (("eta_rho", "xi_rho"), np.zeros((ny, nx))),
        },
    )
    return ds.chunk({"time": 1})


def _grid_ds() -> xr.Dataset:
    """Build the grid dataset: 1 km square cells."""
    shape = (3, 4)
    return xr.Dataset(
        {
            "pm": (("eta_rho", "xi_rho"), np.full(shape, 1e-3)),
            "pn": (("eta_rho", "xi_rho"), np.full(shape, 1e-3)),
        }
    )


@pytest.fixture
def cdr_dirs(tmp_path: Path) -> dict[str, Path]:
    """Two segment directories each holding `_cdr` output, among other files.

    `seg-01` also holds the other kinds of file whose names share the `_cdr`
    prefix, or are otherwise not CDR output, which discovery must skip.
    """
    seg1, seg2 = tmp_path / "seg-01", tmp_path / "seg-02"
    _touch(seg1 / "output" / "x_cdr.20120101000000.nc")
    _touch(seg1 / "output" / "x_cdr.20120101020000.nc")
    _touch(seg2 / "output" / "x_cdr.20120101040000.nc")
    for decoy in (
        "x_cdrgas.20120101000000.nc",
        "x_cdrtrc.20120101000000.nc",
        "x_his.20120101000000.nc",
        "x_cdr.nc",
    ):
        _touch(seg1 / "output" / decoy)
    return {"seg-01": seg1, "seg-02": seg2}


@pytest.fixture
def blueprint_kwargs(tmp_path: Path, cdr_dirs: dict[str, Path]) -> dict[str, Any]:
    """Minimal valid keyword arguments for constructing a `CdrMetricsBlueprint`."""
    return {
        "name": "test-cdr-metrics",
        "description": "A test CDR metrics blueprint.",
        "working_dir": tmp_path / "work",
        "domains": {
            "wio": _domain(
                tmp_path / "grid.nc", cdr_dirs["seg-01"], cdr_dirs["seg-02"]
            ),
        },
    }


def _make_runner(blueprint: CdrMetricsBlueprint) -> CdrMetricsRunner:
    """Construct a `CdrMetricsRunner` wired to *blueprint* without touching disk.

    `RunnerRequest.blueprint` normally deserializes from a URI on first
    access; setting the private `_bp` cache directly lets the request return
    an in-memory blueprint instance instead.
    """
    request: RunnerRequest[CdrMetricsBlueprint] = RunnerRequest(
        "unused://blueprint", CdrMetricsBlueprint
    )
    request._bp = blueprint

    service_config = ServiceConfiguration(
        as_service=False,
        loop_delay=0,
        health_check_frequency=None,
        log_level=logging.DEBUG,
        health_check_log_threshold=10,
        name="test_cdr_metrics_runner",
    )
    job_config = JobConfig(account_id="", walltime="", priority="")

    return CdrMetricsRunner(request, service_config, job_config)


class _FakeRomsTools:
    """Stands in for `roms_tools.Grid` and `roms_tools.ROMSOutput`.

    Records what they were called with and serves the datasets set on it.
    """

    def __init__(self) -> None:
        self.ds: xr.Dataset = _cdr_output_ds()
        self.grid_ds: xr.Dataset = _grid_ds()
        self.grid_paths: list[Path] = []
        self.output_paths: list[list[Path]] = []

    def grid(self, filename: Path) -> mock.Mock:
        self.grid_paths.append(filename)
        return mock.Mock(ds=self.grid_ds)

    def roms_output(self, *, grid: mock.Mock, path: list[Path], use_dask: bool) -> Any:
        assert use_dask is True
        assert grid.ds is self.grid_ds
        self.output_paths.append(list(path))
        return mock.Mock(ds=self.ds)


@pytest.fixture
def fake_roms_tools(monkeypatch: pytest.MonkeyPatch) -> _FakeRomsTools:
    """Replace the grid and output readers the application reaches through its
    `roms_tools` import, leaving the real metrics computation and plotting.
    """
    fake = _FakeRomsTools()
    monkeypatch.setattr(cdr_metrics.roms_tools, "Grid", fake.grid)
    monkeypatch.setattr(cdr_metrics.roms_tools, "ROMSOutput", fake.roms_output)
    return fake


class TestCdrMetricsApplication:
    """Tests for the registration of the application."""

    def test_registered(self) -> None:
        """Verify the application resolves by name to its blueprint and runner."""
        app = get_application(APP_NAME)

        assert isinstance(app, CdrMetricsApplication)
        assert app.blueprint is CdrMetricsBlueprint
        assert app.runner is CdrMetricsRunner


class TestCdrMetricsBlueprint:
    """Tests for the validation of `CdrMetricsBlueprint`."""

    def test_one_domain_validates(self, blueprint_kwargs: dict[str, Any]) -> None:
        """Verify one domain validates, with its sources in the order given."""
        bp = CdrMetricsBlueprint(**blueprint_kwargs)

        assert bp.application == APP_NAME
        assert list(bp.domains) == ["wio"]
        domain = bp.domains["wio"]
        assert [s.location.name for s in domain.sources] == ["seg-01", "seg-02"]
        assert domain.grid.location.name == "grid.nc"

    def test_two_domains_are_refused(
        self, tmp_path: Path, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify more than one domain is refused as not yet supported."""
        domain = _domain(tmp_path / "grid.nc", tmp_path)
        blueprint_kwargs["domains"] = {"child": domain, "parent": domain}

        with pytest.raises(ValidationError, match="not yet supported"):
            CdrMetricsBlueprint(**blueprint_kwargs)

    def test_empty_domains_are_refused(self, blueprint_kwargs: dict[str, Any]) -> None:
        """Verify at least one domain is required."""
        blueprint_kwargs["domains"] = {}

        with pytest.raises(ValidationError, match="at least 1"):
            CdrMetricsBlueprint(**blueprint_kwargs)

    def test_blank_domain_name_is_refused(
        self, tmp_path: Path, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify a domain's name cannot be blank."""
        blueprint_kwargs["domains"] = {" ": _domain(tmp_path / "grid.nc", tmp_path)}

        with pytest.raises(ValidationError, match="at least 1 character"):
            CdrMetricsBlueprint(**blueprint_kwargs)

    def test_list_of_domains_is_refused(
        self, tmp_path: Path, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify `domains` is a mapping by name, not a list."""
        blueprint_kwargs["domains"] = [_domain(tmp_path / "grid.nc", tmp_path)]

        with pytest.raises(ValidationError, match="domains"):
            CdrMetricsBlueprint(**blueprint_kwargs)

    def test_empty_sources_are_refused(
        self, tmp_path: Path, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify a domain needs at least one source."""
        blueprint_kwargs["domains"] = {"wio": _domain(tmp_path / "grid.nc")}

        with pytest.raises(ValidationError, match="at least 1"):
            CdrMetricsBlueprint(**blueprint_kwargs)

    def test_extra_keys_are_forbidden(
        self, tmp_path: Path, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify unknown keys are refused at the blueprint, domain and location levels."""
        with pytest.raises(ValidationError, match="bogus"):
            CdrMetricsBlueprint(**blueprint_kwargs, bogus=1)

        domain = _domain(tmp_path / "grid.nc", tmp_path)
        blueprint_kwargs["domains"] = {"wio": {**domain, "name": "wio"}}
        with pytest.raises(ValidationError, match="name"):
            CdrMetricsBlueprint(**blueprint_kwargs)

        blueprint_kwargs["domains"] = {
            "wio": {**domain, "grid": {"location": "grid.nc", "extra": 1}}
        }
        with pytest.raises(ValidationError, match="extra"):
            CdrMetricsBlueprint(**blueprint_kwargs)

    def test_user_directory_is_expanded(self) -> None:
        """Verify `~` in a location is expanded."""
        assert PathLocation(location=Path("~/x")).location == Path("~/x").expanduser()

    def test_relative_locations_anchor_to_the_blueprint_file(
        self, tmp_path: Path
    ) -> None:
        """Verify loading a blueprint file anchors relative locations to its directory,
        and leaves absolute locations and workplan placeholders as written.
        """
        bp_dir = tmp_path / "blueprints"
        bp_dir.mkdir()
        absolute = tmp_path / "elsewhere" / "seg-03"
        blueprint_file = bp_dir / "cdr.yaml"
        blueprint_file.write_text(
            textwrap.dedent(
                f"""\
                name: cdr
                description: test
                application: {APP_NAME}
                domains:
                  wio:
                    grid:
                      location: grids/wio_grid.nc
                    sources:
                      - location: ../runs/seg-01/output
                      - location: "{{{{output_dir: seg-02}}}}"
                      - location: {absolute}
                """
            )
        )

        bp = load_blueprint(blueprint_file, CdrMetricsBlueprint)

        domain = bp.domains["wio"]
        root = bp_dir.resolve()
        assert domain.grid.location == root / "grids" / "wio_grid.nc"
        assert [s.location for s in domain.sources] == [
            root.parent / "runs" / "seg-01" / "output",
            Path("{{output_dir: seg-02}}"),
            absolute,
        ]


class TestCdrOutputFile:
    """Tests for `CdrOutputFile` and `find_cdr_output`."""

    def test_only_cdr_output_is_found(self, cdr_dirs: dict[str, Path]) -> None:
        """Verify the directory search skips sibling output kinds and the forcing file."""
        found = find_cdr_output(_locations(cdr_dirs["seg-01"]))

        assert [f.path.name for f in found] == [
            "x_cdr.20120101000000.nc",
            "x_cdr.20120101020000.nc",
        ]
        assert all(type(f) is CdrOutputFile for f in found)
        assert [f.timestamp.hour for f in found] == [0, 2]

    def test_sources_are_searched_recursively(self, cdr_dirs: dict[str, Path]) -> None:
        """Verify a source above the step directories finds the output beneath them."""
        found = find_cdr_output(_locations(cdr_dirs["seg-01"].parent))

        assert [f.path.name for f in found] == [
            "x_cdr.20120101000000.nc",
            "x_cdr.20120101020000.nc",
            "x_cdr.20120101040000.nc",
        ]

    def test_source_order_is_preserved(self, cdr_dirs: dict[str, Path]) -> None:
        """Verify files follow the order of the sources, not their names."""
        found = find_cdr_output(_locations(cdr_dirs["seg-02"], cdr_dirs["seg-01"]))

        assert [f.path.name for f in found] == [
            "x_cdr.20120101040000.nc",
            "x_cdr.20120101000000.nc",
            "x_cdr.20120101020000.nc",
        ]

    def test_a_file_can_be_a_source(self, cdr_dirs: dict[str, Path]) -> None:
        """Verify a single `_cdr` file is accepted as a source."""
        file = cdr_dirs["seg-02"] / "output" / "x_cdr.20120101040000.nc"

        found = find_cdr_output(_locations(file))

        assert [f.path for f in found] == [file.resolve()]

    def test_duplicates_are_removed(self, cdr_dirs: dict[str, Path]) -> None:
        """Verify a file reached through several sources is listed once, at its
        first occurrence.
        """
        seg1 = cdr_dirs["seg-01"]
        first = seg1 / "output" / "x_cdr.20120101000000.nc"

        found = find_cdr_output(
            _locations(first, seg1, cdr_dirs["seg-02"], seg1 / "output")
        )

        assert [f.path.name for f in found] == [
            "x_cdr.20120101000000.nc",
            "x_cdr.20120101020000.nc",
            "x_cdr.20120101040000.nc",
        ]

    def test_partitioned_piece_is_refused(self, tmp_path: Path) -> None:
        """Verify a partitioned piece is refused, naming it and asking to join."""
        piece = _touch(tmp_path / "part" / "x_cdr.20120101000000.003.nc")

        with pytest.raises(ValueError, match=r"x_cdr\.20120101000000\.003\.nc") as exc:
            find_cdr_output(_locations(piece.parent))

        assert "Join the pieces" in str(exc.value)

    def test_source_without_cdr_output_names_the_requirement(
        self, tmp_path: Path, cdr_dirs: dict[str, Path]
    ) -> None:
        """Verify a source without `_cdr` output is a not-found error that says
        how to get a run to write it, and names the source.
        """
        no_cdr = tmp_path / "no-cdr"
        _touch(no_cdr / "x_cdrgas.20120101000000.nc")
        _touch(no_cdr / "x_cdr.nc")

        with pytest.raises(FileNotFoundError, match="do_cdr_output") as exc:
            find_cdr_output(_locations(cdr_dirs["seg-01"], no_cdr))

        assert str(no_cdr) in str(exc.value)

    def test_missing_source_is_an_error(self, tmp_path: Path) -> None:
        """Verify a source that does not exist is refused, naming it."""
        with pytest.raises(ValueError, match="No directory or file found"):
            find_cdr_output(_locations(tmp_path / "absent"))

    def test_misnamed_file_passing_the_glob_still_raises(self, tmp_path: Path) -> None:
        """Verify a name that has the timestamp but breaks the naming convention
        raises, as it does for the other kinds of output file.
        """
        _touch(tmp_path / "x_cdr.20120101000000_old.nc")

        with pytest.raises(ValueError, match="naming convention"):
            find_cdr_output(_locations(tmp_path))

    def test_base_search_still_rejects_misnamed_files(self, tmp_path: Path) -> None:
        """Verify the looser search of other output kinds is unchanged: a misnamed
        file in the directory raises rather than being skipped.
        """
        _touch(tmp_path / "roms_cdrgas.20120101000000.nc")
        _touch(tmp_path / "roms_cdrgas_old.nc")

        with pytest.raises(ValueError, match="naming convention"):
            CarbonateSensitivityFile.find(tmp_path)


class TestCheckContiguous:
    """Tests for `check_contiguous`."""

    def test_contiguous_windows_pass(self) -> None:
        """Verify windows meeting end to start are accepted."""
        check_contiguous(_window_ds([0, 10, 20], [10, 20, 30]))

    def test_single_record_passes(self) -> None:
        """Verify a lone record has nothing to be contiguous with."""
        check_contiguous(_window_ds([0], [10]))

    def test_gap_names_a_missing_segment(self) -> None:
        """Verify a gap is an error that points at the sources."""
        ds = _window_ds([0, 10, 50], [10, 20, 60])

        with pytest.raises(ValueError, match="gaps") as exc:
            check_contiguous(ds)

        message = str(exc.value)
        assert "missing from `sources`" in message
        assert "overlap" not in message
        assert "2012-01-01T01:30:00" in message
        assert "2012-01-01T02:30:00" in message

    def test_overlap_is_an_error(self) -> None:
        """Verify overlapping windows are an error that points at the sources."""
        ds = _window_ds([0, 5], [10, 15])

        with pytest.raises(ValueError, match="overlap") as exc:
            check_contiguous(ds)

        assert "listed twice" in str(exc.value)
        assert "gaps" not in str(exc.value)

    def test_exact_duplicate_window_is_an_overlap(self) -> None:
        """Verify a record repeated verbatim is an overlap."""
        with pytest.raises(ValueError, match="overlap"):
            check_contiguous(_window_ds([0, 0], [10, 10]))

    def test_every_problem_is_reported_in_one_error(self) -> None:
        """Verify overlaps and gaps are all reported together."""
        ds = _window_ds([0, 10, 15, 100, 160], [10, 20, 25, 110, 170])

        with pytest.raises(ValueError, match="overlap") as exc:
            check_contiguous(ds)

        message = str(exc.value)
        assert "gaps" in message
        assert "2012-01-01T01:30:00" in message  # 10 s record -> overlap with next
        assert "2012-01-01T02:30:00" in message  # 15 s record -> gap to the next
        assert "2012-01-01T03:30:00" in message  # 100 s record -> gap to the next
        assert "2012-01-01T04:30:00" in message

    def test_sub_second_jitter_is_tolerated(self) -> None:
        """Verify a fraction of a second between windows is not a gap or overlap."""
        check_contiguous(_window_ds([0, 10.4, 20.0], [10.0, 20.5, 30.0]))

    def test_whole_seconds_beyond_the_tolerance_are_not(self) -> None:
        """Verify the tolerance is a second."""
        with pytest.raises(ValueError, match="gaps"):
            check_contiguous(_window_ds([0, 12], [10, 22]))

    @pytest.mark.parametrize("missing", ["avg_begin_time", "avg_end_time"])
    def test_missing_window_times_are_an_error(self, missing: str) -> None:
        """Verify output without the averaging window times says it must be averaged."""
        ds = _window_ds([0, 10], [10, 20]).drop_vars(missing)

        with pytest.raises(ValueError, match="do_avg") as exc:
            check_contiguous(ds)

        assert missing in str(exc.value)


class TestLoadCdrOutput:
    """Tests for `load_cdr_output`."""

    def _files(self, tmp_path: Path) -> list[CdrOutputFile]:
        return [
            CdrOutputFile(path=tmp_path / name)
            for name in ("x_cdr.20120101000000.nc", "x_cdr.20120101040000.nc")
        ]

    def test_reads_the_grid_and_files_lazily(
        self, tmp_path: Path, fake_roms_tools: _FakeRomsTools
    ) -> None:
        """Verify the grid and the files, in order, are handed to roms-tools."""
        files = self._files(tmp_path)

        ds, grid_ds = load_cdr_output(tmp_path / "grid.nc", files)

        assert fake_roms_tools.grid_paths == [tmp_path / "grid.nc"]
        assert fake_roms_tools.output_paths == [[f.path for f in files]]
        assert ds is fake_roms_tools.ds
        assert grid_ds is fake_roms_tools.grid_ds

    def test_records_are_sorted_by_time(
        self, tmp_path: Path, fake_roms_tools: _FakeRomsTools
    ) -> None:
        """Verify records concatenated out of order are sorted, and so pass the
        contiguity check.
        """
        fake_roms_tools.ds = _cdr_output_ds(n=4).isel(time=[2, 3, 0, 1])
        assert not fake_roms_tools.ds["time"].to_index().is_monotonic_increasing

        ds, _ = load_cdr_output(tmp_path / "grid.nc", self._files(tmp_path))

        assert ds["time"].to_index().is_monotonic_increasing
        assert ds["avg_begin_time"].values.tolist() == [0.0, 3600.0, 7200.0, 10800.0]

    def test_sorted_records_are_left_alone(
        self, tmp_path: Path, fake_roms_tools: _FakeRomsTools
    ) -> None:
        """Verify records already in order are returned as loaded."""
        ds, _ = load_cdr_output(tmp_path / "grid.nc", self._files(tmp_path))

        assert ds is fake_roms_tools.ds

    def test_a_gap_after_sorting_is_an_error(
        self, tmp_path: Path, fake_roms_tools: _FakeRomsTools
    ) -> None:
        """Verify a missing segment is detected even when the records arrive shuffled."""
        fake_roms_tools.ds = _cdr_output_ds(n=4).isel(time=[3, 0, 1])

        with pytest.raises(ValueError, match="missing from `sources`"):
            load_cdr_output(tmp_path / "grid.nc", self._files(tmp_path))

    def test_an_overlap_after_sorting_is_an_error(
        self, tmp_path: Path, fake_roms_tools: _FakeRomsTools
    ) -> None:
        """Verify a segment listed twice, under different names, is detected."""
        fake_roms_tools.ds = _cdr_output_ds(n=3).isel(time=[0, 1, 1, 2])

        with pytest.raises(ValueError, match="overlap"):
            load_cdr_output(tmp_path / "grid.nc", self._files(tmp_path))


class TestMetricSeries:
    """Tests for `metric_series`."""

    @pytest.fixture
    def ds_cdr(self) -> xr.Dataset:
        """Build a metrics dataset like `compute_cdr_metrics` returns, with a
        cumulative variable a newer roms-tools adds.
        """
        n, nz, ny, nx = 3, 2, 3, 4
        lateral = {
            "lat_rho": (("eta_rho", "xi_rho"), np.zeros((ny, nx))),
            "lon_rho": (("eta_rho", "xi_rho"), np.zeros((ny, nx))),
        }
        ds = xr.Dataset(
            {
                "cdr_efficiency_from_flux": (
                    "time",
                    np.linspace(0, 1, n),
                    {"units": "nondimensional"},
                ),
                "cdr_cumulative_source": ("time", np.arange(n, dtype=float)),
                "window_length": ("time", np.full(n, WINDOW)),
                "area": (("eta_rho", "xi_rho"), np.ones((ny, nx))),
                "FG_CO2": (("time", "eta_rho", "xi_rho"), np.zeros((n, ny, nx))),
                "hDIC": (
                    ("time", "s_rho", "eta_rho", "xi_rho"),
                    np.zeros((n, nz, ny, nx)),
                ),
            },
            coords={"time": pd.date_range("2012-01-01", periods=n, freq="h")} | lateral,
            attrs={"title": "cdr"},
        )
        return ds.chunk({"time": 1})

    def test_keeps_only_the_series_over_time(self, ds_cdr: xr.Dataset) -> None:
        """Verify every variable over `time` alone is kept, and nothing else."""
        series = metric_series(ds_cdr)

        kept = {
            "cdr_efficiency_from_flux",
            "cdr_cumulative_source",
            "window_length",
        }
        assert kept <= set(series.data_vars)
        assert {"area", "FG_CO2", "hDIC"}.isdisjoint(series.data_vars)
        assert all(var.dims == ("time",) for var in series.data_vars.values())

    def test_lateral_coordinates_are_dropped(self, ds_cdr: xr.Dataset) -> None:
        """Verify coordinates that are not over `time` do not come along."""
        series = metric_series(ds_cdr)

        assert {"lat_rho", "lon_rho"}.isdisjoint(series.coords)
        assert "time" in series.coords

    def test_attributes_are_kept(self, ds_cdr: xr.Dataset) -> None:
        """Verify variable and dataset attributes survive."""
        series = metric_series(ds_cdr)

        assert series["cdr_efficiency_from_flux"].attrs["units"] == "nondimensional"
        assert series.attrs["title"] == "cdr"

    def test_result_is_in_memory(self, ds_cdr: xr.Dataset) -> None:
        """Verify the result no longer holds lazy arrays."""
        assert ds_cdr["window_length"].chunks is not None

        series = metric_series(ds_cdr)

        assert all(var.chunks is None for var in series.data_vars.values())
        assert series["cdr_cumulative_source"].values.tolist() == [0.0, 1.0, 2.0]


class TestCdrMetricsRunner:
    """Tests for `CdrMetricsRunner.run`, with the real metrics and plotting."""

    async def test_writes_the_series_and_figure(
        self,
        blueprint_kwargs: dict[str, Any],
        tmp_path: Path,
        cdr_dirs: dict[str, Path],
        fake_roms_tools: _FakeRomsTools,
    ) -> None:
        """Verify a run completes and writes the metrics file and the figure."""
        bp = CdrMetricsBlueprint(**blueprint_kwargs)
        runner = _make_runner(bp)

        await runner.run()

        assert runner.state.status == ExecutionStatus.COMPLETED
        out_dir = bp.effective_working_dir / "output"
        assert fake_roms_tools.grid_paths == [tmp_path / "grid.nc"]
        assert [p.name for p in fake_roms_tools.output_paths[0]] == [
            "x_cdr.20120101000000.nc",
            "x_cdr.20120101020000.nc",
            "x_cdr.20120101040000.nc",
        ]

        with xr.open_dataset(out_dir / METRICS_FILENAME) as series:
            expected = {
                "cdr_efficiency_from_flux",
                "cdr_efficiency_from_DIC_difference",
                "cdr_carbon_uptake_from_flux",
                "cdr_carbon_uptake_from_DIC_difference",
            }
            assert expected <= set(series.data_vars)
            assert series.attrs["cdr_domain"] == "wio"
            assert series.sizes["time"] == 4
            assert {"hDIC", "area", "lat_rho"}.isdisjoint(series.variables)
            assert np.isfinite(series["cdr_efficiency_from_flux"]).all()
            assert (series["cdr_carbon_uptake_from_flux"] > 0).all()

        figure = out_dir / FIGURE_FILENAME
        assert figure.read_bytes().startswith(b"\x89PNG")

    async def test_the_domain_name_is_recorded(
        self,
        tmp_path: Path,
        cdr_dirs: dict[str, Path],
        fake_roms_tools: _FakeRomsTools,
    ) -> None:
        """Verify the key of `domains` is what the metrics file records."""
        bp = CdrMetricsBlueprint(
            name="named",
            description="A test CDR metrics blueprint.",
            working_dir=tmp_path / "work",
            domains={"child": _domain(tmp_path / "grid.nc", cdr_dirs["seg-01"])},
        )

        await _make_runner(bp).run()

        with xr.open_dataset(
            bp.effective_working_dir / "output" / METRICS_FILENAME
        ) as series:
            assert series.attrs["cdr_domain"] == "child"

    async def test_a_gap_fails_the_run_before_anything_is_written(
        self,
        blueprint_kwargs: dict[str, Any],
        fake_roms_tools: _FakeRomsTools,
    ) -> None:
        """Verify a missing segment stops the run without writing outputs."""
        fake_roms_tools.ds = _cdr_output_ds(n=4).isel(time=[0, 1, 3])
        bp = CdrMetricsBlueprint(**blueprint_kwargs)
        runner = _make_runner(bp)

        with pytest.raises(ValueError, match="missing from `sources`"):
            await runner.run()

        assert runner.state.status != ExecutionStatus.COMPLETED
        assert not (bp.effective_working_dir / "output" / METRICS_FILENAME).exists()

    async def test_missing_output_fails_before_roms_tools_is_used(
        self,
        tmp_path: Path,
        blueprint_kwargs: dict[str, Any],
        fake_roms_tools: _FakeRomsTools,
    ) -> None:
        """Verify a source without `_cdr` output stops the run at discovery."""
        empty = tmp_path / "empty"
        empty.mkdir()
        blueprint_kwargs["domains"] = {"wio": _domain(tmp_path / "grid.nc", empty)}
        runner = _make_runner(CdrMetricsBlueprint(**blueprint_kwargs))

        with pytest.raises(FileNotFoundError, match="do_cdr_output"):
            await runner.run()

        assert fake_roms_tools.grid_paths == []
