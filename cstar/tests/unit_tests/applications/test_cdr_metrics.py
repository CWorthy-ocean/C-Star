import logging
import textwrap
from collections.abc import Sequence
from datetime import UTC, datetime
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
    _REQUIRED_VARIABLES,
    APP_NAME,
    DOMAINS_FIGURE_FILENAME,
    FIGURE_FILENAME,
    METRICS_FILENAME,
    CdrDomain,
    CdrMetricsApplication,
    CdrMetricsBlueprint,
    CdrMetricsRunner,
    check_contiguous,
    check_required_variables,
    compute_domain_metrics,
    find_cdr_output,
    load_cdr_output,
    metric_series,
    source_sign_report,
)
from cstar.applications.core import ArtifactKind, RunnerRequest, get_application
from cstar.applications.roms_marbl.outputs import CdrOutputFile
from cstar.base.exceptions import CstarError
from cstar.entrypoint.config import JobConfig, ServiceConfiguration
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.manifest import (
    STEP_RECORD_NAME,
    Product,
    StepRecord,
    expand_artifacts,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity, PathLocation
from cstar.orchestration.serialization import load_blueprint

WINDOW = 3600.0
"""The averaging window length (seconds) of the synthetic CDR output."""


def _touch(path: Path) -> Path:
    """Create an empty file, and its parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    return path


def _write_cdr_file(path: Path, *, omit: Sequence[str] = ()) -> Path:
    """Write a minimal NetCDF `_cdr` file, holding the variables the metrics need.

    Only the variable names matter: the application reads the names from disk,
    and the data comes from the patched `ROMSOutput`. Variables in *omit* are
    left out.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    names = [name for name in _REQUIRED_VARIABLES if name not in omit]
    xr.Dataset({name: ("time", np.zeros(2)) for name in names}).to_netcdf(path)
    return path


def _domain(
    grid: Path, *sources: Path, source_role: str | None = None
) -> dict[str, Any]:
    """Build the raw mapping of one domain."""
    domain: dict[str, Any] = {
        "grid": {"location": str(grid)},
        "sources": [{"location": str(source)} for source in sources],
    }
    if source_role is not None:
        domain["source_role"] = source_role
    return domain


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


def _cdr_output_ds(
    n: int = 4,
    *,
    lon_range: tuple[float, float] = (0.0, 0.0),
    lat_range: tuple[float, float] = (0.0, 0.0),
    transfer: bool = False,
) -> xr.Dataset:
    """Build a dask-backed CDR output with `n` contiguous records, as `ROMSOutput` yields.

    One cell releases alkalinity (and removes DIC), and the air-sea flux and DIC
    inventory differ from the control (`*_ALT_CO2`) by an amount growing in time.
    With `transfer`, that cell's sources have the opposite signs, as a parent's
    signed perturbation can. The cells span the given longitude and latitude
    ranges.
    """
    nz, ny, nx = 2, 3, 4
    begin = WINDOW * np.arange(n)
    growth = np.arange(1, n + 1, dtype=float)

    sign = -1.0 if transfer else 1.0
    alk = np.zeros((n, nz, ny, nx))
    alk[:, 0, 0, 0] = sign
    dic = np.zeros((n, nz, ny, nx))
    dic[:, 0, 0, 0] = -sign
    lat, lon = np.meshgrid(
        np.linspace(*lat_range, ny), np.linspace(*lon_range, nx), indexing="ij"
    )
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
            "lat_rho": (("eta_rho", "xi_rho"), lat),
            "lon_rho": (("eta_rho", "xi_rho"), lon),
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

    The `_cdr` files are real NetCDF files holding the variables the metrics need.
    `seg-01` also holds the other kinds of file whose names share the `_cdr`
    prefix, or are otherwise not CDR output, which discovery must skip.
    """
    seg1, seg2 = tmp_path / "seg-01", tmp_path / "seg-02"
    _write_cdr_file(seg1 / "output" / "x_cdr.20120101000000.nc")
    _write_cdr_file(seg1 / "output" / "x_cdr.20120101020000.nc")
    _write_cdr_file(seg2 / "output" / "x_cdr.20120101040000.nc")
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

    Records what they were called with and serves the datasets set on it: `ds`
    for every grid, except those of `ds_by_grid` (a domain's output, by the path
    of its grid file).
    """

    def __init__(self) -> None:
        self.ds: xr.Dataset = _cdr_output_ds()
        self.ds_by_grid: dict[Path, xr.Dataset] = {}
        self.grid_ds: xr.Dataset = _grid_ds()
        self.grid_paths: list[Path] = []
        self.output_paths: list[list[Path]] = []

    def grid(self, filename: Path) -> mock.Mock:
        self.grid_paths.append(filename)
        return mock.Mock(ds=self.grid_ds, path=filename)

    def roms_output(self, *, grid: mock.Mock, path: list[Path], use_dask: bool) -> Any:
        assert use_dask is True
        assert grid.ds is self.grid_ds
        self.output_paths.append(list(path))
        return mock.Mock(ds=self.ds_by_grid.get(grid.path, self.ds))


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

    def test_source_role_defaults_to_release(
        self, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify a domain holds the release unless it says otherwise."""
        bp = CdrMetricsBlueprint(**blueprint_kwargs)

        assert bp.domains["wio"].source_role == "release"

    def test_nested_domains_validate(
        self, tmp_path: Path, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify a release child and a transfer parent validate, child first."""
        blueprint_kwargs["domains"] = {
            "child": _domain(tmp_path / "child.nc", tmp_path),
            "parent": _domain(tmp_path / "parent.nc", tmp_path, source_role="transfer"),
        }

        bp = CdrMetricsBlueprint(**blueprint_kwargs)

        assert list(bp.domains) == ["child", "parent"]
        assert [d.source_role for d in bp.domains.values()] == ["release", "transfer"]

    def test_nested_domains_need_a_release(
        self, tmp_path: Path, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify several domains that all receive a transfer are refused, naming them."""
        blueprint_kwargs["domains"] = {
            name: _domain(tmp_path / f"{name}.nc", tmp_path, source_role="transfer")
            for name in ("child", "parent")
        }

        with pytest.raises(
            ValueError, match="need the domain holding the release"
        ) as exc:
            CdrMetricsBlueprint(**blueprint_kwargs)

        assert "'child', 'parent'" in str(exc.value)

    def test_a_lone_transfer_domain_is_allowed(
        self, tmp_path: Path, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify the release is only required of nested domains."""
        blueprint_kwargs["domains"] = {
            "wio": _domain(tmp_path / "grid.nc", tmp_path, source_role="transfer")
        }

        bp = CdrMetricsBlueprint(**blueprint_kwargs)

        assert bp.domains["wio"].source_role == "transfer"

    def test_invalid_source_role_is_refused(
        self, tmp_path: Path, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify a source role other than `release` and `transfer` is refused."""
        blueprint_kwargs["domains"] = {
            "wio": _domain(tmp_path / "grid.nc", tmp_path, source_role="parent")
        }

        with pytest.raises(ValidationError, match="source_role"):
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


class TestFindCdrOutput:
    """Tests for `find_cdr_output`."""

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


class TestRecordedCdrOutput:
    """Tests for `find_cdr_output` on a step's `output` directory, which reads the
    `_cdr` files the step's record lists.
    """

    @staticmethod
    def _write_record(step_root: Path, *paths: Path) -> None:
        """Write the record of a step that listed `paths` as `cdr_output` products."""
        record = StepRecord(
            step="producer",
            application="roms_marbl",
            blueprint=BlueprintIdentity(
                kind="Blueprint", application="roms_marbl", name="bp"
            ),
            outcome="completed",
            finished_at=datetime(2026, 10, 15, 12, tzinfo=UTC),
            products=[
                Product(
                    kind=ArtifactKind.cdr_output,
                    path=path.relative_to(step_root).as_posix(),
                )
                for path in paths
            ],
        )
        write_step_record(step_root, record)

    def test_recorded_files_are_read_and_unrecorded_ones_are_not(
        self, cdr_dirs: dict[str, Path]
    ) -> None:
        """Verify the files the record lists are read, not every `_cdr` file beside them."""
        seg1 = cdr_dirs["seg-01"]
        recorded = seg1 / "output" / "x_cdr.20120101000000.nc"
        self._write_record(seg1, recorded)

        found = find_cdr_output(_locations(seg1 / "output"))

        assert [f.path.name for f in found] == ["x_cdr.20120101000000.nc"]
        assert [f.path for f in found] == [recorded.resolve()]

    def test_a_symlinked_source_reads_the_record_of_the_real_step(
        self, tmp_path: Path, cdr_dirs: dict[str, Path]
    ) -> None:
        """Verify the source is resolved before the record beside it is looked up."""
        seg1 = cdr_dirs["seg-01"]
        recorded = seg1 / "output" / "x_cdr.20120101000000.nc"
        self._write_record(seg1, recorded)
        link = tmp_path / "link"
        link.symlink_to(seg1 / "output", target_is_directory=True)

        found = find_cdr_output(_locations(link))

        assert [f.path for f in found] == [recorded.resolve()]

    def test_a_missing_recorded_file_is_refused(
        self, cdr_dirs: dict[str, Path]
    ) -> None:
        """Verify a `_cdr` file the record lists but that is gone is refused, naming it
        and pointing to `cstar run check`, rather than leaving a gap in the series.
        """
        seg1 = cdr_dirs["seg-01"]
        self._write_record(seg1, seg1 / "output" / "x_cdr.20120101040000.nc")

        with pytest.raises(CstarError, match=r"x_cdr\.20120101040000\.nc") as exc:
            find_cdr_output(_locations(seg1 / "output"))

        assert "cstar run check" in str(exc.value)

    def test_a_missing_recorded_file_names_the_domain(
        self, tmp_path: Path, cdr_dirs: dict[str, Path]
    ) -> None:
        """Verify the refusal of a missing recorded file is prefixed with the domain."""
        seg1 = cdr_dirs["seg-01"]
        self._write_record(seg1, seg1 / "output" / "x_cdr.20120101040000.nc")
        domain = CdrDomain.model_validate(
            _domain(tmp_path / "grid.nc", seg1 / "output")
        )

        with pytest.raises(CstarError, match=r"x_cdr\.20120101040000\.nc") as exc:
            compute_domain_metrics("wio", domain)

        assert str(exc.value).startswith("Domain 'wio': ")

    def test_recorded_files_elsewhere_in_the_step_are_ignored(
        self, cdr_dirs: dict[str, Path]
    ) -> None:
        """Verify a record listing `_cdr` output outside the directory leaves the
        directory to be searched.
        """
        seg1 = cdr_dirs["seg-01"]
        elsewhere = _touch(seg1 / "elsewhere" / "x_cdr.20120101060000.nc")
        self._write_record(seg1, elsewhere)

        found = find_cdr_output(_locations(seg1 / "output"))

        assert [f.path.name for f in found] == [
            "x_cdr.20120101000000.nc",
            "x_cdr.20120101020000.nc",
        ]

    def test_recorded_files_not_named_like_cdr_output_are_skipped(
        self, cdr_dirs: dict[str, Path]
    ) -> None:
        """Verify a record listing only a file that is not named like `_cdr` output
        leaves the directory to be searched.
        """
        seg1 = cdr_dirs["seg-01"]
        self._write_record(seg1, seg1 / "output" / "x_cdr.nc")

        found = find_cdr_output(_locations(seg1 / "output"))

        assert [f.path.name for f in found] == [
            "x_cdr.20120101000000.nc",
            "x_cdr.20120101020000.nc",
        ]

    def test_a_recorded_partitioned_piece_is_refused(
        self, cdr_dirs: dict[str, Path]
    ) -> None:
        """Verify a partitioned piece the record lists is refused, asking to join it."""
        seg1 = cdr_dirs["seg-01"]
        piece = _touch(seg1 / "output" / "x_cdr.20120101000000.003.nc")
        # unrecorded, and fails a directory search with a different error
        _touch(seg1 / "output" / "x_cdr.20120101000000.abc.nc")
        self._write_record(seg1, piece)

        with pytest.raises(ValueError, match="Join the pieces"):
            find_cdr_output(_locations(seg1 / "output"))

    def test_duplicates_across_routes_are_removed(
        self, cdr_dirs: dict[str, Path]
    ) -> None:
        """Verify a file reached as itself and through a recorded directory is listed
        once, at its first occurrence.
        """
        seg1 = cdr_dirs["seg-01"]
        first = seg1 / "output" / "x_cdr.20120101020000.nc"
        self._write_record(seg1, seg1 / "output" / "x_cdr.20120101000000.nc", first)
        _write_cdr_file(seg1 / "output" / "x_cdr.20120101010000.nc")  # unrecorded

        found = find_cdr_output(_locations(first, seg1 / "output"))

        assert [f.path.name for f in found] == [
            "x_cdr.20120101020000.nc",
            "x_cdr.20120101000000.nc",
        ]

    @pytest.mark.parametrize("record", [False, True], ids=["no record", "no products"])
    def test_a_directory_without_recorded_output_is_searched(
        self, cdr_dirs: dict[str, Path], record: bool
    ) -> None:
        """Verify a directory whose step has no record, or a record listing no
        `_cdr` output, is searched for the files.
        """
        seg1 = cdr_dirs["seg-01"]
        if record:
            self._write_record(seg1)
        assert (seg1 / STEP_RECORD_NAME).exists() is record

        found = find_cdr_output(_locations(seg1 / "output"))

        assert [f.path.name for f in found] == [
            "x_cdr.20120101000000.nc",
            "x_cdr.20120101020000.nc",
        ]


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

    def test_windows_of_different_lengths_warn(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Verify contiguous windows of unequal length are accepted with a warning
        naming the lengths.
        """
        caplog.set_level(logging.WARNING, logger=cdr_metrics.log.name)

        check_contiguous(_window_ds([0, 900], [900, 2700]))

        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "900 s" in warnings[0].getMessage()
        assert "1800 s" in warnings[0].getMessage()

    def test_equal_windows_do_not_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        """Verify windows of one length log nothing at warning level."""
        caplog.set_level(logging.WARNING, logger=cdr_metrics.log.name)

        check_contiguous(_window_ds([0, 900, 1800], [900, 1800, 2700]))

        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


class TestCheckRequiredVariables:
    """Tests for `check_required_variables`."""

    def test_complete_files_pass(self, tmp_path: Path) -> None:
        """Verify files holding every variable are accepted."""
        files = [
            CdrOutputFile(path=_write_cdr_file(tmp_path / f"x_cdr.2012010100000{i}.nc"))
            for i in range(2)
        ]

        check_required_variables(files)

    def test_missing_variables_are_reported_together(self, tmp_path: Path) -> None:
        """Verify every missing variable is reported once, with the count of files
        lacking it, the first file affected and the ROMS settings that write it.
        """
        complete = _write_cdr_file(tmp_path / "x_cdr.20120101000000.nc")
        lacking = _write_cdr_file(
            tmp_path / "x_cdr.20120101020000.nc", omit=["ALK_source", "avg_end_time"]
        )
        files = [CdrOutputFile(path=complete), CdrOutputFile(path=lacking)]

        with pytest.raises(ValueError, match="lacks variables") as exc:
            check_required_variables(files)

        message = str(exc.value)
        assert "`ALK_source` (1 of 2 files)" in message
        assert "`avg_end_time` (1 of 2 files)" in message
        assert str(lacking) in message
        assert str(complete) not in message
        assert "cdr_frc.cdr_source" in message
        assert "cdr_output.do_avg" in message
        # variables that are present are not reported
        assert "DIC_source" not in message
        assert "FG_CO2" not in message

    def test_the_first_file_affected_is_named(self, tmp_path: Path) -> None:
        """Verify the file named is the first (by path) of those lacking a variable."""
        paths = [
            _write_cdr_file(tmp_path / f"x_cdr.2012010100000{i}.nc", omit=["hDIC"])
            for i in range(2)
        ]

        with pytest.raises(ValueError, match="2 of 2 files") as exc:
            check_required_variables([CdrOutputFile(path=path) for path in paths])

        assert f"first file affected: {paths[0]}" in str(exc.value)


class TestSourceSignReport:
    """Tests for `source_sign_report`."""

    def test_wrong_sign_cells_are_described(self) -> None:
        """Verify wrong-sign alkalinity cells are counted by cell and by record,
        with the worst value and the share of the integrated source.
        """
        ds = _cdr_output_ds(n=4).compute()
        ds["ALK_source"].values[1, 1, 1, 1] = -2.0
        ds["ALK_source"].values[2, 1, 2, 3] = -0.5

        report = source_sign_report(ds.chunk({"time": 1}))

        lines = report.splitlines()
        assert len(lines) == 2
        alk = next(line for line in lines if line.startswith("ALK_source"))
        assert "2 cells in 2 of 4 records" in alk
        assert "2012-01-01T01:30:00 .. 2012-01-01T02:30:00" in alk
        assert "worst cell -2 " in alk
        # (2 + 0.5) wrong over 4 records of the 1 right, in windows of 3600 s
        assert "62.500% of the integrated ALK source" in alk
        assert "DIC_source: no sign violations" in lines

    def test_clean_sources_report_no_violations(self) -> None:
        """Verify sources of the right signs give a line of no violations each."""
        report = source_sign_report(_cdr_output_ds())

        assert report.splitlines() == [
            "ALK_source: no sign violations",
            "DIC_source: no sign violations",
        ]

    def test_wrong_sign_dic_is_described(self) -> None:
        """Verify positive DIC cells are reported too."""
        ds = _cdr_output_ds().compute()
        ds["DIC_source"].values[3, 0, 1, 1] = 0.25

        report = source_sign_report(ds.chunk({"time": 1}))

        lines = report.splitlines()
        dic = next(line for line in lines if line.startswith("DIC_source"))
        assert "positive values" in dic
        assert "1 cells in 1 of 4 records" in dic
        assert "ALK_source: no sign violations" in lines


class TestComputeDomainMetrics:
    """Tests for `compute_domain_metrics`, with the sign checks of roms-tools."""

    _SIGN_ERROR = (
        "'ALK_source' contains negative values, which violates release constraints."
    )

    @pytest.fixture
    def wrong_sign_ds(self, fake_roms_tools: _FakeRomsTools) -> xr.Dataset:
        """Serve CDR output with negative alkalinity cells, as a forcing error or a
        parent receiving a perturbation gives.
        """
        ds = _cdr_output_ds().compute()
        ds["ALK_source"].values[1, 1, 1, 1] = -2.0
        ds["ALK_source"].values[2, 1, 2, 3] = -0.5
        fake_roms_tools.ds = ds.chunk({"time": 1})
        return fake_roms_tools.ds

    @staticmethod
    def _make_domain(
        tmp_path: Path, cdr_dirs: dict[str, Path], source_role: str
    ) -> CdrDomain:
        return CdrDomain.model_validate(
            _domain(tmp_path / "grid.nc", cdr_dirs["seg-01"], source_role=source_role)
        )

    @staticmethod
    def _raising_metrics(
        monkeypatch: pytest.MonkeyPatch, error: ValueError
    ) -> mock.Mock:
        """Patch roms-tools' metrics to raise *error*, returning the mock."""
        metrics = mock.Mock(side_effect=error)
        monkeypatch.setattr(
            cdr_metrics.roms_tools.analysis.cdr_analysis, "compute_cdr_metrics", metrics
        )
        return metrics

    @pytest.mark.usefixtures("wrong_sign_ds")
    def test_release_sign_failure_is_explained(
        self,
        tmp_path: Path,
        cdr_dirs: dict[str, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify a wrong-sign release source adds where the violations are and the
        hint to use a `transfer` domain to the error of roms-tools.
        """
        metrics = self._raising_metrics(monkeypatch, ValueError(self._SIGN_ERROR))
        domain = self._make_domain(tmp_path, cdr_dirs, "release")

        with pytest.raises(ValueError, match="violates release constraints") as exc:
            compute_domain_metrics("wio", domain)

        message = str(exc.value)
        assert message.startswith("Domain 'wio': ")
        assert self._SIGN_ERROR in message
        assert "ALK_source has negative values" in message
        assert "2 cells in 2 of 4 records" in message
        assert "DIC_source: no sign violations" in message
        assert "'transfer' domain" in message
        assert metrics.call_args.kwargs["source_role"] == "release"

    @pytest.mark.usefixtures("wrong_sign_ds")
    def test_transfer_failure_propagates_unchanged(
        self,
        tmp_path: Path,
        cdr_dirs: dict[str, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify an error of roms-tools for a transfer domain is not explained, and
        the sources are not scanned for it.
        """
        metrics = self._raising_metrics(monkeypatch, ValueError(self._SIGN_ERROR))
        report = mock.Mock()
        monkeypatch.setattr(cdr_metrics, "source_sign_report", report)
        domain = self._make_domain(tmp_path, cdr_dirs, "transfer")

        with pytest.raises(ValueError, match="violates release constraints") as exc:
            compute_domain_metrics("wio", domain)

        assert str(exc.value) == self._SIGN_ERROR
        report.assert_not_called()
        assert metrics.call_args.kwargs["source_role"] == "transfer"

    @pytest.mark.usefixtures("wrong_sign_ds")
    def test_unrelated_error_propagates_unchanged(
        self,
        tmp_path: Path,
        cdr_dirs: dict[str, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify other errors of roms-tools for a release domain are not rewritten."""
        self._raising_metrics(monkeypatch, ValueError("something else"))
        report = mock.Mock()
        monkeypatch.setattr(cdr_metrics, "source_sign_report", report)
        domain = self._make_domain(tmp_path, cdr_dirs, "release")

        with pytest.raises(ValueError, match="something else") as exc:
            compute_domain_metrics("wio", domain)

        assert str(exc.value) == "something else"
        report.assert_not_called()

    @pytest.mark.usefixtures("wrong_sign_ds")
    def test_real_sign_check_is_still_recognised(
        self, tmp_path: Path, cdr_dirs: dict[str, Path]
    ) -> None:
        """Verify the installed roms-tools' own sign check gets the explanation,
        and a transfer domain computes from the same output.
        """
        release = self._make_domain(tmp_path, cdr_dirs, "release")
        with pytest.raises(ValueError, match="'transfer' domain"):
            compute_domain_metrics("wio", release)

        transfer = self._make_domain(tmp_path, cdr_dirs, "transfer")
        ds_cdr, n_files = compute_domain_metrics("wio", transfer)

        assert n_files == 2
        assert ds_cdr.attrs["cdr_source_role"] == "transfer"

    def test_errors_name_the_domain(
        self,
        tmp_path: Path,
        cdr_dirs: dict[str, Path],
        fake_roms_tools: _FakeRomsTools,
    ) -> None:
        """Verify failures to find or load the output are prefixed with the domain."""
        fake_roms_tools.ds = _cdr_output_ds(n=4).isel(time=[0, 1, 3])
        domain = self._make_domain(tmp_path, cdr_dirs, "release")

        with pytest.raises(ValueError, match=r"Domain 'wio': CDR output is not contig"):
            compute_domain_metrics("wio", domain)

        empty = tmp_path / "empty"
        empty.mkdir()
        absent = CdrDomain.model_validate(_domain(tmp_path / "grid.nc", empty))
        with pytest.raises(FileNotFoundError, match=r"Domain 'wio': .*do_cdr_output"):
            compute_domain_metrics("wio", absent)


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
            assert series.sizes["time"] == 4
            assert all(var.dims == ("time",) for var in series.data_vars.values())
            assert {"hDIC", "area", "lat_rho"}.isdisjoint(series.variables)
            assert np.isfinite(series["cdr_efficiency_from_flux"]).all()
            assert (series["cdr_carbon_uptake_from_flux"] > 0).all()

        figure = out_dir / FIGURE_FILENAME
        assert figure.read_bytes().startswith(b"\x89PNG")

    async def test_a_single_domain_writes_no_domains_figure(
        self,
        blueprint_kwargs: dict[str, Any],
        fake_roms_tools: _FakeRomsTools,
    ) -> None:
        """Verify the map of domain outlines is only written for nested domains."""
        bp = CdrMetricsBlueprint(**blueprint_kwargs)

        await _make_runner(bp).run()

        out_dir = bp.effective_working_dir / "output"
        assert (out_dir / FIGURE_FILENAME).exists()
        assert not (out_dir / DOMAINS_FIGURE_FILENAME).exists()

    async def test_the_declared_products_are_the_files_written(
        self,
        blueprint_kwargs: dict[str, Any],
        fake_roms_tools: _FakeRomsTools,
    ) -> None:
        """Verify the products the application declares expand to the files the run wrote."""
        bp = CdrMetricsBlueprint(**blueprint_kwargs)

        await _make_runner(bp).run()

        root = bp.effective_working_dir
        declared = expand_artifacts(root, CdrMetricsApplication().artifacts(bp))
        written = {
            p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
        }
        assert {p.path for p in declared} == written
        assert written == {"output/cdr_metrics.nc", "output/cdr_metrics.png"}

    async def test_provenance_is_recorded(
        self,
        blueprint_kwargs: dict[str, Any],
        tmp_path: Path,
        fake_roms_tools: _FakeRomsTools,
    ) -> None:
        """Verify the metrics file records the domains, the number of source files,
        the grids and the roms-tools version.
        """
        bp = CdrMetricsBlueprint(**blueprint_kwargs)

        await _make_runner(bp).run()

        with xr.open_dataset(
            bp.effective_working_dir / "output" / METRICS_FILENAME
        ) as series:
            assert series.attrs["domains"] == "wio:release"
            assert series.attrs["cdr_source_files"] == 3
            assert series.attrs["cdr_grid_files"] == (tmp_path / "grid.nc").as_posix()
            assert isinstance(series.attrs["roms_tools_version"], str)
            assert series.attrs["roms_tools_version"]

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
            assert series.attrs["domains"] == "child:release"

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


def _save_blank_figure(
    self: Any, margin: float = 10.0, save_path: str | None = None
) -> None:
    """Stand in for `UpscaledCDR.plot_domains`, saving an empty figure."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.figure()
    plt.savefig(save_path)
    plt.close("all")


class TestNestedCdrMetricsRunner:
    """Tests for `CdrMetricsRunner.run` on nested domains, with the real
    `UpscaledCDR`, metrics and plotting.
    """

    @pytest.fixture
    def nested_bp(
        self,
        tmp_path: Path,
        cdr_dirs: dict[str, Path],
        fake_roms_tools: _FakeRomsTools,
        monkeypatch: pytest.MonkeyPatch,
    ) -> CdrMetricsBlueprint:
        """Nested domains: a child holding the release, inside a parent receiving a
        transfer, each with its own grid and `_cdr` files.

        The domain map is replaced by a blank figure: roms-tools draws its
        coastlines with cartopy, which downloads Natural Earth data, and unit
        tests must not use the network.
        """
        monkeypatch.setattr(
            cdr_metrics.roms_tools.UpscaledCDR, "plot_domains", _save_blank_figure
        )
        parent_dir = tmp_path / "parent" / "output"
        for name in ("x_cdr.20120101000000.nc", "x_cdr.20120101020000.nc"):
            _write_cdr_file(parent_dir / name)
        child_grid, parent_grid = tmp_path / "child.nc", tmp_path / "parent.nc"
        fake_roms_tools.ds_by_grid = {
            # the parent's cells 10 and 11 degrees east, 20.5 north, are in the child
            child_grid: _cdr_output_ds(lon_range=(10.0, 11.0), lat_range=(20.0, 21.0)),
            parent_grid: _cdr_output_ds(
                lon_range=(9.0, 12.0), lat_range=(19.0, 22.0), transfer=True
            ),
        }
        return CdrMetricsBlueprint(
            name="nested",
            description="A test nested CDR metrics blueprint.",
            working_dir=tmp_path / "work",
            domains={
                "child": _domain(child_grid, cdr_dirs["seg-01"]),
                "parent": _domain(parent_grid, parent_dir, source_role="transfer"),
            },
        )

    async def test_writes_the_combined_series_and_figures(
        self, nested_bp: CdrMetricsBlueprint
    ) -> None:
        """Verify nested domains are combined into one series with both figures."""
        runner = _make_runner(nested_bp)

        await runner.run()

        assert runner.state.status == ExecutionStatus.COMPLETED
        out_dir = nested_bp.effective_working_dir / "output"
        with xr.open_dataset(out_dir / METRICS_FILENAME) as series:
            expected = {
                "cdr_efficiency_from_flux",
                "cdr_efficiency_from_DIC_difference",
                "cdr_carbon_uptake_from_flux",
                "cdr_carbon_uptake_from_DIC_difference",
                "cdr_cumulative_source",
                "child_carbon_uptake_from_flux",
                "parent_carbon_uptake_from_flux",
                "closure_residual",
                "parent_DIC_difference_inside_child",
            }
            assert expected <= set(series.data_vars)
            assert all(var.dims == ("time",) for var in series.data_vars.values())
            assert series.sizes["time"] == 4
            # only the release counts towards the source and the efficiency
            assert np.isfinite(series["cdr_efficiency_from_flux"]).all()
            assert (series["cdr_cumulative_source"] > 0).all()
            # the parent's cells inside the child's footprint carry a DIC perturbation
            assert (series["parent_DIC_difference_inside_child"] > 0).all()

        for name in (FIGURE_FILENAME, DOMAINS_FIGURE_FILENAME):
            assert (out_dir / name).read_bytes().startswith(b"\x89PNG"), name

    async def test_the_declared_products_are_the_files_written(
        self, nested_bp: CdrMetricsBlueprint
    ) -> None:
        """Verify the products the application declares expand to the files the run
        wrote, the map of the domains among them.
        """
        await _make_runner(nested_bp).run()

        root = nested_bp.effective_working_dir
        declared = expand_artifacts(root, CdrMetricsApplication().artifacts(nested_bp))
        written = {
            p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
        }
        assert {p.path for p in declared} == written
        assert written == {
            "output/cdr_metrics.nc",
            "output/cdr_metrics.png",
            "output/cdr_domains.png",
        }

    async def test_provenance_is_recorded(
        self, nested_bp: CdrMetricsBlueprint, tmp_path: Path
    ) -> None:
        """Verify the metrics file records the domains with their roles, the number of
        source files of all domains, both grids and the roms-tools version.
        """
        await _make_runner(nested_bp).run()

        with xr.open_dataset(
            nested_bp.effective_working_dir / "output" / METRICS_FILENAME
        ) as series:
            assert series.attrs["domains"] == "child:release, parent:transfer"
            assert series.attrs["cdr_source_files"] == 4
            assert series.attrs["cdr_grid_files"] == ", ".join(
                (tmp_path / name).as_posix() for name in ("child.nc", "parent.nc")
            )
            assert isinstance(series.attrs["roms_tools_version"], str)

    async def test_a_domain_error_names_the_domain(
        self, nested_bp: CdrMetricsBlueprint, fake_roms_tools: _FakeRomsTools
    ) -> None:
        """Verify a problem in one domain's output is reported with its name, and
        stops the run before anything is written.
        """
        parent_grid = nested_bp.domains["parent"].grid.location
        fake_roms_tools.ds_by_grid[parent_grid] = _cdr_output_ds(n=4).isel(
            time=[0, 1, 3]
        )
        runner = _make_runner(nested_bp)

        with pytest.raises(ValueError, match="Domain 'parent': .*missing from"):
            await runner.run()

        assert runner.state.status != ExecutionStatus.COMPLETED
        assert not (nested_bp.effective_working_dir / "output").exists()
