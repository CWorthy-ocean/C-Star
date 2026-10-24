import logging
import shutil
from collections.abc import Generator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import netCDF4
import numpy as np
import pytest
import xarray as xr

from cstar.applications.core import ArtifactKind, RunnerRequest
from cstar.applications.upscaler import (
    CDR_TIME_CONVENTION,
    USCL_WINDOWS_FROM_BOUNDS,
    USCL_WINDOWS_INFERRED,
    CDRUpscaler,
    UpscalerBlueprint,
    UpscalerRunner,
)
from cstar.base.exceptions import CstarError
from cstar.base.utils import convert_to_cdf5
from cstar.entrypoint.config import JobConfig, ServiceConfiguration
from cstar.orchestration.manifest import Product, StepRecord, write_step_record
from cstar.orchestration.models import BlueprintIdentity


@pytest.fixture
def blueprint_kwargs(tmp_path: Path) -> dict[str, Any]:
    """Minimal valid keyword arguments for constructing an `UpscalerBlueprint`,
    with a `uscl` directory containing one (empty) matching file.
    """
    uscl_dir = tmp_path / "uscl"
    uscl_dir.mkdir()
    (uscl_dir / "roms_uscl.20240101000000.nc").touch()
    return {
        "name": "test-upscaler",
        "description": "A test upscaler blueprint.",
        "working_dir": tmp_path / "work",
        "uscl_file_location": str(uscl_dir),
    }


def _make_runner(blueprint: UpscalerBlueprint) -> UpscalerRunner:
    """Construct an `UpscalerRunner` wired to *blueprint* without touching disk.

    `RunnerRequest.blueprint` normally deserializes from a URI on first
    access; setting the private `_bp` cache directly lets the request return
    an in-memory blueprint instance instead.
    """
    request: RunnerRequest[UpscalerBlueprint] = RunnerRequest(
        "unused://blueprint", UpscalerBlueprint
    )
    request._bp = blueprint

    service_config = ServiceConfiguration(
        as_service=False,
        loop_delay=0,
        health_check_frequency=None,
        log_level=logging.DEBUG,
        health_check_log_threshold=10,
        name="test_upscaler_runner",
    )
    job_config = JobConfig(account_id="", walltime="", priority="")

    return UpscalerRunner(request, service_config, job_config)


class TestUpscalerBlueprintPio:
    """Tests for the `pio` field on `UpscalerBlueprint`."""

    def test_pio_defaults_true(self, blueprint_kwargs: dict[str, Any]) -> None:
        """Verify that `pio` defaults to True when omitted."""
        bp = UpscalerBlueprint(**blueprint_kwargs)
        assert bp.pio is True

    def test_pio_false_validates(self, blueprint_kwargs: dict[str, Any]) -> None:
        """Verify that `pio=False` is accepted."""
        bp = UpscalerBlueprint(**blueprint_kwargs, pio=False)
        assert bp.pio is False


class TestUpscalerRunnerSaveRouting:
    """Tests for the PIO-conditional save/convert routing in `UpscalerRunner.run`."""

    @pytest.fixture
    def mock_upscaler(self) -> Generator[mock.Mock, None, None]:
        """Replace `CDRUpscaler` so no `uscl` data is opened or processed."""
        with mock.patch("cstar.applications.upscaler.CDRUpscaler") as mock_cls:
            yield mock_cls.return_value

    def _expected_final_path(self, blueprint: UpscalerBlueprint) -> Path:
        return blueprint.effective_working_dir / "output" / "upscaled_cdr.nc"

    async def test_pio_true_saves_to_mangled_path_and_converts(
        self,
        blueprint_kwargs: dict[str, Any],
        mock_upscaler: mock.Mock,
    ) -> None:
        """Verify that with `pio=True`, the CDR forcing is saved to the
        `_nc4`-mangled path and `convert_to_cdf5` is invoked with the mangled
        and final paths.
        """
        bp = UpscalerBlueprint(**blueprint_kwargs, pio=True)
        runner = _make_runner(bp)

        final_path = self._expected_final_path(bp)
        # Hardcoded (not derived via the production mangling expression) so a
        # botched change to the mangling scheme fails this assertion.
        nc4_path = final_path.with_name("upscaled_cdr_nc4.nc")

        with mock.patch("cstar.applications.upscaler.convert_to_cdf5") as mock_convert:
            await runner.run()

        mock_upscaler.save.assert_called_once_with(nc4_path)
        mock_convert.assert_called_once_with(nc4_path, final_path)

    async def test_pio_false_saves_directly_without_conversion(
        self,
        blueprint_kwargs: dict[str, Any],
        mock_upscaler: mock.Mock,
    ) -> None:
        """Verify that with `pio=False`, the CDR forcing is saved directly to
        the final path and no conversion is attempted.
        """
        bp = UpscalerBlueprint(**blueprint_kwargs, pio=False)
        runner = _make_runner(bp)

        with mock.patch("cstar.applications.upscaler.convert_to_cdf5") as mock_convert:
            await runner.run()

        mock_upscaler.save.assert_called_once_with(self._expected_final_path(bp))
        mock_convert.assert_not_called()


class TestUpscalerRunnerUsclFiles:
    """Tests for which `_uscl` files `UpscalerRunner.run` reads."""

    @pytest.fixture
    def mock_upscaler_cls(self) -> Generator[mock.Mock, None, None]:
        """Replace `CDRUpscaler` so no `uscl` data is opened, exposing its class mock."""
        with mock.patch("cstar.applications.upscaler.CDRUpscaler") as mock_cls:
            yield mock_cls

    @staticmethod
    def _write_record(step_root: Path, *paths: Path) -> None:
        """Write the record of a step that listed `paths` as `upscaling` products."""
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
                    kind=ArtifactKind.upscaling,
                    path=path.relative_to(step_root).as_posix(),
                )
                for path in paths
            ],
        )
        write_step_record(step_root, record)

    async def _uscl_files_read(
        self, blueprint_kwargs: dict[str, Any], mock_upscaler_cls: mock.Mock
    ) -> list[Path]:
        """Run the upscaler and return the files it handed to `CDRUpscaler`."""
        runner = _make_runner(UpscalerBlueprint(**blueprint_kwargs, pio=False))
        await runner.run()
        return sorted(mock_upscaler_cls.call_args.args[0])

    async def test_recorded_files_are_read(
        self, blueprint_kwargs: dict[str, Any], mock_upscaler_cls: mock.Mock
    ) -> None:
        """Verify the files the record beside the directory lists are read,
        not every `_uscl` file in the directory.
        """
        uscl_dir = Path(blueprint_kwargs["uscl_file_location"])
        recorded = uscl_dir / "roms_uscl.20240101000000.nc"
        (uscl_dir / "roms_uscl.20240201000000.nc").touch()
        self._write_record(uscl_dir.parent, recorded)

        files = await self._uscl_files_read(blueprint_kwargs, mock_upscaler_cls)

        assert files == [recorded]

    @pytest.mark.parametrize(
        "listed",
        [
            pytest.param(
                "elsewhere/roms_uscl.20240301000000.nc", id="outside the directory"
            ),
            pytest.param("uscl/notes_uscl.nc", id="not named like a uscl file"),
        ],
    )
    async def test_unrelated_recorded_files_are_not_read(
        self,
        blueprint_kwargs: dict[str, Any],
        mock_upscaler_cls: mock.Mock,
        listed: str,
    ) -> None:
        """Verify a record listing no `_uscl` file of the directory leaves the
        directory to be searched.
        """
        uscl_dir = Path(blueprint_kwargs["uscl_file_location"])
        unrelated = uscl_dir.parent / listed
        unrelated.parent.mkdir(exist_ok=True)
        unrelated.touch()
        self._write_record(uscl_dir.parent, unrelated)

        files = await self._uscl_files_read(blueprint_kwargs, mock_upscaler_cls)

        assert files == [uscl_dir / "roms_uscl.20240101000000.nc"]

    async def test_directory_is_searched_without_a_record(
        self, blueprint_kwargs: dict[str, Any], mock_upscaler_cls: mock.Mock
    ) -> None:
        """Verify a directory with no record beside it is searched."""
        uscl_dir = Path(blueprint_kwargs["uscl_file_location"])
        (uscl_dir / "roms_uscl.20240201000000.nc").touch()

        files = await self._uscl_files_read(blueprint_kwargs, mock_upscaler_cls)

        assert files == [
            uscl_dir / "roms_uscl.20240101000000.nc",
            uscl_dir / "roms_uscl.20240201000000.nc",
        ]

    async def test_missing_recorded_file_is_refused(
        self, blueprint_kwargs: dict[str, Any], mock_upscaler_cls: mock.Mock
    ) -> None:
        """Verify a `_uscl` file the record lists but that is gone fails the run,
        rather than upscaling the files that remain.
        """
        uscl_dir = Path(blueprint_kwargs["uscl_file_location"])
        gone = uscl_dir / "roms_uscl.20240201000000.nc"
        self._write_record(uscl_dir.parent, gone)

        with pytest.raises(CstarError, match="roms_uscl.20240201000000.nc"):
            await self._uscl_files_read(blueprint_kwargs, mock_upscaler_cls)


_N_TIME, _N_S_RHO, _N_XI, _N_ETA = 2, 4, 3, 2


def _write_uscl(path: Path, cdr_tracers: dict[str, tuple[str, ...]]) -> xr.Dataset:
    """Write a small joined `_uscl` file with a northern and an eastern boundary.

    Each flux variable gets distinct values so a column mix-up shows.
    `cdr_tracers` maps a CDR-lite tracer name to the boundaries its
    `<tracer>_add_<side>` variable is written for.
    """
    sizes = {"north": _N_XI, "east": _N_ETA}
    along = {"north": "xi_rho", "east": "eta_rho"}
    ds = xr.Dataset(
        coords={
            "s_rho": np.arange(_N_S_RHO),
            "xi_rho": np.arange(_N_XI),
            "eta_rho": np.arange(_N_ETA),
        }
    )
    ds["ocean_time"] = ("time", np.array([0.0, 3600.0]))
    seed = 0

    def field(bry: str) -> np.ndarray:
        nonlocal seed
        seed += 1
        shape = (_N_TIME, _N_S_RHO, sizes[bry])
        return seed * 1000 + np.arange(np.prod(shape), dtype=float).reshape(shape)

    for bry in ("north", "east"):
        dims = ("time", "s_rho", along[bry])
        ds[f"lat_{bry}"] = (along[bry], np.linspace(10, 11, sizes[bry]))
        ds[f"lon_{bry}"] = (along[bry], np.linspace(20, 21, sizes[bry]))
        ds[f"h_{bry}"] = (dims, field(bry))
        ds[f"ALK_add_{bry}"] = (dims, field(bry))
        ds[f"DIC_add_{bry}"] = (dims, field(bry))
    for tracer, brys in cdr_tracers.items():
        for bry in brys:
            ds[f"{tracer}_add_{bry}"] = (("time", "s_rho", along[bry]), field(bry))
    ds.to_netcdf(path)
    return ds


def _profiles(ds: xr.Dataset, prefix: str) -> np.ndarray:
    """`prefix` fluxes of both boundaries as CDRUpscaler orders them (north, east)."""
    return np.concatenate(
        [ds[f"{prefix}_north"].values, ds[f"{prefix}_east"].values], axis=-1
    )


def _shifted(records: np.ndarray) -> np.ndarray:
    """`records` without the first (start-up) record, then a closing zero record."""
    return np.concatenate([records[1:], np.zeros_like(records[:1])])


def _upscale(uscl: Path, out: Path) -> CDRUpscaler:
    upscaler = CDRUpscaler([uscl])
    upscaler.create_cdr_dataset()
    upscaler.populate_cdr_dataset()
    upscaler.save(out)
    return upscaler


class TestCDRUpscalerTracers:
    """The tracer columns `CDRUpscaler` writes (ucla-roms #390)."""

    def test_without_cdr_lite_tracers_writes_the_two_column_file(
        self, tmp_path: Path
    ) -> None:
        """Without CDR-lite tracers the file is unchanged: ALK and DIC along a
        `two` axis and no `cdr_trc_name`, which ucla-roms < 0.11.0 and MARBL
        parents read as before.
        """
        src = _write_uscl(tmp_path / "roms_uscl.20120101000000.nc", {})

        _upscale(tmp_path / "roms_uscl.20120101000000.nc", tmp_path / "cdr.nc")

        with xr.open_dataset(tmp_path / "cdr.nc") as out:
            assert out["cdr_trcflx_profile"].dims == (
                "cdr_time",
                "s_rho",
                "two",
                "ncdr_prof",
            )
            assert "cdr_trc_name" not in out.variables
            np.testing.assert_array_equal(
                out["cdr_trcflx_profile"][:, :, 0, :],
                _shifted(_profiles(src, "ALK_add")),
            )
            np.testing.assert_array_equal(
                out["cdr_trcflx_profile"][:, :, 1, :],
                _shifted(_profiles(src, "DIC_add")),
            )

    def test_cdr_lite_tracers_follow_alk_and_dic_with_their_names(
        self, tmp_path: Path
    ) -> None:
        """CDR-lite fluxes follow ALK and DIC in ROMS tracer order, and
        `cdr_trc_name` names every column. A tracer missing from an active
        boundary is left out.
        """
        uscl = tmp_path / "roms_uscl.20120101000000.nc"
        src = _write_uscl(
            uscl,
            {
                "CDR_DOR_DIC1": ("north", "east"),
                "CDR_OAE_DIC1": ("north", "east"),
                "CDR_OAE_ALK2": ("north",),
                "CDR_OAE_ALK1": ("north", "east"),
            },
        )
        expected = ["ALK", "DIC", "CDR_OAE_ALK1", "CDR_OAE_DIC1", "CDR_DOR_DIC1"]

        upscaler = _upscale(uscl, tmp_path / "cdr.nc")

        assert upscaler.tracer_names == expected
        with xr.open_dataset(tmp_path / "cdr.nc") as out:
            assert out["cdr_trcflx_profile"].dims[2] == "ntrc"
            assert list(out["cdr_trc_name"].values.astype(str)) == expected
            for i, name in enumerate(expected):
                np.testing.assert_array_equal(
                    out["cdr_trcflx_profile"][:, :, i, :],
                    _shifted(_profiles(src, f"{name}_add")),
                )

    def test_a_tracer_missing_from_an_active_boundary_is_left_out_with_a_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """ucla-roms writes every CDR-lite tracer on every open boundary, so a gap
        means incomplete `_uscl` files: the tracer is dropped, and said so.
        """
        uscl = tmp_path / "roms_uscl.20120101000000.nc"
        _write_uscl(
            uscl,
            {"CDR_OAE_ALK1": ("north", "east"), "CDR_OAE_DIC1": ("north",)},
        )

        with caplog.at_level(logging.WARNING, logger="cstar.applications.upscaler"):
            upscaler = CDRUpscaler([uscl])

        assert upscaler.tracer_names == ["ALK", "DIC", "CDR_OAE_ALK1"]
        assert "Leaving CDR_OAE_DIC1 out of the CDR forcing" in caplog.text
        assert "['east']" in caplog.text

    @pytest.mark.skipif(shutil.which("nccopy") is None, reason="nccopy not installed")
    def test_tracer_names_are_fixed_length_characters_after_cdf5_conversion(
        self, tmp_path: Path
    ) -> None:
        """ROMS reads `cdr_trc_name` as NC_CHAR with dimensions (tracer, nchar);
        NC_STRING would abort its init. The PIO path converts the file to CDF-5
        with `nccopy`, which must keep it that way.
        """
        uscl = tmp_path / "roms_uscl.20120101000000.nc"
        _write_uscl(uscl, {"CDR_OAE_ALK1": ("north", "east")})
        _upscale(uscl, tmp_path / "cdr_nc4.nc")

        convert_to_cdf5(tmp_path / "cdr_nc4.nc", tmp_path / "cdr.nc")

        with netCDF4.Dataset(tmp_path / "cdr.nc") as nc:
            assert nc.data_model == "NETCDF3_64BIT_DATA"
            names = nc["cdr_trc_name"]
            assert names.dtype == np.dtype("S1")
            assert names.dimensions == ("ntrc", "trc_name_len")
            assert nc["cdr_trcflx_profile"].dimensions == (
                "cdr_time",
                "s_rho",
                "ntrc",
                "ncdr_prof",
            )
            names.set_auto_chartostring(False)
            assert list(netCDF4.chartostring(names[:])) == [
                "ALK",
                "DIC",
                "CDR_OAE_ALK1",
            ]


_HOUR_S = 3600.0
"""Seconds between records in the time-ordered synthetic `_uscl` files."""


def _write_hourly_uscl(
    path: Path,
    hours: Sequence[float],
    offset_s: float = 0.0,
    windows: Sequence[tuple[float, float]] | None = None,
) -> Path:
    """Write a tiny `_uscl` file with one record per entry of *hours*.

    Every flux and layer thickness at a record equals its hour, so records can be
    traced through the concatenation. *offset_s* is added to every `ocean_time`,
    as the seconds since the model reference date of a real run are large.
    *windows* are the (begin, end) hours of each record's averaging window, written
    as `avg_begin_time` and `avg_end_time` as newer ucla-roms does; without them
    the file is a ucla-roms <= 0.12.0 one.
    """
    h = np.asarray(hours, dtype=float)
    ds = xr.Dataset(
        {
            "ocean_time": (("time",), h * _HOUR_S + offset_s),
            "lat_north": (("xi_rho",), np.linspace(10.0, 11.0, _N_XI)),
            "lon_north": (("xi_rho",), np.linspace(20.0, 21.0, _N_XI)),
            "lat_east": (("eta_rho",), np.linspace(10.0, 11.0, _N_ETA)),
            "lon_east": (("eta_rho",), np.linspace(30.0, 31.0, _N_ETA)),
        }
    )
    if windows is not None:
        begin, end = np.asarray(windows, dtype=float).T * _HOUR_S + offset_s
        ds["avg_begin_time"] = (("time",), begin)
        ds["avg_end_time"] = (("time",), end)
    for bry, dim, n in (("north", "xi_rho", _N_XI), ("east", "eta_rho", _N_ETA)):
        for prefix in ("ALK_add", "DIC_add", "h"):
            ds[f"{prefix}_{bry}"] = (
                ("time", "s_rho", dim),
                np.broadcast_to(h[:, None, None], (len(h), _N_S_RHO, n)).copy(),
            )
    ds.to_netcdf(path, unlimited_dims=["time"])
    return path


class TestCDRUpscalerTimeOrder:
    """`CDRUpscaler` concatenates `_uscl` files in time order, however they arrive."""

    @pytest.fixture
    def uscl_files(self, tmp_path: Path) -> list[Path]:
        """Four `_uscl` files with two hourly records each, in chronological order."""
        names_and_hours = [
            ("roms_uscl.20240101000000.nc", [0, 1]),
            ("roms_uscl.20240101020000.nc", [2, 3]),
            ("roms_uscl.20240101040000.nc", [4, 5]),
            ("roms_uscl.20240101060000.nc", [6, 7]),
        ]
        # Write them out of chronological order, as a directory listing may return
        # them, so the on-disk order differs from the time order too.
        written = {
            name: _write_hourly_uscl(tmp_path / name, hours)
            for name, hours in reversed(names_and_hours)
        }
        return [written[name] for name, _ in names_and_hours]

    @pytest.mark.parametrize(
        "order",
        [
            pytest.param([3, 2, 1, 0], id="reversed"),
            pytest.param([3, 0, 2, 1], id="scrambled"),
            pytest.param([0, 1, 2, 3], id="chronological"),
        ],
    )
    def test_cdr_time_is_chronological_for_any_input_order(
        self, uscl_files: list[Path], order: list[int]
    ) -> None:
        """Verify `cdr_time` is strictly increasing from the earliest record, and
        the flux records follow it, however the files are ordered when passed in.
        The first record is the start-up sample and drops out of the windows.
        """
        upscaler = CDRUpscaler([uscl_files[i] for i in order])
        upscaler.create_cdr_dataset()
        upscaler.populate_cdr_dataset()

        assert upscaler.cdr_dataset is not None
        cdr_time = upscaler.cdr_dataset["cdr_time"].values
        assert np.all(np.diff(cdr_time) > 0)
        assert cdr_time[0] == 0.0
        np.testing.assert_allclose(cdr_time, np.arange(8) * _HOUR_S / 86400.0)

        # Each record's flux and layer thickness were written equal to its hour.
        # Window k is record k + 1, and the closing record has no flux.
        expected = np.append(np.arange(1, 8, dtype=float), 0.0)
        flux = upscaler.cdr_dataset["cdr_trcflx_profile"].values
        np.testing.assert_array_equal(flux[:, 0, 0, 0], expected)
        np.testing.assert_array_equal(flux[:, 0, 1, -1], expected)
        thickness = upscaler.cdr_dataset["cdr_layer_thickness"].values
        np.testing.assert_array_equal(thickness[:, 0, 0], np.arange(1, 9).clip(max=7))

    def test_files_are_stored_in_time_order(self, uscl_files: list[Path]) -> None:
        """Verify `CDRUpscaler.files` is time-sorted, and the input list is untouched."""
        shuffled: list[Path | str] = [uscl_files[i] for i in (2, 0, 3, 1)]
        upscaler = CDRUpscaler(shuffled)

        assert upscaler.files == uscl_files
        assert shuffled == [uscl_files[i] for i in (2, 0, 3, 1)]

    def test_files_without_timestamps_keep_the_given_order(
        self, tmp_path: Path
    ) -> None:
        """Verify names without a 14-digit timestamp are not reordered, so numbered
        files like ``uscl.1 ... uscl.10`` are not sorted alphabetically.
        """
        files: list[Path | str] = [
            _write_hourly_uscl(
                tmp_path / f"a_uscl.{i}.nc", [2 * (i - 1), 2 * (i - 1) + 1]
            )
            for i in range(1, 11)
        ]

        upscaler = CDRUpscaler(files)

        assert upscaler.files == files
        assert np.all(np.diff(upscaler.uscl_dataset.ocean_time.values) > 0)

    def test_files_without_timestamps_sort_before_stamped_files(
        self, tmp_path: Path
    ) -> None:
        """Verify an unstamped file is placed first, ahead of every stamped one."""
        unstamped = _write_hourly_uscl(tmp_path / "a_uscl.restart.nc", [0, 1])
        late = _write_hourly_uscl(tmp_path / "a_uscl.20240101040000.nc", [4, 5])
        early = _write_hourly_uscl(tmp_path / "a_uscl.20240101020000.nc", [2, 3])

        upscaler = CDRUpscaler([late, early, unstamped])

        assert upscaler.files == [unstamped, early, late]
        assert np.all(np.diff(upscaler.uscl_dataset.ocean_time.values) > 0)

    def test_overlapping_records_raise(self, tmp_path: Path) -> None:
        """Verify a record repeated across files is reported, not silently kept."""
        first = _write_hourly_uscl(tmp_path / "roms_uscl.20240101000000.nc", [0, 1, 2])
        second = _write_hourly_uscl(tmp_path / "roms_uscl.20240101020000.nc", [2, 3, 4])

        with pytest.raises(ValueError, match="strictly increasing"):
            CDRUpscaler([first, second])

    def test_overlapping_records_error_names_the_record_and_files(
        self, tmp_path: Path
    ) -> None:
        """Verify the error names the first offending record, its time and the
        files holding it and the record before it.
        """
        first = _write_hourly_uscl(tmp_path / "roms_uscl.20240101000000.nc", [0, 1, 2])
        second = _write_hourly_uscl(tmp_path / "roms_uscl.20240101020000.nc", [2, 3, 4])

        with pytest.raises(ValueError) as excinfo:
            CDRUpscaler([first, second])

        assert str(excinfo.value) == (
            "`ocean_time` of the concatenated `_uscl` files is not strictly "
            "increasing: record 3 (7200 s, in roms_uscl.20240101020000.nc) does "
            "not follow record 2 (7200 s, in roms_uscl.20240101000000.nc). The "
            "files contain repeated or out-of-order records."
        )

    def test_overlapping_records_error_shows_large_times_to_the_second(
        self, tmp_path: Path
    ) -> None:
        """Verify times of a real run (about 3.8e8 s) a minute apart print as
        different numbers, not rounded to the same six significant digits.
        """
        run_start_s = 380_000_000.0
        first = _write_hourly_uscl(
            tmp_path / "roms_uscl.20240101000000.nc", [0, 1, 2], offset_s=run_start_s
        )
        # The second file starts a minute before the last record of the first.
        second = _write_hourly_uscl(
            tmp_path / "roms_uscl.20240101020000.nc",
            [2, 3, 4],
            offset_s=run_start_s - 60.0,
        )

        with pytest.raises(ValueError) as excinfo:
            CDRUpscaler([first, second])

        assert str(excinfo.value) == (
            "`ocean_time` of the concatenated `_uscl` files is not strictly "
            "increasing: record 3 (380007140 s, in roms_uscl.20240101020000.nc) does "
            "not follow record 2 (380007200 s, in roms_uscl.20240101000000.nc). The "
            "files contain repeated or out-of-order records."
        )

    def test_overlapping_records_close_the_dataset(self, tmp_path: Path) -> None:
        """Verify the concatenated dataset is closed when the time check fails."""
        first = _write_hourly_uscl(tmp_path / "roms_uscl.20240101000000.nc", [0, 1, 2])
        second = _write_hourly_uscl(tmp_path / "roms_uscl.20240101020000.nc", [2, 3, 4])

        with (
            mock.patch.object(
                xr.Dataset, "close", autospec=True, side_effect=xr.Dataset.close
            ) as close,
            pytest.raises(ValueError, match="strictly increasing"),
        ):
            CDRUpscaler([first, second])

        close.assert_called()
        # xarray closes other datasets too (the per-file ones); the joined one has
        # all six records.
        assert any(call.args[0].sizes.get("time") == 6 for call in close.call_args_list)


def _upscaled(files: Sequence[Path | str]) -> xr.Dataset:
    """The CDR forcing dataset `CDRUpscaler` builds from *files*."""
    upscaler = CDRUpscaler(list(files))
    upscaler.create_cdr_dataset()
    upscaler.populate_cdr_dataset()
    assert upscaler.cdr_dataset is not None
    return upscaler.cdr_dataset


class TestCDRUpscalerWindows:
    """The averaging windows behind `cdr_time` and the records, with and without the
    `avg_begin_time`/`avg_end_time` of newer ucla-roms.
    """

    @pytest.fixture
    def legacy_files(self, tmp_path: Path) -> list[Path]:
        """Two ucla-roms <= 0.12.0 files: a start-up record at hour 0, then hours 1-5."""
        return [
            _write_hourly_uscl(tmp_path / "roms_uscl.20240101000000.nc", [0, 1, 2]),
            _write_hourly_uscl(tmp_path / "roms_uscl.20240101030000.nc", [3, 4, 5]),
        ]

    @pytest.fixture
    def bounded_files(self, tmp_path: Path) -> list[Path]:
        """Two files with bounds and no start-up record, the last window partial."""
        return [
            _write_hourly_uscl(
                tmp_path / "roms_uscl.20240101010000.nc",
                [1, 2, 3],
                windows=[(0, 1), (1, 2), (2, 3)],
            ),
            _write_hourly_uscl(
                tmp_path / "roms_uscl.20240101040000.nc",
                [4, 5.5],
                windows=[(3, 4), (4, 5.5)],
            ),
        ]

    def test_legacy_files_shift_records_one_earlier_with_a_zero_closing_record(
        self, legacy_files: list[Path]
    ) -> None:
        """Verify the start-up record is dropped, so each hour is applied from the
        start of its own window, and a zero record closes the last one.
        """
        ds = _upscaled(legacy_files)

        np.testing.assert_allclose(ds["cdr_time"], np.arange(6) * _HOUR_S / 86400.0)
        flux = ds["cdr_trcflx_profile"].values
        np.testing.assert_array_equal(flux[:, 0, 0, 0], [1, 2, 3, 4, 5, 0])
        np.testing.assert_array_equal(flux[:, 0, 1, -1], [1, 2, 3, 4, 5, 0])
        assert np.all(flux[-1] == 0)
        thickness = ds["cdr_layer_thickness"].values
        np.testing.assert_array_equal(thickness[:, 0, 0], [1, 2, 3, 4, 5, 5])
        assert ds.attrs["uscl_windows"] == USCL_WINDOWS_INFERRED

    def test_bounded_files_use_their_windows_and_keep_every_record(
        self, bounded_files: list[Path]
    ) -> None:
        """Verify `cdr_time` is the window starts plus the last end, the partial
        last window included, and every record is data, then a zero record.
        """
        ds = _upscaled(bounded_files)

        np.testing.assert_allclose(
            ds["cdr_time"], np.array([0, 1, 2, 3, 4, 5.5]) * _HOUR_S / 86400.0
        )
        flux = ds["cdr_trcflx_profile"].values
        np.testing.assert_array_equal(flux[:, 0, 0, 0], [1, 2, 3, 4, 5.5, 0])
        assert np.all(flux[-1] == 0)
        thickness = ds["cdr_layer_thickness"].values
        np.testing.assert_array_equal(thickness[:, 0, 0], [1, 2, 3, 4, 5.5, 5.5])
        assert ds.attrs["uscl_windows"] == USCL_WINDOWS_FROM_BOUNDS

    @pytest.mark.parametrize(
        ("files", "windows"),
        [
            pytest.param("legacy_files", USCL_WINDOWS_INFERRED, id="legacy"),
            pytest.param("bounded_files", USCL_WINDOWS_FROM_BOUNDS, id="bounded"),
        ],
    )
    def test_attributes_survive_saving(
        self,
        request: pytest.FixtureRequest,
        files: str,
        windows: str,
        tmp_path: Path,
    ) -> None:
        """Verify the convention and where the windows came from are in the file."""
        inputs: list[Path | str] = request.getfixturevalue(files)
        upscaler = CDRUpscaler(inputs)
        upscaler.create_cdr_dataset()
        upscaler.populate_cdr_dataset()
        upscaler.save(tmp_path / "cdr.nc")

        with xr.open_dataset(tmp_path / "cdr.nc") as out:
            assert out["cdr_time"].attrs["convention"] == CDR_TIME_CONVENTION
            assert out.attrs["uscl_windows"] == windows

    @pytest.mark.parametrize(
        ("legacy_hours", "bounded_windows"),
        [
            pytest.param([0, 1, 2, 3, 4], [(0, 1), (1, 2), (2, 3), (3, 4)], id="even"),
            pytest.param([0, 2, 4, 6, 7], [(0, 2), (2, 3), (3, 3.5)], id="uneven"),
        ],
    )
    def test_flux_integral_over_time_is_conserved(
        self,
        tmp_path: Path,
        legacy_hours: list[float],
        bounded_windows: list[tuple[float, float]],
    ) -> None:
        """Verify the flux times its interval, summed over the file, equals the sum
        of each input window's record times its duration: nothing is lost or
        applied for a different time than it was exported over.
        """
        legacy = _write_hourly_uscl(tmp_path / "legacy_uscl.nc", legacy_hours)
        bounded = _write_hourly_uscl(
            tmp_path / "bounded_uscl.nc",
            [end for _, end in bounded_windows],
            windows=bounded_windows,
        )
        # Records equal their hour, so a legacy record is worth its stamp; the
        # first is the start-up sample and has no window.
        legacy_expected = sum(
            record * (end - begin)
            for record, begin, end in zip(
                legacy_hours[1:], legacy_hours[:-1], legacy_hours[1:], strict=True
            )
        )
        bounded_expected = sum(end * (end - begin) for begin, end in bounded_windows)

        for file, expected in ((legacy, legacy_expected), (bounded, bounded_expected)):
            ds = _upscaled([file])
            interval_s = np.diff(ds["cdr_time"].values) * 86400.0
            flux = ds["cdr_trcflx_profile"].values[:-1, 0, 0, 0]
            np.testing.assert_allclose(np.sum(flux * interval_s), expected * _HOUR_S)

    def test_files_with_and_without_bounds_are_refused(
        self, tmp_path: Path, bounded_files: list[Path]
    ) -> None:
        """Verify mixing a file with bounds and one without is an error naming the
        file that lacks them.
        """
        legacy = _write_hourly_uscl(tmp_path / "roms_uscl.20240101060000.nc", [6, 7])

        with pytest.raises(ValueError, match="Only some.*roms_uscl.20240101060000.nc"):
            CDRUpscaler([*bounded_files, legacy])

    def test_a_file_with_only_one_bound_is_refused(
        self, bounded_files: list[Path]
    ) -> None:
        """Verify `avg_begin_time` without `avg_end_time` is an error."""
        with xr.open_dataset(bounded_files[1]) as ds:
            trimmed = ds.drop_vars("avg_end_time").load()
        trimmed.to_netcdf(bounded_files[1])

        with pytest.raises(ValueError, match="incomplete.*only avg_begin_time"):
            CDRUpscaler([*bounded_files])

    @pytest.mark.parametrize(
        ("second_windows", "kind"),
        [
            pytest.param([(3.5, 4.5), (4.5, 5.5)], "gap", id="gap"),
            pytest.param([(2.5, 4), (4, 5.5)], "overlap", id="overlap"),
        ],
    )
    def test_windows_that_are_not_contiguous_are_refused(
        self,
        tmp_path: Path,
        second_windows: list[tuple[float, float]],
        kind: str,
    ) -> None:
        """Verify a gap or overlap between windows names the file and times, and
        closes the dataset it opened.
        """
        first = _write_hourly_uscl(
            tmp_path / "roms_uscl.20240101010000.nc",
            [1, 2, 3],
            windows=[(0, 1), (1, 2), (2, 3)],
        )
        second = _write_hourly_uscl(
            tmp_path / "roms_uscl.20240101040000.nc",
            [end for _, end in second_windows],
            windows=second_windows,
        )

        with (
            mock.patch.object(
                xr.Dataset, "close", autospec=True, side_effect=xr.Dataset.close
            ) as close,
            pytest.raises(ValueError, match="not contiguous") as excinfo,
        ):
            CDRUpscaler([first, second])

        message = str(excinfo.value)
        assert f"a {kind} between" in message
        assert "in roms_uscl.20240101040000.nc" in message
        assert "1800 s" in message
        assert any(call.args[0].sizes.get("time") == 5 for call in close.call_args_list)

    def test_a_single_legacy_record_has_no_window(self, tmp_path: Path) -> None:
        """Verify a file holding only the start-up record is refused."""
        only = _write_hourly_uscl(tmp_path / "roms_uscl.20240101000000.nc", [0])

        with pytest.raises(ValueError, match="no averaging window"):
            CDRUpscaler([only])

    @pytest.mark.parametrize(
        ("hours", "window", "length_s", "expected_s"),
        [
            pytest.param([[0, 1, 2], [4, 5]], 2, 7200, 3600, id="gap-between-files"),
            pytest.param([[0, 1, 2, 4]], 2, 7200, 3600, id="last-window-longer"),
            pytest.param([[0, 2, 3, 4]], 1, 3600, 7200, id="short-before-the-last"),
        ],
    )
    def test_legacy_windows_of_unequal_length_are_refused(
        self,
        tmp_path: Path,
        hours: list[list[float]],
        window: int,
        length_s: int,
        expected_s: int,
    ) -> None:
        """Verify a missing `_uscl` file, which only stretches one inferred window,
        is an error naming the window, its file and the expected length.
        """
        files: list[Path | str] = [
            _write_hourly_uscl(tmp_path / f"roms_uscl.2024010{1 + i}000000.nc", h)
            for i, h in enumerate(hours)
        ]

        with pytest.raises(ValueError, match="not of equal length") as excinfo:
            CDRUpscaler(files)

        message = str(excinfo.value)
        assert f"window {window} " in message
        assert f"is {length_s} s long" in message
        assert f"where {expected_s} s is expected" in message
        assert "missing or out-of-run `_uscl` file" in message
        assert Path(files[-1]).name in message

    def test_a_shorter_last_legacy_window_is_accepted(self, tmp_path: Path) -> None:
        """Verify a shorter final window is kept: it cannot hide a missing file.

        ucla-roms <= 0.12.0 does not write one (a run ending mid-hour drops the
        partial hour), so this only pins that the length check does not refuse it.
        """
        only = _write_hourly_uscl(
            tmp_path / "roms_uscl.20240101000000.nc", [0, 1, 2, 2.5]
        )

        ds = _upscaled([only])

        np.testing.assert_allclose(
            ds["cdr_time"], np.array([0, 1, 2, 2.5]) * _HOUR_S / 86400.0
        )

    def test_legacy_files_of_two_runs_are_refused(self, tmp_path: Path) -> None:
        """Verify a second run's start-up record, which repeats the first run's
        last stamp, is rejected rather than read as a window.
        """
        first = _write_hourly_uscl(tmp_path / "roms_uscl.20240101000000.nc", [0, 1, 2])
        second = _write_hourly_uscl(tmp_path / "roms_uscl.20240101020000.nc", [2, 3, 4])

        with pytest.raises(ValueError, match="not strictly increasing"):
            CDRUpscaler([first, second])
