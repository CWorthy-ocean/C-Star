import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest import mock

import numpy as np
import pytest
import xarray as xr

from cstar.applications.core import RunnerRequest
from cstar.applications.upscaler import CDRUpscaler, UpscalerBlueprint, UpscalerRunner
from cstar.entrypoint.config import JobConfig, ServiceConfiguration


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


N_XI = 3
"""Number of points along the synthetic north boundary."""
N_ETA = 2
"""Number of points along the synthetic east boundary."""
N_S = 2
"""Number of vertical levels in the synthetic `uscl` files."""
HOUR_S = 3600.0
"""Seconds between records in the synthetic `uscl` files."""


def _write_uscl_file(path: Path, hours: list[int]) -> Path:
    """Write a tiny `_uscl` file with one record per entry of *hours*.

    The tracer additionality at each record equals its hour, so that records can be
    traced through the concatenation.
    """
    nt = len(hours)
    h = np.asarray(hours, dtype=float)
    ds = xr.Dataset(
        {
            "ocean_time": (("time",), h * HOUR_S),
            "lat_north": (("xi_rho",), np.linspace(10.0, 11.0, N_XI)),
            "lon_north": (("xi_rho",), np.linspace(20.0, 21.0, N_XI)),
            "lat_east": (("eta_rho",), np.linspace(10.0, 11.0, N_ETA)),
            "lon_east": (("eta_rho",), np.linspace(30.0, 31.0, N_ETA)),
        }
    )
    for bry, dim, n in (("north", "xi_rho", N_XI), ("east", "eta_rho", N_ETA)):
        for prefix in ("ALK_add", "DIC_add", "h"):
            ds[f"{prefix}_{bry}"] = (
                ("time", "s_rho", dim),
                np.broadcast_to(h[:, None, None], (nt, N_S, n)).copy(),
            )
    ds.to_netcdf(path, unlimited_dims=["time"])
    return path


class TestCDRUpscalerTimeOrder:
    """Tests that `CDRUpscaler` concatenates `uscl` files in time order."""

    @pytest.fixture
    def uscl_files(self, tmp_path: Path) -> list[Path]:
        """Four `uscl` files with two hourly records each, in chronological order."""
        names_and_hours = [
            ("roms_uscl.20240101000000.nc", [0, 1]),
            ("roms_uscl.20240101020000.nc", [2, 3]),
            ("roms_uscl.20240101040000.nc", [4, 5]),
            ("roms_uscl.20240101060000.nc", [6, 7]),
        ]
        # Create the files out of chronological order, as a directory listing may
        # return them, so the on-disk order differs from the time order too.
        written = {
            name: _write_uscl_file(tmp_path / name, hours)
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
        """
        upscaler = CDRUpscaler([uscl_files[i] for i in order])
        upscaler.create_cdr_dataset()
        upscaler.populate_cdr_dataset()

        assert upscaler.cdr_dataset is not None
        cdr_time = upscaler.cdr_dataset["cdr_time"].values
        assert np.all(np.diff(cdr_time) > 0)
        assert cdr_time[0] == 0.0
        np.testing.assert_allclose(cdr_time, np.arange(8) * HOUR_S / 86400.0)

        # Each record's flux and layer thickness were written equal to its hour.
        expected = np.arange(8, dtype=float)
        flux = upscaler.cdr_dataset["cdr_trcflx_profile"].values
        np.testing.assert_array_equal(flux[:, 0, 0, 0], expected)
        np.testing.assert_array_equal(flux[:, 0, 1, -1], expected)
        thickness = upscaler.cdr_dataset["cdr_layer_thickness"].values
        np.testing.assert_array_equal(thickness[:, 0, 0], expected)

    def test_files_are_stored_in_time_order(self, uscl_files: list[Path]) -> None:
        """Verify `CDRUpscaler.files` is time-sorted, and the input list is untouched."""
        shuffled: list[Path | str] = [uscl_files[i] for i in (2, 0, 3, 1)]
        upscaler = CDRUpscaler(shuffled)

        assert upscaler.files == uscl_files
        assert shuffled == [uscl_files[i] for i in (2, 0, 3, 1)]

    def test_overlapping_records_raise(self, tmp_path: Path) -> None:
        """Verify a record repeated across files is reported, not silently kept."""
        first = _write_uscl_file(tmp_path / "roms_uscl.20240101000000.nc", [0, 1, 2])
        second = _write_uscl_file(tmp_path / "roms_uscl.20240101020000.nc", [2, 3, 4])

        with pytest.raises(ValueError, match="strictly increasing"):
            CDRUpscaler([first, second])
