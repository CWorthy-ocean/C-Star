import logging
import shutil
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest import mock

import netCDF4
import numpy as np
import pytest
import xarray as xr

from cstar.applications.core import RunnerRequest
from cstar.applications.upscaler import (
    CDRUpscaler,
    UpscalerBlueprint,
    UpscalerRunner,
)
from cstar.base.utils import convert_to_cdf5
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
                out["cdr_trcflx_profile"][:, :, 0, :], _profiles(src, "ALK_add")
            )
            np.testing.assert_array_equal(
                out["cdr_trcflx_profile"][:, :, 1, :], _profiles(src, "DIC_add")
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
                    _profiles(src, f"{name}_add"),
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
