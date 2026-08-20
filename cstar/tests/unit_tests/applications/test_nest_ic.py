import logging
import subprocess
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from cstar.applications.core import RunnerRequest
from cstar.applications.nest_ic import NestIcBlueprint, NestIcRunner
from cstar.entrypoint.config import JobConfig, ServiceConfiguration


@pytest.fixture
def blueprint_kwargs(tmp_path: Path) -> dict[str, Any]:
    """Minimal valid keyword arguments for constructing a `NestIcBlueprint`.

    Paths need not exist on disk; the blueprint model itself performs no
    existence checks on `parent_rst`, `parent_grid`, or `child_grid`.
    """
    return {
        "name": "test-nest-ic",
        "description": "A test nest_ic blueprint.",
        "working_dir": tmp_path,
        "parent_rst": tmp_path / "parent_rst.nc",
        "parent_grid": tmp_path / "parent_grid.nc",
        "child_grid": tmp_path / "child_grid.nc",
    }


def _make_runner(blueprint: NestIcBlueprint) -> NestIcRunner:
    """Construct a `NestIcRunner` wired to *blueprint* without touching disk.

    `RunnerRequest.blueprint` normally deserializes from a URI on first
    access; setting the private `_bp` cache directly lets the request return
    an in-memory blueprint instance instead.

    Parameters
    ----------
    blueprint : NestIcBlueprint
        The blueprint the runner should operate on.

    Returns
    -------
    NestIcRunner
    """
    request: RunnerRequest[NestIcBlueprint] = RunnerRequest(
        "unused://blueprint", NestIcBlueprint
    )
    request._bp = blueprint

    service_config = ServiceConfiguration(
        as_service=False,
        loop_delay=0,
        health_check_frequency=None,
        log_level=logging.DEBUG,
        health_check_log_threshold=10,
        name="test_nest_ic_runner",
    )
    job_config = JobConfig(account_id="", walltime="", priority="")

    return NestIcRunner(request, service_config, job_config)


class TestNestIcBlueprintPio:
    """Tests for the `pio` field on `NestIcBlueprint`."""

    def test_pio_defaults_true(self, blueprint_kwargs: dict[str, Any]) -> None:
        """Verify that `pio` defaults to True when omitted."""
        bp = NestIcBlueprint(**blueprint_kwargs)
        assert bp.pio is True

    def test_pio_false_validates(self, blueprint_kwargs: dict[str, Any]) -> None:
        """Verify that `pio=False` is accepted."""
        bp = NestIcBlueprint(**blueprint_kwargs, pio=False)
        assert bp.pio is False

    def test_pio_defaults_true_via_model_validate(
        self, blueprint_kwargs: dict[str, Any]
    ) -> None:
        """Verify that `pio` defaults to True when validated from a dict lacking
        the key, as would happen deserializing a blueprint YAML without it.
        """
        data = {
            k: str(v) if isinstance(v, Path) else v for k, v in blueprint_kwargs.items()
        }
        bp = NestIcBlueprint.model_validate(data)
        assert bp.pio is True


class TestConvertToCdf5:
    """Tests for `NestIcRunner._convert_to_cdf5`."""

    def test_success_invokes_nccopy_and_removes_source(self, tmp_path: Path) -> None:
        """Verify the exact `nccopy` command is invoked and the nc4 source file
        is removed once the conversion succeeds.
        """
        nc4_path = tmp_path / "ic_nc4.nc"
        final_path = tmp_path / "ic.nc"
        nc4_path.write_bytes(b"fake netcdf4 content")

        with mock.patch("cstar.applications.nest_ic.subprocess.run") as mock_run:
            NestIcRunner._convert_to_cdf5(nc4_path, final_path)

        mock_run.assert_called_once_with(
            ["nccopy", "-k", "cdf5", str(nc4_path), str(final_path)],
            check=True,
        )
        assert not nc4_path.exists()

    def test_failure_propagates_and_keeps_source(self, tmp_path: Path) -> None:
        """Verify that a `nccopy` failure propagates as `CalledProcessError`
        and leaves the nc4 source file untouched.
        """
        nc4_path = tmp_path / "ic_nc4.nc"
        final_path = tmp_path / "ic.nc"
        nc4_path.write_bytes(b"fake netcdf4 content")

        with mock.patch(
            "cstar.applications.nest_ic.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "nccopy"),
        ):
            with pytest.raises(subprocess.CalledProcessError):
                NestIcRunner._convert_to_cdf5(nc4_path, final_path)

        assert nc4_path.exists()
        assert not final_path.exists()


class TestCreateInitialConditionsRouting:
    """Tests for the PIO-conditional save/convert routing in
    `NestIcRunner._create_initial_conditions`.
    """

    @pytest.fixture(autouse=True)
    def mock_ic(self, monkeypatch: pytest.MonkeyPatch) -> mock.Mock:
        """Replace the lazily-imported `roms_tools` module attribute with a
        mock, so no real netCDF/dask work happens, and return the mocked
        `InitialConditions` instance for assertions.
        """
        mock_roms_tools = mock.Mock()
        mock_ic = mock.Mock()
        mock_roms_tools.InitialConditions.return_value = mock_ic
        monkeypatch.setattr("cstar.applications.nest_ic.roms_tools", mock_roms_tools)
        return mock_ic

    @pytest.fixture(autouse=True)
    def _mock_restart_file(self, monkeypatch: pytest.MonkeyPatch) -> mock.Mock:
        """Replace `RestartFile` with a mock exposing a fixed timestamp, so no
        real file needs to exist at `parent_rst`.
        """
        mock_rst = mock.Mock()
        mock_rst.timestamp = "2024-01-01T00:00:00"
        mock_rst.formatted_timestamp = "20240101000000"
        monkeypatch.setattr(
            "cstar.applications.nest_ic.RestartFile",
            mock.Mock(return_value=mock_rst),
        )
        return mock_rst

    @pytest.fixture(autouse=True)
    def _mock_has_bgc(self) -> Generator[mock.Mock, None, None]:
        """Stub `_has_bgc` so it never inspects a (nonexistent) netCDF file."""
        with mock.patch.object(NestIcRunner, "_has_bgc", return_value=False) as mocked:
            yield mocked

    def _expected_final_path(
        self, blueprint: NestIcBlueprint, formatted_timestamp: str
    ) -> Path:
        fname = f"ic_from_parent_rst.{formatted_timestamp}.nc"
        return Path(blueprint.working_dir).expanduser() / "output" / fname

    def test_pio_true_saves_to_mangled_path_and_converts(
        self,
        blueprint_kwargs: dict[str, Any],
        mock_ic: mock.Mock,
    ) -> None:
        """Verify that with `pio=True`, the `InitialConditions` is saved to the
        `_nc4`-mangled path, `_convert_to_cdf5` is invoked with the mangled and
        final paths, and the unmangled final path is returned.
        """
        bp = NestIcBlueprint(**blueprint_kwargs, pio=True)
        runner = _make_runner(bp)

        final_path = self._expected_final_path(bp, "20240101000000")
        # Hardcoded (not derived via the production mangling expression) so a
        # botched change to the mangling scheme fails this assertion.
        nc4_path = final_path.with_name("ic_from_parent_rst.20240101000000_nc4.nc")

        with mock.patch.object(NestIcRunner, "_convert_to_cdf5") as mock_convert:
            result = runner._create_initial_conditions()

        mock_ic.save.assert_called_once_with(nc4_path)
        mock_convert.assert_called_once_with(nc4_path, final_path)
        assert result == final_path

    def test_pio_false_saves_directly_without_conversion(
        self,
        blueprint_kwargs: dict[str, Any],
        mock_ic: mock.Mock,
    ) -> None:
        """Verify that with `pio=False`, the `InitialConditions` is saved
        directly to the final path and no conversion is attempted.
        """
        bp = NestIcBlueprint(**blueprint_kwargs, pio=False)
        runner = _make_runner(bp)

        final_path = self._expected_final_path(bp, "20240101000000")

        with mock.patch.object(NestIcRunner, "_convert_to_cdf5") as mock_convert:
            result = runner._create_initial_conditions()

        mock_ic.save.assert_called_once_with(final_path)
        mock_convert.assert_not_called()
        assert result == final_path
