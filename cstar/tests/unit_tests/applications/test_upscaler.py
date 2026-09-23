import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from cstar.applications.core import RunnerRequest
from cstar.applications.upscaler import UpscalerBlueprint, UpscalerRunner
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
        return Path(blueprint.working_dir) / "output" / "upscaled_cdr.nc"

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
