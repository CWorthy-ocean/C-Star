"""Tier 1: CDR forcing against the real grid and the run's reference date.

Generates only the inputs each test needs (``only_inputs``), with real roms-tools, on
the ``na-test-8x8`` grid of the ``cdr_lite`` case (whose releases sit at 60N 5W).
"""

import copy
import shutil
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import xarray as xr

from cstar.applications.forge.engine import process_forge_blueprint
from cstar.applications.forge.host import HostPaths
from cstar.applications.forge.namelist_model import check_cdr_lite_mode_roms
from cstar.applications.forge.resolve import build_forge_blueprint
from cstar.tests.integration_tests.cases import (
    CDR_LITE_FORCING,
    DT,
    FORGE_CASES,
    MODEL_REFERENCE_DATE,
    MODEL_SPEC,
    ROMS_REF,
    RUN_END,
    RUN_START,
)

if TYPE_CHECKING:
    from cstar.applications.forge.blueprint import ForgeBlueprint
    from cstar.catalog.domain_catalog import LayeredCatalog

pytestmark = pytest.mark.skipif(
    shutil.which("nccopy") is None,
    reason="nccopy is required to convert use_pio inputs to CDF-5",
)

CASE = FORGE_CASES["cdr_lite"]
SECONDS_PER_DAY = 86400.0


@pytest.fixture(autouse=True)
def _cdr_lite_supported() -> None:
    try:
        check_cdr_lite_mode_roms(ROMS_REF)
    except ValueError as exc:
        pytest.skip(str(exc))


def _blueprint(
    catalog: "LayeredCatalog",
    cdr_forcing: dict[str, Any],
    model_reference_date: datetime = MODEL_REFERENCE_DATE,
) -> "ForgeBlueprint":
    """The ``cdr_lite`` case with ``cdr_forcing`` and ``model_reference_date``."""
    domain = catalog.domain_data(CASE.domain)
    return build_forge_blueprint(
        model_path=catalog.model_path(MODEL_SPEC),
        grid_name=domain["grid_name"],
        grid_kwargs=domain["grid_kwargs"],
        open_boundaries=domain["open_boundaries"],
        partitioning=domain["partitioning"],
        start_date=RUN_START,
        end_date=RUN_END,
        model_reference_date=model_reference_date,
        dt=DT,
        forcing_inputs=catalog.forcing_data(CASE.forcing),
        output_settings=catalog.output_data("test-minimal"),
        name="it-cdr",
        run_time_overrides=CASE.run_time_overrides,
        bgc_mode=CASE.bgc_mode,
        cdr={"mode": "simple", "cdr_forcing": cdr_forcing},
        roms_ref=ROMS_REF,
    )


def _generate(cfg: "ForgeBlueprint", working_dir: Path, *inputs: str) -> Path:
    """Generate ``inputs`` for ``cfg``; return the input data directory."""
    host = HostPaths(
        working_dir=working_dir,
        source_data_cache=working_dir / "source_cache",
        system="test",
    )
    process_forge_blueprint(cfg, host=host, use_dask=False, only_inputs=list(inputs))
    return working_dir / "input_data"


def _moved(**position: float) -> dict[str, Any]:
    """``CDR_LITE_FORCING`` with every release moved to ``position``."""
    forcing = copy.deepcopy(CDR_LITE_FORCING)
    for release in forcing["releases"]:
        release.update(position)
    return forcing


class TestCdrReleasePositions:
    """roms-tools checks each release against the grid Forge hands it."""

    def test_the_case_releases_generate(
        self, test_catalog: "LayeredCatalog", tmp_path: Path
    ) -> None:
        input_data = _generate(
            _blueprint(test_catalog, CDR_LITE_FORCING), tmp_path, "cdr_forcing"
        )
        assert list(input_data.glob("*_cdr.nc"))

    @pytest.mark.parametrize(
        ("position", "message"),
        [
            ({"lat": 0.0, "lon": 100.0}, "outside of the grid domain"),
            ({"depth": 9000.0}, "below the seafloor"),
        ],
        ids=["outside-domain", "below-seafloor"],
    )
    def test_a_release_off_the_grid_fails_in_generation(
        self,
        test_catalog: "LayeredCatalog",
        tmp_path: Path,
        position: dict[str, float],
        message: str,
    ) -> None:
        cfg = _blueprint(test_catalog, _moved(**position))
        with pytest.raises(ValueError, match=message):
            _generate(cfg, tmp_path, "cdr_forcing")


class TestCdrReferenceDate:
    """``cdr_time`` is counted from the run's reference date, as the initial
    conditions' ``ocean_time`` is.
    """

    def test_cdr_time_shares_the_initial_conditions_epoch(
        self, test_catalog: "LayeredCatalog", tmp_path: Path
    ) -> None:
        reference = datetime(2010, 1, 1)
        cfg = _blueprint(test_catalog, CDR_LITE_FORCING, model_reference_date=reference)

        input_data = _generate(cfg, tmp_path, "initial_conditions", "cdr_forcing")

        start_days = (RUN_START - reference).total_seconds() / SECONDS_PER_DAY
        (ic_file,) = input_data.glob("*_initial_conditions*.nc")
        with xr.open_dataset(ic_file, decode_times=False) as ic:
            ocean_time = float(ic["ocean_time"].values.ravel()[0])
        (cdr_file,) = input_data.glob("*_cdr.nc")
        with xr.open_dataset(cdr_file, decode_times=False) as cdr:
            cdr_time = cdr["cdr_time"].values.tolist()
        assert ocean_time == pytest.approx(start_days * SECONDS_PER_DAY)
        assert cdr_time[0] == pytest.approx(start_days)
