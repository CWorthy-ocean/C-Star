"""Tier 1: Forge refuses inputs that would not cover the run window (FG-07 / I-S2-3).

The test data has two records per source: GLORYS at 2012-01-01 12:00 and 2012-12-31
12:00, ERA5 at 2012-01-01 00:00 and 2013-01-01 00:00. The ``unified`` case's BGC
sources are climatologies, which ROMS cycles and the check leaves alone. Each test
resolves the ``unified`` case with its own run window and runs
``process_forge_blueprint`` (no ROMS build).
"""

import copy
import shutil
import warnings
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import netCDF4
import pytest
import yaml

from cstar.applications.forge.engine import process_forge_blueprint
from cstar.applications.forge.host import HostPaths
from cstar.applications.forge.resolve import build_forge_blueprint
from cstar.applications.forge.time_coverage import SOURCE_RECORD_ATTR, initial_time
from cstar.tests.integration_tests.cases import (
    DT,
    FORGE_CASES,
    MODEL_REFERENCE_DATE,
    MODEL_SPEC,
    ROMS_REF,
    RUN_START,
)

if TYPE_CHECKING:
    from cstar.catalog.domain_catalog import LayeredCatalog

pytestmark = pytest.mark.skipif(
    shutil.which("nccopy") is None,
    reason="nccopy is required to convert use_pio inputs to CDF-5",
)


def _process(
    catalog: "LayeredCatalog",
    working_dir: Path,
    start: datetime,
    end: datetime,
    *,
    flex_time: bool = False,
) -> HostPaths:
    """Resolve the ``unified`` case over ``start``..``end`` and generate its inputs."""
    case = FORGE_CASES["unified"]
    domain = catalog.domain_data(case.domain)
    forcing = copy.deepcopy(catalog.forcing_data(case.forcing))
    if flex_time:
        forcing["initial_conditions"]["allow_flex_time"] = True
    cfg = build_forge_blueprint(
        model_path=catalog.model_path(MODEL_SPEC),
        grid_name=domain["grid_name"],
        grid_kwargs=domain["grid_kwargs"],
        open_boundaries=domain["open_boundaries"],
        partitioning=domain["partitioning"],
        start_date=start,
        end_date=end,
        model_reference_date=MODEL_REFERENCE_DATE,
        dt=DT,
        forcing_inputs=forcing,
        output_settings=catalog.output_data("test-minimal"),
        name="it-coverage",
        compile_time_overrides=case.compile_time_overrides,
        run_time_overrides=case.run_time_overrides,
        bgc_mode=case.bgc_mode,
        cdr=case.cdr,
        roms_ref=ROMS_REF,
    )
    working_dir.mkdir(parents=True, exist_ok=True)
    cfg.working_dir = working_dir
    host = HostPaths(
        working_dir=working_dir,
        source_data_cache=working_dir / "source_cache",
        system="test",
    )
    process_forge_blueprint(cfg, host=host, use_dask=False)
    return host


@pytest.mark.parametrize("start_hour", [0, 6])
def test_flex_start_before_the_boundary_records_is_refused(
    start_hour: int, tmp_path: Path, test_catalog: "LayeredCatalog"
) -> None:
    """A flexible start at 00:00 or 06:00 takes the 12:00 GLORYS record for the
    initial conditions, but the boundary forcing has nothing at or before the start.
    """
    with pytest.raises(
        ValueError,
        match=r"physics boundary forcing .* the first 'bry_time' record is "
        r"2012-01-01 12:00:00, after the start",
    ):
        _process(
            test_catalog,
            tmp_path,
            datetime(2012, 1, 1, start_hour),
            datetime(2012, 1, 1, 13),
            flex_time=True,
        )


def test_window_past_the_source_records_is_refused(
    tmp_path: Path, test_catalog: "LayeredCatalog"
) -> None:
    """Across New Year the last ERA5 record (01-01 00:00) ends before the run does."""
    with pytest.raises(
        ValueError,
        match=r"physics surface forcing .* the last 'time' record is "
        r"2013-01-01 00:00:00, before the end; the last 'rad_time' record",
    ):
        _process(
            test_catalog,
            tmp_path,
            datetime(2012, 12, 31, 12),
            datetime(2013, 1, 1, 6),
        )


def test_flex_start_uses_the_next_record_as_the_state_at_the_start(
    tmp_path: Path, test_catalog: "LayeredCatalog"
) -> None:
    """With the forcing covering 12-31 06:00 to 07:00, a flexible start takes the
    12:00 GLORYS record and stamps the initial conditions at 06:00, so ROMS starts
    where the blueprint and the forcing do.
    """
    start = datetime(2012, 12, 31, 6)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        host = _process(
            test_catalog, tmp_path, start, datetime(2012, 12, 31, 7), flex_time=True
        )

    assert any("Flexible start time" in str(w.message) for w in caught)
    (ic,) = (host.working_dir / "input_data").glob("*_initial_conditions.nc")
    assert ic.read_bytes()[:4] == b"CDF\x05"  # stamped before the CDF-5 conversion
    assert initial_time(ic, MODEL_REFERENCE_DATE) == start
    with netCDF4.Dataset(ic) as nc:
        assert nc.getncattr(SOURCE_RECORD_ATTR) == "2012-12-31 12:00:00"
    (blueprint,) = (host.working_dir / "blueprints").glob("B_*.yaml")
    runtime = yaml.safe_load(blueprint.read_text())["runtime_params"]
    assert datetime.fromisoformat(runtime["start_date"]) == start


def test_rerun_with_a_longer_window_refuses_the_reused_forcing(
    tmp_path: Path, test_catalog: "LayeredCatalog"
) -> None:
    """Forge reuses same-named files on a rerun, so a window extended past what they
    hold is refused with the regenerate hint instead of reaching ROMS.
    """
    _process(test_catalog, tmp_path, RUN_START, datetime(2012, 1, 1, 13))

    with pytest.raises(ValueError, match=r"before the end.*--clobber"):
        _process(test_catalog, tmp_path, RUN_START, datetime(2013, 1, 1, 6))
