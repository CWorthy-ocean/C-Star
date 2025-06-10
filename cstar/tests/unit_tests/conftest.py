import logging
import pathlib

import pytest

from cstar.base.additional_code import AdditionalCode
from cstar.base.log import get_logger
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.roms.input_dataset import (
    ROMSBoundaryForcing,
    ROMSForcingCorrections,
    ROMSInitialConditions,
    ROMSModelGrid,
    ROMSRiverForcing,
    ROMSSurfaceForcing,
    ROMSTidalForcing,
)
from cstar.roms.simulation import ROMSSimulation


@pytest.fixture
def log() -> logging.Logger:
    """Return a logger instance for unit tests."""
    return get_logger("cstar.tests.unit_tests")


@pytest.fixture
def dotenv_path(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a path to a temp user environment configuration file."""
    return tmp_path / ".cstar.env"


@pytest.fixture
def marbl_path(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a path to a temp directory for writing the marbl code."""
    return tmp_path / "marbl"


@pytest.fixture
def roms_path(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a path to a temp directory for writing the roms code."""
    return tmp_path / "roms"


@pytest.fixture
def system_dotenv_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a path to a temp system-level environment configuration file."""
    return tmp_path / "additional_files" / "env_files"


@pytest.fixture
def mock_system_name() -> str:
    """Return the name for the mock system/platform executing the tests."""
    return "mock_system"


@pytest.fixture
def system_dotenv_path(
    mock_system_name: str, system_dotenv_dir: pathlib.Path
) -> pathlib.Path:
    """Return a path to a temp user-level environment configuration file."""
    if not system_dotenv_dir.exists():
        system_dotenv_dir.mkdir(parents=True)

    return system_dotenv_dir / f"{mock_system_name}.env"


@pytest.fixture
def example_roms_simulation(
    tmp_path: pathlib.Path,
) -> tuple[ROMSSimulation, pathlib.Path]:
    """Fixture providing a `ROMSSimulation` instance for testing.

    This fixture initializes a `ROMSSimulation` with a comprehensive configuration,
    including discretization settings, mock external ROMS and MARBL codebases.
    runtime and compile-time code, and multiple input datasets (grid, initial
    conditions, tidal forcing, boundary forcing, and surface forcing). The
    temporary directory (`tmp_path`) is used as the working directory.

    Yields
    ------
    tuple[ROMSSimulation, Path]
        A tuple containing:
        - `ROMSSimulation` instance with fully configured attributes.
        - The temporary directory where the simulation is stored.
    """
    directory = tmp_path
    sim = ROMSSimulation(
        name="ROMSTest",
        directory=directory,
        discretization=ROMSDiscretization(time_step=60, n_procs_x=2, n_procs_y=3),
        codebase=ROMSExternalCodeBase(
            source_repo="http://my.code/repo.git", checkout_target="dev"
        ),
        runtime_code=AdditionalCode(
            location=str(directory.parent),
            subdir="subdir/",
            checkout_target="main",
            files=[
                "file1",
                "file2.in_TEMPLATE",
                "marbl_in",
                "marbl_tracer_output_list",
                "marbl_diagnostic_output_list",
            ],
        ),
        compile_time_code=AdditionalCode(
            location=str(directory.parent),
            subdir="subdir/",
            checkout_target="main",
            files=["file1.h", "file2.opt"],
        ),
        start_date="2025-01-01",
        end_date="2025-12-31",
        valid_start_date="2024-01-01",
        valid_end_date="2026-01-01",
        marbl_codebase=MARBLExternalCodeBase(
            source_repo="http://marbl.com/repo.git", checkout_target="v1"
        ),
        model_grid=ROMSModelGrid(location="http://my.files/grid.nc", file_hash="123"),
        initial_conditions=ROMSInitialConditions(
            location="http://my.files/initial.nc", file_hash="234"
        ),
        tidal_forcing=ROMSTidalForcing(
            location="http://my.files/tidal.nc", file_hash="345"
        ),
        river_forcing=ROMSRiverForcing(
            location="http://my.files/river.nc", file_hash="543"
        ),
        boundary_forcing=[
            ROMSBoundaryForcing(
                location="http://my.files/boundary.nc", file_hash="456"
            ),
        ],
        surface_forcing=[
            ROMSSurfaceForcing(location="http://my.files/surface.nc", file_hash="567"),
        ],
        forcing_corrections=[
            ROMSForcingCorrections(
                location="http://my.files/sw_corr.nc", file_hash="890"
            ),
        ],
    )

    return sim, directory  # Ensures pytest can handle resource cleanup if needed
