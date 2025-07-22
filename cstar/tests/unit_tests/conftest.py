import functools
import logging
from collections.abc import Callable, Generator
from pathlib import Path
from unittest.mock import patch

import dotenv
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
    return get_logger("cstar.tests.unit_tests")


@pytest.fixture
def marbl_path(tmp_path: Path) -> Path:
    # A path to a temporary directory for writing the marbl code
    return tmp_path / "marbl"


@pytest.fixture
def roms_path(tmp_path: Path) -> Path:
    # A path to a temporary directory for writing the roms code
    return tmp_path / "roms"


@pytest.fixture
def system_dotenv_dir(tmp_path: Path) -> Path:
    # A path to a temporary directory for writing system-level
    # environment configuration file
    return tmp_path / "additional_files" / "env_files"


@pytest.fixture
def mock_system_name() -> str:
    # A name for the mock system/platform executing the tests.
    return "mock_system"


@pytest.fixture
def mock_user_env_name() -> str:
    """Return a unique name for a temporary user .env config file.

    Returns
    -------
    str
        The name of the .env file
    """
    return ".mock.env"


@pytest.fixture
def system_dotenv_path(system_dotenv_dir: Path, mock_system_name: str) -> Path:
    # A path to a temporary, system-level environment configuration file
    if not system_dotenv_dir.exists():
        system_dotenv_dir.mkdir(parents=True)

    return system_dotenv_dir / f"{mock_system_name}.env"


@pytest.fixture
def mock_path_resolve():
    """Fixture to mock Path.resolve() so it returns the calling Path."""

    def fake_resolve(self: Path) -> Path:
        return self

    with patch.object(Path, "resolve", side_effect=fake_resolve, autospec=True) as mock:
        yield mock


@pytest.fixture
def dotenv_path(tmp_path: Path, mock_user_env_name: str) -> Path:
    """Return a complete path to a temporary user .env file.

    Parameters
    ----------
    tmp_path : Path
        The path to a temporary location to write the env file
    mock_user_env_name : str
        The name of the file that will be written

    Returns
    -------
    Path
        The complete path to the config file
    """
    return tmp_path / mock_user_env_name


def _write_custom_env(path: Path, variables: dict[str, str]) -> None:
    """Populate a .env configuration file.

    NOTE: repeated calls will update the file

    Parameters
    ----------
    path: Path
        The complete file path to write to
    variables : dict[str, str]
        The key-value pairs to be written to the env file
    """
    if not path.parent.exists():
        path.parent.mkdir(parents=True)

    for k, v in variables.items():
        dotenv.set_key(path, k, v)


@pytest.fixture
def custom_system_env(
    system_dotenv_path: Path,
) -> Callable[[dict[str, str]], None]:
    """Return a function to populate a mocked system environment config file.

    Parameters
    ----------
    system_dotenv_path: Path
        The path to a temporary location to write the env file

    Returns
    -------
    Callable[[dict[str, str]], None]
        A function that will write a new env config file.
    """
    return functools.partial(_write_custom_env, system_dotenv_path)


@pytest.fixture
def custom_user_env(
    dotenv_path: Path,
) -> Callable[[dict[str, str]], None]:
    """Return a function to populate a mocked user environment config file.

    Parameters
    ----------
    dotenv_path: Path
        The path to a temporary location to write the env file

    Returns
    -------
    Callable[[dict[str, str]], None]
        A function that will write a new env config file.
    """
    return functools.partial(_write_custom_env, dotenv_path)


@pytest.fixture
def mock_lmod_filename() -> str:
    """Return a complete path to an empty, temporary .lmod config file for tests.

    Returns
    -------
    str
        The filename
    """
    return "mock.lmod"


@pytest.fixture
def mock_lmod_path(tmp_path: Path, mock_lmod_filename: str) -> Path:
    """Create an empty, temporary .lmod file and return the path.

    Parameters
    ----------
    tmp_path : Path
        The path to a temporary location to write the lmod file
    mock_lmod_filename : str
        The filename to use for the .lmod file

    Returns
    -------
    str
        The complete path to the file
    """
    tmp_path.mkdir(parents=True, exist_ok=True)

    path = tmp_path / mock_lmod_filename
    path.touch()  # CStarEnvironment expects the file to exist & opens it
    return path


@pytest.fixture
def example_roms_simulation(
    tmp_path: Path,
) -> Generator[tuple[ROMSSimulation, Path], None, None]:
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
                "file2.in",
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

    yield sim, directory  # Ensures pytest can handle resource cleanup if needed
