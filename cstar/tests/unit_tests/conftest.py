import functools
import logging
from collections.abc import Callable, Generator
from pathlib import Path
from unittest import mock

import dotenv
import pytest

from cstar.base import AdditionalCode, Discretization, ExternalCodeBase, InputDataset
from cstar.base.datasource import DataSource
from cstar.base.log import get_logger
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.roms.input_dataset import (
    ROMSBoundaryForcing,
    ROMSCdrForcing,
    ROMSForcingCorrections,
    ROMSInitialConditions,
    ROMSModelGrid,
    ROMSRiverForcing,
    ROMSSurfaceForcing,
    ROMSTidalForcing,
)
from cstar.roms.simulation import ROMSSimulation
from cstar.tests.unit_tests.fake_abc_subclasses import (
    FakeExternalCodeBase,
    FakeInputDataset,
    StubSimulation,
)

################################################################################
# AdditionalCode
################################################################################


@pytest.fixture
def fake_additionalcode_remote() -> AdditionalCode:
    """Pytest fixture that provides an instance of the AdditionalCode class representing
    a remote repository.

    This fixture simulates additional code retrieved from a remote Git
    repository. It sets up the following attributes:

    - `location`: The URL of the remote repository
    - `checkout_target`: The specific branch, tag, or commit to checkout
    - `subdir`: A subdirectory within the repository where files are located
    - `files`: A list of files to be included from the repository

    This fixture can be used in tests that involve handling or manipulating code
    fetched from a remote Git repository.

    Returns
    -------
        AdditionalCode: An instance of the AdditionalCode class with preset
        remote repository details.
    """
    return AdditionalCode(
        location="https://github.com/test/repo.git",
        checkout_target="test123",
        subdir="test/subdir",
        files=["test_file_1.F", "test_file_2.py", "test_file_3.opt"],
    )


@pytest.fixture
def fake_additionalcode_local() -> AdditionalCode:
    """Pytest fixture that provides an instance of the AdditionalCode class representing
    code located on the local filesystem.

    This fixture simulates additional code stored in a local directory. It sets
    up the following attributes:

    - `location`: The path to the local directory containing the code
    - `subdir`: A subdirectory within the local directory where the files are located
    - `files`: A list of files to be included from the local directory

    This fixture can be used in tests that involve handling or manipulating
    code that resides on the local filesystem.

    Returns
    --------
        AdditionalCode: An instance of the AdditionalCode class with preset
        local directory details.
    """
    return AdditionalCode(
        location="/some/local/directory",
        subdir="some/subdirectory",
        files=["test_file_1.F", "test_file_2.py", "test_file_3.opt"],
    )


################################################################################
# ExternalCodeBase
################################################################################
@pytest.fixture
def fake_externalcodebase(
    log: logging.Logger,
) -> Generator[ExternalCodeBase, None, None]:
    """Yields a fake codebase (instance of FakeExternalCodeBase) for
    use in testing.
    """
    # Correctly patch the imported _get_hash_from_checkout_target in the ExternalCodeBase's module
    with mock.patch(
        "cstar.base.external_codebase._get_hash_from_checkout_target",
        return_value="test123",
    ):
        yield FakeExternalCodeBase()


@pytest.fixture
def fake_marblexternalcodebase(
    log: logging.Logger,
) -> Generator[MARBLExternalCodeBase, None, None]:
    """Fixture providing a `MARBLExternalCodeBase` instance for testing."""
    # Correctly patch the imported _get_hash_from_checkout_target in the ExternalCodeBase's module
    with mock.patch(
        "cstar.base.external_codebase._get_hash_from_checkout_target",
        return_value="test123",
    ):
        yield MARBLExternalCodeBase(
            source_repo="https://marbl.com/repo.git", checkout_target="v1"
        )


################################################################################
# InputDataset
################################################################################


@pytest.fixture
def fake_inputdataset_local() -> Generator[InputDataset, None, None]:
    """Fixture to provide a mock local InputDataset instance.

    This fixture patches properties of the DataSource class to simulate a local dataset,
    initializing it with relevant attributes like location, start date, and end date.

    Mocks
    -----
    - Mocked DataSource.location_type property returning 'path'
    - Mocked DataSource.source_type property returning 'netcdf'
    - Mocked DataSource.basename property returning 'local_file.nc'

    Yields
    ------
    FakeInputDataset: Instance representing a local input dataset for testing.
    """
    with (
        mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        ) as mock_location_type,
        mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type,
        mock.patch.object(
            DataSource, "basename", new_callable=mock.PropertyMock
        ) as mock_basename,
    ):
        mock_location_type.return_value = "path"
        mock_source_type.return_value = "netcdf"
        mock_basename.return_value = "local_file.nc"

        dataset = FakeInputDataset(
            location="some/local/source/path/local_file.nc",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def fake_inputdataset_remote() -> Generator[InputDataset, None, None]:
    """Fixture to provide a mock remote InputDataset instance.

    This fixture patches properties of the DataSource class to simulate a remote dataset,
    initializing it with attributes such as URL location, file hash, and date range.

    Mocks
    -----
    - Mocked DataSource.location_type property returning 'url'
    - Mocked DataSource.source_type property returning 'netcdf'
    - Mocked DataSource.basename property returning 'remote_file.nc'

    Yields
    ------
    FakeInputDataset: Instance representing a remote input dataset for testing.
    """
    # Using context managers to patch properties on DataSource
    with (
        mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        ) as mock_location_type,
        mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type,
        mock.patch.object(
            DataSource, "basename", new_callable=mock.PropertyMock
        ) as mock_basename,
    ):
        # Mock property return values for a remote file (URL)
        mock_location_type.return_value = "url"
        mock_source_type.return_value = "netcdf"
        mock_basename.return_value = "remote_file.nc"

        # Create the InputDataset instance; it will use the mocked DataSource
        dataset = FakeInputDataset(
            location="http://example.com/remote_file.nc",
            file_hash="abc123",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        # Yield the dataset for use in the test
        yield dataset


################################################################################
# Simulation
################################################################################


@pytest.fixture
def stub_simulation(fake_externalcodebase, tmp_path) -> StubSimulation:
    """Fixture providing a `StubSimulation` instance for testing.

    This fixture sets up a minimal `StubSimulation` instance with a mock external
    codebase, runtime and compile-time code, and basic discretization settings.
    The temporary directory (`tmp_path`) serves as the working directory for the
    simulation.

    Yields
    ------
    StubSimulation: instance configured for testing

    """
    sim = StubSimulation(
        name="TestSim",
        directory=tmp_path,
        codebase=FakeExternalCodeBase(),
        runtime_code=AdditionalCode(
            location=tmp_path.parent,
            subdir="subdir/",
            checkout_target="main",
            files=["file1", "file2"],
        ),
        compile_time_code=AdditionalCode(
            location=tmp_path.parent,
            subdir="subdir/",
            checkout_target="main",
            files=["file1", "file2"],
        ),
        discretization=Discretization(time_step=60),
        start_date="2025-01-01",
        end_date="2025-12-31",
        valid_start_date="2024-01-01",
        valid_end_date="2026-01-01",
    )
    return sim


################################################################################
# ROMSSimulation
################################################################################


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

    with mock.patch.object(
        Path, "resolve", side_effect=fake_resolve, autospec=True
    ) as mock_resolve:
        yield mock_resolve


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
        cdr_forcing=ROMSCdrForcing(location="http://my.files/cdr.nc", file_hash="542"),
    )

    yield sim, directory  # Ensures pytest can handle resource cleanup if needed
