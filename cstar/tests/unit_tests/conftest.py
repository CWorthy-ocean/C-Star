import logging
import pathlib
from collections.abc import Generator
from pathlib import Path
from unittest import mock

import numpy as np
import pytest

from cstar.base import AdditionalCode, Discretization
from cstar.base.datasource import DataSource
from cstar.base.log import get_logger
from cstar.marbl import MARBLExternalCodeBase
from cstar.roms import ROMSDiscretization, ROMSExternalCodeBase, ROMSSimulation
from cstar.roms.input_dataset import (
    ROMSBoundaryForcing,
    ROMSForcingCorrections,
    ROMSInitialConditions,
    ROMSModelGrid,
    ROMSRiverForcing,
    ROMSSurfaceForcing,
    ROMSTidalForcing,
)
from cstar.roms.runtime_settings import ROMSRuntimeSettings
from cstar.tests.unit_tests.generic_abc_subclasses import (
    MockExternalCodeBase,
    MockInputDataset,
    MockROMSInputDataset,
    MockSimulation,
)

################################################################################
# AdditionalCode
################################################################################


@pytest.fixture
def remote_additional_code():
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

    Returns:
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
def local_additional_code():
    """Pytest fixture that provides an instance of the AdditionalCode class representing
    code located on the local filesystem.

    This fixture simulates additional code stored in a local directory. It sets
    up the following attributes:

    - `location`: The path to the local directory containing the code
    - `subdir`: A subdirectory within the local directory where the files are located
    - `files`: A list of files to be included from the local directory

    This fixture can be used in tests that involve handling or manipulating
    code that resides on the local filesystem.

    Returns:
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
def generic_codebase(log: logging.Logger):
    """Yields a generic codebase (instance of MockExternalCodeBase defined above) for
    use in testing.
    """
    # Correctly patch the imported _get_hash_from_checkout_target in the ExternalCodeBase's module
    with mock.patch(
        "cstar.base.external_codebase._get_hash_from_checkout_target",
        return_value="test123",
    ):
        yield MockExternalCodeBase()


@pytest.fixture
def marbl_codebase():
    """Fixture providing a configured instance of `MARBLExternalCodeBase` for
    testing.
    """
    source_repo = "https://github.com/marbl-ecosys/MARBL.git"
    checkout_target = "marbl0.45.0"
    return MARBLExternalCodeBase(
        source_repo=source_repo, checkout_target=checkout_target
    )


@pytest.fixture
def roms_codebase():
    """Fixture providing a configured instance of `ROMSExternalCodeBase` for testing."""
    source_repo = "https://github.com/CWorthy-ocean/ucla-roms.git"
    checkout_target = "246c11fa537145ba5868f2256dfb4964aeb09a25"
    return ROMSExternalCodeBase(
        source_repo=source_repo, checkout_target=checkout_target
    )


################################################################################
# ROMSRuntimeSettings
################################################################################
@pytest.fixture
def example_runtime_settings():
    """Fixture providing a `ROMSRuntimeSettings` instance for testing.

    The example instance corresponds to the file `fixtures/example_runtime_settings.in`
    in order to test the `ROMSRuntimeSettings.to_file` and `from_file` methods.

    Paths do not correspond to real files.

    Yields
    ------
    ROMSRuntimeSettings
       The example ROMSRuntimeSettings instance
    """
    yield ROMSRuntimeSettings(
        title="Example runtime settings",
        time_stepping={"ntimes": 360, "dt": 60, "ndtfast": 60, "ninfo": 1},
        bottom_drag={
            "rdrg": 0.0e-4,
            "rdrg2": 1e-3,
            "zob": 1e-2,
            "cdb_min": 1e-4,
            "cdb_max": 1e-2,
        },
        initial={"nrrec": 1, "ininame": Path("input_datasets/roms_ini.nc")},
        forcing={
            "filenames": [
                Path("input_datasets/roms_frc.nc"),
                Path("input_datasets/roms_frc_bgc.nc"),
                Path("input_datasets/roms_bry.nc"),
                Path("input_datasets/roms_bry_bgc.nc"),
            ]
        },
        output_root_name="ROMS_test",
        s_coord={"theta_s": 5.0, "theta_b": 2.0, "tcline": 300.0},
        rho0=1000.0,
        lin_rho_eos={"Tcoef": 0.2, "T0": 1.0, "Scoef": 0.822, "S0": 1.0},
        marbl_biogeochemistry={
            "marbl_namelist_fname": Path("marbl_in"),
            "marbl_tracer_list_fname": Path("marbl_tracer_list_fname"),
            "marbl_diag_list_fname": Path("marbl_diagnostic_output_list"),
        },
        lateral_visc=0.0,
        gamma2=1.0,
        tracer_diff2=[
            0.0,
        ]
        * 38,
        vertical_mixing={"Akv_bak": 0, "Akt_bak": np.zeros(37)},
        my_bak_mixing={"Akq_bak": 1.0e-5, "q2nu2": 0.0, "q2nu4": 0.0},
        sss_correction=7.777,
        sst_correction=10.0,
        ubind=0.1,
        v_sponge=0.0,
        grid=Path("input_datasets/roms_grd.nc"),
        climatology=Path("climfile2.nc"),
    )


################################################################################
# InputDataset
################################################################################


@pytest.fixture
def local_input_dataset():
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
    MockInputDataset: Instance representing a local input dataset for testing.
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

        dataset = MockInputDataset(
            location="some/local/source/path/local_file.nc",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def remote_input_dataset():
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
    MockInputDataset: Instance representing a remote input dataset for testing.
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
        dataset = MockInputDataset(
            location="http://example.com/remote_file.nc",
            file_hash="abc123",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        # Yield the dataset for use in the test
        yield dataset


################################################################################
# ROMSInputDataset
################################################################################


@pytest.fixture
def local_roms_netcdf_dataset():
    """Fixture to provide a ROMSInputDataset with a local NetCDF source.

    Mocks:
    ------
    - DataSource.location_type: Property mocked as 'path'
    - DataSource.source_type: Property mocked as 'netcdf'
    - DataSource.basename: Property mocked as 'local_file.nc'

    Yields:
    -------
        MockROMSInputDataset: A mock dataset pointing to a local NetCDF file.
    """
    with (
        mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        ) as mock_location_type,
        mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type,
    ):
        mock_location_type.return_value = "path"
        mock_source_type.return_value = "netcdf"

        dataset = MockROMSInputDataset(
            location="some/local/source/path/local_file.nc",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def local_roms_yaml_dataset():
    """Fixture to provide a ROMSInputDataset with a local YAML source.

    Mocks:
    ------
    - DataSource.location_type: Property mocked as 'path'
    - DataSource.source_type: Property mocked as 'yaml'
    - DataSource.basename: Property mocked as 'local_file.yaml'

    Yields:
    -------
        MockROMSInputDataset: A mock dataset pointing to a local YAML file.
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
        mock_source_type.return_value = "yaml"
        mock_basename.return_value = "local_file.yaml"

        dataset = MockROMSInputDataset(
            location="some/local/source/path/local_file.yaml",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def remote_roms_yaml_dataset():
    """Fixture to provide a ROMSInputDataset with a remote YAML source.

    Mocks:
    ------
    - DataSource.location_type: Property mocked as 'url'
    - DataSource.source_type: Property mocked as 'yaml'
    - DataSource.basename: Property mocked as 'remote_file.yaml'

    Yields:
    -------
        MockROMSInputDataset: A mock dataset pointing to a local YAML file.
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
        mock_location_type.return_value = "url"
        mock_source_type.return_value = "yaml"
        mock_basename.return_value = "remote_file.yaml"

        dataset = MockROMSInputDataset(
            location="https://dodgyfakeyamlfiles.ru/all/remote_file.yaml",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


################################################################################
# Simulation
################################################################################


@pytest.fixture
def example_simulation(tmp_path):
    """Fixture providing a `MockSimulation` instance for testing.

    This fixture sets up a minimal `MockSimulation` instance with a mock external
    codebase, runtime and compile-time code, and basic discretization settings.
    The temporary directory (`tmp_path`) serves as the working directory for the
    simulation.

    Yields
    ------
    tuple[MockSimulation, Path]
        A tuple containing:
        - `MockSimulation` instance configured for testing.
        - The temporary directory where the simulation is stored.
    """
    directory = tmp_path
    sim = MockSimulation(
        name="TestSim",
        directory=directory,
        codebase=MockExternalCodeBase(),
        runtime_code=AdditionalCode(
            location=directory.parent,
            subdir="subdir/",
            checkout_target="main",
            files=["file1", "file2"],
        ),
        compile_time_code=AdditionalCode(
            location=directory.parent,
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
    yield sim, directory


################################################################################
# ROMSSimulation
################################################################################


@pytest.fixture
def example_roms_simulation(
    tmp_path,
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
            location=directory.parent,
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
            location=directory.parent,
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


################################################################################


@pytest.fixture
def log() -> logging.Logger:
    return get_logger("cstar.tests.unit_tests")


@pytest.fixture
def dotenv_path(tmp_path: pathlib.Path) -> pathlib.Path:
    # A path to a temporary user environment configuration file
    return tmp_path / ".cstar.env"


@pytest.fixture
def marbl_path(tmp_path: pathlib.Path) -> pathlib.Path:
    # A path to a temporary directory for writing the marbl code
    return tmp_path / "marbl"


@pytest.fixture
def roms_path(tmp_path: pathlib.Path) -> pathlib.Path:
    # A path to a temporary directory for writing the roms code
    return tmp_path / "roms"


@pytest.fixture
def system_dotenv_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    # A path to a temporary directory for writing system-level
    # environment configuration file
    return tmp_path / "additional_files" / "env_files"


@pytest.fixture
def mock_system_name() -> str:
    # A name for the mock system/platform executing the tests.
    return "mock_system"


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
def system_dotenv_path(
    mock_system_name: str, system_dotenv_dir: pathlib.Path
) -> pathlib.Path:
    # A path to a temporary, system-level environment configuration file
    if not system_dotenv_dir.exists():
        system_dotenv_dir.mkdir(parents=True)

    return system_dotenv_dir / f"{mock_system_name}.env"
