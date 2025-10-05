import logging
import pathlib
from collections.abc import Callable, Generator
from pathlib import Path
from unittest import mock

import pytest

from cstar.base import AdditionalCode, Discretization, ExternalCodeBase, InputDataset
from cstar.base.log import get_logger
from cstar.io.constants import (
    SourceClassification,
)
from cstar.io.source_data import SourceData, _SourceInspector
from cstar.io.staged_data import StagedData
from cstar.io.stager import Stager
from cstar.marbl import MARBLExternalCodeBase
from cstar.tests.unit_tests.fake_abc_subclasses import (
    FakeExternalCodeBase,
    FakeInputDataset,
    StubSimulation,
)

################################################################################
# SourceData
################################################################################


class MockStager(Stager):
    """Mock subclass of Stager to skip any staging and retrieval logic"""

    def stage(self, target_dir: Path, source: "SourceData"):
        return MockStagedData(source=source, path=target_dir)

    @property
    def retriever(self):
        return None


class MockStagedData(StagedData):
    """Mock subclass of StagedData to skip any filesystem and network logic.

    Can be initialized with 'mock_changed_from_source' param to set the value of
    the 'changed_from_source' property
    """

    def __init__(
        self, source: "SourceData", path: "Path", mock_changed_from_source: bool = False
    ):
        super().__init__(source, path)
        self._mock_changed_from_source = mock_changed_from_source

    @property
    def changed_from_source(self) -> bool:
        return self._mock_changed_from_source

    def unstage(self):
        pass

    def reset(self):
        pass


class MockSourceInspector(_SourceInspector):
    """Mock subclass of _SourceInspector to skip any classification logic.

    Tests can initialize with 'classification' parameter to set the desired classification manually.
    """

    def __init__(
        self, location: str | Path, classification: SourceClassification | None = None
    ):
        self._location = str(location)
        # Specifically for this mock, user chooses classification
        self._source_type = classification.value.source_type if classification else None
        self._location_type = (
            classification.value.location_type if classification else None
        )
        self._file_encoding = (
            classification.value.file_encoding if classification else None
        )


class MockSourceData(SourceData):
    """Mock subclass of SourceData to skip any filesystem or network logic.

    Tests can provide 'classification' parameter to set desired classification manually.
    """

    def __init__(
        self,
        location: str | Path,
        identifier: str | None = None,
        # Specifically for this mock, user chooses classification
        classification: SourceClassification = SourceClassification.LOCAL_TEXT_FILE,
    ):
        self._location = str(location)
        self._identifier = identifier

        self._classification = classification

        self._stager = MockStager()


@pytest.fixture
def mock_source_data_factory() -> Callable[
    [SourceClassification, str | Path, str | None], MockSourceData
]:
    """
    Fixture that returns a MockSourceData instance with chosen attributes

    Parameters
    ----------
    classification (SourceClassification):
        The desired classification to be set manually, avoiding complex classification logic
    location (str or Path):
        The desired location attribute associated with the mock data source
    identifier (str, default None)
        The desired identifier attribute associated with the mock data source

    Returns
    -------
    MockSourceData
        A populated SourceData subclass with the specified characteristics
    """

    def factory(
        classification: SourceClassification,
        location: str | Path,
        identifier: str | None = None,
    ) -> MockSourceData:
        instance = MockSourceData(
            classification=classification,
            location=location,
            identifier=identifier,
        )
        return instance

    return factory


@pytest.fixture
def mock_sourcedata_remote_repo() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with remote repository-like characteristics"""

    def _create(location="https://github.com/test/repo.git", identifier="test_target"):
        return MockSourceData(
            classification=SourceClassification.REMOTE_REPOSITORY,
            location=location,
            identifier=identifier,
        )

    return _create


@pytest.fixture
def mock_sourcedata_local_file() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with remote repository-like characteristics"""

    def _create(
        location="some/local/source/path/local_file.nc", identifier="test_target"
    ):
        return MockSourceData(
            classification=SourceClassification.LOCAL_BINARY_FILE,
            location=location,
            identifier=identifier,
        )

    return _create


@pytest.fixture
def mock_sourcedata_remote_file() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with remote repository-like characteristics"""

    def _create(location="http://example.com/remote_file.nc", identifier="abc123"):
        return MockSourceData(
            classification=SourceClassification.REMOTE_BINARY_FILE,
            location=location,
            identifier=identifier,
        )

    return _create


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
    mock_sourcedata_remote_repo,
) -> Generator[ExternalCodeBase, None, None]:
    """Pytest fixutre that provides an instance of the ExternalCodeBase class
    with a mocked SourceData instance.
    """
    source = mock_sourcedata_remote_repo()
    patch_source_data = mock.patch(
        "cstar.base.external_codebase.SourceData", return_value=source
    )

    with patch_source_data:
        fecb = FakeExternalCodeBase()
        fecb._source = source
        yield fecb


@pytest.fixture
def fake_marblexternalcodebase(
    mock_sourcedata_remote_repo,
) -> Generator[MARBLExternalCodeBase, None, None]:
    """Fixture providing a `MARBLExternalCodeBase` instance for testing.

    Patches `SourceData` calls to avoid network and filesystem interaction.
    """
    source_data = mock_sourcedata_remote_repo(
        location="https://marbl.com/repo.git", identifier="v1"
    )
    patch_source_data = mock.patch(
        "cstar.base.external_codebase.SourceData", return_value=source_data
    )

    with patch_source_data:
        yield MARBLExternalCodeBase()


################################################################################
# InputDataset
################################################################################


@pytest.fixture
def fake_inputdataset_local(
    mock_sourcedata_local_file,
) -> Generator[InputDataset, None, None]:
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
    fake_location = "some/local/source/path/local_file.nc"
    source_data = mock_sourcedata_local_file(location=fake_location)
    patch_source_data = mock.patch(
        "cstar.base.input_dataset.SourceData", return_value=source_data
    )
    with patch_source_data:
        dataset = FakeInputDataset(
            location=fake_location,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def fake_inputdataset_remote(
    mock_sourcedata_remote_file,
) -> Generator[InputDataset, None, None]:
    """Fixture to provide a mock remote InputDataset instance.

    This fixture patches properties of the DataSource class to simulate a remote dataset,
    initializing it with attributes such as URL location, file hash, and date range.

    Yields
    ------
    FakeInputDataset: Instance representing a remote input dataset for testing.
    """
    # Using context managers to patch properties on DataSource
    fake_location = "http://example.com/remote_file.nc"
    fake_hash = "abc123"
    source_data = mock_sourcedata_remote_file(
        location=fake_location,
        identifier=fake_hash,
    )
    patch_source_data = mock.patch(
        "cstar.base.input_dataset.SourceData", return_value=source_data
    )
    with patch_source_data:
        # Create the InputDataset instance; it will use the mocked DataSource
        dataset = FakeInputDataset(
            location=fake_location,
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
        codebase=fake_externalcodebase,
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
