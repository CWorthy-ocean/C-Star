import logging
import os
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
from cstar.io.retriever import Retriever
from cstar.io.source_data import SourceData, SourceDataCollection, _SourceInspector
from cstar.io.staged_data import StagedData, StagedDataCollection, StagedFile
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

    @property
    def retriever(self):
        if not hasattr(self, "_retriever"):
            self._retriever = mock.Mock(spec=Retriever)
        return self._retriever

    def stage(self, target_dir: Path, source: "SourceData"):
        return MockStagedData(source=source, path=target_dir / source.basename)


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
    """Fixture to create a MockSourceData instance with local-path-like characteristics"""

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
def mock_sourcedata_local_text_file() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with local-textfile-like characteristics"""

    def _create(
        location="some/local/source/path/local_file.yaml", identifier="test_target"
    ):
        return MockSourceData(
            classification=SourceClassification.LOCAL_TEXT_FILE,
            location=location,
            identifier=identifier,
        )

    return _create


@pytest.fixture
def mock_sourcedata_remote_file() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with remote-binary-file-like characteristics"""

    def _create(location="http://example.com/remote_file.nc", identifier="abc123"):
        return MockSourceData(
            classification=SourceClassification.REMOTE_BINARY_FILE,
            location=location,
            identifier=identifier,
        )

    return _create


@pytest.fixture
def mock_sourcedata_remote_text_file() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with remote textfile--like characteristics"""

    def _create(location="http://example.com/remote_file.yaml", identifier="abc123"):
        return MockSourceData(
            classification=SourceClassification.REMOTE_TEXT_FILE,
            location=location,
            identifier=identifier,
        )

    return _create


@pytest.fixture
def mock_sourcedatacollection_remote_files() -> Callable[
    [str, str], SourceDataCollection
]:
    """Fixture to create a MockSourceDataCollection instance with characteristics of remote binary files."""
    location = "http://example.com/"

    def _create(
        locations=[location + "remote_file_0.nc", location + "remote_file_1.nc"],
        identifiers=["test_target0", "test_target1"],
    ):
        source_data_instances = []
        for i in range(len(locations)):
            identifier = identifiers[i] if identifiers else None
            source_data_instances.append(
                MockSourceData(
                    classification=SourceClassification.REMOTE_BINARY_FILE,
                    location=locations[i],
                    identifier=identifier,
                )
            )
        return SourceDataCollection(sources=source_data_instances)

    return _create


################################################################################
# StagedData
################################################################################
@pytest.fixture
def fake_stagedfile_remote_source(
    mock_sourcedata_remote_file,
) -> Generator[Callable[[Path, "MockSourceData", bool], "StagedFile"], None, None]:
    """Yield a factory that builds a StagedFile with a per-instance patched property."""
    patchers: list[mock._patch] = []
    local_dir = Path("some/local/dir")
    default_source = mock_sourcedata_remote_file()
    default_path = local_dir / default_source.basename

    def _create(
        path: Path = default_path,
        source: "MockSourceData" = default_source,
        changed_from_source: bool = False,
    ) -> "StagedFile":
        fake_stat: os.stat_result = mock.Mock(spec=os.stat_result)
        sf: StagedFile = StagedFile(
            source=source, path=Path(path), sha256=source.identifier, stat=fake_stat
        )

        patcher: mock._patch = mock.patch.object(
            type(sf), "changed_from_source", new_callable=mock.PropertyMock
        )
        prop: mock.PropertyMock = patcher.start()
        prop.return_value = changed_from_source
        patchers.append(patcher)

        return sf

    yield _create  # generator fixture: pytest handles teardown afterward

    for p in reversed(patchers):
        p.stop()


@pytest.fixture
def fake_stageddatacollection_remote_files(
    mock_sourcedatacollection_remote_files, fake_stagedfile_remote_source
) -> Callable[[str, str], StagedDataCollection]:
    """Fixture to create a MockSourceDataCollection instance with characteristics of local binary files"""
    local_dir = Path("some/local/dir")
    default_sources = mock_sourcedatacollection_remote_files()
    default_paths = [local_dir / f.basename for f in default_sources.sources]

    def _create(paths=default_paths, sources=default_sources):
        staged_data_instances = []
        for i in range(len(paths)):
            source = sources[i] if sources else None
            staged_data_instances.append(
                fake_stagedfile_remote_source(path=paths[i], source=source)
            )
        return StagedDataCollection(items=staged_data_instances)

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
