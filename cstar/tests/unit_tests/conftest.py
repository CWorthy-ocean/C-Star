import logging
import pathlib
from collections.abc import Callable, Generator
from pathlib import Path
from unittest import mock

import pytest

from cstar.base import AdditionalCode, Discretization, ExternalCodeBase, InputDataset
from cstar.base.gitutils import git_location_to_raw
from cstar.base.log import get_logger
from cstar.io.constants import (
    SourceClassification,
)
from cstar.io.retriever import Retriever
from cstar.io.source_data import SourceData, SourceDataCollection, _SourceInspector
from cstar.io.staged_data import (
    StagedData,
    StagedDataCollection,
    StagedFile,
    StagedRepository,
)
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
    """Mock subclass of Stager to skip any staging and retrieval logic.

    `stage` returns a MockStagedData instance
    """

    @property
    def retriever(self):
        if not hasattr(self, "_retriever"):
            self._retriever = mock.Mock(spec=Retriever)
        return self._retriever

    def stage(self, target_dir: Path):
        return MockStagedData(
            source=self.source, path=target_dir / self.source.basename
        )


class MockRetriever(Retriever):
    def read(self, bytes_to_have_read: bytes = b"fake_bytes") -> bytes:
        return bytes_to_have_read

    def _save(self, path_to_have_saved_to) -> Path:
        return path_to_have_saved_to

    # def _save(self, target_dir: Path) -> Path:
    #     data = self.read()
    #     target_path = self.source.basename
    #     with open(target_path, "wb",) as f:
    #         f.write(data)
    #     return target_path


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

        self._stager = MockStager(source=self)
        self._retriever = MockRetriever(source=self)
        # self._retriever = mock.Mock(spec=Retriever)


@pytest.fixture
def mocksourcedata_factory() -> Callable[
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
def mocksourcedata_remote_repo() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with remote repository-like characteristics

    Parameters
    ----------
    location, str, optional:
        The desired location associated with this MockSourceData
    identifier, optional:
        The desired identifier associated with this MockSourceData

    """

    def _create(location="https://github.com/test/repo.git", identifier="test_target"):
        return MockSourceData(
            classification=SourceClassification.REMOTE_REPOSITORY,
            location=location,
            identifier=identifier,
        )

    return _create


@pytest.fixture
def mocksourcedata_local_file() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with local-path-like characteristics

    Parameters
    ----------
    location, str, optional:
        The desired location associated with this MockSourceData
    identifier, optional:
        The desired identifier associated with this MockSourceData
    """

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
def mocksourcedata_local_text_file() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with local-textfile-like characteristics

    Parameters
    ----------
    location, str, optional:
        The desired location associated with this MockSourceData
    identifier, optional:
        The desired identifier associated with this MockSourceData
    """

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
def mocksourcedata_remote_file() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with remote-binary-file-like characteristics

    Parameters
    ----------
    location, str, optional:
        The desired location associated with this MockSourceData
    identifier, optional:
        The desired identifier associated with this MockSourceData
    """

    def _create(location="http://example.com/remote_file.nc", identifier="abc123"):
        return MockSourceData(
            classification=SourceClassification.REMOTE_BINARY_FILE,
            location=location,
            identifier=identifier,
        )

    return _create


@pytest.fixture
def mocksourcedata_remote_text_file() -> Callable[[str, str], MockSourceData]:
    """Fixture to create a MockSourceData instance with remote textfile-like characteristics

    Parameters
    ----------
    location, str, optional:
        The desired location associated with this MockSourceData
    identifier, optional:
        The desired identifier associated with this MockSourceData
    """

    def _create(location="http://example.com/remote_file.yaml", identifier="abc123"):
        return MockSourceData(
            classification=SourceClassification.REMOTE_TEXT_FILE,
            location=location,
            identifier=identifier,
        )

    return _create


@pytest.fixture
def mock_sourcedatacollection() -> Callable[[str, str], SourceDataCollection]:
    """Factory fixture to create a MockSourceDataCollection instance with user-supplied classification.

    Parameters
    ----------
    locations, list[str], optional:
        The desired locations associated with each SourceData in the collection
    identifiers, list[str], optional:
        The desired identifiers associated with each SourceData in the collection
    classification, SourceClassification, optional:
        The desired classification this SourceDataCollection should assume
    """
    default_location = "http://example.com/"
    default_locations = [
        default_location + "remote_file_0.nc",
        default_location + "remote_file_1.nc",
    ]
    default_identifiers = ["test_target0", "test_target1"]
    default_classification = SourceClassification.REMOTE_BINARY_FILE

    def _create(
        locations=default_locations,
        identifiers=default_identifiers,
        classification=default_classification,
    ):
        source_data_instances = []
        for i in range(len(locations)):
            identifier = identifiers[i] if identifiers else None
            source_data_instances.append(
                MockSourceData(
                    classification=classification,
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
def stagedfile_remote_source(
    mocksourcedata_remote_file,
) -> Generator[Callable[[Path, "MockSourceData", bool], "StagedFile"], None, None]:
    """Factory fixture to produce a fake StagedFile from a remote source.

    Parameters
    ----------
    path: Path
        The path where this file is supposedly staged
    source: SourceData
        The SourceData from which this file was supposedly staged
    changed_from_source: bool, optional, default False
        User-specified override to the changed_from_source property
    """
    patchers: list[mock._patch] = []
    local_dir = Path("some/local/dir")
    default_source = mocksourcedata_remote_file()
    default_path = local_dir / default_source.basename

    def _create(
        path: Path = default_path,
        source: "MockSourceData" = default_source,
        changed_from_source: bool = False,
    ) -> "StagedFile":
        sf: StagedFile
        with mock.patch("cstar.io.staged_data.os.stat", return_value=None):
            sf = StagedFile(source=source, path=Path(path), sha256=source.identifier)

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
def stagedrepository(
    mocksourcedata_remote_repo,
) -> Generator[
    Callable[[Path, "MockSourceData", bool], "StagedRepository"], None, None
]:
    """Factory fixture to produce a fake StagedRepository from a remote source.

    Parameters
    ----------
    path: Path
        The path where this repository is supposedly staged
    source: SourceData
        The SourceData from which this repository was supposedly staged
    changed_from_source: bool, optional, default False
        User-specified override to the changed_from_source property
    """
    patchers: list[mock._patch] = []
    default_path_parent = Path("some/local/dir")
    default_source = mocksourcedata_remote_repo()
    default_path = default_path_parent / default_source.basename

    def _create(
        path: Path = default_path,
        source: "MockSourceData" = default_source,
        changed_from_source: bool = False,
    ) -> "StagedRepository":
        sr: StagedRepository
        with mock.patch(
            "cstar.io.staged_data._run_cmd", return_value=source.identifier
        ):
            sr = StagedRepository(source=source, path=path)

        patcher: mock._patch = mock.patch.object(
            type(sr), "changed_from_source", new_callable=mock.PropertyMock
        )
        prop: mock.PropertyMock = patcher.start()
        prop.return_value = changed_from_source
        patchers.append(patcher)

        return sr

    yield _create  # generator fixture: pytest handles teardown afterward

    for p in reversed(patchers):
        p.stop()


@pytest.fixture
def stageddatacollection_remote_files(
    mock_sourcedatacollection, stagedfile_remote_source
) -> Callable[[str, str], StagedDataCollection]:
    """Fixture to create a MockStagedDataCollection instance with characteristics of remote binary files.

    Parameters
    ----------
    paths: list[Path]
        Paths of the locally staged data
    sources: list[SourceData]
        list of SourceData instances supposedly corresponding to `paths`
    """
    local_dir = Path("some/local/dir")
    default_sources = mock_sourcedatacollection()
    default_paths = [local_dir / f.basename for f in default_sources.sources]

    def _create(
        paths=default_paths, sources=default_sources, changed_from_source: bool = False
    ):
        staged_data_instances = []
        for i in range(len(paths)):
            source = sources[i] if sources else None
            staged_data_instances.append(
                stagedfile_remote_source(
                    path=paths[i],
                    source=source,
                    changed_from_source=changed_from_source,
                )
            )
        return StagedDataCollection(items=staged_data_instances)

    return _create


################################################################################
# AdditionalCode
################################################################################


@pytest.fixture
def additionalcode_remote(
    mock_sourcedatacollection,
) -> Callable[[str, str, str, list[str]], AdditionalCode]:
    """Pytest fixture that provides an instance of the AdditionalCode class representing
    a remote repository.
    """
    default_location = "https://github.com/test/repo.git"
    default_checkout_target = "test123"
    default_subdir = "test/subdir"
    default_files = ["test_file_1.F", "test_file_2.py", "test_file_3.opt"]

    def _create(
        location=default_location,
        checkout_target=default_checkout_target,
        subdir=default_subdir,
        files=default_files,
    ):
        sd = mock_sourcedatacollection(
            locations=[
                git_location_to_raw(location, checkout_target, f, subdir) for f in files
            ],
            identifiers=["fake_hash" for i in range(len(files))],
            classification=SourceClassification.REMOTE_TEXT_FILE,
        )
        mock_classify_side_effect = [
            SourceClassification.REMOTE_REPOSITORY,
        ]
        for i in range(len(files)):
            mock_classify_side_effect.append(SourceClassification.REMOTE_TEXT_FILE)
        with mock.patch(
            "cstar.base.additional_code.SourceDataCollection.from_common_location",
            return_value=sd,
        ):
            ac = AdditionalCode(
                location=location,
                checkout_target=checkout_target,
                subdir=subdir,
                files=files,
            )

        return ac

    return _create


@pytest.fixture
def additionalcode_local(
    mock_sourcedatacollection,
) -> Callable[[str, str, list[str]], AdditionalCode]:
    """Pytest fixture that provides an instance of the AdditionalCode class representing
    code located on the local filesystem.
    """
    default_location = "/some/local/directory"
    default_subdir = "some/subdirectory"
    default_files = ["test_file_1.F", "test_file_2.py", "test_file_3.opt"]

    def _create(location=default_location, subdir=default_subdir, files=default_files):
        sd = mock_sourcedatacollection(
            locations=[f"{location}/{subdir}/{f}" for f in files],
            identifiers=["fake_hash" for i in range(len(files))],
            classification=SourceClassification.LOCAL_TEXT_FILE,
        )
        mock_classify_side_effect = [
            SourceClassification.LOCAL_DIRECTORY,
        ]
        for i in range(len(files)):
            mock_classify_side_effect.append(SourceClassification.LOCAL_TEXT_FILE)
        with mock.patch(
            "cstar.base.additional_code.SourceDataCollection.from_common_location",
            return_value=sd,
        ):
            ac = AdditionalCode(location=location, subdir=subdir, files=files)
            return ac

    return _create


################################################################################
# ExternalCodeBase
################################################################################


@pytest.fixture
def fakeexternalcodebase(
    mocksourcedata_remote_repo,
) -> Generator[ExternalCodeBase, None, None]:
    """Pytest fixture that provides an instance of the FakeExternalCodeBase class
    with a mocked SourceData instance.
    """
    source = mocksourcedata_remote_repo()
    patch_source_data = mock.patch(
        "cstar.base.external_codebase.SourceData", return_value=source
    )

    with patch_source_data:
        mecb = FakeExternalCodeBase()
        mecb._source = source
        yield mecb


@pytest.fixture
def fakeexternalcodebase_with_mock_get(
    mocksourcedata_remote_repo,
) -> Generator[ExternalCodeBase, None, None]:
    """Pytest fixutre that provides an instance of the MockExternalCodeBase class
    with a mocked SourceData instance.
    """
    source = mocksourcedata_remote_repo()
    patch_source_data = mock.patch(
        "cstar.base.external_codebase.SourceData", return_value=source
    )

    def mock_get(target_dir: Path | None = None) -> None:
        print(f"mock installing ExternalCodeBase at {target_dir}")

    patch_get = mock.patch(
        "cstar.base.external_codebase.ExternalCodeBase.get", mock_get
    )

    with patch_source_data, patch_get:
        mecb = FakeExternalCodeBase()
        mecb._source = source
        yield mecb


@pytest.fixture
def marblexternalcodebase(
    mocksourcedata_remote_repo,
) -> Generator[MARBLExternalCodeBase, None, None]:
    """Fixture providing a `MARBLExternalCodeBase` instance for testing.

    Patches `SourceData` calls to avoid network and filesystem interaction.
    """
    source_data = mocksourcedata_remote_repo(
        location="https://marbl.com/repo.git", identifier="v1"
    )
    patch_source_data = mock.patch(
        "cstar.base.external_codebase.SourceData", return_value=source_data
    )

    with patch_source_data:
        yield MARBLExternalCodeBase()


@pytest.fixture
def marblexternalcodebase_staged(
    marblexternalcodebase,
    stagedrepository,
    tmp_path,
) -> Generator[MARBLExternalCodeBase, None, None]:
    """Fixture providing a staged `MARBLExternalCodeBase` instance for testing.

    Sets `working_copy` to a mock StagedRepository instance.
    """
    mecb = marblexternalcodebase
    staged_data = stagedrepository(
        path=tmp_path, source=mecb.source, changed_from_source=False
    )
    mecb._working_copy = staged_data
    yield mecb


################################################################################
# InputDataset
################################################################################


@pytest.fixture
def fakeinputdataset_local(
    mocksourcedata_local_file,
) -> Generator[InputDataset, None, None]:
    """Fixture to provide a mock local InputDataset instance.

    This fixture patches properties of the SourceData class to simulate a local dataset,
    initializing it with relevant attributes like location, start date, and end date.
    """
    fake_location = "some/local/source/path/local_file.nc"
    source_data = mocksourcedata_local_file(location=fake_location)
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
def fakeinputdataset_remote(
    mocksourcedata_remote_file,
) -> Generator[InputDataset, None, None]:
    """Fixture to provide a mock remote InputDataset instance.

    This fixture patches properties of the SourceData class to simulate a remote dataset,
    initializing it with attributes such as URL location, file hash, and date range.

    Yields
    ------
    FakeInputDataset: Instance representing a remote input dataset for testing.
    """
    # Using context managers to patch properties on DataSource
    fake_location = "http://example.com/remote_file.nc"
    fake_hash = "abc123"
    source_data = mocksourcedata_remote_file(
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
def stub_simulation(
    fakeexternalcodebase_with_mock_get, additionalcode_local, tmp_path
) -> StubSimulation:
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
        codebase=fakeexternalcodebase_with_mock_get,
        runtime_code=additionalcode_local(),
        compile_time_code=additionalcode_local(),
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
