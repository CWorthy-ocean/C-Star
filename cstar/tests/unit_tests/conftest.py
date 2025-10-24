import functools
import logging
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest import mock

import dotenv
import numpy as np
import pytest

from cstar.base import AdditionalCode, Discretization, ExternalCodeBase, InputDataset
from cstar.base.gitutils import git_location_to_raw
from cstar.base.log import get_logger
from cstar.io.constants import SourceClassification
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


@pytest.fixture(scope="module")
def blueprint_path() -> Path:
    """Fixture that creates and returns a blueprint yaml location.

    Returns
    -------
    Path
        The path to the valid, complete blueprint yaml file.
    """
    tests_root = Path(__file__).parent.parent
    bp_path = (
        tests_root / "integration_tests" / "blueprints" / "blueprint_complete.yaml"
    )
    return bp_path


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
    marblexternalcodebase, stagedrepository, marbl_path
) -> Generator[MARBLExternalCodeBase, None, None]:
    """Fixture providing a staged `MARBLExternalCodeBase` instance for testing.

    Sets `working_copy` to a mock StagedRepository instance.
    """
    mecb = marblexternalcodebase
    staged_data = stagedrepository(
        path=marbl_path, source=mecb.source, changed_from_source=False
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


@pytest.fixture(scope="session")
def mock_system_name() -> str:
    # A name for the mock system/platform executing the tests.
    return "mock_system"


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


@pytest.fixture(scope="session")
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


################################################################################
## ROMS-related
## NOTE These should be moved to `roms/conftest.py` longer term. Higher level
## tests should remain general (e.g. using a generic 'Simulation' subclass) rather than
## using ROMS-specific fixtures (e.g. 'ROMSSimulation')
################################################################################
from cstar.roms import (  # noqa: E402
    ROMSDiscretization,
    ROMSExternalCodeBase,
    ROMSSimulation,
)
from cstar.roms.input_dataset import (  # noqa: E402
    ROMSBoundaryForcing,
    ROMSCdrForcing,
    ROMSForcingCorrections,
    ROMSInitialConditions,
    ROMSInputDataset,
    ROMSModelGrid,
    ROMSRiverForcing,
    ROMSSurfaceForcing,
    ROMSTidalForcing,
)
from cstar.roms.runtime_settings import ROMSRuntimeSettings  # noqa: E402
from cstar.tests.unit_tests.fake_abc_subclasses import (  # noqa: E402
    FakeROMSInputDataset,
)


@pytest.fixture
def romsexternalcodebase(
    mocksourcedata_remote_repo,
) -> Generator[ROMSExternalCodeBase, None, None]:
    """Fixture providing a `ROMSExternalCodeBase` instance for testing.

    Patches `SourceData` calls to avoid network and filesystem interaction.
    """
    location = "https://github.com/roms/repo.git"
    identifier = "roms_branch"
    source_data = mocksourcedata_remote_repo(location=location, identifier=identifier)
    patch_source_data = mock.patch(
        "cstar.base.external_codebase.SourceData", return_value=source_data
    )
    with patch_source_data:
        yield ROMSExternalCodeBase(source_repo=location, checkout_target=identifier)


@pytest.fixture
def romsexternalcodebase_staged(
    romsexternalcodebase,
    stagedrepository,
    roms_path,
) -> Generator[ROMSExternalCodeBase, None, None]:
    """Fixture providing a staged `ROMSExternalCodeBase` instance for testing.

    Sets `working_copy` to a mock StagedRepository instance.
    """
    recb = romsexternalcodebase
    staged_data = stagedrepository(
        path=roms_path, source=recb.source, changed_from_source=False
    )
    recb._working_copy = staged_data
    yield recb


################################################################################
# ROMSRuntimeSettings
################################################################################
@pytest.fixture
def romsruntimesettings():
    """Fixture providing a `ROMSRuntimeSettings` instance for testing.

    The example instance corresponds to the file `fixtures/example_runtime_settings.in`
    in order to test the `ROMSRuntimeSettings.to_file` and `from_file` methods.

    Paths do not correspond to real files.

    Returns
    -------
    ROMSRuntimeSettings
       The example ROMSRuntimeSettings instance
    """
    return ROMSRuntimeSettings(
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
# Runtime and compile-time code
################################################################################
@pytest.fixture
def roms_runtime_code(additionalcode_local) -> AdditionalCode:
    """Provides an example of ROMSSimulation.runtime_code with fake values for testing"""
    rc = additionalcode_local(
        location="/some/local/dir",
        subdir="subdir/",
        files=[
            "file1",
            "file2.in",
            "marbl_in",
            "marbl_tracer_output_list",
            "marbl_diagnostic_output_list",
        ],
    )
    return rc


@pytest.fixture
def roms_compile_time_code(additionalcode_local) -> AdditionalCode:
    """Provides an example of ROMSSimulation.compile_time_code with fake values for testing"""
    cc = additionalcode_local(
        location="/some/local/dir",
        subdir="subdir/",
        files=["file1.h", "file2.opt"],
    )
    return cc


################################################################################
# ROMSInputDataset
################################################################################
@pytest.fixture
def romsinputdataset_local_netcdf(
    mocksourcedata_local_file,
) -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a local NetCDF source.

    Yields:
    -------
        FakeROMSInputDataset: A dataset instance pointing to a local NetCDF file.
    """
    fake_location = "some/local/source/path/local_file.nc"
    source_data = mocksourcedata_local_file(location=fake_location)
    patch_source_data = mock.patch(
        "cstar.roms.input_dataset.SourceData", return_value=source_data
    )
    with patch_source_data:
        dataset = FakeROMSInputDataset(
            location=fake_location,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def romsinputdataset_remote_netcdf(
    mocksourcedata_remote_file,
) -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a local NetCDF source.

    Yields:
    -------
        FakeROMSInputDataset: A dataset instance pointing to a local NetCDF file.
    """
    fake_location = "http://example.com/local_file.nc"
    source_data = mocksourcedata_remote_file(location=fake_location)
    patch_source_data = mock.patch(
        "cstar.roms.input_dataset.SourceData", return_value=source_data
    )
    with patch_source_data:
        dataset = FakeROMSInputDataset(
            location=fake_location,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def romsinputdataset_remote_partitioned_source(
    mocksourcedata_remote_file,
    mock_sourcedatacollection,
) -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a remote, partitioned NetCDF source.

    Yields:
    -------
        FakeROMSInputDataset: A dataset instance pointing to remote, partitioned NetCDF data.
    """
    fake_location = "http://example.com//local_file_00.nc"
    fake_np_xi = 5
    fake_np_eta = 2

    nparts = fake_np_xi * fake_np_eta
    source_data = mocksourcedata_remote_file(
        location=fake_location, identifier="unusedhash"
    )
    patch_source_data = mock.patch(
        "cstar.roms.input_dataset.SourceData", return_value=source_data
    )
    parted_locations = [
        fake_location.replace("00", str(i).zfill(2)) for i in range(nparts)
    ]
    unused_identifiers = [f"unusedhash{i}" for i in range(nparts)]
    source_data_collection = mock_sourcedatacollection(
        locations=parted_locations,
        identifiers=unused_identifiers,
        classification=SourceClassification.REMOTE_BINARY_FILE,
    )
    patch_source_data_collection = mock.patch(
        "cstar.roms.input_dataset.SourceDataCollection.from_locations",
        return_value=source_data_collection,
    )

    with patch_source_data, patch_source_data_collection:
        dataset = FakeROMSInputDataset(
            location=fake_location,
            source_np_xi=5,
            source_np_eta=2,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def romsinputdataset_local_yaml(
    mocksourcedata_local_text_file,
) -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a local YAML source.

    Yields:
    -------
        FakeROMSInputDataset: A dataset instance pointing to a local YAML file.
    """
    fake_location = "some/local/source/path/local_file.yaml"
    source_data = mocksourcedata_local_text_file(location=fake_location)
    patch_source_data = mock.patch(
        "cstar.roms.input_dataset.SourceData", return_value=source_data
    )
    with patch_source_data:
        dataset = FakeROMSInputDataset(
            location=fake_location,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def romsinputdataset_remote_yaml(
    mocksourcedata_remote_text_file,
) -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a remote YAML source.

    Yields:
    -------
        FakeROMSInputDataset: A dataset instance pointing to a remote YAML file.
    """
    fake_location = "https://dodgyfakeyamlfiles.ru/all/remote_file.yaml"
    source_data = mocksourcedata_remote_text_file(
        location=fake_location,
    )
    patch_source_data = mock.patch(
        "cstar.roms.input_dataset.SourceData", return_value=source_data
    )
    with patch_source_data:
        dataset = FakeROMSInputDataset(
            location=fake_location,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def roms_model_grid(
    mocksourcedata_remote_file,
) -> Callable[[str, str, SourceData], ROMSModelGrid]:
    """Provides a ROMSModelGrid instance with fake attrs for testing."""
    default_location = "http://my.files/grid.nc"
    default_hash = "123"
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSModelGrid(
                location=location,
                file_hash=file_hash,
            )

    return _create


@pytest.fixture
def roms_initial_conditions(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSInitialConditions
]:
    """Provides a ROMSInitialConditions instance with fake attrs for testing."""
    default_location = "http://my.files/initial.nc"
    default_hash = "234"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSInitialConditions(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
            )

    return _create


@pytest.fixture
def roms_tidal_forcing(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSTidalForcing
]:
    """Provides a ROMSTidalForcing instance with fake attrs for testing."""
    default_location = "http://my.files/tidal.nc"
    default_hash = "345"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSTidalForcing(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


@pytest.fixture
def roms_cdr_forcing(
    mocksourcedata_remote_file,
) -> Callable[[str, str, SourceData, datetime | None, datetime | None], ROMSCdrForcing]:
    """Provides a ROMSCdrForcing instance with fake attrs for testing"""
    default_location = "http://my.files/cdr.nc"
    default_hash = "542"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSCdrForcing(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


@pytest.fixture
def roms_river_forcing(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSRiverForcing
]:
    """Provides a ROMSRiverForcing instance with fake attrs for testing"""
    default_location = "http://my.files/river.nc"
    default_hash = "543"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSRiverForcing(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


@pytest.fixture
def roms_boundary_forcing(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSBoundaryForcing
]:
    """Provides a ROMSBoundaryForcing instance with fake attrs for testing"""
    default_location = "http://my.files/boundary.nc"
    default_hash = "456"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSBoundaryForcing(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


@pytest.fixture
def roms_surface_forcing(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSSurfaceForcing
]:
    """Provides a ROMSSurfaceForcing instance with fake attrs for testing."""
    default_location = "http://my.files/surface.nc"
    default_hash = "567"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSSurfaceForcing(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


@pytest.fixture
def roms_forcing_corrections(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSForcingCorrections
]:
    """Provides a ROMSForcingCorrections instance with fake attrs for testing"""
    default_location = "http://my.files/sw_corr.nc"
    default_hash = "890"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSForcingCorrections(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


################################################################################
# ROMSSimulation
################################################################################


@pytest.fixture
def stub_romssimulation(
    marblexternalcodebase,
    romsexternalcodebase,
    roms_runtime_code,
    roms_compile_time_code,
    roms_model_grid,
    roms_initial_conditions,
    roms_tidal_forcing,
    roms_river_forcing,
    roms_boundary_forcing,
    roms_surface_forcing,
    roms_cdr_forcing,
    roms_forcing_corrections,
    tmp_path,
) -> ROMSSimulation:
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
        codebase=romsexternalcodebase,
        runtime_code=roms_runtime_code,
        compile_time_code=roms_compile_time_code,
        start_date="2025-01-01",
        end_date="2025-12-31",
        valid_start_date="2024-01-01",
        valid_end_date="2026-01-01",
        marbl_codebase=marblexternalcodebase,
        model_grid=roms_model_grid(),
        initial_conditions=roms_initial_conditions(),
        tidal_forcing=roms_tidal_forcing(),
        river_forcing=roms_river_forcing(),
        boundary_forcing=[
            roms_boundary_forcing(),
        ],
        surface_forcing=[
            roms_surface_forcing(),
        ],
        forcing_corrections=[
            roms_forcing_corrections(),
        ],
        cdr_forcing=roms_cdr_forcing(),
    )

    return sim


@pytest.fixture
def stub_romssimulation_dict(stub_romssimulation) -> dict[str, Any]:
    """Fixture returning the dictionary associated with the `stub_romssimulation` fixture.

    Used for independently testing to/from_dict methods.
    """
    sim = stub_romssimulation
    return_dict = {
        "name": sim.name,
        "valid_start_date": sim.valid_start_date,
        "valid_end_date": sim.valid_end_date,
        "codebase": {
            "source_repo": sim.codebase.source.location,
            "checkout_target": sim.codebase.source.checkout_target,
        },
        "discretization": {
            "time_step": sim.discretization.time_step,
            "n_procs_x": sim.discretization.n_procs_x,
            "n_procs_y": sim.discretization.n_procs_y,
        },
        "runtime_code": sim.runtime_code._constructor_args,
        "compile_time_code": sim.compile_time_code._constructor_args,
        "marbl_codebase": {
            "source_repo": sim.marbl_codebase.source.location,
            "checkout_target": sim.marbl_codebase.source.checkout_target,
        },
        "model_grid": {
            "location": sim.model_grid.source.location,
            "file_hash": sim.model_grid.source.file_hash,
        },
        "initial_conditions": {
            "location": sim.initial_conditions.source.location,
            "file_hash": sim.initial_conditions.source.file_hash,
        },
        "tidal_forcing": {
            "location": sim.tidal_forcing.source.location,
            "file_hash": sim.tidal_forcing.source.file_hash,
        },
        "river_forcing": {
            "location": sim.river_forcing.source.location,
            "file_hash": sim.river_forcing.source.file_hash,
        },
        "boundary_forcing": [
            {
                "location": sim.boundary_forcing[0].source.location,
                "file_hash": sim.boundary_forcing[0].source.file_hash,
            },
        ],
        "surface_forcing": [
            {
                "location": sim.surface_forcing[0].source.location,
                "file_hash": sim.surface_forcing[0].source.file_hash,
            }
        ],
        "forcing_corrections": [
            {
                "location": sim.forcing_corrections[0].source.location,
                "file_hash": sim.forcing_corrections[0].source.file_hash,
            }
        ],
        "cdr_forcing": {
            "location": sim.cdr_forcing.source.location,
            "file_hash": sim.cdr_forcing.source.file_hash,
        },
    }
    return return_dict


@pytest.fixture
def stub_romssimulation_dict_no_forcing_lists(
    stub_romssimulation_dict,
) -> dict[str, Any]:
    """As stub_romssimulation_dict, but without list values for certain forcing types."""
    sim_dict = stub_romssimulation_dict
    for k in ["surface_forcing", "boundary_forcing", "forcing_corrections"]:
        sim_dict[k] = sim_dict[k][0]
    return sim_dict


@pytest.fixture
def patch_romssimulation_init_sourcedata(
    stub_romssimulation,
    mocksourcedata_remote_repo,
    mocksourcedata_remote_file,
    mock_sourcedatacollection,
) -> Callable[[], AbstractContextManager[None]]:
    """Fixture returning a contextmanager patching all ROMSSimulation.__init__ SourceData calls.

    Used in tests that create a new ROMSSimulation instance.
    """
    sim = stub_romssimulation

    # External codebase SourceData mocks
    mock_externalcodebase_sourcedata = mocksourcedata_remote_repo(
        location=sim.codebase.source.location,
        identifier=sim.codebase.source.identifier,
    )
    mock_marbl_externalcodebase_sourcedata = mocksourcedata_remote_repo(
        location=sim.marbl_codebase.source.location,
        identifier=sim.marbl_codebase.source.identifier,
    )

    # ROMS input dataset SourceData mocks
    mock_model_grid_sourcedata = mocksourcedata_remote_file(
        location=sim.model_grid.source.location,
        identifier=sim.model_grid.source.identifier,
    )
    mock_initial_conditions_sourcedata = mocksourcedata_remote_file(
        location=sim.initial_conditions.source.location,
        identifier=sim.initial_conditions.source.identifier,
    )
    mock_tidal_forcing_sourcedata = mocksourcedata_remote_file(
        location=sim.tidal_forcing.source.location,
        identifier=sim.tidal_forcing.source.identifier,
    )
    mock_river_forcing_sourcedata = mocksourcedata_remote_file(
        location=sim.river_forcing.source.location,
        identifier=sim.river_forcing.source.identifier,
    )
    mock_boundary_forcing_sourcedata = mocksourcedata_remote_file(
        location=sim.boundary_forcing[0].source.location,
        identifier=sim.boundary_forcing[0].source.identifier,
    )
    mock_surface_forcing_sourcedata = mocksourcedata_remote_file(
        location=sim.surface_forcing[0].source.location,
        identifier=sim.surface_forcing[0].source.identifier,
    )
    mock_forcing_corrections_sourcedata = mocksourcedata_remote_file(
        location=sim.forcing_corrections[0].source.location,
        identifier=sim.forcing_corrections[0].source.identifier,
    )

    mock_cdr_forcing_sourcedata = mocksourcedata_remote_file(
        location=sim.cdr_forcing.source.location,
        identifier=sim.cdr_forcing.source.identifier,
    )

    # AdditionalCode SourceData mocks
    mock_runtime_code_sourcedata = mock_sourcedatacollection(
        locations=sim.runtime_code.source.locations,
        identifiers=["fake_hash" for i in sim.runtime_code.source],
        classification=SourceClassification.LOCAL_TEXT_FILE,
    )
    mock_runtime_code_classify_side_effect = [
        SourceClassification.LOCAL_DIRECTORY,
    ]

    mock_compile_time_code_sourcedata = mock_sourcedatacollection(
        locations=sim.compile_time_code.source.locations,
        identifiers=["fake_hash" for i in sim.compile_time_code.source],
        classification=SourceClassification.LOCAL_TEXT_FILE,
    )
    mock_compile_time_code_classify_side_effect = [
        SourceClassification.LOCAL_DIRECTORY,
    ]

    @contextmanager
    def _context():
        with (
            mock.patch(
                "cstar.base.external_codebase.SourceData",
                side_effect=[
                    mock_externalcodebase_sourcedata,
                    mock_marbl_externalcodebase_sourcedata,
                ],
            ),
            mock.patch(
                "cstar.roms.simulation.ROMSExternalCodeBase.is_configured",
                new_callable=mock.PropertyMock,
                return_value=False,
            ),
            mock.patch(
                "cstar.roms.input_dataset.SourceData",
                side_effect=[
                    mock_model_grid_sourcedata,
                    mock_initial_conditions_sourcedata,
                    mock_tidal_forcing_sourcedata,
                    mock_river_forcing_sourcedata,
                    mock_cdr_forcing_sourcedata,
                    mock_boundary_forcing_sourcedata,
                    mock_surface_forcing_sourcedata,
                    mock_forcing_corrections_sourcedata,
                ],
            ),
            mock.patch(
                "cstar.base.additional_code.SourceDataCollection.from_locations",
                side_effect=[
                    mock_runtime_code_sourcedata,
                    mock_compile_time_code_sourcedata,
                ],
            ),
            mock.patch(
                "cstar.io.source_data._SourceInspector.classify",
                side_effect=[
                    *mock_runtime_code_classify_side_effect,
                    *mock_compile_time_code_classify_side_effect,
                ],
            ),
        ):
            yield

    return _context
