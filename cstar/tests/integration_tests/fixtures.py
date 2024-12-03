import pooch
import pytest
import shutil
import zipfile
from typing import Callable
from pathlib import Path

from cstar.tests.integration_tests.config import (
    ROMS_TOOLS_DATA_DIRECTORY,
    CSTAR_TEST_DATA_DIRECTORY,
)


@pytest.fixture
def fetch_roms_tools_source_data(request) -> Callable[[str | Path], None]:
    """Fixture that provides a factory function to fetch source data needed by roms-
    tools.

    This fixture returns a function that fetches necessary data files from a remote
    repository using pooch and creates a symlink to the fetched data directory. A cleanup
    routine removes this symlink when the calling test is complete.

    Parameters:
    -----
    request (FixtureRequest):
       Built-in pytest fixture that provides access to the test request object,
       allowing the addition of finalizers for cleanup.

    Returns:
    --------
    _fetch_roms_tools_source_data, Callable[[str | Path], None]:
       A factory function that creates (at a specified path) a symlink to the fetched ROMS
       tools data directory, ensuring the data is available during testing.
    """

    def _fetch_roms_tools_source_data(symlink_path: str | Path) -> None:
        """Fetches ROMS tools source data and creates a symlink to the data directory at
        `symlink_path`.

        This function downloads the required data files from a specified remote repository
        to ROMS_TOOLS_DATA_DIRECTORY, specified in tests/config.py. After fetching the files,
        it creates a symlink to the data directory at the specified `symlink_path`.

        The symlink is used as the full path to the cached directory will vary from system to system, but
        calls to roms-tools.ROMSToolsObject.from_yaml accept a yaml file with a predetermined 'path' entry.
        This entry can be relative, so the calling test can create a symlink at a location corresponding to
        this path, but pointing to the correct cache directory.

        A cleanup function is registered to remove the symlink after the test completes.

        Parameters
        ----------
        symlink_path : str or Path
            The path where the symlink to the fetched data directory will be created.

        Returns
        -------
        None
        """

        if isinstance(symlink_path, str):
            symlink_path = Path(symlink_path)

        # cache_dir = Path(pooch.os_cache("roms_tools_datasets_for_cstar_test_case"))
        pup_test_data = pooch.create(
            path=ROMS_TOOLS_DATA_DIRECTORY,
            base_url="https://github.com/CWorthy-ocean/roms-tools-test-data/raw/main/",
            # The registry specifies the files that can be fetched
            registry={
                "GLORYS_NA_2012.nc": "b862add892f5d6e0d670c8f7fa698f4af5290ac87077ca812a6795e120d0ca8c",
                "ERA5_NA_2012.nc": "d07fa7450869dfd3aec54411777a5f7de3cb3ec21492eec36f4980e220c51757",
                "TPXO_global_test_data.nc": "457bfe87a7b247ec6e04e3c7d3e741ccf223020c41593f8ae33a14f2b5255e60",
                "CESM_BGC_2012.nc": "e374d5df3c1be742d564fd26fd861c2d40af73be50a432c51d258171d5638eb6",
                "CESM_BGC_SURFACE_2012.nc": "3c4d156adca97909d0fac36bf50b99583ab37d8020d7a3e8511e92abf2331b38",
            },
        )
        pup_test_data.fetch("GLORYS_NA_2012.nc")
        pup_test_data.fetch("TPXO_global_test_data.nc")
        pup_test_data.fetch("ERA5_NA_2012.nc")
        pup_test_data.fetch("CESM_BGC_2012.nc")
        pup_test_data.fetch("CESM_BGC_SURFACE_2012.nc")

        if symlink_path.is_symlink():
            raise FileExistsError(
                f"{symlink_path} already exists as a symlink to {symlink_path.resolve()}"
            )

        symlink_path.symlink_to(ROMS_TOOLS_DATA_DIRECTORY)

        def cleanup():
            if symlink_path.is_symlink():
                print(f"removing {symlink_path}")
                symlink_path.unlink()

        request.addfinalizer(cleanup)

    return _fetch_roms_tools_source_data


@pytest.fixture
def fetch_remote_test_case_data() -> Callable[[], None]:
    """Fixture that provides a function to fetch remote test case data from a GitHub
    repository.

    Returns
    -------
    Callable[[], None]
        A factory function that fetches the remote test case data and sets it up in the local
        test data directory.
    """

    def _fetch_remote_test_case_data() -> None:
        """Downloads and sets up the remote test case data for testing.

        This function downloads a zip archive of a specific commit of a repo containing test data,
        extracts the archive into the specified test data directory, and performs cleanup of
        intermediate files and directories.

        Data are saved to the CSTAR_TEST_DATA_DIRECTORY, set in tests/config.py

        Returns
        -------
        None
        """

        test_case_repo_url = (
            "https://github.com/CWorthy-ocean/cstar_blueprint_test_case/"
        )
        checkout_target = "935b5d4f6012b2863c2db103596f8f173a024d1c"

        # Construct the URL of this commit as a zip archive:
        archive_url = f"{test_case_repo_url.rstrip('/')}/archive/{checkout_target}.zip"

        # Download the zip with pooch
        zip_path = pooch.retrieve(
            url=archive_url,
            known_hash="c9847301c308044f98731fc70864018a65c82df0981bea7c4e47d5b8e3b41b03",
            fname=f"{checkout_target}.zip",  # Name of the cached file
            path=CSTAR_TEST_DATA_DIRECTORY,  # Set the cache directory (customize as needed)
        )

        # Unzip the files into a subdirectory `extract_dir` of `cache_dir`
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            extract_list = zip_ref.namelist()
            extract_dir = CSTAR_TEST_DATA_DIRECTORY / extract_list[0]
            zip_ref.extractall(CSTAR_TEST_DATA_DIRECTORY)

        # Copy the contents of the subdirectory up one and remove it and the zip file
        shutil.copytree(extract_dir, CSTAR_TEST_DATA_DIRECTORY, dirs_exist_ok=True)
        shutil.rmtree(extract_dir)
        Path(zip_path).unlink()

    return _fetch_remote_test_case_data
