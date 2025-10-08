import shutil
import zipfile
from collections.abc import Callable
from pathlib import Path

import pooch
import pytest

from cstar.tests.integration_tests.config import (
    CSTAR_TEST_DATA_DIRECTORY,
)


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
        checkout_target = "roms_tools_3_1_2"

        # Construct the URL of this commit as a zip archive:
        archive_url = f"{test_case_repo_url.rstrip('/')}/archive/{checkout_target}.zip"

        # Download the zip with pooch
        zip_path = pooch.retrieve(
            url=archive_url,
            known_hash="ea5d149975622bde9aefe4af32e078969b5e68e4bf08efd85d562799aa3e5500",
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
