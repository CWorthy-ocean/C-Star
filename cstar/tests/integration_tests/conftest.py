import logging
import shutil
import zipfile
from collections.abc import Callable
from pathlib import Path

import pooch
import pytest

from cstar.base.log import get_logger


@pytest.fixture
def log() -> logging.Logger:
    return get_logger("cstar.tests.integration_tests")


@pytest.fixture(scope="session")
def cstar_test_data_directory() -> Path:
    """Fixture returning a durable path where downloaded data will be kept."""
    return Path(pooch.os_cache("cstar_test_case_data"))


@pytest.fixture
def fetch_remote_test_case_data(cstar_test_data_directory: Path) -> Callable[[], None]:
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
            path=cstar_test_data_directory,  # Set the cache directory (customize as needed)
        )

        # Unzip the files into a subdirectory `extract_dir` of `cache_dir`
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            extract_list = zip_ref.namelist()
            extract_dir = cstar_test_data_directory / extract_list[0]
            zip_ref.extractall(cstar_test_data_directory)

        # Copy the contents of the subdirectory up one and remove it and the zip file
        shutil.copytree(extract_dir, cstar_test_data_directory, dirs_exist_ok=True)
        shutil.rmtree(extract_dir)
        Path(zip_path).unlink()

    return _fetch_remote_test_case_data


@pytest.fixture
def modify_template_blueprint(
    tmpdir: str,
) -> Callable[[Path | str, dict[str, str], Path | str], str]:
    """Fixture that provides a factory function for modifying template blueprint files.

    This fixture returns a function that can returns a path to a modified a blueprint
    template file based on specified string replacements.

    Parameters:
    -----
    tmpdir:
       Pytest fixture used to create a temporary directory in which to hold a modified version
       of the template file during the test.

    Returns:
    --------
    _modify_template_blueprint, Callable[[Path | str, dict[str, str]], Path]:
       The factory function
    """

    def _modify_template_blueprint(
        template_blueprint_path: Path | str,
        strs_to_replace: dict,
        out_dir: Path | str,
    ) -> str:
        """Creates a temporary, customized blueprint file from a template.

        This function reads a blueprint template file, performs string replacements as specified
        by `strs_to_replace`, saves the modified content to a temporary file within the `tmpdir`
        provided by pytest.

        Parameters:
        -----------
        template_blueprint_path (Path | str):
           The path to the blueprint template file.
        strs_to_replace (dict[str, str]):
           A dictionary where keys are substrings to find in the template,
           and values are the replacements.

        Returns:
        --------
        modified_blueprint_path:
           A temporary path to a modified version of the template blueprint file.
        """
        template_blueprint_path = Path(template_blueprint_path)

        with open(template_blueprint_path) as template_file:
            template_content = template_file.read()

        modified_template_content = template_content
        for oldstr, newstr in strs_to_replace.items():
            modified_template_content = modified_template_content.replace(
                oldstr, newstr
            )
        modified_template_content = modified_template_content.replace(
            "<output_dir>", str(out_dir)
        )
        temp_path = tmpdir.join(template_blueprint_path.name)
        with open(temp_path, "w") as temp_file:
            temp_file.write(modified_template_content)

        return temp_path

    return _modify_template_blueprint


@pytest.fixture
def integration_test_configuration(
    tests_path: Path,
    cstar_test_data_directory: Path,
) -> dict[str, dict[str, str | dict[str, str]]]:
    """Fixture returning a dictionary containing configuration for running multiple
    test simulations.

    Parameters
    ----------
    tests_path : Path
        Fixture returning the directory containing c-star tests; used to build
        absolute paths for other file located relative to the tests directory.
    Returns
    -------
    dict[str, str | dict[str, str]]
    """
    ## Configuration of different cases to test
    return {
        # Remote cases:
        # NetCDF
        "test_case_remote_with_netcdf_datasets": {
            "template_blueprint_path": f"{tests_path}/integration_tests/blueprints/blueprint_template.yaml",
            "strs_to_replace": {
                "<input_datasets_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case/raw/roms_tools_3_1_2/input_datasets/ROMS",
                "<additional_code_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git",
            },
        },
        # Local cases:
        # NetCDF
        "test_case_local_with_netcdf_datasets": {
            "template_blueprint_path": f"{tests_path}/integration_tests/blueprints/blueprint_template.yaml",
            "strs_to_replace": {
                "<input_datasets_location>": f"{cstar_test_data_directory / 'input_datasets/ROMS'}",
                "<additional_code_location>": f"{cstar_test_data_directory}",
            },
        },
    }
