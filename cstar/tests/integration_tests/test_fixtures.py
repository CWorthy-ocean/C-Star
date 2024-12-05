"""Test suite to test fixtures defined in conftest.py and fixtures.py files."""

import yaml
from pathlib import Path
from _pytest._py.path import LocalPath
from cstar.tests.integration_tests.config import (
    ROMS_TOOLS_DATA_DIRECTORY,
    CSTAR_TEST_DATA_DIRECTORY,
    TEST_DIRECTORY,
)


def test_mock_input_fixture(mock_user_input):
    # Mocked input behavior
    with mock_user_input("yes"):
        assert input("Enter your choice: ") == "yes"


def test_modify_template_blueprint(modify_template_blueprint, tmpdir):
    """This test verifies that the modify_template_blueprint fixture correctly reads a
    specified blueprint, performs string replacements, and returns a Case instance with
    the correct parameters corresponding to the string replacements.

    Parameters
    ----------
    modify_template_blueprint : Callable
        A fixture that modifies a template blueprint with specific string replacements.
    tmpdir : Path
        Built-in pytest fixture for creating a temporary directory during the test

    Asserts
    -------
    - The returned object is an instance of Path.
    - The additional_source_code location matches that expected after replacement
    """
    test_blueprint = modify_template_blueprint(
        template_blueprint_path=TEST_DIRECTORY
        / "integration_tests/blueprints/cstar_blueprint_with_netcdf_datasets_template.yaml",
        strs_to_replace={
            "<additional_code_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git"
        },
    )

    assert isinstance(
        test_blueprint, LocalPath
    ), f"Expected type LocalPath, but got {type(test_blueprint)}"
    with open(test_blueprint, "r") as bpfile:
        bpyaml = yaml.safe_load(bpfile)

    assert (
        bpyaml["components"][1]["component"]["additional_source_code"]["location"]
        == "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git"
    )


class TestFetchData:
    """Test class for testing data-fetching fixtures defined in the roms/fixtures.py
    file."""

    def test_fetch_roms_tools_source_data(
        self, request, tmpdir, fetch_roms_tools_source_data
    ):
        """Test the fetch_roms_tools_source_data fixture by validating data fetching and
        symlink creation.

        This test checks that the fetch_roms_tools_source_data fixture correctly fetches the
        expected data files, creates a symlink to the data directory, and ensures all expected
        files are present and accessible through the symlink.

        Parameters
        ----------
        request : FixtureRequest
            Pytest's request object used by the fixture for cleanup (symlink removal)
        tmpdir : Path
           Built-in pytest fixture for creating a temporary directory during the test
        fetch_roms_tools_source_data : Callable
            A fixture that fetches ROMS tools data and sets up a temporary symlink to the data directory

        Asserts
        -------
        - The symlink points to the correct data directory
        - The symlink exists and is valid
        - All expected data files are present in the symlinked directory
        """

        test_data_directory = Path(tmpdir / "test_data_directory")
        fetch_roms_tools_source_data(test_data_directory)

        assert (
            test_data_directory.resolve() == ROMS_TOOLS_DATA_DIRECTORY.resolve()
        ), f"{test_data_directory} links to {test_data_directory.resolve}, not {ROMS_TOOLS_DATA_DIRECTORY}"
        assert test_data_directory.exists(), f"{test_data_directory} does not exist"
        assert (
            test_data_directory.is_symlink()
        ), f"{test_data_directory} is not a symbolic link as expected"

        file_list = [f.name for f in test_data_directory.glob("*")]
        expected_files = [
            "GLORYS_NA_2012.nc",
            "ERA5_NA_2012.nc",
            "TPXO_global_test_data.nc",
            "CESM_BGC_2012.nc",
            "CESM_BGC_SURFACE_2012.nc",
        ]
        missing_files = [f for f in expected_files if f not in file_list]
        assert not missing_files, f"missing expected files {missing_files}"

    def test_fetch_remote_test_case_data(self, fetch_remote_test_case_data):
        """Test the fetch_remote_test_case_data fixture.

        This test verifies that the fetch_remote_test_case_data fixture correctly downloads
        the test data archive, unzips the necessary files into the test data directory, and
        performs cleanup by removing the zip file after extraction.

        Parameters
        ----------
        fetch_remote_test_case_data : Callable
            A fixture that fetches the remote test case data.

        Asserts
        -------
        - No zip files remain in the test data directory after extraction.
        - A subset of expected test files and directories are present in the data directory.
        """
        fetch_remote_test_case_data()

        zip_files = list(CSTAR_TEST_DATA_DIRECTORY.glob("*.zip"))
        assert not zip_files, f"Zip file was not removed: {zip_files[0] if zip_files else 'Unknown zip file'}"

        expected_files = [
            "additional_code/ROMS/namelists/roms.in",
            "additional_code/ROMS/source_mods/Makefile",
            "input_datasets/ROMS/roms_bry.nc",
            "input_datasets/ROMS/roms_frc.nc",
            "roms_tools_yaml_files/roms_bry.yaml",
            "roms_tools_yaml_files/roms_ini.yaml",
        ]

        missing_items = []
        for expected_file in expected_files:
            expected_path = CSTAR_TEST_DATA_DIRECTORY / expected_file
            if not expected_path.exists():
                missing_items.append(expected_file)

        assert (
            not missing_items
        ), f"Missing expected files in the test data directory: {missing_items}"
