"""Test suite to test fixtures defined in conftest.py and fixtures.py files."""

from collections.abc import Callable
from pathlib import Path

import yaml
from _pytest._py.path import LocalPath

from cstar.tests.integration_tests.config import (
    CSTAR_TEST_DATA_DIRECTORY,
    TEST_DIRECTORY,
)


def test_modify_template_blueprint(
    modify_template_blueprint: Callable,
    tmp_path: Path,
):
    """This test verifies that the modify_template_blueprint fixture correctly reads a
    specified blueprint, performs string replacements, and returns a Simulation instance
    with the correct parameters corresponding to the string replacements.

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
        / "integration_tests/blueprints/blueprint_template.yaml",
        strs_to_replace={
            "<additional_code_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git"
        },
        out_dir=tmp_path / "output",
    )

    assert isinstance(test_blueprint, LocalPath), (
        f"Expected type LocalPath, but got {type(test_blueprint)}"
    )
    with open(test_blueprint) as bpfile:
        bpyaml = yaml.safe_load(bpfile)

    assert (
        bpyaml["code"]["compile_time"]["location"]
        == "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git"
    )
    assert bpyaml["runtime_params"]["output_dir"] == str(tmp_path / "output")


class TestFetchData:
    """Test class for testing data-fetching fixtures defined in the roms/fixtures.py
    file.
    """

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
        assert not zip_files, (
            f"Zip file was not removed: {zip_files[0] if zip_files else 'Unknown zip file'}"
        )

        expected_files = [
            "additional_code/ROMS/runtime_code/roms.in",
            "additional_code/ROMS/compile_time_code/Makefile",
            "input_datasets/ROMS/roms_bry.nc",
            "input_datasets/ROMS/roms_frc.nc",
        ]

        missing_items = []
        for expected_file in expected_files:
            expected_path = CSTAR_TEST_DATA_DIRECTORY / expected_file
            if not expected_path.exists():
                missing_items.append(expected_file)

        assert not missing_items, (
            f"Missing expected files in the test data directory: {missing_items}"
        )
