"""Configuration module for testing C-Star with pytest.

The first section sets or gets any globally accessible paths (e.g. that of the test directory itself, TEST_DIRECTORY).

The second section defines the `TEST_CONFIG` dictionary, whose keys serve as parameters for the primary test in
cstar/tests/roms/test_cstar_test_blueprints.py.
The entries in this dictionary should specify a path to a template blueprint file to be modified, and a dictionary
of substrings in that file to modify (e.g. {"oldstr" : "newstr"}). These will be combined to make a modified blueprint
from which the test case is constructed.
"""

from pathlib import Path
from pooch import os_cache

## Common paths used in tests


def _get_test_directory():
    from importlib.util import find_spec
    from pathlib import Path

    spec = find_spec("cstar")
    if spec is not None:
        return Path(spec.origin).parent / "tests"
    else:
        raise RuntimeError("Cannot determine the package location of cstar")


TEST_DIRECTORY = _get_test_directory()
ROMS_TOOLS_DATA_DIRECTORY = Path(os_cache("roms_tools_datasets_for_cstar_test_case"))
CSTAR_TEST_DATA_DIRECTORY = Path(os_cache("cstar_test_case_data"))

## Configuration of different cases to test
TEST_CONFIG = {
    # Remote cases:
    # NetCDF
    "test_case_remote_with_netcdf_datasets": {
        "template_blueprint_path": f"{TEST_DIRECTORY}/integration_tests/blueprints/cstar_blueprint_with_netcdf_datasets_template.yaml",
        "strs_to_replace": {
            "<input_datasets_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case/raw/main/input_datasets/ROMS",
            "<additional_code_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git",
        },
    },
    # YAML
    "test_case_remote_with_yaml_datasets": {
        "template_blueprint_path": f"{TEST_DIRECTORY}/integration_tests/blueprints/cstar_blueprint_with_yaml_datasets_template.yaml",
        "strs_to_replace": {
            "<input_datasets_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case/raw/main/roms_tools_yaml_files",
            "<additional_code_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git",
        },
    },
    # Local cases:
    # NetCDF
    "test_case_local_with_netcdf_datasets": {
        "template_blueprint_path": f"{TEST_DIRECTORY}/integration_tests/blueprints/cstar_blueprint_with_netcdf_datasets_template.yaml",
        "strs_to_replace": {
            "<input_datasets_location>": f"{CSTAR_TEST_DATA_DIRECTORY/'input_datasets/ROMS'}",
            "<additional_code_location>": f"{CSTAR_TEST_DATA_DIRECTORY}",
        },
    },
    # YAML
    "test_case_local_with_yaml_datasets": {
        "template_blueprint_path": f"{TEST_DIRECTORY}/integration_tests/blueprints/cstar_blueprint_with_yaml_datasets_template.yaml",
        "strs_to_replace": {
            "<input_datasets_location>": f"{CSTAR_TEST_DATA_DIRECTORY/'roms_tools_yaml_files'}",
            "<additional_code_location>": f"{CSTAR_TEST_DATA_DIRECTORY}",
        },
    },
}
