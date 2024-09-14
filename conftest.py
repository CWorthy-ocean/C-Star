import builtins
from contextlib import contextmanager
from pathlib import Path
import yaml
from typing import Callable

import pytest


# Prevent errors caused by pytest trying to collect tests from externals 
# (e.g. from errors whilst importing python code in the UCLA-roms repo, see https://github.com/CWorthy-ocean/C-Star/issues/53)
collect_ignore_glob = [
    "cstar/cstar_local_config.py",
    "cstar/externals/**"
]


EXAMPLE_BLUEPRINTS = {
    "ROMS_MARBL": {
        # TODO this assumes you are running pytest from the root directory of the cloned repo, which is fragile
        "base": "./examples/cstar_blueprint_roms_marbl_example.yaml",
        "input_datasets_url_prefix": "https://github.com/CWorthy-ocean/input_datasets_roms_marbl_example/raw/main/",
        "additional_code_url": "https://github.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example.git",
    },
}


@pytest.fixture
def mock_user_input():
    """
    Monkeypatch which will automatically respond to any call for input.
    
    Use it like this:
 
        ```
        def some_test(mock_user_input):
            with mock_user_input("yes"):
                assert input("Enter your choice: ") == "yes"
        ```
    """
    @contextmanager
    def _mock_input(input_string):
        original_input = builtins.input
        def mock_input_function(_):
            return input_string
        builtins.input = mock_input_function
        try:
            yield
        finally:
            builtins.input = original_input
    
    return _mock_input


@pytest.fixture
def example_blueprint_as_dict() -> Callable[[str], dict]:
    """Given the name of a pre-defined blueprint, return it as an in-memory dict."""

    def _base_blueprint_dict(name: str) -> dict:
        base_blueprint_path = EXAMPLE_BLUEPRINTS[name]["base"]

        with open(base_blueprint_path, "r") as file:
            base_blueprint_dict = yaml.safe_load(file)

        return base_blueprint_dict

    return _base_blueprint_dict


# TODO should these local files be in a specified temporary directory?

def change_remote_paths_to_local_paths(
    blueprint: dict,
    name: str,
) -> dict:
    """
    Alter an in-memory blueprint to point to the locally-downloaded versions of files downloaded from remote data sources.

    Parameters
    ----------
    blueprint
    name
    """

    input_datasets_url_prefix: str = EXAMPLE_BLUEPRINTS[name][
        "input_datasets_url_prefix"
    ]
    additional_code_url: str = EXAMPLE_BLUEPRINTS[name]["additional_code_url"]  # noqa

    def is_a_location_containing_input_datasets_url(key: str, value: str) -> bool:
        return key == "location" and input_datasets_url_prefix in value

    def update_path_to_input_datasets(key: str, value: str) -> str:
        filename = Path(value).name
        # TODO make "local_input_files" a parameter?
        local_filepath = Path.cwd() / "local_input_files" / filename
        return str(local_filepath)

    blueprint_with_local_input_datasets = process_nested_dict(
        dictionary=blueprint,
        condition=is_a_location_containing_input_datasets_url,
        update=update_path_to_input_datasets,
    )

    return blueprint_with_local_input_datasets

    # TODO
    # blueprint_with_local_additional_code = ...


def process_nested_dict(
    dictionary: dict,
    condition: Callable[[str, str], bool],
    update: Callable[[str, str], str],
    parent_key: str = "",
):
    result = {}
    for key, value in dictionary.items():
        full_key = f"{parent_key}.{key}" if parent_key else key
        if isinstance(value, dict):
            result[key] = process_nested_dict(value, condition, update, full_key)
        else:
            if condition(full_key, value):
                result[key] = update(full_key, value)
            else:
                result[key] = value
    return result


@pytest.fixture
def example_blueprint_as_path(
    example_blueprint_as_dict, tmp_path
) -> Callable[[str], Path]:
    """Given the name of a pre-defined blueprint, returns it as a (temporary) path to an on-disk file."""

    def _blueprint_as_path(name: str, make_local: bool = False) -> Path:
        # TODO add options to edit in-memory blueprints here?

        blueprint_dict = example_blueprint_as_dict(name)

        if make_local:
            blueprint_dict = change_remote_paths_to_local_paths(blueprint_dict, name)

        # save the blueprint to a temporary path
        blueprint_filepath = tmp_path / "blueprint.yaml"
        with open(blueprint_filepath, "w") as file:
            yaml.safe_dump(blueprint_dict, file)

        return blueprint_filepath

    return _blueprint_as_path
