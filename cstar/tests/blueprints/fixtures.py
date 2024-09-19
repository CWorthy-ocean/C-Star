from pathlib import Path
import yaml
from typing import Any, Callable, Union, cast

import pytest

from cstar.tests.blueprints import TEST_BLUEPRINTS


# Define a type alias for YAML-compatible types
YAMLValue = Union[dict[str, Any], list[Any], str, int, float, bool, None]


@pytest.fixture
def test_blueprint_as_dict() -> Callable[[str], dict]:
    """Given the name of a pre-defined blueprint, return it as an in-memory dict."""

    def _template_blueprint_dict(name: str, local: bool = False) -> dict:
        template_blueprint_path = TEST_BLUEPRINTS[name]["base"]

        with open(template_blueprint_path, "r") as file:
            template_blueprint_dict = yaml.safe_load(file)

        # replace all placeholder values
        full_blueprint_dict = set_locations(template_blueprint_dict, name, local)

        return full_blueprint_dict

    return _template_blueprint_dict


# TODO should these local files be in a specified temporary directory?


def set_locations(
    template_blueprint: dict,
    name: str,
    local: bool,
) -> dict:
    """
    Alter an in-memory template blueprint to point to either the locally-downloaded versions of files or remote data sources.
    """

    input_datasets_location = TEST_BLUEPRINTS[name]["input_datasets_location"]

    def contains_input_datasets_location(path: str, value: YAMLValue) -> bool:
        if path.endswith("location") and isinstance(value, str):
            return "<input_datasets_location>" in value
        else:
            return False

    def modify_to_use_remote_path_to_input_datasets(value: str) -> str:
        return value.replace("<input_datasets_location>", input_datasets_location)

    def modify_to_use_local_path_to_input_datasets(value: str) -> str:
        filename = value.removeprefix("<input_datasets_location>")
        # TODO make "local_input_files" a parameter?
        local_filepath = Path.cwd() / "local_input_files" / filename

        return value.replace("<input_datasets_location>", str(local_filepath))

    if local:
        modify_func = modify_to_use_local_path_to_input_datasets
    else:
        modify_func = modify_to_use_remote_path_to_input_datasets

    blueprint_with_updated_input_datasets_location = modify_yaml(
        template_blueprint,
        condition_func=contains_input_datasets_location,
        modify_func=modify_func,  # type: ignore[arg-type]  # we will only be modifying values if they are strings
    )

    # cast because we know our blueprint yamls always have a dict as the top-level structure
    return cast(dict, blueprint_with_updated_input_datasets_location)

    # TODO
    # blueprint_with_local_additional_code = ...
    #
    # additional_code_url: str = TEST_BLUEPRINTS[name]["additional_code_url"]  # noqa


def modify_yaml(
    data: YAMLValue,
    condition_func: Callable[[str, YAMLValue], bool],
    modify_func: Callable[[YAMLValue], YAMLValue],
    path: str = "",
) -> YAMLValue:
    """
    Modify the in-memory representation of a YAML file.

    If a value satisfies `condition_func`, modifies it using `modify_func`.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{path}.{key}" if path else key
            if condition_func(new_path, value):
                data[key] = modify_func(value)
            else:
                data[key] = modify_yaml(value, condition_func, modify_func, new_path)
    elif isinstance(data, list):
        for index, item in enumerate(data):
            new_path = f"{path}[{index}]"
            if condition_func(new_path, item):
                data[index] = modify_func(item)
            else:
                data[index] = modify_yaml(item, condition_func, modify_func, new_path)
    else:
        if condition_func(path, data):
            return modify_func(data)
    return data


@pytest.fixture
def test_blueprint_as_path(test_blueprint_as_dict, tmp_path) -> Callable[[str], Path]:
    """Given the name of a pre-defined blueprint, returns it as a (temporary) path to an on-disk file."""

    def _blueprint_as_path(name: str, local: bool = False) -> Path:
        blueprint_dict = test_blueprint_as_dict(name, local=local)

        # save the blueprint to a temporary path
        blueprint_filepath = tmp_path / "blueprint.yaml"
        with open(blueprint_filepath, "w") as file:
            yaml.safe_dump(blueprint_dict, file)

        return blueprint_filepath

    return _blueprint_as_path
