import os
from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration.converter.converter import (
    app_to_cmd_map,
    get_command_mapping,
    register_command_mapping,
)
from cstar.orchestration.models import Application, Step
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.utils import ENV_CSTAR_CMD_CONVERTER_OVERRIDE


def custom_map_function(step: Step) -> str:
    """A custom step mapping function for testing purposes."""
    return f"UNIT-TEST-MAPPING: {step.name}"


@pytest.mark.parametrize(
    ("target_application"),
    [
        Application.ROMS_MARBL,
        Application.SLEEP,
    ],
)
def test_converter_defaults(
    tmp_path: Path,
    target_application: Application,
) -> None:
    """Verify that the registration of a converter is not required for the default apps.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs.
    target_application: Application
        The application type to locate a mapping for
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    step = LiveStep(
        name="test step",
        application=target_application.value,
        blueprint=bp_path,
        work_dir=tmp_path / "unit-test-work-dir",
    )

    mapped_fn = get_command_mapping(target_application)

    # confirm a mapping function was returned
    assert mapped_fn(step)


@pytest.mark.parametrize(
    ("target_application"),
    [Application.ROMS_MARBL, Application.SLEEP],
)
def test_converter_registration(
    tmp_path: Path,
    target_application: Application,
) -> None:
    """Verify that the registration of a converter is not required for the default apps.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs
    target_application: Application
        The application type to locate a mapping for
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    step = LiveStep(
        name="test step",
        application=target_application.value,
        blueprint=bp_path,
        work_dir=tmp_path / "unit-test-work-dir",
    )

    # ensure registration doesn't persist after test completes.
    with mock.patch.dict(app_to_cmd_map, {}):
        original_fn = get_command_mapping(target_application)

        register_command_mapping(target_application, custom_map_function)

        mapped_fn = get_command_mapping(target_application)

    # confirm the function is a mapping function
    assert mapped_fn(step)

    # confirm the newly registered function was returned
    assert mapped_fn != original_fn
    assert mapped_fn == custom_map_function


@pytest.mark.parametrize(
    ("target_application"),
    [Application.ROMS_MARBL, Application.SLEEP],
)
def test_converter_override_invalid(
    tmp_path: Path,
    target_application: Application,
) -> None:
    """Verify that an invalid converter override results in an error.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs
    target_application: Application
        The application type to locate a mapping for
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    with (
        mock.patch.dict(os.environ, {ENV_CSTAR_CMD_CONVERTER_OVERRIDE: "DNE-key"}),
        pytest.raises(ValueError) as error,
    ):
        _ = get_command_mapping(target_application)

    assert ENV_CSTAR_CMD_CONVERTER_OVERRIDE in str(error)


@pytest.mark.parametrize(
    ("target_application", "overridden_target"),
    [
        (Application.ROMS_MARBL, Application.SLEEP),
        (Application.SLEEP, Application.ROMS_MARBL),
    ],
)
def test_converter_override_capability(
    tmp_path: Path,
    target_application: Application,
    overridden_target: Application,
) -> None:
    """Verify that the the override specified in an environment variable takes precedence
    over defaults.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs
    target_application: Application
        The application type to locate a mapping for
    overridden_target: Application
        The key to use to locate the override
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    original_fn = get_command_mapping(target_application)

    with mock.patch.dict(
        os.environ,
        {ENV_CSTAR_CMD_CONVERTER_OVERRIDE: overridden_target.value},
    ):
        overridden_fn = get_command_mapping(target_application)

    assert original_fn != overridden_fn
