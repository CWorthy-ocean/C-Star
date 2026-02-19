import os
from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration.converter.converter import (
    get_command_mapping,
    launcher_aware_app_to_cmd_map,
    register_command_mapping,
)
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import Application, Step
from cstar.orchestration.orchestration import Launcher
from cstar.orchestration.utils import ENV_CSTAR_CMD_CONVERTER_OVERRIDE


def custom_map_function(step: Step) -> str:
    """A custom step mapping function for testing purposes."""
    return f"UNIT-TEST-MAPPING: {step.name}"


@pytest.mark.parametrize(
    ("target_application", "launcher_type"),
    [
        (Application.ROMS_MARBL, LocalLauncher),
        (Application.ROMS_MARBL, SlurmLauncher),
        (Application.SLEEP, LocalLauncher),
        (Application.SLEEP, SlurmLauncher),
    ],
)
def test_converter_defaults(
    tmp_path: Path,
    target_application: Application,
    launcher_type: type[Launcher],
) -> None:
    """Verify that the registration of a converter is not required for the default apps.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs.
    target_application: Application
        The application type to locate a mapping for
    launcher_type: type[Launcher]
        The type of launcher that will consume the command
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    step = Step(
        name="test step",
        application=target_application.value,
        blueprint=bp_path,
    )

    mapped_fn = get_command_mapping(target_application, launcher_type)

    # confirm a mapping function was returned
    assert mapped_fn(step)


@pytest.mark.parametrize(
    ("target_application", "launcher_type"),
    [
        (Application.ROMS_MARBL, LocalLauncher),
        (Application.ROMS_MARBL, SlurmLauncher),
        (Application.SLEEP, LocalLauncher),
        (Application.SLEEP, SlurmLauncher),
    ],
)
def test_converter_registration(
    tmp_path: Path,
    target_application: Application,
    launcher_type: type[Launcher],
) -> None:
    """Verify that the registration of a converter is not required for the default apps.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs
    target_application: Application
        The application type to locate a mapping for
    launcher_type: type[Launcher]
        The type of launcher that will consume the command
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    step = Step(
        name="test step",
        application=target_application.value,
        blueprint=bp_path,
    )

    # ensure registration doesn't persist after test completes.
    with mock.patch.dict(launcher_aware_app_to_cmd_map[launcher_type], {}):
        original_fn = get_command_mapping(target_application, launcher_type)

        register_command_mapping(target_application, launcher_type, custom_map_function)

        mapped_fn = get_command_mapping(target_application, launcher_type)

    # confirm the function is a mapping function
    assert mapped_fn(step)

    # confirm the newly registered function was returned
    assert mapped_fn != original_fn
    assert mapped_fn == custom_map_function


@pytest.mark.parametrize(
    ("target_application", "launcher_type"),
    [
        (Application.ROMS_MARBL, LocalLauncher),
        (Application.ROMS_MARBL, SlurmLauncher),
        (Application.SLEEP, LocalLauncher),
        (Application.SLEEP, SlurmLauncher),
    ],
)
def test_converter_override_invalid(
    tmp_path: Path,
    target_application: Application,
    launcher_type: type[Launcher],
) -> None:
    """Verify that an invalid converter override results in an error.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs
    target_application: Application
        The application type to locate a mapping for
    launcher_type: type[Launcher]
        The type of launcher that will consume the command
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    with (
        mock.patch.dict(os.environ, {ENV_CSTAR_CMD_CONVERTER_OVERRIDE: "DNE-key"}),
        pytest.raises(ValueError) as error,
    ):
        _ = get_command_mapping(target_application, launcher_type)

    assert ENV_CSTAR_CMD_CONVERTER_OVERRIDE in str(error)


@pytest.mark.parametrize(
    ("target_application", "overridden_target", "launcher_type"),
    [
        (Application.ROMS_MARBL, Application.SLEEP, LocalLauncher),
        (Application.ROMS_MARBL, Application.SLEEP, SlurmLauncher),
        (Application.SLEEP, Application.ROMS_MARBL, LocalLauncher),
        (Application.SLEEP, Application.ROMS_MARBL, SlurmLauncher),
    ],
)
def test_converter_override_capability(
    tmp_path: Path,
    target_application: Application,
    overridden_target: Application,
    launcher_type: type[Launcher],
) -> None:
    """Verify that the the override specified in an environment variable takes precedence
    over defaults.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs
    target_application: Application
        The application type to locate a mapping for
    target_application: Application
        The application type to locate a mapping for
    launcher_type: type[Launcher]
        The type of launcher that will consume the command
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    original_fn = get_command_mapping(target_application, launcher_type)

    with mock.patch.dict(
        os.environ,
        {ENV_CSTAR_CMD_CONVERTER_OVERRIDE: overridden_target.value},
    ):
        overridden_fn = get_command_mapping(target_application, launcher_type)

    assert original_fn != overridden_fn
