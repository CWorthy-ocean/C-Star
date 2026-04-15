import os
import shutil
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.exceptions import CstarExpectationFailed
from cstar.execution.file_system import RomsFileSystemManager
from cstar.orchestration.converter.converter import (
    convert_step_to_preprocessed_roms_sim,
    get_command_mapping,
    launcher_aware_app_to_cmd_map,
    register_command_mapping,
)
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import (
    Application,
    ContinueFromRequest,
    RomsMarblBlueprint,
    Step,
)
from cstar.orchestration.orchestration import Launcher, LiveStep
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import ContinuanceTransform
from cstar.orchestration.utils import ENV_CSTAR_CMD_CONVERTER_OVERRIDE


def custom_map_function(step: Step) -> str:
    """A custom step mapping function for testing purposes."""
    return f"UNIT-TEST-MAPPING: {step.name}"


@pytest.fixture
def preprocessable_roms_step(
    bp_templates_dir: Path,
    tmp_path: Path,
) -> LiveStep:
    """Create a valid step with an underlying RomsMarblBlueprint.

    Parameters
    ----------
    bp_templates_dir: Path,
        Directory containing blueprint templates. Used to create a valid blueprint
        the preprocessor will read in.
    tmp_path : Path
        Temporary path fixture for writing per-test outputs.
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    tpl_path = bp_templates_dir / "blueprint.yaml"
    tpl_content = tpl_path.read_text()
    bp_path.write_text(tpl_content)

    step_dir = tmp_path / "unit-test-work-dir"
    fs_mgr = RomsFileSystemManager(step_dir)

    # create some fake reset files
    joined_dir = fs_mgr.joined_output_dir
    joined_dir.mkdir(parents=True)
    reset_files = [
        joined_dir / "preprocessable_roms_step_rst.0.nc",
        joined_dir / "preprocessable_roms_step_rst.1.nc",
        joined_dir / "preprocessable_roms_step_rst.2.nc",
    ]
    for file in reset_files:
        file.touch()

    return LiveStep(
        name="test step",
        application="roms_marbl",
        blueprint=bp_path,
        work_dir=step_dir,
        preprocessing=[
            ContinueFromRequest(source=joined_dir),
        ],
    )


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

    step = LiveStep(
        name="test step",
        application=target_application.value,
        blueprint=bp_path,
        work_dir=tmp_path / "unit-test-work-dir",
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

    step = LiveStep(
        name="test step",
        application=target_application.value,
        blueprint=bp_path,
        work_dir=tmp_path / "unit-test-work-dir",
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


def test_convert_step_to_preprocessed_roms_sim_argument_conversion(
    preprocessable_roms_step: LiveStep,
) -> None:
    """Verify that a blueprint containing preprocessor configuration results in
    the expected extra arguments in the resulting command.

    Parameters
    ----------
    preprocessable_roms_step: LiveStep
        A `LiveStep` preconfigured with a continue-from preprocessing directive.
    """
    step = preprocessable_roms_step
    bp_path = str(step.blueprint_path)

    result = convert_step_to_preprocessed_roms_sim(step)
    assert ContinueFromRequest.SOURCE_ARG in result
    assert bp_path in result


def test_convert_step_to_preprocessed_roms_sim_no_reset_files(
    preprocessable_roms_step: LiveStep,
) -> None:
    """Verify that a blueprint containing preprocessor configuration specifying
    a directory that does not include any reset files results in an exception.

    Parameters
    ----------
    preprocessable_roms_step: LiveStep
        A `LiveStep` preconfigured with a continue-from preprocessing directive.
    """
    step = preprocessable_roms_step

    # delete any mocked reset files to trigger validation failure
    assert step.work_dir, "Fixture failed to set work_dir on step"
    fsm = RomsFileSystemManager(step.work_dir)
    shutil.rmtree(fsm.joined_output_dir, ignore_errors=True)
    fsm.joined_output_dir.mkdir(parents=True)

    assert not step.blueprint_overrides, "Empty overrides expected"
    assert step.work_dir, "Ensure fixture sets workdir"

    request = ContinueFromRequest(source=fsm.joined_output_dir)

    with pytest.raises(CstarExpectationFailed, match="No reset files"):
        _ = ContinuanceTransform(request)


def test_continuance_transform(
    preprocessable_roms_step: LiveStep,
) -> None:
    step = preprocessable_roms_step
    assert not step.blueprint_overrides, "Empty overrides expected"
    assert step.work_dir, "Ensure fixture sets workdir"

    bp_path_before = step.blueprint_path
    fsm = RomsFileSystemManager(step.work_dir)

    request = ContinueFromRequest(source=fsm.joined_output_dir)
    trx = ContinuanceTransform(request)

    transformed_step = next(iter(trx(step)), None)
    assert transformed_step, "Transform didn't return a transformed step"

    # confirm overrides aren empty after the transformation is applied
    assert not transformed_step.blueprint_overrides

    # confirm the step is transformed
    bp_path_after = transformed_step.blueprint_path
    assert bp_path_after != bp_path_before, "New step must reference a new blueprint"

    bp = deserialize(bp_path_after, RomsMarblBlueprint)
    assert bp.initial_conditions.data, "data list is unexpectedly empty"

    # confirm the location has been swapped to match the fixture
    d0 = bp.initial_conditions.data[0]
    assert "preprocessable_roms_step" in str(d0.location)
