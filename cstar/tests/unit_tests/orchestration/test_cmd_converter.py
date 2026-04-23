import os
import shutil
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.exceptions import CstarExpectationFailed
from cstar.entrypoint.utils import ARG_DIRECTIVES_URI_LONG
from cstar.execution.file_system import RomsFileSystemManager
from cstar.orchestration.converter.converter import (
    app_to_cmd_map,
    get_command_mapping,
    register_command_mapping,
)
from cstar.orchestration.models import (
    Application,
    RomsMarblBlueprint,
    Step,
)
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import ContinuanceTransform
from cstar.orchestration.utils import ENV_CSTAR_CMD_CONVERTER_OVERRIDE


def custom_map_function(step: Step) -> str:
    """A custom step mapping function for testing purposes."""
    return f"UNIT-TEST-MAPPING: {step.name}"


@pytest.mark.parametrize(
    ("target_application"),
    [
        Application.ROMS_MARBL,
        Application.SLEEP,
        Application.HELLO_WORLD,
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
        application=target_application,
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
        application=target_application,
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
    target_application: str,
    overridden_target: str,
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
        {ENV_CSTAR_CMD_CONVERTER_OVERRIDE: overridden_target},
    ):
        overridden_fn = get_command_mapping(target_application)

    assert original_fn != overridden_fn


@pytest.mark.asyncio
async def test_converter_hello_world(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify that the command converter produces a working CLI
    command.

    Parameters
    ----------
    tmp_path : Path
        Temporary output location for writing the test blueprint.
    hello_world_bp_path : Path
        Path to the hello world blueprint.
    """
    work_dir = tmp_path / "work"

    step = LiveStep(
        name=f"{__name__}",
        application=Application.HELLO_WORLD,
        blueprint=hello_world_bp_path.as_posix(),
        work_dir=work_dir,
    )

    # find the registered command converter for this application and launcher type
    cmd_converter = get_command_mapping(Application.HELLO_WORLD)
    assert cmd_converter is not None, "Command converter not found"

    # use the converter function to generate the command
    command = cmd_converter(step)

    # confirm the retrieved converter produces the output expected
    # from the function: convert_step_to_blueprint_run_command
    assert "cstar blueprint run" in command
    assert str(hello_world_bp_path) in command


def test_convert_step_with_directives(
    preprocessable_roms_livestep: LiveStep,
) -> None:
    """Verify that a Step containing directives in it's configuration results in
    the directive file being created and passed as an argument in the command.

    Parameters
    ----------
    preprocessable_roms_livestep: LiveStep
        A `LiveStep` preconfigured with a continue-from preprocessing directive.
    """
    step = preprocessable_roms_livestep
    bp_path = str(step.blueprint_path)

    cmd_converter = get_command_mapping(Application.ROMS_MARBL)
    result = cmd_converter(step)

    # confirm the parameter is sent
    assert ARG_DIRECTIVES_URI_LONG in result

    # confirm the directive path exists
    dir_path = result.split(ARG_DIRECTIVES_URI_LONG)[1].split(" ", maxsplit=1)[0]
    assert bp_path in result, "The blueprint path should be unchanged"
    assert Path(dir_path).exists()


def test_convert_step_to_preprocessed_roms_sim_no_reset_files(
    preprocessable_roms_livestep: LiveStep,
) -> None:
    """Verify that a Step containing directives in it's configuration specifying
    a directory that does not include any reset files results in an exception.

    Parameters
    ----------
    preprocessable_roms_livestep: LiveStep
        A `LiveStep` preconfigured with a continue-from preprocessing directive.
    """
    step = preprocessable_roms_livestep

    # delete any mocked reset files to trigger validation failure
    assert step.work_dir, "Fixture failed to set work_dir on step"
    fsm = RomsFileSystemManager(step.work_dir)
    shutil.rmtree(fsm.joined_output_dir, ignore_errors=True)
    fsm.joined_output_dir.mkdir(parents=True)

    assert not step.blueprint_overrides, "Empty overrides expected"
    assert step.work_dir, "Ensure fixture sets workdir"

    config = {"path": fsm.joined_output_dir}

    with pytest.raises(CstarExpectationFailed, match="No reset files"):
        _ = ContinuanceTransform(config)


def test_continuance_transform(
    preprocessable_roms_livestep: LiveStep,
) -> None:
    """Verify that the `ContinuanceTransform` materially modifies blueprint content
    to include a path to an initial conditions file located in the directory
    passed to the transform.

    Parameters
    ----------
    preprocessable_roms_livestep: LiveStep
        A `LiveStep` preconfigured with a continue-from preprocessing directive.
    """
    step = preprocessable_roms_livestep
    assert not step.blueprint_overrides, "Empty overrides expected"
    assert step.work_dir, "Ensure fixture sets workdir"

    bp_path_before = step.blueprint_path
    fsm = RomsFileSystemManager(step.work_dir)

    original_bp = deserialize(bp_path_before, RomsMarblBlueprint)
    assert original_bp.initial_conditions.data, "data list is unexpectedly empty"
    original_ic = original_bp.initial_conditions.data[0].location

    trx = ContinuanceTransform(config={"path": str(fsm.joined_output_dir)})

    transformed_step = next(iter(trx(step)), None)
    assert transformed_step, "Transform didn't return a transformed step"

    # confirm overrides aren empty after the transformation is applied
    assert not transformed_step.blueprint_overrides

    # confirm the blueprint path is changed
    bp_path_after = transformed_step.blueprint_path
    assert bp_path_after != bp_path_before, "New step must reference a new blueprint"

    # confirm the path includes a suffix specified by the transform
    assert ContinuanceTransform.suffix() in str(bp_path_after)

    bp = deserialize(bp_path_after, RomsMarblBlueprint)
    assert bp.initial_conditions.data, "data list is unexpectedly empty"

    # confirm the location has been swapped to match the fixture
    transformed_ic = Path(str(bp.initial_conditions.data[0].location))
    assert str(original_ic) != str(transformed_ic)

    # confirm the path has been expanded and resolved
    assert transformed_ic.expanduser().resolve() == transformed_ic
