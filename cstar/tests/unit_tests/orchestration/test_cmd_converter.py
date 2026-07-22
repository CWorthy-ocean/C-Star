import shutil
from pathlib import Path

import pytest

from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.applications.roms_marbl.transforms import ContinuanceDirective
from cstar.entrypoint.utils import ARG_DIRECTIVES_URI_LONG
from cstar.execution.file_system import RomsFileSystemManager
from cstar.orchestration.adapter import (
    StepToPlaceholderAdapter,
    StepToRunRequestAdapter,
)
from cstar.orchestration.formatting import RunRequestCommandFormatter
from cstar.orchestration.models import (
    Application,
    Step,
)
from cstar.orchestration.orchestration import LiveStep, RunRequest
from cstar.orchestration.serialization import deserialize


def custom_map_function(step: Step) -> RunRequest:
    """A custom step mapping function for testing purposes."""
    return RunRequest(command=["UNIT-TEST-MAPPING", step.name])


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
        working_dir=tmp_path / "unit-test-work-dir",
    )

    # confirm a command was returned
    adapter = StepToRunRequestAdapter(step)
    assert adapter.adapt()


@pytest.mark.parametrize(
    ("target_application"),
    [
        Application.ROMS_MARBL,
        Application.SLEEP,
        Application.HELLO_WORLD,
    ],
)
def test_converter_steptoplaceholderadapter(
    tmp_path: Path,
    target_application: Application,
) -> None:
    """Verify that the placeholder adapter produces a placeholder script
    instead of the original command.

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
        working_dir=tmp_path / "unit-test-work-dir",
    )

    adapter = StepToPlaceholderAdapter(step)
    ph_request = adapter.adapt()

    # confirm placeholder executes an alternative script
    ph_command = RunRequestCommandFormatter().format(ph_request)
    assert ph_command == f"sh {step.fsm.run_dir / adapter.SCRIPTFILE_NAME}"

    # confirm the placeholder script was written
    ph_path = Path(ph_command.split(" ", maxsplit=1)[1])
    assert ph_path.exists()

    # confirm the original script is documented in placeholder
    cmd_adapter = StepToRunRequestAdapter(step)
    cmd_request = cmd_adapter.adapt()
    cmd = RunRequestCommandFormatter().format(cmd_request)

    ph_content = ph_path.read_text()
    assert f"replacing: {cmd}" in ph_content


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

    result = StepToRunRequestAdapter(step).adapt()

    # confirm the parameter is sent
    assert ARG_DIRECTIVES_URI_LONG in result.command

    # confirm the run request includes the directives
    cmd = RunRequestCommandFormatter().format(result)

    dir_path = cmd.split(ARG_DIRECTIVES_URI_LONG)[1].split(" ", maxsplit=1)[0]
    assert bp_path in cmd, "The blueprint path should be unchanged"
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
    assert step.working_dir, "Fixture failed to set `working_dir` on step"
    fsm = RomsFileSystemManager(step.working_dir)
    shutil.rmtree(fsm.joined_output_dir, ignore_errors=True)
    fsm.joined_output_dir.mkdir(parents=True)

    assert not step.blueprint_overrides, "Empty overrides expected"
    assert step.working_dir, "Ensure fixture sets workdir"

    config = {ContinuanceDirective.KEY_PATH: fsm.joined_output_dir}

    with pytest.raises(FileNotFoundError, match="No restart files"):
        _ = ContinuanceDirective(config)


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
    assert step.working_dir, "Ensure fixture sets workdir"

    bp_path_before = step.blueprint_path
    fsm = RomsFileSystemManager(step.working_dir)

    original_bp = deserialize(bp_path_before, RomsMarblBlueprint)
    assert original_bp.initial_conditions.data, "data list is unexpectedly empty"
    original_ic = original_bp.initial_conditions.data[0].location

    trx = ContinuanceDirective(
        config={ContinuanceDirective.KEY_PATH: str(fsm.joined_output_dir)}
    )

    transformed_step = next(iter(trx(step)), None)
    assert transformed_step, "Transform didn't return a transformed step"

    # confirm overrides aren empty after the transformation is applied
    assert not transformed_step.blueprint_overrides

    # confirm the blueprint path is changed
    bp_path_after = transformed_step.blueprint_path
    assert bp_path_after != bp_path_before, "New step must reference a new blueprint"

    # confirm the path includes a suffix specified by the transform
    assert ContinuanceDirective.suffix() in str(bp_path_after)

    bp = deserialize(bp_path_after, RomsMarblBlueprint)
    assert bp.initial_conditions.data, "data list is unexpectedly empty"

    # confirm the location has been swapped to match the fixture
    transformed_ic = Path(str(bp.initial_conditions.data[0].location))
    assert str(original_ic) != str(transformed_ic)

    # confirm the path has been expanded and resolved
    assert transformed_ic.expanduser().resolve() == transformed_ic
