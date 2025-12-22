from datetime import datetime
from pathlib import Path

import pytest

from cstar.orchestration.models import RomsMarblBlueprint, Workplan
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import OverrideTransform


@pytest.fixture
def test_wp_path(tmp_path: Path) -> Path:
    """Default path for writing a workplan into the test output directory."""
    return tmp_path / "test_wp.yaml"


@pytest.fixture
def test_bp_path(tmp_path: Path) -> Path:
    """Default path for writing a blueprint into the test output directory."""
    return tmp_path / "test_bp.yaml"


@pytest.fixture
def test_output_dir(tmp_path: Path) -> Path:
    """Default path for writing outputs from a blueprint."""
    return tmp_path / "working_output_dir"


@pytest.fixture
def test_output_dir_override(tmp_path: Path) -> Path:
    """Default path for writing outputs from a blueprint."""
    return tmp_path / "working_output_dir_override"


@pytest.fixture
def step_overiding_wp(
    test_wp_path: Path,
    test_bp_path: Path,
    test_output_dir: Path,
    test_output_dir_override: Path,
) -> Workplan:
    """Copy a template containing blueprint overrides to the test tmp_path.

    The step specifies overrides for the blueprint fields:
    - output_dir (original value "/other_dir")
    - start_date (original value "")
    - end_date (original value "")
    """
    default_blueprint_path = (
        "~/code/cstar/cstar/additional_files/templates/blueprint.yaml"
    )

    bp_tpl_path = (
        Path(__file__).parent.parent.parent.parent
        / "additional_files/templates/bp"
        / "blueprint.yaml"
    )

    bp_content = bp_tpl_path.read_text()
    bp_content = bp_content.replace(
        "output_dir: .", f"output_dir: {test_output_dir.as_posix()}"
    )
    test_bp_path.write_text(bp_content)

    wp_tpl_path = (
        Path(__file__).parent.parent.parent.parent
        / "additional_files/templates/wp"
        / "wp_with_bp_overrides.yaml"
    )
    wp_content = wp_tpl_path.read_text()
    wp_content = wp_content.replace(default_blueprint_path, test_bp_path.as_posix())

    # replace the default so no test outputs leak into working directories
    wp_content = wp_content.replace("/other_dir", test_output_dir_override.as_posix())
    test_wp_path.write_text(wp_content)
    wp = deserialize(test_wp_path, Workplan)

    return wp


# def test_registry() -> None:
#     """Verify that the override transform is not registered by default."""
#     transforms = get_transforms(Application.ROMS_MARBL.value)

#     assert not any(isinstance(tx, OverrideTransform) for tx in transforms)


def test_override_transform(
    step_overiding_wp: Workplan,
    tmp_path: Path,
    test_output_dir: Path,
    test_bp_path: Path,
    test_output_dir_override: Path,
) -> None:
    """Verify that the OverrideTransform overwrites values in the blueprint.

    Parameters
    ----------
    step_overiding_wp : Workplan
        A workplan copied from a template with paths referencing tmp_path.
    tmp_path : Path
        The temporary path for writing test files.
    test_output_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    test_bp_path : Path
        The path to the populated blueprint template for this test.
    """
    transform = OverrideTransform()
    step = step_overiding_wp.steps[0]

    steps = transform(step)
    transformed = list(steps)[0]

    # confirm a attribute of the blueprint is changed (bp.blueprint_path)
    dir_og = test_output_dir
    exp_dir = test_output_dir_override

    # confirm a new blueprint was created.
    assert Path(step.blueprint_path) != Path(transformed.blueprint_path)

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    # confirm nested attributes (bp.runtime_params.xxx)) is changed
    assert Path(bp_old.runtime_params.output_dir) == dir_og
    assert Path(bp_new.runtime_params.output_dir) == exp_dir

    assert bp_old.runtime_params.start_date == datetime(2020, 1, 1)
    assert bp_old.runtime_params.end_date == datetime(2021, 1, 1)

    assert bp_new.runtime_params.start_date == datetime(2010, 1, 15)
    assert bp_new.runtime_params.end_date == datetime(2010, 6, 25)

    # confirm that deeply nested attributes are changed (bp.initial_conditions.data.location)
    assert bp_old.initial_conditions.data[0].location == "http://mockdoc.com/grid"
    assert bp_new.initial_conditions.data[0].location == "http://elsewhere.com/grid2"

    # confirm that after the overrides are applied, they are removed from the step.
    assert not transformed.blueprint_overrides

    # confirm some other attribute of the step is unchanged
    assert bp_old.initial_conditions.data[0].partitioned
    assert bp_new.initial_conditions.data[0].partitioned


# def test_workplan_transformer_overrides(
#     step_overiding_wp: Workplan, tmp_path: Path
# ) -> None:
#     """Verify that using the override tranform via the WorkplanTransformer works as expected."""
#     step = step_overiding_wp.steps[0]

#     transformer = WorkplanTransformer(step_overiding_wp, OverrideTransform())

#     with mock.patch.dict(os.environ, {"CSTAR_FF_ORC_TRANSFORM": "1"}):
#         workplan = transformer.apply()

#     transformed = workplan.steps[0]

#     og_dir = Path(".")
#     mod_dir = tmp_path / "overridden_dir"

#     bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
#     bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

#     assert Path(bp_old.runtime_params.output_dir) == og_dir
#     assert Path(bp_new.runtime_params.output_dir) == mod_dir

#     og_ic_location = "http://mockdoc.com/grid"
#     mod_ic_location = "http://elsewhere.com/grid"

#     assert bp_old.initial_conditions.data[0].location == og_ic_location
#     assert bp_new.initial_conditions.data[0].location == mod_ic_location

#     og_sd = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
#     mod_sd = datetime.strptime("2010-01-15 00:00:00", "%Y-%m-%d %H:%M:%S")

#     assert bp_old.runtime_params.start_date == og_sd
#     assert bp_new.runtime_params.start_date == mod_sd

#     og_ed = datetime.strptime("2021-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
#     mod_ed = datetime.strptime("2010-06-25 00:00:00", "%Y-%m-%d %H:%M:%S")

#     assert bp_old.runtime_params.end_date == og_ed
#     assert bp_new.runtime_params.end_date == mod_ed
