from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration.models import Application, RomsMarblBlueprint, Workplan
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import (
    OverrideTransform,
    WorkplanTransformer,
    get_transforms,
)


@pytest.fixture
def step_overiding_wp(tmp_path: Path) -> Workplan:
    """Generate a workplan with a blueprint overridden by a step."""
    default_blueprint_path = (
        "~/code/cstar/cstar/additional_files/templates/blueprint.yaml"
    )

    bp_tpl_path = (
        Path(__file__).parent.parent.parent.parent
        / "additional_files/templates/bp"
        / "blueprint.yaml"
    )

    bp_path = tmp_path / "og_blueprint.yaml"
    bp_content = bp_tpl_path.read_text()
    bp_path.write_text(bp_content)

    wp_tpl_path = (
        Path(__file__).parent.parent.parent.parent
        / "additional_files/templates/wp"
        / "wp_with_bp_overrides.yaml"
    )
    wp_content = wp_tpl_path.read_text()
    wp_content = wp_content.replace(default_blueprint_path, bp_path.as_posix())
    wp_content = wp_content.replace(
        "/other_dir", (tmp_path / "overridden_dir").as_posix()
    )
    wp_path = tmp_path / "step_overriding_wp.yaml"
    wp_path.write_text(wp_content)
    wp = deserialize(wp_path, Workplan)

    return wp


def test_registry() -> None:
    """Verify that the override transform is not registered by default."""
    transforms = get_transforms(Application.ROMS_MARBL.value)

    assert not any(isinstance(tx, OverrideTransform) for tx in transforms)


def test_direct_override(step_overiding_wp: Workplan, tmp_path: Path) -> None:
    """Verify that using the override tranform directly works as expected."""
    with mock.patch.dict(
        "cstar.orchestration.transforms.TRANSFORMS",
        {Application.ROMS_MARBL.value: [OverrideTransform()]},
        clear=True,
    ):
        transforms = get_transforms(Application.ROMS_MARBL.value)
        transform = next(
            item for item in transforms if isinstance(item, OverrideTransform)
        )
    step = step_overiding_wp.steps[0]

    steps = transform(step)
    transformed = list(steps)[0]

    og_dir = Path(".")
    mod_dir = tmp_path / "overridden_dir"

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    assert Path(bp_old.runtime_params.output_dir) == og_dir
    assert Path(bp_new.runtime_params.output_dir) == mod_dir


def test_workplan_transformer_overrides(
    step_overiding_wp: Workplan, tmp_path: Path
) -> None:
    """Verify that using the override tranform via the WorkplanTransformer works as expected."""
    step = step_overiding_wp.steps[0]

    transformer = WorkplanTransformer(step_overiding_wp, OverrideTransform())
    workplan = transformer.apply()

    transformed = workplan.steps[0]

    og_dir = Path(".")
    mod_dir = tmp_path / "overridden_dir"

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    assert Path(bp_old.runtime_params.output_dir) == og_dir
    assert Path(bp_new.runtime_params.output_dir) == mod_dir

    og_ic_location = "http://mockdoc.com/grid"
    mod_ic_location = "http://elsewhere.com/grid"

    assert bp_old.initial_conditions.data[0].location == og_ic_location
    assert bp_new.initial_conditions.data[0].location == mod_ic_location

    og_sd = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    mod_sd = datetime.strptime("2010-01-15 00:00:00", "%Y-%m-%d %H:%M:%S")

    assert bp_old.runtime_params.start_date == og_sd
    assert bp_new.runtime_params.start_date == mod_sd

    og_ed = datetime.strptime("2021-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    mod_ed = datetime.strptime("2010-06-25 00:00:00", "%Y-%m-%d %H:%M:%S")

    assert bp_old.runtime_params.end_date == og_ed
    assert bp_new.runtime_params.end_date == mod_ed
