from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration.models import Application, RomsMarblBlueprint, Workplan
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import (
    OverrideTransform,
    RomsMarblTimeSplitter,
    get_transforms,
)


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
    wp_templates_dir: Path,
    bp_templates_dir: Path,
    default_blueprint_path: str,
) -> Workplan:
    """Copy a template containing blueprint overrides to the test tmp_path.

    The step specifies overrides for the blueprint fields:
    - output_dir (original value "/other_dir")
    - start_date (original value "")
    - end_date (original value "")

    Parameters
    ----------
    test_wp_path : Path
        Fixture returning default write location for a workplan file
    test_bp_path : Path
        Fixture returning default write location for a blueprint file
    test_output_dir_override : Path
        Fixture returning
    test_output_dir : Path
        Fixture returning the path to a directory for containing orchestration test files
    wp_templates_dir : Path
        Fixture returning the path to the directory containing workplan template files
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files
    default_blueprint_path : str
        Fixture returning the default blueprint path contained in template workplans

    Returns
    -------
    Workplan
    """
    bp_tpl_path = bp_templates_dir / "blueprint.yaml"

    bp_content = bp_tpl_path.read_text()
    bp_content = bp_content.replace(
        "output_dir: .", f"output_dir: {test_output_dir.as_posix()}"
    )
    test_bp_path.write_text(bp_content)

    wp_tpl_path = wp_templates_dir / "wp_with_bp_overrides.yaml"
    wp_content = wp_tpl_path.read_text()
    wp_content = wp_content.replace(default_blueprint_path, test_bp_path.as_posix())

    # replace the default so no test outputs leak into working directories
    wp_content = wp_content.replace("/other_dir", test_output_dir_override.as_posix())
    test_wp_path.write_text(wp_content)
    wp = deserialize(test_wp_path, Workplan)

    return wp


def test_get_transforms() -> None:
    """Confirm that registered transforms are returned via `get_transforms`"""
    with mock.patch.dict(
        "cstar.orchestration.transforms.TRANSFORMS",
        {Application.ROMS_MARBL.value: [OverrideTransform]},
        clear=True,
    ):
        transforms = get_transforms(Application.ROMS_MARBL.value)

    assert not any(isinstance(tx, OverrideTransform) for tx in transforms)

    with mock.patch.dict(
        "cstar.orchestration.transforms.TRANSFORMS",
        {Application.ROMS_MARBL.value: [RomsMarblTimeSplitter()]},
        clear=True,
    ):
        transforms = get_transforms(Application.ROMS_MARBL.value)

    assert any(isinstance(tx, RomsMarblTimeSplitter) for tx in transforms)


def test_get_transforms_empty() -> None:
    """Confirm that `get_transforms` does not blow up when requesting transforms
    for an application that has no transforms registered.
    """
    with mock.patch.dict("cstar.orchestration.transforms.TRANSFORMS", {}, clear=True):
        transforms = get_transforms(Application.ROMS_MARBL.value)

    assert not any(isinstance(tx, OverrideTransform) for tx in transforms)
    assert not any(isinstance(tx, RomsMarblTimeSplitter) for tx in transforms)


def test_override_transform(
    step_overiding_wp: Workplan,
    test_output_dir: Path,
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
    dir_orig = test_output_dir
    exp_dir = test_output_dir_override

    # confirm a new blueprint was created.
    assert Path(step.blueprint_path) != Path(transformed.blueprint_path)

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    # confirm nested attributes (bp.runtime_params.xxx)) is changed
    assert Path(bp_old.runtime_params.output_dir) == dir_orig
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


def test_override_transform_system_precedence(
    tmp_path: Path,
    step_overiding_wp: Workplan,
    test_output_dir: Path,
) -> None:
    """Verify that system-level overrides passed to the transform override
    values specified in the workplan.

    Parameters
    ----------
    step_overiding_wp : Workplan
        A workplan copied from a template with paths referencing tmp_path.
    tmp_path : Path
        The temporary path for writing test files.
    test_output_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    """
    sys_od = tmp_path / "system_output_dir"
    system_od_override = {"runtime_params": {"output_dir": sys_od.as_posix()}}

    transform = OverrideTransform(sys_overrides=system_od_override)
    step = step_overiding_wp.steps[0]

    steps = transform(step)
    transformed = list(steps)[0]

    # confirm a attribute of the blueprint is changed (bp.blueprint_path)
    dir_orig = test_output_dir

    # confirm a new blueprint was created.
    assert Path(step.blueprint_path) != Path(transformed.blueprint_path)

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    # confirm that even though an output_dir override was applied, the
    # system level override was applied last.
    assert Path(bp_old.runtime_params.output_dir) == dir_orig
    assert Path(bp_new.runtime_params.output_dir) == sys_od
