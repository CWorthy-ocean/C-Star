import os
import typing as t
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_RUNID, FLAG_OFF
from cstar.base.feature import ENV_FF_ORCH_TRX_TIMESPLIT
from cstar.orchestration.models import Application, RomsMarblBlueprint, Step, Workplan
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import (
    OverrideTransform,
    RomsMarblTimeSplitter,
    TemplateFillTransform,
    WorkplanTransformer,
    get_transforms,
)

if t.TYPE_CHECKING:
    from cstar.orchestration.orchestration import LiveStep


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

    with mock.patch.dict(os.environ, {ENV_CSTAR_RUNID: "12345"}, clear=True):
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

    with mock.patch.dict(os.environ, {ENV_CSTAR_RUNID: "12345"}, clear=True):
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


def test_workplan_transformer_applies_output_dir_overrides(
    step_overiding_wp: Workplan,
    test_output_dir: Path,
    test_output_dir_override: Path,
) -> None:
    """Verify that the workplan transformer applies a transform to override
    the output directory for all steps.

    Parameters
    ----------
    step_overiding_wp : Workplan
        A workplan copied from a template with paths referencing tmp_path.
    test_output_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    test_output_dir_override : Path
        An override that was already on the step before the WP transformer is invoked
    """
    wp_transformer = WorkplanTransformer(step_overiding_wp, OverrideTransform())
    original_bp_path = step_overiding_wp.steps[0].blueprint_path
    step_orig: Step = step_overiding_wp.steps[0]
    bp_orig = deserialize(step_orig.blueprint_path, RomsMarblBlueprint)

    with mock.patch.dict(
        os.environ,
        {ENV_CSTAR_RUNID: "12345", ENV_FF_ORCH_TRX_TIMESPLIT: FLAG_OFF},
        clear=True,
    ):
        wp_trx = wp_transformer.apply()

    step_trx = t.cast("LiveStep", wp_trx.steps[0])

    # sanity-check expectations for the original blueprint output location and override
    dir_orig = bp_orig.runtime_params.output_dir
    original_override = t.cast(
        "str",
        step_orig.blueprint_overrides["runtime_params"]["output_dir"],  # type: ignore[reportArgumentType,index,call-overload]
    )
    assert dir_orig == test_output_dir
    assert original_override == str(test_output_dir_override)

    # confirm no override remains on the step
    assert "runtime_params" not in step_trx.blueprint_overrides

    # confirm the transformed step includes an updated blueprint path.
    trx_bp_path = step_trx.blueprint_path
    assert str(trx_bp_path) != str(original_bp_path)

    # confirm the original and updated blueprint have different output directories
    blueprint = deserialize(trx_bp_path, RomsMarblBlueprint)
    assert blueprint.runtime_params.output_dir != dir_orig

    # confirm the workplan override took precedence over any original override or output_dir
    exp_dir = step_trx.fsm.root
    assert blueprint.runtime_params.output_dir == exp_dir
    assert blueprint.runtime_params.output_dir != dir_orig
    assert blueprint.runtime_params.output_dir != original_override


@pytest.fixture
def live_step_with_templates(tmp_path: Path) -> LiveStep:
    """A minimal LiveStep whose blueprint_overrides contain template placeholders."""
    bp = tmp_path / "bp.yaml"
    bp.touch()
    step = Step(
        name="fill-step",
        application="sleep",
        blueprint=bp.as_posix(),
        blueprint_overrides={
            "input_dir": "{{base_dir}}/input",
            "output_dir": "{{path: upstream}}/output",
            "variables": ["{{var1}}", "{{var2}}"],
            "nested": {"key": "{{base_dir}}"},
            "count": 42,
        },
    )
    return LiveStep.from_step(step)


def test_template_fill_suffix() -> None:
    """Verify the transform reports the expected suffix."""
    assert TemplateFillTransform.suffix() == "tmpl"


def test_template_fill_variable_substitution(
    live_step_with_templates: LiveStep,
) -> None:
    """Plain {{name}} tokens are replaced using the variable resolver."""
    variables = {"base_dir": "/data/base", "var1": "ALK", "var2": "pH_3D"}
    transform = TemplateFillTransform(variable_resolver=variables.__getitem__)

    # only fill overrides that don't contain path: tokens
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"input_dir": "{{base_dir}}/input"}},
    )
    (result,) = transform(step)

    assert result.blueprint_overrides["input_dir"] == "/data/base/input"


def test_template_fill_path_substitution(
    live_step_with_templates: LiveStep, tmp_path: Path
) -> None:
    """{{path: step_name}} tokens are replaced using the path resolver."""
    upstream_dir = tmp_path / "upstream"
    path_resolver = lambda name: upstream_dir  # noqa: E731

    transform = TemplateFillTransform(path_resolver=path_resolver)
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"output_dir": "{{path: upstream}}/output"}},
    )
    (result,) = transform(step)

    assert result.blueprint_overrides["output_dir"] == f"{upstream_dir}/output"


def test_template_fill_nested_dict(live_step_with_templates: LiveStep) -> None:
    """Placeholders nested inside a dict value are replaced."""
    variables = {"base_dir": "/data/base"}
    transform = TemplateFillTransform(variable_resolver=variables.__getitem__)
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"nested": {"key": "{{base_dir}}"}}},
    )
    (result,) = transform(step)

    assert result.blueprint_overrides["nested"] == {"key": "/data/base"}


def test_template_fill_nested_list(live_step_with_templates: LiveStep) -> None:
    """Placeholders nested inside a list value are replaced."""
    variables = {"var1": "ALK", "var2": "pH_3D"}
    transform = TemplateFillTransform(variable_resolver=variables.__getitem__)
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"variables": ["{{var1}}", "{{var2}}"]}},
    )
    (result,) = transform(step)

    assert result.blueprint_overrides["variables"] == ["ALK", "pH_3D"]


def test_template_fill_scalar_passthrough(live_step_with_templates: LiveStep) -> None:
    """Non-string scalars (int, float) pass through unchanged."""
    transform = TemplateFillTransform()
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"count": 42, "ratio": 3.14}},
    )
    (result,) = transform(step)

    assert result.blueprint_overrides["count"] == 42
    assert result.blueprint_overrides["ratio"] == 3.14


def test_template_fill_missing_variable_resolver_raises(
    live_step_with_templates: LiveStep,
) -> None:
    """ValueError is raised when a plain placeholder is encountered with no variable resolver."""
    transform = TemplateFillTransform()
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"key": "{{missing_var}}"}},
    )
    with pytest.raises(ValueError, match="No variable resolver"):
        list(transform(step))


def test_template_fill_missing_path_resolver_raises(
    live_step_with_templates: LiveStep,
) -> None:
    """ValueError is raised when a path placeholder is encountered with no path resolver."""
    transform = TemplateFillTransform()
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"key": "{{path: some_step}}"}},
    )
    with pytest.raises(ValueError, match="No path resolver"):
        list(transform(step))


def test_template_fill_with_path_resolver_returns_new_instance(
    tmp_path: Path,
) -> None:
    """with_path_resolver returns a new instance; the original is unchanged."""
    original = TemplateFillTransform(variable_resolver=str)
    bound = original.with_path_resolver(lambda _: tmp_path)

    assert bound is not original
    assert bound._path_resolver is not None
    assert original._path_resolver is None
    assert bound._variable_resolver is original._variable_resolver


def test_template_fill_does_not_mutate_original_step(
    live_step_with_templates: LiveStep,
) -> None:
    """__call__ must not mutate the original step's blueprint_overrides."""
    variables = {"base_dir": "/data/base"}
    transform = TemplateFillTransform(variable_resolver=variables.__getitem__)
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"dir": "{{base_dir}}"}},
    )
    original_overrides = dict(step.blueprint_overrides)

    list(transform(step))

    assert step.blueprint_overrides == original_overrides


def test_template_fill_yields_single_step(live_step_with_templates: LiveStep) -> None:
    """__call__ always yields exactly one step."""
    variables = {"base_dir": "/data/base"}
    transform = TemplateFillTransform(variable_resolver=variables.__getitem__)
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"dir": "{{base_dir}}"}},
    )
    results = list(transform(step))

    assert len(results) == 1


def test_template_fill_combined_resolvers(
    tmp_path: Path, live_step_with_templates: LiveStep
) -> None:
    """Variable and path resolvers can both operate in the same transform pass."""
    upstream_dir = tmp_path / "tasks" / "upstream"
    variables = {"var1": "ALK"}
    transform = TemplateFillTransform(
        variable_resolver=variables.__getitem__,
        path_resolver=lambda _: upstream_dir,
    )
    step = LiveStep.from_step(
        live_step_with_templates,
        update={
            "blueprint_overrides": {
                "variable": "{{var1}}",
                "input_dir": "{{path: upstream}}/joined_output",
            }
        },
    )
    (result,) = transform(step)

    assert result.blueprint_overrides["variable"] == "ALK"
    assert result.blueprint_overrides["input_dir"] == f"{upstream_dir}/joined_output"
