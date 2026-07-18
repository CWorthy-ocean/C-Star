# ruff: noqa: SLF001, S101
import os
import typing as t
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.applications.roms_marbl.transforms import (
    ContinuanceDirective,
    RestartFile,
    RestartFileTrxAdapter,
    RomsMarblTimeSplitter,
)
from cstar.base.env import FLAG_OFF
from cstar.base.feature import ENV_FF_ORCH_TRX_TIMESPLIT
from cstar.orchestration.models import (
    Application,
    BlueprintState,
    Step,
    UserDefinedVariables,
    Workplan,
)
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import (
    OverrideTransform,
    TemplateFillTransform,
    WorkplanTransformer,
    apply_automatic_overrides,
    get_fsm_resolver,
    get_system_overrides,
    get_transforms,
    mustache,
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
def test_working_dir(tmp_path: Path) -> Path:
    """Default path for writing outputs from a blueprint."""
    return tmp_path / "working_dir"


@pytest.fixture
def test_working_dir_override(tmp_path: Path) -> Path:
    """Default path for writing outputs from a blueprint."""
    return tmp_path / "working_dir_override"


@pytest.fixture
def step_overiding_wp(
    test_wp_path: Path,
    test_bp_path: Path,
    test_working_dir: Path,
    test_working_dir_override: Path,
    wp_templates_dir: Path,
    bp_templates_dir: Path,
    default_blueprint_path: str,
) -> Workplan:
    """Copy a template containing blueprint overrides to the test tmp_path.

    The step specifies overrides for the blueprint fields:
    - working_dir (original value "/other_dir")
    - start_date (original value "")
    - end_date (original value "")

    Parameters
    ----------
    test_wp_path : Path
        Fixture returning default write location for a workplan file
    test_bp_path : Path
        Fixture returning default write location for a blueprint file
    test_working_dir : Path
        Fixture returning the path to a directory for containing orchestration test files
    test_working_dir_override : Path
        Fixture returning a path that is different from `test_working_dir`
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
        "working_dir: .",
        f"working_dir: {test_working_dir.as_posix()}",
    )
    test_bp_path.write_text(bp_content)

    wp_tpl_path = wp_templates_dir / "wp_with_bp_overrides.yaml"
    wp_content = wp_tpl_path.read_text()
    wp_content = wp_content.replace(default_blueprint_path, test_bp_path.as_posix())

    # replace the default so no test outputs leak into working directories
    wp_content = wp_content.replace(
        "/other_dir",
        test_working_dir_override.as_posix(),
    )
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
    test_working_dir: Path,
    test_working_dir_override: Path,
) -> None:
    """Verify that the OverrideTransform overwrites values in the blueprint.

    Parameters
    ----------
    step_overiding_wp : Workplan
        A workplan copied from a template with paths referencing tmp_path.
    test_working_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    test_working_dir_override : Path
        Fixture returning a path that is different from `test_working_dir`
    """
    transform = OverrideTransform()
    step = step_overiding_wp.steps[0]

    steps = transform(step)

    transformed = list(steps)[0]

    # confirm a attribute of the blueprint is changed (bp.blueprint_path)
    dir_orig = test_working_dir
    exp_dir = test_working_dir_override

    # confirm a new blueprint was created.
    assert Path(step.blueprint_path) != Path(transformed.blueprint_path)

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    # confirm nested attributes (bp.runtime_params.xxx)) is changed
    assert Path(bp_old.working_dir) == dir_orig.expanduser().resolve()
    assert Path(bp_new.working_dir) == exp_dir.expanduser().resolve()

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
    test_working_dir: Path,
) -> None:
    """Verify that system-level overrides passed to the transform override
    values specified in the workplan.

    Parameters
    ----------
    tmp_path : Path
        The temporary path for writing test files.
    step_overiding_wp : Workplan
        A workplan copied from a template with paths referencing tmp_path.
    test_working_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    """
    sys_od = tmp_path / "system_working_dir"
    system_od_override = {"working_dir": sys_od.as_posix()}

    transform = OverrideTransform(sys_overrides=system_od_override)
    step = step_overiding_wp.steps[0]

    steps = transform(step)

    transformed = list(steps)[0]

    # confirm a attribute of the blueprint is changed (bp.blueprint_path)
    dir_orig = test_working_dir

    # confirm a new blueprint was created.
    assert Path(step.blueprint_path) != Path(transformed.blueprint_path)

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    # confirm that even though a working directory override was applied, the
    # system level override was applied last.
    assert Path(bp_old.working_dir) == dir_orig
    assert Path(bp_new.working_dir) == sys_od


def test_continuance_transform_not_supported(test_working_dir: Path) -> None:
    """Verify that unknown configuration results in an exception.

    Parameters
    ----------
    test_working_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    """
    with pytest.raises(NotImplementedError, match="supported"):
        _ = ContinuanceDirective({"not-path": str(test_working_dir)})


def test_continuance_transform_path_dne() -> None:
    """Verify that sending a path to a directory that does not exist results in
    an exception being raised.
    """
    with pytest.raises(ValueError, match="No directory or file found"):
        _ = ContinuanceDirective({ContinuanceDirective.KEY_PATH: "./dir-that-dne"})


@pytest.mark.parametrize("pad_size", range(1, 10))
def test_continuance_transform_happy_path(
    single_step_workplan: Workplan,
    mocked_simulation_outputs: tuple[Path, Path, Path],
    pad_size: int,
) -> None:
    """Verify that applying a well-formed continuance transform causes the
    blueprint initial conditions to be updated.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    mocked_simulation_outputs : Path
        Paths to mocked simulation outputs; used here to pass a valid path
        to the continuance transform (containing files meeting glob pattern *_rst.nc)
    pad_size : int
        Used to vary the amount of zero padding in the partition segment of the file
        name. This ensures that the restart file search can locate files regardless
        of the number of partitions.
    """
    _, continue_from_dir, _ = mocked_simulation_outputs

    for seg_id in ["000", "001", "002"]:
        rf_glob = f"*_rst.*.{seg_id}.nc"
        reset_file_path = next(continue_from_dir.rglob(rf_glob))
        name = reset_file_path.name.replace(
            f"{seg_id}.nc",
            f"{str(int(seg_id)).zfill(pad_size)}.nc",
        )
        reset_file_path = reset_file_path.rename(reset_file_path.with_name(name))

    transform = ContinuanceDirective(
        {ContinuanceDirective.KEY_PATH: str(continue_from_dir)}
    )
    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()  # ensure nothing existing

    steps = transform(step)

    transformed = steps[0]

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    # confirm the old blueprint has a different initial conditions location
    assert str(continue_from_dir) not in str(bp_old.initial_conditions.data[0].location)
    assert str(continue_from_dir) in str(bp_new.initial_conditions.data[0].location)

    # confirm the overrides were removed after being applied
    assert not transformed.blueprint_overrides


def test_workplan_transformer_applies_working_dir_overrides(
    tmp_path: Path,
    step_overiding_wp: Workplan,
    test_working_dir: Path,
    test_working_dir_override: Path,
) -> None:
    """Verify that the workplan transformer applies a transform to override
    the output directory for all steps.

    Parameters
    ----------
    step_overiding_wp : Workplan
        A workplan copied from a template with paths referencing tmp_path.
    test_working_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    test_working_dir_override : Path
        An override that was already on the step before the WP transformer is invoked
    """
    sys_working_dir_override = tmp_path / "system-level-working-dir-override"
    wp_transformer = WorkplanTransformer(step_overiding_wp)
    original_bp_path = Path(step_overiding_wp.steps[0].blueprint_path)
    step_orig: Step = step_overiding_wp.steps[0]
    bp_orig = deserialize(step_orig.blueprint_path, RomsMarblBlueprint)
    mock_overrides = {"working_dir": sys_working_dir_override}

    with (
        mock.patch.dict(os.environ, {ENV_FF_ORCH_TRX_TIMESPLIT: FLAG_OFF}),
        mock.patch(
            "cstar.orchestration.transforms.get_system_overrides",
            mock.Mock(return_value=mock_overrides),
        ),
    ):
        wp_trx = wp_transformer.apply()

    step_trx = t.cast("LiveStep", wp_trx.steps[0])

    # sanity-check expectations for the original blueprint output location and override
    dir_orig = bp_orig.working_dir
    original_override = t.cast(
        "str",
        step_orig.blueprint_overrides["working_dir"],  # type: ignore[reportArgumentType,index,call-overload]
    )
    assert dir_orig == test_working_dir
    assert original_override == str(test_working_dir_override)

    # confirm no override remains on the step
    assert "runtime_params" not in step_trx.blueprint_overrides

    # confirm the transformed step includes an updated blueprint path.
    trx_bp_path = step_trx.blueprint_path
    assert str(trx_bp_path) != str(original_bp_path)

    # confirm the original and updated blueprint have different output directories
    blueprint = deserialize(trx_bp_path, RomsMarblBlueprint)
    assert blueprint.working_dir != dir_orig

    # confirm the workplan override took precedence over user-supplied overrides
    assert blueprint.working_dir == sys_working_dir_override  # exp_dir
    assert blueprint.working_dir != dir_orig
    assert blueprint.working_dir != original_override


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
            "working_dir": "{{work_dir: upstream}}/output",
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


def test_template_fill_scoped_resolver(
    live_step_with_templates: LiveStep, tmp_path: Path
) -> None:
    """{{work_dir: step_name}} tokens are replaced using the scope resolver."""
    upstream_dir = tmp_path / "upstream"

    def _resolve(_x: str, _y: str) -> str:
        return str(upstream_dir)

    transform = TemplateFillTransform(scoped_resolver=_resolve)
    wd_key = "working_dir"
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {wd_key: "{{work_dir: upstream}}/output"}},
    )
    (result,) = transform(step)

    assert result.blueprint_overrides[wd_key] == f"{upstream_dir}/output"


def test_template_fill_scoped_resolver_invalid_lookup(
    live_step_with_templates: LiveStep,
) -> None:
    """Verify that the fill transform raises a value error if an attempt to access
    a valid scope on an invalid member (e.g. look up working_dir, but with a
    step name that doesn't exist).
    """
    wd_key = "working_dir"
    step1 = LiveStep.from_step(
        live_step_with_templates,
        update={"name": "step-1"},
    )
    step2 = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {wd_key: "{{working_dir: step-111}}/output"}},
    )

    resolver = get_fsm_resolver([step1, step2])
    transform = TemplateFillTransform(scoped_resolver=resolver)

    with pytest.raises(KeyError, match="unknown step"):
        _ = transform(step2)


def test_template_fill_variable_resolver_unknown_variable(
    live_step_with_templates: LiveStep,
) -> None:
    """Verify that the fill transform raises a value error if an attempt to access
    an unknown scope on a scoped resolver is encountered.

    E.g. passing {{typo-var}} when the variable is named "ok-var" in the workplan.
    """
    wd_key = "working_dir"
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {wd_key: "{{xxx}}/output"}},
    )
    named_config = UserDefinedVariables(
        keys={"yyy"},
        mapping={"yyy": "value"},
    )
    transform = TemplateFillTransform(variable_resolver=named_config.resolve)

    with pytest.raises(KeyError, match="Unable to resolve variable"):
        _ = transform(step)


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
    """ValueError is raised when a path placeholder is encountered with no scope resolver."""
    transform = TemplateFillTransform()
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"key": "{{work_dir: some_step}}"}},
    )
    with pytest.raises(ValueError, match="No 'work_dir' resolver"):
        list(transform(step))


def test_template_fill_with_path_resolver_returns_new_instance(
    tmp_path: Path,
) -> None:
    """with_path_resolver returns a new instance; the original is unchanged."""
    original = TemplateFillTransform(variable_resolver=str)

    def _resolve(_x: str, _y: str) -> str:
        return str(tmp_path)

    bound = original.with_scoped_resolver(_resolve)

    assert bound is not original
    assert bound.scoped_resolver is not None
    assert original.scoped_resolver is None
    assert bound.variable_resolver is original.variable_resolver


def test_template_fill_with_path_resolver_unknown_purpose(
    live_step_with_templates: LiveStep,
) -> None:
    """Verify that a template matching the purpose format `{{purpose: step_name}}`
    raises an exception if the purpose cannot be resolved.
    """
    step = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {"key": "{{work_dir: some_step}}"}},
    )
    resolver = get_fsm_resolver([step])

    fill = TemplateFillTransform(variable_resolver=str, scoped_resolver=resolver)

    with pytest.raises(KeyError, match="Unable to resolve"):
        list(fill(step))


def test_template_fill_scoped_resolver_unknown_scope(
    live_step_with_templates: LiveStep,
) -> None:
    """Verify that the fill transform raises a value error if an attempt to access
    an unknown scope on a scoped resolver is encountered.

    E.g. in {{oooutput_dir: Step A}} the scope is invalid.
    """
    wd_key = "working_dir"
    step1 = LiveStep.from_step(
        live_step_with_templates,
        update={"name": "step-1"},
    )
    step2 = LiveStep.from_step(
        live_step_with_templates,
        update={"blueprint_overrides": {wd_key: "{{woorking_dir: step-1}}/output"}},
    )

    resolver = get_fsm_resolver([step1, step2])
    transform = TemplateFillTransform(scoped_resolver=resolver)

    with pytest.raises(KeyError, match="Unable to resolve 'woorking_dir'"):
        _ = transform(step2)


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


@pytest.mark.parametrize(
    "purpose",
    [
        "root",
        "input_dir",
        "work_dir",
        "tasks_dir",
        "logs_dir",
        "output_dir",
    ],
)
def test_template_fill_combined_resolvers(
    tmp_path: Path, live_step_with_templates: LiveStep, purpose: str
) -> None:
    """Ensure that variable and scope resolvers both operate in the same transform pass.

    Parammeters
    -----------
    purpose : str
        Parameterized value that ensures all expected/known purpose keys can be retrieved.
    """
    upstream_dir = tmp_path / "tasks" / "upstream"
    variables = {"var1": "ALK"}
    transform = TemplateFillTransform(
        variable_resolver=variables.__getitem__,
        scoped_resolver=lambda _key, _scope: str(upstream_dir),
    )
    template = mustache(f"{purpose}: var1")
    step = LiveStep.from_step(
        live_step_with_templates,
        update={
            "blueprint_overrides": {
                "variable": mustache("var1"),
                "input_dir": f"{template}/joined_output",
            },
        },
    )
    (result,) = transform(step)

    assert result.blueprint_overrides["variable"] == "ALK"
    assert result.blueprint_overrides["input_dir"] == f"{upstream_dir}/joined_output"


@pytest.mark.parametrize(
    "name",
    [
        pytest.param("foo", id="no redeeming qualities"),
        pytest.param("foo.txt", id="name-like, bad extension"),
        pytest.param("foo.nc", id="name-like, good extension"),
        pytest.param("foo_rst.nc", id="missing timestamp segment"),
        pytest.param("foo_rst.0000000000000.nc", id="non-parseable timestamp (zeros)"),
        pytest.param("foo_rst.2026040100000.nc", id="unparted::ts too short"),
        pytest.param("foo_rst.2026040100000a..nc", id="unparted::non-numeric ts"),
        pytest.param("foo_rst.202604010000000..nc", id="unparted::ts too long"),
        pytest.param("foo.20260401000000_rst.nc", id="unparted::suffix on ts segment"),
        pytest.param("foo.20260401000000_rst.000.nc", id="suffix on ts segment"),
        pytest.param("foo.20260401000000.000_rst.nc", id="suffix on partition segment"),
        pytest.param("foo.rst.20260401000000.000.nc", id="dot leader in suffix"),
        pytest.param("foo.rst.20260401000000.nc", id="unparted::dot leader in suffix"),
        pytest.param("foo_rst.20260401000000.000.nc/file.nc", id="match to dir name"),
        pytest.param("foo_rst.20260401000000.nc/xxx.nc", id="unparted::match dir name"),
        pytest.param("foo_rst.0000000000000.000.nc", id="ts too short"),
        pytest.param("foo_rst.0000000000000a.000.nc", id="non-numeric ts"),
        pytest.param("foo_rst.000000000000000.000.nc", id="ts too long"),
        pytest.param("foo_rst.20260401000000..nc", id="partition empty"),
        pytest.param("foo_rst.20260401000000.0000000000.nc", id="10 partition chars"),
        pytest.param("foo_rst.20260401000000.00a.nc", id="non-numeric partition"),
    ],
)
def test_restart_file_bad_path(tmp_path: Path, name: str) -> None:
    """Verify that `RestartFile` reports paths that do not meet reset file naming convention."""
    mismatched_name_path = tmp_path / name

    with pytest.raises(ValueError, match="convention"):
        _ = RestartFile(path=mismatched_name_path)


@pytest.mark.parametrize(
    ("name", "expected_is_parted"),
    [
        pytest.param("foo_rst.20260401000000.000.nc", True, id="parted"),
        pytest.param("foo_rst.20260401000000.001.nc", True, id="non-start segment"),
        pytest.param("foo_rst.20260401000000.1.nc", True, id="no partition padding"),
        pytest.param("foo_rst.20260401000000.999999999.nc", True, id="max 0-padding"),
        pytest.param("foo_rst.20260401000000.nc", False, id="unparted"),
    ],
)
def test_restart_file_happy_path(
    tmp_path: Path,
    name: str,
    expected_is_parted: bool,
) -> None:
    """Verify that `RestartFile` handles good inputs correctly."""
    path = tmp_path / name

    rf = RestartFile(path=path)
    assert rf.is_partitioned == expected_is_parted


@pytest.mark.parametrize(
    "pad_size",
    range(1, 10),
)
def test_restart_file_find(tmp_path: Path, pad_size: int) -> None:
    """Verify that `RestartFile.find` locates a reset file when expected."""
    now = datetime.now(tz=timezone.utc)
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)
    segment = "0".zfill(pad_size)
    reset_path = search_path / f"foo_rst.{now.strftime('%Y%m%d%H%M%S')}.{segment}.nc"
    reset_path.touch()

    # confirm root of search path is searched
    reset_file = RestartFile.find(search_path)
    assert reset_file
    assert reset_file.path == Path(reset_path).expanduser().resolve()

    # confirm search is recursive
    reset_file = RestartFile.find(tmp_path)
    assert reset_file
    assert reset_file.path == Path(reset_path).expanduser().resolve()


def test_restart_file_find_dne(tmp_path: Path) -> None:
    """Verify that `RestartFile.find` returns None when no files are found."""
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)

    reset_file = RestartFile.find(search_path)
    assert reset_file is None


def test_restart_file_find_dne_notok(tmp_path: Path) -> None:
    """Verify that `RestartFile.find` raises an exception when no files are found
    and find is passed `notfound_ok=False`.
    """
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)

    with pytest.raises(FileNotFoundError, match="No restart files"):
        _ = RestartFile.find(search_path, notfound_ok=False)


def test_restart_file_from_parts_unparted(tmp_path: Path) -> None:
    """Verify that a `RestartFile` instance is created without a segment ID in the
    path if it is not supplied.
    """
    now = datetime.now(tz=timezone.utc)
    ts = now.strftime("%Y%m%d%H%M%S")
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)

    reset_file = RestartFile.from_parts("test_restart_file_from_parts", now)

    # confirm no empty segment is added
    assert ".000." not in reset_file.path.as_posix()

    # confirm full file name
    assert reset_file.path.as_posix().endswith(f"_rst.{ts}.{RestartFile.EXT}")


@pytest.mark.parametrize(
    ("segment", "exp_segment"),
    [
        pytest.param("0", ".0.", id="edge-case, 0-th segment"),
        pytest.param("1", ".1.", id="valid non-boundary index 1"),
        pytest.param("123", ".123.", id="valid non-boundary index 123"),
        pytest.param("042", ".042.", id="two-digit padding"),
        pytest.param("999", ".999.", id="edge-case, final 3-digit segment"),
    ],
)
def test_restart_file_from_parts_parted(
    tmp_path: Path, segment: str, exp_segment: str
) -> None:
    """Verify that a `RestartFile` instance is created with a segment ID in the
    path if it is supplied.
    """
    now = datetime.now(tz=timezone.utc)
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)
    reset_path = search_path / f"foo_rst.{now.strftime('%Y%m%d%H%M%S')}.000.nc"
    reset_path.touch()

    reset_file = RestartFile.from_parts("test_restart_file_from_parts", now, segment)

    assert exp_segment in reset_file.path.as_posix()


def test_restart_file_from_parts_with_base(tmp_path: Path) -> None:
    """Verify that a `RestartFile` instance is created with a segment ID in the
    path if it is supplied.
    """
    now = datetime.now(tz=timezone.utc)
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)

    reset_file = RestartFile.from_parts(
        "test-base",
        now,
        directory=search_path,
    )
    assert reset_file is not None

    assert reset_file.path.as_posix().startswith(
        (search_path / "test-base_rst").as_posix(),
    )


@pytest.mark.parametrize(
    ("path", "exp_partition", "exp_is_partitioned"),
    [
        pytest.param(
            f"foo_rst.{datetime.now(tz=timezone.utc).strftime(RestartFile.FMT_TS)}.010.nc",
            10,
            True,
            id="parted path",
        ),
        pytest.param(
            f"foo_rst.{datetime.now(tz=timezone.utc).strftime(RestartFile.FMT_TS)}.nc",
            None,
            False,
            id="unparted path",
        ),
    ],
)
def test_restart_file_from_path(
    path: str,
    exp_partition: int | None,
    exp_is_partitioned: bool,
) -> None:
    """Verify that `RestartFile.__init__` results in the correct settings on the instance."""
    reset_path = Path(path)
    reset_file = RestartFile(path=reset_path)

    assert reset_file.is_partitioned == exp_is_partitioned
    assert reset_file.partition == exp_partition


def test_restart_file_adapter(tmp_path: Path) -> None:
    """Verify that a partitioned reset file contains the correct partition information
    when converted into an override.
    """
    now = datetime.now(tz=timezone.utc)
    ts = now.strftime("%Y%m%d%H%M%S")
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)
    reset_path = search_path / f"foo_rst.{ts}.000.nc"
    reset_path.touch()

    reset_file = RestartFile(path=reset_path)
    result = RestartFileTrxAdapter.adapt(reset_file)

    # confirm all fields exist and the partioned flag is True
    rp = result.get("runtime_params", None)
    assert rp
    assert "start_date" in rp
    ic = result.get("initial_conditions", None)
    assert ic
    data = ic.get("data", None)
    assert data
    data0 = data[0]
    assert data0["location"] == reset_file.path.as_posix()
    assert data0["partitioned"]

    reset_path = search_path / f"foo_rst.{ts}.nc"
    reset_file = RestartFile(path=reset_path)
    result = RestartFileTrxAdapter.adapt(reset_file)

    # confirm all fields exist and the partioned flag is False
    rp = result.get("runtime_params", None)
    assert rp
    assert "start_date" in rp
    ic = result.get("initial_conditions", None)
    assert ic
    data = ic.get("data", None)
    assert data
    data0 = data[0]
    assert data0["location"] == reset_file.path.as_posix()
    assert not data0["partitioned"]


def test_app_specific_system_overrides(live_step_with_templates: LiveStep) -> None:
    """Verify the default behavior contains an override for roms-marbl."""
    live_step_with_templates.application = "roms_marbl"

    sys_overrides_for_app = get_system_overrides(live_step_with_templates)
    assert sys_overrides_for_app


def test_apply_automatic_overrides(
    live_step_with_templates: LiveStep, bp_templates_dir: Path
) -> None:
    """Verify that app-specific overrides are applied."""
    live_step_with_templates.blueprint_overrides.clear()
    Path(live_step_with_templates.blueprint_path).write_text(
        (bp_templates_dir / "blueprint.yaml").read_text(),
    )
    assert live_step_with_templates.blueprint.state != BlueprintState.Validated

    value = "validated"
    mock_overrides = {"state": BlueprintState.Validated}

    with mock.patch(
        "cstar.orchestration.transforms.get_system_overrides",
        mock.Mock(return_value=mock_overrides),
    ):
        step = apply_automatic_overrides(live_step_with_templates)

    # the mocked system overrides should be applied
    assert step.blueprint.state == value
