import os
import typing as t
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_RUNID, FLAG_OFF
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.feature import ENV_FF_ORCH_TRX_TIMESPLIT
from cstar.orchestration.models import Application, RomsMarblBlueprint, Step, Workplan
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import (
    ContinuanceTransform,
    OverrideTransform,
    ResetFileTrxAdapter,
    RestartFile,
    RomsMarblTimeSplitter,
    TemplateFillTransform,
    WorkplanTransformer,
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


def test_continuance_transform_not_supported(test_output_dir: Path) -> None:
    """Verify that unknown configuration results in an exception.

    Parameters
    ----------
    test_output_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    """
    with pytest.raises(NotImplementedError, match="supported"):
        _ = ContinuanceTransform({"not-path": str(test_output_dir)})


def test_continuance_transform_path_dne() -> None:
    """Verify that sending a path to a directory that does not exist results in
    an exception being raised.

    Parameters
    ----------
    test_output_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    """
    with pytest.raises(ValueError, match="No directory found"):
        _ = ContinuanceTransform({"path": "./dir-that-dne"})


def test_continuance_transform_happy_path(
    single_step_workplan: Workplan,
    mock_sim_output_dir: tuple[Path, Path, Path],
) -> None:
    """Verify that applying a well-formed continuance transform causes the
    blueprint initial conditions to be updated.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    mock_sim_output_dir : Path
        Paths to mocked simulation outputs; used here to pass a valid path
        to the continuance transform (containing files meeting glob pattern *_rst.nc)
    """
    _, continue_from_dir, _ = mock_sim_output_dir

    transform = ContinuanceTransform({"path": str(continue_from_dir)})
    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()  # ensure nothing existing

    with mock.patch.dict(os.environ, {ENV_CSTAR_RUNID: "12345"}, clear=True):
        steps = transform(step)

    transformed = steps[0]

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    # confirm the old blueprint has a different initial conditions location
    assert str(continue_from_dir) not in str(bp_old.initial_conditions.data[0].location)
    assert str(continue_from_dir) in str(bp_new.initial_conditions.data[0].location)

    # confirm the overrides were removed after being applied
    assert not transformed.blueprint_overrides


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
        pytest.param("foo_rst.20260401000000.00.nc", id="partition too short"),
        pytest.param("foo_rst.20260401000000.00a.nc", id="non-numeric partition"),
        pytest.param("foo_rst.20260401000000.0000.nc", id="partition too long"),
    ],
)
def test_reset_file_bad_path(tmp_path: Path, name: str) -> None:
    """Verify that `ResetFile` reports paths that do not meet reset file naming convention."""
    mismatched_name_path = tmp_path / name

    with pytest.raises(ValueError, match="convention"):
        _ = RestartFile(path=mismatched_name_path)


@pytest.mark.parametrize(
    ("name", "expected_is_parted"),
    [
        pytest.param("foo_rst.20260401000000.000.nc", True, id="parted"),
        pytest.param("foo_rst.20260401000000.001.nc", True, id="non-start segment"),
        pytest.param("foo_rst.20260401000000.nc", False, id="unparted"),
    ],
)
def test_reset_file_happy_path(
    tmp_path: Path,
    name: str,
    expected_is_parted: bool,
) -> None:
    """Verify that `ResetFile` handles good inputs correctly."""
    path = tmp_path / name

    rf = RestartFile(path=path)
    assert rf.is_partitioned == expected_is_parted


def test_reset_file_find(tmp_path: Path) -> None:
    """Verify that ResetFile.find locates a reset file when expected."""
    now = datetime.now(tz=timezone.utc)
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)
    reset_path = search_path / f"foo_rst.{now.strftime('%Y%m%d%H%M%S')}.000.nc"
    reset_path.touch()

    # confirm root of search path is searched
    reset_file = RestartFile.find(search_path)
    assert reset_file
    assert reset_file.path == Path(reset_path).expanduser().resolve()

    # confirm search is recursive
    reset_file = RestartFile.find(tmp_path)
    assert reset_file
    assert reset_file.path == Path(reset_path).expanduser().resolve()


def test_reset_file_find_dne(tmp_path: Path) -> None:
    """Verify that ResetFile.find returns None when no files are found."""
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)

    reset_file = RestartFile.find(search_path)
    assert reset_file is None


def test_reset_file_find_dne_notok(tmp_path: Path) -> None:
    """Verify that ResetFile.find raises an exception when no files are found
    and find is passed `notfound_ok=False`.
    """
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)

    with pytest.raises(CstarExpectationFailed, match="No restart files"):
        _ = RestartFile.find(search_path, notfound_ok=False)


def test_reset_file_from_parts_unparted(tmp_path: Path) -> None:
    """Verify that a ResetFile instance is created without a segment ID in the
    path if it is not supplied.
    """
    now = datetime.now(tz=timezone.utc)
    ts = now.strftime("%Y%m%d%H%M%S")
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)

    reset_file = RestartFile.from_parts("test_reset_file_from_parts", now)

    # confirm no empty segment is added
    assert ".000." not in reset_file.path.as_posix()

    # confirm full file name
    assert reset_file.path.as_posix().endswith(f"_rst.{ts}.{RestartFile.EXT}")


@pytest.mark.parametrize(
    ("segment", "exp_segment"),
    [
        pytest.param(0, ".000.", id="edge-case, 0-th segment"),
        pytest.param(1, ".001.", id="valid non-boundary index 1"),
        pytest.param(123, ".123.", id="valid non-boundary index 123"),
        pytest.param(42, ".042.", id="two-digit padding"),
        pytest.param(999, ".999.", id="edge-case, final 3-digit segment"),
    ],
)
def test_reset_file_from_parts_parted(
    tmp_path: Path, segment: int, exp_segment: str
) -> None:
    """Verify that a ResetFile instance is created with a segment ID in the
    path if it is supplied.
    """
    now = datetime.now(tz=timezone.utc)
    search_path = tmp_path / "test-reset-file-find"
    search_path.mkdir(parents=True)
    reset_path = search_path / f"foo_rst.{now.strftime('%Y%m%d%H%M%S')}.000.nc"
    reset_path.touch()

    reset_file = RestartFile.from_parts("test_reset_file_from_parts", now, segment)

    assert exp_segment in reset_file.path.as_posix()


def test_reset_file_from_parts_with_base(tmp_path: Path) -> None:
    """Verify that a ResetFile instance is created with a segment ID in the
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
def test_reset_file_from_path(
    path: str,
    exp_partition: int | None,
    exp_is_partitioned: bool,
) -> None:
    """Verify that ResetFile.__init__ results in the correct settings on the instance."""
    reset_path = Path(path)
    reset_file = RestartFile(path=reset_path)

    assert reset_file.is_partitioned == exp_is_partitioned
    assert reset_file.partition == exp_partition


def test_reset_file_adapter(tmp_path: Path) -> None:
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
    result = ResetFileTrxAdapter.adapt(reset_file)

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
    result = ResetFileTrxAdapter.adapt(reset_file)

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
