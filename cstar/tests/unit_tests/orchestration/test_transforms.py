import dataclasses
import logging
import os
import shutil
import typing as t
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from unittest import mock

import pytest
from pydantic import ValidationError

from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.applications.roms_marbl.file_system import RomsFileSystemManager
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.applications.roms_marbl.transforms import (
    ContinuanceDirective,
    NestingDirective,
    RestartFile,
    RestartFileTrxAdapter,
    RomsMarblTimeSplitter,
    _split_sources,
    restart_timestamp,
    warn_on_restart_start_date_mismatch,
)
from cstar.applications.roms_marbl.transforms import (
    log as transforms_log,
)
from cstar.base.env import ENV_CSTAR_RUNID, FLAG_OFF
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.feature import ENV_FF_ORCH_TRX_TIMESPLIT
from cstar.execution.file_system import JobFileSystemManager, StateDirectoryManager
from cstar.orchestration.adapter import DIRECTIVES_FILENAME, prepare_directive_file
from cstar.orchestration.compute_environment import ComputeEnvironment
from cstar.orchestration.dag_runner import prepare_workplan
from cstar.orchestration.launch.local import LocalHandle, LocalLauncher
from cstar.orchestration.launch.slurm import SlurmComputeSpec
from cstar.orchestration.models import (
    Application,
    BlueprintState,
    DeferredBlueprintRef,
    RunRef,
    Step,
    StepRef,
    UserDefinedVariables,
    Workplan,
)
from cstar.orchestration.orchestration import (
    Launcher,
    LiveStep,
    LiveWorkplan,
    ProcessHandle,
    Status,
)
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.state import StateRepository
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.orchestration.transforms import (
    ApplyOverridesDirective,
    DirectiveConfig,
    ExternalRuns,
    OverrideDirective,
    OverrideTransform,
    TemplateFillTransform,
    WorkplanTransformer,
    _inject_compute_defaults,
    apply_automatic_overrides,
    collect_directive_problems,
    effective_blueprint,
    external_dependencies,
    fill_runs,
    get_fsm_resolver,
    get_system_overrides,
    get_transforms,
    lookup_step,
    materialize_inline_blueprints,
    mustache,
    package_runtime_overrides,
    resolve_deferred_blueprint,
)

TRANSFORMS_LOGGER_NAME = "cstar.applications.roms_marbl.transforms"
"""The logger name used by `cstar.applications.roms_marbl.transforms`."""


@pytest.fixture(autouse=True)
def _run_in_tmp_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run every test in this module from ``tmp_path``.

    Several tests build in-memory ``Step``/``Workplan`` objects with no working
    directory, so the job file-system root resolves relative to the CWD and the
    transformer's derived artifacts (``*.ovrd.yaml``, ``*.cfrom.yaml``) land in
    ``<cwd>/work/`` -- littering the repository checkout when pytest runs from
    the repo root. All fixture paths used here are absolute (``tmp_path`` or
    ``__file__``-anchored templates), so changing the CWD is safe.
    """
    monkeypatch.chdir(tmp_path)


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
    default_working_dir = "working_dir: ."

    bp_content = bp_tpl_path.read_text()
    bp_content = bp_content.replace(
        default_working_dir, f"working_dir: {test_working_dir}"
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

    transformed = next(iter(steps))

    # confirm a attribute of the blueprint is changed (bp.blueprint_path)
    dir_orig = test_working_dir
    exp_dir = test_working_dir_override

    # confirm a new blueprint was created.
    assert Path(step.blueprint_path) != Path(transformed.blueprint_path)

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    # confirm nested attributes (bp.runtime_params.xxx)) is changed
    assert bp_old.effective_working_dir == dir_orig.expanduser().resolve()
    assert bp_new.effective_working_dir == exp_dir.expanduser().resolve()

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

    transformed = next(iter(steps))

    # confirm a attribute of the blueprint is changed (bp.blueprint_path)
    dir_orig = test_working_dir

    # confirm a new blueprint was created.
    assert Path(step.blueprint_path) != Path(transformed.blueprint_path)

    bp_old = deserialize(step.blueprint_path, RomsMarblBlueprint)
    bp_new = deserialize(transformed.blueprint_path, RomsMarblBlueprint)

    # confirm that even though a working directory override was applied, the
    # system level override was applied last.
    assert bp_old.effective_working_dir == dir_orig
    assert bp_new.effective_working_dir == sys_od


def test_override_transform_system_list_replaces_wholesale(
    single_step_workplan: Workplan,
    tmp_path: Path,
) -> None:
    """Verify `replace_lists=True` makes a system-level override list replace
    the blueprint's list wholesale, instead of merging element-wise -- no
    stale entries or fields (e.g. `hash`) from the original entries survive.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    tmp_path : Path
        Temporary directory used to build a user-level override value.
    """
    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()
    user_working_dir = tmp_path / "user_override"
    step.blueprint_overrides["working_dir"] = user_working_dir.as_posix()

    bp_before = deserialize(step.blueprint_path, RomsMarblBlueprint)
    assert len(bp_before.forcing.boundary.data) == 3

    location = "http://mockdoc.com/x_bry.20230201003000.nc"
    transform = OverrideTransform(
        sys_overrides={
            "forcing": {
                "boundary": {"data": [{"location": location, "partitioned": False}]}
            }
        },
        replace_lists=True,
    )

    steps = transform(step)
    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)

    assert len(bp_after.forcing.boundary.data) == 1
    assert bp_after.forcing.boundary.data[0].location == location
    assert getattr(bp_after.forcing.boundary.data[0], "hash", None) is None

    # the user-level scalar override still applies alongside the
    # system-level list replacement.
    assert bp_after.effective_working_dir == user_working_dir.expanduser().resolve()


def test_override_transform_system_list_without_flag_merges_elementwise(
    single_step_workplan: Workplan,
) -> None:
    """Verify that without `replace_lists=True` (the default), a
    system-level override list merges element-wise just like a user-level
    one, leaving stale entries and fields (e.g. `hash`) in place.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    """
    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()

    location = "http://mockdoc.com/x_bry.20230201003000.nc"
    transform = OverrideTransform(
        sys_overrides={
            "forcing": {
                "boundary": {"data": [{"location": location, "partitioned": False}]}
            }
        }
    )

    steps = transform(step)
    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)
    data = bp_after.forcing.boundary.data

    assert len(data) == 3
    assert data[0].location == location
    assert data[0].hash == "abc"
    assert data[1].location == "http://mockdoc.com/partitioning2.nc"
    assert data[2].location == "http://mockdoc.com/partitioning3.nc"


def test_override_transform_user_list_merges_elementwise(
    single_step_workplan: Workplan,
) -> None:
    """Verify a user-level (step `blueprint_overrides`) list still merges
    element-wise into the blueprint's list, unlike a system-level override.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    """
    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()
    step.blueprint_overrides["forcing"] = {
        "boundary": {"data": [{"location": "http://mockdoc.com/replaced1.nc"}]}
    }

    transform = OverrideTransform()
    steps = transform(step)

    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)
    data = bp_after.forcing.boundary.data

    assert len(data) == 3
    assert data[0].location == "http://mockdoc.com/replaced1.nc"
    assert data[0].hash == "abc"
    assert data[1].location == "http://mockdoc.com/partitioning2.nc"
    assert data[2].location == "http://mockdoc.com/partitioning3.nc"


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
async def test_continuance_directive_step_resolution(
    tmp_path: Path,
    bp_templates_dir: Path,
    wp_templates_dir: Path,
    create_mocked_simulation_outputs: Callable[[Path, Path, str], Awaitable[None]],
    mock_run_id: str,
) -> None:
    """Verify that a continuance directive uses context information to identify
    the search path when a step name is provided.

    The directive resolves a named step's `output` directory and locates the
    whole restart file written there.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    bp_templates_dir: Path
        Fixture returning the path to the directory containing blueprint template files
    wp_templates_dir: Path
        Fixture returning the path to the directory containing workplan template files
    mock_run_id
        A unique run-id that has already been added to os.environ
    """
    run_id = mock_run_id

    wp_template_file = "linear.yaml"
    wp_template_path = wp_templates_dir / wp_template_file

    bp_template_file = "blueprint.yaml"
    bp_template_path = bp_templates_dir / bp_template_file

    local_bp = tmp_path / bp_template_file
    local_bp.write_text(bp_template_path.read_text())

    wp = deserialize(wp_template_path, Workplan)

    # Prepare the templated workplan by adding directive configuration
    live_steps = [LiveStep.from_step(s) for s in wp.steps]

    for i, step in enumerate(live_steps):
        if i > 0:
            step.directives[ContinuanceDirective.key()] = {"step": wp.steps[i - 1].name}
        attributes = step.model_dump(exclude={"working_dir", "blueprint_path"})
        attributes["working_dir"] = tmp_path / run_id / step.safe_name
        attributes["blueprint"] = local_bp
        live_steps[i] = LiveStep(**attributes)

    live_plan = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    live_wp_path = tmp_path / wp_template_file
    assert serialize(live_wp_path, live_plan)

    await create_mocked_simulation_outputs(wp_template_path, live_wp_path, run_id)

    expected_name = "output_rst.20120201000000.nc"

    for i, step in enumerate(t.cast("list[LiveStep]", live_plan.steps)):
        if i > 0:
            prior_step = live_plan.steps[i - 1]
            directives = t.cast("dict[str, dict[str, str]]", step.directives)
            assert ContinuanceDirective.key() in directives

            config = directives.get(ContinuanceDirective.key(), {})
            assert ContinuanceDirective.KEY_STEP in config
            assert ContinuanceDirective.KEY_PATH not in config

            modifier = ContinuanceDirective(config, workplan=live_plan)
            altered = modifier(step)[0]

            current_directives = t.cast("dict[str, dict[str, str]]", altered.directives)
            assert ContinuanceDirective.key() in current_directives

            config = current_directives.get(ContinuanceDirective.key(), {})

            # confirm the path still doesn't exist in the directive
            assert not altered.blueprint_overrides
            assert ContinuanceDirective.KEY_STEP in config
            assert ContinuanceDirective.KEY_PATH not in config

            # confirm the initial conditions were overridden to continue from
            # the latest whole restart file in the named step's `output`.
            prior_fsm = RomsFileSystemManager(prior_step.fsm.root_dir)
            expected_dir = prior_fsm.output_dir

            bp = deserialize(altered.blueprint_path, RomsMarblBlueprint)
            location = Path(bp.initial_conditions.data[0].location)
            assert location.is_relative_to(expected_dir)
            assert location.name == expected_name


@pytest.mark.asyncio
async def test_continuance_directive_context_not_supplied() -> None:
    """Verify that constructing a `ContinuanceDirective` that requires contextual
    information without supplying the necessary context info results in a failure.

    """
    workplan: LiveWorkplan | None = None
    config = {"step": "any-step-name"}

    with pytest.raises(CstarError, match="Directive did not receive workplan"):
        ContinuanceDirective(config, workplan=workplan)


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
async def test_continuance_directive_step_DNE(
    tmp_path: Path,
    bp_templates_dir: Path,
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify that constructing a `ContinuanceDirective` that has an invalid
    step name as the source results in a failure.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    bp_templates_dir: Path
        Fixture returning the path to the directory containing blueprint template files
    wp_templates_dir: Path
        Fixture returning the path to the directory containing workplan template files
    mock_run_id
        A unique run-id that has already been added to os.environ
    """
    run_id = mock_run_id

    wp_template_file = "linear.yaml"
    wp_template_path = wp_templates_dir / wp_template_file

    bp_template_file = "blueprint.yaml"
    bp_template_path = bp_templates_dir / bp_template_file

    local_bp = tmp_path / bp_template_file
    local_bp.write_text(bp_template_path.read_text())

    wp = deserialize(wp_template_path, Workplan)

    # Prepare the templated workplan by adding directive configuration
    live_steps = [LiveStep.from_step(s) for s in wp.steps]

    for i, step in enumerate(live_steps):
        if i > 0:
            step.directives[ContinuanceDirective.key()] = {"step": wp.steps[i - 1].name}
        attributes = step.model_dump(exclude={"working_dir", "blueprint_path"})
        attributes["working_dir"] = tmp_path / run_id / step.safe_name
        attributes["blueprint"] = local_bp
        live_steps[i] = LiveStep(**attributes)

    live_plan = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    bad_name = str(uuid.uuid4())
    config = {"step": bad_name}

    with pytest.raises(KeyError, match=f"Unable to locate step {bad_name!r}"):
        ContinuanceDirective(config, workplan=live_plan)


def test_continuance_directive_incomplete_config_supplied(
    test_working_dir: Path,
) -> None:
    """Verify that unknown configuration results in an exception.

    Parameters
    ----------
    test_working_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    """
    with pytest.raises(NotImplementedError, match="supported"):
        _ = ContinuanceDirective({"not-path": str(test_working_dir)})


def test_continuance_directive_path_and_step_are_mutually_exclusive(
    test_working_dir: Path,
) -> None:
    """Verify supplying both `path` and `step` is rejected instead of one silently winning."""
    with pytest.raises(NotImplementedError, match="mutually exclusive"):
        _ = ContinuanceDirective(
            {
                ContinuanceDirective.KEY_PATH: str(test_working_dir),
                ContinuanceDirective.KEY_STEP: "previous",
            }
        )


def test_continuance_directive_extra_unknown_key_rejected(
    test_working_dir: Path,
) -> None:
    """Verify an unrecognized key alongside a valid `path` is rejected, not
    silently ignored.

    Parameters
    ----------
    test_working_dir : Path
        The value that replaced the static content of the blueprint template
        and was written to the test directory, tmp_path.
    """
    with pytest.raises(NotImplementedError, match="supported"):
        _ = ContinuanceDirective(
            {
                ContinuanceDirective.KEY_PATH: str(test_working_dir),
                "unknown-key": "value",
            }
        )


@pytest.fixture
def roms_marbl_step(tmp_path: Path, hello_world_bp_path: Path) -> LiveStep:
    """A minimal `roms_marbl` LiveStep to validate directive configs against.

    Schedule-time directive validation never reads the step's blueprint, so the
    hello-world blueprint file only has to exist.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    return LiveStep(
        name="s1",
        application="roms_marbl",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "s1",
    )


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("2012-01-15 00:00:00", id="iso string, space"),
        pytest.param("2012-01-15T00:00:00", id="iso string, T"),
        pytest.param(" 2012-01-15 00:00:00 ", id="surrounding whitespace"),
        pytest.param(datetime(2012, 1, 15), id="yaml datetime"),
        pytest.param(date(2012, 1, 15), id="yaml date"),
        pytest.param("2012-01-15", id="date string"),
        pytest.param("20120115", id="compact date string"),
        pytest.param(20120115, id="yaml compact date int"),
        pytest.param("20120115000000", id="restart stamp string"),
        pytest.param(20120115000000, id="yaml restart stamp int"),
    ],
)
def test_continuance_directive_timestamp_accepted_forms(
    value: t.Any, roms_marbl_step: LiveStep
) -> None:
    """Verify every form YAML can deliver for a `timestamp` parses to the same
    instant, a date alone meaning midnight, and passes schedule-time validation.

    Parameters
    ----------
    value : t.Any
        The `timestamp` value as it would arrive from the workplan.
    roms_marbl_step : LiveStep
        A minimal step to validate the directive config against.
    """
    assert ContinuanceDirective._parse_timestamp(value) == datetime(2012, 1, 15)

    config = {
        ContinuanceDirective.KEY_PATH: "x",
        ContinuanceDirective.KEY_TIMESTAMP: value,
    }
    assert not ContinuanceDirective.validate_directives(config, roms_marbl_step)


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        pytest.param("2012-02", "ISO 8601", id="partial date"),
        pytest.param("junk", "ISO 8601", id="not a date"),
        pytest.param(None, "ISO 8601", id="null"),
        pytest.param("2012-01-15T00:00:00Z", "timezone", id="timezone"),
        pytest.param("2012-01-15 00:00:00.5", "whole number", id="fractional seconds"),
        pytest.param("2012011500000", "ISO 8601", id="13 digits"),
        pytest.param("20121315000000", "ISO 8601", id="14 digits, month 13"),
    ],
)
def test_continuance_directive_timestamp_rejected_values(
    value: t.Any, reason: str, roms_marbl_step: LiveStep
) -> None:
    """Verify a malformed `timestamp` is reported once at schedule time rather
    than completed, guessed at, or read as "latest".

    Parameters
    ----------
    value : t.Any
        The malformed `timestamp` value.
    reason : str
        A fragment of the message expected for this kind of malformation.
    roms_marbl_step : LiveStep
        A minimal step to validate the directive config against.
    """
    config = {
        ContinuanceDirective.KEY_PATH: "x",
        ContinuanceDirective.KEY_TIMESTAMP: value,
    }

    problems = ContinuanceDirective.validate_directives(config, roms_marbl_step)

    assert len(problems) == 1
    assert ContinuanceDirective.KEY_TIMESTAMP in problems[0]
    assert reason in problems[0]


def test_continuance_directive_timestamp_without_source_rejected(
    roms_marbl_step: LiveStep,
) -> None:
    """Verify `timestamp` without a `step` or `path` source is rejected with a
    message naming the missing source.

    Parameters
    ----------
    roms_marbl_step : LiveStep
        A minimal step to validate the directive config against.
    """
    config = {ContinuanceDirective.KEY_TIMESTAMP: "2012-01-15"}

    problems = ContinuanceDirective.validate_directives(config, roms_marbl_step)

    assert len(problems) == 1
    assert "also supply" in problems[0]


def test_continuance_directive_timestamp_with_unknown_key_rejected(
    roms_marbl_step: LiveStep,
) -> None:
    """Verify an unrecognized key alongside `path` and `timestamp` gets the
    generic message, listing the supported and provided keys in sorted order.

    Parameters
    ----------
    roms_marbl_step : LiveStep
        A minimal step to validate the directive config against.
    """
    config = {
        ContinuanceDirective.KEY_PATH: "x",
        ContinuanceDirective.KEY_TIMESTAMP: "2012-01-15",
        "bogus": 1,
    }

    problems = ContinuanceDirective.validate_directives(config, roms_marbl_step)

    assert len(problems) == 1
    assert "supported configuration: path, step, timestamp" in problems[0]
    assert "provided configuration: bogus, path, timestamp" in problems[0]


def test_continuance_directive_path_dne() -> None:
    """Verify that sending a path to a directory that does not exist results in
    an exception being raised.
    """
    with pytest.raises(ValueError, match="No directory or file found"):
        _ = ContinuanceDirective({ContinuanceDirective.KEY_PATH: "./dir-that-dne"})


@pytest.mark.parametrize("pad_size", range(1, 10))
def test_continuance_directive_happy_path(
    single_step_workplan: Workplan,
    mocked_simulation_outputs: tuple[Path, Path, Path],
    pad_size: int,
) -> None:
    """Verify that applying a well-formed continuance directive causes the
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


def _set_explicit_start_date_override(step: LiveStep, start_date: str) -> None:
    """Mirror the shape `package_runtime_overrides` produces, packaging an
    explicit `runtime_params.start_date` override into the step's runtime
    `apply-overrides` directive config.

    Parameters
    ----------
    step : LiveStep
        The step to attach the directive config to.
    start_date : str
        The explicit start date to package, as an ISO-like string (as it
        would arrive from YAML).
    """
    step.directives[ApplyOverridesDirective.key()] = {
        ApplyOverridesDirective.KEY_OVERRIDES: {
            "runtime_params": {"start_date": start_date},
        },
        ApplyOverridesDirective.KEY_APPLICATION: step.application,
    }


def test_continuance_directive_warns_on_explicit_start_date_mismatch(
    single_step_workplan: Workplan,
    mocked_simulation_outputs: tuple[Path, Path, Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a `ContinuanceDirective` warns when the step explicitly
    requested a `start_date` that disagrees with the located restart file's
    timestamp, and that the resulting blueprint's `start_date` reflects the
    restart's timestamp.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    mocked_simulation_outputs : tuple[Path, Path, Path]
        Paths to mocked simulation outputs; the latest restart there is dated
        2012-01-01T00:20:00.
    caplog : pytest.LogCaptureFixture
        Captures log records emitted during the test.
    """
    _, continue_from_dir, _ = mocked_simulation_outputs
    expected_ts = datetime(2012, 1, 1, 0, 20, 0)
    explicit_start = "2023-02-15 00:00:00"

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()
    _set_explicit_start_date_override(step, explicit_start)

    transform = ContinuanceDirective(
        {ContinuanceDirective.KEY_PATH: str(continue_from_dir)}
    )

    with caplog.at_level(logging.WARNING, logger=TRANSFORMS_LOGGER_NAME):
        steps = transform(step)

    records = [r for r in caplog.records if r.name == TRANSFORMS_LOGGER_NAME]
    assert len(records) == 1
    assert explicit_start in records[0].getMessage()
    assert str(expected_ts) in records[0].getMessage()

    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)
    assert bp_after.runtime_params.start_date == expected_ts


def test_continuance_directive_no_warning_without_explicit_start_date(
    single_step_workplan: Workplan,
    mocked_simulation_outputs: tuple[Path, Path, Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a `ContinuanceDirective` stays quiet for a normal chained step
    that leaves `start_date` to the directive, even though the base
    blueprint's `start_date` is unrelated to the located restart.

    This is the false-positive regression case: every chained
    `continue-from: step: <prev>` step would otherwise warn, since the base
    blueprint's `start_date` has no relationship to whatever restart is
    discovered.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    mocked_simulation_outputs : tuple[Path, Path, Path]
        Paths to mocked simulation outputs; the latest restart there is dated
        2012-01-01T00:20:00, which differs from the base blueprint's
        start_date (2020-01-01) -- and no explicit override is set.
    caplog : pytest.LogCaptureFixture
        Captures log records emitted during the test.
    """
    _, continue_from_dir, _ = mocked_simulation_outputs

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()

    bp_before = deserialize(step.blueprint_path, RomsMarblBlueprint)
    assert bp_before.runtime_params.start_date != datetime(2012, 1, 1, 0, 20, 0)
    assert ApplyOverridesDirective.key() not in step.directives

    transform = ContinuanceDirective(
        {ContinuanceDirective.KEY_PATH: str(continue_from_dir)}
    )

    with caplog.at_level(logging.WARNING, logger=TRANSFORMS_LOGGER_NAME):
        transform(step)

    records = [r for r in caplog.records if r.name == TRANSFORMS_LOGGER_NAME]
    assert not records


def test_continuance_directive_no_warning_when_explicit_start_date_matches_restart(
    single_step_workplan: Workplan,
    mocked_simulation_outputs: tuple[Path, Path, Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a `ContinuanceDirective` does not warn when the step's explicit
    `start_date` override already matches the located restart file's
    timestamp.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    mocked_simulation_outputs : tuple[Path, Path, Path]
        Paths to mocked simulation outputs; the latest restart there is dated
        2012-01-01T00:20:00.
    caplog : pytest.LogCaptureFixture
        Captures log records emitted during the test.
    """
    _, continue_from_dir, _ = mocked_simulation_outputs

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()
    _set_explicit_start_date_override(step, "2012-01-01 00:20:00")

    transform = ContinuanceDirective(
        {ContinuanceDirective.KEY_PATH: str(continue_from_dir)}
    )

    with caplog.at_level(logging.WARNING, logger=TRANSFORMS_LOGGER_NAME):
        transform(step)

    records = [r for r in caplog.records if r.name == TRANSFORMS_LOGGER_NAME]
    assert not records


def test_nesting_directive_path_only_sets_boundary_only(
    single_step_workplan: Workplan,
    tmp_path: Path,
) -> None:
    """Verify a `path`-only `nest-from` sets only the boundary forcing,
    leaving `initial_conditions` and `start_date` untouched.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    tmp_path : Path
        Temporary directory used to hold a mocked boundary file.
    """
    bry_dir = tmp_path / "bry"
    bry_dir.mkdir()
    (bry_dir / "parent_bry.20230201003000.nc").write_text("mock boundary data")

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()

    bp_before = deserialize(step.blueprint_path, RomsMarblBlueprint)

    transform = NestingDirective({NestingDirective.KEY_PATH: str(bry_dir)})
    assert set(transform._system_overrides) == {"forcing"}

    steps = transform(step)

    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)
    assert Path(bp_after.forcing.boundary.data[0].location).name == (
        "parent_bry.20230201003000.nc"
    )
    assert bp_after.initial_conditions == bp_before.initial_conditions
    assert bp_after.runtime_params.start_date == bp_before.runtime_params.start_date


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
async def test_nesting_directive_step_resolution(
    tmp_path: Path,
    bp_templates_dir: Path,
    wp_templates_dir: Path,
    create_mocked_simulation_outputs: Callable[[Path, Path, str], Awaitable[None]],
    mock_run_id: str,
) -> None:
    """Verify a `nest-from` `step` config resolves the boundary search path
    the same way `continue-from` resolves its restart search path: the named
    step's `output` directory.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files
    wp_templates_dir : Path
        Fixture returning the path to the directory containing workplan template files
    mock_run_id
        A unique run-id that has already been added to os.environ
    """
    run_id = mock_run_id
    expected_name = "parent_bry.20230201003000.nc"

    wp_template_file = "linear.yaml"
    wp_template_path = wp_templates_dir / wp_template_file

    bp_template_file = "blueprint.yaml"
    bp_template_path = bp_templates_dir / bp_template_file

    local_bp = tmp_path / bp_template_file
    local_bp.write_text(bp_template_path.read_text())

    wp = deserialize(wp_template_path, Workplan)

    live_steps = [LiveStep.from_step(s) for s in wp.steps]

    for i, step in enumerate(live_steps):
        if i > 0:
            step.directives[NestingDirective.key()] = {"step": wp.steps[i - 1].name}
        attributes = step.model_dump(exclude={"working_dir", "blueprint_path"})
        attributes["working_dir"] = tmp_path / run_id / step.safe_name
        attributes["blueprint"] = local_bp
        live_steps[i] = LiveStep(**attributes)

    live_plan = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    live_wp_path = tmp_path / wp_template_file
    assert serialize(live_wp_path, live_plan)

    await create_mocked_simulation_outputs(wp_template_path, live_wp_path, run_id)

    prior_step = t.cast("LiveStep", live_plan.steps[0])
    prior_fsm = RomsFileSystemManager(prior_step.fsm.root_dir)
    expected_dir = prior_fsm.output_dir
    expected_dir.mkdir(parents=True, exist_ok=True)
    (expected_dir / expected_name).write_text("mock boundary data")

    step = t.cast("LiveStep", live_plan.steps[1])
    directives = t.cast("dict[str, dict[str, str]]", step.directives)
    config = directives[NestingDirective.key()]

    modifier = NestingDirective(config, workplan=live_plan)
    altered = modifier(step)[0]

    bp_after = deserialize(altered.blueprint_path, RomsMarblBlueprint)
    location = Path(bp_after.forcing.boundary.data[0].location)
    assert location.is_relative_to(expected_dir)
    assert location.name == expected_name


async def test_continuance_directive_resolves_step_output_for_non_roms_marbl_source(
    tmp_path: Path,
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify `continue-from: {step: <name>}` resolves a restart-style file
    produced under a referenced step's `output` directory even when that
    step's own blueprint is not a `RomsMarblBlueprint`, mirroring the
    tutorial's nest_ic-conversion-step -> ROMS-MARBL-child migration path.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.
    """
    run_id = mock_run_id

    nest_ic_step = LiveStep(
        name="nest_ic",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "nest_ic",
    )
    nest_fsm = RomsFileSystemManager(nest_ic_step.fsm.root_dir)
    nest_fsm.prepare()

    ic_file = nest_fsm.output_dir / "ic_from_parent_rst.20120101000000.nc"
    ic_file.write_text("mock restart data")

    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    child_bp_path = tmp_path / "child_bp.yaml"
    child_bp_path.write_text(
        bp_tpl_path.read_text().replace("working_dir: .", f"working_dir: {tmp_path}")
    )

    child_step = LiveStep(
        name="child",
        application="roms_marbl",
        blueprint=child_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "child",
        directives={
            ContinuanceDirective.key(): {ContinuanceDirective.KEY_STEP: "nest_ic"}
        },
    )

    live_plan = LiveWorkplan(
        name="nest-ic-plan",
        description="mocked nest_ic to child plan",
        steps=[nest_ic_step, child_step],
    )

    config = t.cast("dict[str, str]", child_step.directives[ContinuanceDirective.key()])
    modifier = ContinuanceDirective(config, workplan=live_plan)
    altered = modifier(child_step)[0]

    bp_after = deserialize(altered.blueprint_path, RomsMarblBlueprint)
    location = Path(bp_after.initial_conditions.data[0].location)
    assert location == ic_file.resolve()


async def test_continuance_directive_step_output_dir_missing(
    tmp_path: Path,
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify `continue-from: {step: <name>}` raises `FileNotFoundError`
    when the referenced step has no `output` directory yet (it has not run,
    or failed before producing output).

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.
    """
    run_id = mock_run_id

    # deliberately never `.prepare()`d: no `output` directory exists on disk
    parent_step = LiveStep(
        name="parent",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "parent",
    )

    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    child_bp_path = tmp_path / "child_bp.yaml"
    child_bp_path.write_text(
        bp_tpl_path.read_text().replace("working_dir: .", f"working_dir: {tmp_path}")
    )

    child_step = LiveStep(
        name="child",
        application="roms_marbl",
        blueprint=child_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "child",
        directives={
            ContinuanceDirective.key(): {ContinuanceDirective.KEY_STEP: "parent"}
        },
    )

    live_plan = LiveWorkplan(
        name="missing-output-plan",
        description="parent step with no output directory",
        steps=[parent_step, child_step],
    )

    config = t.cast("dict[str, str]", child_step.directives[ContinuanceDirective.key()])

    with pytest.raises(FileNotFoundError, match="no output directory"):
        ContinuanceDirective(config, workplan=live_plan)


async def test_continuance_directive_step_output_rejects_partitioned(
    tmp_path: Path,
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify `continue-from: {step: <name>}` raises `FileNotFoundError`
    pointing at `cstar admin migrate-outputs` when the referenced step's
    `output` holds only a legacy partition piece.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.
    """
    run_id = mock_run_id

    parent_step = LiveStep(
        name="parent",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "parent",
    )
    parent_fsm = RomsFileSystemManager(parent_step.fsm.root_dir)
    parent_fsm.prepare()
    (parent_fsm.output_dir / "output_rst.20120201000000.000.nc").write_text(
        "mock partitioned restart data"
    )

    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    child_bp_path = tmp_path / "child_bp.yaml"
    child_bp_path.write_text(
        bp_tpl_path.read_text().replace("working_dir: .", f"working_dir: {tmp_path}")
    )

    child_step = LiveStep(
        name="child",
        application="roms_marbl",
        blueprint=child_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "child",
        directives={
            ContinuanceDirective.key(): {ContinuanceDirective.KEY_STEP: "parent"}
        },
    )

    live_plan = LiveWorkplan(
        name="partitioned-output-plan",
        description="parent step whose output holds only a partition piece",
        steps=[parent_step, child_step],
    )

    config = t.cast("dict[str, str]", child_step.directives[ContinuanceDirective.key()])

    with pytest.raises(FileNotFoundError, match="migrate-outputs"):
        ContinuanceDirective(config, workplan=live_plan)


def _plan_with_parent_restarts(
    tmp_path: Path,
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
    restart_names: Sequence[str],
    directive: dict[str, t.Any],
) -> tuple[LiveWorkplan, LiveStep, Path]:
    """Build a `parent` -> `child` plan whose parent `output` holds mock restarts.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    restart_names : Sequence[str]
        The names of the files to create in the parent's `output` directory.
    directive : dict[str, t.Any]
        The `continue-from` configuration of the `child` step.

    Returns
    -------
    tuple[LiveWorkplan, LiveStep, Path]
        The plan, its `child` step, and the parent's `output` directory.
    """
    parent_step = LiveStep(
        name="parent",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "parent",
    )
    parent_fsm = RomsFileSystemManager(parent_step.fsm.root_dir)
    parent_fsm.prepare()
    for name in restart_names:
        (parent_fsm.output_dir / name).write_text("mock restart data")

    child_bp_path = tmp_path / "child_bp.yaml"
    child_bp_path.write_text(
        (bp_templates_dir / "blueprint.yaml")
        .read_text()
        .replace("working_dir: .", f"working_dir: {tmp_path}")
    )
    child_step = LiveStep(
        name="child",
        application="roms_marbl",
        blueprint=child_bp_path.as_posix(),
        working_dir=tmp_path / "child",
        directives={ContinuanceDirective.key(): directive},
    )
    live_plan = LiveWorkplan(
        name="parent-restarts-plan",
        description="a child continuing from restarts held by its parent",
        steps=[parent_step, child_step],
    )
    return live_plan, child_step, parent_fsm.output_dir


@pytest.mark.parametrize(
    ("restart_names", "directive_extra", "expected_name", "expected_start"),
    [
        pytest.param(
            ["output_rst.20120115000000.nc", "output_rst.20120201000000.nc"],
            {ContinuanceDirective.KEY_TIMESTAMP: "2012-01-15 00:00:00"},
            "output_rst.20120115000000.nc",
            datetime(2012, 1, 15),
            id="timestamp selects an older restart",
        ),
        pytest.param(
            ["output_rst.20120115000000.nc", "output_rst.20120201000000.nc"],
            {},
            "output_rst.20120201000000.nc",
            datetime(2012, 2, 1),
            id="no timestamp keeps the latest",
        ),
        pytest.param(
            ["output_rst.20120201000000.000.nc", "output_rst.20120115000000.nc"],
            {ContinuanceDirective.KEY_TIMESTAMP: "2012-01-15 00:00:00"},
            "output_rst.20120115000000.nc",
            datetime(2012, 1, 15),
            id="partition piece at another timestamp is not selected",
        ),
    ],
)
def test_continuance_directive_step_timestamp_selects_restart(
    tmp_path: Path,
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
    restart_names: list[str],
    directive_extra: dict[str, str],
    expected_name: str,
    expected_start: datetime,
) -> None:
    """Verify `continue-from: {step: <name>, timestamp: <ts>}` continues from
    the restart dated `<ts>` in the referenced step's `output`, starting the
    child at that restart's date, and from the latest restart without a
    `timestamp`.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    restart_names : list[str]
        The names of the files in the parent's `output` directory.
    directive_extra : dict[str, str]
        The keys added to the directive's `step` source.
    expected_name : str
        The name of the file the child is expected to continue from.
    expected_start : datetime
        The `start_date` the child is expected to end up with.
    """
    live_plan, child_step, parent_output = _plan_with_parent_restarts(
        tmp_path,
        bp_templates_dir,
        hello_world_bp_path,
        restart_names,
        {ContinuanceDirective.KEY_STEP: "parent", **directive_extra},
    )

    config = t.cast(
        "dict[str, t.Any]", child_step.directives[ContinuanceDirective.key()]
    )
    altered = ContinuanceDirective(config, workplan=live_plan)(child_step)[0]

    bp_after = deserialize(altered.blueprint_path, RomsMarblBlueprint)
    location = Path(bp_after.initial_conditions.data[0].location)
    assert location == (parent_output / expected_name).resolve()
    assert bp_after.runtime_params.start_date == expected_start


def test_continuance_directive_step_timestamp_not_found(
    tmp_path: Path,
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify `continue-from: {step: <name>, timestamp: <ts>}` raises
    `FileNotFoundError` listing the timestamps that exist, without the
    legacy-layout hint, when the referenced step's `output` holds no restart
    dated `<ts>`.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    live_plan, child_step, _ = _plan_with_parent_restarts(
        tmp_path,
        bp_templates_dir,
        hello_world_bp_path,
        ["output_rst.20120115000000.nc", "output_rst.20120301000000.nc"],
        {
            ContinuanceDirective.KEY_STEP: "parent",
            ContinuanceDirective.KEY_TIMESTAMP: "2012-02-01 00:00:00",
        },
    )
    config = t.cast(
        "dict[str, t.Any]", child_step.directives[ContinuanceDirective.key()]
    )

    with pytest.raises(FileNotFoundError) as error:
        ContinuanceDirective(config, workplan=live_plan)

    assert "2012-02-01 00:00:00" in str(error.value)
    assert "2012-01-15 00:00:00, 2012-03-01 00:00:00" in str(error.value)
    assert "migrate-outputs" not in str(error.value)


def test_continuance_directive_step_timestamp_no_restarts(
    tmp_path: Path,
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify `continue-from: {step: <name>, timestamp: <ts>}` still points at
    `cstar admin migrate-outputs` when the referenced step's `output` holds no
    restarts at all, rather than only listing no timestamps.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    live_plan, child_step, _ = _plan_with_parent_restarts(
        tmp_path,
        bp_templates_dir,
        hello_world_bp_path,
        [],
        {
            ContinuanceDirective.KEY_STEP: "parent",
            ContinuanceDirective.KEY_TIMESTAMP: "2012-02-01 00:00:00",
        },
    )
    config = t.cast(
        "dict[str, t.Any]", child_step.directives[ContinuanceDirective.key()]
    )

    with pytest.raises(FileNotFoundError, match="migrate-outputs"):
        ContinuanceDirective(config, workplan=live_plan)


def test_nesting_directive_step_output_rejects_partitioned(
    tmp_path: Path,
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify `nest-from: {step: <name>}` raises `FileNotFoundError` pointing
    at `cstar admin migrate-outputs` when the referenced step's `output`
    holds only a legacy partition piece.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.
    """
    run_id = mock_run_id

    parent_step = LiveStep(
        name="parent",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "parent",
    )
    parent_fsm = RomsFileSystemManager(parent_step.fsm.root_dir)
    parent_fsm.prepare()
    (parent_fsm.output_dir / "parent_bry.20230201003000.000.nc").write_text(
        "mock partitioned boundary data"
    )

    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    child_bp_path = tmp_path / "child_bp.yaml"
    child_bp_path.write_text(
        bp_tpl_path.read_text().replace("working_dir: .", f"working_dir: {tmp_path}")
    )

    child_step = LiveStep(
        name="child",
        application="roms_marbl",
        blueprint=child_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "child",
        directives={NestingDirective.key(): {NestingDirective.KEY_STEP: "parent"}},
    )

    live_plan = LiveWorkplan(
        name="partitioned-boundary-plan",
        description="parent step whose output holds only a partition piece",
        steps=[parent_step, child_step],
    )

    config = t.cast("dict[str, str]", child_step.directives[NestingDirective.key()])

    with pytest.raises(FileNotFoundError, match="migrate-outputs"):
        NestingDirective(config, workplan=live_plan)


def test_nesting_directive_path_allows_partitioned(
    single_step_workplan: Workplan,
    tmp_path: Path,
) -> None:
    """Verify `nest-from: {path: <dir>}` still succeeds against a directory
    holding only a legacy partition piece -- the partitioned-output guard
    (`_reject_partitioned_step_output`) applies only to `step:` sources.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    tmp_path : Path
        Temporary directory used to hold a mocked, partitioned boundary file.
    """
    bry_dir = tmp_path / "bry"
    bry_dir.mkdir()
    (bry_dir / "parent_bry.20230201003000.000.nc").write_text("mock boundary data")

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()

    transform = NestingDirective({NestingDirective.KEY_PATH: str(bry_dir)})
    steps = transform(step)

    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)
    assert Path(bp_after.forcing.boundary.data[0].location).name == (
        "parent_bry.20230201003000.000.nc"
    )


def test_nesting_directive_bry_path_deprecated_matches_path(
    single_step_workplan: Workplan,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify the deprecated `bry_path` key warns (`FutureWarning` and a log
    warning) and produces the same boundary override as the equivalent
    `path` config.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    tmp_path : Path
        Temporary directory used to hold a mocked boundary file.
    caplog : pytest.LogCaptureFixture
        Captures log records emitted during the test.
    """
    bry_dir = tmp_path / "bry"
    bry_dir.mkdir()
    (bry_dir / "parent_bry.20230201003000.nc").write_text("mock boundary data")

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()

    path_transform = NestingDirective({NestingDirective.KEY_PATH: str(bry_dir)})
    bp_via_path = deserialize(
        path_transform(step)[0].blueprint_path, RomsMarblBlueprint
    )

    with (
        caplog.at_level(logging.WARNING, logger=TRANSFORMS_LOGGER_NAME),
        pytest.warns(FutureWarning, match="bry_path"),
    ):
        bry_transform = NestingDirective({NestingDirective.KEY_BRY_PATH: str(bry_dir)})

    records = [r for r in caplog.records if r.name == TRANSFORMS_LOGGER_NAME]
    assert any("bry_path" in r.getMessage() for r in records)

    bp_via_bry = deserialize(bry_transform(step)[0].blueprint_path, RomsMarblBlueprint)

    assert bp_via_bry.forcing.boundary == bp_via_path.forcing.boundary


def test_nesting_directive_rst_path_deprecated_still_applies_restart(
    single_step_workplan: Workplan,
    mocked_simulation_outputs: tuple[Path, Path, Path],
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify the deprecated `rst_path` key warns (`FutureWarning` and a log
    warning) and still applies the restart override (regression guard for
    the deprecation period).

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    mocked_simulation_outputs : tuple[Path, Path, Path]
        Paths to mocked simulation outputs; the latest restart there is dated
        2012-01-01T00:20:00.
    tmp_path : Path
        Temporary directory used to hold a mocked boundary file.
    caplog : pytest.LogCaptureFixture
        Captures log records emitted during the test.
    """
    _, rst_dir, _ = mocked_simulation_outputs

    bry_dir = tmp_path / "bry"
    bry_dir.mkdir()
    (bry_dir / "parent_bry.20230201003000.nc").write_text("mock boundary data")

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()

    with (
        caplog.at_level(logging.WARNING, logger=TRANSFORMS_LOGGER_NAME),
        pytest.warns(FutureWarning, match="rst_path"),
    ):
        transform = NestingDirective(
            {
                NestingDirective.KEY_RST_PATH: str(rst_dir),
                NestingDirective.KEY_PATH: str(bry_dir),
            }
        )

    records = [r for r in caplog.records if r.name == TRANSFORMS_LOGGER_NAME]
    assert any("rst_path" in r.getMessage() for r in records)

    steps = transform(step)
    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)

    assert bp_after.runtime_params.start_date == datetime(2012, 1, 1, 0, 20, 0)
    assert Path(bp_after.forcing.boundary.data[0].location).name == (
        "parent_bry.20230201003000.nc"
    )


def test_nesting_directive_rst_path_conflicts_with_continue_from(
    single_step_workplan: Workplan,
    mocked_simulation_outputs: tuple[Path, Path, Path],
    tmp_path: Path,
) -> None:
    """Verify a `nest-from` `rst_path` combined with a `continue-from`
    directive on the same step raises `ValueError` at call time.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    mocked_simulation_outputs : tuple[Path, Path, Path]
        Paths to mocked simulation outputs; used as both the (unused)
        continue-from source and the nest-from rst_path source.
    tmp_path : Path
        Temporary directory used to hold a mocked boundary file.
    """
    _, rst_dir, _ = mocked_simulation_outputs

    bry_dir = tmp_path / "bry"
    bry_dir.mkdir()
    (bry_dir / "parent_bry.20230201003000.nc").write_text("mock boundary data")

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()
    step.directives[ContinuanceDirective.key()] = {
        ContinuanceDirective.KEY_PATH: str(rst_dir)
    }

    with pytest.warns(FutureWarning):
        transform = NestingDirective(
            {
                NestingDirective.KEY_RST_PATH: str(rst_dir),
                NestingDirective.KEY_PATH: str(bry_dir),
            }
        )

    with pytest.raises(ValueError, match="continue-from"):
        transform(step)


def test_nesting_directive_no_boundary_source_raises() -> None:
    """Verify configuration lacking `path`, `step`, and `bry_path` is rejected."""
    with pytest.raises(NotImplementedError, match="supported"):
        NestingDirective({"not-a-boundary-key": "value"})


def test_nesting_directive_bry_path_conflicts_with_path(tmp_path: Path) -> None:
    """Verify supplying `bry_path` alongside `path` is rejected."""
    bry_dir = tmp_path / "bry"
    bry_dir.mkdir()

    with (
        pytest.warns(FutureWarning),
        pytest.raises(NotImplementedError, match="conflicts"),
    ):
        NestingDirective(
            {
                NestingDirective.KEY_PATH: str(bry_dir),
                NestingDirective.KEY_BRY_PATH: str(bry_dir),
            }
        )


def test_nesting_directive_path_and_step_are_mutually_exclusive(tmp_path: Path) -> None:
    """Verify supplying both `path` and `step` is rejected instead of `step` silently winning."""
    with pytest.raises(NotImplementedError, match="mutually exclusive"):
        NestingDirective(
            {
                NestingDirective.KEY_PATH: str(tmp_path),
                NestingDirective.KEY_STEP: "outer",
            }
        )


def test_nesting_directive_extra_unknown_key_rejected(tmp_path: Path) -> None:
    """Verify an unrecognized key alongside a valid `path` is rejected, not
    silently ignored.
    """
    with pytest.raises(NotImplementedError, match="supported"):
        NestingDirective(
            {
                NestingDirective.KEY_PATH: str(tmp_path),
                "unknown-key": "value",
            }
        )


def test_nesting_directive_rst_path_alone_has_no_boundary_source(
    tmp_path: Path,
) -> None:
    """Verify `rst_path` alone (a recognized key, but not itself a boundary
    source) is rejected for lacking a boundary source, not treated as an
    unrecognized key.

    Config-shape validation runs before the deprecated-key warnings, so no
    `FutureWarning` is emitted for this rejected config.
    """
    rst_dir = tmp_path / "rst"
    rst_dir.mkdir()

    with pytest.raises(NotImplementedError, match="supported"):
        NestingDirective({NestingDirective.KEY_RST_PATH: str(rst_dir)})


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("a; b", ["a", "b"]),
        ("a ;b ; c;d", ["a", "b", "c", "d"]),
        ("a;", ["a"]),
        (" ; ", []),
        ("single", ["single"]),
        ("", []),
    ],
)
def test_split_sources(value: str, expected: list[str]) -> None:
    """Verify `_split_sources` trims whitespace and drops empty tokens.

    Parameters
    ----------
    value : str
        The delimited scalar to split.
    expected : list[str]
        The expected ordered list of tokens.
    """
    assert _split_sources(value, ";") == expected


def test_nesting_directive_multiple_paths(
    single_step_workplan: Workplan,
    tmp_path: Path,
) -> None:
    """Verify a `path` value with multiple `;`-delimited sources combines
    boundary files from each source, in the order given, replacing the
    template's static boundary entries wholesale.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    tmp_path : Path
        Temporary directory used to hold two mocked boundary sources.
    """
    dir_a = tmp_path / "a"
    dir_a.mkdir()
    (dir_a / "parent_bry.20230201003000.nc").write_text("mock boundary data")

    dir_b = tmp_path / "b"
    dir_b.mkdir()
    (dir_b / "sibling_bry.20230301003000.nc").write_text("mock boundary data")

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()

    transform = NestingDirective({NestingDirective.KEY_PATH: f"{dir_a} ; {dir_b}"})
    steps = transform(step)

    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)
    assert [Path(d.location).name for d in bp_after.forcing.boundary.data] == [
        "parent_bry.20230201003000.nc",
        "sibling_bry.20230301003000.nc",
    ]
    assert all(getattr(d, "hash", None) is None for d in bp_after.forcing.boundary.data)


def test_nesting_directive_multiple_steps(
    tmp_path: Path,
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify a `step` value with multiple `;`-delimited step names combines
    boundary files from each step's output directory, in the order given.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.
    """
    run_id = mock_run_id

    first_step = LiveStep(
        name="first",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "first",
    )
    first_fsm = RomsFileSystemManager(first_step.fsm.root_dir)
    first_fsm.prepare()
    (first_fsm.output_dir / "first_bry.20230201003000.nc").write_text(
        "mock boundary data"
    )

    second_step = LiveStep(
        name="second",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "second",
    )
    second_fsm = RomsFileSystemManager(second_step.fsm.root_dir)
    second_fsm.prepare()
    (second_fsm.output_dir / "second_bry.20230301003000.nc").write_text(
        "mock boundary data"
    )

    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    child_bp_path = tmp_path / "child_bp.yaml"
    child_bp_path.write_text(
        bp_tpl_path.read_text().replace("working_dir: .", f"working_dir: {tmp_path}")
    )

    child_step = LiveStep(
        name="child",
        application="roms_marbl",
        blueprint=child_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "child",
        directives={
            NestingDirective.key(): {NestingDirective.KEY_STEP: "first; second ;"}
        },
    )

    live_plan = LiveWorkplan(
        name="multiple-steps-plan",
        description="two parent steps supplying boundary files",
        steps=[first_step, second_step, child_step],
    )

    config = t.cast("dict[str, str]", child_step.directives[NestingDirective.key()])
    transform = NestingDirective(config, workplan=live_plan)
    steps = transform(child_step)

    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)
    data = bp_after.forcing.boundary.data
    assert len(data) == 2
    assert Path(data[0].location).is_relative_to(first_fsm.output_dir)
    assert Path(data[1].location).is_relative_to(second_fsm.output_dir)


def test_nesting_directive_multiple_steps_missing_output_raises(
    tmp_path: Path,
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify a `step` value listing a step with no output directory raises
    `FileNotFoundError` naming that step, even when an earlier listed step
    resolves fine.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.
    """
    run_id = mock_run_id

    first_step = LiveStep(
        name="first",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "first",
    )
    first_fsm = RomsFileSystemManager(first_step.fsm.root_dir)
    first_fsm.prepare()
    (first_fsm.output_dir / "first_bry.20230201003000.nc").write_text(
        "mock boundary data"
    )

    second_step = LiveStep(
        name="second",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "second",
    )

    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    child_bp_path = tmp_path / "child_bp.yaml"
    child_bp_path.write_text(
        bp_tpl_path.read_text().replace("working_dir: .", f"working_dir: {tmp_path}")
    )

    child_step = LiveStep(
        name="child",
        application="roms_marbl",
        blueprint=child_bp_path.as_posix(),
        working_dir=tmp_path / run_id / "child",
        directives={
            NestingDirective.key(): {NestingDirective.KEY_STEP: "first; second"}
        },
    )

    live_plan = LiveWorkplan(
        name="multiple-steps-missing-output-plan",
        description="second parent step has no output directory",
        steps=[first_step, second_step, child_step],
    )

    config = t.cast("dict[str, str]", child_step.directives[NestingDirective.key()])

    with pytest.raises(FileNotFoundError, match="second"):
        NestingDirective(config, workplan=live_plan)


def test_nesting_directive_duplicate_paths_deduplicated(
    single_step_workplan: Workplan,
    tmp_path: Path,
) -> None:
    """Verify listing the same source twice yields one boundary entry, not two.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    tmp_path : Path
        Temporary directory used to hold a mocked boundary source.
    """
    bry_dir = tmp_path / "bry"
    bry_dir.mkdir()
    (bry_dir / "parent_bry.20230201003000.nc").write_text("mock boundary data")

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()

    transform = NestingDirective({NestingDirective.KEY_PATH: f"{bry_dir}; {bry_dir}"})
    steps = transform(step)

    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)
    assert len(bp_after.forcing.boundary.data) == 1


def test_nesting_directive_empty_sources_raises() -> None:
    """Verify a `path` value that is empty after splitting is rejected."""
    with pytest.raises(NotImplementedError, match="no boundary source"):
        NestingDirective({NestingDirective.KEY_PATH: " ; "})


def test_nesting_directive_bry_path_multiple_paths(
    single_step_workplan: Workplan,
    tmp_path: Path,
) -> None:
    """Verify the deprecated `bry_path` key also accepts multiple
    `;`-delimited sources, combining boundary files from each.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    tmp_path : Path
        Temporary directory used to hold two mocked boundary sources.
    """
    dir_a = tmp_path / "a"
    dir_a.mkdir()
    (dir_a / "parent_bry.20230201003000.nc").write_text("mock boundary data")

    dir_b = tmp_path / "b"
    dir_b.mkdir()
    (dir_b / "sibling_bry.20230301003000.nc").write_text("mock boundary data")

    step = single_step_workplan.steps[0]
    step.blueprint_overrides.clear()

    with pytest.warns(FutureWarning, match="bry_path"):
        transform = NestingDirective(
            {NestingDirective.KEY_BRY_PATH: f"{dir_a} ; {dir_b}"}
        )

    steps = transform(step)
    bp_after = deserialize(steps[0].blueprint_path, RomsMarblBlueprint)
    assert len(bp_after.forcing.boundary.data) == 2


def test_workplan_transformer_applies_working_dir_overrides(
    tmp_path: Path,
    step_overiding_wp: Workplan,
    test_working_dir: Path,
    test_working_dir_override: Path,
) -> None:
    """Verify that the workplan transformer packages the working-dir override
    for a step without an active app transform, rather than rewriting its
    blueprint file.

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

    # the step keeps its original blueprint path; nothing is rewritten to disk
    assert str(step_trx.blueprint_path) == str(original_bp_path)

    # user overrides moved out of the step and into the runtime directive
    assert not step_trx.blueprint_overrides

    directives = t.cast("dict[str, dict[str, t.Any]]", step_trx.directives)
    assert ApplyOverridesDirective.key() in directives

    config = directives[ApplyOverridesDirective.key()]
    overrides = config[ApplyOverridesDirective.KEY_OVERRIDES]

    # user-supplied override keys are carried into the directive payload
    assert "runtime_params" in overrides

    # the system-level override took precedence over the user-supplied value
    assert overrides["working_dir"] == sys_working_dir_override.as_posix()
    assert overrides["working_dir"] != dir_orig
    assert overrides["working_dir"] != original_override

    assert config[ApplyOverridesDirective.KEY_APPLICATION] == step_orig.application


def test_package_runtime_overrides_orders_apply_overrides_first(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify apply-overrides is packaged ahead of the step's own directives.

    Directives run in mapping order at runtime; a content directive running
    before apply-overrides would persist its intermediate blueprint into the
    raw blueprint's working_dir.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = LiveStep(
        name="ordering-step",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "step-root",
        directives={ContinuanceDirective.key(): {"path": "prior/run"}},
    )

    packaged = package_runtime_overrides(step)

    assert list(packaged.directives) == [
        ApplyOverridesDirective.key(),
        ContinuanceDirective.key(),
    ]


def test_collect_directive_problems_rejects_nest_from_rst_path_with_continue_from(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify `collect_directive_problems` reports a `nest-from` `rst_path`
    combined with `continue-from` on the same step at schedule time, before
    the workplan is submitted (rather than failing later on the compute
    node).

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a blueprint file; `collect_directive_problems`
        never reads it, so any existing file works.
    """
    step = LiveStep(
        name="ordering-step",
        application="roms_marbl",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "step-root",
        directives={
            ContinuanceDirective.key(): {"path": "prior/run"},
            NestingDirective.key(): {
                NestingDirective.KEY_RST_PATH: "prior/run",
                NestingDirective.KEY_BRY_PATH: "prior/bry",
            },
        },
    )

    problems = collect_directive_problems([step])

    assert any("continue-from" in problem for problem in problems)


def test_collect_directive_problems_allows_boundary_only_nest_from(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a boundary-only `nest-from` (no `rst_path`) passes schedule-time
    validation even when `continue-from` is also present on the step.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a blueprint file; `collect_directive_problems`
        never reads it, so any existing file works.
    """
    step = LiveStep(
        name="ordering-step",
        application="roms_marbl",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "step-root",
        directives={
            ContinuanceDirective.key(): {"path": "prior/run"},
            NestingDirective.key(): {NestingDirective.KEY_PATH: "prior/bry"},
        },
    )

    assert collect_directive_problems([step]) == []


def test_collect_directive_problems_rejects_malformed_continue_from_timestamp(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify `collect_directive_problems` reports a malformed `continue-from`
    `timestamp` at schedule time, naming the step and the directive, before any
    step runs.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a blueprint file; `collect_directive_problems`
        never reads it, so any existing file works.
    """
    step = LiveStep(
        name="s1",
        application="roms_marbl",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "s1",
        directives={
            ContinuanceDirective.key(): {
                ContinuanceDirective.KEY_PATH: "prior/run",
                ContinuanceDirective.KEY_TIMESTAMP: "2012-02",
            }
        },
    )

    problems = collect_directive_problems([step])

    assert len(problems) == 1
    assert problems[0].startswith("step 's1' directive 'continue-from'")
    assert ContinuanceDirective.KEY_TIMESTAMP in problems[0]


def test_package_runtime_overrides_no_longer_validates_directives(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify `package_runtime_overrides` no longer validates directive
    configuration -- that responsibility moved to `collect_directive_problems`,
    called once by `WorkplanTransformer.apply` before any step is packaged.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = LiveStep(
        name="ordering-step",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "step-root",
        directives={
            ContinuanceDirective.key(): {"path": "prior/run"},
            NestingDirective.key(): {
                NestingDirective.KEY_RST_PATH: "prior/run",
                NestingDirective.KEY_BRY_PATH: "prior/bry",
            },
        },
    )

    packaged = package_runtime_overrides(step)

    assert list(packaged.directives) == [
        ApplyOverridesDirective.key(),
        ContinuanceDirective.key(),
        NestingDirective.key(),
    ]


def test_collect_directive_problems_unknown_key_on_hello_world_step(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify an unrecognized directive key on a `hello_world` step (which
    declares no directives) is reported, naming the step, the key, and the
    application's allowed keys.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = LiveStep(
        name="s1",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "s1",
        directives={"not-a-directive": {"key": "value"}},
    )

    problems = collect_directive_problems([step])

    assert len(problems) == 1
    assert "s1" in problems[0]
    assert "not-a-directive" in problems[0]
    assert ApplyOverridesDirective.key() in problems[0]


def test_collect_directive_problems_roms_directive_on_hello_world_step(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a `roms_marbl` directive (`nest-from`) on a `hello_world` step
    is reported as unknown -- `hello_world` does not declare it.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = LiveStep(
        name="s1",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "s1",
        directives={NestingDirective.key(): {NestingDirective.KEY_PATH: "bry"}},
    )

    problems = collect_directive_problems([step])

    assert len(problems) == 1
    assert NestingDirective.key() in problems[0]


def test_collect_directive_problems_non_mapping_config(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a directive config that is not a mapping is reported.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = LiveStep(
        name="s1",
        application="roms_marbl",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "s1",
        directives={ContinuanceDirective.key(): "not-a-mapping"},
    )

    problems = collect_directive_problems([step])

    assert len(problems) == 1
    assert "mapping" in problems[0]


def test_collect_directive_problems_unknown_step_reference(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a `step:` reference to a step absent from the workplan is reported.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = LiveStep(
        name="s1",
        application="roms_marbl",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "s1",
        directives={
            ContinuanceDirective.key(): {ContinuanceDirective.KEY_STEP: "ghost"}
        },
    )

    problems = collect_directive_problems([step])

    assert len(problems) == 1
    assert "unknown step" in problems[0]
    assert "ghost" in problems[0]


def test_collect_directive_problems_sibling_not_ancestor(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a `step:` reference to a step that exists but is not an
    upstream dependency (via `depends_on`) is reported.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    sibling = LiveStep(
        name="sibling",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "sibling",
    )
    step = LiveStep(
        name="s1",
        application="roms_marbl",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "s1",
        directives={
            ContinuanceDirective.key(): {ContinuanceDirective.KEY_STEP: "sibling"}
        },
    )

    problems = collect_directive_problems([sibling, step])

    assert len(problems) == 1
    assert "not an upstream dependency" in problems[0]


def test_collect_directive_problems_accepts_transitive_ancestor(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a `step:` reference to a transitive ancestor (A -> B -> C, C
    refers to A) passes, not only a direct dependency.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    a = LiveStep(
        name="A",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "A",
    )
    b = LiveStep(
        name="B",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "B",
        depends_on=["A"],
    )
    c = LiveStep(
        name="C",
        application="roms_marbl",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "C",
        depends_on=["B"],
        directives={ContinuanceDirective.key(): {ContinuanceDirective.KEY_STEP: "A"}},
    )

    assert collect_directive_problems([a, b, c]) == []


def test_collect_directive_problems_aggregates_across_steps_and_directives(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify problems from two steps and two directives are reported
    together in one list, not just the first found.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step_a = LiveStep(
        name="a",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "a",
        directives={"not-a-directive": {"key": "value"}},
    )
    step_b = LiveStep(
        name="b",
        application="roms_marbl",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "b",
        directives={
            ContinuanceDirective.key(): {ContinuanceDirective.KEY_STEP: "ghost"},
            NestingDirective.key(): {"not-a-boundary-key": "value"},
        },
    )

    problems = collect_directive_problems([step_a, step_b])

    assert len(problems) == 3


def test_workplan_transformer_reports_all_directive_problems_in_one_error(
    hello_world_bp_path: Path,
) -> None:
    """Verify `WorkplanTransformer.apply` aggregates directive problems from
    every step into a single `ValueError`, rather than failing on the first.

    Uses `hello_world` steps, since unknown-key problems are detected before
    any blueprint is loaded.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step_a = Step(
        name="a",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        directives={"not-a-directive": {"key": "value"}},
    )
    step_b = Step(
        name="b",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        directives={"also-not-a-directive": {"key": "value"}},
    )
    wp = Workplan(
        name="bad-directives-plan",
        description="two steps with unknown directive keys",
        steps=[step_a, step_b],
    )

    with pytest.raises(ValueError, match="2 directive problem") as error:
        WorkplanTransformer(wp).apply()

    assert "not-a-directive" in str(error.value)
    assert "also-not-a-directive" in str(error.value)


def test_nesting_directive_referenced_steps_tokenizes_step_value() -> None:
    """Verify `NestingDirective.referenced_steps` splits `a; b` into ordered
    step-name tokens, matching `_split_sources`.
    """
    config = {NestingDirective.KEY_STEP: "a; b"}
    assert NestingDirective.referenced_steps(config) == ["a", "b"]


def test_apply_directives_applies_overrides_before_content_directives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify apply-overrides runs before other directives even when the
    directive file lists it last (files written before the packaging order
    change do), so no directive writes into the raw blueprint's working_dir.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to clear the run-id environment variable.
    """
    raw_dir = tmp_path / "raw-working-dir"
    override_dir = tmp_path / "overridden-working-dir"

    bp_path = tmp_path / "ordering_bp.yaml"
    bp_path.write_text(
        f"""\
name: ordering
description: directive ordering test
application: hello_world
state: draft
target: 'world'
schema_version: '1.0.0'
working_dir: {raw_dir.as_posix()}
"""
    )

    seen_working_dirs: list[Path] = []

    class RecordingDirective(OverrideDirective):
        """Content directive recording the step working_dir it runs with."""

        @classmethod
        def key(cls) -> str:
            return "record-working-dir"

        def __call__(self, step: LiveStep) -> Sequence[LiveStep]:
            seen_working_dirs.append(step.working_dir)
            return super().__call__(step)

    directive_path = tmp_path / "directives.yaml"
    directive_path.write_text(
        f"""\
directives:
  {RecordingDirective.key()}:
    enabled: true
  {ApplyOverridesDirective.key()}:
    overrides:
      working_dir: {override_dir.as_posix()}
    application: hello_world
"""
    )

    monkeypatch.delenv(ENV_CSTAR_RUNID, raising=False)
    DirectiveConfig.register(RecordingDirective.key(), RecordingDirective)
    try:
        result = DirectiveConfig.apply_directives(str(directive_path), str(bp_path))
    finally:
        DirectiveConfig.directive_map.pop(RecordingDirective.key(), None)

    # the content directive ran with the overridden working_dir...
    assert seen_working_dirs == [override_dir.resolve()]
    # ...its intermediate blueprint landed there, and the raw dir was untouched
    assert Path(result).is_relative_to(override_dir.resolve())
    assert not raw_dir.exists()


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(datetime(2012, 1, 15), id="datetime"),
        pytest.param(20120115000000, id="restart stamp int"),
    ],
)
def test_continue_from_timestamp_survives_directive_file_round_trip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bp_templates_dir: Path,
    value: t.Any,
) -> None:
    """Verify a YAML-typed `continue-from` `timestamp` survives the directive
    file written for a step, and selects the same restart when the directives
    are applied to the blueprint on the compute node.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to clear the run-id environment variable.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    value : t.Any
        The `timestamp` value, as a type YAML writes and reads back natively.
    """
    rst_dir = tmp_path / "prior" / "output"
    rst_dir.mkdir(parents=True)
    for name in ("output_rst.20120115000000.nc", "output_rst.20120201000000.nc"):
        (rst_dir / name).write_text("mock restart data")

    bp_path = tmp_path / "bp.yaml"
    bp_path.write_text(
        (bp_templates_dir / "blueprint.yaml")
        .read_text()
        .replace("working_dir: .", f"working_dir: {tmp_path}")
    )
    step = LiveStep(
        name="step",
        application="roms_marbl",
        blueprint=bp_path.as_posix(),
        working_dir=tmp_path / "step",
        directives={
            ContinuanceDirective.key(): {
                ContinuanceDirective.KEY_PATH: str(rst_dir),
                ContinuanceDirective.KEY_TIMESTAMP: value,
            }
        },
    )
    directive_path = prepare_directive_file(step)

    monkeypatch.delenv(ENV_CSTAR_RUNID, raising=False)
    result = DirectiveConfig.apply_directives(str(directive_path), str(bp_path))

    bp_after = deserialize(Path(result), RomsMarblBlueprint)
    location = Path(bp_after.initial_conditions.data[0].location)
    assert location == (rst_dir / "output_rst.20120115000000.nc").resolve()
    assert bp_after.runtime_params.start_date == datetime(2012, 1, 15)


def test_restore_directive_file_recreates_missing_file(
    tmp_path: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify `restore_directive_file` rewrites a directive file that has been
    lost while its step waited in a scheduler queue, and that the restored
    content deserializes back to the same directives as the original.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    mock_run_id : str
        Fixture exporting a run-id so restoration proceeds past the guard
        that requires an active run.
    """
    step = LiveStep(
        name="restore-me",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "restore-step",
        directives={"continue-from": {"path": "somewhere"}},
    )
    original_path = prepare_directive_file(step)
    original_directives = deserialize(original_path, DirectiveConfig).directives

    # the work directory is cleaned while the step waits in the queue
    shutil.rmtree(original_path.parent)
    assert not original_path.exists()

    live_plan = LiveWorkplan(
        name="restore-workplan",
        description="a live workplan used to restore a missing directive file",
        steps=[step],
    )

    with mock.patch(
        "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
        mock.Mock(return_value=live_plan),
    ):
        restored_path = DirectiveConfig.restore_directive_file(original_path)

    assert restored_path == original_path
    assert restored_path.exists()

    restored_directives = deserialize(restored_path, DirectiveConfig).directives
    assert restored_directives == original_directives


def test_restore_directive_file_no_matching_step_raises(
    tmp_path: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify `restore_directive_file` raises `CstarError` when no step in the
    run's recorded workplan writes its directives to the missing path.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    mock_run_id : str
        Fixture exporting a run-id so restoration proceeds past the guard
        that requires an active run.
    """
    other_step = LiveStep(
        name="unrelated-step",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "unrelated-step",
    )
    live_plan = LiveWorkplan(
        name="restore-workplan",
        description="a live workplan with no step matching the target path",
        steps=[other_step],
    )
    missing_path = tmp_path / "orphaned" / "directives.yaml"

    with (
        mock.patch(
            "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
            mock.Mock(return_value=live_plan),
        ),
        pytest.raises(CstarError, match="no step in the workplan"),
    ):
        DirectiveConfig.restore_directive_file(missing_path)


def test_restore_directive_file_no_run_id_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `restore_directive_file` raises `CstarError` naming
    `CSTAR_RUNID` without ever loading the workplan when no run-id is set.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to clear the run-id environment variable.
    """
    monkeypatch.delenv(ENV_CSTAR_RUNID, raising=False)
    mock_load_workplan = mock.Mock()

    with (
        mock.patch(
            "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
            mock_load_workplan,
        ),
        pytest.raises(CstarError, match=ENV_CSTAR_RUNID),
    ):
        DirectiveConfig.restore_directive_file(tmp_path / "directives.yaml")

    mock_load_workplan.assert_not_called()


def test_restore_directive_file_write_failure_wrapped(
    tmp_path: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify an `OSError` raised while writing the restored file (here, the
    target path is occupied by a directory) is wrapped in a `CstarError`
    naming the step whose file could not be written.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    mock_run_id : str
        Fixture exporting a run-id so restoration proceeds past the guard
        that requires an active run.
    """
    step = LiveStep(
        name="oserror-step",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "oserror-step",
    )
    # occupy the directive file's path with a directory so writing to it fails
    directive_path = step.fsm.run_dir / DIRECTIVES_FILENAME
    directive_path.mkdir(parents=True)

    live_plan = LiveWorkplan(
        name="restore-workplan",
        description="a live workplan whose step's directive file cannot be written",
        steps=[step],
    )

    with (
        mock.patch(
            "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
            mock.Mock(return_value=live_plan),
        ),
        pytest.raises(CstarError, match=f"step {step.name!r} could not be written"),
    ):
        DirectiveConfig.restore_directive_file(directive_path)


def test_restore_directive_file_wraps_load_workplan_error(
    tmp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify a failure loading the recorded workplan is wrapped in a
    `CstarError` that chains the original exception as its cause.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    mock_run_id : str
        Fixture exporting a run-id so restoration proceeds past the guard
        that requires an active run.
    """
    original = RuntimeError("no run context available")

    with (
        mock.patch(
            "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
            mock.Mock(side_effect=original),
        ),
        pytest.raises(CstarError, match="could not be loaded") as exc_info,
    ):
        DirectiveConfig.restore_directive_file(tmp_path / "directives.yaml")

    assert exc_info.value.__cause__ is original


@pytest.fixture
def live_step_with_templates(tmp_path: Path) -> LiveStep:
    """A minimal LiveStep whose blueprint_overrides contain template placeholders."""
    bp = tmp_path / "bp.yaml"
    bp.touch()
    step = Step(
        name="fill-step",
        application="roms_marbl",
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


@pytest.mark.parametrize(
    ("use_var", "exp_value"),
    [
        ("v0", "/data/1/input"),
        ("v1", "/data/2/input"),
        ("v2", "/data/3/input"),
    ],
)
def test_template_fill_variable_substitution(
    live_step_with_templates: LiveStep,
    use_var: str,
    exp_value: str,
) -> None:
    """Verify that variable-only tokens (e.g. `{{name}}`) are replaced
    using the variable resolver.
    """
    variables = {"v0": "/data/1", "v1": "/data/2", "v2": "/data/3"}
    transform = TemplateFillTransform(variable_resolver=variables.__getitem__)

    # create a template that will use the variable resolver
    step = LiveStep.from_step(
        live_step_with_templates,
        update={
            "blueprint_overrides": {
                # use `{{baseX}}/input` to ensure `/input` part is not modified
                "input_dir": f"{mustache(f'{use_var}')}/input"
            }
        },
    )
    (result,) = transform(step)
    assert result.blueprint_overrides["input_dir"] == exp_value


@pytest.mark.parametrize(
    ("purpose", "exp_value", "wd_name"),
    [
        ("root_dir", "my-test-path1", "my-test-path1"),
        ("input_dir", "my-test-path2/input", "my-test-path2"),
        ("run_dir", "my-test-path3/work", "my-test-path3"),
        ("tasks_dir", "my-test-path4/tasks", "my-test-path4"),
        ("logs_dir", "my-test-path5/logs", "my-test-path5"),
        ("output_dir", "my-test-path6/output", "my-test-path6"),
    ],
)
def test_template_fill_scoped_resolver(
    live_step_with_templates: LiveStep,
    tmp_path: Path,
    purpose: str,
    exp_value: str,
    wd_name: str,
) -> None:
    """Verify `{{<fsm-attr>: step_name}}` tokens are replaced using the scoped resolver.

    Varies the `working_dir` on the upstream (source) step to ensure it's not
    "getting lucky" by using a default working directory.
    """
    upstream_dir = tmp_path / wd_name
    KEY_WD: t.Final[str] = "working_dir"

    step1 = LiveStep.from_step(
        live_step_with_templates,
        update={
            "name": str(uuid.uuid4()),
            KEY_WD: upstream_dir,
        },
    )
    step2 = LiveStep.from_step(
        live_step_with_templates,
        update={
            "blueprint_overrides": {
                KEY_WD: mustache(f"{purpose}: {step1.name}"),
            },
        },
    )
    resolver = get_fsm_resolver([step1, step2], ExternalRuns({}))

    transform = TemplateFillTransform(scoped_resolver=resolver)
    (result,) = transform(step2)

    source_value = str(getattr(step1.fsm, purpose))
    actual_value = str(result.blueprint_overrides[KEY_WD])

    assert actual_value == source_value
    assert actual_value.endswith(exp_value)
    assert Path(actual_value).is_relative_to(upstream_dir)


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

    resolver = get_fsm_resolver([step1, step2], ExternalRuns({}))
    transform = TemplateFillTransform(scoped_resolver=resolver)

    with pytest.raises(KeyError, match="unknown step"):
        _ = transform(step2)


def test_template_fill_variable_resolver_unknown_variable(
    live_step_with_templates: LiveStep,
) -> None:
    """Verify that the fill transform raises a value error if an attempt to access
    an unknown variable on a variable resolver is encountered.

    E.g. Template contains {{typo-var}} but variable is named "ok-var" in the workplan.
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
    transform = TemplateFillTransform(variable_resolver=lambda name: named_config[name])

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
    resolver = get_fsm_resolver([step], ExternalRuns({}))

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

    resolver = get_fsm_resolver([step1, step2], ExternalRuns({}))
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
    ("use_var", "use_placeholder", "exp_resolved_var", "exp_resolved_ph"),
    [
        ("var1", "ph1", "123", "ABC/final_output"),
        ("var1", "ph2", "123", "DEF/final_output"),
        ("var1", "ph3", "123", "GHI/final_output"),
        ("var2", "ph1", "XYZ", "ABC/final_output"),
        ("var2", "ph2", "XYZ", "DEF/final_output"),
        ("var2", "ph3", "XYZ", "GHI/final_output"),
        ("var3", "ph1", "PQR", "ABC/final_output"),
        ("var3", "ph2", "PQR", "DEF/final_output"),
        ("var3", "ph3", "PQR", "GHI/final_output"),
    ],
)
def test_template_fill_combined_resolvers(
    live_step_with_templates: LiveStep,
    use_var: str,
    use_placeholder: str,
    exp_resolved_var: str,
    exp_resolved_ph: str,
) -> None:
    """Ensure that variable and scope resolvers both operate in the same transform pass.

    Parammeters
    -----------
    use_var: str
        The mock name of variable that will be resolved by the variable resolver.
    use_placeholder : str
        The mock name of a `Step` that will be resolved by the scoped resolver.
    exp_resolved_var : str
        The expected value after resolving the variable.
    exp_resolved_ph : str
        The expected value after resolving the placeholder.
    """
    variables = {"var1": "123", "var2": "XYZ", "var3": "PQR"}
    placeholders = {"ph1": "ABC", "ph2": "DEF", "ph3": "GHI"}

    def scoped_resolver(placeholder: str, scope_: str) -> str:
        """Return from the mocked resolver data in `placeholders`."""
        return placeholders[placeholder]

    transform = TemplateFillTransform(
        variable_resolver=variables.__getitem__,
        scoped_resolver=scoped_resolver,
    )
    template = mustache(f"ignored-here: {use_placeholder}")
    step = LiveStep.from_step(
        live_step_with_templates,
        update={
            "blueprint_overrides": {
                "variable": mustache(use_var),
                "input_dir": f"{template}/final_output",
            },
        },
    )
    (result,) = transform(step)

    # confirm the variable resolver was applied
    assert result.blueprint_overrides["variable"] == exp_resolved_var

    # confirm the scoped resolver was applied
    actual_input = result.blueprint_overrides["input_dir"]
    assert str(actual_input) == exp_resolved_ph


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
    now = datetime.now(tz=UTC)
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


def test_restart_file_find_selects_latest_partition_zero(tmp_path: Path) -> None:
    """Verify that `RestartFile.find` continues from partition 0 of the most
    recent restart timestamp when the directory holds several timestamps, each
    with a full set of partition files.
    """
    search_path = tmp_path / "output"
    search_path.mkdir(parents=True)

    # two timestamps, each with a 3-partition set (deliberately created out of
    # chronological / partition order to prove the selection, not the order)
    for timestamp in ("20120201000000", "20120101000000"):
        for segment in ("002", "000", "001"):
            (search_path / f"foo_rst.{timestamp}.{segment}.nc").touch()

    reset_file = RestartFile.find(search_path, notfound_ok=False)
    assert reset_file
    assert reset_file.path.name == "foo_rst.20120201000000.000.nc"


def test_restart_file_find_skips_malformed(tmp_path: Path) -> None:
    """Verify that a stray file matching the restart glob shape but not the
    strict naming convention is skipped rather than aborting the search.
    """
    search_path = tmp_path / "output"
    search_path.mkdir(parents=True)

    (search_path / "foo_rst.20120101000000.000.nc").touch()
    # matches the glob (valid timestamp) but not `PATTERN_RST` (non-numeric
    # partition segment), so it must be filtered out rather than constructed
    (search_path / "foo_rst.20120101000000.xyz.nc").touch()

    reset_file = RestartFile.find(search_path, notfound_ok=False)
    assert reset_file
    assert reset_file.path.name == "foo_rst.20120101000000.000.nc"


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


def test_restart_file_find_at_selects_partition_zero_of_requested_timestamp(
    tmp_path: Path,
) -> None:
    """Verify that `RestartFile.find_at` continues from partition 0 of the
    requested timestamp, not the latest, when the directory holds several
    timestamps, each with a full set of partition files.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    """
    search_path = tmp_path / "output"
    search_path.mkdir(parents=True)

    # two timestamps, each with a 3-partition set (deliberately created out of
    # chronological / partition order to prove the selection, not the order)
    for timestamp in ("20120201000000", "20120101000000"):
        for segment in ("002", "000", "001"):
            (search_path / f"foo_rst.{timestamp}.{segment}.nc").touch()

    reset_file = RestartFile.find_at(search_path, datetime(2012, 1, 1))
    assert reset_file.path.name == "foo_rst.20120101000000.000.nc"


def test_restart_file_find_at_whole_file_beside_other_partition_pieces(
    tmp_path: Path,
) -> None:
    """Verify that `RestartFile.find_at` returns a whole file dated at the
    requested timestamp even when partition pieces exist, but only at another
    timestamp (which `find` would prefer).

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    """
    search_path = tmp_path / "output"
    search_path.mkdir(parents=True)

    for segment in ("000", "001"):
        (search_path / f"foo_rst.20120201000000.{segment}.nc").touch()
    (search_path / "foo_rst.20120101000000.nc").touch()

    reset_file = RestartFile.find_at(search_path, datetime(2012, 1, 1))
    assert reset_file.path.name == "foo_rst.20120101000000.nc"
    assert not reset_file.is_partitioned


def test_restart_file_find_at_prefers_partition_pieces(tmp_path: Path) -> None:
    """Verify that, as `RestartFile.find` does, `RestartFile.find_at` prefers
    partition 0 over a whole file when both are dated at the requested
    timestamp.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    """
    search_path = tmp_path / "output"
    search_path.mkdir(parents=True)

    for name in ("foo_rst.20120101000000.nc", "foo_rst.20120101000000.001.nc"):
        (search_path / name).touch()
    (search_path / "foo_rst.20120101000000.000.nc").touch()

    reset_file = RestartFile.find_at(search_path, datetime(2012, 1, 1))
    assert reset_file.path.name == "foo_rst.20120101000000.000.nc"


@pytest.mark.parametrize(
    ("names", "expected_listing"),
    [
        pytest.param(
            [
                "foo_rst.20120301000000.nc",
                "foo_rst.20120101000000.000.nc",
                "foo_rst.20120101000000.001.nc",
            ],
            "2012-01-01 00:00:00, 2012-03-01 00:00:00",
            id="sorted and de-duplicated across whole files and pieces",
        ),
        pytest.param([], "none", id="no restart files"),
    ],
)
def test_restart_file_find_at_miss_lists_available_timestamps(
    tmp_path: Path,
    names: list[str],
    expected_listing: str,
) -> None:
    """Verify that `RestartFile.find_at` raises an exception naming the
    requested timestamp and every timestamp that does exist when no restart
    file is dated as requested.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    names : list[str]
        The names of the restart files to create.
    expected_listing : str
        The timestamp listing expected at the end of the error message.
    """
    search_path = tmp_path / "output"
    search_path.mkdir(parents=True)
    for name in names:
        (search_path / name).touch()

    with pytest.raises(FileNotFoundError) as error:
        _ = RestartFile.find_at(search_path, datetime(2012, 2, 1))

    assert "2012-02-01 00:00:00" in str(error.value)
    assert str(error.value).endswith(expected_listing)


def test_restart_file_find_at_file_path(tmp_path: Path) -> None:
    """Verify that `RestartFile.find_at` returns a file path whose name carries
    the requested timestamp, and raises for one that does not.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    """
    reset_path = tmp_path / "foo_rst.20120101000000.nc"
    reset_path.touch()

    reset_file = RestartFile.find_at(reset_path, datetime(2012, 1, 1))
    assert reset_file.path == reset_path.resolve()

    with pytest.raises(FileNotFoundError, match=r"2012-02-01 00:00:00.*2012-01-01"):
        _ = RestartFile.find_at(reset_path, datetime(2012, 2, 1))


def test_restart_file_find_at_path_dne(tmp_path: Path) -> None:
    """Verify that `RestartFile.find_at` raises an exception when nothing
    exists at the search path.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    """
    with pytest.raises(ValueError, match="No directory or file found"):
        _ = RestartFile.find_at(tmp_path / "dne", datetime(2012, 1, 1))


def test_restart_file_from_parts_unparted(tmp_path: Path) -> None:
    """Verify that a `RestartFile` instance is created without a segment ID in the
    path if it is not supplied.
    """
    now = datetime.now(tz=UTC)
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
    now = datetime.now(tz=UTC)
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
    now = datetime.now(tz=UTC)
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
            f"foo_rst.{datetime.now(tz=UTC).strftime(RestartFile.FMT_TS)}.010.nc",
            10,
            True,
            id="parted path",
        ),
        pytest.param(
            f"foo_rst.{datetime.now(tz=UTC).strftime(RestartFile.FMT_TS)}.nc",
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
    now = datetime.now(tz=UTC)
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


@pytest.mark.parametrize(
    ("location", "expected"),
    [
        pytest.param(
            "foo_rst.20230201000000.nc",
            datetime(2023, 2, 1),
            id="unparted",
        ),
        pytest.param(
            "foo_rst.20230201000000.003.nc",
            datetime(2023, 2, 1),
            id="parted",
        ),
        pytest.param(
            "domain_initial_conditions.nc",
            None,
            id="non-restart name",
        ),
        pytest.param(
            "/some/dir/foo_rst.20230201000000.nc",
            datetime(2023, 2, 1),
            id="full path",
        ),
    ],
)
def test_restart_timestamp(location: str, expected: datetime | None) -> None:
    """Verify `restart_timestamp` parses a restart-style file name (with or
    without a partition segment) and returns `None` for anything else,
    without requiring the file to exist.

    Parameters
    ----------
    location : str
        The file name or path to parse.
    expected : datetime | None
        The expected timestamp, or `None` when `location` is not a
        restart-style name.
    """
    assert restart_timestamp(location) == expected


def test_warn_on_restart_start_date_mismatch_warns_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a single warning is emitted, containing both dates, when the
    restart file's timestamp disagrees with `start_date`.

    Parameters
    ----------
    caplog : pytest.LogCaptureFixture
        Captures log records emitted during the test.
    """
    mismatched_start = datetime(2023, 2, 15)

    with caplog.at_level(logging.WARNING, logger=TRANSFORMS_LOGGER_NAME):
        warn_on_restart_start_date_mismatch(
            "foo_rst.20230201000000.nc", mismatched_start, log=transforms_log
        )

    records = [r for r in caplog.records if r.name == TRANSFORMS_LOGGER_NAME]
    assert len(records) == 1
    message = records[0].getMessage()
    assert str(datetime(2023, 2, 1)) in message
    assert str(mismatched_start) in message


def test_warn_on_restart_start_date_mismatch_silent_on_match(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify no warning is emitted when the restart file's timestamp matches
    `start_date`.

    Parameters
    ----------
    caplog : pytest.LogCaptureFixture
        Captures log records emitted during the test.
    """
    with caplog.at_level(logging.WARNING, logger=TRANSFORMS_LOGGER_NAME):
        warn_on_restart_start_date_mismatch(
            "foo_rst.20230201000000.nc", datetime(2023, 2, 1), log=transforms_log
        )

    records = [r for r in caplog.records if r.name == TRANSFORMS_LOGGER_NAME]
    assert not records


def test_warn_on_restart_start_date_mismatch_silent_for_non_restart_name(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify no warning is emitted for a location that is not a restart-style
    file name, regardless of `start_date`.

    Parameters
    ----------
    caplog : pytest.LogCaptureFixture
        Captures log records emitted during the test.
    """
    with caplog.at_level(logging.WARNING, logger=TRANSFORMS_LOGGER_NAME):
        warn_on_restart_start_date_mismatch(
            "domain_initial_conditions.nc", datetime(2023, 2, 1), log=transforms_log
        )

    records = [r for r in caplog.records if r.name == TRANSFORMS_LOGGER_NAME]
    assert not records


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
    blueprint = live_step_with_templates.blueprint
    assert blueprint is not None
    assert blueprint.state != BlueprintState.Validated

    value = "validated"
    mock_overrides = {"state": BlueprintState.Validated}

    with mock.patch(
        "cstar.orchestration.transforms.get_system_overrides",
        mock.Mock(return_value=mock_overrides),
    ):
        step = apply_automatic_overrides(live_step_with_templates)

    # the mocked system overrides should be applied
    updated = step.blueprint
    assert updated is not None
    assert updated.state == value


@pytest.fixture
def deferred_workplan(hello_world_bp_path: Path) -> Workplan:
    """Generate a workplan whose second step defers its blueprint to the first.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.

    Returns
    -------
    Workplan
    """
    producer = Step(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
    )
    consumer = Step.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "producer", "filename": "generated.yaml"},
            "depends_on": ["producer"],
            "blueprint_overrides": {"target": "@overridden"},
        },
    )

    return Workplan(
        name="deferred-workplan",
        description="A workplan with a deferred blueprint.",
        steps=[producer, consumer],
    )


def test_workplan_transformer_deferred_step(deferred_workplan: Workplan) -> None:
    """Verify the transformer skips blueprint reads for a deferred step and
    packages its overrides into an `apply-overrides` directive.

    Parameters
    ----------
    deferred_workplan : Workplan
        A workplan whose second step defers its blueprint to the first.
    """
    transformer = WorkplanTransformer(deferred_workplan)
    transformed = transformer.apply()

    assert len(transformed.steps) == 2

    consumer = t.cast(
        "LiveStep",
        next(s for s in transformed.steps if s.name == "consumer"),
    )

    # the deferred reference survives transformation untouched
    assert consumer.is_deferred
    ref = t.cast("DeferredBlueprintRef", consumer.blueprint_path)
    assert ref.from_step == "producer"

    # user overrides moved out of the step and into the runtime directive
    assert not consumer.blueprint_overrides
    directives = t.cast("dict[str, dict[str, t.Any]]", consumer.directives)
    assert ApplyOverridesDirective.key() in directives

    config = directives[ApplyOverridesDirective.key()]
    overrides = config[ApplyOverridesDirective.KEY_OVERRIDES]
    assert overrides["target"] == "@overridden"

    # the system working_dir override is deferred to runtime as well
    assert overrides["working_dir"] == consumer.fsm.root_dir.as_posix()

    # the declared application is packaged for the runtime mismatch check
    assert config[ApplyOverridesDirective.KEY_APPLICATION] == "hello_world"


def test_workplan_transformer_deferred_untouched_by_producer_transform(
    deferred_workplan: Workplan,
) -> None:
    """Verify the producer step is transformed via the common (no-rewrite)
    path while the deferred consumer is left for runtime resolution.

    Parameters
    ----------
    deferred_workplan : Workplan
        A workplan whose second step defers its blueprint to the first.
    """
    original_producer = next(s for s in deferred_workplan.steps if s.name == "producer")
    original_bp_path = Path(original_producer.blueprint_path)

    transformer = WorkplanTransformer(deferred_workplan)
    transformed = transformer.apply()

    producer = t.cast(
        "LiveStep",
        next(s for s in transformed.steps if s.name == "producer"),
    )

    # the producer keeps its original blueprint path; nothing is rewritten to disk
    assert str(producer.blueprint_path) == str(original_bp_path)
    assert Path(producer.blueprint_path).exists()

    # the producer's (empty) overrides were still packaged into a runtime directive
    directives = t.cast("dict[str, dict[str, t.Any]]", producer.directives)
    assert ApplyOverridesDirective.key() in directives


def test_workplan_transformer_deferred_active_transform_raises(
    hello_world_bp_path: Path,
) -> None:
    """Verify that a deferred step whose application has an active transform
    is rejected at schedule time.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    producer = Step(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
    )
    consumer = Step.model_validate(
        {
            "name": "consumer",
            "application": Application.ROMS_MARBL.value,
            "blueprint": {"from_step": "producer"},
            "depends_on": ["producer"],
        },
    )
    wp = Workplan(
        name="deferred-workplan",
        description="A workplan with a deferred roms-marbl blueprint.",
        steps=[producer, consumer],
    )

    with mock.patch.dict(os.environ, {ENV_FF_ORCH_TRX_TIMESPLIT: "1"}):
        transformer = WorkplanTransformer(wp)

        with pytest.raises(CstarExpectationFailed) as error:
            _ = transformer.apply()

    assert "deferred" in str(error.value)
    assert RomsMarblTimeSplitter.__name__ in str(error.value)


def test_workplan_transformer_deferred_inactive_transform_ok(
    hello_world_bp_path: Path,
    test_bp_path: Path,
    bp_templates_dir: Path,
) -> None:
    """Verify that a deferred roms-marbl step is permitted when the time
    splitter feature flag is disabled.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    test_bp_path : Path
        Default path for writing a blueprint into the test output directory.
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint templates.
    """
    producer = Step(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
    )
    consumer = Step.model_validate(
        {
            "name": "consumer",
            "application": Application.ROMS_MARBL.value,
            "blueprint": {"from_step": "producer"},
            "depends_on": ["producer"],
        },
    )
    wp = Workplan(
        name="deferred-workplan",
        description="A workplan with a deferred roms-marbl blueprint.",
        steps=[producer, consumer],
    )

    with mock.patch.dict(os.environ, {ENV_FF_ORCH_TRX_TIMESPLIT: FLAG_OFF}):
        transformed = WorkplanTransformer(wp).apply()

    consumer_trx = next(s for s in transformed.steps if s.name == "consumer")
    assert consumer_trx.is_deferred


def test_workplan_transformer_bad_overrides_fail_at_transform_time(
    hello_world_bp_path: Path,
) -> None:
    """Verify overrides invalid for a readable blueprint fail fast at
    schedule (transform) time rather than being deferred to runtime.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = Step(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        blueprint_overrides={"not_a_real_field": "oops"},
    )
    wp = Workplan(
        name="bad-overrides-workplan",
        description="A workplan whose step declares an unknown override key.",
        steps=[step],
    )

    with pytest.raises(ValidationError):
        _ = WorkplanTransformer(wp).apply()


def test_preflight_overrides_injects_cpus_needed(
    hello_world_bp_path: Path,
) -> None:
    """Verify a step without a declared cpu count has its `compute_overrides`
    enriched with the merged blueprint's `cpus_needed`.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = Step(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
    )
    wp = Workplan(
        name="cpus-injection-workplan",
        description="A workplan whose step declares no compute overrides.",
        steps=[step],
    )
    expected_cpus = deserialize(hello_world_bp_path, HelloWorldBlueprint).cpus_needed

    transformed = WorkplanTransformer(wp).apply()
    trx_step = t.cast("LiveStep", transformed.steps[0])
    compute_overrides = t.cast(
        "dict[str, dict[str, t.Any]]", trx_step.compute_overrides
    )

    assert compute_overrides["slurm"]["num_cpus"] == expected_cpus
    # a blueprint that does not confine itself to one node records nothing
    assert "single_node" not in compute_overrides["slurm"]


def test_preflight_overrides_records_single_node(
    hello_world_bp_path: Path,
) -> None:
    """Verify a blueprint declaring `single_node` has that recorded in its
    step's `compute_overrides` alongside `num_cpus`, so the launcher can clamp
    the request to one node without re-reading the blueprint.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = Step(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
    )
    wp = Workplan(
        name="single-node-workplan",
        description="A workplan whose blueprint is confined to one node.",
        steps=[step],
    )

    with mock.patch.object(
        HelloWorldBlueprint,
        "single_node",
        new_callable=mock.PropertyMock,
        return_value=True,
    ):
        transformed = WorkplanTransformer(wp).apply()

    trx_step = t.cast("LiveStep", transformed.steps[0])
    compute_overrides = t.cast(
        "dict[str, dict[str, t.Any]]", trx_step.compute_overrides
    )

    assert compute_overrides["slurm"]["single_node"] is True
    assert compute_overrides["slurm"]["num_cpus"] == 1


def test_preflight_overrides_respects_declared_cpus(
    hello_world_bp_path: Path,
) -> None:
    """Verify a step that already declares `num_cpus` keeps its declared
    value rather than having it replaced by the blueprint's `cpus_needed`.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = Step(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        compute_overrides={"slurm": {"num_cpus": 7}},
    )
    wp = Workplan(
        name="cpus-declared-workplan",
        description="A workplan whose step already declares num_cpus.",
        steps=[step],
    )

    transformed = WorkplanTransformer(wp).apply()
    trx_step = t.cast("LiveStep", transformed.steps[0])
    compute_overrides = t.cast(
        "dict[str, dict[str, t.Any]]", trx_step.compute_overrides
    )

    assert compute_overrides["slurm"]["num_cpus"] == 7


def test_preflight_overrides_deferred_step_skips_cpu_injection(
    deferred_workplan: Workplan,
) -> None:
    """Verify a deferred step's `compute_overrides` are left untouched: its
    blueprint is not available at schedule time, so no `cpus_needed` can be
    read to inject.

    Parameters
    ----------
    deferred_workplan : Workplan
        A workplan whose second step defers its blueprint to the first.
    """
    transformed = WorkplanTransformer(deferred_workplan).apply()

    consumer = t.cast(
        "LiveStep",
        next(s for s in transformed.steps if s.name == "consumer"),
    )

    assert consumer.is_deferred
    assert "slurm" not in consumer.compute_overrides


def test_preflight_overrides_rejects_non_mapping_slurm(
    hello_world_bp_path: Path,
) -> None:
    """Verify a non-mapping `slurm` compute override fails loudly at
    transform time rather than crashing with an opaque internal error.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = Step(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        compute_overrides={"slurm": "oops"},
    )
    wp = Workplan(
        name="bad-compute-overrides",
        description="A workplan whose step declares a non-mapping slurm override.",
        steps=[step],
    )

    with pytest.raises(CstarExpectationFailed, match="non-mapping"):
        _ = WorkplanTransformer(wp).apply()


def test_effective_blueprint_merges_packaged_overrides(
    hello_world_bp_path: Path,
) -> None:
    """Verify `effective_blueprint` reflects a transformed step's packaged
    runtime overrides without persisting anything.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = Step(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        blueprint_overrides={"target": "overridden-at-runtime"},
    )
    wp = Workplan(
        name="effective-blueprint",
        description="A workplan exercising effective_blueprint.",
        steps=[step],
    )
    transformed = WorkplanTransformer(wp).apply()
    step_trx = t.cast("LiveStep", transformed.steps[0])

    blueprint = effective_blueprint(step_trx)

    # the merged content is visible even though the file on disk is unchanged
    assert blueprint.target == "overridden-at-runtime"  # type: ignore[attr-defined]
    original = deserialize(step_trx.blueprint_path, type(blueprint))
    assert original.target != "overridden-at-runtime"  # type: ignore[attr-defined]


def test_apply_overrides_directive(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify the directive applies packaged overrides to a blueprint at runtime.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = LiveStep(
        name="directive-step",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "wd",
    )

    config: dict[str, t.Any] = {
        ApplyOverridesDirective.KEY_OVERRIDES: {"target": "@overridden"},
        ApplyOverridesDirective.KEY_APPLICATION: "hello_world",
    }
    directive = ApplyOverridesDirective(config)
    transformed = directive(step)[0]

    assert Path(transformed.blueprint_path) != hello_world_bp_path

    bp = deserialize(Path(transformed.blueprint_path), HelloWorldBlueprint)
    assert bp.target == "@overridden"


def test_apply_overrides_directive_application_mismatch(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify the directive rejects a blueprint whose application differs from
    the one declared by the step.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = LiveStep(
        name="directive-step",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "wd",
    )

    config: dict[str, t.Any] = {
        ApplyOverridesDirective.KEY_OVERRIDES: {"target": "@overridden"},
        ApplyOverridesDirective.KEY_APPLICATION: "plotter",
    }
    directive = ApplyOverridesDirective(config)

    with pytest.raises(CstarExpectationFailed) as error:
        _ = directive(step)

    assert "hello_world" in str(error.value)
    assert "plotter" in str(error.value)


def test_apply_overrides_directive_requires_overrides() -> None:
    """Verify the directive rejects configuration without an overrides mapping."""
    with pytest.raises(ValueError, match="overrides"):
        _ = ApplyOverridesDirective({"application": "hello_world"})


def test_replace_lists_flag_matches_directive_semantics() -> None:
    """Verify each directive's `REPLACE_LISTS` matches its semantics.

    `ApplyOverridesDirective` carries packaged user overrides (see
    `test_apply_overrides_directive_user_list_merges_elementwise`), so it
    must keep the element-wise default. `NestingDirective` locates a
    complete set of boundary files, so it replaces lists wholesale.
    """
    assert ApplyOverridesDirective.REPLACE_LISTS is False
    assert NestingDirective.REPLACE_LISTS is True


def test_apply_overrides_directive_user_list_merges_elementwise(
    single_step_workplan: Workplan,
) -> None:
    """Regression test: a user-supplied list override, packaged into an
    `apply-overrides` directive by `package_runtime_overrides`, still merges
    element-wise at runtime instead of replacing the blueprint's list
    wholesale.

    `ApplyOverridesDirective._generate_overrides` returns the packaged user
    `blueprint_overrides` verbatim, so those overrides land in
    `OverrideTransform._system_overrides` the same way a directive-located
    file list does; `ApplyOverridesDirective.REPLACE_LISTS` must stay `False`
    so a user's shorter override list doesn't silently drop the blueprint's
    other static entries.

    Parameters
    ----------
    single_step_workplan : Workplan
        A workplan with a valid blueprint file on disk.
    """
    step = LiveStep.from_step(single_step_workplan.steps[0])
    step.blueprint_overrides.clear()
    step.blueprint_overrides["forcing"] = {
        "boundary": {"data": [{"location": "http://mockdoc.com/replaced1.nc"}]}
    }

    packaged = package_runtime_overrides(step)
    config = t.cast(
        "dict[str, t.Any]", packaged.directives[ApplyOverridesDirective.key()]
    )
    directive = ApplyOverridesDirective(config)
    transformed = directive(packaged)[0]

    bp_after = deserialize(Path(transformed.blueprint_path), RomsMarblBlueprint)
    data = bp_after.forcing.boundary.data

    assert len(data) == 3
    assert data[0].location == "http://mockdoc.com/replaced1.nc"
    assert data[0].hash == "abc"
    assert data[1].location == "http://mockdoc.com/partitioning2.nc"
    assert data[2].location == "http://mockdoc.com/partitioning3.nc"


@pytest.fixture
async def deferred_run_context(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
) -> LiveWorkplan:
    """Persist a WorkplanRun and transformed workplan so deferred blueprint
    resolution can locate the producer step at runtime.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    mock_run_id : str
        A unique run-id that has already been added to os.environ
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.

    Returns
    -------
    LiveWorkplan
        The persisted, transformed workplan containing the producer step.
    """
    producer = LiveStep(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "producer",
    )
    live_plan = LiveWorkplan(
        name="deferred-run",
        description="A workplan for deferred blueprint resolution.",
        steps=[producer],
    )

    trx_wp_path = tmp_path / "deferred_run_trx.yaml"
    assert serialize(trx_wp_path, live_plan)

    repo = TrackingRepository()
    await repo.put_workplan_run(
        WorkplanRun(
            workplan_path=tmp_path / "deferred_run.yaml",
            trx_workplan_path=trx_wp_path,
            output_path=tmp_path,
            run_id=mock_run_id,
        ),
    )

    producer.fsm.output_dir.mkdir(parents=True, exist_ok=True)
    return live_plan


async def test_resolve_deferred_blueprint_by_filename(
    deferred_run_context: LiveWorkplan,
    hello_world_bp_content: str,
) -> None:
    """Verify resolution locates the named blueprint in the producer output.

    Parameters
    ----------
    deferred_run_context : LiveWorkplan
        The persisted, transformed workplan containing the producer step.
    hello_world_bp_content : str
        The content of a minimal hello-world blueprint.
    """
    producer = deferred_run_context["producer"]
    generated = producer.fsm.output_dir / "generated.yaml"
    generated.write_text(hello_world_bp_content)

    # a decoy that would make auto-discovery ambiguous but not filename matching
    (producer.fsm.output_dir / "other.yaml").write_text(hello_world_bp_content)

    ref = DeferredBlueprintRef(from_step="producer", filename="generated.yaml")
    assert resolve_deferred_blueprint(ref) == generated


async def test_resolve_deferred_blueprint_auto_discovery(
    deferred_run_context: LiveWorkplan,
    hello_world_bp_content: str,
) -> None:
    """Verify resolution finds a single blueprint without a filename.

    Parameters
    ----------
    deferred_run_context : LiveWorkplan
        The persisted, transformed workplan containing the producer step.
    hello_world_bp_content : str
        The content of a minimal hello-world blueprint.
    """
    producer = deferred_run_context["producer"]
    generated = producer.fsm.output_dir / "anything.yaml"
    generated.write_text(hello_world_bp_content)

    ref = DeferredBlueprintRef(from_step="producer")
    assert resolve_deferred_blueprint(ref) == generated


async def test_resolve_deferred_blueprint_ambiguous(
    deferred_run_context: LiveWorkplan,
    hello_world_bp_content: str,
) -> None:
    """Verify resolution fails when multiple candidates exist and no filename
    is specified.

    Parameters
    ----------
    deferred_run_context : LiveWorkplan
        The persisted, transformed workplan containing the producer step.
    hello_world_bp_content : str
        The content of a minimal hello-world blueprint.
    """
    producer = deferred_run_context["producer"]
    (producer.fsm.output_dir / "one.yaml").write_text(hello_world_bp_content)
    (producer.fsm.output_dir / "two.yaml").write_text(hello_world_bp_content)

    ref = DeferredBlueprintRef(from_step="producer")

    with pytest.raises(CstarError, match="Multiple candidate blueprints"):
        _ = resolve_deferred_blueprint(ref)


async def test_resolve_deferred_blueprint_missing(
    deferred_run_context: LiveWorkplan,
) -> None:
    """Verify resolution fails when the producer did not generate a blueprint.

    Parameters
    ----------
    deferred_run_context : LiveWorkplan
        The persisted, transformed workplan containing the producer step.
    """
    ref = DeferredBlueprintRef(from_step="producer", filename="generated.yaml")

    with pytest.raises(CstarError, match="did not produce"):
        _ = resolve_deferred_blueprint(ref)


async def test_resolve_deferred_blueprint_unknown_step(
    deferred_run_context: LiveWorkplan,
) -> None:
    """Verify resolution fails when the reference names an unknown step.

    Parameters
    ----------
    deferred_run_context : LiveWorkplan
        The persisted, transformed workplan containing the producer step.
    """
    ref = DeferredBlueprintRef(from_step="no-such-step")

    with pytest.raises(CstarError, match="does not exist in the workplan"):
        _ = resolve_deferred_blueprint(ref)


def test_resolve_deferred_blueprint_requires_run_context() -> None:
    """Verify resolution fails with a clear error when no run-id is configured."""
    ref = DeferredBlueprintRef(from_step="producer")

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        pytest.raises(RuntimeError, match="run-id"),
    ):
        _ = resolve_deferred_blueprint(ref)


def test_workplan_transformer_deferred_split_producer_raises(
    tmp_path: Path,
    bp_templates_dir: Path,
) -> None:
    """Verify that a deferred blueprint referencing a producer that is split
    into sub-steps is rejected at schedule time.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint templates.
    """
    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    bp_path = tmp_path / "blueprint.yaml"
    bp_content = bp_tpl_path.read_text()
    bp_content = bp_content.replace(
        "working_dir: .",
        f"working_dir: {tmp_path.as_posix()}",
    )
    bp_path.write_text(bp_content)

    producer = Step(
        name="producer",
        application=Application.ROMS_MARBL.value,
        blueprint=bp_path.as_posix(),
    )
    consumer = Step.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "producer"},
            "depends_on": ["producer"],
        },
    )
    wp = Workplan(
        name="deferred-split-workplan",
        description="A deferred blueprint referencing a split producer.",
        steps=[producer, consumer],
    )

    with (
        mock.patch.dict(os.environ, {ENV_FF_ORCH_TRX_TIMESPLIT: "1"}),
        pytest.raises(CstarExpectationFailed, match="split"),
    ):
        _ = WorkplanTransformer(wp).apply()


async def test_load_workplan_missing_trx_file_raises_runtime_error(
    tmp_path: Path,
    mock_run_id: str,
) -> None:
    """A run record pointing at a nonexistent trx workplan surfaces
    `FileNotFoundError` rather than a generic `RuntimeError`.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.
    """
    trx_path = tmp_path / "missing_trx.yaml"

    repo = TrackingRepository()
    await repo.put_workplan_run(
        WorkplanRun(
            workplan_path=tmp_path / "wp.yaml",
            trx_workplan_path=trx_path,
            output_path=tmp_path,
            run_id=mock_run_id,
        ),
    )

    with pytest.raises(RuntimeError, match="Unable to load workplan"):
        DirectiveConfig.load_workplan()


async def test_load_workplan_invalid_content_raises_runtime_error(
    tmp_path: Path,
    mock_run_id: str,
) -> None:
    """A run record whose trx workplan fails validation surfaces the
    underlying `ValidationError` (a `ValueError` subclass) rather than a
    generic `RuntimeError`.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.
    """
    trx_path = tmp_path / "bad_trx.yaml"
    # valid YAML, but missing the `name` required by `Workplan`
    trx_path.write_text(
        """\
description: a workplan missing its required name
steps: []
"""
    )

    repo = TrackingRepository()
    await repo.put_workplan_run(
        WorkplanRun(
            workplan_path=tmp_path / "wp.yaml",
            trx_workplan_path=trx_path,
            output_path=tmp_path,
            run_id=mock_run_id,
        ),
    )

    with pytest.raises(RuntimeError, match="Unable to load workplan"):
        DirectiveConfig.load_workplan()


async def test_load_workplan_malformed_yaml_raises_runtime_error(
    tmp_path: Path,
    mock_run_id: str,
) -> None:
    """A trx workplan with broken YAML syntax (e.g. a partial write) is
    reported as the documented `RuntimeError`, chained from the parser error.

    Parameters
    ----------
    tmp_path : Path
        The pytest-provided temporary directory.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.
    """
    trx_path = tmp_path / "truncated_trx.yaml"
    trx_path.write_text("name: truncated\nsteps: [\n  - name: step-a\n")

    repo = TrackingRepository()
    await repo.put_workplan_run(
        WorkplanRun(
            workplan_path=tmp_path / "wp.yaml",
            trx_workplan_path=trx_path,
            output_path=tmp_path,
            run_id=mock_run_id,
        ),
    )

    with pytest.raises(RuntimeError, match="Unable to load workplan") as exc_info:
        DirectiveConfig.load_workplan()

    assert exc_info.value.__cause__ is not None


@pytest.fixture
def inline_workplan(hello_world_bp_path: Path) -> Workplan:
    """Generate a workplan whose second step declares an inline blueprint.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.

    Returns
    -------
    Workplan
    """
    producer = Step(
        name="producer",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
    )
    consumer = Step.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": "inline",
            "depends_on": ["producer"],
            "blueprint_overrides": {"target": "@inline"},
        },
    )

    return Workplan(
        name="inline-workplan",
        description="A workplan with an inline blueprint.",
        steps=[producer, consumer],
    )


def test_live_step_inline_blueprint_is_synthesized() -> None:
    """Verify an inline step's blueprint is built from its identity and overrides."""
    step = LiveStep.from_step(
        Step.model_validate(
            {
                "name": "consumer",
                "application": "hello_world",
                "blueprint": "inline",
                "blueprint_overrides": {"target": "@inline"},
            },
        ),
    )

    bp = step.blueprint

    assert isinstance(bp, HelloWorldBlueprint)
    assert bp.name == "consumer"
    assert bp.application == "hello_world"
    assert bp.target == "@inline"
    assert bp.working_dir == step.working_dir
    assert bp.description.startswith("Inline blueprint for step")


def test_live_step_inline_blueprint_overrides_win() -> None:
    """Verify user-supplied values take precedence over the identity seed."""
    step = LiveStep.from_step(
        Step.model_validate(
            {
                "name": "consumer",
                "application": "hello_world",
                "blueprint": "inline",
                "blueprint_overrides": {"target": "@inline", "name": "custom"},
            },
        ),
    )

    assert step.blueprint.name == "custom"


def test_live_step_inline_blueprint_incomplete_raises() -> None:
    """Verify an incomplete inline blueprint is reported with every missing field."""
    step = LiveStep.from_step(
        Step.model_validate(
            {"name": "nester", "application": "nest_ic", "blueprint": "inline"},
        ),
    )

    with pytest.raises(CstarExpectationFailed) as error:
        _ = step.blueprint

    message = str(error.value)
    assert "'nester'" in message
    assert "'nest_ic'" in message
    for field in ("parent_rst", "parent_grid", "child_grid"):
        assert field in message


def test_workplan_transformer_inline_step(inline_workplan: Workplan) -> None:
    """Verify the transformer packages an inline step's overrides like any other
    step and writes nothing to disk.

    Parameters
    ----------
    inline_workplan : Workplan
        A workplan whose second step declares an inline blueprint.
    """
    transformed = WorkplanTransformer(inline_workplan).apply()
    consumer = t.cast(
        "LiveStep",
        next(s for s in transformed.steps if s.name == "consumer"),
    )

    assert consumer.is_inline
    assert not consumer.blueprint_overrides

    directives = t.cast("dict[str, dict[str, t.Any]]", consumer.directives)
    overrides = directives[ApplyOverridesDirective.key()][
        ApplyOverridesDirective.KEY_OVERRIDES
    ]
    assert overrides["target"] == "@inline"
    assert overrides["working_dir"] == consumer.fsm.root_dir.as_posix()

    # preflight ran against the synthesized blueprint
    compute_overrides = t.cast(
        "dict[str, dict[str, t.Any]]", consumer.compute_overrides
    )
    assert compute_overrides["slurm"]["num_cpus"] == 1

    # a schedule-time check must not write the blueprint
    assert not (consumer.fsm.run_dir / "blueprint.yaml").exists()


def test_workplan_transformer_inline_active_transform_raises() -> None:
    """Verify an inline step whose application has an active transform is
    rejected at schedule time.
    """
    consumer = Step.model_validate(
        {
            "name": "consumer",
            "application": Application.ROMS_MARBL.value,
            "blueprint": "inline",
        },
    )
    wp = Workplan(
        name="inline-workplan",
        description="A workplan with an inline roms-marbl blueprint.",
        steps=[consumer],
    )

    with mock.patch.dict(os.environ, {ENV_FF_ORCH_TRX_TIMESPLIT: "1"}):
        with pytest.raises(CstarExpectationFailed) as error:
            _ = WorkplanTransformer(wp).apply()

    assert "inline" in str(error.value)
    assert RomsMarblTimeSplitter.__name__ in str(error.value)


def test_materialize_inline_blueprints(inline_workplan: Workplan) -> None:
    """Verify an inline step's blueprint is written to its work directory and
    its `apply-overrides` directive is dropped, leaving other steps untouched.

    Parameters
    ----------
    inline_workplan : Workplan
        A workplan whose second step declares an inline blueprint.
    """
    transformed = WorkplanTransformer(inline_workplan).apply()
    steps = [t.cast("LiveStep", s) for s in transformed.steps]
    # a directive other than `apply-overrides` must survive materialization
    steps[1] = LiveStep.from_step(
        steps[1],
        update={"directives": {**steps[1].directives, "other": {"key": "value"}}},
    )

    result = materialize_inline_blueprints(steps)

    producer, consumer = result
    assert producer is steps[0]

    path = consumer.fsm.run_dir / "blueprint.yaml"
    assert not consumer.is_inline
    assert consumer.blueprint_path == path
    assert path.exists()

    bp = deserialize(path, HelloWorldBlueprint)
    assert bp.target == "@inline"
    assert bp.working_dir == consumer.fsm.root_dir

    assert consumer.directives == {"other": {"key": "value"}}


def test_effective_blueprint_inline_step_uses_packaged_overrides(
    inline_workplan: Workplan,
) -> None:
    """Verify `effective_blueprint` builds a transformed inline step's content
    from its packaged `apply-overrides` payload, whose `blueprint_overrides`
    are empty by then.

    Parameters
    ----------
    inline_workplan : Workplan
        A workplan whose second step declares an inline blueprint.
    """
    transformed = WorkplanTransformer(inline_workplan).apply()
    consumer = t.cast("LiveStep", transformed.steps[1])
    assert consumer.is_inline
    assert not consumer.blueprint_overrides

    blueprint = effective_blueprint(consumer)

    assert isinstance(blueprint, HelloWorldBlueprint)
    assert blueprint.target == "@inline"
    assert blueprint.working_dir == consumer.fsm.root_dir


def test_materialize_inline_blueprints_requires_transformed_step() -> None:
    """Verify an inline step that was not transformed first is rejected."""
    step = LiveStep.from_step(
        Step.model_validate(
            {
                "name": "consumer",
                "application": "hello_world",
                "blueprint": "inline",
                "blueprint_overrides": {"target": "@inline"},
            },
        ),
    )

    with pytest.raises(CstarExpectationFailed, match="consumer"):
        _ = materialize_inline_blueprints([step])


# ---------------------------------------------------------------------------
# References to steps of other workplan runs (`<step>@<alias>`)
# ---------------------------------------------------------------------------

EXTERNAL_START_AT = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
"""The start time recorded for fabricated external runs."""


class _FakeHandle(ProcessHandle):
    """A launcher-specific handle type, distinct from the generic `ProcessHandle`."""


class _FakeLauncher(Launcher[ProcessHandle]):
    """A launcher that reports the status persisted on a handle."""

    @classmethod
    async def query_status(cls, item: t.Any) -> Status:
        assert isinstance(item, cls.handle_klass())
        return t.cast("ProcessHandle", item).status

    @classmethod
    async def launch(cls, step: LiveStep, dependencies: list[ProcessHandle]) -> t.Any:
        raise NotImplementedError

    @classmethod
    async def update_status(cls, item: t.Any) -> t.Any:
        raise NotImplementedError

    @classmethod
    async def cancel(cls, item: t.Any) -> t.Any:
        raise NotImplementedError

    @classmethod
    def handle_klass(cls) -> type[ProcessHandle]:
        return _FakeHandle


class _FakeSlurmLauncher(_FakeLauncher):
    """A launcher that can wait on in-progress handles of other runs."""

    name = "slurm"
    supports_foreign_dependencies = True


class _FakeLocalLauncher(_FakeLauncher):
    """A launcher that cannot wait on in-progress handles of other runs."""

    name = "local"
    supports_foreign_dependencies = False


@dataclasses.dataclass(frozen=True)
class FabricatedRun:
    """An external workplan run laid out on disk."""

    run_id: str
    record: WorkplanRun
    step: LiveStep
    sentinel: Path

    @property
    def ref(self) -> StepRef:
        """A reference to the run's step, using the alias `spinup`."""
        return StepRef(step=self.step.name, run="spinup")

    @property
    def runs(self) -> dict[str, RunRef]:
        """The `runs` declaration of a workplan referring to this run."""
        return {"spinup": RunRef(run_id=self.run_id)}


@pytest.fixture
def fabricate_external_run(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> Callable[..., FabricatedRun]:
    """Create a factory that fabricates a completed run under the test's
    (autouse-redirected) C-Star data and state homes.

    The run has a tracking record, a transformed workplan holding a step
    `outer`, and a sentinel holding a handle with a chosen status/launcher.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """

    def _make(
        run_id: str = "spinup",
        *,
        status: Status = Status.Done,
        launcher_name: str = "slurm",
        sentinel: bool = True,
    ) -> FabricatedRun:
        root_fsm = JobFileSystemManager(StateDirectoryManager.data_dir(run_id))
        step = LiveStep(
            name="outer",
            application="hello_world",
            blueprint=hello_world_bp_path.as_posix(),
            working_dir=root_fsm.get_subtask_manager("outer").root_dir,
        )
        plan = LiveWorkplan(
            name=f"{run_id}-run",
            description="A fabricated external run.",
            steps=[step],
        )
        trx_path = tmp_path / f"{run_id}_trx.yaml"
        assert serialize(trx_path, plan)

        record = WorkplanRun(
            workplan_path=tmp_path / f"{run_id}.yaml",
            trx_workplan_path=trx_path,
            output_path=tmp_path,
            run_id=run_id,
            start_at=EXTERNAL_START_AT,
        )
        TrackingRepository().put_workplan_run_sync(record)

        sentinel_path = StateRepository.sentinel_path("outer", run_id=run_id)
        if sentinel:
            sentinel_path.parent.mkdir(parents=True, exist_ok=True)
            handle = ProcessHandle(
                pid="1234",
                name="outer",
                run_id=run_id,
                launcher_name=launcher_name,
                status=status,
            )
            assert serialize(sentinel_path, handle)

        step.fsm.output_dir.mkdir(parents=True, exist_ok=True)
        return FabricatedRun(run_id, record, step, sentinel_path)

    return _make


def test_fill_runs_fills_placeholder() -> None:
    """Verify a placeholder in a run-id is filled and `start_at` is kept."""
    runs = {"spinup": RunRef(run_id="{{spin_id}}", start_at=EXTERNAL_START_AT)}
    fill = TemplateFillTransform(variable_resolver=lambda name: f"id-of-{name}")

    filled = fill_runs(runs, fill)

    assert filled["spinup"].run_id == "id-of-spin_id"
    assert filled["spinup"].start_at == EXTERNAL_START_AT
    assert runs["spinup"].run_id == "{{spin_id}}"


def test_fill_runs_without_fill_transform() -> None:
    """Verify a plain run-id passes through without a transform, and a
    placeholder raises naming the alias.
    """
    assert fill_runs({"a": RunRef(run_id="plain")}, None)["a"].run_id == "plain"

    with pytest.raises(CstarExpectationFailed, match="'spinup'"):
        _ = fill_runs({"spinup": RunRef(run_id="{{spin_id}}")}, None)


def test_external_runs_undeclared_alias() -> None:
    """Verify an undeclared alias is reported with the declared ones."""
    registry = ExternalRuns({"known": RunRef(run_id="r")})

    with pytest.raises(CstarError, match=r"'nope'.*\['known'\]"):
        _ = registry.record("nope")


def test_external_runs_unknown_run() -> None:
    """Verify a run without a tracking record raises, naming alias and run-id."""
    registry = ExternalRuns({"spinup": RunRef(run_id="never-ran")})

    with pytest.raises(CstarError, match="'spinup'.*'never-ran'"):
        _ = registry.record("spinup")


def test_external_runs_unknown_step(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify a step missing from the external workplan raises, naming it."""
    fab = fabricate_external_run()
    registry = ExternalRuns(fab.runs)

    with pytest.raises(CstarError, match="'missing'.*'spinup'"):
        _ = registry.step(StepRef.parse("missing@spinup"))


def test_external_runs_missing_workplan_file(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify an unreadable recorded workplan is reported as a `CstarError`."""
    fab = fabricate_external_run()
    fab.record.trx_workplan_path.unlink()

    with pytest.raises(CstarError, match="Unable to load workplan"):
        _ = ExternalRuns(fab.runs).step(fab.ref)


def test_external_runs_missing_sentinel(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify a step that was never submitted has no handle."""
    fab = fabricate_external_run(sentinel=False)

    with pytest.raises(CstarError, match="has not been submitted"):
        _ = ExternalRuns(fab.runs).handle(fab.ref)


def test_external_runs_step_and_handle(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify the step and handle of an external run are read from its records."""
    fab = fabricate_external_run(status=Status.Running)
    registry = ExternalRuns(fab.runs)

    assert registry.step(fab.ref).working_dir == fab.step.working_dir
    assert registry.handle(fab.ref).status == Status.Running


async def test_external_runs_pinned_carries_start_at(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify `pinned` records the resolved record's start time."""
    fab = fabricate_external_run()
    registry = ExternalRuns(fab.runs)
    await registry.refresh([fab.ref], _FakeSlurmLauncher())

    pinned = registry.pinned()

    assert pinned["spinup"].run_id == fab.run_id
    assert pinned["spinup"].start_at == EXTERNAL_START_AT


async def test_external_runs_pinned_passes_unreferenced_alias_through(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify an alias no refreshed step uses is not resolved, so a stale `runs`
    entry cannot abort pinning, and keeps its authored `start_at`.
    """
    fab = fabricate_external_run()
    authored = RunRef(run_id="pruned-run")
    registry = ExternalRuns({**fab.runs, "old": authored})
    await registry.refresh([fab.ref], _FakeSlurmLauncher())

    pinned = registry.pinned()

    assert pinned["old"] == authored
    assert pinned["old"].start_at is None
    assert pinned["spinup"].start_at == EXTERNAL_START_AT


@pytest.mark.parametrize(
    ("status", "launcher", "handle_launcher", "usable"),
    [
        (Status.Done, _FakeLocalLauncher, "local", True),
        (Status.Done, _FakeLocalLauncher, "slurm", True),
        (Status.Running, _FakeSlurmLauncher, "slurm", True),
        (Status.Submitted, _FakeSlurmLauncher, "slurm", True),
        (Status.Running, _FakeLocalLauncher, "local", False),
        (Status.Running, _FakeSlurmLauncher, "local", False),
        (Status.Failed, _FakeSlurmLauncher, "slurm", False),
        (Status.Cancelled, _FakeSlurmLauncher, "slurm", False),
    ],
)
async def test_external_runs_problem_gate(
    fabricate_external_run: Callable[..., FabricatedRun],
    status: Status,
    launcher: type[_FakeLauncher],
    handle_launcher: str,
    usable: bool,
) -> None:
    """Verify an external step is usable when done, or in progress under a
    launcher that supports foreign dependencies and created the handle.

    Parameters
    ----------
    status : Status
        The status persisted on the external step's handle.
    launcher : type[_FakeLauncher]
        The launcher of the current run.
    handle_launcher : str
        The launcher recorded on the handle.
    usable : bool
        Whether the step may be depended upon.
    """
    fab = fabricate_external_run(status=status, launcher_name=handle_launcher)
    registry = ExternalRuns(fab.runs)

    await registry.refresh([fab.ref], launcher())
    problem = registry.problem(fab.ref)

    assert registry.is_done(fab.ref) == (status == Status.Done)
    if usable:
        assert problem == ""
        return

    assert "'spinup'" in problem
    assert f"'{fab.run_id}'" in problem
    assert "'outer'" in problem
    assert status.name in problem
    assert f"launcher '{launcher.name}'" in problem
    assert ("cstar workplan status spinup" in problem) == Status.is_in_progress(status)


async def test_external_runs_problem_not_refreshed(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify a step whose status was never queried is a problem."""
    fab = fabricate_external_run()

    assert "status unknown" in ExternalRuns(fab.runs).problem(fab.ref)


async def test_external_runs_refresh_records_resolution_errors(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify refresh stores a resolution failure per reference instead of
    raising, and never writes a sentinel.
    """
    fab = fabricate_external_run(sentinel=False)
    registry = ExternalRuns({**fab.runs, "ghost": RunRef(run_id="ghost-run")})
    refs = [fab.ref, StepRef.parse("outer@ghost")]

    await registry.refresh(refs, _FakeSlurmLauncher())

    assert "has not been submitted" in registry.problem(refs[0])
    assert "No run record" in registry.problem(refs[1])
    assert not registry.is_done(refs[0])
    assert not fab.sentinel.exists()


async def test_external_runs_refresh_records_corrupt_sentinel(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify a truncated sentinel is a recorded problem, not a crash."""
    fab = fabricate_external_run()
    fab.sentinel.write_text("{ not a handle")
    registry = ExternalRuns(fab.runs)

    await registry.refresh([fab.ref], _FakeSlurmLauncher())

    assert registry.problem(fab.ref)
    assert str(fab.ref) in registry.errors()
    assert registry.tasks() == {}


async def test_external_runs_refresh_records_query_failure(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify an `OSError` while querying status is a recorded problem."""
    fab = fabricate_external_run()

    class _FailingLauncher(_FakeSlurmLauncher):
        @classmethod
        async def query_status(cls, item: t.Any) -> Status:
            raise OSError("sacct unavailable")

    registry = ExternalRuns(fab.runs)

    await registry.refresh([fab.ref], _FailingLauncher())

    assert registry.errors() == {str(fab.ref): "sacct unavailable"}
    assert "sacct unavailable" in registry.problem(fab.ref)


async def test_external_runs_refresh_resolves_step_up_front(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify an unreadable external workplan is recorded by `refresh`, so
    `tasks` (which only reads probes) cannot fail after the gate passed.
    """
    fab = fabricate_external_run()
    fab.record.trx_workplan_path.unlink()
    registry = ExternalRuns(fab.runs)

    await registry.refresh([fab.ref], _FakeSlurmLauncher())

    assert "Unable to load workplan" in registry.problem(fab.ref)
    assert registry.tasks() == {}


async def test_external_runs_refresh_with_real_local_launcher(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify refresh loads the sentinel as the launcher's own handle type, so
    the real `LocalLauncher` can report a finished run's persisted status.
    """
    fab = fabricate_external_run(status=Status.Done, launcher_name="local")
    handle = LocalHandle(
        pid="1234",
        name="outer",
        run_id=fab.run_id,
        launcher_name="local",
        status=Status.Done,
        start_at=EXTERNAL_START_AT.timestamp(),
    )
    assert serialize(fab.sentinel, handle)
    registry = ExternalRuns(fab.runs)

    await registry.refresh([fab.ref], LocalLauncher())

    assert registry.is_done(fab.ref)
    assert registry.problem(fab.ref) == ""


async def test_external_runs_tasks_carry_refreshed_status(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify refreshed steps are exposed as tasks keyed by their token, holding
    a copy of the handle with the status the launcher reported, while the
    persisted sentinel keeps its own.
    """
    fab = fabricate_external_run(status=Status.Submitted)
    registry = ExternalRuns(fab.runs)
    before = fab.sentinel.read_text()

    class _RunningLauncher(_FakeSlurmLauncher):
        @classmethod
        async def query_status(cls, item: t.Any) -> Status:
            return Status.Running

    await registry.refresh([fab.ref], _RunningLauncher())
    tasks = registry.tasks()

    assert list(tasks) == [str(fab.ref)]
    task = tasks[str(fab.ref)]
    assert task.step.working_dir == fab.step.working_dir
    assert task.handle.status == Status.Running
    assert task.handle.pid == "1234"
    assert registry.statuses() == {str(fab.ref): Status.Running}
    assert fab.sentinel.read_text() == before


def test_external_runs_tasks_empty_before_refresh() -> None:
    """Verify nothing is reported for steps that were never refreshed."""
    registry = ExternalRuns({"spinup": RunRef(run_id="spinup")})

    assert registry.tasks() == {}
    assert registry.statuses() == {}


async def test_external_runs_refresh_leaves_sentinel_untouched(
    fabricate_external_run: Callable[..., FabricatedRun],
) -> None:
    """Verify refreshing does not write back to the external run's sentinel."""
    fab = fabricate_external_run(status=Status.Running)
    before = fab.sentinel.read_text()

    await ExternalRuns(fab.runs).refresh([fab.ref], _FakeSlurmLauncher())

    assert fab.sentinel.read_text() == before


def test_external_dependencies_distinct_in_first_seen_order(
    hello_world_bp_path: Path,
) -> None:
    """Verify only external `depends_on` tokens are returned, once each."""

    def step(name: str, deps: list[str]) -> Step:
        return Step(
            name=name,
            application="hello_world",
            blueprint=hello_world_bp_path.as_posix(),
            depends_on=deps,
        )

    wp = Workplan(
        name="ext",
        description="external dependencies",
        runs={"a": RunRef(run_id="ra"), "b": RunRef(run_id="rb")},
        steps=[
            step("s1", ["x@b", "y@a"]),
            step("s2", ["s1", "y@a", "x@b"]),
        ],
    )

    refs = external_dependencies(wp)

    assert [str(r) for r in refs] == ["x@b", "y@a"]


@pytest.fixture
def external_consumer_workplan(hello_world_bp_path: Path) -> Workplan:
    """A workplan whose step depends on `outer@spinup` and templates its output dir.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    return Workplan(
        name="consumer-plan",
        description="Consumes the output of another run.",
        runtime_vars=["spin_id"],
        runs={"spinup": RunRef(run_id="{{spin_id}}")},
        steps=[
            Step.model_validate(
                {
                    "name": "consumer",
                    "application": "hello_world",
                    "blueprint": hello_world_bp_path.as_posix(),
                    "depends_on": ["outer@spinup"],
                    "blueprint_overrides": {"target": "{{output_dir: outer@spinup}}"},
                }
            )
        ],
    )


async def test_workplan_transformer_resolves_external_step(
    fabricate_external_run: Callable[..., FabricatedRun],
    external_consumer_workplan: Workplan,
) -> None:
    """Verify an external placeholder resolves to the external step's output
    directory, the run-id placeholder is filled from the variables, and the
    transformed workplan pins the run record's start time.

    Parameters
    ----------
    external_consumer_workplan : Workplan
        A workplan whose step depends on a step of another run.
    """
    fab = fabricate_external_run()
    wp = external_consumer_workplan
    fill = TemplateFillTransform(
        variable_resolver=lambda name: {"spin_id": "spinup"}[name]
    )
    external = ExternalRuns(fill_runs(wp.runs, fill))
    await external.refresh(external_dependencies(wp), _FakeSlurmLauncher())

    transformed = WorkplanTransformer(wp, fill, external).apply()

    expected = StateDirectoryManager.data_dir("spinup") / "tasks" / "outer" / "output"
    assert fab.step.fsm.output_dir == expected

    consumer = t.cast("LiveStep", transformed.steps[0])
    config = t.cast(
        "dict[str, t.Any]", consumer.directives[ApplyOverridesDirective.key()]
    )
    assert config[ApplyOverridesDirective.KEY_OVERRIDES]["target"] == str(expected)
    assert consumer.depends_on == ["outer@spinup"]

    assert transformed.runs["spinup"].run_id == "spinup"
    assert transformed.runs["spinup"].start_at == EXTERNAL_START_AT
    assert wp.runs["spinup"].start_at is None


async def test_workplan_transformer_reports_every_gate_problem(
    fabricate_external_run: Callable[..., FabricatedRun],
    hello_world_bp_path: Path,
) -> None:
    """Verify an unusable external dependency is reported in the aggregated
    schedule-time error.
    """
    fab = fabricate_external_run(status=Status.Running, launcher_name="local")
    wp = Workplan(
        name="gated",
        description="Depends on a running local step.",
        runs=fab.runs,
        steps=[
            Step(
                name="consumer",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
                depends_on=["outer@spinup"],
            )
        ],
    )
    external = ExternalRuns(fab.runs)
    await external.refresh(external_dependencies(wp), _FakeLocalLauncher())

    with pytest.raises(ValueError, match="Running") as error:
        _ = WorkplanTransformer(wp, None, external).apply()

    assert "cstar workplan status spinup" in str(error.value)


def test_workplan_transformer_without_registry_rejects_external_dependency(
    hello_world_bp_path: Path,
) -> None:
    """Verify an external dependency with no registry is an undeclared-alias problem."""
    wp = Workplan(
        name="gated",
        description="Depends on another run.",
        runs={"spinup": RunRef(run_id="spinup")},
        steps=[
            Step(
                name="consumer",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
                depends_on=["outer@spinup"],
            )
        ],
    )

    with pytest.raises(ValueError, match="not declared"):
        _ = WorkplanTransformer(wp).apply()


def test_workplan_transformer_without_runs_does_not_add_runs(
    hello_world_bp_path: Path,
) -> None:
    """Verify a workplan with no `runs` transforms without touching them."""
    wp = Workplan(
        name="plain",
        description="No external runs.",
        steps=[
            Step(
                name="only",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
            )
        ],
    )

    assert WorkplanTransformer(wp).apply().runs == {}


@pytest.fixture
def external_nesting_steps(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> Callable[..., tuple[LiveStep, LiveStep]]:
    """Create a factory for a `local` step and a `child` step nesting from
    `local;outer@spinup`, depending on the given tokens.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """

    def _make(*depends_on: str) -> tuple[LiveStep, LiveStep]:
        local = LiveStep(
            name="local",
            application="hello_world",
            blueprint=hello_world_bp_path.as_posix(),
            working_dir=tmp_path / "local",
        )
        child = LiveStep(
            name="child",
            application="roms_marbl",
            blueprint=hello_world_bp_path.as_posix(),
            working_dir=tmp_path / "child",
            depends_on=list(depends_on),
            directives={
                NestingDirective.key(): {
                    NestingDirective.KEY_STEP: "local;outer@spinup"
                }
            },
        )
        return local, child

    return _make


def test_collect_directive_problems_external_dependency(
    external_nesting_steps: Callable[..., tuple[LiveStep, LiveStep]],
) -> None:
    """Verify a `nest-from` with a local and an external source passes when
    both are declared dependencies.
    """
    local, child = external_nesting_steps("local", "outer@spinup")

    assert collect_directive_problems([local, child]) == []


def test_collect_directive_problems_external_not_a_dependency(
    external_nesting_steps: Callable[..., tuple[LiveStep, LiveStep]],
) -> None:
    """Verify an external reference missing from `depends_on` breaks the
    ancestor rule, with the same wording as for a local step.
    """
    local, child = external_nesting_steps("local")
    problems = collect_directive_problems([local, child])

    assert len(problems) == 1
    assert "step 'outer@spinup' is not an upstream dependency" in problems[0]
    assert "(via depends_on)" in problems[0]


def test_collect_directive_problems_malformed_token(
    external_nesting_steps: Callable[..., tuple[LiveStep, LiveStep]],
) -> None:
    """Verify a malformed step token is reported rather than raised."""
    local, child = external_nesting_steps("local")
    child.directives[NestingDirective.key()] = {NestingDirective.KEY_STEP: "a@b@c"}

    problems = collect_directive_problems([local, child])

    assert any("Invalid step reference" in problem for problem in problems)


def test_lookup_step_local_and_unknown(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify local lookups return the step, or raise `KeyError`, and a
    malformed token raises `CstarError`.
    """
    step = LiveStep(
        name="here",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "here",
    )
    plan = LiveWorkplan(name="p", description="d", steps=[step])

    assert lookup_step(plan, "here").name == "here"
    with pytest.raises(KeyError, match="Unable to locate step 'there'"):
        _ = lookup_step(plan, "there")
    with pytest.raises(CstarError, match="Invalid step reference"):
        _ = lookup_step(plan, "a@b@c")


def _live_plan_with_external(
    fab: FabricatedRun,
    child: LiveStep,
    *siblings: LiveStep,
) -> LiveWorkplan:
    """Build the transformed workplan of a run depending on a fabricated run."""
    return LiveWorkplan(
        name="child-run",
        description="Depends on an external run.",
        runs={"spinup": RunRef(run_id=fab.run_id, start_at=fab.record.start_at)},
        steps=[*siblings, child],
    )


def test_lookup_step_external(
    fabricate_external_run: Callable[..., FabricatedRun],
    hello_world_bp_path: Path,
    tmp_path: Path,
) -> None:
    """Verify an external token resolves through the workplan's `runs`."""
    fab = fabricate_external_run()
    child = LiveStep(
        name="child",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "child",
        depends_on=["outer@spinup"],
    )

    found = lookup_step(_live_plan_with_external(fab, child), "outer@spinup")

    assert found.working_dir == fab.step.working_dir


async def test_resolve_deferred_blueprint_external_step(
    fabricate_external_run: Callable[..., FabricatedRun],
    hello_world_bp_path: Path,
    hello_world_bp_content: str,
    mock_run_id: str,
    tmp_path: Path,
) -> None:
    """Verify a deferred blueprint produced by a step of another run is found
    in that step's output directory.

    Parameters
    ----------
    hello_world_bp_content : str
        The content of a minimal hello-world blueprint.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.
    """
    fab = fabricate_external_run()
    generated = fab.step.fsm.output_dir / "generated.yaml"
    generated.write_text(hello_world_bp_content)

    consumer = LiveStep.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "outer@spinup", "filename": "generated.yaml"},
            "depends_on": ["outer@spinup"],
            "working_dir": tmp_path / "consumer",
        }
    )
    plan = _live_plan_with_external(fab, consumer)
    trx_path = tmp_path / "consumer_trx.yaml"
    assert serialize(trx_path, plan)
    await TrackingRepository().put_workplan_run(
        WorkplanRun(
            workplan_path=tmp_path / "consumer.yaml",
            trx_workplan_path=trx_path,
            output_path=tmp_path,
            run_id=mock_run_id,
        )
    )

    ref = DeferredBlueprintRef(from_step="outer@spinup", filename="generated.yaml")

    assert resolve_deferred_blueprint(ref) == generated


def test_continuance_directive_step_from_external_run(
    fabricate_external_run: Callable[..., FabricatedRun],
    bp_templates_dir: Path,
    tmp_path: Path,
) -> None:
    """Verify `continue-from: {step: outer@spinup}` locates the restart file
    in the external step's output directory.

    Parameters
    ----------
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    """
    fab = fabricate_external_run()
    rst = RomsFileSystemManager(fab.step.fsm.root_dir).output_dir
    rst.mkdir(parents=True, exist_ok=True)
    restart = rst / "output_rst.20120201000000.nc"
    restart.write_text("mock restart data")

    child_bp_path = tmp_path / "child_bp.yaml"
    child_bp_path.write_text(
        (bp_templates_dir / "blueprint.yaml")
        .read_text()
        .replace("working_dir: .", f"working_dir: {tmp_path}")
    )
    child = LiveStep(
        name="child",
        application="roms_marbl",
        blueprint=child_bp_path.as_posix(),
        working_dir=tmp_path / "child",
        depends_on=["outer@spinup"],
        directives={
            ContinuanceDirective.key(): {ContinuanceDirective.KEY_STEP: "outer@spinup"}
        },
    )
    plan = _live_plan_with_external(fab, child)

    config = t.cast("dict[str, str]", child.directives[ContinuanceDirective.key()])
    altered = ContinuanceDirective(config, workplan=plan)(child)[0]

    bp_after = deserialize(altered.blueprint_path, RomsMarblBlueprint)
    assert Path(bp_after.initial_conditions.data[0].location) == restart.resolve()


def test_nesting_directive_step_from_external_run(
    fabricate_external_run: Callable[..., FabricatedRun],
    bp_templates_dir: Path,
    hello_world_bp_path: Path,
    tmp_path: Path,
) -> None:
    """Verify `nest-from: {step: "local;outer@spinup"}` combines the boundary
    files of a local step and of a step of another run.

    Parameters
    ----------
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files.
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    fab = fabricate_external_run()
    external_out = RomsFileSystemManager(fab.step.fsm.root_dir).output_dir
    external_out.mkdir(parents=True, exist_ok=True)
    (external_out / "outer_bry.20230301003000.nc").write_text("mock boundary data")

    local = LiveStep(
        name="local",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "local",
    )
    local_fsm = RomsFileSystemManager(local.fsm.root_dir)
    local_fsm.prepare()
    (local_fsm.output_dir / "local_bry.20230201003000.nc").write_text(
        "mock boundary data"
    )

    child_bp_path = tmp_path / "child_bp.yaml"
    child_bp_path.write_text(
        (bp_templates_dir / "blueprint.yaml")
        .read_text()
        .replace("working_dir: .", f"working_dir: {tmp_path}")
    )
    child = LiveStep(
        name="child",
        application="roms_marbl",
        blueprint=child_bp_path.as_posix(),
        working_dir=tmp_path / "child",
        depends_on=["local", "outer@spinup"],
        directives={
            NestingDirective.key(): {NestingDirective.KEY_STEP: "local;outer@spinup"}
        },
    )
    plan = _live_plan_with_external(fab, child, local)

    config = t.cast("dict[str, str]", child.directives[NestingDirective.key()])
    altered = NestingDirective(config, workplan=plan)(child)[0]

    data = deserialize(altered.blueprint_path, RomsMarblBlueprint).forcing.boundary.data
    assert len(data) == 2
    assert Path(data[0].location).is_relative_to(local_fsm.output_dir)
    assert Path(data[1].location).is_relative_to(external_out)


async def test_prepare_workplan_pins_external_run(
    fabricate_external_run: Callable[..., FabricatedRun],
    external_consumer_workplan: Workplan,
    tmp_path: Path,
) -> None:
    """Verify `prepare_workplan` refreshes external steps and persists the pinned
    run record in the transformed workplan.

    Parameters
    ----------
    external_consumer_workplan : Workplan
        A workplan whose step depends on a step of another run.
    """
    _ = fabricate_external_run()
    wp_path = tmp_path / "consumer.yaml"
    assert serialize(wp_path, external_consumer_workplan)
    output_dir = tmp_path / "prepared"
    output_dir.mkdir()

    with mock.patch(
        "cstar.orchestration.dag_runner.get_launcher",
        return_value=_FakeSlurmLauncher(),
    ):
        wp, trx_path, _ = await prepare_workplan(
            wp_path, output_dir, {"spin_id": "spinup"}
        )

    persisted = deserialize(trx_path, LiveWorkplan)
    assert wp.runs["spinup"].start_at == EXTERNAL_START_AT
    assert persisted.runs["spinup"] == wp.runs["spinup"]
    assert persisted.runs["spinup"].run_id == "spinup"


def test_inject_compute_defaults_step_value_wins() -> None:
    """Verify the workplan's SLURM defaults fill in around a step's own values."""
    step = LiveStep(
        name="only",
        application="hello_world",
        blueprint="bp.yaml",
        compute_overrides={"slurm": {"max_walltime": "01:00:00", "num_cpus": 4}},
    )
    env = ComputeEnvironment(
        slurm=SlurmComputeSpec(
            account_name="x-acct", queue_name="wholenode", max_walltime="04:00:00"
        )
    )

    result = _inject_compute_defaults(step, env)

    assert result.compute_overrides["slurm"] == {
        "account_name": "x-acct",
        "queue_name": "wholenode",
        "max_walltime": "01:00:00",
        "num_cpus": 4,
    }
    assert step.compute_overrides["slurm"] == {
        "max_walltime": "01:00:00",
        "num_cpus": 4,
    }


def _live_step_for_defaults(compute_overrides: dict[str, t.Any]) -> LiveStep:
    """A minimal step for the compute-defaults injection tests."""
    return LiveStep(
        name="only",
        application="hello_world",
        blueprint="bp.yaml",
        compute_overrides=compute_overrides,
    )


def test_inject_compute_defaults_skips_num_nodes_for_single_node_steps() -> None:
    """A workplan-wide node count is dropped for a step pinned to one node."""
    step = _live_step_for_defaults({"slurm": {"single_node": True, "num_cpus": 4}})
    env = ComputeEnvironment(slurm=SlurmComputeSpec(num_nodes=2, queue_name="q"))
    result = _inject_compute_defaults(step, env)
    assert result.compute_overrides["slurm"] == {
        "single_node": True,
        "num_cpus": 4,
        "queue_name": "q",
    }


def test_inject_compute_defaults_conflict_is_loud() -> None:
    """A merge the SLURM spec rejects fails at schedule time, naming the step."""
    step = _live_step_for_defaults({"slurm": {"max_walltime": "not-a-walltime"}})
    env = ComputeEnvironment(slurm=SlurmComputeSpec(queue_name="q"))
    with pytest.raises(ValueError, match=step.name):
        _inject_compute_defaults(step, env)


@pytest.mark.parametrize(
    "env",
    [
        pytest.param(ComputeEnvironment(), id="empty"),
        pytest.param(ComputeEnvironment(launcher="local"), id="no-slurm-block"),
    ],
)
def test_inject_compute_defaults_noop_without_slurm(env: ComputeEnvironment) -> None:
    """Verify a step is untouched when the workplan declares no SLURM defaults."""
    step = LiveStep(name="only", application="hello_world", blueprint="bp.yaml")

    assert _inject_compute_defaults(step, env) is step


async def test_prepare_workplan_injects_compute_defaults(
    hello_world_bp_path: Path,
    tmp_path: Path,
) -> None:
    """Verify the transformed workplan records the workplan-wide SLURM defaults,
    with a step's own values winning.
    """
    wp = Workplan(
        name="defaults",
        description="Workplan-wide SLURM defaults.",
        compute_environment={
            "slurm": {"account_name": "x-acct", "max_walltime": "04:00:00"},
        },
        steps=[
            Step(
                name="plain",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
            ),
            Step(
                name="declared",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
                compute_overrides={"slurm": {"max_walltime": "00:30:00"}},
            ),
        ],
    )
    wp_path = tmp_path / "defaults.yaml"
    assert serialize(wp_path, wp)
    output_dir = tmp_path / "prepared"
    output_dir.mkdir()

    _, trx_path, _ = await prepare_workplan(wp_path, output_dir, {})

    slurm = {
        s.name: t.cast("dict[str, t.Any]", s.compute_overrides["slurm"])
        for s in deserialize(trx_path, LiveWorkplan).steps
    }
    assert slurm["plain"]["account_name"] == "x-acct"
    assert slurm["plain"]["max_walltime"] == "04:00:00"
    assert slurm["declared"]["account_name"] == "x-acct"
    assert slurm["declared"]["max_walltime"] == "00:30:00"


async def test_prepare_workplan_without_external_refs_skips_launcher(
    hello_world_bp_path: Path,
    tmp_path: Path,
) -> None:
    """Verify a workplan with no external dependencies never builds a launcher."""
    wp = Workplan(
        name="plain",
        description="No external runs.",
        steps=[
            Step(
                name="only",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
            )
        ],
    )
    wp_path = tmp_path / "plain.yaml"
    assert serialize(wp_path, wp)
    output_dir = tmp_path / "prepared"
    output_dir.mkdir()

    with mock.patch("cstar.orchestration.dag_runner.get_launcher") as get_launcher:
        _ = await prepare_workplan(wp_path, output_dir, {})

    get_launcher.assert_not_called()
