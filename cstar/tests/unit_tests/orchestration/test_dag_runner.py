import asyncio
import hashlib
import os
import random
import re
import shutil
import textwrap
import typing as t
from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

import pytest
import yaml

from cstar.applications.core import get_application
from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.applications.ucla_roms.models import UclaRomsBlueprint
from cstar.base.env import ENV_CSTAR_RUNID, ENV_CSTAR_STATE_HOME, FLAG_ON
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.feature import ENV_FF_ORCH_TRX_TIMESPLIT, ENV_FF_ORCH_TRX_TIMESPLIT_FREQ
from cstar.base.utils import slugify
from cstar.cli.workplan.run import migrate_steps
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_PRE_RUN, ARG_RUN_ID
from cstar.execution.file_system import (
    DirectoryManager,
    JobFileSystemManager,
    LockTimeoutError,
    LockUnsupportedError,
    StateDirectoryManager,
)
from cstar.orchestration.adapter import StepToRunRequestAdapter
from cstar.orchestration.dag_runner import (
    DagStatus,
    ExecutiveRunSummary,
    NoPreparableStepsError,
    _carry_inputs,
    _record_attempt,
    apply_clobber_overrides,
    apply_pre_run_overrides,
    apply_resume_overrides,
    build_and_run_dag,
    build_dag,
    build_planner,
    check_clobber_dependents,
    check_clobber_targets,
    check_pre_run_support,
    drop_spent_clobbers,
    get_launcher,
    get_orchestrator,
    get_status_detail_map,
    load_run_state,
    on_launched,
    on_status_changed,
    original_workplan_backup,
    prepare_workplan,
    prune_unpreparable_steps,
    recorded_workplan,
    refresh_run_manifest,
    refuse_edited_steps,
    run_dag,
)
from cstar.orchestration.launch.local import LocalHandle, LocalLauncher
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.manifest import (
    ATTEMPT_BLUEPRINTS_DIR_NAME,
    ATTEMPT_DIR_FORMAT,
    ATTEMPT_TRANSFORMED_NAME,
    ATTEMPT_VARIABLES_NAME,
    ATTEMPT_WORKPLAN_NAME,
    ATTEMPTS_DIR_NAME,
    DEFINITION_HASH_SCHEME,
    RUN_MANIFEST_NAME,
    Attempt,
    AttemptStatus,
    RunManifest,
    StepEntry,
    StepFacts,
    StepRecord,
    carried_digest,
    describe_steps,
    latest_attempt_dir,
    read_run_manifest,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import (
    KEY_CLOBBER,
    KEY_PRE_RUN,
    KEY_RESUME,
    Application,
    BlueprintIdentity,
    BlueprintState,
    Step,
    UserDefinedVariables,
    Workplan,
    WorkplanState,
)
from cstar.orchestration.orchestration import (
    Launcher,
    LiveStep,
    LiveWorkplan,
    Planner,
    ProcessHandle,
    Status,
    Task,
    check_environment,
    configure_environment,
)
from cstar.orchestration.run_check import TRANSFORMED_WORKPLAN_GLOB
from cstar.orchestration.serialization import (
    deserialize,
    read_raw,
    serialize,
)
from cstar.orchestration.state import StateRepository
from cstar.orchestration.tracking import KEY_RUN_UUID, TrackingRepository, WorkplanRun
from cstar.orchestration.transforms import SplitFrequency
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
    get_run_id,
)
from cstar.system.environment import EnvSettingsBase, SlurmSettingsBase
from cstar.system.manager import AnvilEnvSettings
from cstar.tests.unit_tests.orchestration.conftest import (
    EXTERNAL_TOKEN,
    ExternalTaskFactory,
)
from cstar.tests.unit_tests.orchestration.manifest_runs import write_identity


def draw_graph(planner: Planner) -> None:
    import matplotlib.pyplot as plt
    import networkx as nx

    plt.cla()
    plt.clf()
    pos = nx.circular_layout(planner.graph)
    nx.draw_networkx(planner.graph, pos, with_labels=True)
    plt.savefig("g.png", bbox_inches="tight", dpi=500)


@pytest.fixture
async def layered_workplan(
    tmp_path: Path,
    mock_run_id: str,
) -> AsyncGenerator[tuple[Workplan, dict[str, LocalHandle]]]:
    """Create a layered workplan with the structure:
    0
    | \
    1  2
       | \
       3  4
           \
            5
    """
    app_name = "hello_world"
    schema = "1.0.0"
    steps: list[Step] = []
    last_parent: str | None = None
    asset_path = tmp_path / "assets"
    handles: dict[str, LocalHandle] = {}
    mock_data_dir = DirectoryManager.data_home()

    fsm_map = {"": JobFileSystemManager(mock_data_dir)}

    for idx in range(6):
        depends_on = []
        wd_path = asset_path / f"wd{idx}"
        wd_path.mkdir(parents=True)
        bp_path = wd_path / f"bp{idx}.yaml"
        bp_name = f"BP {idx}"
        step_name = f"Step {idx}"
        target = f"@{idx}"
        fsm_map[step_name] = JobFileSystemManager(
            StateDirectoryManager.data_dir(run_id=mock_run_id)
        )

        bp = HelloWorldBlueprint(
            name=bp_name,
            description=bp_name,
            application=app_name,
            state=BlueprintState.Draft,
            schema_version=schema,
            working_dir=wd_path,
            target=target,
        )
        serialize(bp_path, bp)

        depends_on = [last_parent] if last_parent else []

        if idx % 2 == 0:
            last_parent = step_name

        step = Step(
            name=step_name,
            application=app_name,
            depends_on=depends_on,
            blueprint=bp_path,
        )

        parent_fsm = fsm_map[last_parent] if last_parent else fsm_map[""]
        step_fsm = parent_fsm.get_subtask_manager(step_name)
        step_fsm.prepare()
        log_path = step_fsm.logs_dir / f"{step.safe_name}.out"
        log_path.write_text(f"{step.name} message {idx}")

        handle = LocalHandle(
            pid=f"100{idx}",
            name=step.name,
            run_id=mock_run_id,
            start_at=datetime.now(),
        )
        handles[step.name] = handle
        state_repo = StateRepository()
        await state_repo.put_sentinel(handle)

        steps.append(step)

    workplan = Workplan(
        name="test-wp-with-dependencies",
        description="A workplan with nested dependencies demonstrating dependency-based status",
        steps=steps,
        state=WorkplanState.Draft,
    )
    wp_path = asset_path / "workplan.yaml"
    serialize(wp_path, workplan)

    repo = TrackingRepository()
    output_path = tmp_path / "mock-output"
    # the run directory holds the transformed workplan the record derives
    output_path.mkdir(parents=True, exist_ok=True)
    shutil.copy(wp_path, output_path / f"{wp_path.stem}_transformed.yaml")
    write_identity(output_path, mock_run_id, workplan=wp_path)
    wp_run = WorkplanRun(
        output_path=output_path,
        run_id=mock_run_id,
    )
    await repo.put_workplan_run(wp_run)

    yield workplan, handles


@pytest.mark.parametrize(
    ("closed_indices", "open_indices"),
    [
        pytest.param([], ["all"], id="all cancelled"),
        pytest.param([], ["all"], id="all done"),
        pytest.param(["all"], [], id="all ending"),
        pytest.param([], ["all"], id="all failed"),
        pytest.param(["all"], [], id="all running"),
        pytest.param(["all"], [], id="all submitted"),
        pytest.param(["all"], [], id="all unsubmitted"),
        pytest.param([0, 2, 4], [1, 3, 5], id="critical path closed"),
        pytest.param([0], [1, 2, 3, 4, 5], id="one closed task"),
        pytest.param([0, 1], [2, 3, 4, 5], id="two closed tasks (0,1)"),
        pytest.param([0, 2], [1, 3, 4, 5], id="two closed tasks (0,2)"),
        pytest.param([0, 1, 2], [3, 4, 5], id="three closed tasks (0,1,2)"),
        pytest.param([0, 2, 3], [1, 4, 5], id="three closed tasks (0,2,3)"),
        pytest.param([0, 2, 4], [1, 3, 5], id="three closed tasks (0,2,4)"),
        pytest.param([0, 1, 2, 3], [4, 5], id="four closed tasks (0,1,2,3)"),
        pytest.param([0, 1, 2, 4], [3, 5], id="four closed tasks (0,1,2,4)"),
        pytest.param([0, 2, 3, 4], [1, 5], id="four closed tasks (0,2,3,4)"),
        pytest.param([0, 2, 4, 5], [1, 3], id="four closed tasks (0,2,4,5)"),
        pytest.param([0, 1, 2, 3, 4], [5], id="five closed tasks (0,1,2,3,4)"),
        pytest.param([0, 1, 2, 4, 5], [3], id="five closed tasks (0,1,2,4,5)"),
        pytest.param([0, 2, 3, 4, 5], [1], id="five closed tasks (0,1,2,3,4)"),
    ],
)
@pytest.mark.asyncio
async def test_dag_runner_load_run_state(
    open_indices: list[str],
    closed_indices: list[str],
    layered_workplan: tuple[Workplan, dict[str, LocalHandle]],
    mock_run_id: str,
) -> None:
    """Verify the status output matches expectations when all states are a single value."""
    workplan, handles = layered_workplan

    state_repo = StateRepository()
    open_names = [f"Step {idx}" for idx in open_indices]
    closed_names = [f"Step {idx}" for idx in closed_indices]

    if "all" in open_indices:
        open_names = [f"Step {i}" for i in range(len(workplan.steps))]

    if "all" in closed_indices:
        closed_names = [f"Step {i}" for i in range(len(workplan.steps))]

    for handle in handles.values():
        if handle.name in open_names:
            handle.status = Status.Submitted
        if handle.name in closed_names:
            handle.status = Status.Done

        await state_repo.put_sentinel(handle)

    launcher = LocalLauncher()

    # the fake pids have no live process; an unfinalized handle is only
    # treated as in progress while its process is alive
    with mock.patch.object(LocalLauncher, "_is_alive", return_value=True):
        dag_status = await load_run_state(mock_run_id, launcher)

    # verify that state is loaded for every step
    open_items = list(dag_status.open_items)
    closed_items = list(dag_status.closed_items)

    exp_open = len(workplan.steps) if "all" in open_names else len(open_names)
    exp_closed = len(workplan.steps) if "all" in closed_names else len(closed_names)

    assert len(open_items) == exp_open
    assert len(closed_items) == exp_closed


@pytest.mark.parametrize(
    ("closed_indices", "open_indices"),
    [
        pytest.param([], ["all"], id="all cancelled"),
        pytest.param([], ["all"], id="all done"),
        pytest.param(["all"], [], id="all ending"),
        pytest.param([], ["all"], id="all failed"),
        pytest.param(["all"], [], id="all running"),
        pytest.param(["all"], [], id="all submitted"),
        pytest.param(["all"], [], id="all unsubmitted"),
        pytest.param([0, 2, 4], [1, 3, 5], id="critical path closed"),
        pytest.param([0], [1, 2, 3, 4, 5], id="one closed task"),
        pytest.param([0, 1], [2, 3, 4, 5], id="two closed tasks (0,1)"),
        pytest.param([0, 2], [1, 3, 4, 5], id="two closed tasks (0,2)"),
        pytest.param([0, 1, 2], [3, 4, 5], id="three closed tasks (0,1,2)"),
        pytest.param([0, 2, 3], [1, 4, 5], id="three closed tasks (0,2,3)"),
        pytest.param([0, 2, 4], [1, 3, 5], id="three closed tasks (0,2,4)"),
        pytest.param([0, 1, 2, 3], [4, 5], id="four closed tasks (0,1,2,3)"),
        pytest.param([0, 1, 2, 4], [3, 5], id="four closed tasks (0,1,2,4)"),
        pytest.param([0, 2, 3, 4], [1, 5], id="four closed tasks (0,2,3,4)"),
        pytest.param([0, 2, 4, 5], [1, 3], id="four closed tasks (0,2,4,5)"),
        pytest.param([0, 1, 2, 3, 4], [5], id="five closed tasks (0,1,2,3,4)"),
        pytest.param([0, 1, 2, 4, 5], [3], id="five closed tasks (0,1,2,4,5)"),
        pytest.param([0, 2, 3, 4, 5], [1], id="five closed tasks (0,1,2,3,4)"),
    ],
)
@pytest.mark.asyncio
async def test_dag_runner_get_status_detail_map(
    open_indices: list[str],
    closed_indices: list[str],
    layered_workplan: tuple[Workplan, dict[str, LocalHandle]],
) -> None:
    """Verify the status output matches expectations when both open and closed tasks exist."""
    workplan, handles = layered_workplan
    deps = {step.name: step.depends_on for step in workplan.steps}

    state_repo = StateRepository()
    open_names = [f"Step {idx}" for idx in open_indices]
    closed_names = [f"Step {idx}" for idx in closed_indices]

    if "all" in open_indices:
        open_names = [f"Step {i}" for i in range(len(workplan.steps))]

    if "all" in closed_indices:
        closed_names = [f"Step {i}" for i in range(len(workplan.steps))]

    for handle in handles.values():
        if handle.name in open_names:
            handle.status = Status.Submitted
        if handle.name in closed_names:
            handle.status = Status.Done

        await state_repo.put_sentinel(handle)

    update_sideeffects = [(k in open_names, h) for k, h in handles.items()]

    # mock out update_status - with LocalLauncher, it  _starts_
    # anything marked as "submitted", resulting in a status change.
    with mock.patch(
        "cstar.orchestration.dag_runner.LocalLauncher.update_status",
        mock.AsyncMock(side_effect=update_sideeffects),
    ):
        launcher = LocalLauncher()

        run_id = os.getenv(ENV_CSTAR_RUNID) or ""

        planner = Planner(workplan)
        # draw_graph(planner). # leave for quicker debugging

        dag_status = await load_run_state(run_id, launcher)
        detail_map = get_status_detail_map(planner, dag_status)
        ordered_names = list(detail_map.keys())

        # shuffle steps to ensure workplan order does not matter
        steps = list(workplan.steps)
        random.shuffle(steps)

        for step in steps:
            detail = detail_map[step.name]

            if step.name == "Step 0":
                # by the graph structure, we know the first step has no dependdencies
                continue

            # get the position of the step in the output
            slug_idx = ordered_names.index(step.name)
            # ... and the position of all tasks it depends on to start
            dep_indices = [ordered_names.index(d) for d in deps[step.name]]
            # then, confirm the step comes afer all of it's dependencies
            assert slug_idx > max(dep_indices)

            # confirm the step indicates it's waiting on a dependency if any are open
            if (
                set(step.depends_on).intersection(open_names)
                and step.name in open_names
            ):
                assert not detail.ready
            elif step.name in open_names:
                # it has no dependencies in the open set
                assert detail.ready


def _make_step(
    tmp_path: Path,
    name: str,
    depends_on: list[str] | None = None,
    application: Application = Application.HELLO_WORLD,
) -> Step:
    """Build a minimal `Step` for use in `_apply_clobber_overrides` tests."""
    bp_path = tmp_path / f"{name}.yaml"
    bp_path.touch()
    return Step(
        name=name,
        application=application,
        blueprint=bp_path,
        depends_on=depends_on or [],
    )


def _make_workplan(steps: list[Step]) -> Workplan:
    """Build a minimal `Workplan` wrapping the given steps."""
    return Workplan(
        name="test-workplan",
        description="A workplan used to test `_apply_clobber_overrides`",
        steps=steps,
    )


def test_apply_clobber_overrides_noop_when_none(tmp_path: Path) -> None:
    """Verify no overrides are applied when `clobber_steps` is `None`."""
    step_a = _make_step(tmp_path, "Step A")
    wp = _make_workplan([step_a])

    apply_clobber_overrides(wp, None)

    assert wp.steps[0].workflow_overrides == {}


def test_apply_clobber_overrides_noop_when_empty(tmp_path: Path) -> None:
    """Verify no overrides are applied when `clobber_steps` is empty."""
    step_a = _make_step(tmp_path, "Step A")
    wp = _make_workplan([step_a])

    apply_clobber_overrides(wp, [])

    assert wp.steps[0].workflow_overrides == {}


def test_apply_clobber_overrides_matches_name_and_safe_name(tmp_path: Path) -> None:
    """Verify a token matching either `name` or `safe_name` marks only the
    matching step's `workflow_overrides` with `clobber: True`.
    """
    step_a = _make_step(tmp_path, "Step A")
    step_b = _make_step(tmp_path, "Step B")
    step_c = _make_step(tmp_path, "Step C")
    wp = _make_workplan([step_a, step_b, step_c])

    apply_clobber_overrides(wp, [step_a.name, step_b.safe_name])

    assert wp.steps[0].workflow_overrides[KEY_CLOBBER] is True
    assert wp.steps[1].workflow_overrides[KEY_CLOBBER] is True
    assert not wp.steps[2].workflow_overrides.get(KEY_CLOBBER, False)


def test_apply_clobber_overrides_unknown_raises(tmp_path: Path) -> None:
    """Verify an unresolvable token raises `ValueError` listing valid step names."""
    step_a = _make_step(tmp_path, "Step A")
    wp = _make_workplan([step_a])

    with pytest.raises(ValueError, match=r"Unknown clobber step selection\(s\)"):
        apply_clobber_overrides(wp, ["does-not-exist"])


def test_apply_clobber_overrides_unknown_lists_all_bad_tokens(tmp_path: Path) -> None:
    """Verify all unresolvable tokens are reported in a single `ValueError`."""
    step_a = _make_step(tmp_path, "Step A")
    wp = _make_workplan([step_a])

    with pytest.raises(ValueError, match=r"'bad-one'.*'bad-two'|'bad-two'.*'bad-one'"):
        apply_clobber_overrides(wp, ["bad-one", "bad-two"])


def test_apply_clobber_overrides_all_is_not_special(tmp_path: Path) -> None:
    """Verify `all` carries no meaning at this layer (the CLI expands it):
    it resolves like any other name, matching a step literally named `all`
    and raising `ValueError` otherwise.
    """
    wp = _make_workplan([_make_step(tmp_path, "Step A")])
    with pytest.raises(ValueError, match=r"Unknown clobber step selection\(s\)"):
        apply_clobber_overrides(wp, ["all"])


def test_apply_clobber_overrides_warns_untargeted_dependents_once(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a single warning lists every untargeted step that depends on a
    clobbered step.
    """
    step_a = _make_step(tmp_path, "Step A")
    step_b = _make_step(tmp_path, "Step B")
    step_c = _make_step(tmp_path, "Step C", depends_on=[step_a.name, step_b.name])
    step_d = _make_step(tmp_path, "Step D", depends_on=[step_a.name])
    wp = _make_workplan([step_a, step_b, step_c, step_d])

    with caplog.at_level("WARNING"):
        apply_clobber_overrides(wp, [step_a.safe_name, step_b.safe_name])

    assert len(caplog.records) == 1
    assert "Step C" in caplog.text
    assert "Step D" in caplog.text
    assert "stale" in caplog.text


def test_check_clobber_targets_reports_unknown_selections(tmp_path: Path) -> None:
    """Verify names and safe_names resolve while unknown selections are
    returned sorted.
    """
    step_a = _make_step(tmp_path, "Step A")
    step_b = _make_step(tmp_path, "Step B")
    wp = _make_workplan([step_a, step_b])

    assert check_clobber_targets(wp, [step_a.name, step_b.safe_name]) == []
    assert check_clobber_targets(wp, ["zzz", "aaa", step_a.name]) == ["aaa", "zzz"]


def test_check_clobber_dependents_reports_untargeted_dependents(
    tmp_path: Path,
) -> None:
    """Verify untargeted dependents of clobbered steps are returned, and that
    targeting every step yields none.
    """
    step_a = _make_step(tmp_path, "Step A")
    step_b = _make_step(tmp_path, "Step B", depends_on=[step_a.name])
    step_c = _make_step(tmp_path, "Step C", depends_on=[step_b.name])
    wp = _make_workplan([step_a, step_b, step_c])

    assert check_clobber_dependents(wp, [step_a.safe_name]) == [step_b.name]
    assert check_clobber_dependents(wp, [step_c.name]) == []
    assert check_clobber_dependents(wp, [step_a.name, step_b.name, step_c.name]) == []


@pytest.mark.asyncio
async def test_apply_resume_overrides_marks_resumable_failed_steps(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a failed step whose application is resumable is marked for
    resume, a failed step whose application is not resumable is left
    unmarked and named in a warning, and a `Done` step is untouched.
    """
    step_resumable = _make_step(
        tmp_path, "Resumable Step", application=Application.HELLO_WORLD
    )
    step_unresumable = _make_step(
        tmp_path, "Unresumable Step", application=Application.UCLA_ROMS
    )
    step_done = _make_step(tmp_path, "Done Step")
    wp = _make_workplan([step_resumable, step_unresumable, step_done])

    status = DagStatus(
        {
            step_resumable.name: Status.Failed,
            step_unresumable.name: Status.Cancelled,
            step_done.name: Status.Done,
        }
    )

    def fake_get_application(name: str) -> mock.Mock:
        return mock.Mock(resumable=name == step_resumable.application)

    with (
        mock.patch(
            "cstar.orchestration.dag_runner.get_application",
            side_effect=fake_get_application,
        ),
        caplog.at_level("WARNING"),
    ):
        resumed = await apply_resume_overrides(wp, status)

    by_name = {step.name: step for step in wp.steps}

    assert resumed == [step_resumable.name]
    assert by_name[step_resumable.name].workflow_overrides[KEY_RESUME] is True
    assert not by_name[step_unresumable.name].workflow_overrides.get(KEY_RESUME, False)
    assert by_name[step_done.name].workflow_overrides == {}

    assert step_unresumable.name in caplog.text
    assert step_unresumable.application in caplog.text
    assert "do not support resume" in caplog.text


@pytest.mark.asyncio
async def test_apply_resume_overrides_no_failed_steps_logs_info(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a run with no failed steps logs a single informational message
    and marks nothing for resume.
    """
    step_done = _make_step(tmp_path, "Done Step")
    wp = _make_workplan([step_done])

    status = DagStatus({step_done.name: Status.Done})

    with caplog.at_level("INFO"):
        resumed = await apply_resume_overrides(wp, status)

    assert resumed == []
    assert step_done.workflow_overrides == {}
    assert "no failed steps" in caplog.text.lower()


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
async def test_prepare_workplan_persists_clobber_overrides(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify the transformed workplan written to disk carries the
    `--clobber` selection in the targeted step's `workflow_overrides`.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    output_dir = tmp_path / "output"

    _, prepared_path, _, _ = await prepare_workplan(
        wp_path, output_dir, clobber_steps=["Prepare"]
    )

    persisted = deserialize(prepared_path, LiveWorkplan)
    by_name = {step.name: step for step in persisted.steps}

    assert by_name["Prepare"].workflow_overrides[KEY_CLOBBER] is True
    assert not by_name["Ensemble X"].workflow_overrides.get(KEY_CLOBBER, False)


AUTHORED_WORKPLAN = """\
# the workplan as authored: a comment, the quoting and the key order all survive
name: carried   # the name is the run-id
description: "A workplan whose inputs are carried."
runtime_vars: [bpdir]
steps:
    - name: Relative
      application: hello_world
      blueprint: ./blueprints/relative.yaml
    - name: Absolute
      application: hello_world
      blueprint: {absolute}
    - name: Linked
      application: hello_world
      blueprint: ./blueprints/linked.yaml
    - name: Filled
      application: hello_world
      blueprint: "{{{{bpdir}}}}/filled.yaml"
    - name: Deferred
      application: hello_world
      depends_on: [Relative]
      blueprint:
          from_step: Relative
    - name: Inline
      application: hello_world
      depends_on: [Relative]
      blueprint: inline
      blueprint_overrides:
          target: "@inline"
"""

REMOTE_BLUEPRINT = "https://example.com/blueprints/remote-blueprint.yaml"


class CarriedFixture(t.NamedTuple):
    """An authored workplan and the blueprint files its steps name."""

    wp_path: Path
    """The authored workplan."""

    blueprints: dict[str, Path]
    """The file each file-backed step's blueprint resolves to, by step name."""

    variables: dict[str, str]
    """The runtime variables that fill the placeholder."""


@pytest.fixture
def carried_workplan(tmp_path: Path) -> CarriedFixture:
    """Author a workplan with one step of every blueprint kind.

    The workplan lives in `plans/`, away from the run directory and from the
    other blueprints, so a relative path only resolves against its own directory.
    """
    plans = tmp_path / "plans"
    (plans / "blueprints").mkdir(parents=True)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    filled = tmp_path / "filled"
    filled.mkdir()

    blueprints = {
        "Relative": _hello_blueprint(plans / "blueprints" / "relative.yaml", "rel"),
        "Absolute": _hello_blueprint(elsewhere / "absolute.yaml", "abs"),
        "Linked": _hello_blueprint(elsewhere / "real.yaml", "linked"),
        "Filled": _hello_blueprint(filled / "filled.yaml", "filled"),
    }
    (plans / "blueprints" / "linked.yaml").symlink_to(blueprints["Linked"])

    wp_path = plans / "carried.yaml"
    wp_path.write_text(
        AUTHORED_WORKPLAN.format(absolute=blueprints["Absolute"].as_posix())
    )
    return CarriedFixture(wp_path, blueprints, {"bpdir": filled.as_posix()})


def _digests(root: Path) -> dict[str, str]:
    """Digest every file under a directory by its path relative to it."""
    return {
        file.relative_to(
            root
        ).as_posix(): f"sha256:{hashlib.sha256(file.read_bytes()).hexdigest()}"
        for file in sorted(root.rglob("*"))
        if file.is_file()
    }


@pytest.mark.asyncio
async def test_prepare_workplan_carries_the_workplan_and_blueprints(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    """Verify an attempt leaves, in a directory named after its start under the
    run root, the authored workplan byte for byte, a copy of the transformed
    workplan and the blueprint file of each step that has one.
    """
    wp_path, blueprints, variables = carried_workplan
    output_dir = tmp_path / "run"

    _, trx_path, _, carried = await prepare_workplan(wp_path, output_dir, variables)

    attempt_dir = output_dir / carried.directory
    assert attempt_dir.parent == output_dir / ATTEMPTS_DIR_NAME
    assert (attempt_dir / ATTEMPT_WORKPLAN_NAME).read_bytes() == wp_path.read_bytes()
    assert (
        b"# the workplan as authored"
        in (attempt_dir / ATTEMPT_WORKPLAN_NAME).read_bytes()
    )
    assert (
        attempt_dir / ATTEMPT_TRANSFORMED_NAME
    ).read_bytes() == trx_path.read_bytes()

    carried_blueprints = {
        step: output_dir / relative
        for step, relative in carried.carried_blueprints.items()
    }
    assert carried_blueprints == {
        "Relative": attempt_dir / "blueprints/relative/relative.yaml",
        "Absolute": attempt_dir / "blueprints/absolute/absolute.yaml",
        "Linked": attempt_dir / "blueprints/linked/linked.yaml",
        "Filled": attempt_dir / "blueprints/filled/filled.yaml",
    }
    for step, copy in carried_blueprints.items():
        assert copy.read_bytes() == blueprints[step].read_bytes()


@pytest.mark.asyncio
async def test_prepare_workplan_carries_a_symlinked_blueprint_as_content(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    """Verify a blueprint that is a symlink is carried as the file it names, so
    the carried copy stays readable when the link or its target moves.
    """
    wp_path, blueprints, variables = carried_workplan
    assert wp_path.parent.joinpath("blueprints/linked.yaml").is_symlink()
    output_dir = tmp_path / "run"

    _, _, _, carried = await prepare_workplan(wp_path, output_dir, variables)

    copy = output_dir / carried.carried_blueprints["Linked"]
    assert not copy.is_symlink()
    assert copy.read_bytes() == blueprints["Linked"].read_bytes()


@pytest.mark.asyncio
async def test_prepare_workplan_carries_no_deferred_or_inline_blueprint(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    """Verify a deferred step has no blueprint to carry yet and an inline step's
    blueprint is built from the workplan, so neither has a copy or a pointer.
    """
    wp_path, _, variables = carried_workplan
    output_dir = tmp_path / "run"

    _, _, _, carried = await prepare_workplan(wp_path, output_dir, variables)

    assert {"Deferred", "Inline"}.isdisjoint(carried.carried_blueprints)
    attempt_dir = output_dir / carried.directory
    assert not (attempt_dir / ATTEMPT_BLUEPRINTS_DIR_NAME / "deferred").exists()
    assert not (attempt_dir / ATTEMPT_BLUEPRINTS_DIR_NAME / "inline").exists()
    # the authored workplan still says so
    authored = (attempt_dir / ATTEMPT_WORKPLAN_NAME).read_text()
    assert "from_step: Relative" in authored
    assert "blueprint: inline" in authored


@pytest.mark.asyncio
async def test_prepare_workplan_carries_a_blueprint_named_by_a_variable(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    """Verify the blueprint a `{{variable}}` path was filled to is the one carried,
    while the carried workplan keeps the placeholder as authored.
    """
    wp_path, blueprints, variables = carried_workplan
    output_dir = tmp_path / "run"

    _, _, _, carried = await prepare_workplan(wp_path, output_dir, variables)

    copy = output_dir / carried.carried_blueprints["Filled"]
    assert copy.read_bytes() == blueprints["Filled"].read_bytes()
    assert (
        "{{bpdir}}/filled.yaml"
        in (output_dir / carried.directory / ATTEMPT_WORKPLAN_NAME).read_text()
    )


@pytest.mark.asyncio
async def test_prepare_workplan_anchors_a_relative_blueprint_to_the_workplan(
    tmp_path: Path,
    carried_workplan: CarriedFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a relative blueprint path is read from the workplan's directory,
    not from the directory the command runs in.
    """
    wp_path, blueprints, variables = carried_workplan
    elsewhere = tmp_path / "cwd"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    _, _, _, carried = await prepare_workplan(wp_path, tmp_path / "run", variables)

    copy = tmp_path / "run" / carried.carried_blueprints["Relative"]
    assert copy.read_bytes() == blueprints["Relative"].read_bytes()


@pytest.mark.asyncio
async def test_prepare_workplan_carries_variables_only_when_given(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify the runtime variables are carried when the attempt was given any,
    and that no variables file is written otherwise.
    """
    wp_path = tmp_path / "vars.yaml"
    wp = Workplan(
        name="vars",
        description="A workplan with a runtime variable.",
        runtime_vars=["target"],
        steps=[_hello_step(hello_world_bp_path.as_posix(), "hello")],
    )
    assert serialize(wp_path, wp)

    _, _, _, given = await prepare_workplan(wp_path, tmp_path / "with", {"target": "x"})
    _, _, _, none = await prepare_workplan(wp_path, tmp_path / "without")

    saved = tmp_path / "with" / given.directory / ATTEMPT_VARIABLES_NAME
    assert deserialize(saved, UserDefinedVariables).mapping == {"target": "x"}
    assert ATTEMPT_VARIABLES_NAME in given.carried
    assert not (tmp_path / "without" / none.directory / ATTEMPT_VARIABLES_NAME).exists()
    assert ATTEMPT_VARIABLES_NAME not in none.carried


@pytest.mark.asyncio
async def test_prepare_workplan_digests_every_carried_file(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    """Verify the digests are those of the files in the attempt directory, keyed
    by their path relative to it, with nothing left unrecorded.
    """
    wp_path, _, variables = carried_workplan
    output_dir = tmp_path / "run"

    _, _, _, carried = await prepare_workplan(wp_path, output_dir, variables)

    attempt_dir = output_dir / carried.directory
    assert dict(carried.carried) == _digests(attempt_dir)
    assert set(carried.carried) >= {
        ATTEMPT_WORKPLAN_NAME,
        ATTEMPT_VARIABLES_NAME,
        ATTEMPT_TRANSFORMED_NAME,
        *(
            Path(path).relative_to(carried.directory).as_posix()
            for path in carried.carried_blueprints.values()
        ),
    }
    for relative, digest in carried.carried.items():
        assert digest == carried_digest(attempt_dir / relative)


@pytest.mark.asyncio
async def test_prepare_workplan_writes_the_run_root_files_but_no_backup(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    """Verify the transformed workplan is still written in the run root, as the
    record points at it, and that the original backup and the variables file are
    no longer written there.
    """
    wp_path, _, variables = carried_workplan
    output_dir = tmp_path / "run"

    _, trx_path, _, _ = await prepare_workplan(wp_path, output_dir, variables)

    assert trx_path == output_dir / "carried_transformed.yaml"
    assert trx_path.is_file()
    assert sorted(path.name for path in output_dir.iterdir() if path.is_file()) == [
        "carried_transformed.yaml"
    ]
    assert not list(output_dir.glob("*.bak*"))
    assert not list(output_dir.glob("*.vars*"))
    assert not original_workplan_backup(wp_path, output_dir).exists()


@pytest.mark.asyncio
async def test_prepare_workplan_overwrites_the_transformed_workplan_each_attempt(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    """Verify the run root keeps one transformed workplan, replaced by each
    attempt, while every attempt keeps its own copy of it.
    """
    wp_path, _, variables = carried_workplan
    output_dir = tmp_path / "run"

    _, trx_path, _, first = await prepare_workplan(wp_path, output_dir, variables)
    first_text = trx_path.read_text()
    _, _, _, second = await prepare_workplan(
        wp_path, output_dir, variables, clobber_steps=["Relative"]
    )

    assert trx_path.read_text() != first_text
    assert not list(output_dir.glob("*_transformed.yaml.*"))
    first_copy = output_dir / first.directory / ATTEMPT_TRANSFORMED_NAME
    second_copy = output_dir / second.directory / ATTEMPT_TRANSFORMED_NAME
    assert first_copy.read_text() == first_text
    assert second_copy.read_bytes() == trx_path.read_bytes()


@pytest.mark.asyncio
async def test_prepare_workplan_names_attempts_in_the_same_second_apart(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    """Verify two attempts prepared in one second get distinct directories, so the
    later one does not overwrite what the earlier one carried.
    """
    wp_path, _, variables = carried_workplan
    output_dir = tmp_path / "run"
    start = datetime(2026, 10, 15, 12, 0, 0, 500_000, tzinfo=UTC)

    with mock.patch("cstar.orchestration.dag_runner.utc_now", return_value=start):
        _, _, _, first = await prepare_workplan(wp_path, output_dir, variables)
        _, _, _, second = await prepare_workplan(wp_path, output_dir, variables)

    assert (first.directory, second.directory) == (
        "attempts/20261015T120000",
        "attempts/20261015T120000-2",
    )
    assert first.start_at == second.start_at == start
    assert (output_dir / first.directory / ATTEMPT_WORKPLAN_NAME).is_file()
    assert (output_dir / second.directory / ATTEMPT_WORKPLAN_NAME).is_file()


@pytest.mark.asyncio
async def test_prepare_workplan_refused_attempt_writes_nothing_new(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    """Verify an attempt refused during preparation leaves no attempt directory
    and does not touch what the earlier attempt carried.
    """
    wp_path, _, variables = carried_workplan
    output_dir = tmp_path / "run"
    _, _, _, first = await prepare_workplan(wp_path, output_dir, variables)
    before = _digests(output_dir)

    with pytest.raises(ValueError, match="Unknown clobber"):
        await prepare_workplan(
            wp_path, output_dir, variables, clobber_steps=["no-such-step"]
        )

    assert _digests(output_dir) == before
    assert [p.name for p in (output_dir / ATTEMPTS_DIR_NAME).iterdir()] == [
        Path(first.directory).name
    ]


@pytest.mark.asyncio
async def test_prepare_workplan_refused_first_attempt_leaves_no_directory(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    wp_path, _, variables = carried_workplan
    output_dir = tmp_path / "run"

    with pytest.raises(ValueError, match="Unknown clobber"):
        await prepare_workplan(
            wp_path, output_dir, variables, clobber_steps=["no-such-step"]
        )

    assert not (output_dir / ATTEMPTS_DIR_NAME).exists()


@pytest.mark.asyncio
async def test_prepare_workplan_fails_when_a_blueprint_cannot_be_carried(
    tmp_path: Path, carried_workplan: CarriedFixture
) -> None:
    """Verify a blueprint that cannot be read fails the preparation rather than
    being left out of what the attempt carried, and leaves the run as the earlier
    attempt left it: the plan in the run root and no new attempt directory.
    """
    wp_path, blueprints, variables = carried_workplan
    output_dir = tmp_path / "run"
    _, trx_path, _, first = await prepare_workplan(
        wp_path, output_dir, variables, clobber_steps=["Relative"]
    )
    before = _digests(output_dir)
    blueprints["Absolute"].unlink()

    with pytest.raises(FileNotFoundError):
        await prepare_workplan(wp_path, output_dir, variables)

    # the earlier attempt clobbered a step, so a replaced root plan would differ
    assert _digests(output_dir) == before
    assert trx_path.is_file()
    assert [p.name for p in (output_dir / ATTEMPTS_DIR_NAME).iterdir()] == [
        Path(first.directory).name
    ]


@pytest.mark.parametrize("suffix", [".json", ".yml"])
@pytest.mark.asyncio
async def test_prepare_workplan_names_the_run_root_plan_yaml(
    tmp_path: Path, hello_world_bp_path: Path, suffix: str
) -> None:
    """Verify the plan written into the run root is named `.yaml` whatever the
    authored suffix, since it is rendered as YAML, so it loads back as written
    and is the one file the run-root glob finds.
    """
    wp = Workplan(
        name="authored",
        description="A workplan authored under another suffix.",
        steps=[_hello_step(hello_world_bp_path.as_posix(), "hello")],
    )
    wp_path = tmp_path / f"plan{suffix}"
    serialize(wp_path, wp)
    output_dir = tmp_path / "run"

    prepared, trx_path, _, _ = await prepare_workplan(wp_path, output_dir)

    assert trx_path == output_dir / "plan_transformed.yaml"
    assert sorted(output_dir.glob(TRANSFORMED_WORKPLAN_GLOB)) == [trx_path]
    assert LiveWorkplan.load(trx_path) == prepared


def test_carry_inputs_copies_a_remote_blueprint_under_its_url_name(
    tmp_path: Path, serve_remote_files: dict[str, Path]
) -> None:
    """Verify a blueprint at a remote URL is downloaded and carried under the last
    segment of the URL's path, and a sentinel URI, which names no file yet, is
    skipped.
    """
    served = _hello_blueprint(tmp_path / "served.yaml", "remote")
    serve_remote_files[REMOTE_BLUEPRINT] = served
    run_root = tmp_path / "run"
    run_root.mkdir()
    steps = [
        _hello_step(REMOTE_BLUEPRINT, "Remote"),
        _hello_step("cstar-forge:domain", "Sentinel"),
    ]
    start = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)

    carried = _carry_inputs(
        run_root,
        start,
        b"name: plan\n",
        "name: plan\n",
        steps,
        None,
    )

    copy = "attempts/20261015T120000/blueprints/remote/remote-blueprint.yaml"
    assert dict(carried.carried_blueprints) == {"Remote": copy}
    assert (run_root / copy).read_bytes() == served.read_bytes()
    assert carried.carried["blueprints/remote/remote-blueprint.yaml"] == carried_digest(
        served
    )
    assert not (run_root / carried.directory / "blueprints" / "sentinel").exists()


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
async def test_prepare_workplan_carries_the_template_workplan(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify the authored workplan is carried as it was written, and that it
    reads back as the workplan a later `--resume` with a path verifies against.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    output_dir = tmp_path / "output"

    _, trx_path, _, carried = await prepare_workplan(
        wp_path, output_dir, clobber_steps=["Prepare"]
    )

    copy = output_dir / carried.directory / ATTEMPT_WORKPLAN_NAME
    assert copy.read_bytes() == wp_path.read_bytes()
    assert deserialize(copy, Workplan) == deserialize(wp_path, Workplan)
    # the transformed copy keeps the name, which is the run-id, as written
    assert read_raw(trx_path)["name"] == deserialize(wp_path, Workplan).name


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
async def test_build_dag_reports_orphans_of_a_re_entered_run(
    wp_templates_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify re-running a workplan under a run-id whose directory holds a step
    that the workplan no longer has reports the leftover without deleting it.
    """
    run_id = "re-entered-run"
    leftover = (
        JobFileSystemManager(StateDirectoryManager.data_dir(run_id)).tasks_dir
        / "renamed-step"
    )
    leftover.mkdir(parents=True)

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        caplog.at_level("WARNING"),
    ):
        await build_dag(wp_templates_dir / "single_step.yaml", run_id)

    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any(str(leftover) in w and "left in place" in w for w in warnings)
    assert leftover.is_dir()


@pytest.mark.asyncio
async def test_prepare_workplan_materializes_inline_blueprint(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify the transformed workplan records an inline step's blueprint as a
    materialized file, while the carried workplan keeps the `inline` token.
    """
    wp = Workplan(
        name="inline-workplan",
        description="A workplan with an inline blueprint.",
        steps=[
            Step(
                name="producer",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
            ),
            Step.model_validate(
                {
                    "name": "consumer",
                    "application": "hello_world",
                    "blueprint": "inline",
                    "depends_on": ["producer"],
                    "blueprint_overrides": {"target": "@inline"},
                },
            ),
        ],
    )
    wp_path = tmp_path / "inline-workplan.yaml"
    assert serialize(wp_path, wp)
    output_dir = tmp_path / "output"

    prepared, prepared_path, _, carried = await prepare_workplan(wp_path, output_dir)

    # `build_dag` reports orphans from the steps' working directories
    assert all(isinstance(s, LiveStep) for s in prepared.steps)

    persisted = deserialize(prepared_path, LiveWorkplan)
    consumer = next(s for s in persisted.steps if s.name == "consumer")

    assert not consumer.is_inline
    assert Path(consumer.blueprint_path) == consumer.fsm.run_dir / "blueprint.yaml"
    assert Path(consumer.blueprint_path).exists()
    assert "apply-overrides" not in consumer.directives

    authored = deserialize(
        output_dir / carried.directory / ATTEMPT_WORKPLAN_NAME,
        Workplan,
    )
    assert next(s for s in authored.steps if s.name == "consumer").is_inline
    assert set(carried.carried_blueprints) == {"producer"}


@pytest.mark.asyncio
async def test_prepare_workplan_records_the_run_root(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
) -> None:
    """Verify the transformed workplan records the run root it was written under,
    and that a copy of the run directory loads with its step directories rebased.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory the run directory is copied into
    mock_run_id : str
        Fixture exporting the run-id the steps' working directories derive from
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    wp = Workplan(
        name="rooted",
        description="A workplan whose transformed copy records its run root.",
        steps=[
            Step(
                name="hello",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
            ),
        ],
    )
    wp_path = tmp_path / "rooted.yaml"
    assert serialize(wp_path, wp)
    output_dir = StateDirectoryManager.data_dir(mock_run_id)

    prepared, prepared_path, _, _ = await prepare_workplan(wp_path, output_dir)

    assert isinstance(prepared, LiveWorkplan)
    assert prepared.root == output_dir.resolve()
    assert f"root: {output_dir.resolve()}" in prepared_path.read_text()
    assert LiveWorkplan.load(prepared_path) == deserialize(prepared_path, LiveWorkplan)

    moved = tmp_path / "moved"
    shutil.copytree(output_dir, moved)
    relocated = LiveWorkplan.load(moved / prepared_path.name)

    assert relocated.root == moved.resolve()
    assert relocated.steps[0].working_dir == moved.resolve() / "tasks" / "hello"
    assert prepared.steps[0].working_dir == output_dir / "tasks" / "hello"


@pytest.mark.asyncio
async def test_run_dag_records_the_root_of_the_transformed_workplan(
    tmp_path: Path, mock_run_id: str
) -> None:
    """Verify a run reloaded from a root other than the current data home's keeps
    that root on its record, rather than a directory the data home never held.
    """
    run_root = tmp_path / "scratch" / "workplan_runs" / mock_run_id
    run_root.mkdir(parents=True)
    trx_path = run_root / "workplan_transformed.yaml"
    assert not StateDirectoryManager.data_dir(mock_run_id).exists()

    step = LiveStep.from_step(_make_step(tmp_path, "hello"))
    workplan = LiveWorkplan(
        name="reloaded", description="A reloaded run.", steps=[step], root=run_root
    )
    assert serialize(trx_path, workplan)

    with (
        mock.patch("cstar.orchestration.dag_runner.get_orchestrator"),
        mock.patch("cstar.orchestration.dag_runner.process_plan", mock.AsyncMock()),
    ):
        wp_run = await run_dag(
            tmp_path / "workplan.yaml", trx_path, mock_run_id, Planner(workplan)
        )

    persisted = await TrackingRepository().get_workplan_run(mock_run_id)

    assert wp_run.output_path == run_root
    assert persisted is not None
    assert persisted.output_path == run_root
    assert persisted.trx_workplan_path == trx_path


async def _schedule(
    tmp_path: Path, run_id: str, blueprint: Path
) -> tuple[Path, WorkplanRun]:
    """Prepare and schedule a one-step workplan; return its path and the run."""
    wp = Workplan(
        name="manifested",
        description="A workplan whose run gets a manifest.",
        steps=[
            Step(
                name="hello", application="hello_world", blueprint=blueprint.as_posix()
            )
        ],
    )
    wp_path = tmp_path / "manifested.yaml"
    assert serialize(wp_path, wp)

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        mock.patch("cstar.orchestration.dag_runner.get_orchestrator"),
        mock.patch("cstar.orchestration.dag_runner.process_plan", mock.AsyncMock()),
    ):
        planner, trx_path, facts, carried = await build_dag(wp_path, run_id)
        wp_run = await run_dag(
            wp_path,
            trx_path,
            run_id,
            planner,
            step_facts=facts,
            carried=carried,
        )
    return wp_path, wp_run


async def _reenter(wp_run: WorkplanRun) -> WorkplanRun:
    """Schedule a recorded run again as a re-entry does: its transformed workplan
    is replayed, with no facts and nothing carried.
    """
    planner = build_planner(LiveWorkplan.load(wp_run.trx_workplan_path))
    with (
        mock.patch("cstar.orchestration.dag_runner.get_orchestrator"),
        mock.patch("cstar.orchestration.dag_runner.process_plan", mock.AsyncMock()),
    ):
        return await run_dag(
            wp_run.workplan_path, wp_run.trx_workplan_path, wp_run.run_id, planner
        )


@pytest.mark.asyncio
async def test_run_dag_writes_the_run_manifest(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify scheduling a workplan writes the manifest to the run root: one
    attempt, an entry per step in its task directory, with the step's definition
    hash, and that scheduling again adds an attempt without minting a new id.
    """
    wp_path, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)

    manifest = read_run_manifest(wp_run.output_path)
    assert manifest is not None
    assert (wp_run.output_path / RUN_MANIFEST_NAME).is_file()
    assert manifest.kind == "workplan"
    assert manifest.run_id == mock_run_id
    assert manifest.name == "manifested"
    assert manifest.workplan is not None
    assert manifest.workplan.path == str(wp_path)
    assert len(manifest.attempts) == 1

    facts = describe_steps(deserialize(wp_path, Workplan), wp_path.parent)
    entry = manifest.step("hello")
    assert entry is not None
    assert entry.application == "hello_world"
    assert entry.working_dir == "tasks/hello"
    assert entry.definition_hash == facts["hello"].definition_hash
    assert wp_run.metadata[KEY_RUN_UUID] == manifest.id

    # a re-entry brings no facts: the recorded entries stay
    reentered = await _reenter(wp_run)
    again = read_run_manifest(wp_run.output_path)
    assert again is not None
    assert again.id == manifest.id
    assert reentered.metadata[KEY_RUN_UUID] == manifest.id
    persisted = await TrackingRepository().get_workplan_run(mock_run_id)
    assert persisted is not None
    assert persisted.uuid == manifest.id
    assert len(again.attempts) == 2
    entry = again.step("hello")
    assert entry is not None
    assert entry.definition_hash == facts["hello"].definition_hash


@pytest.mark.asyncio
async def test_build_dag_returns_what_the_attempt_carried(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify the carried inputs come back from `build_dag`, with the files in the
    run directory the workplan will run in.
    """
    wp_path = tmp_path / "plan.yaml"
    wp = Workplan(
        name="plan",
        description="A plan.",
        steps=[_hello_step(hello_world_bp_path.as_posix(), "hello")],
    )
    assert serialize(wp_path, wp)

    with mock.patch("cstar.orchestration.dag_runner.check_environment"):
        _, trx_path, _, carried = await build_dag(wp_path, mock_run_id)

    run_root = trx_path.parent
    assert run_root == StateDirectoryManager.data_dir(mock_run_id)
    assert carried.directory.startswith(f"{ATTEMPTS_DIR_NAME}/")
    assert (run_root / carried.directory / ATTEMPT_WORKPLAN_NAME).is_file()
    assert carried.carried_blueprints == {
        "hello": f"{carried.directory}/blueprints/hello/helloworld.yaml"
    }


@pytest.mark.asyncio
async def test_run_dag_records_the_carried_inputs(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify the attempt records the directory it carried its inputs in and the
    digest of each file, named after the attempt's own start, and each step
    points at the blueprint copy the attempt carried for it.
    """
    wp_path, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)

    manifest = read_run_manifest(wp_run.output_path)
    assert manifest is not None
    (attempt,) = manifest.attempts
    attempt_dir = wp_run.output_path / attempt.directory
    assert attempt_dir.parent == wp_run.output_path / ATTEMPTS_DIR_NAME
    assert attempt_dir.name == attempt.start_at.strftime(ATTEMPT_DIR_FORMAT)
    assert attempt.carried == _digests(attempt_dir)
    assert attempt.carried[ATTEMPT_WORKPLAN_NAME] == carried_digest(wp_path)
    assert ATTEMPT_VARIABLES_NAME not in attempt.carried

    entry = manifest.step("hello")
    assert entry is not None
    assert (
        entry.carried_blueprint
        == f"{attempt.directory}/blueprints/hello/helloworld.yaml"
    )
    assert (wp_run.output_path / entry.carried_blueprint).read_bytes() == (
        hello_world_bp_path.read_bytes()
    )


@pytest.mark.asyncio
async def test_run_dag_re_entry_carries_nothing_and_keeps_the_pointers(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify an attempt replaying the recorded plan records no directory and no
    digests, leaves each step pointing at the prepared attempt's blueprint, and
    does not hide the preparing attempt from `latest_attempt_dir`.
    """
    _, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    before = read_run_manifest(wp_run.output_path)
    assert before is not None
    pointer = before.step("hello").carried_blueprint  # type: ignore[union-attr]

    await _reenter(wp_run)

    manifest = read_run_manifest(wp_run.output_path)
    assert manifest is not None
    prepared, reentry = manifest.attempts
    assert prepared.directory
    assert (reentry.directory, reentry.carried) == ("", {})
    assert manifest.step("hello").carried_blueprint == pointer  # type: ignore[union-attr]
    assert latest_attempt_dir(manifest, wp_run.output_path) == (
        wp_run.output_path / prepared.directory
    )
    assert [p.name for p in (wp_run.output_path / ATTEMPTS_DIR_NAME).iterdir()] == [
        Path(prepared.directory).name
    ]


@pytest.mark.asyncio
async def test_run_dag_points_the_steps_at_each_preparing_attempt(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify each attempt that prepares the workplan re-points the steps at its
    own blueprint copies, while the earlier attempt keeps its record.
    """
    _, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    hello_world_bp_path.write_text(hello_world_bp_path.read_text() + "# edited\n")

    await _schedule(tmp_path, mock_run_id, hello_world_bp_path)

    manifest = read_run_manifest(wp_run.output_path)
    assert manifest is not None
    first, second = manifest.attempts
    assert first.directory != second.directory
    entry = manifest.step("hello")
    assert entry is not None
    assert entry.carried_blueprint.startswith(f"{second.directory}/")
    assert (
        (wp_run.output_path / entry.carried_blueprint)
        .read_text()
        .endswith("# edited\n")
    )
    first_copy = (
        wp_run.output_path / first.directory / "blueprints/hello/helloworld.yaml"
    )
    assert not first_copy.read_text().endswith("# edited\n")
    assert latest_attempt_dir(manifest, wp_run.output_path) == (
        wp_run.output_path / second.directory
    )


@pytest.mark.asyncio
async def test_run_dag_survives_a_manifest_that_cannot_be_written(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify scheduling carries on, with a warning, when the manifest write fails."""
    with (
        mock.patch(
            "cstar.orchestration.dag_runner.ensure_run_manifest",
            side_effect=OSError("disk full"),
        ),
        caplog.at_level("WARNING"),
    ):
        _, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)

    assert not (wp_run.output_path / RUN_MANIFEST_NAME).exists()
    assert "disk full" in caplog.text
    assert KEY_RUN_UUID not in wp_run.metadata
    assert await TrackingRepository().get_workplan_run(mock_run_id) is not None
    assert wp_run.uuid == ""


def _attempt_args(
    tmp_path: Path, run_id: str, blueprint: Path
) -> tuple[Path, str, Path, Workplan, list[LiveStep]]:
    """The arguments `_record_attempt` takes before the facts and what was carried."""
    root = tmp_path / "run-root"
    root.mkdir()
    workplan = Workplan(
        name="attempted",
        description="A workplan to record.",
        steps=[_hello_step(blueprint.as_posix(), "hello")],
    )
    steps = [LiveStep.from_step(step) for step in workplan.steps]
    return root, run_id, tmp_path / "attempted.yaml", workplan, steps


def test_record_attempt_returns_the_id_of_the_manifest(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify recording an attempt returns the uuid of the manifest it wrote, the
    same one when the run is recorded again.
    """
    root, run_id, wp_path, workplan, steps = _attempt_args(
        tmp_path, mock_run_id, hello_world_bp_path
    )

    first = _record_attempt(root, run_id, wp_path, workplan, steps, {}, None)
    second = _record_attempt(root, run_id, wp_path, workplan, steps, {}, None)

    manifest = read_run_manifest(root)
    assert manifest is not None
    assert first == second == manifest.id


def test_record_attempt_returns_nothing_when_the_manifest_is_not_written(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a failed manifest write is a warning and an empty uuid."""
    root, run_id, wp_path, workplan, steps = _attempt_args(
        tmp_path, mock_run_id, hello_world_bp_path
    )

    with (
        mock.patch(
            "cstar.orchestration.dag_runner.ensure_run_manifest",
            side_effect=OSError("disk full"),
        ),
        caplog.at_level("WARNING"),
    ):
        uuid = _record_attempt(root, run_id, wp_path, workplan, steps, {}, None)

    assert uuid == ""
    assert "disk full" in caplog.text


@pytest.mark.asyncio
async def test_recorded_workplan_is_the_latest_carried_copy(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify the recorded workplan is the copy the latest preparing attempt
    carried, also after a re-entry that carried nothing.
    """
    _, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    await _reenter(wp_run)

    manifest = read_run_manifest(wp_run.output_path)
    assert manifest is not None
    _, second, _ = manifest.attempts

    assert recorded_workplan(wp_run) == (
        wp_run.output_path / second.directory / ATTEMPT_WORKPLAN_NAME
    )


@pytest.mark.asyncio
async def test_recorded_workplan_deleted_with_its_attempt_is_not_replaced(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a deleted latest copy is reported as none rather than replaced by an
    older attempt's copy or a legacy backup, both older than that attempt.
    """
    _, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    original_workplan_backup(wp_run.workplan_path, wp_run.output_path).write_text(
        "name: legacy\n"
    )

    manifest = read_run_manifest(wp_run.output_path)
    assert manifest is not None
    _, second = manifest.attempts
    shutil.rmtree(wp_run.output_path / second.directory)

    assert recorded_workplan(wp_run) is None


def _legacy_run(tmp_path: Path) -> WorkplanRun:
    """A recorded run whose run directory exists and holds no manifest yet, only
    its transformed workplan.
    """
    wp_run = WorkplanRun(
        output_path=tmp_path / "run",
        run_id="legacy",
    )
    wp_run.output_path.mkdir()
    (wp_run.output_path / "plan_transformed.yaml").touch()
    return wp_run


@pytest.mark.parametrize("has_manifest", [False, True])
def test_recorded_workplan_falls_back_to_the_legacy_backup(
    tmp_path: Path, has_manifest: bool
) -> None:
    """Verify a run with no manifest, or no attempt that carried inputs, is
    recorded by its legacy backup, and by nothing when that is gone too.
    """
    wp_run = _legacy_run(tmp_path)
    if has_manifest:
        start = datetime(2026, 10, 1, tzinfo=UTC)
        manifest = RunManifest(
            run_id="legacy",
            name="legacy",
            kind="workplan",
            created_at=start,
            attempts=[Attempt(start_at=start)],
            steps=[],
        )
        write_run_manifest(wp_run.output_path, manifest)

    assert recorded_workplan(wp_run) is None

    backup = original_workplan_backup(wp_run.workplan_path, wp_run.output_path)
    backup.write_text("name: legacy\n")
    assert recorded_workplan(wp_run) == backup


def test_recorded_workplan_raises_for_an_unreadable_manifest(tmp_path: Path) -> None:
    """Verify a manifest that cannot be read is reported, not taken for a run
    with nothing recorded, even when a legacy backup exists.
    """
    wp_run = _legacy_run(tmp_path)
    (wp_run.output_path / RUN_MANIFEST_NAME).write_text("name: [unclosed\n")
    original_workplan_backup(wp_run.workplan_path, wp_run.output_path).write_text(
        "name: legacy\n"
    )

    with pytest.raises(CstarError, match="Unable to read the run manifest"):
        recorded_workplan(wp_run)


def _hello_blueprint(path: Path, target: str = "world") -> Path:
    """Write a hello_world blueprint file whose content varies with `target`."""
    path.write_text(
        "name: hello\ndescription: says hello\napplication: hello_world\n"
        f"schema_version: '1.0.0'\ntarget: {target}\n"
    )
    return path


def _guard_workplan(*steps: Step) -> Workplan:
    """Build a `Workplan` of the given steps for the edited-step guard tests."""
    return Workplan(name="guarded", description="a guarded plan", steps=list(steps))


def _hello_step(blueprint: t.Any, name: str, **kwargs: t.Any) -> Step:
    """Build a hello_world `Step` over the given blueprint reference."""
    return Step(name=name, application="hello_world", blueprint=blueprint, **kwargs)


def _recorded(
    run_id: str,
    hashes: t.Mapping[str, str],
    *,
    now: datetime | None = None,
    schema_versions: t.Mapping[str, str] | None = None,
    application: str = "hello_world",
) -> RunManifest:
    """Build a `RunManifest` recording the given definition hash for each step.

    A step named in `schema_versions` also records the schema version of the
    blueprint its hash was computed from.
    """
    now = now or datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
    schema_versions = schema_versions or {}
    return RunManifest(
        run_id=run_id,
        name=run_id,
        kind="workplan",
        created_at=now,
        attempts=[Attempt(start_at=now)],
        steps=[
            StepEntry(
                name=name,
                application=application,
                working_dir=f"tasks/{slugify(name)}",
                definition_hash=definition_hash,
                blueprint_schema_version=schema_versions.get(name, ""),
            )
            for name, definition_hash in hashes.items()
        ],
    )


EDITED_HASH = f"{DEFINITION_HASH_SCHEME}:{'0' * 64}"
"""A recorded definition hash under the current scheme that no step has: the one a
step edited since it was recorded leaves behind."""

UNTAGGED_HASH = "hashed-by-an-earlier-recipe"
"""A recorded definition hash with no scheme, as releases before the tag recorded."""


def _stale(facts: t.Mapping[str, StepFacts]) -> dict[str, str]:
    """Return a recorded hash for each step that differs from its current one."""
    return {name: EDITED_HASH for name in facts}


def test_refuse_edited_steps_refuses_an_edited_done_step(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify a Done step whose definition changed is refused, naming the
    step, the clobber to pass and the run-id alternative.
    """
    wp = _guard_workplan(
        _hello_step(hello_world_bp_path.as_posix(), "A"),
        _hello_step(hello_world_bp_path.as_posix(), "B"),
    )
    facts = describe_steps(wp, tmp_path)
    recorded = {n: f.definition_hash for n, f in facts.items()} | {"B": EDITED_HASH}
    status = DagStatus({"A": Status.Done, "B": Status.Done})

    with pytest.raises(CstarExpectationFailed) as exc_info:
        refuse_edited_steps(wp, _recorded("run", recorded), status, facts, [])

    message = str(exc_info.value)
    assert "'B'" in message
    assert "'A'" not in message
    assert f"{ARG_CLOBBER} B" in message
    assert f"{ARG_CLOBBER} A" not in message
    assert ARG_RUN_ID in message


def test_refuse_edited_steps_names_every_edited_step(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify each edited Done step is named, with one clobber per step."""
    wp = _guard_workplan(
        _hello_step(hello_world_bp_path.as_posix(), "A"),
        _hello_step(hello_world_bp_path.as_posix(), "B"),
    )
    facts = describe_steps(wp, tmp_path)
    status = DagStatus({"A": Status.Done, "B": Status.Done})

    with pytest.raises(CstarExpectationFailed) as exc_info:
        refuse_edited_steps(wp, _recorded("run", _stale(facts)), status, facts, [])

    message = str(exc_info.value)
    assert "'A'" in message
    assert "'B'" in message
    assert message.count(ARG_CLOBBER) == 2
    assert f"{ARG_CLOBBER} A" in message
    assert f"{ARG_CLOBBER} B" in message


@pytest.mark.parametrize("status", [{"A": Status.Failed}, {}])
def test_refuse_edited_steps_ignores_edited_steps_that_failed_or_never_ran(
    tmp_path: Path, hello_world_bp_path: Path, status: dict[str, Status]
) -> None:
    """Verify an edited step that failed or was never submitted is not refused."""
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)

    refuse_edited_steps(
        wp, _recorded("run", _stale(facts)), DagStatus(status), facts, []
    )


def test_refuse_edited_steps_accepts_an_unchanged_done_step(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify a Done step whose definition is unchanged is not refused."""
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)
    recorded = {n: f.definition_hash for n, f in facts.items()}

    refuse_edited_steps(
        wp, _recorded("run", recorded), DagStatus({"A": Status.Done}), facts, []
    )


@pytest.mark.parametrize("by", ["name", "safe_name"])
def test_refuse_edited_steps_honors_clobber(
    tmp_path: Path, hello_world_bp_path: Path, by: str
) -> None:
    """Verify clobbering an edited Done step, by name or safe name, clears it."""
    step = _hello_step(hello_world_bp_path.as_posix(), "Step B")
    assert step.name != step.safe_name
    wp = _guard_workplan(step)
    facts = describe_steps(wp, tmp_path)
    manifest = _recorded("run", _stale(facts))
    status = DagStatus({"Step B": Status.Done})

    with pytest.raises(CstarExpectationFailed):
        refuse_edited_steps(wp, manifest, status, facts, ["other"])

    refuse_edited_steps(wp, manifest, status, facts, [f" {getattr(step, by)} "])


@pytest.mark.parametrize(
    "recorded", [EDITED_HASH, UNTAGGED_HASH], ids=["edited", "untagged"]
)
def test_refuse_edited_steps_honors_a_clobber_the_workplan_sets(
    tmp_path: Path,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
    recorded: str,
) -> None:
    """Verify a Done step whose `workflow_overrides` set `clobber` runs again, so
    it is neither refused nor warned about, as with `--clobber`.
    """
    step = _hello_step(
        hello_world_bp_path.as_posix(), "B", workflow_overrides={"clobber": True}
    )
    wp = _guard_workplan(step)
    facts = describe_steps(wp, tmp_path)

    with caplog.at_level("WARNING"):
        refuse_edited_steps(
            wp,
            _recorded("run", {"B": recorded}),
            DagStatus({"B": Status.Done}),
            facts,
            [],
        )

    assert _warnings(caplog) == []


def test_refuse_edited_steps_names_the_consumer_of_an_edited_producer(
    tmp_path: Path,
) -> None:
    """Verify editing a producer's blueprint also refuses its deferred consumer."""
    blueprint = _hello_blueprint(tmp_path / "bp.yaml")
    wp = _guard_workplan(
        _hello_step(blueprint.as_posix(), "A"),
        _hello_step({"from_step": "A"}, "B", depends_on=["A"]),
    )
    recorded = {n: f.definition_hash for n, f in describe_steps(wp, tmp_path).items()}
    status = DagStatus({"A": Status.Done, "B": Status.Done})

    _hello_blueprint(blueprint, target="moon")
    facts = describe_steps(wp, tmp_path)

    with pytest.raises(CstarExpectationFailed) as exc_info:
        refuse_edited_steps(wp, _recorded("run", recorded), status, facts, [])

    message = str(exc_info.value)
    assert "'A'" in message
    assert "'B'" in message


def test_refuse_edited_steps_skips_an_unrecorded_definition(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify a step recorded without a definition hash is not checked."""
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)

    refuse_edited_steps(
        wp, _recorded("run", {"A": ""}), DagStatus({"A": Status.Done}), facts, []
    )


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the messages of the warnings captured so far."""
    return [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]


@pytest.mark.parametrize(
    "recorded",
    [UNTAGGED_HASH, f"sha256:{'0' * 64}"],
    ids=["untagged", "another-scheme"],
)
def test_refuse_edited_steps_warns_about_a_step_of_an_earlier_recipe(
    tmp_path: Path,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
    recorded: str,
) -> None:
    """Verify a Done step whose recorded hash has another scheme than its current
    one is not refused, and that one warning names it.
    """
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)

    with caplog.at_level("WARNING"):
        refuse_edited_steps(
            wp,
            _recorded("run", {"A": recorded}),
            DagStatus({"A": Status.Done}),
            facts,
            [],
        )

    (message,) = _warnings(caplog)
    assert "'A'" in message
    assert "another C-Star release" in message


def test_refuse_edited_steps_shares_one_warning_between_steps(
    tmp_path: Path,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify several steps recorded under an earlier recipe share one warning."""
    wp = _guard_workplan(
        _hello_step(hello_world_bp_path.as_posix(), "A"),
        _hello_step(hello_world_bp_path.as_posix(), "B"),
    )
    facts = describe_steps(wp, tmp_path)
    recorded = {"A": UNTAGGED_HASH, "B": UNTAGGED_HASH}
    status = DagStatus({"A": Status.Done, "B": Status.Done})

    with caplog.at_level("WARNING"):
        refuse_edited_steps(wp, _recorded("run", recorded), status, facts, [])

    (message,) = _warnings(caplog)
    assert "'A'" in message
    assert "'B'" in message


def test_refuse_edited_steps_refuses_an_edit_beside_an_unchecked_step(
    tmp_path: Path,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a step of the current scheme that was edited is refused even when
    another step could not be compared, and that the refusal names only the edited
    step and warns about nothing: the attempt it stops would not record anything.
    """
    wp = _guard_workplan(
        _hello_step(hello_world_bp_path.as_posix(), "A"),
        _hello_step(hello_world_bp_path.as_posix(), "B"),
    )
    facts = describe_steps(wp, tmp_path)
    recorded = {"A": UNTAGGED_HASH, "B": EDITED_HASH}
    status = DagStatus({"A": Status.Done, "B": Status.Done})

    with caplog.at_level("WARNING"), pytest.raises(CstarExpectationFailed) as exc_info:
        refuse_edited_steps(wp, _recorded("run", recorded), status, facts, [])

    assert "'B'" in str(exc_info.value)
    assert "'A'" not in str(exc_info.value)
    assert _warnings(caplog) == []


@pytest.mark.parametrize("by", ["name", "safe_name"])
def test_refuse_edited_steps_does_not_warn_about_a_clobbered_step(
    tmp_path: Path,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
    by: str,
) -> None:
    """Verify a step of an earlier recipe that is selected for clobber is not
    warned about: it runs again anyway.
    """
    step = _hello_step(hello_world_bp_path.as_posix(), "Step B")
    assert step.name != step.safe_name
    wp = _guard_workplan(step)
    facts = describe_steps(wp, tmp_path)
    manifest = _recorded("run", {"Step B": UNTAGGED_HASH})

    with caplog.at_level("WARNING"):
        refuse_edited_steps(
            wp, manifest, DagStatus({"Step B": Status.Done}), facts, [getattr(step, by)]
        )

    assert _warnings(caplog) == []


@pytest.mark.parametrize("status", [{"A": Status.Failed}, {}])
def test_refuse_edited_steps_does_not_warn_about_a_step_that_failed_or_never_ran(
    tmp_path: Path,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
    status: dict[str, Status],
) -> None:
    """Verify a step of an earlier recipe that failed or was never submitted is
    not warned about: nothing it recorded is relied on.
    """
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)

    with caplog.at_level("WARNING"):
        refuse_edited_steps(
            wp, _recorded("run", {"A": UNTAGGED_HASH}), DagStatus(status), facts, []
        )

    assert _warnings(caplog) == []


def test_refuse_edited_steps_skips_a_step_new_to_the_run(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify a step the manifest does not list is not refused."""
    wp = _guard_workplan(
        _hello_step(hello_world_bp_path.as_posix(), "A"),
        _hello_step(hello_world_bp_path.as_posix(), "New"),
    )
    facts = describe_steps(wp, tmp_path)
    recorded = {"A": facts["A"].definition_hash}
    status = DagStatus({"A": Status.Done, "New": Status.Done})

    refuse_edited_steps(wp, _recorded("run", recorded), status, facts, [])


IN_PROGRESS = pytest.mark.parametrize(
    "in_progress",
    [Status.Submitted, Status.Running, Status.Ending],
    ids=lambda status: status.name,
)
"""The statuses of a step that is still queued or running."""


@IN_PROGRESS
def test_refuse_edited_steps_refuses_an_edited_in_progress_step(
    tmp_path: Path, hello_world_bp_path: Path, in_progress: Status
) -> None:
    """Verify a queued or running step whose definition changed is refused,
    naming the step, the clobber to pass and the run-id alternative.
    """
    wp = _guard_workplan(
        _hello_step(hello_world_bp_path.as_posix(), "A"),
        _hello_step(hello_world_bp_path.as_posix(), "B"),
    )
    facts = describe_steps(wp, tmp_path)
    recorded = {n: f.definition_hash for n, f in facts.items()} | {"B": EDITED_HASH}
    status = DagStatus({"A": in_progress, "B": in_progress})

    with pytest.raises(CstarExpectationFailed) as exc_info:
        refuse_edited_steps(wp, _recorded("run", recorded), status, facts, [])

    message = str(exc_info.value)
    assert "'B'" in message
    assert "'A'" not in message
    assert "queued or running" in message
    assert "reached Done" not in message
    assert f"{ARG_CLOBBER} B" in message
    assert f"{ARG_CLOBBER} A" not in message
    assert ARG_RUN_ID in message


@IN_PROGRESS
def test_refuse_edited_steps_accepts_an_unchanged_in_progress_step(
    tmp_path: Path,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
    in_progress: Status,
) -> None:
    """Verify a queued or running step whose definition is unchanged is neither
    refused nor warned about.
    """
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)
    recorded = {n: f.definition_hash for n, f in facts.items()}

    with caplog.at_level("WARNING"):
        refuse_edited_steps(
            wp, _recorded("run", recorded), DagStatus({"A": in_progress}), facts, []
        )

    assert _warnings(caplog) == []


@IN_PROGRESS
@pytest.mark.parametrize("by", ["name", "safe_name"])
def test_refuse_edited_steps_honors_clobber_of_an_in_progress_step(
    tmp_path: Path, hello_world_bp_path: Path, in_progress: Status, by: str
) -> None:
    """Verify clobbering an edited queued or running step, by name or safe name,
    clears it.
    """
    step = _hello_step(hello_world_bp_path.as_posix(), "Step B")
    assert step.name != step.safe_name
    wp = _guard_workplan(step)
    facts = describe_steps(wp, tmp_path)
    manifest = _recorded("run", _stale(facts))
    status = DagStatus({"Step B": in_progress})

    with pytest.raises(CstarExpectationFailed):
        refuse_edited_steps(wp, manifest, status, facts, ["other"])

    refuse_edited_steps(wp, manifest, status, facts, [f" {getattr(step, by)} "])


@IN_PROGRESS
def test_refuse_edited_steps_honors_a_clobber_the_workplan_sets_on_an_in_progress_step(
    tmp_path: Path, hello_world_bp_path: Path, in_progress: Status
) -> None:
    """Verify a queued or running step whose `workflow_overrides` set `clobber`
    is not refused, as with `--clobber`.
    """
    step = _hello_step(
        hello_world_bp_path.as_posix(), "B", workflow_overrides={"clobber": True}
    )
    wp = _guard_workplan(step)
    facts = describe_steps(wp, tmp_path)

    refuse_edited_steps(
        wp, _recorded("run", _stale(facts)), DagStatus({"B": in_progress}), facts, []
    )


def test_refuse_edited_steps_reports_done_and_in_progress_steps_in_one_error(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify edited Done steps and edited queued or running steps are named in
    one error, each with its own clobber, and that the run-id alternative is
    given once.
    """
    wp = _guard_workplan(
        _hello_step(hello_world_bp_path.as_posix(), "A"),
        _hello_step(hello_world_bp_path.as_posix(), "B"),
        _hello_step(hello_world_bp_path.as_posix(), "C"),
        _hello_step(hello_world_bp_path.as_posix(), "D"),
    )
    facts = describe_steps(wp, tmp_path)
    status = DagStatus(
        {"A": Status.Done, "B": Status.Submitted, "C": Status.Running, "D": Status.Done}
    )
    recorded = _stale(facts) | {"D": facts["D"].definition_hash}

    with pytest.raises(CstarExpectationFailed) as exc_info:
        refuse_edited_steps(wp, _recorded("run", recorded), status, facts, [])

    message = str(exc_info.value)
    assert "'D'" not in message
    assert message.count(ARG_CLOBBER) == 3
    for name in "ABC":
        assert f"{ARG_CLOBBER} {name}" in message
    assert "Steps 'A' reached Done" in message
    assert "Steps 'B', 'C' are still queued or running" in message
    assert message.count(ARG_RUN_ID) == 1


@IN_PROGRESS
def test_refuse_edited_steps_warns_about_an_in_progress_step_of_an_earlier_recipe(
    tmp_path: Path,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
    in_progress: Status,
) -> None:
    """Verify a queued or running step whose recorded hash has another scheme than
    its current one is not refused, and shares one warning with a Done step.
    """
    wp = _guard_workplan(
        _hello_step(hello_world_bp_path.as_posix(), "A"),
        _hello_step(hello_world_bp_path.as_posix(), "B"),
    )
    facts = describe_steps(wp, tmp_path)
    recorded = {"A": UNTAGGED_HASH, "B": UNTAGGED_HASH}
    status = DagStatus({"A": Status.Done, "B": in_progress})

    with caplog.at_level("WARNING"):
        refuse_edited_steps(wp, _recorded("run", recorded), status, facts, [])

    (message,) = _warnings(caplog)
    assert "'A'" in message
    assert "'B'" in message
    assert "another C-Star release" in message


T1 = datetime(2026, 10, 20, 9, 0, tzinfo=UTC)
T2 = T1 + timedelta(hours=1)
T3 = T1 + timedelta(hours=2)
"""Increasing moments for the attempts of a run and the submissions of its steps."""

Attempts = t.Sequence[tuple[datetime, t.Sequence[str]]]
"""The attempts of a run as the moment each started and the steps it clobbered."""


def _clobber_plans(
    blueprint: Path,
    *,
    selected: t.Collection[str] = (),
    standing: t.Collection[str] = (),
) -> tuple[Workplan, Workplan]:
    """Build the workplan a run was authored with and the one replayed from it.

    The authored workplan sets `clobber` on the `standing` steps; the replayed one
    also has the `selected` steps, as an earlier `--clobber` left them.
    """

    def plan(clobbered: t.Collection[str]) -> Workplan:
        return _guard_workplan(
            *(
                _hello_step(
                    blueprint.as_posix(),
                    name,
                    workflow_overrides={KEY_CLOBBER: True} if name in clobbered else {},
                )
                for name in ("A", "B", "C")
            )
        )

    return plan(standing), plan({*selected, *standing})


async def _clobbered_run(
    root: Path,
    run_id: str,
    authored: Workplan,
    attempts: Attempts,
    submitted: t.Mapping[str, datetime | None],
) -> WorkplanRun:
    """Record a run whose attempts clobbered steps and whose steps were submitted.

    The first attempt carries the authored workplan, as a preparing attempt does;
    the others replay it. A step absent from `submitted` has no sentinel.
    """
    carried = f"{ATTEMPTS_DIR_NAME}/first"
    assert serialize(root / carried / ATTEMPT_WORKPLAN_NAME, authored)
    manifest = RunManifest(
        run_id=run_id,
        name=run_id,
        kind="workplan",
        created_at=attempts[0][0],
        attempts=[
            Attempt(
                start_at=start,
                clobbered=list(clobbered),
                directory=carried if i == 0 else "",
            )
            for i, (start, clobbered) in enumerate(attempts)
        ],
        steps=[],
    )
    write_run_manifest(root, manifest)

    repo = StateRepository()
    for name, at in submitted.items():
        handle = ProcessHandle(
            pid="1", name=name, run_id=run_id, status=Status.Done, submitted_at=at
        )
        assert await repo.put_sentinel(handle)
    return WorkplanRun(output_path=root, run_id=run_id)


def _clobbering(wp: Workplan) -> list[str]:
    """Return the names of the steps of a workplan that set `clobber`."""
    return [step.name for step in wp.steps if step.clobber]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "submitted",
    [T2, T1, T2.replace(tzinfo=None)],
    ids=["after-the-attempt-started", "at-the-attempt-start", "naive-moment-is-utc"],
)
async def test_drop_spent_clobbers_drops_a_step_submitted_since_its_attempt_started(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path, submitted: datetime
) -> None:
    """Verify a selected step submitted at or after the start of the attempt that
    clobbered it has spent its clobber: it is dropped and reported.
    """
    authored, wp = _clobber_plans(hello_world_bp_path, selected=["B"])
    wp_run = await _clobbered_run(
        tmp_path / "run", mock_run_id, authored, [(T1, ["B"])], {"B": submitted}
    )

    assert await drop_spent_clobbers(wp, wp_run) == ["B"]
    assert _clobbering(wp) == []


@pytest.mark.asyncio
async def test_drop_spent_clobbers_keeps_a_step_submitted_before_its_attempt(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a selected step last submitted before the attempt that clobbered it
    keeps its clobber, since that attempt stopped before it reached the step.
    """
    authored, wp = _clobber_plans(hello_world_bp_path, selected=["B"])
    wp_run = await _clobbered_run(
        tmp_path / "run", mock_run_id, authored, [(T2, ["B"])], {"B": T1}
    )

    assert await drop_spent_clobbers(wp, wp_run) == []
    assert _clobbering(wp) == ["B"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("attempts", "submitted", "dropped"),
    [
        ([(T1, ["B"]), (T3, ["B"])], T2, False),
        ([(T1, ["B"]), (T3, ["B"])], T3, True),
        ([(T1, ["B"]), (T3, ["A"])], T2, True),
    ],
    ids=[
        "between-two-clobbers",
        "since-the-latest-clobber",
        "later-attempt-clobbers-another-step",
    ],
)
async def test_drop_spent_clobbers_counts_the_latest_clobbering_attempt(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
    attempts: Attempts,
    submitted: datetime,
    dropped: bool,
) -> None:
    """Verify the latest attempt that clobbered the step decides whether its
    clobber is spent, not the first, and not a later attempt that clobbered
    another step.
    """
    authored, wp = _clobber_plans(hello_world_bp_path, selected=["B"])
    wp_run = await _clobbered_run(
        tmp_path / "run", mock_run_id, authored, attempts, {"B": submitted}
    )

    assert await drop_spent_clobbers(wp, wp_run) == (["B"] if dropped else [])
    assert _clobbering(wp) == ([] if dropped else ["B"])


@pytest.mark.asyncio
async def test_drop_spent_clobbers_keeps_a_clobber_the_authored_workplan_sets(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a clobber the authored workplan sets is kept though the step was
    submitted since, since every re-entry applies it.
    """
    authored, wp = _clobber_plans(hello_world_bp_path, selected=["C"], standing=["B"])
    wp_run = await _clobbered_run(
        tmp_path / "run",
        mock_run_id,
        authored,
        [(T1, ["B", "C"])],
        {"B": T2, "C": T2},
    )

    assert await drop_spent_clobbers(wp, wp_run) == ["C"]
    assert _clobbering(wp) == ["B"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "submitted",
    [{}, {"B": None}],
    ids=["no-sentinel", "no-submission-time"],
)
async def test_drop_spent_clobbers_keeps_a_step_without_a_submission_time(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
    submitted: t.Mapping[str, datetime | None],
) -> None:
    """Verify a selected step with no sentinel, or one written before submission
    times were recorded, keeps its clobber: nothing shows it was submitted since.
    """
    authored, wp = _clobber_plans(hello_world_bp_path, selected=["B"])
    wp_run = await _clobbered_run(
        tmp_path / "run", mock_run_id, authored, [(T1, ["B"])], submitted
    )

    assert await drop_spent_clobbers(wp, wp_run) == []
    assert _clobbering(wp) == ["B"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attempts",
    [[(T1, ["A"])], [(T1, [])]],
    ids=["another-step-clobbered", "nothing-clobbered"],
)
async def test_drop_spent_clobbers_keeps_a_step_no_attempt_clobbered(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path, attempts: Attempts
) -> None:
    """Verify a selected step that no attempt lists as clobbered keeps its clobber
    though it was submitted since.
    """
    authored, wp = _clobber_plans(hello_world_bp_path, selected=["B"])
    wp_run = await _clobbered_run(
        tmp_path / "run", mock_run_id, authored, attempts, {"B": T2}
    )

    assert await drop_spent_clobbers(wp, wp_run) == []
    assert _clobbering(wp) == ["B"]


@pytest.mark.asyncio
async def test_drop_spent_clobbers_without_a_manifest(
    tmp_path: Path, hello_world_bp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify a run with no manifest, which cannot say when a step was selected,
    drops nothing and says nothing.
    """
    _, wp = _clobber_plans(hello_world_bp_path, selected=["B"])
    wp_run = _legacy_run(tmp_path)

    with caplog.at_level("WARNING"):
        assert await drop_spent_clobbers(wp, wp_run) == []

    assert _clobbering(wp) == ["B"]
    assert _warnings(caplog) == []


@pytest.mark.asyncio
async def test_drop_spent_clobbers_warns_once_for_an_unreadable_manifest(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a manifest that cannot be read drops nothing and warns once, naming
    the run, since the clobber selections are then applied again.
    """
    _, wp = _clobber_plans(hello_world_bp_path, selected=["B"])
    root = tmp_path / "run"
    root.mkdir()
    (root / RUN_MANIFEST_NAME).write_text("name: [unclosed\n")
    wp_run = WorkplanRun(output_path=root, run_id=mock_run_id)

    with caplog.at_level("WARNING"):
        assert await drop_spent_clobbers(wp, wp_run) == []

    assert _clobbering(wp) == ["B"]
    assert len(warnings := _warnings(caplog)) == 1
    assert mock_run_id in warnings[0]


@pytest.mark.asyncio
async def test_drop_spent_clobbers_leaves_steps_without_clobber_alone(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a workplan that selects no step is left as it is, whatever the
    attempts clobbered and the steps did since.
    """
    authored, wp = _clobber_plans(hello_world_bp_path)
    wp_run = await _clobbered_run(
        tmp_path / "run",
        mock_run_id,
        authored,
        [(T1, ["A", "B", "C"])],
        {"A": T2, "B": T2, "C": T2},
    )

    assert await drop_spent_clobbers(wp, wp_run) == []
    assert [step.workflow_overrides for step in wp.steps] == [{}, {}, {}]


@pytest.mark.asyncio
async def test_drop_spent_clobbers_drops_only_the_spent_steps(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify of two selected steps only the one submitted since its attempt
    started is dropped and reported; the other keeps its clobber, and a step that
    was not selected is not touched.
    """
    authored, wp = _clobber_plans(hello_world_bp_path, selected=["B", "C"])
    wp_run = await _clobbered_run(
        tmp_path / "run",
        mock_run_id,
        authored,
        [(T2, ["A", "B", "C"])],
        {"A": T3, "B": T3, "C": T1},
    )

    assert await drop_spent_clobbers(wp, wp_run) == ["B"]
    assert _clobbering(wp) == ["C"]
    assert [step.workflow_overrides for step in wp.steps if step.name != "C"] == [
        {},
        {},
    ]


def _build_guarded(tmp_path: Path, run_id: str, blueprint: Path) -> tuple[Path, Path]:
    """Author a one-step workplan and return its path and its run directory."""
    wp_path = tmp_path / "guarded.yaml"
    wp = _guard_workplan(_hello_step(blueprint.as_posix(), "hello"))
    assert serialize(wp_path, wp)
    output_dir = StateDirectoryManager.data_dir(run_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    return wp_path, output_dir


@pytest.mark.asyncio
async def test_build_dag_without_a_manifest_loads_no_run_state(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a run directory without a manifest is entered without reading
    the run's state.
    """
    wp_path, _ = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        mock.patch(
            "cstar.orchestration.dag_runner.load_run_state", mock.AsyncMock()
        ) as load_state,
    ):
        await build_dag(wp_path, mock_run_id)

    load_state.assert_not_called()


def _leave_sentinel(run_id: str, step: str) -> Path:
    """Write a Done sentinel for `step`, as an earlier run of `run_id` left it."""
    sentinel = StateRepository.sentinel_path(step, run_id=run_id)
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    handle = ProcessHandle(
        pid="1", name=step, run_id=run_id, status=Status.Done, run_uuid="earlier"
    )
    assert serialize(sentinel, handle)
    return sentinel


@pytest.mark.asyncio
async def test_build_dag_refuses_the_state_of_a_run_whose_directory_is_gone(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a run-id whose sentinels outlived its run directory is refused
    before anything is written, naming the state directory and how to clear it.
    """
    wp_path, output_dir = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)
    sentinel = _leave_sentinel(mock_run_id, "hello")

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        pytest.raises(CstarExpectationFailed) as exc_info,
    ):
        await build_dag(wp_path, mock_run_id)

    msg = str(exc_info.value)
    assert str(sentinel.parent) in msg
    assert f"cstar admin clean {ARG_RUN_ID} {mock_run_id}" in msg
    assert f"{ARG_RUN_ID} <other-id>" in msg
    assert not list(output_dir.glob(TRANSFORMED_WORKPLAN_GLOB))


@pytest.mark.asyncio
async def test_build_dag_refuses_state_over_a_run_directory_left_without_its_files(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a run directory that holds something but neither its manifest nor
    its transformed workplan is refused without advice to `admin clean` it,
    which would remove what is left.
    """
    wp_path, output_dir = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)
    sentinel = _leave_sentinel(mock_run_id, "hello")
    (output_dir / "tasks").mkdir()

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        pytest.raises(CstarExpectationFailed) as exc_info,
    ):
        await build_dag(wp_path, mock_run_id)

    msg = str(exc_info.value)
    assert f"Remove {sentinel.parent}" in msg
    assert "admin clean" not in msg


@pytest.mark.asyncio
async def test_build_dag_refuses_state_of_a_run_tracked_in_another_directory(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a run-id whose tracked run lives in another directory (another
    data home) is refused naming that directory, without advice to
    `admin clean` it, which would remove that run.
    """
    wp_path, _ = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)
    _leave_sentinel(mock_run_id, "hello")
    elsewhere = tmp_path / "other-data-home" / mock_run_id
    elsewhere.mkdir(parents=True)
    TrackingRepository().put_workplan_run_sync(
        WorkplanRun(output_path=elsewhere, run_id=mock_run_id)
    )

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        pytest.raises(CstarExpectationFailed) as exc_info,
    ):
        await build_dag(wp_path, mock_run_id)

    msg = str(exc_info.value)
    assert f"tracked in {elsewhere}" in msg
    assert "admin clean" not in msg


@pytest.mark.asyncio
async def test_build_dag_re_enters_a_run_directory_that_has_no_manifest(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a run directory that still holds its transformed workplan is
    re-entered with its sentinels, even without a manifest: one could not be
    written, or the run predates manifests.
    """
    wp_path, output_dir = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)
    _leave_sentinel(mock_run_id, "hello")
    (output_dir / f"{wp_path.stem}_transformed.yaml").write_text(wp_path.read_text())

    with mock.patch("cstar.orchestration.dag_runner.check_environment"):
        await build_dag(wp_path, mock_run_id)

    assert read_run_manifest(output_dir) is None


@pytest.mark.asyncio
async def test_build_dag_refuses_a_corrupt_manifest_before_writing_anything(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify an unreadable manifest refuses the re-entry before the transformed
    workplan is written.
    """
    wp_path, output_dir = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)
    (output_dir / RUN_MANIFEST_NAME).write_text("[]")

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        pytest.raises(CstarExpectationFailed, match="cannot be re-entered"),
    ):
        await build_dag(wp_path, mock_run_id)

    assert not list(output_dir.glob("*_transformed.yaml*"))
    assert not (output_dir / ATTEMPTS_DIR_NAME).exists()


@pytest.mark.asyncio
async def test_build_dag_refuses_a_too_new_manifest_with_upgrade_guidance(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a manifest written by a newer release refuses the re-entry with the
    upgrade guidance, not with advice to remove the file.
    """
    wp_path, output_dir = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)
    manifest = _recorded(mock_run_id, {"hello": "stale"})
    manifest.schema_version = "999.0.0"
    # as a newer build wrote it: `write_run_manifest` stamps this build's version
    assert serialize(output_dir / RUN_MANIFEST_NAME, manifest)

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        pytest.raises(CstarExpectationFailed, match="Upgrade cstar-ocean") as exc_info,
    ):
        await build_dag(wp_path, mock_run_id)

    assert "remove the file" not in str(exc_info.value)
    assert ARG_RUN_ID in str(exc_info.value)
    assert not list(output_dir.glob("*_transformed.yaml*"))
    assert not (output_dir / ATTEMPTS_DIR_NAME).exists()


@pytest.mark.asyncio
async def test_build_dag_refuses_an_edited_done_step_before_writing_anything(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify an edited Done step refuses the re-entry before the transformed
    workplan is written, and that clobbering the step lets it through.
    """
    wp_path, output_dir = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)
    write_run_manifest(output_dir, _recorded(mock_run_id, {"hello": EDITED_HASH}))
    done = DagStatus({"hello": Status.Done})

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        mock.patch(
            "cstar.orchestration.dag_runner.load_run_state",
            mock.AsyncMock(return_value=done),
        ),
    ):
        with pytest.raises(
            CstarExpectationFailed, match=re.escape(f"{ARG_CLOBBER} hello")
        ):
            await build_dag(wp_path, mock_run_id)

        assert not list(output_dir.glob("*_transformed.yaml*"))
        assert not (output_dir / ATTEMPTS_DIR_NAME).exists()

        await build_dag(wp_path, mock_run_id, clobber_steps=["hello"])

    assert list(output_dir.glob("*_transformed.yaml*"))
    assert len(list((output_dir / ATTEMPTS_DIR_NAME).iterdir())) == 1


@pytest.mark.asyncio
async def test_build_dag_refuses_an_edited_queued_step_before_writing_anything(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify an edited queued step refuses the re-entry before the transformed
    workplan is written, and that clobbering the step lets it through.
    """
    wp_path, output_dir = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)
    write_run_manifest(output_dir, _recorded(mock_run_id, {"hello": EDITED_HASH}))
    queued = DagStatus({"hello": Status.Submitted})

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        mock.patch(
            "cstar.orchestration.dag_runner.load_run_state",
            mock.AsyncMock(return_value=queued),
        ),
    ):
        with pytest.raises(
            CstarExpectationFailed, match=re.escape(f"{ARG_CLOBBER} hello")
        ):
            await build_dag(wp_path, mock_run_id)

        assert not list(output_dir.glob("*_transformed.yaml*"))
        assert not (output_dir / ATTEMPTS_DIR_NAME).exists()

        await build_dag(wp_path, mock_run_id, clobber_steps=["hello"])

    assert list(output_dir.glob("*_transformed.yaml*"))
    assert len(list((output_dir / ATTEMPTS_DIR_NAME).iterdir())) == 1


CURRENT_MANIFEST_SCHEMA = RunManifest.model_fields["schema_version"].default
"""The schema version this build writes in a run manifest."""


def _write_manifest_as_of(root: Path, manifest: RunManifest, version: str) -> None:
    """Write a manifest as the build of schema `version` left it.

    Writers stamp the newer of the document's version and this build's, so
    the version is set in the text written, and read back to be sure it took.
    """
    path = root / RUN_MANIFEST_NAME
    assert serialize(path, manifest)
    stamped = f"schema_version: {manifest.schema_version}"
    assert stamped in path.read_text()
    path.write_text(path.read_text().replace(stamped, f"schema_version: {version}", 1))
    assert f"schema_version: {version}" in path.read_text()


@pytest.mark.asyncio
async def test_build_dag_accepts_a_done_step_hashed_by_an_earlier_recipe(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify re-entering a run whose Done step records an untagged hash (one an
    earlier release computed) is not refused, that a warning names the step, and
    that scheduling it rewrites the manifest under the current schema with the
    step's current, tagged hash.
    """
    wp_path, output_dir = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)
    older = _recorded(mock_run_id, {"hello": UNTAGGED_HASH})
    _write_manifest_as_of(output_dir, older, "1.4.0")
    done = DagStatus({"hello": Status.Done})

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        mock.patch(
            "cstar.orchestration.dag_runner.load_run_state",
            mock.AsyncMock(return_value=done),
        ),
        mock.patch("cstar.orchestration.dag_runner.get_orchestrator"),
        mock.patch("cstar.orchestration.dag_runner.process_plan", mock.AsyncMock()),
        caplog.at_level("WARNING"),
    ):
        planner, trx_path, facts, carried = await build_dag(wp_path, mock_run_id)
        await run_dag(
            wp_path,
            trx_path,
            mock_run_id,
            planner,
            step_facts=facts,
            carried=carried,
        )

    (message,) = [m for m in _warnings(caplog) if "another C-Star release" in m]
    assert "'hello'" in message
    rewritten = read_run_manifest(output_dir)
    assert rewritten is not None
    assert rewritten.schema_version == CURRENT_MANIFEST_SCHEMA
    entry = rewritten.step("hello")
    assert entry is not None
    assert entry.definition_hash == facts["hello"].definition_hash
    assert entry.definition_hash.startswith(f"{DEFINITION_HASH_SCHEME}:")
    assert UNTAGGED_HASH not in (output_dir / RUN_MANIFEST_NAME).read_text()


@pytest.mark.asyncio
@pytest.mark.parametrize("writer", ["reentry", "refresh"])
@pytest.mark.parametrize(
    ("recorded", "refused"),
    [
        pytest.param(UNTAGGED_HASH, False, id="untagged"),
        pytest.param(EDITED_HASH, True, id="current-scheme"),
    ],
)
async def test_restamping_does_not_make_an_older_hash_comparable(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
    writer: str,
    recorded: str,
    refused: bool,
) -> None:
    """Verify a writer that restamps an older manifest and keeps its recorded
    hashes (a re-entry that replays the recorded plan, or a status refresh) does
    not turn an untagged one into a hash the next authored re-entry compares.

    A hash under the current scheme that differs still has its Done step refused.
    """
    wp_path, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    root = wp_run.output_path
    manifest = read_run_manifest(root)
    assert manifest is not None
    manifest.steps[0].definition_hash = recorded
    _write_manifest_as_of(root, manifest, "1.4.0")

    if writer == "reentry":
        await _reenter(wp_run)
    else:
        refresh_run_manifest(root, mock_run_id, DagStatus({"hello": Status.Done}))

    # the writer restamped the file under this build's schema
    assert (
        f"schema_version: {CURRENT_MANIFEST_SCHEMA}"
        in (root / RUN_MANIFEST_NAME).read_text()
    )
    wp = deserialize(wp_path, Workplan)
    facts = describe_steps(wp, wp_path.parent)
    reread = read_run_manifest(root)
    assert reread is not None
    done = DagStatus({"hello": Status.Done})

    if refused:
        with pytest.raises(CstarExpectationFailed):
            refuse_edited_steps(wp, reread, done, facts, [])
    else:
        refuse_edited_steps(wp, reread, done, facts, [])


def test_refuse_edited_steps_warns_about_a_step_whose_blueprint_changed_major(
    tmp_path: Path,
    hello_world_bp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a Done step whose definition differs under the current scheme is not
    refused when its blueprint's schema major moved since it was recorded (an
    automatic migration changes the content hashed), and that one warning names it.
    """
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)
    current_major = int(facts["A"].blueprint_schema_version.split(".")[0])
    recorded = _recorded(
        "run", {"A": EDITED_HASH}, schema_versions={"A": f"{current_major - 1}.2.0"}
    )

    with caplog.at_level("WARNING"):
        refuse_edited_steps(wp, recorded, DagStatus({"A": Status.Done}), facts, [])

    (message,) = _warnings(caplog)
    assert "'A'" in message
    assert "migrated" in message


@pytest.mark.parametrize(
    "recorded_version",
    ["1.0.0", "1.9.9", ""],
    ids=["same-version", "same-major", "unrecorded"],
)
def test_refuse_edited_steps_refuses_a_changed_hash_within_a_schema_major(
    tmp_path: Path, hello_world_bp_path: Path, recorded_version: str
) -> None:
    """Verify a Done step whose hash differs is still refused when its blueprint's
    schema major did not move, or no schema version was recorded to compare.
    """
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)
    assert facts["A"].blueprint_schema_version == "1.0.0"
    recorded = _recorded(
        "run", {"A": EDITED_HASH}, schema_versions={"A": recorded_version}
    )

    with pytest.raises(CstarExpectationFailed, match="'A'"):
        refuse_edited_steps(wp, recorded, DagStatus({"A": Status.Done}), facts, [])


def test_refuse_edited_steps_refuses_an_edit_beside_a_migrated_step(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify a migrated step's warning does not excuse an edit to another step:
    only the edited step is refused.
    """
    wp = _guard_workplan(
        _hello_step(hello_world_bp_path.as_posix(), "A"),
        _hello_step(hello_world_bp_path.as_posix(), "B"),
    )
    facts = describe_steps(wp, tmp_path)
    recorded = _recorded(
        "run",
        {"A": EDITED_HASH, "B": EDITED_HASH},
        schema_versions={"A": "0.9.0", "B": "1.0.0"},
    )

    with pytest.raises(CstarExpectationFailed) as exc_info:
        refuse_edited_steps(
            wp, recorded, DagStatus({"A": Status.Done, "B": Status.Done}), facts, []
        )

    assert "'B'" in str(exc_info.value)
    assert "'A'" not in str(exc_info.value)


STALE_STEP = "roms"
"""The name of the step over the stale blueprint of the migration re-entry tests."""


def _author_stale_roms(tmp_path: Path, bp_templates_dir: Path) -> tuple[Path, Path]:
    """Author a one-step ucla_roms workplan over a copy of a schema 2.1.0 blueprint.

    Returns the path of the workplan, which opens with a comment that
    re-serializing it would drop, and the path of the blueprint copy.
    """
    blueprint = tmp_path / "authored_blueprint.yaml"
    shutil.copy(bp_templates_dir / "roms_marbl" / "blueprint.2.1.0.yaml", blueprint)
    wp = _guard_workplan(
        Step(name=STALE_STEP, application="ucla_roms", blueprint=blueprint.as_posix())
    )
    wp_path = tmp_path / "roms_plan.yaml"
    assert serialize(wp_path, wp)
    wp_path.write_text(f"# user comment\n{wp_path.read_text()}")
    return wp_path, blueprint


def _migrate(wp_path: Path) -> dict[str, Path]:
    """Migrate the blueprints of the workplan at `wp_path` as a submit does."""
    return migrate_steps(wp_path, deserialize(wp_path, Workplan))


def _record_pre_migration_run(run_id: str) -> Path:
    """Record the stale step as Done under the hash a release before the bump left.

    The hash is one no current definition has, recorded beside the schema version
    of the blueprint it was computed from. Returns the run directory.
    """
    output_dir = StateDirectoryManager.data_dir(run_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = _recorded(
        run_id,
        {STALE_STEP: EDITED_HASH},
        schema_versions={STALE_STEP: "2.1.0"},
        application="ucla_roms",
    )
    write_run_manifest(output_dir, manifest)
    return output_dir


async def _reenter_authored(
    wp_path: Path, run_id: str, sources: t.Mapping[str, Path]
) -> tuple[Planner, Path, dict[str, StepFacts], t.Any]:
    """Prepare and schedule the authored workplan over a run whose steps are Done.

    The guard, `describe_steps` and `prepare_workplan` run for real; the
    environment check, the launcher's state and the orchestrator are stood in for.
    """
    done = DagStatus({STALE_STEP: Status.Done})
    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        mock.patch(
            "cstar.orchestration.dag_runner.load_run_state",
            mock.AsyncMock(return_value=done),
        ),
        mock.patch("cstar.orchestration.dag_runner.get_orchestrator"),
        mock.patch("cstar.orchestration.dag_runner.process_plan", mock.AsyncMock()),
    ):
        planner, trx_path, facts, carried = await build_dag(
            wp_path, run_id, blueprint_sources=sources
        )
        await run_dag(
            wp_path,
            trx_path,
            run_id,
            planner,
            step_facts=facts,
            carried=carried,
        )
    return planner, trx_path, facts, carried


def _migration_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the captured warnings about definitions the guard cannot compare."""
    return [m for m in _warnings(caplog) if "another C-Star release" in m]


@pytest.mark.asyncio
async def test_reentry_after_blueprint_migration_reuses_done_step(
    tmp_path: Path,
    mock_run_id: str,
    bp_templates_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify re-entering a run whose Done step was recorded under a blueprint of
    an older schema major (the release before the bump hashed the blueprint as
    authored) does not refuse it, that a warning names the step, and that the
    step is prepared from the migrated copy while the authored workplan stays as
    written and the manifest records the new schema version and the definition
    hash of the authored reference.
    """
    wp_path, blueprint = _author_stale_roms(tmp_path, bp_templates_dir)
    authored = wp_path.read_bytes()
    output_dir = _record_pre_migration_run(mock_run_id)
    sources = _migrate(wp_path)
    assert set(sources) == {STALE_STEP}

    with caplog.at_level("WARNING"):
        _, trx_path, facts, carried = await _reenter_authored(
            wp_path, mock_run_id, sources
        )

    # not refused, and the step is warned about by name
    (message,) = _migration_warnings(caplog)
    assert f"'{STALE_STEP}'" in message
    assert "migrated" in message

    # the Done step is reused: nothing marks it to run again
    prepared = LiveWorkplan.load(trx_path)
    assert not prepared.steps[0].clobber

    # the authored workplan and blueprint are as written
    assert wp_path.read_bytes() == authored
    assert "schema_version: 2.1.0" in blueprint.read_text()

    # the attempt prepared the migrated copy
    current = get_application("ucla_roms").schema_version
    carried_copy = output_dir / carried.carried_blueprints[STALE_STEP]
    assert f"schema_version: {current}" in carried_copy.read_text()

    # the manifest records the current schema and the authored reference's hash
    manifest = read_run_manifest(output_dir)
    assert manifest is not None
    entry = manifest.step(STALE_STEP)
    assert entry is not None
    assert entry.blueprint_schema_version == current
    assert entry.definition_hash == facts[STALE_STEP].definition_hash
    assert entry.definition_hash != EDITED_HASH
    authored_hash = describe_steps(deserialize(wp_path, Workplan), tmp_path, sources)
    assert entry.definition_hash == authored_hash[STALE_STEP].definition_hash
    at_copy = _guard_workplan(
        Step(
            name=STALE_STEP,
            application="ucla_roms",
            blueprint=sources[STALE_STEP].as_posix(),
        )
    )
    by_copy = describe_steps(at_copy, tmp_path)
    assert by_copy[STALE_STEP].blueprint_schema_version == current
    assert by_copy[STALE_STEP].definition_hash != entry.definition_hash


@pytest.mark.asyncio
async def test_second_reentry_after_migration_is_compared_again(
    tmp_path: Path,
    mock_run_id: str,
    bp_templates_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify once a re-entry recorded the migrated step's definition, the next one
    raises no warning about it, and an edit to the authored blueprint that
    survives the migration is refused as an edit to a Done step.
    """
    wp_path, blueprint = _author_stale_roms(tmp_path, bp_templates_dir)
    _record_pre_migration_run(mock_run_id)
    await _reenter_authored(wp_path, mock_run_id, _migrate(wp_path))

    caplog.clear()
    with caplog.at_level("WARNING"):
        await _reenter_authored(wp_path, mock_run_id, _migrate(wp_path))
    assert not _migration_warnings(caplog)

    # the schema version recorded is the current one, so a major no longer moved
    blueprint.write_text(
        blueprint.read_text().replace("n_procs_x: 16", "n_procs_x: 8", 1)
    )
    sources = _migrate(wp_path)
    with pytest.raises(CstarExpectationFailed, match="different definition"):
        await _reenter_authored(wp_path, mock_run_id, sources)


@pytest.mark.asyncio
async def test_reentry_after_migration_with_user_edit_is_warned(
    tmp_path: Path,
    mock_run_id: str,
    bp_templates_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify an edit made to the blueprint while its schema major moved is warned
    about, not refused: the guard cannot tell the edit from the migration, and
    names the step so the user can pass the clobber for it.
    """
    wp_path, blueprint = _author_stale_roms(tmp_path, bp_templates_dir)
    blueprint.write_text(
        blueprint.read_text().replace("n_procs_x: 16", "n_procs_x: 8", 1)
    )
    _record_pre_migration_run(mock_run_id)
    sources = _migrate(wp_path)

    with caplog.at_level("WARNING"):
        await _reenter_authored(wp_path, mock_run_id, sources)

    (message,) = _migration_warnings(caplog)
    assert f"'{STALE_STEP}'" in message
    assert f"{ARG_CLOBBER} <step-name>" in message


@pytest.mark.asyncio
async def test_refresh_run_manifest_folds_records_and_closes_the_attempt(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a refresh copies the observed statuses and a step's record into the
    manifest, and closes the attempt once every step is terminal.
    """
    _, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    root = wp_run.output_path
    manifest = read_run_manifest(root)
    assert manifest is not None
    assert manifest.observed_at is None

    running = refresh_run_manifest(
        root, mock_run_id, DagStatus({"hello": Status.Running})
    )
    assert running is not None
    entry = running.step("hello")
    assert entry is not None
    assert entry.status == Status.Running.name
    assert entry.record is None
    assert running.attempts[-1].finished_at is None
    assert running.observed_at is not None

    (root / "tasks" / "hello").mkdir(parents=True, exist_ok=True)
    write_step_record(
        root / "tasks" / "hello",
        StepRecord(
            step="hello",
            application="hello_world",
            blueprint=BlueprintIdentity(
                kind="Blueprint", application="hello_world", name="bp"
            ),
            outcome="completed",
            finished_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
    )
    done = refresh_run_manifest(root, mock_run_id, DagStatus({"hello": Status.Done}))

    assert done is not None
    entry = done.step("hello")
    assert entry is not None
    assert entry.status == Status.Done.name
    assert entry.record is not None
    assert done.attempts[-1].status == AttemptStatus.done
    assert done.attempts[-1].finished_at is not None
    assert read_run_manifest(root) == done


@pytest.mark.asyncio
async def test_refresh_run_manifest_leaves_an_unchanged_manifest_untouched(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify refreshing again, with nothing changed besides the time observed,
    does not rewrite the manifest: not its bytes, not its modification time.
    """
    _, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    root = wp_run.output_path
    path = root / RUN_MANIFEST_NAME
    refresh_run_manifest(root, mock_run_id, DagStatus({"hello": Status.Running}))
    before = (path.read_bytes(), path.stat().st_mtime_ns)

    again = refresh_run_manifest(
        root, mock_run_id, DagStatus({"hello": Status.Running})
    )

    assert again is not None
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


@pytest.mark.asyncio
async def test_refresh_run_manifest_does_not_write_a_newer_steps_record(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a refresh that folds in a step record a newer minor release wrote,
    with a key this build does not know, returns the folded manifest but does not
    write it, so the key is not dropped from the run's files.
    """
    _, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    root = wp_run.output_path
    step_root = root / "tasks" / "hello"
    step_root.mkdir(parents=True, exist_ok=True)
    record = write_step_record(
        step_root,
        StepRecord(
            step="hello",
            application="hello_world",
            blueprint=BlueprintIdentity(
                kind="Blueprint", application="hello_world", name="bp"
            ),
            outcome="completed",
            finished_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
    )
    document = yaml.safe_load(record.read_text())
    major, minor, _ = document["schema_version"].split(".")
    document["schema_version"] = f"{major}.{int(minor) + 1}.0"
    document["future_field"] = True
    record.write_text(yaml.safe_dump(document))
    path = root / RUN_MANIFEST_NAME
    before = (path.read_bytes(), path.stat().st_mtime_ns)

    refreshed = refresh_run_manifest(
        root, mock_run_id, DagStatus({"hello": Status.Done})
    )

    assert refreshed is not None
    entry = refreshed.step("hello")
    assert entry is not None
    assert entry.status == Status.Done.name
    assert entry.record is not None
    assert entry.record.unknown_keys == ("future_field",)
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


@pytest.mark.asyncio
async def test_refresh_run_manifest_does_not_write_a_newer_manifest_with_new_keys(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a manifest a newer minor release wrote with a key this build does
    not know is refreshed in memory only, and its file is left as it was.
    """
    _, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    root = wp_run.output_path
    path = root / RUN_MANIFEST_NAME
    document = yaml.safe_load(path.read_text())
    major, minor, _ = document["schema_version"].split(".")
    document["schema_version"] = f"{major}.{int(minor) + 1}.0"
    document["future_field"] = True
    path.write_text(yaml.safe_dump(document))
    before = (path.read_bytes(), path.stat().st_mtime_ns)

    refreshed = refresh_run_manifest(
        root, mock_run_id, DagStatus({"hello": Status.Running})
    )

    assert refreshed is not None
    entry = refreshed.step("hello")
    assert entry is not None
    assert entry.status == Status.Running.name
    assert refreshed.unknown_keys == ("future_field",)
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


@pytest.mark.asyncio
async def test_refresh_run_manifest_keeps_the_version_of_a_newer_manifest(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify a refresh that does write a newer minor manifest holding nothing
    this build does not know keeps the version it was written at.
    """
    _, wp_run = await _schedule(tmp_path, mock_run_id, hello_world_bp_path)
    root = wp_run.output_path
    path = root / RUN_MANIFEST_NAME
    document = yaml.safe_load(path.read_text())
    major, minor, _ = document["schema_version"].split(".")
    newer = f"{major}.{int(minor) + 1}.0"
    document["schema_version"] = newer
    path.write_text(yaml.safe_dump(document))

    refreshed = refresh_run_manifest(
        root, mock_run_id, DagStatus({"hello": Status.Running})
    )

    assert refreshed is not None
    assert f"schema_version: {newer}" in path.read_text()
    reread = read_run_manifest(root)
    assert reread is not None
    entry = reread.step("hello")
    assert entry is not None
    assert entry.status == Status.Running.name


def test_refresh_run_manifest_without_a_manifest(
    tmp_path: Path, mock_run_id: str
) -> None:
    """Verify a run that predates the manifest is left alone."""
    result = refresh_run_manifest(tmp_path, mock_run_id, DagStatus({}))

    assert result is None
    assert not (tmp_path / RUN_MANIFEST_NAME).exists()


def test_refresh_run_manifest_survives_a_corrupt_manifest(
    tmp_path: Path, mock_run_id: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify an observer command warns about, rather than crashes on, a corrupt manifest."""
    (tmp_path / RUN_MANIFEST_NAME).write_text("{not: [valid")

    with caplog.at_level("WARNING"):
        result = refresh_run_manifest(tmp_path, mock_run_id, DagStatus({}))

    assert result is None
    assert RUN_MANIFEST_NAME in caplog.text


@pytest.mark.parametrize(
    "error",
    [
        LockTimeoutError("another holder kept the lock"),
        LockUnsupportedError("flock is not supported"),
        PermissionError("read-only file system"),
    ],
    ids=["lock-busy", "no-flock", "unwritable"],
)
def test_refresh_run_manifest_warns_when_the_manifest_cannot_be_updated(
    tmp_path: Path,
    mock_run_id: str,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
) -> None:
    """Verify `--resume`, which refreshes the manifest before scheduling, is not
    stopped by a manifest it cannot update: the refresh warns and returns nothing.
    """
    with (
        mock.patch("cstar.orchestration.dag_runner.manifest_lock", side_effect=error),
        caplog.at_level("WARNING"),
    ):
        result = refresh_run_manifest(tmp_path, mock_run_id, DagStatus({}))

    assert result is None
    assert str(error) in caplog.text


@pytest.mark.asyncio
async def test_executive_run_summary_pairs_each_step_with_its_own_sentinel(
    layered_workplan: tuple[Workplan, dict[str, LocalHandle]],
    mock_run_id: str,
) -> None:
    """Verify the run summary reports each step's own status and task id.

    `WorkplanRun.sentinels` is an unordered set, so the summary must match
    sentinels to steps by name rather than by iteration position.
    """
    workplan, handles = layered_workplan
    state_repo = StateRepository()
    run_repo = TrackingRepository()

    done_names = {"Step 0", "Step 2", "Step 4"}
    unlaunched_name = "Step 5"
    expected: dict[str, Status] = {}

    wp_run = await run_repo.get_workplan_run(mock_run_id)
    assert wp_run is not None

    for handle in handles.values():
        handle.status = Status.Done if handle.name in done_names else Status.Running
        path = await state_repo.put_sentinel(handle)
        assert path is not None

        if handle.name == unlaunched_name:
            # a step that was never launched has no sentinel on disk or on the run
            path.unlink()
            expected[handle.name] = Status.Unsubmitted
            continue

        wp_run.sentinels.add(path)
        expected[handle.name] = handle.status

    summary = await ExecutiveRunSummary.from_run(wp_run)

    assert [s.name for s in summary.steps] == [s.name for s in workplan.steps]
    for step_summary in summary.steps:
        assert step_summary.status == expected[step_summary.name].name
        handle = handles[step_summary.name]
        if step_summary.name == unlaunched_name:
            assert step_summary.task_id == ""
        else:
            assert step_summary.task_id == handle.pid
        assert step_summary.sentinel_path.name == StateRepository.sentinel_name(
            handle.safe_name
        )


@pytest.fixture
async def status_change_workplan_run(
    tmp_path: Path,
    mock_run_id: str,
) -> WorkplanRun:
    """Persist a `WorkplanRun` whose transformed workplan is a real `LiveWorkplan`.

    Parameters
    ----------
    tmp_path : Path
        Unique path for test-specific files.
    mock_run_id : str
        A unique run-id that has already been added to os.environ.

    Returns
    -------
    WorkplanRun
        The persisted run record, with a single step named `step-a`.
    """
    bp_path = tmp_path / "bp.yaml"
    bp_path.touch()
    step = LiveStep(
        name="step-a",
        application=Application.HELLO_WORLD,
        blueprint=bp_path,
        working_dir=tmp_path / "step-a-wd",
    )
    live_plan = LiveWorkplan(
        name="status-change-wp",
        description="A workplan used to test on_status_changed",
        steps=[step],
    )
    trx_path = tmp_path / "trx_transformed.yaml"
    assert serialize(trx_path, live_plan)

    run = WorkplanRun(
        output_path=tmp_path,
        run_id=mock_run_id,
    )
    repo = TrackingRepository()
    await repo.put_workplan_run(run)
    return run


@pytest.mark.asyncio
async def test_on_status_changed_non_terminal_skips_measurement(
    status_change_workplan_run: WorkplanRun,
    mock_run_id: str,
) -> None:
    """A non-terminal status leaves `step_sizes` untouched and never measures."""
    handle = LocalHandle(
        pid="1",
        name="step-a",
        run_id=mock_run_id,
        start_at=datetime.now(),
        status=Status.Running,
    )

    with (
        mock.patch(
            "cstar.orchestration.dag_runner.measure_step_sizes",
            mock.AsyncMock(),
        ) as mock_measure,
        mock.patch(
            "cstar.orchestration.tracking.TrackingRepository.get_workplan_run",
            mock.AsyncMock(return_value=status_change_workplan_run),
        ),
        mock.patch(
            "cstar.orchestration.tracking.TrackingRepository.update_workplan_run",
            mock.AsyncMock(),
        ) as mock_update,
        mock.patch(
            "cstar.orchestration.state.StateRepository.put_sentinel",
            mock.AsyncMock(return_value=Path("/tmp/sentinel")),
        ),
    ):
        await on_status_changed(handle)

    mock_measure.assert_not_awaited()
    assert status_change_workplan_run.step_sizes == {}
    mock_update.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["get_workplan_run", "update_workplan_run"])
async def test_on_status_changed_warns_when_the_run_record_fails(
    status_change_workplan_run: WorkplanRun,
    mock_run_id: str,
    failing: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A run record that cannot be read or written is a warning, not an error
    that stops loading the run's status; the sentinel is still written.
    """
    handle = LocalHandle(
        pid="1",
        name="step-a",
        run_id=mock_run_id,
        start_at=datetime.now(),
        status=Status.Running,
    )
    calls = {
        "get_workplan_run": mock.AsyncMock(return_value=status_change_workplan_run),
        "update_workplan_run": mock.AsyncMock(),
    }
    calls[failing] = mock.AsyncMock(side_effect=OSError(22, "Invalid argument"))

    with (
        mock.patch(
            "cstar.orchestration.tracking.TrackingRepository.get_workplan_run",
            calls["get_workplan_run"],
        ),
        mock.patch(
            "cstar.orchestration.tracking.TrackingRepository.update_workplan_run",
            calls["update_workplan_run"],
        ),
        mock.patch(
            "cstar.orchestration.state.StateRepository.put_sentinel",
            mock.AsyncMock(return_value=Path("/tmp/sentinel")),
        ) as mock_put,
        caplog.at_level("WARNING"),
    ):
        await on_status_changed(handle)

    mock_put.assert_awaited_once()
    assert "Invalid argument" in caplog.text
    assert mock_run_id in caplog.text


@pytest.mark.asyncio
async def test_on_status_changed_terminal_measures_known_step(
    tmp_path: Path,
    status_change_workplan_run: WorkplanRun,
    mock_run_id: str,
) -> None:
    """A terminal status for a real step measures only that step's directory."""
    handle = LocalHandle(
        pid="1",
        name="step-a",
        run_id=mock_run_id,
        start_at=datetime.now(),
        status=Status.Done,
    )

    with (
        mock.patch(
            "cstar.orchestration.dag_runner.measure_step_sizes",
            mock.AsyncMock(),
        ) as mock_measure,
        mock.patch(
            "cstar.orchestration.tracking.TrackingRepository.get_workplan_run",
            mock.AsyncMock(return_value=status_change_workplan_run),
        ),
        mock.patch(
            "cstar.orchestration.tracking.TrackingRepository.update_workplan_run",
            mock.AsyncMock(),
        ) as mock_update,
        mock.patch(
            "cstar.orchestration.state.StateRepository.put_sentinel",
            mock.AsyncMock(return_value=Path("/tmp/sentinel")),
        ),
    ):
        await on_status_changed(handle)

    mock_measure.assert_awaited_once_with(
        status_change_workplan_run,
        {"step-a": tmp_path / "step-a-wd"},
        mock.ANY,
    )
    mock_update.assert_awaited_once()


@pytest.mark.asyncio
async def test_on_status_changed_terminal_unknown_step_skips_measurement(
    status_change_workplan_run: WorkplanRun,
    mock_run_id: str,
) -> None:
    """A terminal status for a step absent from the workplan is not measured,
    but the run is still persisted.
    """
    handle = LocalHandle(
        pid="1",
        name="not-a-real-step",
        run_id=mock_run_id,
        start_at=datetime.now(),
        status=Status.Failed,
    )

    with (
        mock.patch(
            "cstar.orchestration.dag_runner.measure_step_sizes",
            mock.AsyncMock(),
        ) as mock_measure,
        mock.patch(
            "cstar.orchestration.tracking.TrackingRepository.get_workplan_run",
            mock.AsyncMock(return_value=status_change_workplan_run),
        ),
        mock.patch(
            "cstar.orchestration.tracking.TrackingRepository.update_workplan_run",
            mock.AsyncMock(),
        ) as mock_update,
        mock.patch(
            "cstar.orchestration.state.StateRepository.put_sentinel",
            mock.AsyncMock(return_value=Path("/tmp/sentinel")),
        ),
    ):
        await on_status_changed(handle)

    mock_measure.assert_not_awaited()
    mock_update.assert_awaited_once()


@pytest.mark.asyncio
async def test_on_status_changed_terminal_persists_size_and_sentinel(
    tmp_path: Path,
    status_change_workplan_run: WorkplanRun,
    mock_run_id: str,
) -> None:
    """A terminal transition records the step's size and sentinel on the
    persisted record through the locked update, not an in-memory copy.
    """
    handle = LocalHandle(
        pid="1",
        name="step-a",
        run_id=mock_run_id,
        start_at=datetime.now(),
        status=Status.Done,
    )
    sentinel = tmp_path / "step-a.sentinel"

    with (
        mock.patch(
            "cstar.orchestration.tracking.step_disk_usage",
            mock.AsyncMock(return_value=5),
        ),
        mock.patch(
            "cstar.orchestration.state.StateRepository.put_sentinel",
            mock.AsyncMock(return_value=sentinel),
        ),
    ):
        await on_status_changed(handle)

    persisted = await TrackingRepository().get_workplan_run(mock_run_id)
    assert persisted is not None
    assert persisted.step_sizes == {"step-a": 5}
    assert persisted.size_measured_at is not None
    assert sentinel in persisted.sentinels
    # the object handed to the hook was not the one persisted
    assert status_change_workplan_run.sentinels == set()


def _roms_step(
    tmp_path: Path,
    name: str,
    depends_on: list[str] | None = None,
    directives: dict[str, t.Any] | None = None,
) -> Step:
    """Build a `Step` for an application that supports pre-run."""
    bp_path = tmp_path / f"{name}.yaml"
    bp_path.touch()
    return Step(
        name=name,
        application=Application.UCLA_ROMS,
        blueprint=bp_path,
        depends_on=depends_on or [],
        directives=directives or {},
    )


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_prepare_workplan_persists_pre_run_overrides(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify `pre_run` marks every step and, unlike clobber, the marks are
    persisted in the transformed workplan so the run can be recognized later.
    """
    wp_path = wp_templates_dir / "workplan.yaml"

    wp, prepared_path, _, _ = await prepare_workplan(
        wp_path, tmp_path / "output", pre_run=True
    )
    persisted = deserialize(prepared_path, LiveWorkplan)

    assert all(step.pre_run for step in wp.steps)
    assert all(step.pre_run for step in persisted.steps)
    assert persisted.pre_run


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_prepare_workplan_without_pre_run_leaves_steps_unmarked(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify a plain run does not mark any step for pre-run."""
    wp, prepared_path, _, _ = await prepare_workplan(
        wp_templates_dir / "workplan.yaml", tmp_path / "output"
    )

    assert not any(step.pre_run for step in wp.steps)
    assert not deserialize(prepared_path, LiveWorkplan).pre_run


def test_apply_pre_run_overrides_marks_every_step(tmp_path: Path) -> None:
    """Verify every step receives the pre-run override."""
    wp = _make_workplan([_make_step(tmp_path, "A"), _make_step(tmp_path, "B")])

    apply_pre_run_overrides(wp)

    assert [s.workflow_overrides[KEY_PRE_RUN] for s in wp.steps] == [True, True]


def test_check_pre_run_support_reports_every_unsupported_step(
    tmp_path: Path,
) -> None:
    """Verify one error names every step that requests pre-run on an
    application without support, and leaves supported steps out.
    """
    unsupported = [
        Step(
            name=name,
            application=Application.HELLO_WORLD,
            blueprint=_make_step(tmp_path, name).blueprint_path,
            workflow_overrides={KEY_PRE_RUN: True},
        )
        for name in ("Hello A", "Hello B")
    ]
    supported = _roms_step(tmp_path, "Roms")
    supported.workflow_overrides[KEY_PRE_RUN] = True
    unmarked = _make_step(tmp_path, "Unmarked")
    wp = _make_workplan([*unsupported, supported, unmarked])

    with pytest.raises(ValueError, match="do(es)? not support") as exc_info:
        check_pre_run_support(wp)

    message = str(exc_info.value)
    unsupported_part, partial_part = message.split("; ", maxsplit=1)
    assert "'Hello A'" in unsupported_part
    assert "'Hello B'" in unsupported_part
    assert "Roms" not in unsupported_part
    # a partially marked workplan is reported in the same error
    assert "not on every step" in partial_part
    assert "'Roms'" in partial_part
    assert "Unmarked" not in partial_part


def test_check_pre_run_support_rejects_partial_marking(tmp_path: Path) -> None:
    """Verify `pre_run` on some but not all steps is rejected: a pre-run's
    sentinels are local processes, so it is a property of the whole run.
    """
    marked = _roms_step(tmp_path, "Marked")
    marked.workflow_overrides[KEY_PRE_RUN] = True
    wp = _make_workplan([marked, _roms_step(tmp_path, "Unmarked")])

    with pytest.raises(ValueError, match="not on every step"):
        check_pre_run_support(wp)


def test_check_pre_run_support_accepts_all_or_none(tmp_path: Path) -> None:
    """Verify fully marked and fully unmarked workplans pass."""
    steps = [_roms_step(tmp_path, "A"), _roms_step(tmp_path, "B")]
    check_pre_run_support(_make_workplan(steps))
    for step in steps:
        step.workflow_overrides[KEY_PRE_RUN] = True
    check_pre_run_support(_make_workplan(steps))


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_prepare_workplan_pre_run_does_not_report_injected_overrides(
    tmp_path: Path,
) -> None:
    """Verify the support guard looks at the workplan as written, so the
    overrides injected by `pre_run=True` on unsupporting steps are not errors
    (the steps are skipped later instead).
    """
    step = _make_step(tmp_path, "Hello")
    wp_path = tmp_path / "hello.yaml"
    serialize(wp_path, _make_workplan([step]))

    wp, _, _, _ = await prepare_workplan(wp_path, tmp_path / "output", pre_run=True)

    assert wp.steps[0].pre_run
    assert all(isinstance(s, LiveStep) for s in wp.steps)


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_prepare_workplan_rejects_explicit_pre_run_on_unsupported_app(
    tmp_path: Path,
) -> None:
    """Verify a pre-run override written in the workplan on an unsupporting
    application fails fast at schedule time.
    """
    step = Step(
        name="Hello",
        application=Application.HELLO_WORLD,
        blueprint=_make_step(tmp_path, "Hello").blueprint_path,
        workflow_overrides={KEY_PRE_RUN: True},
    )
    wp_path = tmp_path / "hello.yaml"
    serialize(wp_path, _make_workplan([step]))

    with pytest.raises(ValueError, match="'Hello'"):
        await prepare_workplan(wp_path, tmp_path / "output")


def test_prune_unpreparable_steps_keeps_preparable_workplan_intact(
    tmp_path: Path,
) -> None:
    """Verify nothing is dropped, and no copy is made, when every step can be
    prepared.
    """
    wp = _make_workplan(
        [_roms_step(tmp_path, "A"), _roms_step(tmp_path, "B", depends_on=["A"])]
    )

    pruned, reasons = prune_unpreparable_steps(wp)

    assert pruned is wp
    assert reasons == {}


def test_prune_unpreparable_steps_drops_transitively(tmp_path: Path) -> None:
    """Verify unsupported-application, deferred-blueprint and step-referencing
    directive steps are dropped with their reasons, directives that reference
    nothing are kept, and every step downstream of a dropped step is dropped
    too (a `depends_on` edge may carry data the planner cannot see).
    """
    producer = _roms_step(tmp_path, "Producer")
    unsupported = _make_step(tmp_path, "Hello", depends_on=["Producer"])
    deferred = Step.model_validate(
        {
            "name": "Deferred",
            "application": Application.UCLA_ROMS,
            "blueprint": {"from_step": "Producer", "filename": "generated.yaml"},
            "depends_on": ["Producer"],
        }
    )
    nested = _roms_step(
        tmp_path,
        "Nested",
        depends_on=["Producer"],
        directives={"nest-from": {"step": "Producer"}},
    )
    pathed = _roms_step(
        tmp_path,
        "Pathed",
        depends_on=["Producer"],
        directives={"continue-from": {"path": "prior/run"}},
    )
    survivor = _roms_step(
        tmp_path, "Survivor", depends_on=["Producer", "Hello", "Nested", "Pathed"]
    )
    wp = _make_workplan([producer, unsupported, deferred, nested, pathed, survivor])

    pruned, reasons = prune_unpreparable_steps(wp)

    assert reasons == {
        f"application does not support {ARG_PRE_RUN}": ["Hello"],
        "blueprint is produced by another step": ["Deferred"],
        "directive references another step's output": ["Nested"],
        "depends on a skipped step": ["Survivor"],
    }
    assert [s.name for s in pruned.steps] == ["Producer", "Pathed"]
    assert {s.name: s.depends_on for s in pruned.steps} == {
        "Producer": [],
        "Pathed": ["Producer"],
    }
    # the source workplan is untouched
    assert len(wp.steps) == 6
    assert wp.steps[-1].depends_on == ["Producer", "Hello", "Nested", "Pathed"]


def test_prune_unpreparable_steps_reaches_fixpoint_out_of_order(
    tmp_path: Path,
) -> None:
    """Verify a dependent listed before the step it depends on is still
    dropped (steps are not guaranteed to be in dependency order).
    """
    grandchild = _roms_step(tmp_path, "Grandchild", depends_on=["Child"])
    child = _roms_step(tmp_path, "Child", depends_on=["Hello"])
    unsupported = _make_step(tmp_path, "Hello")
    wp = _make_workplan([grandchild, child, unsupported])

    pruned, reasons = prune_unpreparable_steps(wp)

    assert pruned.steps == []
    assert sorted(reasons["depends on a skipped step"]) == ["Child", "Grandchild"]


def test_build_planner_prunes_pre_run_and_logs_one_line_per_reason(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a pre-run workplan is planned without its unpreparable steps and
    each reason is reported once with every affected step.
    """
    wp = _make_workplan(
        [
            _roms_step(tmp_path, "Roms"),
            _make_step(tmp_path, "Hello A"),
            _make_step(tmp_path, "Hello B"),
        ]
    )
    apply_pre_run_overrides(wp)

    with caplog.at_level("INFO"):
        planner = build_planner(wp)

    assert [s.name for s in planner.flatten()] == ["Roms"]
    lines = [r.getMessage() for r in caplog.records if "pre-run mode" in r.getMessage()]
    assert lines == [
        "Skipping 2 step(s) in pre-run mode (application does not support "
        "--pre-run): Hello A, Hello B"
    ]


def test_build_planner_does_not_prune_a_plain_workplan(tmp_path: Path) -> None:
    """Verify only pre-run workplans are pruned."""
    wp = _make_workplan([_roms_step(tmp_path, "Roms"), _make_step(tmp_path, "Hello")])

    planner = build_planner(wp)

    assert {s.name for s in planner.flatten()} == {"Roms", "Hello"}


def test_build_planner_raises_when_nothing_can_be_prepared(tmp_path: Path) -> None:
    """Verify a pre-run workplan with no preparable step is not planned."""
    wp = _make_workplan([_make_step(tmp_path, "Hello")])
    apply_pre_run_overrides(wp)

    with pytest.raises(NoPreparableStepsError, match="nothing was scheduled"):
        build_planner(wp)


@pytest.mark.parametrize(
    ("force_local", "exp_klass"),
    [
        pytest.param(True, LocalLauncher, id="forced-local"),
        pytest.param(False, SlurmLauncher, id="scheduler"),
    ],
)
def test_get_launcher_force_local(
    force_local: bool, exp_klass: type[LocalLauncher | SlurmLauncher]
) -> None:
    """Verify `force_local` selects the local launcher even when the system has
    a scheduler.
    """
    with (
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=mock.MagicMock()),
        ),
        mock.patch.object(SlurmLauncher, "check_preconditions"),
    ):
        launcher = get_launcher(force_local=force_local)

    assert type(launcher) is exp_klass


def _env_workplan(tmp_path: Path, compute_environment: dict[str, t.Any]) -> Workplan:
    """Build a workplan declaring the given compute environment."""
    return Workplan(
        name="env-workplan",
        description="A workplan declaring a compute environment",
        steps=[_make_step(tmp_path, "Hello")],
        compute_environment=compute_environment,
    )


@pytest.mark.parametrize(
    ("launcher", "has_scheduler", "exp_klass"),
    [
        pytest.param("", False, LocalLauncher, id="auto-no-scheduler"),
        pytest.param("", True, SlurmLauncher, id="auto-scheduler"),
        pytest.param("local", False, LocalLauncher, id="local-no-scheduler"),
        pytest.param("local", True, LocalLauncher, id="local-scheduler"),
        pytest.param("slurm", True, SlurmLauncher, id="slurm-scheduler"),
    ],
)
def test_get_launcher_honours_compute_environment(
    tmp_path: Path,
    launcher: str,
    has_scheduler: bool,
    exp_klass: type[LocalLauncher | SlurmLauncher],
) -> None:
    """Verify the workplan's `compute_environment.launcher` selects the launcher."""
    wp = _env_workplan(tmp_path, {"launcher": launcher} if launcher else {})

    with (
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=mock.MagicMock() if has_scheduler else None),
        ),
        mock.patch.object(SlurmLauncher, "check_preconditions"),
    ):
        assert type(get_launcher(wp)) is exp_klass


def test_get_launcher_slurm_without_scheduler_fails(tmp_path: Path) -> None:
    """Verify a workplan requesting slurm on a scheduler-less system is refused."""
    wp = _env_workplan(tmp_path, {"launcher": "slurm"})

    with (
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=None),
        ),
        pytest.raises(CstarExpectationFailed, match="no scheduler"),
    ):
        get_launcher(wp)


def test_get_launcher_checks_preconditions_against_the_workplan(tmp_path: Path) -> None:
    """The launcher's precondition check sees the workplan, so settings it supplies count."""
    wp = _env_workplan(tmp_path, {"launcher": "slurm"})

    with (
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=mock.MagicMock()),
        ),
        mock.patch.object(SlurmLauncher, "check_preconditions") as check,
    ):
        get_launcher(wp)

    check.assert_called_once_with(wp)


def test_get_launcher_local_checks_preconditions(tmp_path: Path) -> None:
    """The local launcher's precondition check sees the workplan before any launch."""
    wp = _env_workplan(tmp_path, {"launcher": "local"})

    with mock.patch.object(LocalLauncher, "check_preconditions") as check:
        get_launcher(wp)

    check.assert_called_once_with(wp)


def test_get_launcher_force_local_wins_over_slurm(tmp_path: Path) -> None:
    """Verify `force_local` beats the workplan's request for slurm."""
    wp = _env_workplan(tmp_path, {"launcher": "slurm"})

    with mock.patch(
        "cstar.system.manager.CStarSystemManager.scheduler",
        mock.PropertyMock(return_value=mock.MagicMock()),
    ):
        assert type(get_launcher(wp, force_local=True)) is LocalLauncher


def test_get_launcher_warns_on_local_with_scheduler(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify asking for local on a system with a scheduler is called out."""
    wp = _env_workplan(tmp_path, {"launcher": "local"})

    with (
        caplog.at_level("WARNING"),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=mock.MagicMock()),
        ),
    ):
        _ = get_launcher(wp)

    assert "local launcher" in caplog.text


@pytest.mark.parametrize(
    ("system", "warns"),
    [
        pytest.param("elsewhere", True, id="mismatch"),
        pytest.param("this-system", False, id="match"),
        pytest.param("", False, id="unset"),
    ],
)
def test_get_launcher_warns_on_system_mismatch(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    system: str,
    warns: bool,
) -> None:
    """Verify a `compute_environment.system` mismatch warns exactly once."""
    wp = _env_workplan(tmp_path, {"system": system} if system else {})

    with (
        caplog.at_level("WARNING"),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.name",
            mock.PropertyMock(return_value="this-system"),
        ),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=None),
        ),
    ):
        _ = get_launcher(wp)

    assert caplog.text.count("was written for system") == (1 if warns else 0)


@pytest.mark.parametrize(
    ("pre_run", "exp_klass"),
    [
        pytest.param(True, LocalLauncher, id="pre-run"),
        pytest.param(False, SlurmLauncher, id="regular"),
    ],
)
def test_get_orchestrator_uses_local_launcher_for_pre_run_workplan(
    tmp_path: Path,
    pre_run: bool,
    exp_klass: type[LocalLauncher | SlurmLauncher],
) -> None:
    """Verify the orchestrator of a pre-run workplan runs locally on a system
    with a scheduler, whether the workplan was just prepared or reloaded.
    """
    wp = _make_workplan([_roms_step(tmp_path, "Roms")])
    if pre_run:
        apply_pre_run_overrides(wp)

    with (
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=mock.MagicMock()),
        ),
        mock.patch.object(SlurmLauncher, "check_preconditions"),
    ):
        orchestrator = get_orchestrator(Planner(wp), root=tmp_path, run_uuid="")

    assert type(orchestrator.launcher) is exp_klass


SUBMITTED = datetime(2026, 10, 21, 9, 30, 15, 500000, tzinfo=UTC)
"""When the launched handles of the tests below were submitted."""


def _launched(
    run_id: str, *, submitted_at: datetime | None = SUBMITTED
) -> ProcessHandle:
    """Build the handle of a step a launcher has just submitted."""
    return ProcessHandle(
        pid="4242",
        name="step-a",
        run_id=run_id,
        launcher_name="slurm",
        status=Status.Submitted,
        submitted_at=submitted_at,
    )


@pytest.mark.asyncio
async def test_on_launched_persists_the_handle_then_records_the_submission(
    tmp_path: Path, mock_run_id: str
) -> None:
    """Verify a launch persists the sentinel as ever and records the launcher, the
    job id and the submission time the handle carries.
    """
    handle = _launched(mock_run_id)
    events: list[str] = []

    async def persisted(_: ProcessHandle) -> None:
        events.append("persisted")

    def recorded(*_: t.Any, **__: t.Any) -> bool:
        events.append("recorded")
        return True

    with (
        mock.patch("cstar.orchestration.dag_runner.on_status_changed", persisted),
        mock.patch(
            "cstar.orchestration.dag_runner.record_submission", side_effect=recorded
        ) as record,
    ):
        await on_launched(handle, root=tmp_path)

    assert events == ["persisted", "recorded"]
    record.assert_called_once_with(
        tmp_path,
        mock_run_id,
        "step-a",
        launcher="slurm",
        job_id="4242",
        submitted_at=SUBMITTED,
    )


@pytest.mark.asyncio
async def test_on_launched_records_nothing_without_a_submission_time(
    tmp_path: Path, mock_run_id: str
) -> None:
    """Verify a handle adopted from a sentinel written before submission times
    were recorded is persisted, and leaves the manifest alone.
    """
    handle = _launched(mock_run_id, submitted_at=None)

    with (
        mock.patch(
            "cstar.orchestration.dag_runner.on_status_changed", mock.AsyncMock()
        ) as persisted,
        mock.patch("cstar.orchestration.dag_runner.record_submission") as record,
    ):
        await on_launched(handle, root=tmp_path)

    persisted.assert_awaited_once_with(handle)
    record.assert_not_called()


@pytest.mark.asyncio
async def test_on_launched_warns_and_goes_on_when_the_manifest_cannot_be_written(
    tmp_path: Path, mock_run_id: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify scheduling is not stopped by a manifest that cannot be updated: the
    warning names the step and the manifest.
    """
    handle = _launched(mock_run_id)

    with (
        mock.patch(
            "cstar.orchestration.dag_runner.on_status_changed", mock.AsyncMock()
        ) as persisted,
        mock.patch(
            "cstar.orchestration.dag_runner.record_submission",
            side_effect=CstarError("manifest is corrupt"),
        ),
        caplog.at_level("WARNING"),
    ):
        await on_launched(handle, root=tmp_path)

    persisted.assert_awaited_once_with(handle)
    assert "step-a" in caplog.text
    assert str(tmp_path / RUN_MANIFEST_NAME) in caplog.text
    assert "manifest is corrupt" in caplog.text


@pytest.mark.asyncio
async def test_on_launched_of_an_adopted_sentinel_writes_nothing(
    tmp_path: Path, mock_run_id: str
) -> None:
    """Verify the handle a later launch adopts from its sentinel, whose submission
    survived the sentinel's YAML, leaves the manifest it was recorded in as it was.
    """
    write_run_manifest(tmp_path, _recorded(mock_run_id, {"step-a": "h"}))
    handle = _launched(mock_run_id)

    await on_launched(handle, root=tmp_path)

    entry = (read_run_manifest(tmp_path) or pytest.fail("no manifest")).step("step-a")
    assert entry is not None
    assert (entry.launcher, entry.job_id) == ("slurm", "4242")
    assert entry.submitted_at == SUBMITTED.replace(microsecond=0)

    adopted = await StateRepository().get_sentinel("step-a", ProcessHandle)
    assert adopted is not None
    assert adopted.submitted_at == SUBMITTED
    path = tmp_path / RUN_MANIFEST_NAME
    before = (path.read_bytes(), path.stat().st_mtime_ns)

    await on_launched(adopted, root=tmp_path)

    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


class _SubmittingLauncher(Launcher[ProcessHandle]):
    """A launcher that submits every step at once, as a job named after it."""

    name = "queue"

    @classmethod
    async def launch(
        cls, step: LiveStep, dependencies: list[ProcessHandle], *, run_uuid: str
    ) -> t.Any:
        await asyncio.sleep(0)  # let the steps of one pass interleave
        handle = ProcessHandle(
            pid=f"job-{step.name}",
            name=step.name,
            run_id=os.environ[ENV_CSTAR_RUNID],
            launcher_name=cls.name,
            status=Status.Running,
            submitted_at=SUBMITTED + timedelta(seconds=len(step.name)),
        )
        return Task(step=step, handle=handle)

    @classmethod
    def handle_klass(cls) -> type[ProcessHandle]:
        return ProcessHandle


@pytest.mark.asyncio
async def test_run_dag_records_every_submission_in_the_manifest(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify scheduling a workplan records the launcher, the job id and the
    submission time of every step: the steps launched in one pass, writing the
    manifest together, each keep their own, and so does a step launched later.
    Every handle records the uuid of the run's manifest.
    """
    monkeypatch.setattr("cstar.orchestration.dag_runner.POLL_DELAYS", (0.0,))
    bp = hello_world_bp_path.as_posix()
    wp = Workplan(
        name="submitted",
        description="A workplan whose steps are submitted.",
        steps=[
            _hello_step(bp, "a"),
            _hello_step(bp, "bb"),
            _hello_step(bp, "ccc", depends_on=["a"]),
        ],
    )
    wp_path = tmp_path / "submitted.yaml"
    assert serialize(wp_path, wp)

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        mock.patch(
            "cstar.orchestration.dag_runner.get_launcher",
            return_value=_SubmittingLauncher,  # launchers work as classes
        ),
    ):
        planner, trx_path, facts, carried = await build_dag(wp_path, mock_run_id)
        wp_run = await run_dag(
            wp_path, trx_path, mock_run_id, planner, step_facts=facts, carried=carried
        )

    manifest = read_run_manifest(wp_run.output_path)
    assert manifest is not None
    submissions = {
        entry.name: (entry.launcher, entry.job_id, entry.submitted_at)
        for entry in manifest.steps
    }
    assert submissions == {
        name: (
            "queue",
            f"job-{name}",
            (SUBMITTED + timedelta(seconds=len(name))).replace(microsecond=0),
        )
        for name in ("a", "bb", "ccc")
    }
    sentinel = await StateRepository().get_sentinel("a", ProcessHandle)
    assert sentinel is not None
    assert sentinel.submitted_at == SUBMITTED + timedelta(seconds=1)
    assert sentinel.run_uuid == manifest.id


@pytest.mark.asyncio
async def test_run_dag_keeps_the_uuid_of_a_run_whose_manifest_was_removed(
    tmp_path: Path,
    mock_run_id: str,
    hello_world_bp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a manifest recreated in a run directory takes the uuid the run's
    sentinels record, so the run still adopts them.
    """
    monkeypatch.setattr("cstar.orchestration.dag_runner.POLL_DELAYS", (0.0,))
    wp = Workplan(
        name="recreated",
        description="A workplan whose manifest is removed.",
        steps=[_hello_step(hello_world_bp_path.as_posix(), "a")],
    )
    wp_path = tmp_path / "recreated.yaml"
    assert serialize(wp_path, wp)

    async def _schedule() -> str:
        planner, trx_path, facts, carried = await build_dag(wp_path, mock_run_id)
        wp_run = await run_dag(
            wp_path, trx_path, mock_run_id, planner, step_facts=facts, carried=carried
        )
        manifest = read_run_manifest(wp_run.output_path)
        assert manifest is not None
        return manifest.id

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        mock.patch(
            "cstar.orchestration.dag_runner.get_launcher",
            return_value=_SubmittingLauncher,
        ),
    ):
        first = await _schedule()
        (StateDirectoryManager.data_dir(mock_run_id) / RUN_MANIFEST_NAME).unlink()
        second = await _schedule()

    assert second == first


@pytest.mark.parametrize(
    ("status", "bucket"),
    [
        pytest.param(Status.Done, "satisfied", id="done"),
        pytest.param(Status.Running, "awaiting", id="running"),
        pytest.param(Status.Failed, "blocking", id="failed"),
    ],
)
def test_get_status_detail_map_external_dependency(
    status: Status,
    bucket: str,
    external_workplan: Workplan,
    external_task: ExternalTaskFactory,
) -> None:
    """Verify an external dependency is reported under the bucket matching its
    status, alongside local dependencies.
    """
    planner = Planner(external_workplan, {EXTERNAL_TOKEN: external_task(status)})
    dag_status = DagStatus({EXTERNAL_TOKEN: status, "first": Status.Done})

    detail_map = get_status_detail_map(planner, dag_status)

    assert list(detail_map) == ["first", "second"]
    assert getattr(detail_map["first"], bucket) == [EXTERNAL_TOKEN]
    assert EXTERNAL_TOKEN in getattr(detail_map["second"], bucket)
    assert "first" in detail_map["second"].satisfied


def test_build_planner_passes_external_tasks(
    external_workplan: Workplan, external_task: ExternalTaskFactory
) -> None:
    """Verify `build_planner` hands the external tasks to the planner."""
    task = external_task(Status.Done)

    planner = build_planner(external_workplan, {EXTERNAL_TOKEN: task})

    assert planner.retrieve(EXTERNAL_TOKEN, "task") is task


@pytest.mark.asyncio
async def test_prepare_workplan_anchors_relative_blueprint_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hello_world_bp_content: str,
) -> None:
    """Verify a relative `blueprint` path is resolved against the workplan's
    directory when the workplan is prepared from another working directory,
    the absolute path is persisted in the transformed workplan, and the step
    command carries it.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for the workplan, blueprint and run outputs
    monkeypatch : pytest.MonkeyPatch
        Fixture used to change the working directory
    hello_world_bp_content : str
        Fixture providing the content of a minimal hello-world blueprint
    """
    plans = tmp_path / "plans"
    plans.mkdir()
    (plans / "hw.yaml").write_text(hello_world_bp_content)
    wp_path = plans / "wp.yaml"
    wp_path.write_text(
        textwrap.dedent("""\
            name: relative-blueprint
            description: A step whose blueprint sits beside the workplan
            state: draft
            steps:
              - name: Say Hello
                application: hello_world
                blueprint: ./hw.yaml
            """)
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    wp, prepared_path, _, _ = await prepare_workplan(wp_path, tmp_path / "output")

    expected = (plans / "hw.yaml").resolve()
    assert Path(wp.steps[0].blueprint_path) == expected

    persisted = deserialize(prepared_path, LiveWorkplan)
    step = persisted.steps[0]
    assert Path(step.blueprint_path) == expected

    command = StepToRunRequestAdapter().adapt(step).command
    assert command[:4] == ["cstar", "blueprint", "run", str(expected)]
    assert command[4:6] == ["--step", step.name]


@pytest.mark.parametrize(
    ("drop_var", "key", "settings_klass"),
    [
        (ENV_CSTAR_SLURM_ACCOUNT, "ACCOUNT", SlurmSettingsBase),
        (ENV_CSTAR_SLURM_QUEUE, "QUEUE", SlurmSettingsBase),
        (ENV_CSTAR_SLURM_ACCOUNT, "ACCOUNT", AnvilEnvSettings),
        (ENV_CSTAR_SLURM_QUEUE, "QUEUE", AnvilEnvSettings),
    ],
)
@pytest.mark.asyncio
async def test_build_and_run_dag_env(
    drop_var: str,
    key: str,
    settings_klass: type[EnvSettingsBase],
    tmp_path: Path,
    bp_templates_dir: Path,
    create_host_workplan: Callable[[str, Path, Path, str], Path],
) -> None:
    """Verify that the DAG runner fails when required environment variables are not passed.

    Parameters
    ----------
    drop_var : str
        The environment variable to be removed from the environment.
    key : str
        A unique string verifying the correct exception was caught.
    tmp_path : Path
        Temporary directory for test outputs
    bp_templates_dir: Path
        Fixture returning the path to the directory containing blueprint template files
    create_host_workplan : Callable
        Fixture that writes a template workplan hosting a blueprint, returning its path
    """
    workplan_name: str = "single_step"

    bp_template_file = "blueprint.yaml"
    bp_template_path = bp_templates_dir / bp_template_file

    working_dir = tmp_path / "original-output-dir"
    working_dir_override = tmp_path / "overridden-output-dir"
    run_id = "my-run"

    mock_process = mock.AsyncMock()

    # mock the sys manager to engage the checks for Slurm env-vars
    mock_sys_manager = mock.Mock()
    mock_sys_manager.scheduler = mock.Mock()
    mock_get_mgr = mock.Mock(return_value=mock_sys_manager)

    mock_env = {
        ENV_CSTAR_STATE_HOME: working_dir_override.as_posix(),
        ENV_CSTAR_SLURM_ACCOUNT: "xyz",
        ENV_CSTAR_SLURM_QUEUE: "wholenode",
        ENV_CSTAR_SLURM_MAX_WALLTIME: "00:5:00",
        ENV_CSTAR_RUNID: run_id,
    }

    # get rid of one required env var.
    del mock_env[drop_var]

    with (
        mock.patch("cstar.orchestration.dag_runner.process_plan", mock_process),
        mock.patch("cstar.orchestration.dag_runner.get_sysmgr", mock_get_mgr),
        mock.patch.dict(os.environ, mock_env, clear=True),
        mock.patch(
            "cstar.system.environment.CStarEnvironment.settings_klass",
            settings_klass,
        ),
        pytest.raises(CstarExpectationFailed) as ex,
    ):
        generated_wp_path = create_host_workplan(
            workplan_name, bp_template_path, working_dir, run_id
        )

        await build_and_run_dag(generated_wp_path, run_id)

    assert key in str(ex)


@pytest.mark.asyncio
async def test_prepare_template_hosted_dag(
    tmp_path: Path,
    bp_templates_dir: Path,
    create_host_workplan: Callable[[str, Path, Path, str], Path],
) -> None:
    """Verify that a template-hosted DAG is transformed by the DAG runner.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    bp_templates_dir: Path
        Fixture returning the path to the directory containing blueprint template files
    create_host_workplan : Callable
        Fixture that writes a template workplan hosting a blueprint, returning its path
    """
    workplan_name: str = "single_step"

    bp_template_file = "blueprint.yaml"
    bp_template_path = bp_templates_dir / bp_template_file

    working_dir = tmp_path / "original-output-dir"
    run_id = "my-run"

    mock_env = {
        ENV_CSTAR_SLURM_ACCOUNT: "xyz",
        ENV_CSTAR_SLURM_QUEUE: "wholenode",
        ENV_CSTAR_SLURM_MAX_WALLTIME: "00:5:00",
        ENV_FF_ORCH_TRX_TIMESPLIT: FLAG_ON,
        ENV_FF_ORCH_TRX_TIMESPLIT_FREQ: SplitFrequency.Monthly.value,
    }

    def _raise_ex(*args, **kwargs) -> None:
        msg = "on-purpose-fail"
        raise RuntimeError(msg)

    with (
        mock.patch("cstar.orchestration.orchestration.Planner.__init__", _raise_ex),
        mock.patch.dict(os.environ, mock_env),
    ):
        generated_wp_path = create_host_workplan(
            workplan_name, bp_template_path, working_dir, run_id
        )

        configure_environment(working_dir, run_id)
        run_id = get_run_id()
        check_environment()
        wp, wp_path, _, _ = await prepare_workplan(
            generated_wp_path, StateDirectoryManager.data_dir(run_id)
        )

    wp = deserialize(wp_path, LiveWorkplan)
    steps = list(wp.steps)

    # default blueprint is 366 days; confirm 366 steps after tranform is applied.
    num_timeslices = 12
    assert len(steps) == num_timeslices

    paths: set[Path] = set()
    for step in steps:
        bp = deserialize(step.blueprint_path, UclaRomsBlueprint)
        paths.add(bp.effective_working_dir)

        assert bp.effective_working_dir.exists()

    # confirm that every step has a unique output path
    assert len(steps) == num_timeslices
