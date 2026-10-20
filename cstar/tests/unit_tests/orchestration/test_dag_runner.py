import os
import random
import re
import shutil
import textwrap
import typing as t
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest

from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.base.env import (
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_RUNID,
    FLAG_OFF,
    FLAG_ON,
)
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.utils import slugify
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_PRE_RUN, ARG_RUN_ID
from cstar.execution.file_system import (
    DirectoryManager,
    JobFileSystemManager,
    StateDirectoryManager,
)
from cstar.orchestration.adapter import StepToRunRequestAdapter
from cstar.orchestration.dag_runner import (
    DagStatus,
    ExecutiveRunSummary,
    NoPreparableStepsError,
    _ignore_ambient_clobber_env,
    apply_clobber_overrides,
    apply_pre_run_overrides,
    apply_resume_overrides,
    build_dag,
    build_planner,
    check_clobber_dependents,
    check_clobber_targets,
    check_pre_run_support,
    get_launcher,
    get_orchestrator,
    get_status_detail_map,
    load_run_state,
    on_status_changed,
    original_workplan_backup,
    prepare_workplan,
    prune_unpreparable_steps,
    refresh_run_manifest,
    refuse_edited_done_steps,
    run_dag,
)
from cstar.orchestration.launch.local import LocalHandle, LocalLauncher
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    Attempt,
    AttemptStatus,
    RunManifest,
    StepEntry,
    StepFacts,
    StepRecord,
    describe_steps,
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
    Workplan,
    WorkplanState,
)
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan, Planner, Status
from cstar.orchestration.serialization import (
    PersistenceMode,
    deserialize,
    read_raw,
    serialize,
)
from cstar.orchestration.state import StateRepository
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.orchestration.transforms import WorkplanTransformer
from cstar.tests.unit_tests.orchestration.conftest import (
    EXTERNAL_TOKEN,
    ExternalTaskFactory,
)


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
    wp_run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=wp_path,
        output_path=tmp_path / "mock-output",
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


@pytest.mark.parametrize("value", [FLAG_ON, "boo", "  "])
def test_ignore_ambient_clobber_env_pops_and_warns(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    value: str,
) -> None:
    """An exported CSTAR_CLOBBER_WORKING_DIR is scrubbed (so it cannot leak
    into every step subprocess) and the user is pointed at `--clobber`.
    """
    monkeypatch.setenv(ENV_CSTAR_CLOBBER_WORKING_DIR, value)

    with caplog.at_level("WARNING"):
        _ignore_ambient_clobber_env()

    assert ENV_CSTAR_CLOBBER_WORKING_DIR not in os.environ
    assert len(caplog.records) == 1
    assert ENV_CSTAR_CLOBBER_WORKING_DIR in caplog.text
    assert ARG_CLOBBER in caplog.text


def test_ignore_ambient_clobber_env_silent_when_off(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An explicit "off" value is scrubbed without a warning (it would have
    had no effect on the steps).
    """
    monkeypatch.setenv(ENV_CSTAR_CLOBBER_WORKING_DIR, FLAG_OFF)

    with caplog.at_level("WARNING"):
        _ignore_ambient_clobber_env()

    assert ENV_CSTAR_CLOBBER_WORKING_DIR not in os.environ
    assert not caplog.records


def test_ignore_ambient_clobber_env_noop_when_unset(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Absence of the variable is a silent no-op."""
    monkeypatch.delenv(ENV_CSTAR_CLOBBER_WORKING_DIR, raising=False)

    with caplog.at_level("WARNING"):
        _ignore_ambient_clobber_env()

    assert ENV_CSTAR_CLOBBER_WORKING_DIR not in os.environ
    assert not caplog.records


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
        tmp_path, "Unresumable Step", application=Application.ROMS_MARBL
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

    _, prepared_path, _ = await prepare_workplan(
        wp_path, output_dir, clobber_steps=["Prepare"]
    )

    persisted = deserialize(prepared_path, LiveWorkplan)
    by_name = {step.name: step for step in persisted.steps}

    assert by_name["Prepare"].workflow_overrides[KEY_CLOBBER] is True
    assert not by_name["Ensemble X"].workflow_overrides.get(KEY_CLOBBER, False)


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
async def test_prepare_workplan_persists_original_backup(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify `prepare_workplan` writes an untouched copy of the source
    workplan that a later `--resume` with a path can verify against.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    output_dir = tmp_path / "output"

    _, trx_path, _ = await prepare_workplan(
        wp_path, output_dir, clobber_steps=["Prepare"]
    )

    backup_path = original_workplan_backup(wp_path, output_dir)
    assert backup_path.exists()

    backup = deserialize(backup_path, Workplan, mode=PersistenceMode.yaml)
    assert backup == deserialize(wp_path, Workplan)
    # the transformed copy keeps the name, which is the run-id, as written
    assert read_raw(trx_path)["name"] == backup.name


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
async def test_prepare_workplan_rotates_the_files_of_prior_attempts(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify each attempt moves the prior attempt's original, transformed and
    variable files aside as `<name>.<i>` instead of overwriting them, and that
    a failed preparation leaves the prior attempt's files where they are.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    output_dir = tmp_path / "output"
    first_vars = {"var1": "a", "var2": "b"}

    _, prepared_path, _ = await prepare_workplan(wp_path, output_dir, first_vars)
    persisted = (
        original_workplan_backup(wp_path, output_dir),
        prepared_path,
        WorkplanTransformer.derived_path(
            wp_path, output_dir, suffix="", extension=".vars"
        ),
    )
    first_text = {path: path.read_text() for path in persisted}
    assert all(path.exists() for path in persisted)
    assert not list(output_dir.glob("*.1"))

    with pytest.raises(ValueError, match="Unknown clobber"):
        await prepare_workplan(wp_path, output_dir, clobber_steps=["no-such-step"])
    assert not list(output_dir.glob("*.1"))
    assert {path: path.read_text() for path in persisted} == first_text

    second_vars = {"var1": "c", "var2": "d"}
    await prepare_workplan(wp_path, output_dir, second_vars, ["Prepare"])

    for path in persisted:
        assert path.with_name(f"{path.name}.1").read_text() == first_text[path]
    assert persisted[1].read_text() != first_text[persisted[1]]
    assert persisted[2].read_text() != first_text[persisted[2]]

    second_text = {path: path.read_text() for path in persisted}
    await prepare_workplan(wp_path, output_dir, first_vars)

    for path in persisted:
        assert path.with_name(f"{path.name}.1").read_text() == first_text[path]
        assert path.with_name(f"{path.name}.2").read_text() == second_text[path]

    # an attempt without variables moves the last attempt's variables aside too,
    # rather than leaving them beside the new originals
    third_vars_text = persisted[2].read_text()
    await prepare_workplan(wp_path, output_dir)

    assert not persisted[2].exists()
    assert persisted[2].with_name(f"{persisted[2].name}.3").read_text() == (
        third_vars_text
    )
    for path in persisted[:2]:
        assert path.exists()
        assert path.with_name(f"{path.name}.3").exists()


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
    materialized file, while the original backup keeps the `inline` token.
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

    prepared, prepared_path, _ = await prepare_workplan(wp_path, output_dir)

    # `build_dag` reports orphans from the steps' working directories
    assert all(isinstance(s, LiveStep) for s in prepared.steps)

    persisted = deserialize(prepared_path, LiveWorkplan)
    consumer = next(s for s in persisted.steps if s.name == "consumer")

    assert not consumer.is_inline
    assert Path(consumer.blueprint_path) == consumer.fsm.run_dir / "blueprint.yaml"
    assert Path(consumer.blueprint_path).exists()
    assert "apply-overrides" not in consumer.directives

    backup = deserialize(
        original_workplan_backup(wp_path, output_dir),
        Workplan,
        mode=PersistenceMode.yaml,
    )
    assert next(s for s in backup.steps if s.name == "consumer").is_inline


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

    prepared, prepared_path, _ = await prepare_workplan(wp_path, output_dir)

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
    tmp_path: Path, run_id: str, blueprint: Path, *, with_facts: bool = True
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
        planner, trx_path, facts = await build_dag(wp_path, run_id)
        wp_run = await run_dag(
            wp_path,
            trx_path,
            run_id,
            planner,
            step_facts=facts if with_facts else None,
        )
    return wp_path, wp_run


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

    # a re-entry brings no facts: the recorded entries stay
    await _schedule(tmp_path, mock_run_id, hello_world_bp_path, with_facts=False)
    again = read_run_manifest(wp_run.output_path)
    assert again is not None
    assert again.id == manifest.id
    assert len(again.attempts) == 2
    entry = again.step("hello")
    assert entry is not None
    assert entry.definition_hash == facts["hello"].definition_hash


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
    assert await TrackingRepository().get_workplan_run(mock_run_id) is not None


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
    run_id: str, hashes: t.Mapping[str, str], *, now: datetime | None = None
) -> RunManifest:
    """Build a `RunManifest` recording the given definition hash for each step."""
    now = now or datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
    return RunManifest(
        run_id=run_id,
        name=run_id,
        kind="workplan",
        created_at=now,
        attempts=[Attempt(start_at=now)],
        steps=[
            StepEntry(
                name=name,
                application="hello_world",
                working_dir=f"tasks/{slugify(name)}",
                definition_hash=definition_hash,
            )
            for name, definition_hash in hashes.items()
        ],
    )


def _stale(facts: t.Mapping[str, StepFacts]) -> dict[str, str]:
    """Return a recorded hash for each step that differs from its current one."""
    return {name: f"stale-{f.definition_hash}" for name, f in facts.items()}


def test_refuse_edited_done_steps_refuses_an_edited_done_step(
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
    recorded = {n: f.definition_hash for n, f in facts.items()} | {"B": "old"}
    status = DagStatus({"A": Status.Done, "B": Status.Done})

    with pytest.raises(CstarExpectationFailed) as exc_info:
        refuse_edited_done_steps(wp, _recorded("run", recorded), status, facts, [])

    message = str(exc_info.value)
    assert "'B'" in message
    assert "'A'" not in message
    assert f"{ARG_CLOBBER} B" in message
    assert f"{ARG_CLOBBER} A" not in message
    assert ARG_RUN_ID in message


def test_refuse_edited_done_steps_names_every_edited_step(
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
        refuse_edited_done_steps(wp, _recorded("run", _stale(facts)), status, facts, [])

    message = str(exc_info.value)
    assert "'A'" in message
    assert "'B'" in message
    assert message.count(ARG_CLOBBER) == 2
    assert f"{ARG_CLOBBER} A" in message
    assert f"{ARG_CLOBBER} B" in message


@pytest.mark.parametrize("status", [{"A": Status.Failed}, {}])
def test_refuse_edited_done_steps_ignores_edited_steps_that_are_not_done(
    tmp_path: Path, hello_world_bp_path: Path, status: dict[str, Status]
) -> None:
    """Verify an edited step that failed or was never submitted is not refused."""
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)

    refuse_edited_done_steps(
        wp, _recorded("run", _stale(facts)), DagStatus(status), facts, []
    )


def test_refuse_edited_done_steps_accepts_an_unchanged_done_step(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify a Done step whose definition is unchanged is not refused."""
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)
    recorded = {n: f.definition_hash for n, f in facts.items()}

    refuse_edited_done_steps(
        wp, _recorded("run", recorded), DagStatus({"A": Status.Done}), facts, []
    )


@pytest.mark.parametrize("by", ["name", "safe_name"])
def test_refuse_edited_done_steps_honors_clobber(
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
        refuse_edited_done_steps(wp, manifest, status, facts, ["other"])

    refuse_edited_done_steps(wp, manifest, status, facts, [f" {getattr(step, by)} "])


def test_refuse_edited_done_steps_names_the_consumer_of_an_edited_producer(
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
        refuse_edited_done_steps(wp, _recorded("run", recorded), status, facts, [])

    message = str(exc_info.value)
    assert "'A'" in message
    assert "'B'" in message


def test_refuse_edited_done_steps_skips_an_unrecorded_definition(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify a step recorded without a definition hash is not checked."""
    wp = _guard_workplan(_hello_step(hello_world_bp_path.as_posix(), "A"))
    facts = describe_steps(wp, tmp_path)

    refuse_edited_done_steps(
        wp, _recorded("run", {"A": ""}), DagStatus({"A": Status.Done}), facts, []
    )


def test_refuse_edited_done_steps_skips_a_step_new_to_the_run(
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

    refuse_edited_done_steps(wp, _recorded("run", recorded), status, facts, [])


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
    write_run_manifest(output_dir, manifest)

    with (
        mock.patch("cstar.orchestration.dag_runner.check_environment"),
        pytest.raises(CstarExpectationFailed, match="Upgrade cstar-ocean") as exc_info,
    ):
        await build_dag(wp_path, mock_run_id)

    assert "remove the file" not in str(exc_info.value)
    assert ARG_RUN_ID in str(exc_info.value)
    assert not list(output_dir.glob("*_transformed.yaml*"))


@pytest.mark.asyncio
async def test_build_dag_refuses_an_edited_done_step_before_writing_anything(
    tmp_path: Path, mock_run_id: str, hello_world_bp_path: Path
) -> None:
    """Verify an edited Done step refuses the re-entry before the transformed
    workplan is written, and that clobbering the step lets it through.
    """
    wp_path, output_dir = _build_guarded(tmp_path, mock_run_id, hello_world_bp_path)
    write_run_manifest(output_dir, _recorded(mock_run_id, {"hello": "stale"}))
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

        await build_dag(wp_path, mock_run_id, clobber_steps=["hello"])

    assert list(output_dir.glob("*_transformed.yaml*"))


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
    """Persist a `WorkplanRun` whose `trx_workplan_path` holds a real `LiveWorkplan`.

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
    trx_path = tmp_path / "trx.yaml"
    assert serialize(trx_path, live_plan)

    run = WorkplanRun(
        workplan_path=tmp_path / "wp.yaml",
        trx_workplan_path=trx_path,
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
        application=Application.ROMS_MARBL,
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

    wp, prepared_path, _ = await prepare_workplan(
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
    wp, prepared_path, _ = await prepare_workplan(
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

    wp, _, _ = await prepare_workplan(wp_path, tmp_path / "output", pre_run=True)

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
            "application": Application.ROMS_MARBL,
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
        orchestrator = get_orchestrator(Planner(wp))

    assert type(orchestrator.launcher) is exp_klass


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

    wp, prepared_path, _ = await prepare_workplan(wp_path, tmp_path / "output")

    expected = (plans / "hw.yaml").resolve()
    assert Path(wp.steps[0].blueprint_path) == expected

    persisted = deserialize(prepared_path, LiveWorkplan)
    step = persisted.steps[0]
    assert Path(step.blueprint_path) == expected

    command = StepToRunRequestAdapter().adapt(step).command
    assert command[:4] == ["cstar", "blueprint", "run", str(expected)]
    assert command[4:6] == ["--step", step.name]
