import os
import random
from collections.abc import AsyncGenerator
from datetime import datetime
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
from cstar.entrypoint.utils import ARG_CLOBBER
from cstar.execution.file_system import (
    DirectoryManager,
    JobFileSystemManager,
    StateDirectoryManager,
)
from cstar.orchestration.dag_runner import (
    _ignore_ambient_clobber_env,
    apply_clobber_overrides,
    check_clobber_dependents,
    check_clobber_targets,
    get_status_detail_map,
    load_run_state,
    prepare_workplan,
)
from cstar.orchestration.launch.local import LocalHandle, LocalLauncher
from cstar.orchestration.models import (
    KEY_CLOBBER,
    Application,
    BlueprintState,
    Step,
    Workplan,
    WorkplanState,
)
from cstar.orchestration.orchestration import LiveWorkplan, Planner, Status
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.state import StateRepository
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun


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
) -> Step:
    """Build a minimal `Step` for use in `_apply_clobber_overrides` tests."""
    bp_path = tmp_path / f"{name}.yaml"
    bp_path.touch()
    return Step(
        name=name,
        application=Application.HELLO_WORLD,
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
    run_id = "clobber-persist-run"

    _, prepared_path = await prepare_workplan(
        wp_path, output_dir, run_id, clobber_steps=["Prepare"]
    )

    persisted = deserialize(prepared_path, LiveWorkplan)
    by_name = {step.name: step for step in persisted.steps}

    assert by_name["Prepare"].workflow_overrides[KEY_CLOBBER] is True
    assert not by_name["Ensemble X"].workflow_overrides.get(KEY_CLOBBER, False)
