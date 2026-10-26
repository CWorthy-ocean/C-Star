"""Tests for `cstar blueprint submit`."""

import os
import typing as t
from collections.abc import Callable
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_RUNID
from cstar.cli.blueprint import app as blueprint_app
from cstar.cli.blueprint.submit import app
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_PRE_RUN, ARG_RESUME, ARG_RUN_ID
from cstar.execution.file_system import StateDirectoryManager
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.models import KEY_PRE_RUN, Step, Workplan
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan, Status
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.tracking import WorkplanRun
from cstar.tests.unit_tests.orchestration.manifest_runs import write_identity

SLUG = "say-hello-to-my-little-friend"
"""The slug of the name of the hello-world blueprint fixture."""
STEP = "hello_world"
"""The name of the only step of a composed workplan: its application's."""

BUILD_AND_RUN_DAG = "cstar.cli.workplan.run.build_and_run_dag"
RUN_LOOKUP = "cstar.cli.workplan.run.TrackingRepository.get_workplan_run"


def _place_run(output_path: Path, run_id: str, wp_path: Path, trx: Workplan) -> None:
    """Make `output_path` the run directory of a run of `wp_path`.

    Its manifest names `wp_path` and its transformed workplan is `trx`.
    """
    write_identity(output_path, run_id, workplan=wp_path)
    serialize(output_path / f"{wp_path.stem}_transformed.yaml", trx)


async def fake_build_and_run_dag(
    wp_path: Path,
    run_id: str,
    user_variables: dict[str, str] | None = None,
    clobber_steps: list[str] | None = None,
    pre_run: bool = False,
) -> WorkplanRun:
    """Stand in for scheduling: the run directory of the workplan is all it leaves."""
    output_path = StateDirectoryManager.data_dir(run_id)
    wp = deserialize(wp_path, Workplan)
    _place_run(output_path, run_id, wp_path, wp)
    return WorkplanRun(output_path=output_path, run_id=run_id)


@pytest.fixture
def build_and_run_dag() -> t.Generator[mock.AsyncMock]:
    """Replace the scheduling of a workplan with one that only places its run."""
    with mock.patch(BUILD_AND_RUN_DAG, wraps=fake_build_and_run_dag) as mocked:
        yield mocked


def _submit(*args: str | Path) -> t.Any:
    """Run `cstar blueprint submit` with `args`."""
    return CliRunner().invoke(app, [str(a) for a in args], color=False)


def test_default_run_id_is_the_blueprint_slug(
    hello_world_bp_path: Path, build_and_run_dag: mock.AsyncMock
) -> None:
    """Verify the run is named for the blueprint, runs the workplan composed
    under the state directory, and the derived run-id is checked against any
    earlier run of that name.
    """
    with mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=None)) as lookup:
        result = _submit(hello_world_bp_path)

    assert result.exit_code == 0, result.output
    build_and_run_dag.assert_awaited_once()
    lookup.assert_awaited_once_with(SLUG)
    assert os.environ[ENV_CSTAR_RUNID] == SLUG

    wp_path, run_id = build_and_run_dag.call_args.args
    assert run_id == SLUG
    assert wp_path.is_relative_to(StateDirectoryManager.composed_workplans_dir())
    wp = deserialize(wp_path, Workplan)
    assert wp.name == SLUG
    assert [(s.name, s.application) for s in wp.steps] == [(STEP, STEP)]
    assert Path(wp.steps[0].blueprint_path) == hello_world_bp_path.resolve()
    assert build_and_run_dag.call_args.kwargs["user_variables"] == {}
    assert "run scheduling has completed" in result.output


def test_run_id_option_is_honoured(
    hello_world_bp_path: Path, build_and_run_dag: mock.AsyncMock
) -> None:
    """Verify an explicit run-id is normalized and used as given, without the
    check of a derived run-id against the run it names.
    """
    with mock.patch(RUN_LOOKUP, mock.AsyncMock()) as lookup:
        result = _submit(hello_world_bp_path, ARG_RUN_ID, "  My Run ")

    assert result.exit_code == 0, result.output
    assert build_and_run_dag.call_args.args[1] == "my-run"
    assert os.environ[ENV_CSTAR_RUNID] == "my-run"
    lookup.assert_not_awaited()
    # the workplan is still named for the blueprint
    assert deserialize(build_and_run_dag.call_args.args[0], Workplan).name == SLUG


@pytest.mark.parametrize(
    ("flags", "expected"),
    [([], []), ([ARG_CLOBBER], [STEP])],
)
def test_clobber_selects_the_only_step(
    hello_world_bp_path: Path,
    build_and_run_dag: mock.AsyncMock,
    flags: list[str],
    expected: list[str],
) -> None:
    """Verify `--clobber`, a flag here, clobbers the workplan's one step."""
    result = _submit(hello_world_bp_path, *flags)

    assert result.exit_code == 0, result.output
    assert build_and_run_dag.call_args.kwargs["clobber_steps"] == expected


@pytest.mark.parametrize("pre_run", [True, False])
def test_pre_run_is_forwarded_and_names_this_command(
    hello_world_bp_path: Path,
    build_and_run_dag: mock.AsyncMock,
    squash_cli_output: Callable[[str], str],
    pre_run: bool,
) -> None:
    """Verify `--pre-run` reaches the scheduler, and only a pre-run ends by naming
    this command, with the user's blueprint, as the way to launch.
    """
    result = _submit(hello_world_bp_path, *([ARG_PRE_RUN] if pre_run else []))

    assert result.exit_code == 0, result.output
    assert build_and_run_dag.call_args.kwargs["pre_run"] is pre_run
    output = squash_cli_output(result.output)
    assert (squash_cli_output("Pre-run scheduled.") in output) is pre_run
    follow_up = f"`cstar blueprint submit {hello_world_bp_path} {ARG_RUN_ID} {SLUG}`"
    assert (squash_cli_output(follow_up) in output) is pre_run
    assert squash_cli_output("cstar workplan submit") not in output


def test_conflicting_flags_are_all_reported(
    hello_world_bp_path: Path,
    build_and_run_dag: mock.AsyncMock,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify every conflict of `--resume` is reported in one error, before
    anything is composed or scheduled.
    """
    with mock.patch("cstar.cli.workplan.run.run_dag", mock.AsyncMock()) as run_dag:
        result = _submit(hello_world_bp_path, ARG_RESUME, ARG_CLOBBER, ARG_PRE_RUN)

    assert result.exit_code == 2
    output = flatten_cli_output(result.output)
    assert f"{ARG_RESUME} cannot be combined with {ARG_CLOBBER}" in output
    assert f"{ARG_RESUME} cannot be combined with {ARG_PRE_RUN}" in output
    build_and_run_dag.assert_not_awaited()
    run_dag.assert_not_awaited()
    assert not StateDirectoryManager.composed_workplans_dir().exists()


def test_a_workplan_is_pointed_to_workplan_submit(
    tmp_path: Path,
    hello_world_bp_path: Path,
    build_and_run_dag: mock.AsyncMock,
    squash_cli_output: Callable[[str], str],
) -> None:
    """Verify a workplan is refused with the command that submits it."""
    wp_path = tmp_path / "workplan.yaml"
    serialize(
        wp_path,
        Workplan(
            name="a-workplan",
            description="not a blueprint",
            steps=[
                Step(
                    name=STEP,
                    application="hello_world",
                    blueprint=hello_world_bp_path,
                )
            ],
        ),
    )

    result = _submit(wp_path)

    assert result.exit_code == 2
    assert squash_cli_output(f"cstar workplan submit {wp_path}") in squash_cli_output(
        result.output
    )
    build_and_run_dag.assert_not_awaited()
    assert not StateDirectoryManager.composed_workplans_dir().exists()


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (None, "Blueprint file not found"),
        ("some: [unrelated, content]\n", "invalid"),
    ],
)
def test_a_path_that_is_no_blueprint_is_refused(
    tmp_path: Path,
    build_and_run_dag: mock.AsyncMock,
    flatten_cli_output: Callable[[str], str],
    content: str | None,
    message: str,
) -> None:
    """Verify a missing file and a file that is not a blueprint are refused."""
    path = tmp_path / "not-a-blueprint.yaml"
    if content is not None:
        path.write_text(content)

    result = _submit(path)

    assert result.exit_code == 2
    assert message in flatten_cli_output(result.output)
    build_and_run_dag.assert_not_awaited()
    assert not StateDirectoryManager.composed_workplans_dir().exists()


def test_a_submission_leaves_the_blueprint_directory_as_it_was(
    readonly_blueprint: Path, build_and_run_dag: mock.AsyncMock
) -> None:
    """Verify submitting a blueprint in a read-only location works, and adds
    nothing to its directory and changes nothing in the blueprint.
    """
    catalog = readonly_blueprint.parent
    content = readonly_blueprint.read_bytes()
    files_before = {p.name for p in catalog.iterdir()}

    result = _submit(readonly_blueprint)

    assert result.exit_code == 0, result.output
    assert {p.name for p in catalog.iterdir()} == files_before
    assert readonly_blueprint.read_bytes() == content
    assert build_and_run_dag.call_args.args[0].is_file()


def test_submitting_again_composes_the_same_workplan(
    hello_world_bp_path: Path, build_and_run_dag: mock.AsyncMock
) -> None:
    """Verify a blueprint submitted twice runs the same workplan file, so the run
    it started is re-entered rather than refused as another workplan's.
    """
    first = _submit(hello_world_bp_path)
    second = _submit(hello_world_bp_path)

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    paths = [call.args[0] for call in build_and_run_dag.call_args_list]
    assert len(paths) == 2
    assert paths[0] == paths[1]


def _recorded_live_run(
    output_path: Path, run_id: str, blueprint: Path, *, pre_run: bool = False
) -> WorkplanRun:
    """The record of a run of a single-step workplan of `blueprint`, whose
    transformed workplan is in `output_path`.
    """
    wp_path = output_path / "workplan.yaml"
    wp = Workplan(
        name=run_id,
        description="a recorded run",
        steps=[Step(name=STEP, application="hello_world", blueprint=blueprint)],
    )
    serialize(wp_path, wp)
    update = {"workflow_overrides": {KEY_PRE_RUN: True}} if pre_run else None
    live = LiveWorkplan(
        **wp.model_dump(exclude={"steps"}),
        steps=[LiveStep.from_step(step, update=update) for step in wp.steps],
    )
    _place_run(output_path, run_id, wp_path, live)
    return WorkplanRun(output_path=output_path, run_id=run_id)


def test_resume_reloads_the_run_and_writes_nothing(
    tmp_path: Path,
    readonly_blueprint: Path,
    build_and_run_dag: mock.AsyncMock,
) -> None:
    """Verify `--resume` re-enters the run of the blueprint's slug as recorded:
    no workplan is composed, nothing is written beside the blueprint, and the
    failed step is resumed in place.
    """
    catalog = readonly_blueprint.parent
    content = readonly_blueprint.read_bytes()
    files_before = {p.name for p in catalog.iterdir()}
    record = _recorded_live_run(tmp_path / "run", SLUG, readonly_blueprint)
    loaded = DagStatus({STEP: Status.Failed})

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=record),
        ) as reload,
        mock.patch(
            "cstar.cli.workplan.run.run_dag", mock.AsyncMock(return_value=record)
        ) as run_dag,
        mock.patch(
            "cstar.cli.workplan.run.load_run_state",
            mock.AsyncMock(return_value=loaded),
        ),
        mock.patch("cstar.cli.workplan.run.refresh_run_manifest"),
        mock.patch(
            "cstar.cli.workplan.run.apply_resume_overrides",
            mock.AsyncMock(return_value=[]),
        ) as apply_resume,
    ):
        result = _submit(readonly_blueprint, ARG_RESUME)

    assert result.exit_code == 0, result.output
    reload.assert_awaited_once_with(SLUG)
    apply_resume.assert_awaited_once()
    run_dag.assert_awaited_once()
    build_and_run_dag.assert_not_awaited()
    assert not StateDirectoryManager.composed_workplans_dir().exists()
    assert {p.name for p in catalog.iterdir()} == files_before
    assert readonly_blueprint.read_bytes() == content


def test_resume_of_a_pre_run_names_this_command(
    tmp_path: Path,
    hello_world_bp_path: Path,
    flatten_cli_output: Callable[[str], str],
    squash_cli_output: Callable[[str], str],
) -> None:
    """Verify the refusal to resume a pre-run names how to retry it with this
    command and the user's blueprint, not with `cstar workplan submit`.
    """
    record = _recorded_live_run(
        tmp_path / "run", SLUG, hello_world_bp_path, pre_run=True
    )

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=record),
        ),
        mock.patch("cstar.cli.workplan.run.run_dag", mock.AsyncMock()) as run_dag,
    ):
        result = _submit(hello_world_bp_path, ARG_RESUME)

    assert result.exit_code == 2
    assert "is a pre-run" in flatten_cli_output(result.output)
    retry = f"`cstar blueprint submit {hello_world_bp_path} {ARG_PRE_RUN}`"
    assert squash_cli_output(retry) in squash_cli_output(result.output)
    assert "cstar workplan submit" not in flatten_cli_output(result.output)
    run_dag.assert_not_awaited()


def test_blueprint_help_lists_submit() -> None:
    """Verify the command is registered under `cstar blueprint`."""
    result = CliRunner().invoke(blueprint_app, ["--help"], color=False)

    assert result.exit_code == 0, result.output
    assert "submit" in result.output
