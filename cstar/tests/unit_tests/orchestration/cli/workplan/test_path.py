import shlex
import shutil
import typing as t
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.cli.workplan.path import COMMAND_CD, COMMAND_PATH, app
from cstar.orchestration.models import Step
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.serialization import serialize
from cstar.orchestration.tracking import WorkplanRun

STEP_NAMES: t.Final[tuple[str, ...]] = ("Spin Up", "run")
LOOKUP: t.Final[str] = "cstar.cli.workplan.shared.TrackingRepository.get_workplan_run"


class PreparedRun(t.NamedTuple):
    """A run record whose run and step directories exist on disk."""

    run: WorkplanRun
    step_dirs: dict[str, Path]


@pytest.fixture
def prepared(tmp_path: Path, hello_world_bp_path: Path) -> PreparedRun:
    """A run record, its serialized transformed workplan, and the directories
    for the run and each of its steps.
    """
    output = tmp_path / "run output"
    step_dirs = {name: output / "tasks" / name.replace(" ", "_") for name in STEP_NAMES}
    for directory in step_dirs.values():
        directory.mkdir(parents=True)

    steps = [
        LiveStep.from_step(
            Step(
                name=name,
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
            ),
            update={"working_dir": directory},
        )
        for name, directory in step_dirs.items()
    ]
    trx_path = tmp_path / "trx.yaml"
    assert serialize(
        trx_path, LiveWorkplan(name="paths", description="step paths", steps=steps)
    )
    run = WorkplanRun(
        workplan_path=trx_path,
        trx_workplan_path=trx_path,
        output_path=output,
        run_id="my-run",
    )
    return PreparedRun(run, step_dirs)


def invoke(run: WorkplanRun | None, *args: str) -> t.Any:
    """Run the command group against a repository that holds only `run`."""
    with mock.patch(LOOKUP, mock.AsyncMock(return_value=run)):
        return CliRunner().invoke(app, list(args), color=False)


def target(prepared: PreparedRun, with_step: bool) -> tuple[Path, list[str]]:
    """The directory and command arguments selecting the run or its first step."""
    run_id = prepared.run.run_id
    if with_step:
        return prepared.step_dirs[STEP_NAMES[0]], [run_id, STEP_NAMES[0]]
    return prepared.run.output_path, [run_id]


def test_path_prints_only_the_run_directory(prepared: PreparedRun) -> None:
    """Verify the run's recorded output path is the only thing on stdout."""
    result = invoke(prepared.run, COMMAND_PATH, prepared.run.run_id)

    assert result.exit_code == 0, result.stderr
    assert result.stdout == f"{prepared.run.output_path}\n"
    assert result.stderr == ""


@pytest.mark.parametrize(
    ("typed", "step_name"),
    [("Spin Up", "Spin Up"), ("spin-up", "Spin Up"), ("run", "run")],
)
def test_path_with_step_prints_the_step_directory(
    prepared: PreparedRun, typed: str, step_name: str
) -> None:
    """Verify a step is found by name or slug and its working directory is printed."""
    expected = prepared.step_dirs[step_name]

    result = invoke(prepared.run, COMMAND_PATH, prepared.run.run_id, typed)

    assert result.exit_code == 0, result.stderr
    assert result.stdout == f"{expected}\n"
    assert result.stderr == ""


def test_path_without_step_ignores_an_unloadable_workplan(
    prepared: PreparedRun,
) -> None:
    """Verify the run directory is found from the record alone, so runs whose
    transformed workplan no longer deserializes can still be located.
    """
    prepared.run.trx_workplan_path.write_text("not: [a, workplan")

    result = invoke(prepared.run, COMMAND_PATH, prepared.run.run_id)

    assert result.exit_code == 0, result.stderr
    assert result.stdout == f"{prepared.run.output_path}\n"


def test_path_with_step_requires_a_loadable_workplan(
    prepared: PreparedRun, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a step cannot be resolved without the transformed workplan."""
    prepared.run.trx_workplan_path.write_text("not: [a, workplan")

    result = invoke(prepared.run, COMMAND_PATH, prepared.run.run_id, STEP_NAMES[0])

    assert result.exit_code != 0
    assert result.stdout == ""
    assert "Unable to deserialize workplan" in flatten_cli_output(result.stderr)


def test_path_unknown_step(
    prepared: PreparedRun, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify an unknown step is rejected with the valid step names."""
    result = invoke(prepared.run, COMMAND_PATH, prepared.run.run_id, "step-DNE")

    stderr = flatten_cli_output(result.stderr)
    assert result.exit_code != 0
    assert result.stdout == ""
    assert "unknown step" in stderr
    assert "step-DNE" in stderr
    assert all(repr(name) in stderr for name in STEP_NAMES)


def test_path_unknown_run() -> None:
    """Verify an unrecorded run-id is rejected."""
    result = invoke(None, COMMAND_PATH, "no-such-run")

    assert result.exit_code != 0
    assert result.stdout == ""
    assert "unknown run-id" in result.stderr


@pytest.mark.parametrize("run_id", ["", "  ", "!!!"])
def test_path_invalid_run_id(run_id: str) -> None:
    """Verify a run-id that is empty once slugified is rejected, not looked up."""
    result = CliRunner().invoke(app, [COMMAND_PATH, run_id], color=False)

    assert result.exit_code != 0
    assert result.stdout == ""
    assert "run-id" in result.stderr


@pytest.mark.parametrize("with_step", [False, True])
def test_path_missing_directory_fails(prepared: PreparedRun, with_step: bool) -> None:
    """Verify a directory that does not exist is reported, never printed."""
    directory, args = target(prepared, with_step)
    shutil.rmtree(directory)

    result = invoke(prepared.run, COMMAND_PATH, *args)

    assert result.exit_code == 1
    assert result.stdout == ""
    assert str(directory) in result.stderr
    assert prepared.run.run_id in result.stderr


@pytest.mark.parametrize("with_step", [False, True])
def test_cd_explains_the_shell_function(prepared: PreparedRun, with_step: bool) -> None:
    """Verify the binary cannot change directory: it exits non-zero with guidance
    naming the directory and `cstar env shell-init`.
    """
    expected, args = target(prepared, with_step)

    result = invoke(prepared.run, COMMAND_CD, *args)

    assert result.exit_code == 1
    assert result.stdout == ""
    assert f"cd {shlex.quote(str(expected))}" in result.stderr
    assert "shell-init" in result.stderr


def test_cd_missing_directory_fails(prepared: PreparedRun) -> None:
    """Verify no `cd` suggestion is made for a directory that does not exist."""
    missing = prepared.run.output_path / "gone"
    run = prepared.run.model_copy(update={"output_path": missing})

    result = invoke(run, COMMAND_CD, run.run_id)

    assert result.exit_code == 1
    assert result.stdout == ""
    assert str(missing) in result.stderr
    assert "shell-init" not in result.stderr
