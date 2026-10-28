"""Tests for the run handles the run-level commands accept.

A handle is a run-id, `run://<uuid>` or a run directory. These runs are known
only by their manifests: tracking holds no record of them.
"""

import os
import shlex
import shutil
import typing as t
from pathlib import Path

import pytest
from click.testing import Result
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_RUNID
from cstar.cli.run import app as app_run
from cstar.execution.file_system import StateDirectoryManager
from cstar.orchestration.index import run_uri
from cstar.tests.unit_tests.orchestration.manifest_runs import BuiltRun

EXIT_BAD_ARGS: t.Final[int] = 2
STEP: t.Final[str] = "Spin Up"
"""The step of a built run that has a log."""


def invoke(*args: str) -> Result:
    """Run a `cstar run` command against the tracking of the test's data home."""
    return CliRunner().invoke(app_run, list(args), color=False)


@pytest.mark.parametrize("by", ["uuid", "directory"])
def test_path_prints_the_directory_of_a_run_and_of_a_step(
    local_run: t.Callable[..., BuiltRun], by: str
) -> None:
    """Verify `path` accepts a uuid, and a directory, for the run and for a step."""
    built = local_run("lonely")
    handle = run_uri(built.uuid) if by == "uuid" else str(built.root)

    run = invoke("path", handle)
    step = invoke("path", handle, STEP)

    assert run.exit_code == 0, run.stderr
    assert run.stdout == f"{built.root}\n"
    assert step.exit_code == 0, step.stderr
    assert step.stdout == f"{built.step_dir(STEP)}\n"


def test_cd_names_the_directory_of_a_run_found_by_its_uuid(
    local_run: t.Callable[..., BuiltRun],
) -> None:
    """Verify `cd` explains the shell function, naming the directory of the step."""
    built = local_run("lonely")

    result = invoke("cd", run_uri(built.uuid), STEP)

    assert result.exit_code == 1
    assert f"cd {shlex.quote(str(built.step_dir(STEP)))}" in result.stderr


def test_a_run_in_the_project_area_is_found_by_its_uuid(
    project_run: t.Callable[..., BuiltRun],
) -> None:
    """Verify the project's runs area is looked in."""
    built = project_run("shared-run")

    result = invoke("path", run_uri(built.uuid))

    assert result.exit_code == 0, result.stderr
    assert result.stdout == f"{built.root}\n"


def test_a_run_copied_to_the_project_area_has_its_steps_where_it_is(
    local_run: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify the step directories of a run that moved are the ones in the copy,
    not where the run was first made.
    """
    built = local_run("travelled")
    copy = project_runs / "travelled"
    shutil.copytree(built.root, copy, symlinks=True)
    shutil.rmtree(built.root)

    result = invoke("path", run_uri(built.uuid), STEP)

    assert result.exit_code == 0, result.stderr
    assert result.stdout == f"{copy / 'tasks' / 'spin-up'}\n"


def test_log_prints_the_log_of_a_run_found_by_its_uuid(
    local_run: t.Callable[..., BuiltRun], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify `log` accepts a uuid, and exports the run-id of the run it found."""
    built = local_run("lonely")
    monkeypatch.setenv(ENV_CSTAR_RUNID, "stale")

    result = invoke("log", run_uri(built.uuid), STEP)

    assert result.exit_code == 0, result.stderr
    assert result.stdout == f"{STEP} message"
    assert os.environ[ENV_CSTAR_RUNID] == built.run_id


@pytest.mark.parametrize(
    ("where", "by"),
    [
        ("local", "uuid"),
        ("local", "directory"),
        ("local", "run-id"),
        ("project", "uuid"),
        ("project", "directory"),
        ("project", "run-id"),
    ],
)
def test_gather_writes_into_no_run_that_is_not_tracked_here(
    local_run: t.Callable[..., BuiltRun],
    project_run: t.Callable[..., BuiltRun],
    where: str,
    by: str,
) -> None:
    """Verify `gather` refuses a run known only by its manifest, however it is
    named, and writes nothing: neither into the run nor into the data home.
    """
    built = (local_run if where == "local" else project_run)("shared-run")
    output = built.step_dir(STEP) / "output"
    output.mkdir()
    (output / "only.nc").write_text("data")
    handle = {
        "uuid": run_uri(built.uuid),
        "directory": str(built.root),
        "run-id": built.run_id,
    }[by]

    result = invoke("gather", handle)

    assert result.exit_code == EXIT_BAD_ARGS
    assert not (built.root / "gathered_output").exists()
    if where == "project":
        assert not StateDirectoryManager.data_dir(built.run_id).exists()


@pytest.mark.parametrize("verb", ["log"])
def test_a_standalone_blueprint_run_has_no_workplan(
    build_run: t.Callable[..., BuiltRun],
    flatten_cli_output: t.Callable[[str], str],
    verb: str,
) -> None:
    """Verify the commands that read a workplan refuse a run that has none."""
    root = StateDirectoryManager.blueprint_runs_dir() / "hello_world" / "solo"
    built = build_run(root, "solo", kind="blueprint", steps=("solo",))
    extra = ["solo"] if verb == "log" else []

    result = invoke(verb, run_uri(built.uuid), *extra)

    assert result.exit_code == EXIT_BAD_ARGS
    assert "is a standalone blueprint run" in flatten_cli_output(result.stderr)


@pytest.mark.parametrize("verb", ["path", "log"])
def test_a_run_id_is_still_slugified(
    local_run: t.Callable[..., BuiltRun], verb: str
) -> None:
    """Verify a run-id spelled as a title finds the run of its slug."""
    built = local_run("lonely-run")
    extra = {"path": [], "log": [STEP]}[verb]

    result = invoke(verb, "Lonely Run", *extra)

    assert result.exit_code == 0, result.stderr
    if verb == "path":
        assert result.stdout == f"{built.root}\n"


@pytest.mark.parametrize("verb", ["path", "cd", "log", "gather"])
def test_a_workplan_file_is_not_a_run(
    tmp_path: Path, flatten_cli_output: t.Callable[[str], str], verb: str
) -> None:
    """Verify a workplan file is sent to `cstar workplan check`."""
    result = invoke(verb, str(tmp_path / "plan.yaml"))

    assert result.exit_code == EXIT_BAD_ARGS
    assert "cstar workplan check" in flatten_cli_output(result.stderr)


def test_a_directory_with_no_manifest_is_not_a_run(
    tmp_path: Path, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a directory the user names must hold a run manifest."""
    (tmp_path / "plain").mkdir()

    result = invoke("path", str(tmp_path / "plain"))

    assert result.exit_code == EXIT_BAD_ARGS
    assert "holds no run manifest" in flatten_cli_output(result.stderr)


def test_a_run_id_that_names_two_runs_is_ambiguous(
    local_run: t.Callable[..., BuiltRun],
    project_run: t.Callable[..., BuiltRun],
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify the runs a run-id names are listed, with how to say which."""
    mine = local_run("twin")
    theirs = project_run("twin")

    result = invoke("path", "twin")

    assert result.exit_code == EXIT_BAD_ARGS
    text = flatten_cli_output(result.stderr)
    assert run_uri(mine.uuid) in text
    assert run_uri(theirs.uuid) in text
    assert "run://<uuid>" in text


@pytest.mark.parametrize("by", ["uuid", "directory"])
def test_status_reads_only_runs_tracked_here(
    local_run: t.Callable[..., BuiltRun],
    squash_cli_output: t.Callable[[str], str],
    by: str,
) -> None:
    """Verify `status` refuses a uuid and a directory, and names `show` for them."""
    built = local_run("lonely")
    handle = run_uri(built.uuid) if by == "uuid" else str(built.root)

    result = invoke("status", handle)

    assert result.exit_code == EXIT_BAD_ARGS
    text = squash_cli_output(result.stderr)
    assert squash_cli_output(f"use `cstar run show {handle}` to read that run") in text
    assert squash_cli_output("a run tracked on this machine") in text


def test_status_does_not_find_a_run_only_a_manifest_names(
    local_run: t.Callable[..., BuiltRun], flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a run-id is still looked up in tracking alone for `status`."""
    built = local_run("lonely")

    result = invoke("status", built.run_id)

    assert result.exit_code == EXIT_BAD_ARGS
    assert "Unable to locate run with unknown run-id" in flatten_cli_output(
        result.stderr
    )


def test_show_names_the_directory_as_it_was_typed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a run-id that is no run, and is a directory here under another
    spelling than its slug, gets the hint for the directory as it was typed.
    """
    (tmp_path / "Spin Up").mkdir()
    monkeypatch.chdir(tmp_path)

    result = invoke("show", "Spin Up")

    assert result.exit_code == EXIT_BAD_ARGS
    text = flatten_cli_output(result.stderr)
    assert "To show the directory ./Spin Up, pass it as a path." in text
    assert "./spin-up" not in text
