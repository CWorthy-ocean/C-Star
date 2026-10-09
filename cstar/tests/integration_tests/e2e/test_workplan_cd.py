"""Tier 2: finding a run's directory with ``cstar workplan path`` and ``cstar workplan cd``.

A one-step ``hello_world`` workplan runs through the real CLI, so no ROMS is compiled
and the run takes seconds. ``workplan path`` must print the recorded directory and
nothing else on stdout; ``workplan cd`` only changes directory through the shell
function from ``cstar env shell-init``, so that is exercised in real shells.
"""

import asyncio
import os
import shlex
import shutil
import subprocess
import typing as t
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

import pytest
import yaml

from cstar.base.env import (
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_LOG_LEVEL,
    ENV_CSTAR_STATE_HOME,
)
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    kill_run,
    make_cli_env,
    run_cstar,
    run_log_tails,
    step_root,
    wait_for_terminal,
)

RUN_ID = "workplan-cd-e2e"
STEP = "hello"
WALLTIME = "00:05:00"
RUN_TIMEOUT = 180.0
POLL_INTERVAL = 0.5
SHELLS: t.Final[dict[str, tuple[str, ...]]] = {
    "bash": ("--norc", "--noprofile", "-c"),
    "zsh": ("-f", "-c"),
}
"""Per shell, the arguments that run a command string without reading rc files."""


@dataclass(frozen=True)
class FinishedRun:
    """A completed workplan run and the environment that reaches its records."""

    env: dict[str, str]
    data_home: Path
    record: WorkplanRun
    """The recorded run, read back from the state home."""

    @property
    def run_dir(self) -> str:
        """The recorded run directory."""
        return str(self.record.output_path)

    @property
    def step_dir(self) -> str:
        """The working directory the transformed workplan gives the step."""
        workplan = deserialize(self.record.trx_workplan_path, LiveWorkplan)
        return str(next(s for s in workplan.steps if s.name == STEP).working_dir)


def write_workplan(root: Path) -> Path:
    """Write a ``hello_world`` blueprint and a workplan with one step running it."""
    blueprint = root / "hello.yaml"
    blueprint.write_text(
        yaml.safe_dump(
            {
                "name": "hello cd",
                "description": "says hello",
                "application": "hello_world",
                "state": "draft",
                "target": "cd",
                "schema_version": "1.0.0",
            },
            sort_keys=False,
        )
    )
    workplan = root / "workplan.yaml"
    workplan.write_text(
        yaml.safe_dump(
            {
                "name": "cd",
                "description": "one step to find",
                "steps": [
                    {
                        "name": STEP,
                        "application": "hello_world",
                        "blueprint": str(blueprint),
                        "compute_overrides": {"local": {"max_walltime": WALLTIME}},
                    }
                ],
            },
            sort_keys=False,
        )
    )
    return workplan


def read_record(env: dict[str, str]) -> WorkplanRun:
    """Read the run record the CLI wrote, through the homes of its environment."""
    homes = {
        k: env[k]
        for k in (ENV_CSTAR_CONFIG_HOME, ENV_CSTAR_DATA_HOME, ENV_CSTAR_STATE_HOME)
    }
    with mock.patch.dict(os.environ, homes):
        record = asyncio.run(TrackingRepository().get_workplan_run(RUN_ID))

    assert record is not None, f"no record of run {RUN_ID!r} was written"
    return record


@pytest.fixture(scope="module")
def finished_run(
    tmp_path_factory: pytest.TempPathFactory, cstar_shim: Path
) -> Iterator[FinishedRun]:
    """Run the workplan to completion and read back its record."""
    root = tmp_path_factory.mktemp("workplan_cd")
    workplan = write_workplan(root)
    env = make_cli_env(root, cstar_shim)
    env.pop(ENV_CSTAR_LOG_LEVEL, None)
    state_home, data_home = root / "state", root / "data"

    proc = run_cstar(env, "workplan", "run", "--run-id", RUN_ID, str(workplan))
    try:
        statuses = wait_for_terminal(
            state_home, RUN_ID, [STEP], timeout=RUN_TIMEOUT, poll_interval=POLL_INTERVAL
        )
        if proc.returncode != 0 or statuses != {STEP: DONE}:
            pytest.fail(
                f"workplan run exited {proc.returncode} with {statuses}\n"
                f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}\n"
                f"{run_log_tails(data_home, RUN_ID)}"
            )
        yield FinishedRun(env, data_home, read_record(env))
    finally:
        kill_run(state_home, RUN_ID, [STEP])


def run_shell(
    shell: str,
    finished_run: FinishedRun,
    script: str,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a script in a shell with the CLI on its PATH and debug logging exported."""
    env = {**finished_run.env, ENV_CSTAR_LOG_LEVEL: "DEBUG", **(extra_env or {})}
    return subprocess.run(
        [shutil.which(shell) or shell, *SHELLS[shell], script],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


@pytest.fixture(params=SHELLS)
def shell(request: pytest.FixtureRequest) -> str:
    """Each shell the function supports, skipped when it is not installed."""
    name = str(request.param)
    if shutil.which(name) is None:
        pytest.skip(f"{name} is not installed")
    return name


def test_run_directory_is_the_recorded_output_path(finished_run: FinishedRun) -> None:
    """The run directory is the only thing ``workplan path`` prints."""
    assert Path(finished_run.run_dir).is_dir()
    assert os.path.realpath(finished_run.run_dir) == os.path.realpath(
        finished_run.data_home / "workplan_runs" / RUN_ID
    )

    proc = run_cstar(finished_run.env, "workplan", "path", RUN_ID)

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == f"{finished_run.run_dir}\n"


def test_step_directory_is_the_workplans_working_dir(finished_run: FinishedRun) -> None:
    """With a step, ``workplan path`` prints the step's working directory."""
    assert os.path.realpath(finished_run.step_dir) == os.path.realpath(
        step_root(finished_run.data_home, RUN_ID, STEP)
    )

    proc = run_cstar(finished_run.env, "workplan", "path", RUN_ID, STEP)

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == f"{finished_run.step_dir}\n"


def test_path_accepts_the_run_id_in_any_case(finished_run: FinishedRun) -> None:
    """The run-id is looked up as the repository stores it, not as typed."""
    proc = run_cstar(finished_run.env, "workplan", "path", RUN_ID.upper())

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == f"{finished_run.run_dir}\n"


def test_path_unknown_run_prints_nothing_on_stdout(finished_run: FinishedRun) -> None:
    """A failed lookup leaves stdout empty so ``cd "$(...)"`` cannot misfire."""
    proc = run_cstar(finished_run.env, "workplan", "path", "no-such-run")

    assert proc.returncode != 0
    assert proc.stdout == ""
    assert "no-such-run" in proc.stderr


@pytest.mark.parametrize("alias", ["workplan", "wp"])
@pytest.mark.parametrize("with_step", [False, True])
def test_shell_function_changes_directory(
    finished_run: FinishedRun, shell: str, alias: str, with_step: bool
) -> None:
    """The generated function ends the shell in the run (or step) directory,
    even with debug logging exported, which would otherwise pollute the path.
    """
    expected = finished_run.step_dir if with_step else finished_run.run_dir
    target = f"{RUN_ID} {STEP}" if with_step else RUN_ID
    script = (
        f'eval "$(cstar env shell-init {shell})"\n'
        f'cd /\ncstar {alias} cd {target}\necho "rc=$?"\npwd -P\n'
    )

    proc = run_shell(shell, finished_run, script)

    assert proc.stdout.splitlines() == ["rc=0", os.path.realpath(expected)], proc.stderr


def test_shell_function_stays_put_when_the_run_is_unknown(
    finished_run: FinishedRun, shell: str
) -> None:
    """A failed lookup returns its status without changing directory."""
    script = (
        f'eval "$(cstar env shell-init {shell})"\n'
        'cd /\ncstar wp cd no-such-run\necho "rc=$?"\npwd -P\n'
    )

    proc = run_shell(shell, finished_run, script)

    rc, cwd = proc.stdout.splitlines()
    assert rc != "rc=0", proc.stderr
    assert cwd == os.path.realpath("/")
    assert "no-such-run" in proc.stderr


@pytest.mark.parametrize("with_step", [False, True])
def test_installed_shell_function_changes_directory(
    finished_run: FinishedRun, shell: str, tmp_path: Path, with_step: bool
) -> None:
    """`cstar env shell-init --install` sets a shell up from nothing: a fresh shell
    reading the rc file it wrote ends in the run (or step) directory.

    Every location the install writes is under ``tmp_path``, never the real home.
    """
    home, zdotdir = tmp_path / "home", tmp_path / "zdotdir"
    home.mkdir()
    sandbox = {
        "HOME": str(home),
        "ZDOTDIR": str(zdotdir),
        ENV_CSTAR_CONFIG_HOME: str(tmp_path / "config"),
        "XDG_CONFIG_HOME": str(tmp_path / "xdg"),
    }
    rc = zdotdir / ".zshrc" if shell == "zsh" else home / ".bashrc"

    install = run_cstar(
        {**finished_run.env, **sandbox}, "env", "shell-init", shell, "--install"
    )

    assert install.returncode == 0, install.stderr
    assert rc.is_file()
    assert (tmp_path / "config" / "shell" / f"cstar.{shell}").is_file()

    expected = finished_run.step_dir if with_step else finished_run.run_dir
    target = f"{RUN_ID} {STEP}" if with_step else RUN_ID
    script = f'cd /\n. "{rc}"\ncstar wp cd {target}\necho "rc=$?"\npwd -P\n'

    proc = run_shell(shell, finished_run, script, sandbox)

    assert proc.stdout.splitlines() == ["rc=0", os.path.realpath(expected)], proc.stderr


@pytest.mark.parametrize("with_step", [False, True])
def test_cd_without_the_shell_function_explains_itself(
    finished_run: FinishedRun, with_step: bool
) -> None:
    """The bare executable cannot change the shell's directory; it says how to."""
    args = [RUN_ID, STEP] if with_step else [RUN_ID]
    expected = finished_run.step_dir if with_step else finished_run.run_dir

    proc = run_cstar(finished_run.env, "workplan", "cd", *args)

    assert proc.returncode == 1
    assert proc.stdout == ""
    assert f"cd {shlex.quote(expected)}" in proc.stderr
    assert "shell-init" in proc.stderr
