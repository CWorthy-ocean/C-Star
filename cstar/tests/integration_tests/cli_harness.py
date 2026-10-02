"""How integration tests invoke the real ``cstar`` CLI as a subprocess.

A shim pins the ``cstar`` executable to this checkout, so scheduler-spawned step
proxies (which call ``cstar`` from ``PATH``) run the code under test.
"""

import os
import re
import signal
import stat
import subprocess
import sys
import time
import typing as t
from pathlib import Path

import yaml

import cstar
from cstar.base.utils import slugify

if t.TYPE_CHECKING:
    from collections.abc import Sequence

REPO_ROOT = Path(cstar.__file__).resolve().parents[1]
DONE, CANCELLED, FAILED = 5, 6, 7
"""Sentinel ``status:`` values of the terminal states of a step."""
TERMINAL = {DONE, CANCELLED, FAILED}


def make_shim(directory: Path, startup_delay: float = 0.0) -> Path:
    """Write a ``cstar`` executable that runs this checkout's CLI.

    Parameters
    ----------
    directory : Path
        The directory to write the shim into; created if missing.
    startup_delay : float
        Seconds every invocation sleeps before starting the CLI, for tests that need a
        step to outlast a short walltime regardless of how fast the CLI imports.

    Returns
    -------
    Path
        The shim's path.
    """
    directory.mkdir(parents=True, exist_ok=True)
    shim = directory / "cstar"
    delay = f"sleep {startup_delay}\n" if startup_delay else ""
    shim.write_text(
        "#!/bin/sh\n"
        f"{delay}"
        f'export PYTHONPATH="{REPO_ROOT}"\n'
        f'exec "{sys.executable}" -m cstar.cli.cli "$@"\n'
    )
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return shim


def make_cli_env(root: Path, shim: Path) -> dict[str, str]:
    """Build the environment for ``cstar`` subprocesses, rooted at ``root``.

    Parameters
    ----------
    root : Path
        The directory under which the isolated C-Star data, state, cache and config
        homes are placed.
    shim : Path
        The shim written by `make_shim`.

    Returns
    -------
    dict[str, str]
        The parent environment with isolated C-Star directories, the shim and this
        interpreter's ``bin`` first on ``PATH``, and ``CONDA_PREFIX`` defaulted to
        ``sys.prefix`` (the system env files expand ``${CONDA_PREFIX}``).
    """
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in {"CSTAR_RUNID", "CSTAR_CLOBBER_WORKING_DIR"}
    }
    env_bin = str(Path(sys.prefix) / "bin")
    env.setdefault("CONDA_PREFIX", sys.prefix)
    env.update(
        CSTAR_DATA_HOME=str(root / "data"),
        CSTAR_STATE_HOME=str(root / "state"),
        CSTAR_CACHE_HOME=str(root / "cache"),
        CSTAR_CONFIG_HOME=str(root / "config"),
        CSTAR_ORCH_LOCAL_DELAY="1",
        PATH=os.pathsep.join([str(shim.parent), env_bin, os.environ["PATH"]]),
        PYTHONPATH=str(REPO_ROOT),
    )
    return env


def run_cstar(
    env: dict[str, str], *args: str, timeout: float = 900
) -> subprocess.CompletedProcess[str]:
    """Run ``cstar`` through the shim and capture its output."""
    return subprocess.run(
        ["cstar", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def sentinel_path(state_home: Path, run_id: str, step: str) -> Path:
    """Path of the sentinel file of ``step`` in ``run_id``."""
    return state_home / "run_state" / run_id / f"{slugify(step)}.sentinel.yaml"


def step_root(data_home: Path, run_id: str, step: str) -> Path:
    """The directory ``step`` of ``run_id`` runs in, which holds its ``logs/``."""
    return data_home / run_id / "tasks" / slugify(step)


def read_status(path: Path) -> int | None:
    """Read the integer ``status:`` line of a sentinel, or ``None`` if unreadable."""
    try:
        match = re.search(r"^status:\s*(\d+)", path.read_text(), re.MULTILINE)
    except OSError:
        return None
    return int(match.group(1)) if match else None


def wait_for_terminal(
    state_home: Path,
    run_id: str,
    steps: "Sequence[str]",
    *,
    timeout: float,
    poll_interval: float = 5.0,
) -> dict[str, int | None]:
    """Poll the sentinels of ``steps`` until each is terminal, or ``timeout`` seconds pass.

    Parameters
    ----------
    state_home : Path
        The ``CSTAR_STATE_HOME`` of the run.
    run_id : str
        The run whose sentinels are polled.
    steps : Sequence[str]
        The step names to wait for.
    timeout : float
        Seconds to wait before returning the statuses read so far.
    poll_interval : float
        Seconds between polls.

    Returns
    -------
    dict[str, int | None]
        The last status read for each step, keyed by step name.
    """
    deadline = time.monotonic() + timeout
    while True:
        statuses = {
            step: read_status(sentinel_path(state_home, run_id, step)) for step in steps
        }
        if all(s in TERMINAL for s in statuses.values()):
            return statuses
        if time.monotonic() > deadline:
            return statuses
        time.sleep(poll_interval)


def kill_run(state_home: Path, run_id: str, steps: "Sequence[str]") -> None:
    """Best-effort termination of the step proxies whose sentinels are not terminal.

    Terminal steps are skipped because their recorded PIDs are stale and may have
    been recycled.
    """
    for step in steps:
        path = sentinel_path(state_home, run_id, step)
        if read_status(path) in TERMINAL:
            continue
        try:
            pid = yaml.safe_load(path.read_text())["pid"]
            os.kill(int(pid), signal.SIGTERM)
        except (OSError, KeyError, TypeError, ValueError, yaml.YAMLError):
            continue
