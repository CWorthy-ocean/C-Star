"""How integration tests invoke the real ``cstar`` CLI as a subprocess.

A shim pins the ``cstar`` executable to this checkout, so scheduler-spawned step
proxies (which call ``cstar`` from ``PATH``) run the code under test.
"""

import os
import stat
import subprocess
import sys
from pathlib import Path

import cstar

REPO_ROOT = Path(cstar.__file__).resolve().parents[1]


def make_shim(directory: Path) -> Path:
    """Write a ``cstar`` executable that runs this checkout's CLI.

    Parameters
    ----------
    directory : Path
        The directory to write the shim into; created if missing.

    Returns
    -------
    Path
        The shim's path.
    """
    directory.mkdir(parents=True, exist_ok=True)
    shim = directory / "cstar"
    shim.write_text(
        "#!/bin/sh\n"
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
