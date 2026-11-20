"""Skip or fail the integration tiers that need a tool the machine may lack.

A missing tool skips a module on a developer's machine, where the tier is opt-in. Under
CI (``CI`` set) it fails instead: a runner image that loses ``nccopy`` or a compiler
would otherwise turn the job green with no ROMS test run, since pytest exits 0 when
everything is skipped. This is the rule the ``integration_test_data`` fixture in
``conftest.py`` follows for a failed download.
"""

import os
import shutil
from collections.abc import Callable

import pytest

from cstar.orchestration.launch.local import find_timeout_exe

TOOLS: dict[str, Callable[[], str | None]] = {
    "nccopy": lambda: shutil.which("nccopy"),
    "mpirun": lambda: shutil.which("mpirun"),
    "mpifort or gfortran": lambda: shutil.which("mpifort") or shutil.which("gfortran"),
    "GNU timeout or gtimeout": find_timeout_exe,
}
"""Each tool a tier can need, by the name a skip or failure message gives, and its finder."""


def require_tools(*names: str) -> None:
    """Skip the test, or fail it under CI, when any of the named tools is missing.

    Parameters
    ----------
    *names : str
        Keys of `TOOLS`.
    """
    missing = [name for name in names if not TOOLS[name]()]
    if not missing:
        return
    reason = f"ROMS toolchain missing: {', '.join(missing)}"
    if os.environ.get("CI"):
        pytest.fail(reason, pytrace=False)
    pytest.skip(reason)


def tools_required(*names: str) -> object:
    """Build the autouse module fixture that applies `require_tools` to a test module.

    Bind it to a module-level name, ``_toolchain = tools_required("nccopy")``, so that
    every test in the module is skipped (failed under CI) when a tool is missing.

    Parameters
    ----------
    *names : str
        Keys of `TOOLS`.

    Returns
    -------
    object
        The pytest fixture.
    """

    @pytest.fixture(scope="module", autouse=True)
    def _require() -> None:
        require_tools(*names)

    return _require
