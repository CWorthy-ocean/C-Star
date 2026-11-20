"""Skip or fail the integration tiers that need a tool the machine may lack.

A missing tool skips a module on a developer's machine, where the tier is opt-in. Under
CI (``CI`` set) it fails instead: a runner image that loses ``nccopy`` or a compiler
would otherwise turn the job green with no ROMS test run, since pytest exits 0 when
everything is skipped. This is the rule the ``integration_test_data`` fixture in
``conftest.py`` follows for a failed download.

A module declares its tools with ``pytestmark = pytest.mark.requires_tools("nccopy")``;
the ``pytest_runtest_setup`` hook in ``conftest.py`` applies `require_tools` before any
fixture is set up, so a runner without the tool does not first download the test data.
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
