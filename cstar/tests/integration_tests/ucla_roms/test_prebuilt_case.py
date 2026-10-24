"""Tier 2b: the prebuilt ``cstar_blueprint_test_case`` run through ``cstar blueprint run``.

The case has ``use_pio`` off, so inputs are partitioned with ``roms_tools`` before the
run and the per-processor outputs are joined with ``ncjoin`` afterwards. Code and inputs
come from GitHub, and ROMS and MARBL are compiled, so the test needs network and takes
minutes.
"""

import re
import shutil
import typing as t
from collections.abc import Callable
from pathlib import Path

import pytest

from cstar.tests.integration_tests.cli_harness import make_cli_env, run_cstar

CASE = "test_case_remote_with_netcdf_datasets"
COMPLETED_MESSAGE = "Blueprint execution completed"
RUN_TIMEOUT = 25 * 60
PARTITIONED_NC = re.compile(r"\.\d+\.nc$")
# joined outputs end in the timestamp; per-processor pieces add a rank after it
PARTITIONED_OUTPUT = re.compile(r"\.\d{14}\.\d+\.nc$")
MISSING_TOOLS = [
    tool
    for tool, found in {
        "mpirun": shutil.which("mpirun"),
        "mpifort or gfortran": shutil.which("mpifort") or shutil.which("gfortran"),
    }.items()
    if not found
]

pytestmark = pytest.mark.skipif(
    bool(MISSING_TOOLS), reason=f"ROMS toolchain missing: {', '.join(MISSING_TOOLS)}"
)


def test_prebuilt_case_runs_with_partitioned_inputs(
    tmp_path: Path,
    cstar_shim: Path,
    modify_template_blueprint: Callable[
        [Path | str, dict[str, str], Path | str], t.Any
    ],
    integration_test_configuration: dict[str, dict[str, str | dict[str, str]]],
) -> None:
    """The run succeeds, inputs are partitioned, and restarts are joined into ``output``."""
    config = integration_test_configuration[CASE]
    working_dir = tmp_path / "cstar_test_simulation"
    blueprint = modify_template_blueprint(
        str(config["template_blueprint_path"]),
        t.cast("dict[str, str]", config["strs_to_replace"]),
        working_dir,
    )

    proc = run_cstar(
        make_cli_env(tmp_path / "cli", cstar_shim),
        "blueprint",
        "run",
        str(blueprint),
        timeout=RUN_TIMEOUT,
    )
    tail = f"stdout:\n{proc.stdout[-3000:]}\nstderr:\n{proc.stderr[-3000:]}"
    assert proc.returncode == 0, f"blueprint run exited {proc.returncode}\n{tail}"
    assert COMPLETED_MESSAGE in proc.stdout, tail

    inputs = sorted((working_dir / "input" / "input_datasets").rglob("*.nc"))
    assert [p for p in inputs if PARTITIONED_NC.search(p.name)], (
        f"no partitioned input files among {[p.name for p in inputs]}"
    )

    joined = sorted((working_dir / "output").glob("*_rst*.nc"))
    assert joined, f"no joined restart in {sorted((working_dir / 'output').glob('*'))}"
    assert [p for p in joined if PARTITIONED_OUTPUT.search(p.name)] == []
