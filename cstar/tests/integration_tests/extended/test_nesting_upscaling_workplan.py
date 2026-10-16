"""Tier 3: the tiny nesting + upscaling example workplan run end to end through the ``cstar`` CLI.

The workplan is ``examples/nesting_upscaling/tiny/workplan.yaml``: a parent run that
extracts boundary data for a nested child, the child's initial state regridded from the
parent's restart, a child run with an alkalinity release (CDR) that writes the boundary
fluxes of the CDR tracers, the upscaler turning those into a CDR forcing file for the
parent grid, and a parent rerun that reads it. It is eight steps and compiles ROMS with
ParallelIO three times, so a run takes about ten minutes on a 10-core laptop.

As in the e2e tier, ``cstar workplan run`` only schedules: each step is a detached
``sh`` proxy that calls the ``cstar`` executable on ``PATH``, and a shim pins that to
this checkout. The module polls the step sentinels until all eight are terminal.
"""

import re
import shutil
import time
import typing as t
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest
import yaml

from cstar.base.utils import slugify
from cstar.orchestration.launch.local import find_timeout_exe
from cstar.tests.integration_tests.cases import ROMS_REF
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    REPO_ROOT,
    TERMINAL,
    kill_run,
    make_cli_env,
    run_cstar,
    run_log_tails,
    step_root,
    wait_for_terminal,
)

if t.TYPE_CHECKING:
    import xarray as xr

EXAMPLE_DIR = REPO_ROOT / "examples" / "nesting_upscaling" / "tiny"
WORKPLAN = EXAMPLE_DIR / "workplan.yaml"
RUN_ID = "nesting-upscaling-it"
SCHEDULED_MESSAGE = "run scheduling has completed"
FORGE_STEPS = ("forge_parent", "forge_child", "forge_parent_up")
ROMS_STEPS = ("parent", "child", "parent_up")
STEPS = (*FORGE_STEPS, "parent", "nest_ic", "child", "upscale", "parent_up")
ALL_DONE = dict.fromkeys(STEPS, DONE)
POLL_INTERVAL = 5.0
RUN_TIMEOUT = 60 * 60
ROMS_FAILURE_PATTERN = re.compile(
    r"\bERROR \["
    r"|BLOWUP"
    r"|MPI_(?:ABORT|Abort)"
    r"|Ran out of time records"
    r"|First available forcing record is past current time"
    r"|Traceback"
)
"""ROMS failure markers in a step log, which also carries C-Star's own logging.

ucla-roms prints ``ERROR [raised ...] [module/routine]:`` (``ERROR`` then a bracket) and then
calls ``MPI_Abort``; a blow-up prints ``BLOWUP``. C-Star's logger writes its level as
``[ERROR]``, so a transient ``[ERROR] ... An error occurred while refreshing ...git`` (C-Star
carries on after it) does not match.
"""
CDR_TIME_EPOCH = datetime(2000, 1, 1)
"""The upscaler counts ``cdr_time`` in days since this date, whatever the model reference date."""
CDF5_MAGIC = b"CDF\x05"
MISSING_TOOLS = [
    tool
    for tool, found in {
        "nccopy": shutil.which("nccopy"),
        "mpirun": shutil.which("mpirun"),
        "mpifort or gfortran": shutil.which("mpifort") or shutil.which("gfortran"),
        "GNU timeout or gtimeout": find_timeout_exe(),
    }.items()
    if not found
]

pytestmark = pytest.mark.skipif(
    bool(MISSING_TOOLS), reason=f"ROMS toolchain missing: {', '.join(MISSING_TOOLS)}"
)


@dataclass(frozen=True)
class NestingRun:
    """A finished run of the nesting + upscaling workplan and where its artifacts live."""

    env: dict[str, str]
    data_home: Path
    statuses: dict[str, int | None]
    wall_seconds: float

    def root(self, step: str) -> Path:
        """The directory ``step`` ran in; it holds ``output/`` and ``logs/``."""
        return step_root(self.data_home, RUN_ID, step)

    def outputs(self, step: str, pattern: str) -> list[Path]:
        """The files of ``step``'s ``output/`` matching ``pattern``, ordered by name."""
        return sorted((self.root(step) / "output").glob(pattern))

    def log_text(self, step: str) -> str:
        """The stdout log of ``step``."""
        return (self.root(step) / "logs" / f"{slugify(step)}.out").read_text(
            errors="replace"
        )


def write_workplan(path: Path) -> Path:
    """Write a copy of the example workplan that pins the ucla-roms ref under test.

    The weekly workflow sets ``CSTAR_IT_ROMS_REF`` to each ref it covers, so every forge
    step adds ``code.roms.commit`` to its ``blueprint_overrides``: the published
    blueprints, and with them the ROMS the next steps build, are for that ref. The copy
    names its forge blueprints by absolute path because it does not sit next to them.

    This replaces the example's own pin (ucla-roms main after #390), so with
    ``CSTAR_IT_ROMS_REF`` unset a run tests ``PINNED_ROMS_REF`` (0.10.1), where
    ``parent_up`` injects about 1 % less flux than the upscaled file holds. The
    assertions check that the chain runs cleanly, not conservation, so they hold on both.

    Returns
    -------
    Path
        ``path``.
    """
    workplan = yaml.safe_load(WORKPLAN.read_text())
    for step in workplan["steps"]:
        if step["application"] != "forge":
            continue
        step["blueprint"] = str(EXAMPLE_DIR / step["blueprint"])
        step["blueprint_overrides"] = {
            **step.get("blueprint_overrides", {}),
            "code": {"roms": {"commit": ROMS_REF}},
        }
    path.write_text(yaml.safe_dump(workplan, sort_keys=False))
    return path


@pytest.fixture(scope="module")
def nesting_run(
    integration_test_data: dict[str, Path],
    cstar_shim: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[NestingRun]:
    """Schedule the workplan once for the module and wait for all steps to finish.

    Prints the wall time from ``workplan run`` to the last step's sentinel; ``-s``
    shows it.

    Yields
    ------
    NestingRun
        The finished run. Any step proxy still running at teardown is stopped.
    """
    root = tmp_path_factory.mktemp("nesting_upscaling")
    state_home = root / "state"
    try:
        run = _schedule_and_wait(integration_test_data, cstar_shim, root)
    except BaseException:
        kill_run(state_home, RUN_ID, STEPS)
        raise
    print(
        f"\nnesting + upscaling workplan: {run.wall_seconds / 60:.1f} min "
        f"({run.wall_seconds:.0f} s) with ucla-roms {ROMS_REF}"
    )
    yield run
    kill_run(state_home, RUN_ID, STEPS)


def _schedule_and_wait(
    integration_test_data: dict[str, Path], shim: Path, root: Path
) -> NestingRun:
    """Run ``cstar workplan run`` on the example and poll the sentinels to the end.

    Returns
    -------
    NestingRun
        The finished run.
    """
    workplan = write_workplan(root / "workplan.yaml")
    env = make_cli_env(root, shim)
    # pooch keeps every registry file in one cache directory
    example_data = integration_test_data["GLORYS_NA_2012.nc"].parent

    started = time.monotonic()
    proc = run_cstar(
        env,
        "workplan",
        "run",
        str(workplan),
        "--var",
        f"example_data={example_data}",
        "--run-id",
        RUN_ID,
    )
    data_home = root / "data"
    output = f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    if proc.returncode != 0:
        tails = run_log_tails(data_home, RUN_ID)
        pytest.fail(f"workplan run exited {proc.returncode}\n{output}\n{tails}")
    assert SCHEDULED_MESSAGE in proc.stdout, output

    statuses = wait_for_terminal(
        root / "state", RUN_ID, STEPS, timeout=RUN_TIMEOUT, poll_interval=POLL_INTERVAL
    )
    wall_seconds = time.monotonic() - started
    if not all(s in TERMINAL for s in statuses.values()):
        kill_run(root / "state", RUN_ID, STEPS)
        pytest.fail(
            f"workplan did not finish within {RUN_TIMEOUT}s: {statuses}\n"
            f"{run_log_tails(data_home, RUN_ID)}"
        )
    return NestingRun(
        env=env, data_home=data_home, statuses=statuses, wall_seconds=wall_seconds
    )


def open_dataset(path: Path) -> "xr.Dataset":
    """Open ``path`` with times left as the raw numbers the model wrote."""
    import xarray as xr

    return xr.open_dataset(path, decode_times=False, mask_and_scale=False)


def child_start_days() -> float:
    """The child's start, in days since `CDR_TIME_EPOCH`, from its forge blueprint."""
    blueprint = yaml.safe_load((EXAMPLE_DIR / "forge_child.yaml").read_text())
    start = datetime.fromisoformat(blueprint["run"]["start_date"])
    return (start - CDR_TIME_EPOCH).total_seconds() / 86400


def test_all_steps_done(nesting_run: NestingRun) -> None:
    """All eight sentinels reached ``Done``."""
    assert nesting_run.statuses == ALL_DONE, run_log_tails(
        nesting_run.data_home, RUN_ID
    )


def test_workplan_status_names_all_steps(nesting_run: NestingRun) -> None:
    """``cstar workplan status`` succeeds and shows every step as done."""
    # a wide terminal keeps rich from truncating step names
    proc = run_cstar(
        {**nesting_run.env, "COLUMNS": "200"}, "workplan", "status", RUN_ID
    )
    assert proc.returncode == 0, proc.stderr
    for step in STEPS:
        assert step in proc.stdout
    assert proc.stdout.count("✔") == len(STEPS), proc.stdout


def test_parent_wrote_child_boundaries(nesting_run: NestingRun) -> None:
    """The parent extracted boundary data for the child, next to its restarts."""
    assert nesting_run.outputs("parent", "child_bry.*.nc")
    assert nesting_run.outputs("parent", "output_rst.*.nc")


def test_nest_ic_wrote_child_initial_conditions(nesting_run: NestingRun) -> None:
    """``nest_ic`` regridded the parent's restart into the child's initial conditions."""
    assert nesting_run.outputs("nest_ic", "ic_from_parent_rst.*.nc")


def test_child_wrote_upscaling_fluxes(nesting_run: NestingRun) -> None:
    """The child wrote ``_uscl`` boundary fluxes for the upscaler and ``_cdr`` output."""
    assert nesting_run.outputs("child", "*_uscl.*.nc")
    assert nesting_run.outputs("child", "*_cdr.*.nc")


def test_upscaled_forcing_is_cdf5(nesting_run: NestingRun) -> None:
    """``upscaled_cdr.nc`` is classic CDF-5 netCDF, which ParallelIO needs to read it."""
    (path,) = nesting_run.outputs("upscale", "upscaled_cdr.nc")
    assert path.read_bytes()[:4] == CDF5_MAGIC


def test_upscaled_forcing_time_and_flux(nesting_run: NestingRun) -> None:
    """``cdr_time`` increases from the child's start, and the ALK flux is finite and non-zero."""
    (path,) = nesting_run.outputs("upscale", "upscaled_cdr.nc")
    with open_dataset(path) as ds:
        cdr_time = ds["cdr_time"].values
        # tracer 0 of the profile is ALK
        alk_flux = ds["cdr_trcflx_profile"].isel(two=0).values
    assert np.all(np.diff(cdr_time) > 0), cdr_time
    # a 1e-6 day tolerance is 0.09 s; the default relative one is minutes
    assert cdr_time[0] == pytest.approx(child_start_days(), rel=0, abs=1e-6)
    assert np.isfinite(alk_flux).all()
    assert np.abs(alk_flux).max() > 0


def test_parent_up_wrote_cdr_source(nesting_run: NestingRun) -> None:
    """Each ``_cdr`` file of ``parent_up`` has a finite ``ALK_source`` with a positive maximum."""
    files = nesting_run.outputs("parent_up", "*_cdr.*.nc")
    assert files, nesting_run.root("parent_up")
    for path in files:
        with open_dataset(path) as ds:
            source = ds["ALK_source"].values
        assert np.isfinite(source).all(), f"{path.name}: ALK_source not finite"
        assert source.max() > 0, f"{path.name}: ALK_source is never positive"


def test_child_cdr_metrics_are_finite(nesting_run: NestingRun) -> None:
    """roms-tools computes the CDR metrics of the child's ``_cdr`` files, all finite.

    The child only: ``parent_up``'s ``ALK_source`` dips slightly negative while the
    upscaled flux is still settling, which roms-tools' source check rejects.
    """
    import roms_tools as rt
    from roms_tools.analysis.cdr_analysis import compute_cdr_metrics

    files = nesting_run.outputs("child", "*_cdr.*.nc")
    grid_path = (
        nesting_run.root("forge_child") / "input_data" / "nest-tiny-child_grid.nc"
    )
    grid = rt.Grid(filename=grid_path)
    output = rt.ROMSOutput(grid=grid, path=[str(p) for p in files], use_dask=False)
    metrics = compute_cdr_metrics(output.ds, output.grid.ds)
    nonfinite = [n for n, v in metrics.data_vars.items() if not np.isfinite(v).all()]
    assert not nonfinite, nonfinite


@pytest.mark.parametrize("step", ROMS_STEPS)
def test_roms_logs_are_clean(nesting_run: NestingRun, step: str) -> None:
    """The ROMS step's stdout shows time stepping ran and contains no ROMS failure markers."""
    lines = nesting_run.log_text(step).splitlines()
    assert any("started time-stepping" in line for line in lines), "ROMS never stepped"
    hits = [line.strip() for line in lines if ROMS_FAILURE_PATTERN.search(line)]
    assert not hits, hits[:10]
