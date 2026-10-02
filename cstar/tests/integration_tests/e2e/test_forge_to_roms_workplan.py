"""Tier 2: a forge -> roms_marbl workplan run end to end through the ``cstar`` CLI.

``cstar workplan run`` only schedules: each step is a detached ``sh`` proxy script that
calls the ``cstar`` executable on ``PATH``. A shim pins that executable to this
checkout, and the fixture then polls the step sentinels until both steps are terminal.
The run compiles ROMS with MARBL and ParallelIO, so it takes minutes.
"""

import re
import shutil
import typing as t
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import f90nml
import numpy as np
import pytest
import yaml

from cstar.base.utils import slugify
from cstar.tests.integration_tests.cases import (
    DT,
    MODEL_REFERENCE_DATE,
    RUN_END,
    RUN_START,
)
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    TERMINAL,
    kill_run,
    make_cli_env,
    run_cstar,
    sentinel_path,
    step_root,
    wait_for_terminal,
)

if t.TYPE_CHECKING:
    from cstar.applications.forge.blueprint import ForgeBlueprint

RUN_ID = "forge-roms-e2e"
FORGE_STEP = "make_inputs"
ROMS_STEP = "run_roms"
SCHEDULED_MESSAGE = "run scheduling has completed"
STEPS = (FORGE_STEP, ROMS_STEP)
POLL_INTERVAL = 5.0
RUN_TIMEOUT = 40 * 60
RESTART_PERIOD = 1800.0
ERROR_PATTERN = re.compile(
    r"\bERROR\b|BLOWUP|blow.?up|MPI_ABORT|Traceback", re.IGNORECASE
)
PARTITIONED_NC = re.compile(r"\.\d+\.nc$")
MISSING_TOOLS = [
    tool
    for tool, found in {
        "nccopy": shutil.which("nccopy"),
        "mpirun": shutil.which("mpirun"),
        "mpifort or gfortran": shutil.which("mpifort") or shutil.which("gfortran"),
    }.items()
    if not found
]

pytestmark = pytest.mark.skipif(
    bool(MISSING_TOOLS), reason=f"ROMS toolchain missing: {', '.join(MISSING_TOOLS)}"
)


@dataclass(frozen=True)
class E2ERun:
    """A completed forge -> roms_marbl workplan run and where its artifacts live."""

    run_id: str
    env: dict[str, str]
    shim: Path
    data_home: Path
    state_home: Path
    forge_root: Path
    roms_root: Path
    statuses: dict[str, int | None]
    stdout: str

    @property
    def roms_working_dir(self) -> Path:
        """The directory ROMS ran in, read from the blueprint that was executed.

        The scheduler overrides ``working_dir`` with the step root through an
        ``apply-overrides`` directive, which is persisted as ``work/*.ovrd.yaml``;
        the blueprint forge published is the fallback if no override was written.
        """
        candidates = sorted(
            (self.roms_root / "work").glob("*.ovrd.yaml"),
            key=lambda p: p.stat().st_mtime,
        ) or sorted((self.forge_root / "output").glob("B_*.yaml"))
        return Path(yaml.safe_load(candidates[-1].read_text())["working_dir"])

    def sentinel(self, step: str) -> Path:
        """Path of the sentinel file holding ``step``'s status."""
        return sentinel_path(self.state_home, self.run_id, step)

    def log_tail(self, root: Path, lines: int = 60) -> str:
        """The last ``lines`` lines of each log under ``root / "logs"``."""
        return "\n".join(
            f"--- {log} ---\n" + "\n".join(log.read_text().splitlines()[-lines:])
            for log in sorted((root / "logs").glob("*.out"))
        )


def write_workplan(path: Path, forge_blueprint: Path) -> Path:
    """Write the two-step forge -> roms_marbl workplan.

    Every step sets ``max_walltime`` because a ``local`` override is what wraps the
    step command in ``timeout``; without one a hung step never ends.

    Returns
    -------
    Path
        ``path``.
    """
    workplan = {
        "name": "forge-roms-e2e",
        "description": "forge then roms_marbl",
        "steps": [
            {
                "name": FORGE_STEP,
                "application": "forge",
                "blueprint": str(forge_blueprint),
                "compute_overrides": {"local": {"max_walltime": "00:30:00"}},
            },
            {
                "name": ROMS_STEP,
                "application": "roms_marbl",
                "blueprint": {"from_step": FORGE_STEP},
                "depends_on": [FORGE_STEP],
                "compute_overrides": {"local": {"max_walltime": "00:45:00"}},
            },
        ],
    }
    path.write_text(yaml.safe_dump(workplan, sort_keys=False))
    return path


@pytest.fixture(scope="module")
def e2e_run(
    forge_blueprint_factory: Callable[..., tuple["ForgeBlueprint", Path]],
    cstar_shim: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> t.Iterator[E2ERun]:
    """Schedule the workplan once for the module and wait for both steps to finish.

    Yields
    ------
    E2ERun
        The finished run. Any step proxy still running at teardown is stopped.
    """
    root = tmp_path_factory.mktemp("e2e")
    try:
        run = _schedule_and_wait(forge_blueprint_factory, cstar_shim, root)
    except BaseException:
        kill_run(root / "state", RUN_ID, STEPS)
        raise
    yield run
    kill_run(run.state_home, run.run_id, STEPS)


def _schedule_and_wait(
    forge_blueprint_factory: Callable[..., tuple["ForgeBlueprint", Path]],
    shim: Path,
    root: Path,
) -> E2ERun:
    """Build the blueprint and workplan, run ``cstar workplan run``, and poll to the end.

    Returns
    -------
    E2ERun
        The finished run.
    """
    _, blueprint_path = forge_blueprint_factory("unified", root / "forge")
    workplan = write_workplan(root / "workplan.yaml", blueprint_path)
    env = make_cli_env(root, shim)

    proc = run_cstar(env, "workplan", "run", "--run-id", RUN_ID, str(workplan))
    output = f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    assert proc.returncode == 0, f"workplan run exited {proc.returncode}\n{output}"
    assert SCHEDULED_MESSAGE in proc.stdout, output

    state_home = root / "state"
    statuses = wait_for_terminal(
        state_home, RUN_ID, STEPS, timeout=RUN_TIMEOUT, poll_interval=POLL_INTERVAL
    )
    data_home = root / "data"
    step_roots = {s: step_root(data_home, RUN_ID, s) for s in statuses}
    run = E2ERun(
        run_id=RUN_ID,
        env=env,
        shim=shim,
        data_home=data_home,
        state_home=state_home,
        forge_root=step_roots[FORGE_STEP],
        roms_root=step_roots[ROMS_STEP],
        statuses=statuses,
        stdout=proc.stdout,
    )
    if not all(s in TERMINAL for s in statuses.values()):
        kill_run(state_home, RUN_ID, STEPS)
        pytest.fail(
            f"workplan did not finish within {RUN_TIMEOUT}s: {statuses}\n"
            f"{run.log_tail(run.forge_root)}\n{run.log_tail(run.roms_root)}"
        )
    return run


def restart_files(run: E2ERun) -> list[Path]:
    """The ROMS restart files, ordered by their timestamp."""
    return sorted((run.roms_working_dir / "output").glob("*_rst*.nc"))


def restart_times(run: E2ERun) -> list[np.ndarray]:
    """The ``ocean_time`` values (seconds) of each restart file, in file order."""
    import xarray as xr

    times = []
    for path in restart_files(run):
        with xr.open_dataset(path, decode_times=False, mask_and_scale=False) as ds:
            times.append(np.atleast_1d(ds["ocean_time"].values))
    return times


def tracers_to_write(run: E2ERun) -> list[str]:
    """The MARBL tracers named in the namelist forge generated."""
    namelist = f90nml.read(run.forge_root / "builds" / "run-time" / "namelist.nml")
    return list(namelist["marbl_biogeochemistry_settings"]["marbl_tracers_to_write"])


def test_both_steps_done(e2e_run: E2ERun) -> None:
    """Both sentinels reached ``Done``."""
    tails = (
        f"{e2e_run.log_tail(e2e_run.forge_root)}\n{e2e_run.log_tail(e2e_run.roms_root)}"
    )
    assert e2e_run.statuses == {FORGE_STEP: DONE, ROMS_STEP: DONE}, tails


def test_forge_published_blueprint_and_classic_inputs(e2e_run: E2ERun) -> None:
    """Forge published one blueprint, and its input files are CDF-5 classic netCDF."""
    published = list((e2e_run.forge_root / "output").glob("B_*.yaml"))
    assert len(published) == 1, published

    inputs = sorted((e2e_run.forge_root / "input_data").glob("*.nc"))
    assert inputs, "forge wrote no input files"
    headers = {p.name: p.read_bytes()[:4] for p in inputs}
    assert all(h == b"CDF\x05" for h in headers.values()), headers


def test_roms_staged_unpartitioned_inputs(e2e_run: E2ERun) -> None:
    """ROMS with ParallelIO read the staged inputs without partitioning them."""
    staged = sorted((e2e_run.roms_root / "input" / "input_datasets").rglob("*.nc"))
    assert staged, "no input datasets were staged"
    assert [p for p in staged if PARTITIONED_NC.search(p.name)] == []


def test_restart_records(e2e_run: E2ERun) -> None:
    """Two restart files are written 1800 s apart, each holding the last two time levels."""
    times = restart_times(e2e_run)
    assert len(times) == 2, [p.name for p in restart_files(e2e_run)]
    # ROMS restarts hold the previous and current time level, one DT apart
    assert [np.diff(t) for t in times] == [pytest.approx([DT])] * 2
    assert times[1][-1] - times[0][-1] == pytest.approx(RESTART_PERIOD)


def test_restart_ends_at_run_end(e2e_run: E2ERun) -> None:
    """The last restart record is at the run end, counted from the reference date."""
    import xarray as xr

    with xr.open_dataset(restart_files(e2e_run)[0], decode_times=False) as ds:
        long_name = ds["ocean_time"].attrs.get("long_name", "")
    match = re.search(r"(\d{4})/(\d{2})/(\d{2})", long_name)
    assert match, f"no reference date in ocean_time long_name {long_name!r}"
    year, month, day = (int(g) for g in match.groups())
    epoch = datetime(year, month, day)
    assert epoch == MODEL_REFERENCE_DATE

    expected = (RUN_END - MODEL_REFERENCE_DATE).total_seconds()
    assert restart_times(e2e_run)[-1][-1] == pytest.approx(expected)
    first = (RUN_START - MODEL_REFERENCE_DATE).total_seconds()
    assert restart_times(e2e_run)[0][-1] == pytest.approx(first + RESTART_PERIOD)


def test_restart_fields_are_finite(e2e_run: E2ERun) -> None:
    """The physical state and every written MARBL tracer are finite in each file."""
    import xarray as xr

    names = ["zeta", "temp", "salt", *tracers_to_write(e2e_run)]
    bad: dict[str, list[str]] = {}
    for path in restart_files(e2e_run):
        with xr.open_dataset(path, decode_times=False, mask_and_scale=False) as ds:
            missing = [n for n in names if n not in ds.variables]
            nonfinite = [
                n
                for n in names
                if n in ds.variables and not np.isfinite(ds[n].values).all()
            ]
        if missing or nonfinite:
            bad[path.name] = [f"missing: {missing}", f"non-finite: {nonfinite}"]
    assert not bad, bad


def test_roms_logs_are_clean(e2e_run: E2ERun) -> None:
    """The ROMS step's stdout shows time stepping ran and contains no error markers."""
    log = e2e_run.roms_root / "logs" / f"{slugify(ROMS_STEP)}.out"
    lines = log.read_text(errors="replace").splitlines()
    assert any("started time-stepping" in line for line in lines), "ROMS never stepped"
    hits = [line.strip() for line in lines if ERROR_PATTERN.search(line)]
    assert not hits, hits[:10]


def test_workplan_status_names_both_steps(e2e_run: E2ERun) -> None:
    """``cstar workplan status`` succeeds and shows both steps as done."""
    # a wide terminal keeps rich from truncating step names
    proc = run_cstar(
        {**e2e_run.env, "COLUMNS": "200"}, "workplan", "status", e2e_run.run_id
    )
    assert proc.returncode == 0, proc.stderr
    assert FORGE_STEP in proc.stdout
    assert ROMS_STEP in proc.stdout
    assert proc.stdout.count("\u2714") == 2, proc.stdout


def test_resume_of_completed_run_keeps_steps_done(e2e_run: E2ERun) -> None:
    """Resuming a finished run succeeds and leaves both steps ``Done``."""
    proc = run_cstar(
        e2e_run.env, "workplan", "run", "--run-id", e2e_run.run_id, "--resume"
    )
    assert proc.returncode == 0, f"{proc.stdout}\n{proc.stderr}"

    statuses = wait_for_terminal(
        e2e_run.state_home,
        e2e_run.run_id,
        STEPS,
        timeout=300,
        poll_interval=POLL_INTERVAL,
    )
    assert statuses == {FORGE_STEP: DONE, ROMS_STEP: DONE}
