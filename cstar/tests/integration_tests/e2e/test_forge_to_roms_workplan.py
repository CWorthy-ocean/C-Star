"""Tier 2: a forge -> roms_marbl -> CDR-LiTE workplan run end to end through the ``cstar`` CLI.

``cstar workplan run`` only schedules: each step is a detached ``sh`` proxy script that
calls the ``cstar`` executable on ``PATH``. A shim pins that executable to this
checkout, and the fixture then polls the step sentinels until all four are terminal.

The first two steps are a MARBL forge and ROMS run that also writes the ``_cdrgas``
gas-exchange sensitivities; the last two are a ``cdr_lite`` forge and a ROMS run
(no MARBL) that reads those sensitivities through the ``carbonate-sensitivity-from``
directive. The run compiles ROMS with ParallelIO twice, so it takes tens of minutes.
"""

import re
import shutil
import typing as t
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from functools import partial
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
    run_log_tails,
    sentinel_path,
    step_root,
    wait_for_terminal,
)

if t.TYPE_CHECKING:
    from cstar.applications.forge.blueprint import ForgeBlueprint

RUN_ID = "forge-roms-e2e"
FORGE_STEP = "make_inputs"
ROMS_STEP = "run_roms"
CDR_FORGE_STEP = "make_cdr_lite_inputs"
CDR_STEP = "run_cdr_lite"
SCHEDULED_MESSAGE = "run scheduling has completed"
STEPS = (FORGE_STEP, ROMS_STEP, CDR_FORGE_STEP, CDR_STEP)
ALL_DONE = dict.fromkeys(STEPS, DONE)
POLL_INTERVAL = 5.0
RUN_TIMEOUT = 100 * 60
RESTART_PERIOD = 1800.0
GAS_EXCH_PERIOD = 900.0
"""The ``cdr_gas_exch_output`` window of ``make_inputs``; it and ``nrpf`` 2 divide ``RESTART_PERIOD``."""
GAS_EXCH_OVERRIDES = {
    "cdr_gas_exch_output": {
        "do_cdr_gas_exch_output": True,
        "output_period": GAS_EXCH_PERIOD,
        "nrpf": 2,
    }
}
MISSING_FLX_MESSAGE = "CDR_OAE_DIC1_flx not in forcing files"
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
    cdr_forge_root: Path
    cdr_root: Path
    statuses: dict[str, int | None]
    stdout: str

    @property
    def roms_working_dir(self) -> Path:
        """The directory ROMS ran in, read from the blueprint that was executed."""
        return _executed_working_dir(self.roms_root, self.forge_root)

    @property
    def cdr_working_dir(self) -> Path:
        """The directory the CDR-LiTE ROMS step ran in."""
        return _executed_working_dir(self.cdr_root, self.cdr_forge_root)

    def working_dir_of(self, step: str) -> Path:
        """The directory the roms_marbl ``step`` ran in."""
        return {ROMS_STEP: self.roms_working_dir, CDR_STEP: self.cdr_working_dir}[step]

    def sentinel(self, step: str) -> Path:
        """Path of the sentinel file holding ``step``'s status."""
        return sentinel_path(self.state_home, self.run_id, step)

    def all_log_tails(self) -> str:
        """The log tails of every step."""
        roots = (self.forge_root, self.roms_root, self.cdr_forge_root, self.cdr_root)
        return "\n".join(self.log_tail(root) for root in roots)

    def log_tail(self, root: Path, lines: int = 60) -> str:
        """The last ``lines`` lines of each log under ``root / "logs"``."""
        return "\n".join(
            f"--- {log} ---\n" + "\n".join(log.read_text().splitlines()[-lines:])
            for log in sorted((root / "logs").glob("*.out"))
        )


def _executed_working_dir(step_root: Path, forge_root: Path) -> Path:
    """The ``working_dir`` of the blueprint a roms_marbl step executed.

    The scheduler overrides ``working_dir`` with the step root through an
    ``apply-overrides`` directive, which is persisted as ``work/*.ovrd.yaml``; the
    blueprint the forge step published is the fallback if no override was written.
    """
    candidates = sorted(
        (step_root / "work").glob("*.ovrd.yaml"), key=lambda p: p.stat().st_mtime
    ) or sorted((forge_root / "output").glob("B_*.yaml"))
    return Path(yaml.safe_load(candidates[-1].read_text())["working_dir"])


def write_workplan(
    path: Path, forge_blueprint: Path, cdr_forge_blueprint: Path
) -> Path:
    """Write the four-step workplan.

    ``make_inputs`` -> ``run_roms`` is a MARBL run that writes ``_cdrgas`` files;
    ``make_cdr_lite_inputs`` -> ``run_cdr_lite`` is a CDR-LiTE run (no MARBL) whose
    ``carbonate-sensitivity-from`` directive reads them from ``run_roms``.

    Every step sets ``max_walltime`` because a ``local`` override is what wraps the
    step command in ``timeout``; without one a hung step never ends.

    Returns
    -------
    Path
        ``path``.
    """
    workplan = {
        "name": "forge-roms-e2e",
        "description": "forge, roms_marbl, then a CDR-LiTE forge and roms_marbl run",
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
                "compute_overrides": {"local": {"max_walltime": "01:00:00"}},
            },
            {
                "name": CDR_FORGE_STEP,
                "application": "forge",
                "blueprint": str(cdr_forge_blueprint),
                "compute_overrides": {"local": {"max_walltime": "00:30:00"}},
            },
            {
                "name": CDR_STEP,
                "application": "roms_marbl",
                "blueprint": {"from_step": CDR_FORGE_STEP},
                "depends_on": [CDR_FORGE_STEP, ROMS_STEP],
                "directives": {"carbonate-sensitivity-from": {"step": ROMS_STEP}},
                "compute_overrides": {"local": {"max_walltime": "01:00:00"}},
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
    """Schedule the workplan once for the module and wait for all steps to finish.

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
    _, blueprint_path = forge_blueprint_factory(
        "unified", root / "forge", run_time_overrides=GAS_EXCH_OVERRIDES
    )
    _, cdr_blueprint_path = forge_blueprint_factory(
        "cdr_lite", root / "cdr_forge", name="it-cdr_lite-e2e"
    )
    workplan = write_workplan(
        root / "workplan.yaml", blueprint_path, cdr_blueprint_path
    )
    env = make_cli_env(root, shim)

    proc = run_cstar(env, "workplan", "run", "--run-id", RUN_ID, str(workplan))
    output = f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    if proc.returncode != 0:
        tails = run_log_tails(root / "data", RUN_ID)
        pytest.fail(f"workplan run exited {proc.returncode}\n{output}\n{tails}")
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
        cdr_forge_root=step_roots[CDR_FORGE_STEP],
        cdr_root=step_roots[CDR_STEP],
        statuses=statuses,
        stdout=proc.stdout,
    )
    if not all(s in TERMINAL for s in statuses.values()):
        kill_run(state_home, RUN_ID, STEPS)
        pytest.fail(
            f"workplan did not finish within {RUN_TIMEOUT}s: {statuses}\n"
            f"{run.all_log_tails()}"
        )
    return run


def restart_files(run: E2ERun, step: str = ROMS_STEP) -> list[Path]:
    """The restart files of the roms_marbl ``step``, ordered by their timestamp."""
    return sorted((run.working_dir_of(step) / "output").glob("*_rst*.nc"))


def restart_times(run: E2ERun, step: str = ROMS_STEP) -> list[np.ndarray]:
    """The ``ocean_time`` values (seconds) of each restart file of ``step``, in file order."""
    import xarray as xr

    times = []
    for path in restart_files(run, step):
        with xr.open_dataset(path, decode_times=False, mask_and_scale=False) as ds:
            times.append(np.atleast_1d(ds["ocean_time"].values))
    return times


def tracers_to_write(run: E2ERun) -> list[str]:
    """The MARBL tracers named in the namelist forge generated."""
    namelist = f90nml.read(run.forge_root / "builds" / "run-time" / "namelist.nml")
    return list(namelist["marbl_biogeochemistry_settings"]["marbl_tracers_to_write"])


def test_all_steps_done(e2e_run: E2ERun) -> None:
    """All four sentinels reached ``Done``."""
    assert e2e_run.statuses == ALL_DONE, e2e_run.all_log_tails()


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


def assert_restarts_span_run(run: E2ERun, step: str) -> None:
    """Assert ``step``'s restarts are counted from the reference date and end at the run end."""
    import xarray as xr

    with xr.open_dataset(restart_files(run, step)[0], decode_times=False) as ds:
        long_name = ds["ocean_time"].attrs.get("long_name", "")
    match = re.search(r"(\d{4})/(\d{2})/(\d{2})", long_name)
    assert match, f"no reference date in ocean_time long_name {long_name!r}"
    year, month, day = (int(g) for g in match.groups())
    epoch = datetime(year, month, day)
    assert epoch == MODEL_REFERENCE_DATE

    times = restart_times(run, step)
    expected = (RUN_END - MODEL_REFERENCE_DATE).total_seconds()
    assert times[-1][-1] == pytest.approx(expected)
    first = (RUN_START - MODEL_REFERENCE_DATE).total_seconds()
    assert times[0][-1] == pytest.approx(first + RESTART_PERIOD)


def test_restart_ends_at_run_end(e2e_run: E2ERun) -> None:
    """The last restart record is at the run end, counted from the reference date."""
    assert_restarts_span_run(e2e_run, ROMS_STEP)


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


def gas_exch_files(run: E2ERun) -> list[Path]:
    """The ``_cdrgas`` gas-exchange sensitivity files ``run_roms`` wrote, by timestamp."""
    return sorted((run.roms_working_dir / "output").glob("*_cdrgas*.nc"))


def cdr_namelist(run: E2ERun) -> f90nml.Namelist:
    """The namelist ROMS read in ``run_cdr_lite``, with the directive's inputs added."""
    return f90nml.read(run.cdr_working_dir / "work" / "cstar_generated_roms.nml")


def test_gas_exch_files_span_run(e2e_run: E2ERun) -> None:
    """``run_roms`` wrote ``_cdrgas`` records at the run start, the 900 s window midpoints, and the run end."""
    import xarray as xr

    files = gas_exch_files(e2e_run)
    assert files, [p.name for p in (e2e_run.roms_working_dir / "output").glob("*.nc")]
    records = []
    for path in files:
        with xr.open_dataset(path, decode_times=False, mask_and_scale=False) as ds:
            records.append(ds["ddic_dco2_time"].values)
    days = np.sort(np.concatenate(records))
    start = (RUN_START - MODEL_REFERENCE_DATE).total_seconds() / 86400
    end = (RUN_END - MODEL_REFERENCE_DATE).total_seconds() / 86400
    n_windows = round((RUN_END - RUN_START).total_seconds() / GAS_EXCH_PERIOD)
    midpoints = start + (np.arange(n_windows) + 0.5) * GAS_EXCH_PERIOD / 86400
    # an absolute tolerance of 1e-6 day (0.09 s): the default relative one is ~6 minutes
    approx = partial(pytest.approx, rel=0, abs=1e-6)
    assert days[0] == approx(start)
    assert days[-1] == approx(end)
    assert days[1:-1] == approx(midpoints)


def test_gas_exch_sensitivities_are_finite(e2e_run: E2ERun) -> None:
    """``ddic_dco2`` and ``ddic_dalk`` are finite everywhere, and non-zero over the ocean."""
    import xarray as xr

    for path in gas_exch_files(e2e_run):
        with xr.open_dataset(path, decode_times=False, mask_and_scale=False) as ds:
            for name in ("ddic_dco2", "ddic_dalk"):
                values = ds[name].values
                assert np.isfinite(values).all(), f"{path.name}: {name} not finite"
                assert np.abs(values).max() > 0, f"{path.name}: {name} is all zero"


def test_cdr_lite_published_blueprint_leaves_sensitivity_to_directive(
    e2e_run: E2ERun,
) -> None:
    """The forge-emitted ``cdr_lite`` blueprint has no carbonate sensitivity; the directive's does."""
    published = sorted((e2e_run.cdr_forge_root / "output").glob("B_*.yaml"))
    assert len(published) == 1, published
    emitted = yaml.safe_load(published[0].read_text())
    # None is the default, so the serialized blueprint may omit the key
    assert emitted["forcing"].get("carbonate_sensitivity") is None

    applied = sorted((e2e_run.cdr_root / "work").glob("*.csfrom*.yaml"))
    assert applied, sorted(p.name for p in (e2e_run.cdr_root / "work").iterdir())
    locations = {
        Path(d["location"])
        for d in yaml.safe_load(applied[-1].read_text())["forcing"][
            "carbonate_sensitivity"
        ]["data"]
    }
    assert locations == set(gas_exch_files(e2e_run))


def test_cdr_lite_namelist(e2e_run: E2ERun) -> None:
    """The CDR-LiTE run has the CDR tracers and no BGC tracers, and forces from the ``_cdrgas`` files."""
    nml = cdr_namelist(e2e_run)
    assert nml["param_settings"]["nt_cdr_oae"] == 1
    assert nml["param_settings"]["nt_bgc"] == 0
    cdr_lite = nml.get("cdr_lite_settings", {})
    assert cdr_lite.get("cdr_online_carbonate_sensitivity", False) is False
    frcfiles = nml["forcing_files"]["frcfiles"]
    frcfiles = [frcfiles] if isinstance(frcfiles, str) else frcfiles
    names = [Path(f).name for f in frcfiles]
    assert {p.name for p in gas_exch_files(e2e_run)} <= set(names), names


def test_cdr_lite_cppdefs(e2e_run: E2ERun) -> None:
    """The CDR-LiTE build defines ``CDR_LITE`` and leaves MARBL out."""
    text = (
        e2e_run.cdr_working_dir / "input" / "compile_time_code" / "cppdefs.opt"
    ).read_text()
    assert re.search(r"^#define CDR_LITE\b", text, re.MULTILINE)
    assert re.search(r"^#undef MARBL\b", text, re.MULTILINE)


def test_cdr_lite_log_is_clean(e2e_run: E2ERun) -> None:
    """``run_cdr_lite`` stepped, read its forcing, and found only the expected missing flux."""
    log = e2e_run.cdr_root / "logs" / f"{slugify(CDR_STEP)}.out"
    lines = log.read_text(errors="replace").splitlines()
    assert any("started time-stepping" in line for line in lines), "ROMS never stepped"
    hits = [line.strip() for line in lines if ERROR_PATTERN.search(line)]
    assert not hits, hits[:10]
    # the _cdrgas files hold sensitivities only, so the DIC flux is expected to be absent
    assert sum(MISSING_FLX_MESSAGE in line for line in lines) == 1
    assert not [ln for ln in lines if "Could not find var" in ln]
    assert not [ln for ln in lines if "Ran out of time records" in ln]


def test_cdr_lite_restart_ends_at_run_end(e2e_run: E2ERun) -> None:
    """``run_cdr_lite`` wrote two finite-time restarts that end at the run end."""
    times = restart_times(e2e_run, CDR_STEP)
    assert len(times) == 2, [p.name for p in restart_files(e2e_run, CDR_STEP)]
    assert [np.diff(t) for t in times] == [pytest.approx([DT])] * 2
    assert times[1][-1] - times[0][-1] == pytest.approx(RESTART_PERIOD)
    assert_restarts_span_run(e2e_run, CDR_STEP)


def test_cdr_lite_restart_fields_are_finite(e2e_run: E2ERun) -> None:
    """The physical state and the CDR tracers are finite in each ``run_cdr_lite`` restart."""
    import xarray as xr

    names = ["zeta", "temp", "salt", "CDR_OAE_ALK1", "CDR_OAE_DIC1", "CDR_DOR_DIC1"]
    bad: dict[str, list[str]] = {}
    for path in restart_files(e2e_run, CDR_STEP):
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


def test_workplan_status_names_all_steps(e2e_run: E2ERun) -> None:
    """``cstar workplan status`` succeeds and shows every step as done."""
    # a wide terminal keeps rich from truncating step names
    proc = run_cstar(
        {**e2e_run.env, "COLUMNS": "200"}, "workplan", "status", e2e_run.run_id
    )
    assert proc.returncode == 0, proc.stderr
    for step in STEPS:
        assert step in proc.stdout
    assert proc.stdout.count("\u2714") == len(STEPS), proc.stdout


def test_resume_of_completed_run_keeps_steps_done(e2e_run: E2ERun) -> None:
    """Resuming a finished run succeeds and leaves every step ``Done``."""
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
    assert statuses == ALL_DONE
