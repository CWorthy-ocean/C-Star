"""Tier 2: a forge -> ucla_roms -> CDR metrics and CDR-LiTE workplan run end to end through the ``cstar`` CLI.

``cstar workplan submit`` only schedules: each step is a detached ``sh`` proxy script that
calls the ``cstar`` executable on ``PATH``. A shim pins that executable to this
checkout, and the fixture then polls the step sentinels until all eight are terminal.

A MARBL forge with an ALK release feeds three ROMS runs: ``run_roms`` covers the whole
hour and also writes the ``_cdrgas`` gas-exchange sensitivities, and ``run_cdr_seg1`` and
``run_cdr_seg2`` cover it as two half-hour segments chained with ``continue-from``. All
three write the ``_cdr`` output, and two ``cdr_metrics`` steps compute the CDR metrics
from the segments and from the whole run, which must agree. The remaining two steps are a
``cdr_lite`` forge and a ROMS run (no MARBL) that reads the ``_cdrgas`` sensitivities
through the ``carbonate-sensitivity-from`` directive. The run compiles ROMS with
ParallelIO four times, so it takes tens of minutes.
"""

import json
import re
import shutil
import subprocess
import typing as t
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path

import f90nml
import numpy as np
import pytest
import yaml

from cstar.applications.cdr_metrics import FIGURE_FILENAME, METRICS_FILENAME
from cstar.applications.core import ArtifactKind
from cstar.base.utils import slugify
from cstar.orchestration.manifest import product_paths, read_step_record
from cstar.orchestration.models import InlineBlueprintRef
from cstar.tests.integration_tests.cases import (
    DT,
    MARBL_CDR_FORCING,
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
CDR_SEG1_STEP = "run_cdr_seg1"
CDR_SEG2_STEP = "run_cdr_seg2"
METRICS_SEGMENTED_STEP = "cdr_metrics_segmented"
METRICS_WHOLE_STEP = "cdr_metrics_whole"
SCHEDULED_MESSAGE = "run scheduling has completed"
STEPS = (
    FORGE_STEP,
    ROMS_STEP,
    CDR_FORGE_STEP,
    CDR_STEP,
    CDR_SEG1_STEP,
    CDR_SEG2_STEP,
    METRICS_SEGMENTED_STEP,
    METRICS_WHOLE_STEP,
)
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
CDR_OUTPUT_PERIOD = 900.0
"""The ``cdr_output`` window of ``make_inputs``; it and ``nrpf`` 2 divide ``RESTART_PERIOD``."""
MAKE_INPUTS_OVERRIDES = {
    **GAS_EXCH_OVERRIDES,
    "cdr_output": {"output_period": CDR_OUTPUT_PERIOD, "nrpf": 2},
    "marbl_bgc": {"marbl_timestep": CDR_OUTPUT_PERIOD},
}
"""Run-time overrides of ``make_inputs``: the ``_cdrgas`` and ``_cdr`` streams (forge turns
the latter on itself for a MARBL run with an active CDR forcing), and a MARBL timestep the
``_cdr`` window is a multiple of, which ucla-roms >= 0.13.0 requires."""
SEGMENT_SPLIT = RUN_START + timedelta(seconds=RESTART_PERIOD)
"""Where ``run_cdr_seg1`` ends and ``run_cdr_seg2`` continues: the first restart record."""
METRICS_RECORDS = round((RUN_END - RUN_START).total_seconds() / CDR_OUTPUT_PERIOD)
"""The ``_cdr`` records, hence the ``cdr_metrics`` time steps, of a run of the whole window."""
METRICS_STEPS = (METRICS_SEGMENTED_STEP, METRICS_WHOLE_STEP)
CDR_OUTPUT_STEPS = (ROMS_STEP, CDR_SEG1_STEP, CDR_SEG2_STEP)
METRICS_VARIABLES = (
    "cdr_efficiency_from_flux",
    "cdr_efficiency_from_DIC_difference",
    "cdr_carbon_uptake_from_flux",
    "cdr_carbon_uptake_from_DIC_difference",
)
UPTAKE_VARIABLES = tuple(v for v in METRICS_VARIABLES if "uptake" in v)
MISSING_FLX_MESSAGE = "CDR_OAE_DIC1_flx not in forcing files"
ERROR_PATTERN = re.compile(
    r"\bERROR\b|BLOWUP|blow.?up|MPI_ABORT|Traceback", re.IGNORECASE
)
LOG_RECORD_START = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} ")
ERROR_CONTEXT_LINES = 20
PARTITIONED_NC = re.compile(r"\.\d+\.nc$")
TIMESTAMP = re.compile(r"^\d{14}$")
PRODUCT_HEADINGS = ("Products missing", "Products whose size", "Products whose content")
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
    """A completed forge -> ucla_roms workplan run and where its artifacts live."""

    run_id: str
    env: dict[str, str]
    shim: Path
    data_home: Path
    state_home: Path
    forge_root: Path
    roms_root: Path
    cdr_forge_root: Path
    cdr_root: Path
    cdr_seg1_root: Path
    cdr_seg2_root: Path
    metrics_segmented_root: Path
    metrics_whole_root: Path
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
        """The directory the ucla_roms ``step`` ran in."""
        # lazy: resolving a step that never ran raises, so resolve only ``step``
        working_dirs = {
            ROMS_STEP: lambda: self.roms_working_dir,
            CDR_STEP: lambda: self.cdr_working_dir,
            CDR_SEG1_STEP: lambda: _executed_working_dir(
                self.cdr_seg1_root, self.forge_root
            ),
            CDR_SEG2_STEP: lambda: _executed_working_dir(
                self.cdr_seg2_root, self.forge_root
            ),
        }
        return working_dirs[step]()

    def metrics_dir_of(self, step: str) -> Path:
        """The ``output`` directory of the ``cdr_metrics`` ``step``.

        An inline blueprint's ``working_dir`` is the step root.
        """
        roots = {
            METRICS_SEGMENTED_STEP: self.metrics_segmented_root,
            METRICS_WHOLE_STEP: self.metrics_whole_root,
        }
        return roots[step] / "output"

    def sentinel(self, step: str) -> Path:
        """Path of the sentinel file holding ``step``'s status."""
        return sentinel_path(self.state_home, self.run_id, step)

    def all_log_tails(self) -> str:
        """The log tails of every step."""
        roots = (
            self.forge_root,
            self.roms_root,
            self.cdr_forge_root,
            self.cdr_root,
            self.cdr_seg1_root,
            self.cdr_seg2_root,
            self.metrics_segmented_root,
            self.metrics_whole_root,
        )
        return "\n".join(self.log_tail(root) for root in roots)

    def log_tail(self, root: Path, lines: int = 60) -> str:
        """The last ``lines`` lines of each log under ``root / "logs"``."""
        return "\n".join(
            f"--- {log} ---\n" + "\n".join(log.read_text().splitlines()[-lines:])
            for log in sorted((root / "logs").glob("*.out"))
        )


def _executed_working_dir(step_root: Path, forge_root: Path) -> Path:
    """The ``working_dir`` of the blueprint a ucla_roms step executed.

    The scheduler overrides ``working_dir`` with the step root through an
    ``apply-overrides`` directive, which is persisted as ``work/*.ovrd.yaml``; the
    blueprint the forge step published is the fallback if no override was written.
    """
    candidates = sorted(
        (step_root / "work").glob("*.ovrd.yaml"), key=lambda p: p.stat().st_mtime
    ) or sorted((forge_root / "output").glob("B_*.yaml"))
    return Path(yaml.safe_load(candidates[-1].read_text())["working_dir"])


def write_workplan(
    path: Path, forge_blueprint: Path, cdr_forge_blueprint: Path, grid_filename: str
) -> Path:
    """Write the eight-step workplan.

    ``make_inputs`` is a MARBL forge with an ALK release and feeds ROMS runs of one hour:
    ``run_roms`` is the whole run, and also writes ``_cdrgas`` files, while
    ``run_cdr_seg1`` and ``run_cdr_seg2`` are two half-hour segments, the second
    continuing from the first's restart. All three write ``_cdr`` output.
    ``cdr_metrics_whole`` and ``cdr_metrics_segmented`` compute the CDR metrics from the
    whole run and from the segments; their blueprints are synthesized from overrides
    that point at the producing steps' output through placeholders, and at the grid
    ``make_inputs`` wrote (``grid_filename`` in its ``input_data`` directory).

    ``make_cdr_lite_inputs`` -> ``run_cdr_lite`` is a CDR-LiTE run (no MARBL) whose
    ``carbonate-sensitivity-from`` directive reads the ``_cdrgas`` files from ``run_roms``.

    No two ROMS steps run at once: ``run_cdr_seg1`` waits for ``run_cdr_lite``, which
    waits for ``run_roms``. Every ROMS step stages its codebases from one shared clone
    cache, and concurrent refreshes of it fail (``CachedRemoteRepositoryStager`` removes
    the cache when a ``git pull`` fails, under a step still copying from it).

    Every step sets ``max_walltime`` because a ``local`` override is what wraps the
    step command in ``timeout``; without one a hung step never ends.

    Returns
    -------
    Path
        ``path``.
    """

    def metrics_overrides(*producers: str) -> dict[str, t.Any]:
        """The ``cdr_metrics`` blueprint's ``domains``, with ``producers``' output as sources."""
        grid = f"{{{{root_dir: {FORGE_STEP}}}}}/input_data/{grid_filename}"
        sources = [{"location": f"{{{{output_dir: {s}}}}}"} for s in producers]
        return {"domains": {"oae": {"grid": {"location": grid}, "sources": sources}}}

    workplan = {
        "name": "forge-roms-e2e",
        "description": (
            "forge, whole and segmented ucla_roms runs with CDR metrics, "
            "then a CDR-LiTE forge and ucla_roms run"
        ),
        "steps": [
            {
                "name": FORGE_STEP,
                "application": "forge",
                "blueprint": str(forge_blueprint),
                "compute_overrides": {"local": {"max_walltime": "00:30:00"}},
            },
            {
                "name": ROMS_STEP,
                # the earlier application name, kept so this run covers the alias end to end
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
            {
                "name": CDR_SEG1_STEP,
                "application": "roms_marbl",
                "blueprint": {"from_step": FORGE_STEP},
                # after the last other ROMS step: see the docstring
                "depends_on": [FORGE_STEP, CDR_STEP],
                "blueprint_overrides": {"runtime_params": {"end_date": SEGMENT_SPLIT}},
                "compute_overrides": {"local": {"max_walltime": "01:00:00"}},
            },
            {
                "name": CDR_SEG2_STEP,
                "application": "roms_marbl",
                "blueprint": {"from_step": FORGE_STEP},
                "depends_on": [FORGE_STEP, CDR_SEG1_STEP],
                "directives": {"continue-from": {"step": CDR_SEG1_STEP}},
                "compute_overrides": {"local": {"max_walltime": "01:00:00"}},
            },
            {
                "name": METRICS_SEGMENTED_STEP,
                "application": "cdr_metrics",
                "blueprint": InlineBlueprintRef.TOKEN,
                "depends_on": [CDR_SEG1_STEP, CDR_SEG2_STEP],
                "blueprint_overrides": metrics_overrides(CDR_SEG1_STEP, CDR_SEG2_STEP),
                "compute_overrides": {"local": {"max_walltime": "00:10:00"}},
            },
            {
                "name": METRICS_WHOLE_STEP,
                "application": "cdr_metrics",
                "blueprint": InlineBlueprintRef.TOKEN,
                "depends_on": [ROMS_STEP],
                "blueprint_overrides": metrics_overrides(ROMS_STEP),
                "compute_overrides": {"local": {"max_walltime": "00:10:00"}},
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
    """Build the blueprints and workplan, run ``cstar workplan submit``, and poll to the end.

    Returns
    -------
    E2ERun
        The finished run.
    """
    blueprint, blueprint_path = forge_blueprint_factory(
        "unified",
        root / "forge",
        run_time_overrides=MAKE_INPUTS_OVERRIDES,
        cdr={"mode": "simple", "cdr_forcing": MARBL_CDR_FORCING},
    )
    _, cdr_blueprint_path = forge_blueprint_factory(
        "cdr_lite", root / "cdr_forge", name="it-cdr_lite-e2e"
    )
    _warm_grid_downloads(blueprint)
    # lazy: pulls in roms_tools, like the grid warm-up
    from cstar.applications.forge.input_generator import netcdf_basename

    workplan = write_workplan(
        root / "workplan.yaml",
        blueprint_path,
        cdr_blueprint_path,
        netcdf_basename(blueprint.name, "grid"),
    )
    env = make_cli_env(root, shim)

    proc = run_cstar(env, "workplan", "submit", "--run-id", RUN_ID, str(workplan))
    output = f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    if proc.returncode != 0:
        tails = run_log_tails(root / "data", RUN_ID)
        pytest.fail(f"workplan submit exited {proc.returncode}\n{output}\n{tails}")
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
        cdr_seg1_root=step_roots[CDR_SEG1_STEP],
        cdr_seg2_root=step_roots[CDR_SEG2_STEP],
        metrics_segmented_root=step_roots[METRICS_SEGMENTED_STEP],
        metrics_whole_root=step_roots[METRICS_WHOLE_STEP],
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


def _warm_grid_downloads(blueprint: "ForgeBlueprint") -> None:
    """Build ``blueprint``'s grid once so the data roms-tools downloads for it is cached.

    Both forge steps are workplan roots, so they start together and each builds a grid.
    On a cold cache each step's land mask has regionmask download and unzip the Natural
    Earth shapefiles into one shared directory, and that unzip is not safe to run twice
    at once: one step can read a shapefile the other is still rewriting.
    """
    import roms_tools as rt

    rt.Grid(**blueprint.domain.grid_kwargs)


def restart_files(run: E2ERun, step: str = ROMS_STEP) -> list[Path]:
    """The restart files of the ucla_roms ``step``, ordered by their timestamp."""
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
    """All eight sentinels reached ``Done``."""
    assert e2e_run.statuses == ALL_DONE, e2e_run.all_log_tails()


def test_forge_published_blueprint_and_classic_inputs(e2e_run: E2ERun) -> None:
    """Forge published one blueprint, and its input files are CDF-5 classic netCDF."""
    published = list((e2e_run.forge_root / "output").glob("B_*.yaml"))
    assert len(published) == 1, published

    inputs = sorted((e2e_run.forge_root / "input_data").glob("*.nc"))
    assert inputs, "forge wrote no input files"
    headers = {p.name: p.read_bytes()[:4] for p in inputs}
    assert all(h == b"CDF\x05" for h in headers.values()), headers


def _output(proc: subprocess.CompletedProcess[str]) -> str:
    """The captured streams of a finished ``cstar`` process, for assertion messages."""
    return f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


def test_forge_record_lists_its_products(e2e_run: E2ERun) -> None:
    """The forge step record lists its inputs, blueprint, configuration and log."""
    record = read_step_record(e2e_run.forge_root)
    assert record is not None, "forge wrote no step record"

    kinds = {p.kind for p in record.products}
    expected = {
        "emitted_blueprint",
        "grid",
        "initial_conditions",
        "boundary_forcing",
        "surface_forcing",
        "config",
        "log",
    }
    assert expected <= kinds, sorted(kinds)

    roles = {p.metadata["role"] for p in record.products if p.kind == "config"}
    assert roles == {"cppdefs", "namelist", "marbl_in"}

    (grid,) = (p for p in record.products if p.kind == "grid")
    assert "sidecar" in grid.metadata, grid
    empty = [p.path for p in record.products if p.size_bytes <= 0]
    assert not empty, empty


def test_roms_record_lists_its_products(e2e_run: E2ERun) -> None:
    """The ucla_roms step record lists its restarts by stream, its build, configuration and log."""
    record = read_step_record(e2e_run.roms_working_dir)
    assert record is not None, "ucla_roms wrote no step record"

    restarts = [p for p in record.products if p.kind == "restart"]
    assert restarts, [p.kind for p in record.products]
    for product in restarts:
        assert product.metadata["stream"] == "rst", product
        assert TIMESTAMP.match(str(product.metadata["timestamp"])), product
        # the e2e forge blueprints set `use_pio`, so ROMS wrote ParallelIO output
        assert product.metadata["format"] == "pio", product

    kinds = {p.kind for p in record.products}
    assert {"build", "log", "gas_exchange"} <= kinds, sorted(kinds)
    roles = {p.metadata["role"] for p in record.products if p.kind == "config"}
    assert {"namelist", "cppdefs", "marbl_in"} <= roles, roles


def test_show_totals_and_check_finds_no_product_problem(e2e_run: E2ERun) -> None:
    """``cstar run show`` lists the products of the forge, ROMS and ``cdr_metrics``
    steps, and ``cstar run check`` finds every recorded product as recorded.

    The exit code is not asserted: the emitted blueprints and the transformed
    workplan name absolute paths, which the absolute-path check reports.
    """
    proc = run_cstar(e2e_run.env, "run", "show", e2e_run.run_id, "--format", "json")
    assert proc.returncode == 0, _output(proc)
    entries = {entry["name"]: entry for entry in json.loads(proc.stdout)["steps"]}
    for step in (FORGE_STEP, ROMS_STEP, *METRICS_STEPS):
        record = entries[step]["record"]
        assert record is not None, f"no step record for {step}"
        total = sum(product["size_bytes"] for product in record["products"])
        assert total > 0, (step, record["products"])

    proc = run_cstar(e2e_run.env, "run", "check", e2e_run.run_id)
    assert "Skipped products" not in proc.stdout, _output(proc)
    assert not any(h in proc.stdout for h in PRODUCT_HEADINGS), _output(proc)


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


def error_hits(lines: Sequence[str]) -> list[str]:
    """The lines matching ``ERROR_PATTERN``, each with the lines that continue it.

    A logged command failure puts the command's stderr on the lines after the
    ``[ERROR]`` record, so each hit carries the following lines up to the next log
    record, at most ``ERROR_CONTEXT_LINES`` of them.
    """
    hits = []
    for i, line in enumerate(lines):
        if not ERROR_PATTERN.search(line):
            continue
        rest = lines[i + 1 : i + 1 + ERROR_CONTEXT_LINES]
        n = next((j for j, ln in enumerate(rest) if LOG_RECORD_START.match(ln)), None)
        hits.append("\n".join([line.strip(), *rest[:n]]))
    return hits


def test_roms_logs_are_clean(e2e_run: E2ERun) -> None:
    """The ROMS step's stdout shows time stepping ran and contains no error markers."""
    log = e2e_run.roms_root / "logs" / f"{slugify(ROMS_STEP)}.out"
    lines = log.read_text(errors="replace").splitlines()
    assert any("started time-stepping" in line for line in lines), "ROMS never stepped"
    hits = error_hits(lines)
    assert not hits, "\n\n".join(hits[:10])


def gas_exch_files(run: E2ERun) -> list[Path]:
    """The ``_cdrgas`` gas-exchange sensitivity files ``run_roms`` wrote, by timestamp."""
    return sorted((run.roms_working_dir / "output").glob("*_cdrgas*.nc"))


def cdr_namelist(run: E2ERun) -> f90nml.Namelist:
    """The namelist ROMS read in ``run_cdr_lite``, with the directive's inputs added."""
    return f90nml.read(run.cdr_working_dir / "work" / "cstar_generated_roms.nml")


def directive_sensitivity_files(run: E2ERun) -> list[Path]:
    """The ``forcing.carbonate_sensitivity`` locations the directive set for ``run_cdr_lite``, in order."""
    work = run.cdr_root / "work"
    applied = sorted(work.glob("*.csfrom*.yaml"))
    assert applied, sorted(p.name for p in work.iterdir())
    blueprint = yaml.safe_load(applied[-1].read_text())
    data = blueprint["forcing"]["carbonate_sensitivity"]["data"]
    return [Path(d["location"]) for d in data]


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

    assert set(directive_sensitivity_files(e2e_run)) == set(gas_exch_files(e2e_run))


def test_cdr_lite_sensitivity_is_the_recorded_gas_exchange(e2e_run: E2ERun) -> None:
    """The directive's files are, in order, the ``gas_exchange`` products ``run_roms`` recorded.

    The record and the ``output`` directory list the same files here, so this checks
    that the directive and the producer's record agree on real ROMS output.
    """
    record = read_step_record(e2e_run.roms_working_dir)
    assert record is not None, "ucla_roms wrote no step record"
    recorded = [
        (e2e_run.roms_working_dir / p.path).resolve()
        for p in record.products
        if p.kind == "gas_exchange"
    ]
    assert recorded, [p.kind for p in record.products]

    used = [p.resolve() for p in directive_sensitivity_files(e2e_run)]
    assert used == sorted(recorded)


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
    hits = error_hits(lines)
    assert not hits, "\n\n".join(hits[:10])
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


def seconds_since_reference(when: datetime) -> float:
    """``when`` as ROMS time: seconds since ``MODEL_REFERENCE_DATE``."""
    return (when - MODEL_REFERENCE_DATE).total_seconds()


def cdr_output_files(run: E2ERun, step: str) -> list[Path]:
    """The ``_cdr`` output files of the ucla_roms ``step``, ordered by their timestamp."""
    return sorted((run.working_dir_of(step) / "output").glob("*_cdr.[0-9]*.nc"))


def averaging_windows(run: E2ERun, step: str) -> tuple[np.ndarray, np.ndarray]:
    """The ``avg_begin_time`` and ``avg_end_time`` (seconds) of every ``_cdr`` record of ``step``."""
    import xarray as xr

    begins, ends = [], []
    for path in cdr_output_files(run, step):
        with xr.open_dataset(
            path, decode_times=False, decode_timedelta=False, mask_and_scale=False
        ) as ds:
            begins.append(np.atleast_1d(ds["avg_begin_time"].values))
            ends.append(np.atleast_1d(ds["avg_end_time"].values))
    return np.concatenate(begins), np.concatenate(ends)


def max_relative_difference(a: np.ndarray, b: np.ndarray) -> float:
    """The largest ``|a - b| / |b|``, ignoring the records where it is undefined (``0 / 0``, NaN)."""
    with np.errstate(divide="ignore", invalid="ignore"):
        relative = np.abs(a - b) / np.abs(b)
    relative = relative[~np.isnan(relative)]
    return float(relative.max()) if relative.size else 0.0


def test_cdr_output_written(e2e_run: E2ERun) -> None:
    """The whole run and both segments wrote ``_cdr`` output, one record per window,
    and listed it in their step records for ``cdr_metrics`` to read by kind.
    """
    records = {}
    for step in CDR_OUTPUT_STEPS:
        root = e2e_run.working_dir_of(step)
        output = root / "output"
        files = cdr_output_files(e2e_run, step)
        assert files, sorted(p.name for p in output.glob("*"))
        assert product_paths(root, ArtifactKind.cdr_output) == files, step
        records[step] = len(averaging_windows(e2e_run, step)[0])
    assert records == {
        ROMS_STEP: METRICS_RECORDS,
        CDR_SEG1_STEP: METRICS_RECORDS // 2,
        CDR_SEG2_STEP: METRICS_RECORDS // 2,
    }


def test_segments_meet_at_restart(e2e_run: E2ERun) -> None:
    """``run_cdr_seg2`` continues where ``run_cdr_seg1`` ended: the chain covers the run without gap or overlap."""
    begin1, end1 = averaging_windows(e2e_run, CDR_SEG1_STEP)
    begin2, end2 = averaging_windows(e2e_run, CDR_SEG2_STEP)
    # an absolute tolerance of 1 ms: the default relative one is minutes at these times
    approx = partial(pytest.approx, rel=0, abs=1e-3)
    assert begin1[0] == approx(seconds_since_reference(RUN_START))
    assert end1[-1] == approx(seconds_since_reference(SEGMENT_SPLIT))
    assert begin2[0] == approx(end1[-1])
    assert end2[-1] == approx(seconds_since_reference(RUN_END))
    # every window starts where the previous one ended
    assert np.concatenate([begin1, begin2])[1:] == approx(
        np.concatenate([end1, end2])[:-1]
    )


def test_cdr_metrics_written(e2e_run: E2ERun) -> None:
    """Both ``cdr_metrics`` steps wrote the figure and a series of positive CO2 uptake, one value per window, and recorded both as products."""
    import xarray as xr

    for step in METRICS_STEPS:
        output = e2e_run.metrics_dir_of(step)
        record = read_step_record(output.parent)
        assert record is not None, f"{step} wrote no step record"
        analyses = [p for p in record.products if p.kind == "analysis"]
        figures = [p for p in record.products if p.kind == "figure"]
        assert [(p.path, p.metadata) for p in analyses] == [
            (f"output/{METRICS_FILENAME}", {"role": "cdr_metrics"})
        ], (step, record.products)
        # one domain: the map of the domains is only written for nested ones
        assert [(p.path, p.metadata) for p in figures] == [
            (f"output/{FIGURE_FILENAME}", {"role": "metrics"})
        ], (step, record.products)
        assert all(p.size_bytes > 0 for p in (*analyses, *figures)), (
            step,
            record.products,
        )

        assert (output / FIGURE_FILENAME).is_file(), sorted(
            p.name for p in output.glob("*")
        )
        with xr.open_dataset(output / METRICS_FILENAME) as ds:
            assert set(METRICS_VARIABLES) <= set(ds.data_vars), list(ds.data_vars)
            not_series = {
                n: v.dims for n, v in ds.data_vars.items() if v.dims != ("time",)
            }
            assert not not_series, (step, not_series)
            assert ds.sizes["time"] == METRICS_RECORDS, (step, dict(ds.sizes))
            for name in UPTAKE_VARIABLES:
                final = float(ds[name].values[-1])
                assert np.isfinite(final) and final > 0, (step, name, ds[name].values)


def assert_segmented_metrics_match_whole_run(run: E2ERun, names: Sequence[str]) -> None:
    """Assert the ``names`` series of the two continued segments equal the whole run's."""
    import xarray as xr

    paths = [run.metrics_dir_of(s) / METRICS_FILENAME for s in METRICS_STEPS]
    with xr.open_dataset(paths[0]) as segmented, xr.open_dataset(paths[1]) as whole:
        differences = {
            name: max_relative_difference(segmented[name].values, whole[name].values)
            for name in names
        }
        for name in names:
            # the efficiencies are NaN where the cumulative source is zero
            np.testing.assert_allclose(
                segmented[name].values,
                whole[name].values,
                rtol=1e-6,
                equal_nan=True,
                err_msg=f"{name}: max relative differences {differences}",
            )


def test_segmented_dic_metrics_match_whole_run(e2e_run: E2ERun) -> None:
    """The DIC-inventory metrics of the two continued segments equal the whole run's.

    The restarted model state is bit-identical to the uninterrupted run's.
    """
    names = [n for n in METRICS_VARIABLES if n.endswith("_from_DIC_difference")]
    assert_segmented_metrics_match_whole_run(e2e_run, names)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "observed with ucla-roms 0.10.1 (macOS, ubuntu) and 0.11.0 (ubuntu): the "
        "averaged FG_CO2/FG_ALT_CO2 written after a continue-from restart differ from "
        "an uninterrupted run's (up to 2e-6 mmol/m^2/s) while DIC, ALK and hDIC stay "
        "bit-identical and the namelists differ only in inifile and ntimes, so "
        "flux-based metrics of a segmented run differ"
    ),
)
def test_segmented_flux_metrics_match_whole_run(e2e_run: E2ERun) -> None:
    """The air-sea flux metrics of the two continued segments equal the whole run's."""
    names = [n for n in METRICS_VARIABLES if n.endswith("_from_flux")]
    assert_segmented_metrics_match_whole_run(e2e_run, names)


def test_workplan_status_names_all_steps(e2e_run: E2ERun) -> None:
    """``cstar workplan status`` succeeds and shows every step as done."""
    # a wide terminal keeps rich from truncating step names
    proc = run_cstar({**e2e_run.env, "COLUMNS": "200"}, "run", "status", e2e_run.run_id)
    assert proc.returncode == 0, proc.stderr
    for step in STEPS:
        assert step in proc.stdout
    assert proc.stdout.count("\u2714") == len(STEPS), proc.stdout


def test_resume_of_completed_run_keeps_steps_done(e2e_run: E2ERun) -> None:
    """Resuming a finished run succeeds and leaves every step ``Done``."""
    proc = run_cstar(
        e2e_run.env, "workplan", "submit", "--run-id", e2e_run.run_id, "--resume"
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
