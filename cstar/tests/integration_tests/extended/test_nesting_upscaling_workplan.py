"""Tier 3: the tiny nesting + upscaling example workplan run end to end through the ``cstar`` CLI.

The workplan is ``docs/tutorials/nesting_upscaling_tiny/workplan.yaml``: a parent run that
extracts boundary data for a nested child, the child's initial state regridded from the
parent's restart, a child run with an alkalinity release (CDR) that writes the boundary
fluxes of the CDR tracers, the upscaler turning those into a CDR forcing file for the
parent grid, a parent rerun that reads it, and the CDR metrics of the child and the rerun
combined. It is nine steps and compiles ROMS with ParallelIO three times, so a run takes
about ten minutes on a 10-core laptop.

As in the e2e tier, ``cstar workplan submit`` only schedules: each step is a detached
``sh`` proxy that calls the ``cstar`` executable on ``PATH``, and a shim pins that to
this checkout. The module polls the step sentinels until all nine are terminal.
"""

import re
import shutil
import time
import typing as t
import warnings
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest
import yaml

from cstar.applications.cdr_metrics import (
    DOMAINS_FIGURE_FILENAME,
    FIGURE_FILENAME,
    METRICS_FILENAME,
)
from cstar.applications.upscaler import (
    USCL_WINDOWS_FROM_BOUNDS,
    USCL_WINDOWS_INFERRED,
)
from cstar.base.utils import slugify
from cstar.orchestration.launch.local import find_timeout_exe
from cstar.roms.namelist import RomsNamelistV0_13_0, namelist_schema_for_ref
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

EXAMPLE_DIR = REPO_ROOT / "docs" / "tutorials" / "nesting_upscaling_tiny"
WORKPLAN = EXAMPLE_DIR / "workplan.yaml"
RUN_ID = "nesting-upscaling-it"
SCHEDULED_MESSAGE = "run scheduling has completed"
FORGE_STEPS = ("forge_parent", "forge_child", "forge_parent_up")
ROMS_STEPS = ("parent", "child", "parent_up")
STEPS = (
    *FORGE_STEPS,
    "parent",
    "nest_ic",
    "child",
    "upscale",
    "parent_up",
    "cdr_metrics",
)
ALL_DONE = dict.fromkeys(STEPS, DONE)
POLL_INTERVAL = 5.0
RUN_TIMEOUT = 60 * 60
ROMS_FAILURE_PATTERN = re.compile(
    r"\bERROR \["
    r"|BLOWUP"
    r"|MPI_(?:ABORT|Abort)"
    r"|Ran out of time records"
    r"|First available forcing record is past current time"
)
"""ROMS failure markers in a step log, which also carries C-Star's own logging.

ucla-roms prints ``ERROR [raised ...] [module/routine]:`` (``ERROR`` then a bracket) and then
calls ``MPI_Abort``; a blow-up prints ``BLOWUP``. C-Star's logger writes its level as
``[ERROR]``, so a transient ``[ERROR] ... An error occurred while refreshing ...git`` (C-Star
carries on after it, logging the exception with a traceback) does not match. A
``Traceback`` is not a marker for that reason; a real exception fails the step's status.
"""
CDF5_MAGIC = b"CDF\x05"
COMBINED_SERIES = (
    "cdr_carbon_uptake_from_flux",
    "cdr_carbon_uptake_from_DIC_difference",
    "cdr_efficiency_from_flux",
    "closure_residual",
)
"""The ``cdr_metrics.nc`` series that combine the child and ``parent_up``."""
CDR_DOMAINS = ("child", "parent_up")
"""The ``cdr_metrics`` step's domains, release first, as the workplan names them."""
UPTAKE_SERIES = ("carbon_uptake_from_flux", "carbon_uptake_from_DIC_difference")
"""The uptake series ``cdr_metrics.nc`` holds per domain (``<domain>_<series>``) and combined (``cdr_<series>``)."""
FOOTPRINT_SERIES = "parent_up_DIC_difference_inside_child"
"""The DIC perturbation ``parent_up`` carries inside the child, which the combined uptake would count twice."""
MAX_CLOSURE_FRACTION = 1e-3
"""The largest share of the final flux-based uptake the closure residual may be.

The two uptakes are two sums over one carbon budget once the child's ``_uscl`` export
holds the mixing flux (ucla-roms #399) and each domain's metrics leave out its boundary
rows and columns (roms-tools 5.3.0), so the residual is round-off: 7e-6 on this example.
"""
NO_MIXING_EXPORT_REFS = ("0.11.0", "0.12.0")
"""ucla-roms releases whose ``_uscl`` export leaves out the mixing flux (before #399)."""
NO_MIXING_EXPORT_MAX_CLOSURE_FRACTION = 0.15
"""`MAX_CLOSURE_FRACTION` for `NO_MIXING_EXPORT_REFS`: the missing flux is +9.8 % here."""
CONSERVATION_RTOL = 1e-3
"""How far ``parent_up``'s injected alkalinity may be from the upscaled file's flux."""
REGISTRY_SOURCES = {
    "GLORYS": "GLORYS_NA_2012.nc",
    "ERA5": "ERA5_NA_2012.nc",
    "UNIFIED": "coarsened_UNIFIED_bgc_dataset_v2_1.nc",
}
"""The pinned registry file that stands in for each source the example's Forge downloads."""
REGISTRY_GLORYS_START = "2012-01-01T12:00:00"
"""The first record of the registry's GLORYS extract, where a run with initial conditions starts."""
USCL_BOUNDARIES = ("north", "south", "east", "west")
"""The sides of the child that can carry an ``ALK_add_<side>`` export in ``_uscl``."""
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


def with_registry_sources(
    blueprint: dict[str, t.Any], data: dict[str, Path]
) -> dict[str, t.Any]:
    """Point ``blueprint``'s forcing sources at the pinned registry files.

    The example's blueprints leave every source path empty, so Forge downloads GLORYS
    (which needs a Copernicus Marine login) and UNIFIED and streams ERA5. CI has neither
    the login nor the time, so each source gets its ``REGISTRY_SOURCES`` file, and the
    sources leave ``resolved_datasets`` and ``datasets`` as the resolver leaves them for
    a source with a path: Forge stages every dataset ``datasets`` lists, path or not.

    Copernicus stamps a GLORYS daily mean at midnight, where the example's parent runs
    start, but the extract has only a noon record, so a blueprint with initial conditions
    starts at ``REGISTRY_GLORYS_START`` instead, with ``ntimes`` to match.

    Returns
    -------
    dict
        ``blueprint``, changed in place.
    """
    forcing = blueprint["forcing"]
    blocks = [forcing[k] for k in ("initial_conditions", "boundary") if forcing.get(k)]
    sources = [block["source"] for block in [*blocks, *forcing["surface"]]]
    sources += [bgc["source"] for block in blocks for bgc in block["bgc_sources"]]
    for source in sources:
        source["path"] = str(data[REGISTRY_SOURCES[source["name"]]])
    forcing["resolved_datasets"] = {
        name: dataset
        for name, dataset in forcing["resolved_datasets"].items()
        if name not in REGISTRY_SOURCES
    }
    blueprint["datasets"] = sorted(
        dataset["dataset_key"] for dataset in forcing["resolved_datasets"].values()
    )
    if forcing.get("initial_conditions"):
        run = blueprint["run"]
        run["start_date"] = REGISTRY_GLORYS_START
        length = datetime.fromisoformat(run["end_date"]) - datetime.fromisoformat(
            REGISTRY_GLORYS_START
        )
        stepping = blueprint["model_settings"]["time_stepping"]
        stepping["ntimes"] = round(length.total_seconds() / stepping["dt"])
    return blueprint


def without_v_sponge_trc_before_0_13(
    blueprint: dict[str, t.Any], roms_ref: str
) -> dict[str, t.Any]:
    """Drop ``v_sponge_trc`` from ``blueprint`` when ``roms_ref`` predates ucla-roms 0.13.0.

    The example's child sets ``v_sponge_trc: 0.0`` (a sponge on currents only), a key
    that releases before 0.13.0 stop on and that Forge rejects for them. A ref below that
    tier runs the full sponge, as the example did before the key existed, so the key goes
    from ``model_settings.v_sponge`` and from ``composition.overrides``. The tier is the
    one C-Star selects for the ref, so a branch such as ``main`` (the newest tier) keeps it.

    Returns
    -------
    dict
        ``blueprint``, changed in place.
    """
    # A branch or commit-hash ref selects the newest tier here, so it keeps the key: it must
    # contain the ucla-roms change that adds v_sponge_trc, or ROMS (or Forge, for a hash
    # below 0.13.0) stops on it.
    with warnings.catch_warnings():  # a branch ref warns that it is not a release tag
        warnings.simplefilter("ignore")
        schema = namelist_schema_for_ref(roms_ref)
    if issubclass(schema, RomsNamelistV0_13_0):
        return blueprint
    blueprint["model_settings"].get("v_sponge", {}).pop("v_sponge_trc", None)
    overrides = blueprint.get("composition", {}).get("overrides", {})
    if "v_sponge" in overrides:
        overrides["v_sponge"].pop("v_sponge_trc", None)
        if not overrides["v_sponge"]:
            del overrides["v_sponge"]
    return blueprint


def write_workplan(path: Path, data: dict[str, Path]) -> Path:
    """Write a copy of the example workplan and its forge blueprints for the run under test.

    The example's forge blueprints come from the bundled ``roms-marbl-0.13-default``
    ModelSpec, so they build ucla-roms 0.13.0 unless ``CSTAR_IT_ROMS_REF`` (default
    ``PINNED_ROMS_REF``) says otherwise; a ref below 0.13.0 loses the child's
    ``v_sponge_trc`` (`without_v_sponge_trc_before_0_13`). The weekly workflow sets
    ``CSTAR_IT_ROMS_REF`` to each ref it covers (0.12.0 and main), so every forge step
    adds ``code.roms.commit`` to its ``blueprint_overrides``: the published blueprints,
    and with them the ROMS the next steps build, are for that ref. The blueprint copies
    sit next to the workplan copy and read the pinned registry data
    (`with_registry_sources`).

    Returns
    -------
    Path
        ``path``.
    """
    workplan = yaml.safe_load(WORKPLAN.read_text())
    for step in workplan["steps"]:
        if step["application"] != "forge":
            continue
        name = Path(step["blueprint"]).name
        blueprint = yaml.safe_load((EXAMPLE_DIR / name).read_text())
        copy = path.parent / name
        copy.write_text(
            yaml.safe_dump(
                without_v_sponge_trc_before_0_13(
                    with_registry_sources(blueprint, data), ROMS_REF
                ),
                sort_keys=False,
            )
        )
        step["blueprint"] = str(copy)
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

    Prints the wall time from ``workplan submit`` to the last step's sentinel; ``-s``
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
    """Run ``cstar workplan submit`` on the example and poll the sentinels to the end.

    Returns
    -------
    NestingRun
        The finished run.
    """
    workplan = write_workplan(root / "workplan.yaml", integration_test_data)
    env = make_cli_env(root, shim)

    started = time.monotonic()
    proc = run_cstar(env, "workplan", "submit", str(workplan), "--run-id", RUN_ID)
    data_home = root / "data"
    output = f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    if proc.returncode != 0:
        tails = run_log_tails(data_home, RUN_ID)
        pytest.fail(f"workplan submit exited {proc.returncode}\n{output}\n{tails}")
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


def final_record(path: Path, names: Sequence[str]) -> dict[str, float]:
    """The value of each series ``names`` of the netCDF file ``path`` at its last record."""
    import xarray as xr

    with xr.open_dataset(path) as ds:
        return {name: float(ds[name].values[-1]) for name in names}


def alk_profile(ds: "xr.Dataset") -> "xr.DataArray":
    """The ALK flux of ``ds``'s ``cdr_trcflx_profile``, tracer 0 of its third axis.

    The axis is named ``two``, or ``ntrc`` when the file also holds CDR-lite tracers, so
    it is indexed by position.
    """
    profile = ds["cdr_trcflx_profile"]
    return profile.isel({profile.dims[2]: 0})


def _alk_source_total(path: Path) -> "np.ndarray":
    """The ``ALK_source`` of the ``_cdr`` file ``path`` summed over the domain, per record."""
    with open_dataset(path) as ds:
        return ds["ALK_source"].sum(("s_rho", "eta_rho", "xi_rho"), skipna=False).values


def child_export(nesting_run: NestingRun) -> "np.ndarray":
    """The child's ALK export at each ``_uscl`` record, summed over its open boundaries.

    The sum runs over every dimension but time, of ``ALK_add_<side>`` for each side the
    child writes. These are the per-cell rates in meq/s that the upscaler copies into
    ``cdr_trcflx_profile``, so the sum is comparable with ``alk_profile`` summed over
    its depth and profile axes. The files are read as the upscaler reads them, with
    fill values masked.
    """
    import xarray as xr

    records = []
    for path in nesting_run.outputs("child", "*_uscl.*.nc"):
        with xr.open_dataset(path) as ds:
            exports = [
                ds[name].sum([d for d in ds[name].dims if d != "time"], skipna=False)
                for name in (f"ALK_add_{side}" for side in USCL_BOUNDARIES)
                if name in ds
            ]
            assert exports, f"{path.name} has no ALK_add_<side> variable"
            records.append(sum(exports).values)
    return np.concatenate(records)


def child_uscl_times(nesting_run: NestingRun) -> dict[str, "np.ndarray"]:
    """The child's ``_uscl`` ``ocean_time`` (s), and ``avg_begin_time``/``avg_end_time`` if the files carry them.

    Each series is concatenated over the files, in order, as the upscaler reads them. The
    window bounds are left out for the files of ucla-roms 0.12.0 and earlier.
    """
    names = ("ocean_time", "avg_begin_time", "avg_end_time")
    series: dict[str, list[np.ndarray]] = {}
    for path in nesting_run.outputs("child", "*_uscl.*.nc"):
        with open_dataset(path) as ds:
            for name in names:
                if name in ds.variables:
                    series.setdefault(name, []).append(ds[name].values)
    return {name: np.concatenate(values) for name, values in series.items()}


def child_start_days() -> float:
    """The child's start, in days since its model reference date, from its forge blueprint.

    The upscaler labels ``cdr_time`` "days since 2000/01/01" but writes the child's model
    time (``ocean_time`` / 86400), which counts from the model reference date, so that
    is the epoch to use.
    """
    run = yaml.safe_load((EXAMPLE_DIR / "forge_child.yaml").read_text())["run"]
    start = datetime.fromisoformat(run["start_date"])
    epoch = datetime.fromisoformat(run["model_reference_date"])
    return (start - epoch).total_seconds() / 86400


def test_all_steps_done(nesting_run: NestingRun) -> None:
    """All nine sentinels reached ``Done``."""
    assert nesting_run.statuses == ALL_DONE, run_log_tails(
        nesting_run.data_home, RUN_ID
    )


def test_workplan_status_names_all_steps(nesting_run: NestingRun) -> None:
    """``cstar run status`` succeeds and shows every step as done."""
    # a wide terminal keeps rich from truncating step names
    proc = run_cstar({**nesting_run.env, "COLUMNS": "200"}, "run", "status", RUN_ID)
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
    """``cdr_time`` increases from the child's start, and the ALK flux is finite, non-zero and closed by a zero record."""
    (path,) = nesting_run.outputs("upscale", "upscaled_cdr.nc")
    with open_dataset(path) as ds:
        cdr_time = ds["cdr_time"].values
        alk_flux = alk_profile(ds).values
    assert np.all(np.diff(cdr_time) > 0), cdr_time
    # cdr_time holds the start of each hour the flux covers, so the first is the child's
    # start; a 1e-6 day tolerance is 0.09 s, where the default relative one is minutes
    assert cdr_time[0] == pytest.approx(child_start_days(), rel=0, abs=1e-6)
    assert np.isfinite(alk_flux).all()
    assert np.abs(alk_flux).max() > 0
    # the file ends with a closing record of zeros: cdr_time[-1] is the end of the last hour
    assert not alk_flux[-1].any()


def test_parent_up_wrote_cdr_source(nesting_run: NestingRun) -> None:
    """Each ``_cdr`` file of ``parent_up`` has a finite ``ALK_source`` with a positive maximum."""
    files = nesting_run.outputs("parent_up", "*_cdr.*.nc")
    assert files, nesting_run.root("parent_up")
    for path in files:
        with open_dataset(path) as ds:
            source = ds["ALK_source"].values
        assert np.isfinite(source).all(), f"{path.name}: ALK_source not finite"
        assert source.max() > 0, f"{path.name}: ALK_source is never positive"


def test_parent_up_injects_the_upscaled_alk_flux(nesting_run: NestingRun) -> None:
    """``parent_up``'s ``ALK_source``, summed over the domain, matches the file's ALK flux record by record.

    ``parent_up`` reads ``upscaled_cdr.nc`` without time interpolation: ROMS holds each
    hourly record from its time stamp. The upscaler stamps each record at the start of
    the hour it covers, so every record but the last is applied in its own hour. The last
    is the all-zero closing record, which ends the final hour, and ``parent_up`` stops
    there. So the injected alkalinity is compared with every record but the last. Both
    are hourly rates in meq/s. Each record must agree to ``CONSERVATION_RTOL``, with an
    absolute floor of that fraction of the largest flux because hours with next to no
    flux, such as the first hours after the release starts, have no usable ratio.

    ucla-roms 0.11.0 and later conserve the flux (``remap_src_to_grid`` rescaled it
    before, so 0.10.1 injects about 1 % less here), which this checks. That the file's
    records are the right hours of the child's export is checked by
    ``test_parent_up_injects_the_childs_export_in_the_same_hour``.
    """
    (upscaled,) = nesting_run.outputs("upscale", "upscaled_cdr.nc")
    with open_dataset(upscaled) as ds:
        flux = alk_profile(ds).sum(("s_rho", "ncdr_prof"), skipna=False).values
    injected = np.concatenate(
        [
            _alk_source_total(path)
            for path in nesting_run.outputs("parent_up", "*_cdr.*.nc")
        ]
    )

    # parent_up runs to the child's end: one hourly record for each flux record but the closing one
    assert len(injected) == len(flux) - 1, (len(injected), len(flux))
    np.testing.assert_allclose(
        injected,
        flux[:-1],
        rtol=CONSERVATION_RTOL,
        atol=CONSERVATION_RTOL * np.abs(flux).max(),
    )


def test_parent_up_injects_the_childs_export_in_the_same_hour(
    nesting_run: NestingRun,
) -> None:
    """In each hour, ``parent_up`` injects the alkalinity the child exported in that hour.

    This ties ``parent_up`` to the child's own ``_uscl`` output, which the record-by-record
    comparison with ``upscaled_cdr.nc`` cannot: that one passes when the upscaler stamps
    every record an hour late. ``ALK_add_<side>`` is the child's export across each side,
    summed here over all its cells and depths. A ``_uscl`` record is the average over the
    hour that ends at its stamp, and ucla-roms 0.12.0 and earlier also write a start-up
    sample before the first hour. The alignment is decided from the child's own ``_uscl``
    files, not from what the upscaler wrote about them:

    - no ``avg_begin_time`` in ``_uscl``: the first record is the start-up sample, so
      record ``j + 1`` is hour ``j``, and ``cdr_time`` is the files' ``ocean_time``.
    - ``avg_begin_time``/``avg_end_time`` in ``_uscl``: there is no start-up sample, so
      record ``j`` is hour ``j``, and ``cdr_time`` is the window starts then the last end.

    ``upscaled_cdr.nc`` must agree on both counts: its ``uscl_windows`` attribute names
    the same alignment, and its ``cdr_time`` is the one given above, within a 1e-6 day
    tolerance. Every hour must then agree to ``CONSERVATION_RTOL``, with the same
    absolute floor as the record-by-record test, and so must the totals. A one-hour lag
    would not: the export changes from hour to hour while the release is on.
    """
    times = child_uscl_times(nesting_run)
    has_bounds = "avg_begin_time" in times
    first_window_record = 0 if has_bounds else 1
    (upscaled,) = nesting_run.outputs("upscale", "upscaled_cdr.nc")
    with open_dataset(upscaled) as ds:
        windows = ds.attrs["uscl_windows"]
        cdr_time = ds["cdr_time"].values

    assert windows == (
        USCL_WINDOWS_FROM_BOUNDS if has_bounds else USCL_WINDOWS_INFERRED
    )
    if has_bounds:
        expected_time = np.append(times["avg_begin_time"], times["avg_end_time"][-1])
    else:
        expected_time = times["ocean_time"]
    assert len(cdr_time) == len(expected_time), (len(cdr_time), len(expected_time))
    np.testing.assert_allclose(cdr_time, expected_time / 86400, rtol=0, atol=1e-6)

    injected = np.concatenate(
        [
            _alk_source_total(path)
            for path in nesting_run.outputs("parent_up", "*_cdr.*.nc")
        ]
    )
    exported = child_export(nesting_run)[first_window_record:]

    assert len(injected) == len(exported), (windows, len(injected), len(exported))
    assert np.abs(exported).max() > 0
    atol = CONSERVATION_RTOL * np.abs(exported).max()
    np.testing.assert_allclose(injected, exported, rtol=CONSERVATION_RTOL, atol=atol)
    assert injected.sum() == pytest.approx(exported.sum(), rel=CONSERVATION_RTOL)


def test_cdr_metrics_wrote_its_outputs(nesting_run: NestingRun) -> None:
    """``cdr_metrics`` wrote the combined metrics and the two figures."""
    for name in (METRICS_FILENAME, FIGURE_FILENAME, DOMAINS_FIGURE_FILENAME):
        assert nesting_run.outputs("cdr_metrics", name), name


def test_cdr_metrics_assigns_the_source_roles(nesting_run: NestingRun) -> None:
    """``cdr_metrics.nc`` records the child as the release and ``parent_up`` as a transfer.

    A swapped or dropped role would count the release twice, or not at all, in the
    combined series.
    """
    (path,) = nesting_run.outputs("cdr_metrics", METRICS_FILENAME)
    with open_dataset(path) as ds:
        assert ds.attrs["domains"] == "child:release, parent_up:transfer"


def test_cdr_metrics_are_finite_at_the_final_record(nesting_run: NestingRun) -> None:
    """The series that combine the child and ``parent_up`` have a finite last value.

    Both domains need a record at the last time for that, which holds only because
    ``parent_up`` runs to the child's end.
    """
    (path,) = nesting_run.outputs("cdr_metrics", METRICS_FILENAME)
    final = final_record(path, COMBINED_SERIES)
    assert all(np.isfinite(value) for value in final.values()), final


def test_cdr_metrics_close_at_the_final_record(nesting_run: NestingRun) -> None:
    """The closure residual is a small share of the flux-based CO2 uptake at the last time.

    The residual is the combined flux-based uptake minus the combined DIC-difference
    uptake, two estimates of the same CO2 that agree when the domains account for the
    release consistently. A ucla-roms release without the mixing flux in its export gets
    the looser bound.
    """
    (path,) = nesting_run.outputs("cdr_metrics", METRICS_FILENAME)
    final = final_record(path, COMBINED_SERIES)
    uptake = final["cdr_carbon_uptake_from_flux"]
    bound = (
        NO_MIXING_EXPORT_MAX_CLOSURE_FRACTION
        if ROMS_REF in NO_MIXING_EXPORT_REFS
        else MAX_CLOSURE_FRACTION
    )
    assert uptake > 0, final
    assert abs(final["closure_residual"]) / uptake < bound, final


def test_cdr_metrics_keep_each_domains_series(nesting_run: NestingRun) -> None:
    """``cdr_metrics.nc`` holds each domain's uptake and the parent's DIC inside the child, finite at the last time.

    These come from roms-tools' ``UpscaledCDR``, so they show the step combined the
    two domains rather than analysing one of them.
    """
    (path,) = nesting_run.outputs("cdr_metrics", METRICS_FILENAME)
    names = [f"{domain}_{series}" for domain in CDR_DOMAINS for series in UPTAKE_SERIES]
    final = final_record(path, [*names, FOOTPRINT_SERIES])
    assert all(np.isfinite(value) for value in final.values()), final
    assert final["child_carbon_uptake_from_flux"] > 0, final


@pytest.mark.parametrize("series", UPTAKE_SERIES)
def test_cdr_metrics_combined_uptake_is_the_domain_sum(
    nesting_run: NestingRun, series: str
) -> None:
    """At every record, the combined uptake is the child's plus ``parent_up``'s.

    ``parent_up`` is a transfer domain: its uptake adds to the total, while its source
    stays out of the efficiency's denominator. Both domains run from the child's start
    to its end, so every record is complete and finite.
    """
    import xarray as xr

    (path,) = nesting_run.outputs("cdr_metrics", METRICS_FILENAME)
    with xr.open_dataset(path) as ds:
        combined = ds[f"cdr_{series}"].values
        parts = [ds[f"{domain}_{series}"].values for domain in CDR_DOMAINS]
    assert np.isfinite(combined).all(), combined
    np.testing.assert_allclose(combined, sum(parts), rtol=1e-12)


@pytest.mark.parametrize("step", ROMS_STEPS)
def test_roms_logs_are_clean(nesting_run: NestingRun, step: str) -> None:
    """The ROMS step's stdout shows time stepping ran and contains no ROMS failure markers."""
    lines = nesting_run.log_text(step).splitlines()
    assert any("started time-stepping" in line for line in lines), "ROMS never stepped"
    hits = [line.strip() for line in lines if ROMS_FAILURE_PATTERN.search(line)]
    assert not hits, hits[:10]
