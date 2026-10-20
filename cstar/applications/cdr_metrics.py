"""Compute CDR (carbon dioxide removal) metrics from ROMS-MARBL `_cdr` output.

The application reads the `_cdr` output (``<root>_cdr.<YYYYMMDDHHMMSS>.nc``) of
one or more ROMS-MARBL runs, computes the CDR metrics with roms-tools'
``compute_cdr_metrics``, and writes the resulting time series and figures to
``<working_dir>/output``:

- ``cdr_metrics.nc``: the 1-D time series (uptake efficiencies, cumulative
  uptake in tonnes CO2, averaging window length). For nested domains, the
  combined series and each domain's uptake.
- ``cdr_metrics.png``: roms-tools' "CO2 uptake and CDR efficiency" figure.
- ``cdr_domains.png``: for nested domains, a map of the domain outlines.

A run may be split into segments (e.g. chained with ``continue-from``), each
writing its own `_cdr` files; the application stitches them along time. With
ucla-roms 0.10.1 the averaged air-sea flux diagnostics written after a restart
differ from an uninterrupted run's, so only the DIC-difference metrics of a
segmented run reproduce the unsegmented run exactly. The blueprint maps
a domain name to the domain's ROMS grid file and the ordered sources of its
`_cdr` output::

    application: cdr_metrics
    name: wio-cdr-metrics
    description: CDR metrics for the western Indian Ocean release.
    domains:
      wio:
        grid:
          location: input/wio_grid.nc
        sources:
          - location: runs/segment-01/output
          - location: runs/segment-02/output

``domains`` is a mapping, not a list, so that an inline workplan step can set
it through its ``blueprint_overrides``, which do not accept a top-level list
of mappings.

Each source is a directory searched recursively for `_cdr` output, or a single
`_cdr` file. The first source must hold the segment containing the release,
because the cumulative sums start at the first record. Records are sorted by
time after loading, and a gap or overlap between the averaging windows of
consecutive records is an error. In a workplan, a source can be a
``{{output_dir: <step>}}`` placeholder naming the producing step.

A nested (upscaled) experiment lists the domains child first. The child holds
the release; each parent receives the child's boundary perturbation as its
source and has ``source_role: transfer``::

    domains:
      child:
        grid:
          location: input/child_grid.nc
        sources:
          - location: runs/child/output
      parent:
        source_role: transfer
        grid:
          location: input/parent_grid.nc
        sources:
          - location: runs/parent/output

The domains are combined with roms-tools' ``UpscaledCDR`` (uptake summed over
the domains, efficiency normalised by the release domains' cumulative source),
which also writes ``cdr_domains.png``. Every domain must be a MARBL run writing
`_cdr` output: a CDR-lite parent (which writes `_cdrtrc`) cannot be analysed.
"""

import contextlib
import functools
import typing as t
import warnings
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path

import numpy as np
from pydantic import Field, field_validator

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerResult,
    register_application,
)
from cstar.applications.roms_marbl.transforms import (
    TIMESTAMP_GLOB,
    TimestampedOutputFile,
)
from cstar.base.log import get_logger
from cstar.base.utils import lazy_import
from cstar.entrypoint.runner import BlueprintRunner
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Blueprint, ConfiguredBaseModel, RequiredString

if t.TYPE_CHECKING:
    from types import ModuleType

    import xarray as xr

roms_tools = lazy_import("roms_tools")

APP_NAME: t.Final[str] = "cdr_metrics"
"""The unique identifier for the CDR metrics application type."""
_APP_NAME_LONG: t.Final[str] = "CDR Metrics"
"""The long-form application name."""

METRICS_FILENAME: t.Final[str] = "cdr_metrics.nc"
"""The name of the metrics time series file, written to the step's `output` directory."""
FIGURE_FILENAME: t.Final[str] = "cdr_metrics.png"
"""The name of the metrics figure, written to the step's `output` directory."""
DOMAINS_FIGURE_FILENAME: t.Final[str] = "cdr_domains.png"
"""The name of the map of nested domain outlines, written for two or more domains."""

_CDR_OUTPUT_HINT: t.Final[str] = (
    "The producing run must be a MARBL run with CDR forcing and "
    "`cdr_output.do_cdr_output` set (forge turns it on for an active MARBL CDR forcing)."
)
"""Appended to not-found errors so users know what a run needs to write `_cdr` output."""

_AVG_SETTING: t.Final[str] = (
    "`cdr_output.do_cdr_output` and `cdr_output.do_avg` "
    "(window times are only written for averaged output)"
)
_MARBL_SETTING: t.Final[str] = (
    "`cdr_output.do_cdr_output` on a MARBL build with CDR forcing "
    "(a CDR-lite parent writes `_cdrtrc`, not `_cdr`, output)"
)
_SOURCE_SETTING: t.Final[str] = (
    "`cdr_output.do_cdr_output` with an active CDR forcing (`cdr_frc.cdr_source`)"
)
_RELEASE_CONSTRAINT_MARKER: t.Final[str] = "release constraints"
"""The phrase roms-tools' `compute_cdr_metrics` uses when a release's source has
the wrong sign; the error then gets a `source_sign_report`."""

_REQUIRED_VARIABLES: t.Final[dict[str, str]] = {
    "avg_begin_time": _AVG_SETTING,
    "avg_end_time": _AVG_SETTING,
    "FG_CO2": _MARBL_SETTING,
    "FG_ALT_CO2": _MARBL_SETTING,
    "hDIC": _MARBL_SETTING,
    "hDIC_ALT_CO2": _MARBL_SETTING,
    "ALK_source": _SOURCE_SETTING,
    "DIC_source": _SOURCE_SETTING,
}
"""The variables roms-tools' `compute_cdr_metrics` reads from `_cdr` output, each
with the ROMS setting that writes it."""

log = get_logger(__name__)


class CdrOutputFile(TimestampedOutputFile):
    """Reference to a ROMS-MARBL CDR output file, `<base>_cdr.<timestamp>.nc`."""

    SUFFIX: t.ClassVar[str] = "_cdr"
    """A unique suffix found in the name of CDR output files."""
    LABEL: t.ClassVar[str] = "CDR output"

    @classmethod
    def _search_glob(cls) -> str:
        """Return a glob requiring the timestamp right after the suffix.

        `_cdr` is a prefix of the suffixes of sibling ROMS output (`_cdrgas`,
        `_cdrtrc`) and of the CDR forcing file `<name>_cdr.nc`, so the base
        class's looser glob would match other kinds of file. A name that passes
        this glob but not the naming convention still raises, as in the base
        class.

        Returns
        -------
        str
        """
        return f"*{cls.SUFFIX}.{TIMESTAMP_GLOB}*.{cls.EXT}"


class PathLocation(ConfiguredBaseModel):
    """A file or directory path in a blueprint.

    The key is named `location` so that a relative value in a blueprint file is
    anchored to the blueprint file's directory by
    `cstar.orchestration.serialization.anchor_locations` when the file is
    loaded, the same rule as for dataset locations. In a workplan, placeholders
    such as ``{{output_dir: <step>}}`` are filled before validation. The path
    is not checked for existence here: a placeholder's path does not exist
    until its step has run.
    """

    location: Path
    """The path; relative values are anchored to the blueprint file's directory."""

    @field_validator("location", mode="after")
    @classmethod
    def _expand_user(cls, value: Path) -> Path:
        return value.expanduser()


class CdrDomain(ConfiguredBaseModel):
    """A model domain and the `_cdr` output to compute its metrics from."""

    grid: PathLocation
    """The domain's ROMS grid file."""
    sources: list[PathLocation] = Field(min_length=1)
    """The ordered sources of the domain's `_cdr` output: each a directory
    searched recursively, or a single `_cdr` file. The first must hold the
    segment containing the release, because cumulative sums start at the first
    record."""
    source_role: t.Literal["release", "transfer"] = Field(default="release")
    """``"release"`` for the domain holding the CDR release; ``"transfer"`` for a
    parent that receives the child's upscaled boundary perturbation as its
    source (roms-tools skips the release sign checks and leaves that domain's
    efficiencies undefined; its uptake counts in the total)."""


class CdrMetricsBlueprint(Blueprint):
    """A blueprint for computing CDR metrics from the `_cdr` output of ROMS-MARBL runs."""

    application: str = APP_NAME
    """The application identifier."""
    domains: dict[RequiredString, CdrDomain] = Field(min_length=1)
    """The domains to compute metrics for, by domain name, ordered child to parent:
    first the domain holding the release, then its parent, and so on (C-Star's
    YAML I/O preserves mapping order). One domain gives single-domain metrics;
    several are combined with roms-tools' `UpscaledCDR`, which sums the uptake
    and normalises the efficiency by the release domains' cumulative source."""

    @field_validator("domains", mode="after")
    @classmethod
    def _require_release_domain(
        cls, value: Mapping[str, CdrDomain]
    ) -> Mapping[str, CdrDomain]:
        if len(value) > 1 and not any(
            domain.source_role == "release" for domain in value.values()
        ):
            names = ", ".join(repr(name) for name in value)
            msg = (
                "nested CDR metrics need the domain holding the release: "
                f"domains {names} all have source_role 'transfer'. Set "
                "`source_role: release` on the child domain."
            )
            raise ValueError(msg)
        return value


def find_cdr_output(sources: Sequence[PathLocation]) -> list[CdrOutputFile]:
    """Locate the `_cdr` output files in each source.

    Parameters
    ----------
    sources : Sequence[PathLocation]
        The directories (searched recursively) or files to search, in order.

    Returns
    -------
    list[CdrOutputFile]
        The files of every source in the order given, without duplicates.

    Raises
    ------
    FileNotFoundError
        If a source holds no `_cdr` output.
    ValueError
        If a source does not exist, names a file that is not `_cdr` output, or
        holds a partitioned piece of `_cdr` output.
    """
    found: dict[Path, CdrOutputFile] = {}
    for source in sources:
        try:
            files = CdrOutputFile.find(source.location, notfound_ok=False) or ()
        except FileNotFoundError as err:
            raise FileNotFoundError(f"{err}. {_CDR_OUTPUT_HINT}") from err

        for file in files:
            if file.is_partitioned:
                msg = (
                    f"{file.path} is a partitioned piece of CDR output, not a whole "
                    "file. Join the pieces first (C-Star steps leave whole files in "
                    "`<step>/output/`), or point the source at the step's `output/`."
                )
                raise ValueError(msg)
            found.setdefault(file.path, file)

    log.debug(f"Found {len(found)} CDR output files in {len(sources)} sources")
    return list(found.values())


def check_required_variables(files: Sequence[CdrOutputFile]) -> None:
    """Verify that `_cdr` output holds the variables the CDR metrics need.

    Only the variable names are read, so the check is cheap for large output.

    Parameters
    ----------
    files : Sequence[CdrOutputFile]
        The output files of one domain.

    Raises
    ------
    ValueError
        If a file lacks any of the variables; every missing variable is
        reported with the number of files lacking it and the ROMS setting that
        writes it.
    """
    import xarray as xr

    lacking: dict[str, list[Path]] = defaultdict(list)
    for file in files:
        with xr.open_dataset(file.path, decode_times=False) as ds:
            for name in _REQUIRED_VARIABLES:
                if name not in ds.variables:
                    lacking[name].append(file.path)

    if not lacking:
        return

    by_setting: dict[str, list[str]] = defaultdict(list)
    for name, paths in lacking.items():
        by_setting[_REQUIRED_VARIABLES[name]].append(
            f"`{name}` ({len(paths)} of {len(files)} files)"
        )
    problems = [
        f"{', '.join(names)}: written by {setting}"
        for setting, names in by_setting.items()
    ]
    first = min(path for paths in lacking.values() for path in paths)
    msg = (
        "CDR output lacks variables needed for CDR metrics (first file affected: "
        f"{first}). {'; '.join(problems)}."
    )
    raise ValueError(msg)


def _time_labels(times: np.ndarray) -> list[str]:
    """Format time coordinate values for a message."""
    if np.issubdtype(times.dtype, np.datetime64):
        return [str(label) for label in np.datetime_as_string(times, unit="s")]
    return [str(value) for value in times]


def check_contiguous(ds: "xr.Dataset") -> None:
    """Verify that the averaging windows of consecutive records meet end to start.

    Only the 1-D `avg_begin_time` and `avg_end_time` variables are read.

    Parameters
    ----------
    ds : xr.Dataset
        The time-sorted CDR output, with `time`, `avg_begin_time` and
        `avg_end_time` (seconds).

    Raises
    ------
    ValueError
        If the output lacks the averaging window times, or if consecutive
        windows overlap or leave a gap (more than a second); every problem is
        reported. Windows of differing lengths only log a warning.
    """
    missing = [name for name in ("avg_begin_time", "avg_end_time") if name not in ds]
    if missing:
        msg = (
            f"CDR output is missing {missing}. The `_cdr` output must be averaged "
            "(`cdr_output.do_avg`) for CDR metrics to be computed."
        )
        raise ValueError(msg)

    begin = np.asarray(ds["avg_begin_time"].values, dtype=float)
    end = np.asarray(ds["avg_end_time"].values, dtype=float)
    labels = _time_labels(np.asarray(ds["time"].values))

    overlaps: list[str] = []
    gaps: list[str] = []
    mismatched = ~np.isclose(begin[1:], end[:-1], atol=1.0, rtol=0)
    for i in np.flatnonzero(mismatched):
        offset = begin[i + 1] - end[i]
        between = f"records {labels[i]} and {labels[i + 1]} ({abs(offset):g} s)"
        (gaps if offset > 0 else overlaps).append(between)

    problems: list[str] = []
    if overlaps:
        problems.append(
            "overlapping averaging windows between "
            + "; ".join(overlaps)
            + ": sources overlap or a segment is listed twice"
        )
    if gaps:
        problems.append(
            "gaps between "
            + "; ".join(gaps)
            + ": a segment is probably missing from `sources`"
        )
    if problems:
        msg = "CDR output is not contiguous in time: " + "; and ".join(problems) + "."
        raise ValueError(msg)

    lengths = np.unique(np.round(end - begin))
    if lengths.size > 1:
        log.warning(
            "CDR output has averaging windows of different lengths "
            f"({', '.join(f'{length:g} s' for length in lengths)}); cumulative "
            "sums weight each window by its own length, so check the output setup."
        )


def load_cdr_output(
    grid_path: Path, files: Sequence[CdrOutputFile]
) -> tuple["xr.Dataset", "xr.Dataset"]:
    """Open `_cdr` output as one time-sorted dataset and check it is contiguous.

    Parameters
    ----------
    grid_path : Path
        The ROMS grid file of the output's domain.
    files : Sequence[CdrOutputFile]
        The output files to concatenate along time.

    Returns
    -------
    tuple[xr.Dataset, xr.Dataset]
        The lazily loaded output and the grid dataset.

    Raises
    ------
    ValueError
        If the records are not contiguous in time (see `check_contiguous`).
    """
    grid = roms_tools.Grid(filename=grid_path)
    ds = roms_tools.ROMSOutput(
        grid=grid, path=[file.path for file in files], use_dask=True
    ).ds

    # roms-tools concatenates in lexical order of the full file paths, which is
    # not chronological across segment directories
    if not ds["time"].to_index().is_monotonic_increasing:
        ds = ds.sortby("time")
        log.debug(f"Sorted the records of {len(files)} CDR output files by time")

    check_contiguous(ds)
    return ds, grid.ds


def metric_series(ds_cdr: "xr.Dataset") -> "xr.Dataset":
    """Select the 1-D time series from the metrics computed by roms-tools.

    Keeping every variable over `time` alone, rather than naming them, follows
    whichever series the installed roms-tools computes. The 2-D area and 3-D
    `hDIC` fields, with their lateral coordinates, are left out.

    Parameters
    ----------
    ds_cdr : xr.Dataset
        The result of `compute_cdr_metrics`.

    Returns
    -------
    xr.Dataset
        The time series, loaded into memory.
    """
    names = [name for name, var in ds_cdr.data_vars.items() if var.dims == ("time",)]
    series = ds_cdr[names]
    series = series.drop_vars(
        [name for name, coord in series.coords.items() if coord.dims != ("time",)]
    )
    return series.load()


def source_sign_report(ds: "xr.Dataset") -> str:
    """Describe where the CDR source has the wrong sign for a release.

    A release adds alkalinity and removes DIC, so roms-tools requires
    ``ALK_source >= 0`` and ``DIC_source <= 0``. This realizes both source
    fields, so call it only to explain a failed check.

    Parameters
    ----------
    ds : xr.Dataset
        The time-sorted CDR output, with `time`, `avg_begin_time`,
        `avg_end_time`, `ALK_source` and `DIC_source`.

    Returns
    -------
    str
        One line per source variable: the cells and records with the wrong sign,
        when the first and last occur, the worst value, and the time-integrated
        wrong-sign amount as a share of the integrated ALK source.
    """
    import dask

    window = np.asarray((ds["avg_end_time"] - ds["avg_begin_time"]).values, dtype=float)
    labels = _time_labels(np.asarray(ds["time"].values))

    stats: dict[str, dict[str, t.Any]] = {}
    for name, sign in (("ALK_source", "negative"), ("DIC_source", "positive")):
        da = ds[name]
        dims = [dim for dim in da.dims if dim != "time"]
        bad = da < 0 if sign == "negative" else da > 0
        n_cells, n_bad_records, wrong, right, worst = dask.compute(
            bad.sum(),
            bad.any(dims),
            da.where(bad, 0.0).sum(dims),
            da.where(~bad, 0.0).sum(dims),
            da.min() if sign == "negative" else da.max(),
        )
        stats[name] = {
            "sign": sign,
            "n_cells": int(n_cells),
            "records": np.flatnonzero(n_bad_records.values),
            "wrong": float((wrong.values * window).sum()),
            "right": float((right.values * window).sum()),
            "worst": float(worst),
            "units": da.attrs.get("units", "?"),
        }

    reference = stats["ALK_source"]["right"]
    lines = []
    for name, stat in stats.items():
        records = stat["records"]
        if not stat["n_cells"]:
            lines.append(f"{name}: no sign violations")
            continue
        share = (
            f"{100 * abs(stat['wrong']) / reference:.3f}% of the integrated ALK source"
            if reference
            else "undefined against a zero integrated ALK source"
        )
        lines.append(
            f"{name} has {stat['sign']} values (roms-tools requires the opposite): "
            f"{stat['n_cells']} cells in {records.size} of {window.size} records "
            f"({labels[records[0]]} .. {labels[records[-1]]}), worst cell "
            f"{stat['worst']:.4g} {stat['units']}; time-integrated wrong-sign amount "
            f"{stat['wrong']:.4g} {stat['units'].removesuffix('/s')} = {share}"
        )
    return "\n".join(lines)


@contextlib.contextmanager
def _figure_backend() -> Iterator["ModuleType"]:
    """Provide pyplot on the non-interactive backend and close its figures after.

    roms-tools' plot functions end with ``plt.show()``, which warns on this
    backend and does nothing.

    Yields
    ------
    module
        `matplotlib.pyplot`.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*non-interactive.*")
            yield plt
    finally:
        plt.close("all")


def _save_figure(series: "xr.Dataset", path: Path) -> None:
    """Plot the uptake efficiency and uptake of a metrics series to a file.

    Parameters
    ----------
    series : xr.Dataset
        The metrics time series.
    path : Path
        The image file to write.
    """
    with _figure_backend() as plt:
        roms_tools.plot.plot_uptake_efficiency(series)
        plt.gcf().savefig(path)


def _save_upscaled_figures(
    upscaled: t.Any, figure_path: Path, domains_path: Path
) -> None:
    """Plot the combined metrics of nested domains and a map of the domains.

    Parameters
    ----------
    upscaled : roms_tools.UpscaledCDR
        The combined metrics.
    figure_path : Path
        The image file for the combined efficiency and uptake.
    domains_path : Path
        The image file for the outlines of the domains.
    """
    with _figure_backend():
        upscaled.plot(save_path=str(figure_path))
        upscaled.plot_domains(save_path=str(domains_path))


def compute_domain_metrics(name: str, domain: CdrDomain) -> tuple["xr.Dataset", int]:
    """Compute the CDR metrics of one domain from its `_cdr` output.

    Parameters
    ----------
    name : str
        The domain's name, for messages.
    domain : CdrDomain
        The domain's grid, sources and source role.

    Returns
    -------
    tuple[xr.Dataset, int]
        The result of roms-tools' `compute_cdr_metrics` (still lazy) and the
        number of `_cdr` files it was computed from.

    Raises
    ------
    FileNotFoundError
        If a source holds no `_cdr` output.
    ValueError
        If the output is incomplete or not contiguous, or if a release domain's
        source has the wrong sign (the message reports where).
    """
    prefix = f"Domain {name!r}: "
    try:
        files = find_cdr_output(domain.sources)
        check_required_variables(files)
        ds, grid_ds = load_cdr_output(domain.grid.location, files)
    except FileNotFoundError as err:
        raise FileNotFoundError(f"{prefix}{err}") from err
    except ValueError as err:
        raise ValueError(f"{prefix}{err}") from err

    try:
        ds_cdr = roms_tools.analysis.cdr_analysis.compute_cdr_metrics(
            ds, grid_ds, source_role=domain.source_role
        )
    except ValueError as err:
        if domain.source_role != "release" or _RELEASE_CONSTRAINT_MARKER not in str(
            err
        ):
            raise
        msg = (
            f"{prefix}{err}\n{source_sign_report(ds)}\n"
            "A parent receiving an upscaled perturbation is a 'transfer' domain "
            "(source_role: transfer); a release domain with wrong-sign sources is "
            "a forcing error."
        )
        raise ValueError(msg) from err
    return ds_cdr, len(files)


class CdrMetricsRunner(BlueprintRunner[CdrMetricsBlueprint]):
    """Worker class that computes CDR metrics from ROMS-MARBL `_cdr` output."""

    @t.override
    async def run(self) -> RunnerResult[CdrMetricsBlueprint]:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        domains = self.blueprint.domains
        results = {
            name: compute_domain_metrics(name, domain)
            for name, domain in domains.items()
        }
        ds_cdrs = {name: ds_cdr for name, (ds_cdr, _) in results.items()}

        out_dir = self.blueprint.effective_working_dir / "output"
        out_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = out_dir / METRICS_FILENAME
        figure_path = out_dir / FIGURE_FILENAME
        written = [metrics_path, figure_path]

        if len(ds_cdrs) == 1:
            ((name, ds_cdr),) = ds_cdrs.items()
            series = metric_series(ds_cdr)
            series.attrs["cdr_domain"] = name
            save_figures = functools.partial(_save_figure, series, figure_path)
        else:
            upscaled = roms_tools.UpscaledCDR(domains=ds_cdrs)
            series = metric_series(upscaled.ds)
            domains_path = out_dir / DOMAINS_FIGURE_FILENAME
            written.append(domains_path)
            save_figures = functools.partial(
                _save_upscaled_figures, upscaled, figure_path, domains_path
            )

        series.attrs["cdr_domains"] = ", ".join(
            f"{name}:{domain.source_role}" for name, domain in domains.items()
        )
        series.attrs["cdr_grid_files"] = ", ".join(
            domain.grid.location.as_posix() for domain in domains.values()
        )
        series.attrs["cdr_source_files"] = int(sum(n for _, n in results.values()))
        series.attrs["roms_tools_version"] = roms_tools.__version__

        series.to_netcdf(metrics_path)
        save_figures()
        log.info(f"Wrote CDR metrics to {', '.join(str(path) for path in written)}")

        self.add_state(ExecutionStatus.COMPLETED)
        return self.result


@register_application
class CdrMetricsApplication(
    ApplicationDefinition[CdrMetricsBlueprint, CdrMetricsRunner],
):
    name = APP_NAME
    long_name = _APP_NAME_LONG
    runner = CdrMetricsRunner
    blueprint = CdrMetricsBlueprint
    applicable_transforms = ()
