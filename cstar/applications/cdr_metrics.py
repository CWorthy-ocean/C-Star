"""Compute CDR (carbon dioxide removal) metrics from ROMS-MARBL `_cdr` output.

The application reads the `_cdr` output (``<root>_cdr.<YYYYMMDDHHMMSS>.nc``) of
one or more ROMS-MARBL runs, computes the CDR metrics with roms-tools'
``compute_cdr_metrics``, and writes the resulting time series and a figure to
``<working_dir>/output``:

- ``cdr_metrics.nc``: the 1-D time series (uptake efficiencies, cumulative
  uptake in tonnes CO2, averaging window length).
- ``cdr_metrics.png``: roms-tools' "CO2 uptake and CDR efficiency" figure.

A run may be split into segments (e.g. by the time splitter), each writing its
own `_cdr` files; the application stitches them along time. The blueprint maps
a domain name to the domain's ROMS grid file and the ordered sources of its
`_cdr` output (exactly one domain is supported)::

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

Upscaled (nested) metrics are not implemented yet. To add them, lift the
one-domain guard, add ``CdrDomain.source_role`` (``"release"`` by default,
``"transfer"`` for a parent; a minor schema bump), compute each domain's
``ds_cdr`` with that role, and combine them with roms-tools' ``UpscaledCDR``,
whose members are ordered child to parent like ``domains``; `metric_series` is
unchanged. This needs a roms-tools release with ``UpscaledCDR``. Each
``ds_cdr`` stays in process because it carries the 3-D ``hDIC`` fields, so the
written series can be an ``Ensemble`` member but not an ``UpscaledCDR`` one.
"""

import typing as t
import warnings
from collections.abc import Mapping, Sequence
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

_CDR_OUTPUT_HINT: t.Final[str] = (
    "The producing run must be a MARBL run with CDR forcing and "
    "`cdr_output.do_cdr_output` set (forge turns it on for an active MARBL CDR forcing)."
)
"""Appended to not-found errors so users know what a run needs to write `_cdr` output."""

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


class CdrMetricsBlueprint(Blueprint):
    """A blueprint for computing CDR metrics from the `_cdr` output of ROMS-MARBL runs."""

    application: str = APP_NAME
    """The application identifier."""
    domains: dict[RequiredString, CdrDomain] = Field(min_length=1)
    """The domains to compute metrics for, by domain name; exactly one is
    supported. The order matters for upscaled (nested) metrics, which list a
    child domain before its parent; C-Star's YAML I/O preserves mapping order."""

    @field_validator("domains", mode="after")
    @classmethod
    def _single_domain(cls, value: Mapping[str, CdrDomain]) -> Mapping[str, CdrDomain]:
        if len(value) > 1:
            msg = (
                "CDR metrics over more than one (nested) domain are not yet "
                "supported; list exactly one domain."
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
        reported.
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


def _save_figure(series: "xr.Dataset", path: Path) -> None:
    """Plot the uptake efficiency and uptake of a metrics series to a file.

    Parameters
    ----------
    series : xr.Dataset
        The metrics time series.
    path : Path
        The image file to write.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    try:
        with warnings.catch_warnings():
            # roms-tools ends the plot with `plt.show()`, a no-op on this backend
            warnings.filterwarnings("ignore", message=".*non-interactive.*")
            roms_tools.plot.plot_uptake_efficiency(series)
        plt.gcf().savefig(path)
    finally:
        plt.close("all")


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
        name, domain = next(iter(self.blueprint.domains.items()))

        files = find_cdr_output(domain.sources)
        ds, grid_ds = load_cdr_output(domain.grid.location, files)
        series = metric_series(
            roms_tools.analysis.cdr_analysis.compute_cdr_metrics(ds, grid_ds)
        )
        series.attrs["cdr_domain"] = name

        out_dir = self.blueprint.effective_working_dir / "output"
        out_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = out_dir / METRICS_FILENAME
        figure_path = out_dir / FIGURE_FILENAME

        series.to_netcdf(metrics_path)
        _save_figure(series, figure_path)
        log.info(f"Wrote CDR metrics to {metrics_path} and {figure_path}")

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
