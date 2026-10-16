"""CDR metrics for a nested + upscaled CDR workplan run (post-processing only).

This is the one script in the nesting/upscaling example that is NOT run through
``cstar``.  It only READS the outputs of a finished workplan run (nothing under
the run directory is created, modified or removed) and uses roms-tools' CDR
analysis (``ROMSOutput.cdr_metrics`` -> ``compute_cdr_metrics``) to turn the
``output_cdr.*.nc`` files of the ROMS-MARBL steps into CO2-uptake and
CDR-efficiency time series.

Usage
-----
::

    python cdr_metrics.py --run-dir ~/cstar/workplan_runs/<run-id> \
        [--child-step child] [--parent-up-step parent_up] [--out DIR] \
        [--clip-negative-sources] [--use-dask] [--model-reference-date YYYY-MM-DD]

Each step is looked up as ``<run-dir>/tasks/<step>/``.  A step that is not
there is skipped (with a log message), so a child-only run works too.  For each
step that is present the script

* finds ``output/*_cdr.<YYYYmmddHHMMSS>.nc`` (time ordered) and the grid the
  step actually ran with (the staged ``input/input_datasets/*_grid.nc``; the grid
  location in the step's emitted blueprint, ``work/B_*.yaml``, is the fallback),
* takes the model reference date from the files' ``ocean_time`` metadata (the
  emitted blueprint does not carry it; ``--model-reference-date`` is only
  needed if the metadata lacks it) and checks the resulting times against the
  blueprint's ``runtime_params`` window,
* builds ``roms_tools.ROMSOutput`` and runs ``cdr_metrics()``,
* writes ``<out>/<step>_cdr_metrics.nc`` (the ``ds_cdr`` dataset) and
  ``<out>/<step>_uptake_efficiency.png`` (roms-tools' own uptake/efficiency plot).

With two steps it also writes a side-by-side ``<out>/comparison.png`` (efficiency
and uptake of both domains against model time, no summing) and, when
``roms_tools.Ensemble`` can handle the data, ``<out>/ensemble.png`` (otherwise the
reason is logged), and prints a summary of the final efficiency and uptake of each
domain.  ``--out`` defaults to
``./cdr_metrics_out/<run-dir name>`` and may not be inside the run directory.
The exit status is non-zero if any present step failed.

The two efficiencies are DIFFERENT quantities -- do not add them
---------------------------------------------------------------
* ``child``: CO2 taken up *inside the child domain* per unit of alkalinity (ALK)
  *released* into the child (the CDR forcing of the child run).
* ``parent_up`` (the upscaled run): CO2 taken up *in the parent domain* per unit
  of ALK that *entered through the child's boundary* (the upscaled forcing,
  which is what left the child, not what was originally released).

The denominators differ (original release vs. the ALK flux across the child
boundary) and so do the control volumes in which the uptake is counted.  This
script therefore reports the two side by side and NEVER sums or otherwise
combines them into a single total or a single efficiency.  How (and whether) to
combine them is an open question for the scientists.  Likewise the ensemble
statistics (mean/std/min-max band) that ``roms_tools.Ensemble`` computes across
its members are meaningless for these two members; only its per-member curves
are meaningful.

Real-data problems handled, never hidden
----------------------------------------
* roms-tools requires ``ALK_source >= 0`` and ``DIC_source <= 0`` everywhere.
  The upscaled ALK/DIC flux can legitimately have small opposite-sign values (for
  example before the plume reaches the child boundary, or back-flow at the
  boundary).  By default roms-tools' ``ValueError`` is surfaced as the step's
  error, together with a report of where and how large the violations are.  The
  explicit opt-in ``--clip-negative-sources`` clips ALK_source to ``>= 0`` and
  DIC_source to ``<= 0`` (logged loudly, and recorded in the output attributes);
  this removes the offending (net-outflow) amounts from the cumulative source
  used as the efficiency denominator, so the efficiencies become approximate.
* Averaging windows: the script reports the window length(s) found, and warns on
  gaps or overlaps between consecutive windows (missing or duplicated files).  The
  cumulative sums are only meaningful for a gapless record, and non-increasing
  times are an error.
* Missing variables are reported by name with the ROMS settings that produce
  them (``cdr_output.do_cdr_output``/``do_avg`` and ``cdr_frc.cdr_source``).
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import textwrap
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # non-interactive: figures are only ever saved to file

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import roms_tools
import xarray as xr
import yaml
from roms_tools import Ensemble, Grid, ROMSOutput

logger = logging.getLogger("cdr_metrics")

CDR_FILE_RE = re.compile(r"^(?P<prefix>.+)_cdr\.(?P<stamp>\d{14})\.nc$")

# What roms-tools' compute_cdr_metrics reads from the ROMS "cdr" output files,
# and the ROMS settings that make ucla-roms write each of them.
REQUIRED_VARS = {
    "avg_begin_time": "cdr_output.do_cdr_output: true and cdr_output.do_avg: true "
    "(namelist wrt_cdr_avg = .true.; the window times are only written for averaged output)",
    "avg_end_time": "cdr_output.do_cdr_output: true and cdr_output.do_avg: true "
    "(namelist wrt_cdr_avg = .true.)",
    "FG_CO2": "cdr_output.do_cdr_output: true (needs MARBL, MARBL_DIAGS and CDR_FORCING)",
    "FG_ALT_CO2": "cdr_output.do_cdr_output: true (needs MARBL, MARBL_DIAGS and CDR_FORCING)",
    "hDIC": "cdr_output.do_cdr_output: true (needs MARBL, MARBL_DIAGS and CDR_FORCING)",
    "hDIC_ALT_CO2": "cdr_output.do_cdr_output: true (needs MARBL, MARBL_DIAGS and CDR_FORCING)",
    "ALK_source": "cdr_output.do_cdr_output: true and cdr_frc.cdr_source: true",
    "DIC_source": "cdr_output.do_cdr_output: true and cdr_frc.cdr_source: true",
}

# Role of each step, used only for labels and the saved attributes.
STEP_DESCRIPTIONS = {
    "child": (
        "CO2 uptake inside the child domain per unit of ALK released into the child"
    ),
    "parent_up": (
        "CO2 uptake in the parent domain per unit of ALK entering via the "
        "upscaled child-boundary flux"
    ),
}


class StepError(Exception):
    """A step cannot be processed; the message says why and what to do."""


@dataclass
class StepResult:
    """Outcome of processing one ROMS step."""

    step: str
    role: str
    ds_cdr: xr.Dataset
    nc_path: Path
    png_path: Path
    summary: dict = field(default_factory=dict)


# --------------------------------------------------------------------------
# Locating inputs
# --------------------------------------------------------------------------
def find_cdr_files(step_dir: Path) -> list[Path]:
    """Return the step's ``*_cdr.<timestamp>.nc`` files, in time order."""
    out_dir = step_dir / "output"
    if not out_dir.is_dir():
        raise StepError(f"no output directory {out_dir} (did the step run?)")
    matches = [p for p in out_dir.iterdir() if CDR_FILE_RE.match(p.name)]
    if not matches:
        present = sorted(p.name for p in out_dir.iterdir())
        if not present:
            raise StepError(f"{out_dir} is empty (did the step finish?)")
        raise StepError(
            f"no '*_cdr.<YYYYmmddHHMMSS>.nc' files in {out_dir}; contains: "
            f"{present[:12]}{' ...' if len(present) > 12 else ''}. The step needs "
            "cdr_output.do_cdr_output: true and PIO-joined output (partitioned "
            "per-tile files are not supported)."
        )
    return sorted(matches, key=lambda p: p.name.rsplit(".", 2)[-2])  # the timestamp


def find_blueprint(step_dir: Path) -> tuple[Path | None, dict]:
    """Return the step's most-derived emitted blueprint (``work/B_*.yaml``) and its content.

    Successive directives (overrides, continue-from, nest-from) append suffixes, so
    the longest file name is the blueprint that was finally run.
    """
    candidates = sorted(
        (step_dir / "work").glob("B_*.yaml"), key=lambda p: (len(p.name), p.name)
    )
    if not candidates:
        return None, {}
    path = candidates[-1]
    with path.open() as fh:
        return path, yaml.safe_load(fh) or {}


def find_grid(step_dir: Path, blueprint: dict) -> Path:
    """Find the grid the step ran with.

    The staged ``input/input_datasets/*_grid.nc`` is what ROMS read; the blueprint's
    ``grid.data[0].location`` is the fallback.  If both exist and resolve to
    different files, that is logged.
    """
    staged = sorted((step_dir / "input" / "input_datasets").glob("*_grid.nc"))
    if len(staged) > 1:
        raise StepError(f"more than one staged grid file: {[p.name for p in staged]}")

    bp_grid = None
    try:
        loc = blueprint["grid"]["data"][0]["location"]
        if not str(loc).startswith(("http", "cstar-forge:")):
            bp_grid = Path(loc)
    except (KeyError, IndexError, TypeError):
        pass

    if staged and staged[0].exists():
        if (
            bp_grid is not None
            and bp_grid.exists()
            and staged[0].resolve() != bp_grid.resolve()
        ):
            logger.warning(
                "staged grid %s and blueprint grid %s differ; using the staged one",
                staged[0].resolve(),
                bp_grid,
            )
        return staged[0].resolve()
    if bp_grid is not None and bp_grid.exists():
        logger.info("no staged grid found; using the blueprint's grid %s", bp_grid)
        return bp_grid
    raise StepError(
        f"cannot find the grid: no usable {step_dir / 'input/input_datasets/*_grid.nc'}"
        f" and blueprint grid location is {bp_grid!r}"
    )


def check_variables(files: list[Path]) -> None:
    """Fail with a precise message when required variables are missing from any file."""
    missing: dict[str, list[str]] = {}
    for path in files:
        with xr.open_dataset(path, decode_times=False) as ds:
            for var in REQUIRED_VARS:
                if var not in ds.variables:
                    missing.setdefault(var, []).append(path.name)
    if missing:
        lines = [
            f"  {var}: missing in {len(names)}/{len(files)} file(s) "
            f"(e.g. {names[0]}); produced by {REQUIRED_VARS[var]}"
            for var, names in missing.items()
        ]
        raise StepError(
            "required variables missing from the cdr output:\n" + "\n".join(lines)
        )


# --------------------------------------------------------------------------
# Checks on the loaded record
# --------------------------------------------------------------------------
def _fmt_time(t) -> str:
    return pd.Timestamp(t).strftime("%Y-%m-%d %H:%M")


def check_time_record(ds: xr.Dataset, blueprint: dict) -> dict:
    """Check ordering, window lengths and coverage; return a few numbers for the summary."""
    time = ds["time"].values
    if time.size > 1 and not np.all(np.diff(time) > np.timedelta64(0)):
        bad = int(np.argmin(np.diff(time) > np.timedelta64(0)))
        raise StepError(
            "model time is not strictly increasing (duplicate or overlapping "
            f"files, e.g. from a restarted run?) near {_fmt_time(time[bad])}"
        )

    begin = ds["avg_begin_time"].values.astype(float)
    end = ds["avg_end_time"].values.astype(float)
    window = end - begin
    if np.any(window <= 0):
        raise StepError("an averaging window has non-positive length")
    uniq = np.unique(window)
    logger.info(
        "%d averaging windows of length %s s (first %s, last %s)",
        window.size,
        ", ".join(f"{w:g}" for w in uniq[:5]) + (" ..." if uniq.size > 5 else ""),
        _fmt_time(time[0]),
        _fmt_time(time[-1]),
    )
    if uniq.size > 1:
        logger.warning(
            "averaging windows are not all the same length (%s s): the cumulative "
            "sums weight each window by its own length, but check the output setup",
            uniq,
        )
    gaps = begin[1:] - end[:-1]
    if np.any(np.abs(gaps) > 1.0):
        n_bad = int(np.sum(np.abs(gaps) > 1.0))
        logger.warning(
            "%d gap(s)/overlap(s) between consecutive averaging windows (largest %g s,"
            " first near %s): missing or duplicated files? The cumulative source and "
            "uptake are not trustworthy across them",
            n_bad,
            float(np.max(np.abs(gaps))),
            _fmt_time(time[1:][np.argmax(np.abs(gaps) > 1.0)]),
        )

    rp = blueprint.get("runtime_params", {}) or {}
    first_begin = pd.Timestamp(time[0]) - pd.Timedelta(seconds=float(window[0]))
    last_end = pd.Timestamp(time[-1])
    if "start_date" in rp and "end_date" in rp:
        start, stop = pd.Timestamp(rp["start_date"]), pd.Timestamp(rp["end_date"])
        slack = pd.Timedelta(seconds=1)
        if first_begin < start - slack or last_end > stop + slack:
            logger.warning(
                "output time span %s .. %s lies outside the blueprint's run window "
                "%s .. %s -- the model reference date may be wrong",
                first_begin,
                last_end,
                start,
                stop,
            )
        else:
            logger.info(
                "output span %s .. %s is inside the blueprint run window %s .. %s",
                first_begin,
                last_end,
                start,
                stop,
            )
    else:
        logger.info(
            "blueprint has no runtime_params window; time span not cross-checked"
        )
    return {"window_s": uniq}


def source_sign_report(ds: xr.Dataset) -> dict:
    """Quantify violations of ``ALK_source >= 0`` and ``DIC_source <= 0``."""
    window = (ds["avg_end_time"] - ds["avg_begin_time"]).values.astype(float)
    time = ds["time"].values
    report: dict = {}
    for var, bad_sign in (("ALK_source", "neg"), ("DIC_source", "pos")):
        da = ds[var]
        dims = [d for d in da.dims if d != "time"]
        bad = da < 0 if bad_sign == "neg" else da > 0
        wrong = da.where(bad, 0.0)
        right = da.where(~bad, 0.0)
        wrong_t = wrong.sum(dims).values  # per record, in units/s
        right_t = right.sum(dims).values
        recs = np.nonzero(bad.any(dims).values)[0]
        report[var] = {
            "n_cells": int(bad.sum()),
            "n_records": int(recs.size),
            "n_total_records": int(time.size),
            "first": _fmt_time(time[recs[0]]) if recs.size else None,
            "last": _fmt_time(time[recs[-1]]) if recs.size else None,
            "worst_cell": float(da.min() if bad_sign == "neg" else da.max()),
            "integrated_wrong": float((wrong_t * window).sum()),
            "integrated_right": float((right_t * window).sum()),
            "units": da.attrs.get("units", "?"),
        }
    return report


def log_sign_report(report: dict, level: int) -> None:
    """Log the sign-violation report (one line per variable)."""
    alk_ref = abs(report["ALK_source"]["integrated_right"])
    for var, r in report.items():
        if r["n_cells"] == 0:
            logger.log(level, "%s: no sign violations", var)
            continue
        frac = abs(r["integrated_wrong"]) / alk_ref if alk_ref else float("nan")
        logger.log(
            level,
            "%s has %s values (roms-tools requires %s): %d cells in %d of %d records "
            "(%s .. %s), worst cell %.4g %s; time-integrated wrong-sign amount "
            "%.4g %s = %.3f%% of the integrated ALK source (%.4g meq)",
            var,
            "negative" if var == "ALK_source" else "positive",
            ">= 0" if var == "ALK_source" else "<= 0",
            r["n_cells"],
            r["n_records"],
            r["n_total_records"],
            r["first"],
            r["last"],
            r["worst_cell"],
            r["units"],
            r["integrated_wrong"],
            r["units"].replace("/s", ""),
            100 * frac,
            alk_ref,
        )


def _dic_net_tco2eq(report: dict) -> float:
    """Net time-integrated DIC_source, as tonnes CO2 equivalent (1 mol DIC = 1 mol CO2)."""
    r = report["DIC_source"]
    if not r["units"].startswith("mmol"):
        return float("nan")
    net_mmol = r["integrated_wrong"] + r["integrated_right"]
    return net_mmol * 1e-3 * 44.0095e-3 / 1000.0  # mmol -> mol -> kg -> tonnes


def clip_sources(roms_output: ROMSOutput, report: dict) -> None:
    """Opt-in workaround: clip ALK_source to >= 0 and DIC_source to <= 0 (loudly)."""
    ds = roms_output.ds
    alk = ds["ALK_source"].clip(min=0.0)
    dic = ds["DIC_source"].clip(max=0.0)
    alk.attrs = ds["ALK_source"].attrs
    dic.attrs = ds["DIC_source"].attrs
    roms_output.ds = ds.assign(ALK_source=alk, DIC_source=dic)
    logger.warning(
        "--clip-negative-sources: ALK_source clipped to >= 0 and DIC_source to <= 0 "
        "(removed %.4g %s of ALK and %.4g %s of DIC from the cumulative source). "
        "The efficiencies below use the clipped source and are APPROXIMATE.",
        abs(report["ALK_source"]["integrated_wrong"]),
        report["ALK_source"]["units"].replace("/s", ""),
        abs(report["DIC_source"]["integrated_wrong"]),
        report["DIC_source"]["units"].replace("/s", ""),
    )


# --------------------------------------------------------------------------
# One step
# --------------------------------------------------------------------------
def _limit_efficiency_axis(ax, series: list[np.ndarray]) -> None:
    """Set efficiency y-limits from the latter half of each record.

    Efficiency is uptake / cumulative source, so while the cumulative source is
    still tiny (e.g. the first hours of an upscaled run) it is huge and noisy and
    flattens the rest of the curve.  Those early points stay in the NetCDF; they
    are only left off-scale in the plot, and the plot says so.
    """
    tail = np.concatenate([np.asarray(s)[len(s) // 2 :] for s in series])
    tail = tail[np.isfinite(tail)]
    if tail.size == 0:
        return
    lo, hi = min(0.0, float(tail.min())), max(0.0, float(tail.max()))
    pad = 0.08 * ((hi - lo) or 1.0)
    ax.set_ylim(lo - pad, hi + pad)
    everything = np.concatenate([np.asarray(s) for s in series])
    everything = everything[np.isfinite(everything)]
    if everything.min() < lo - pad or everything.max() > hi + pad:
        ax.text(
            0.99,
            0.03,
            "early-time efficiency off-scale (cumulative source still tiny)",
            transform=ax.transAxes,
            ha="right",
            fontsize=7,
        )


def process_step(
    run_dir: Path, step: str, role: str, out_dir: Path, args: argparse.Namespace
) -> StepResult:
    step_dir = run_dir / "tasks" / step
    logger.info("== %s (%s)", step, role)

    files = find_cdr_files(step_dir)
    bp_path, blueprint = find_blueprint(step_dir)
    grid_path = find_grid(step_dir, blueprint)
    logger.info(
        "%d cdr files %s .. %s; grid %s; blueprint %s",
        len(files),
        files[0].name,
        files[-1].name,
        grid_path,
        bp_path.name if bp_path else None,
    )
    check_variables(files)

    grid = Grid(filename=grid_path)
    try:
        roms_output = ROMSOutput(
            grid=grid,
            path=[str(p) for p in files],
            model_reference_date=args.model_reference_date,
            use_dask=args.use_dask,
        )
    except ValueError as exc:
        if "reference date" in str(exc).lower():
            raise StepError(
                f"{exc} -- the files' ocean_time long_name lacks 'Time since "
                "YYYY/MM/DD'; pass --model-reference-date YYYY-MM-DD"
            ) from exc
        raise
    logger.info(
        "model reference date %s (%s)",
        roms_output.model_reference_date.date(),
        "from --model-reference-date"
        if args.model_reference_date
        else "inferred from ocean_time metadata",
    )

    ds = roms_output.ds
    info = check_time_record(ds, blueprint)

    report = source_sign_report(ds)
    violated = any(r["n_cells"] for r in report.values())
    if violated and args.clip_negative_sources:
        log_sign_report(report, logging.WARNING)
        clip_sources(roms_output, report)
    elif violated:
        log_sign_report(report, logging.ERROR)
    else:
        log_sign_report(report, logging.INFO)

    plt.close("all")
    try:
        with warnings.catch_warnings():
            # roms-tools ends its plot with plt.show(), a no-op on the Agg backend
            warnings.filterwarnings("ignore", message=".*non-interactive.*")
            roms_output.cdr_metrics()  # raises ValueError on sign violations
    except ValueError as exc:
        if violated and not args.clip_negative_sources:
            raise StepError(
                f"roms-tools rejected the model output: {exc} (details above). "
                "Re-run with --clip-negative-sources to proceed with clipped "
                "sources (approximate efficiencies)."
            ) from exc
        raise
    fig = plt.gcf()
    ds_cdr = roms_output.ds_cdr

    # --- label, save the roms-tools plot and the metrics --------------------
    fig.axes[0].set_title(
        f"{step}: CO$_2$ uptake and CDR efficiency\n({role.replace('CO2', 'CO$_2$')})",
        fontsize=9,
    )
    # cosmetics on roms-tools' figure: hour-resolution ticks (it formats dates as
    # %Y-%m-%d, which makes every tick of a sub-daily record identical) and an
    # efficiency axis that is not flattened by the tiny early cumulative source
    fig.axes[0].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    _limit_efficiency_axis(
        fig.axes[0],
        [
            ds_cdr["cdr_efficiency_from_flux"].values,
            ds_cdr["cdr_efficiency_from_DIC_difference"].values,
        ],
    )
    png_path = out_dir / f"{step}_uptake_efficiency.png"
    fig.savefig(png_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    ds_cdr.attrs.update(
        cstar_step=step,
        cstar_run_dir=str(run_dir),
        efficiency_meaning=role,
        not_combinable=(
            "efficiency/uptake of different cstar steps are different quantities "
            "(different denominators and control volumes); do not add them"
        ),
        grid_file=str(grid_path),
        n_cdr_files=len(files),
        model_reference_date=str(roms_output.model_reference_date.date()),
        roms_tools_version=roms_tools.__version__,
        clip_negative_sources=str(
            bool(violated and args.clip_negative_sources)
        ).lower(),
    )
    if violated and args.clip_negative_sources:
        ds_cdr.attrs.update(
            clipped_alk_negative_integrated=report["ALK_source"]["integrated_wrong"],
            clipped_dic_positive_integrated=report["DIC_source"]["integrated_wrong"],
        )
    nc_path = out_dir / f"{step}_cdr_metrics.nc"
    ds_cdr.to_netcdf(nc_path)
    logger.info("wrote %s and %s", nc_path, png_path)

    return StepResult(
        step=step,
        role=role,
        ds_cdr=ds_cdr,
        nc_path=nc_path,
        png_path=png_path,
        summary=summarize(roms_output.ds, ds_cdr)
        | info
        | {
            "dic_source_net_tco2eq": _dic_net_tco2eq(report),
            "clipped": bool(violated and args.clip_negative_sources),
        },
    )


def summarize(ds: xr.Dataset, ds_cdr: xr.Dataset) -> dict:
    """Final efficiency/uptake of one domain (the last valid record)."""
    eff_flux = ds_cdr["cdr_efficiency_from_flux"].dropna("time")
    if eff_flux.size == 0:
        raise StepError("cdr_efficiency_from_flux has no valid values (no source?)")
    last = eff_flux["time"].values[-1]
    window = ds_cdr["window_length"]
    dims = [d for d in ds["ALK_source"].dims if d != "time"]
    alk_total = float(
        (ds["ALK_source"].sum(dims) * window).sum()
    )  # meq: ALK injected over the whole record
    pick = {
        k: float(ds_cdr[k].sel(time=last))
        for k in (
            "cdr_efficiency_from_flux",
            "cdr_efficiency_from_DIC_difference",
            "cdr_carbon_uptake_from_flux",
            "cdr_carbon_uptake_from_DIC_difference",
        )
    }
    return {
        "last_time": last,
        "alk_injected_meq": alk_total,
        "alk_units": ds["ALK_source"].attrs.get("units", "meq/s").replace("/s", ""),
        **pick,
    }


# --------------------------------------------------------------------------
# Combined plot and summary
# --------------------------------------------------------------------------
def plot_side_by_side(results: list[StepResult], path: Path) -> None:
    """Two stacked panels (efficiency, uptake), one colour per domain, absolute time.

    Deliberately not a dual-axis plot and nothing is summed across domains.
    """
    colors = ["#2a78d6", "#eb6834"]  # categorical slots 1 and 2, fixed order
    fig, (ax_e, ax_u) = plt.subplots(2, 1, figsize=(9, 7), sharex=True)
    for color, res in zip(colors, results):
        d = res.ds_cdr
        label = f"{res.step}"
        for key, ls, kind in (
            ("flux", "-", "CO$_2$ flux"),
            ("DIC_difference", "--", "DIC difference"),
        ):
            ax_e.plot(
                d["time"],
                d[f"cdr_efficiency_from_{key}"],
                color=color,
                ls=ls,
                lw=2,
                label=f"{label}, {kind}",
            )
            ax_u.plot(
                d["time"],
                d[f"cdr_carbon_uptake_from_{key}"],
                color=color,
                ls=ls,
                lw=2,
                label=f"{label}, {kind}",
            )
    _limit_efficiency_axis(
        ax_e,
        [
            r.ds_cdr[f"cdr_efficiency_from_{k}"].values
            for r in results
            for k in ("flux", "DIC_difference")
        ],
    )
    ax_e.set_ylabel("CDR efficiency")
    ax_u.set_ylabel(r"CO$_2$ uptake (tonnes CO$_2$)")
    ax_u.set_xlabel("model time")
    ax_e.legend(loc="best", fontsize=8, framealpha=0.9)
    for ax in (ax_e, ax_u):
        ax.grid(alpha=0.3)
    fig.suptitle(
        "Per-domain CDR metrics side by side: different quantities, NOT additive",
        fontsize=10,
    )
    fig.text(
        0.01,
        0.005,
        "\n".join(textwrap.fill(f"{r.step}: {r.role}", width=110) for r in results),
        fontsize=7,
        va="top",
    )
    fig.autofmt_xdate()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def combined_plot(results: list[StepResult], out_dir: Path) -> list[Path]:
    """Write the combined plots; return their paths.

    ``comparison.png`` (side by side, absolute time) is always written.
    ``ensemble.png`` (``roms_tools.Ensemble``) is written when Ensemble can handle
    the data; it fails for records shorter than a few days (see the warning).
    """
    if len(results) < 2:
        logger.info("only one domain processed; no combined plot")
        return []
    paths = []
    path = out_dir / "comparison.png"
    plot_side_by_side(results, path)
    paths.append(path)

    members = {r.step: r.ds_cdr for r in results}
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*non-interactive.*")
            ens = Ensemble(members=members)
            path = out_dir / "ensemble.png"
            ens.plot(save_path=str(path))
        paths.append(path)
        logger.warning(
            "ensemble.png: Ensemble aligns members on integer days since each "
            "member's own release start, and its shaded min-max band is a spread "
            "between members; neither is meaningful for child vs parent_up, which "
            "are different quantities. Only the per-member curves are meaningful "
            "-- prefer comparison.png."
        )
    except Exception as exc:  # noqa: BLE001 - report, keep the side-by-side plot
        logger.warning(
            "roms_tools.Ensemble cannot be used for these members (%s: %s); "
            "only the side-by-side comparison.png is written",
            type(exc).__name__,
            str(exc).splitlines()[0] if str(exc) else "",
        )
    finally:
        plt.close("all")
    return paths


def print_summary(results: list[StepResult]) -> None:
    print("\n" + "=" * 78)
    print("CDR metrics -- final values per domain")
    print("=" * 78)
    for r in results:
        s = r.summary
        print(f"\n[{r.step}] {r.role}")
        print(f"  last record            : {_fmt_time(s['last_time'])}")
        clipped = " (clipped)" if s.get("clipped") else ""
        print(
            f"  ALK source, summed     : {s['alk_injected_meq']:.4g} {s['alk_units']}{clipped}"
        )
        print(f"  efficiency (CO2 flux)  : {s['cdr_efficiency_from_flux']:.5f}")
        print(
            f"  efficiency (DIC diff)  : {s['cdr_efficiency_from_DIC_difference']:.5f}"
        )
        print(
            f"  uptake (CO2 flux)      : {s['cdr_carbon_uptake_from_flux']:.5g} t CO2"
        )
        print(
            f"  uptake (DIC diff)      : {s['cdr_carbon_uptake_from_DIC_difference']:.5g} t CO2"
        )
        if abs(s["dic_source_net_tco2eq"]) > 0:
            print(
                f"  net DIC_source (record): {s['dic_source_net_tco2eq']:.4g} t CO2-eq -- the "
                "DIC-difference uptake includes this\n"
                "                           unless DIC_ALT_source mirrors it (flux-based "
                "numbers are unaffected)"
            )
    print(
        "\nNOTE: these are DIFFERENT quantities and are deliberately not combined.\n"
        "  child     = uptake inside the child per unit ALK released into the child.\n"
        "  parent_up = uptake in the parent per unit ALK entering through the child's\n"
        "              boundary (the upscaled flux).\n"
        "  Whether/how to merge them into one total is an open question for the\n"
        "  scientists; this script never sums them."
    )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        epilog="See the module docstring for the scientific caveats.",
    )
    p.add_argument("--run-dir", type=Path, required=True, help="workplan run directory")
    p.add_argument(
        "--child-step", default="child", help="child ROMS step (default: child)"
    )
    p.add_argument(
        "--parent-up-step",
        default="parent_up",
        help="upscaled parent ROMS step (default: parent_up)",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="output directory (default: ./cdr_metrics_out/<run-dir name>)",
    )
    p.add_argument(
        "--clip-negative-sources",
        action="store_true",
        help="OPT-IN workaround: clip ALK_source to >= 0 and DIC_source to <= 0 so "
        "roms-tools accepts the output (logged; efficiencies become approximate). "
        "Default: surface roms-tools' error.",
    )
    p.add_argument(
        "--model-reference-date",
        type=datetime.fromisoformat,
        default=None,
        help="only if the files' ocean_time metadata lacks 'Time since YYYY/MM/DD'",
    )
    p.add_argument("--use-dask", action="store_true", help="load the output lazily")
    p.add_argument(
        "-v", "--verbose", action="store_true", help="debug logging, tracebacks"
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
        force=True,  # roms-tools/cstar may already have configured the root logger
    )
    run_dir = args.run_dir.expanduser().resolve()
    if not (run_dir / "tasks").is_dir():
        logger.error("%s has no tasks/ directory", run_dir)
        return 2
    out_dir = (
        args.out.expanduser()
        if args.out
        else Path.cwd() / "cdr_metrics_out" / run_dir.name
    ).resolve()
    if out_dir == run_dir or run_dir in out_dir.parents:
        logger.error(
            "--out %s is inside the run directory; this script only reads it", out_dir
        )
        return 2
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info(
        "roms-tools %s from %s",
        roms_tools.__version__,
        Path(roms_tools.__file__).parent,
    )

    results: list[StepResult] = []
    failures: list[str] = []
    for step, role_key in (
        (args.child_step, "child"),
        (args.parent_up_step, "parent_up"),
    ):
        if not (run_dir / "tasks" / step).is_dir():
            logger.info("step %r not present in %s; skipping", step, run_dir / "tasks")
            continue
        try:
            results.append(
                process_step(run_dir, step, STEP_DESCRIPTIONS[role_key], out_dir, args)
            )
        except Exception as exc:
            plt.close("all")
            failures.append(step)
            logger.error("step %r FAILED: %s: %s", step, type(exc).__name__, exc)
            if args.verbose:
                logger.exception("traceback")

    if not results and not failures:
        logger.error(
            "none of the steps %s/%s found", args.child_step, args.parent_up_step
        )
        return 2

    if results:
        print_summary(results)
        try:
            for path in combined_plot(results, out_dir):
                logger.info("wrote %s", path)
        except Exception as exc:  # noqa: BLE001
            failures.append("combined plot")
            logger.error("combined plot FAILED: %s: %s", type(exc).__name__, exc)
    if failures:
        logger.error("FAILED: %s", ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
