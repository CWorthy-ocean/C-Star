"""Check that the generated ROMS inputs cover the run window.

ucla-roms takes its start time from the initial-conditions file's ``ocean_time``. For
every forcing variable it then needs a record at or before the model time on the first
step, and stops the run once a variable has no record at or after the model time.
roms-tools only logs when the source data does not reach that far, so a run window
past the source's coverage writes truncated forcing that ROMS reads until its clock
passes the last record, possibly hours into the run.

These helpers read only the one-dimensional time variables of the written files, never
their data variables, so their cost does not grow with the grid.
"""

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path

import netCDF4

_SECONDS_PER_UNIT = {"seconds": 1.0, "days": 86400.0}
"""The units roms-tools writes on the relative time variables ROMS reads."""

_ABS_TIME = "abs_time"
"""roms-tools' readable copy of the time axis, which ROMS does not read."""

_INITIAL_TIME = "ocean_time"
"""The initial-conditions time variable ucla-roms takes its start time from."""

SOURCE_RECORD_ATTR = "ini_time_source_record"
"""Global attribute recording the source record a flexible-start initial-conditions
file was taken from, when that record is not at the run's start date."""

_TOLERANCE_S = 1e-3
"""Float rounding allowed when comparing a record time with the window, in seconds;
far below any model time step."""

_REGENERATE_HINT = (
    "delete the file(s) or rerun with --clobber so Forge regenerates them for this "
    "window"
)


def _as_paths(paths: str | Path | Iterable[str | Path]) -> list[Path]:
    if isinstance(paths, (str, Path)):
        return [Path(paths)]
    return [Path(p) for p in paths]


def _stamp(reference: datetime, seconds: float) -> datetime:
    return reference + timedelta(seconds=seconds)


def _time_spans(path: Path) -> dict[str, tuple[float, float] | None]:
    """The first and last record, in seconds, of each relative time variable in ``path``.

    A variable with a ``cycle_length`` attribute is a climatology, which ROMS cycles
    through, and maps to ``None``. Only the header and the first and last value of
    each time variable are read.

    Raises
    ------
    ValueError
        If ``path`` has no time variable in units ROMS reads, so that a layout this
        check does not know cannot pass unchecked.
    """
    spans: dict[str, tuple[float, float] | None] = {}
    with netCDF4.Dataset(path) as nc:
        for name, var in nc.variables.items():
            if var.ndim != 1 or not name.endswith("time") or name == _ABS_TIME:
                continue
            scale = _SECONDS_PER_UNIT.get(getattr(var, "units", ""))
            if scale is None:
                continue
            if "cycle_length" in var.ncattrs():
                spans[name] = None
                continue
            n = var.shape[0]
            if n == 0:
                raise ValueError(f"{path.name}: the time variable {name!r} is empty.")
            var.set_auto_mask(False)
            spans[name] = (float(var[0]) * scale, float(var[n - 1]) * scale)
    if not spans:
        raise ValueError(
            f"{path.name}: no time variable in units of {' or '.join(_SECONDS_PER_UNIT)} "
            "was found, so Forge cannot check that this file covers the run window."
        )
    return spans


def check_forcing_covers_window(
    paths: str | Path | Iterable[str | Path],
    *,
    start: datetime,
    end: datetime,
    reference: datetime,
    label: str,
) -> None:
    """Refuse forcing files whose records do not span ``start`` to ``end``.

    ``paths`` are the files of one forcing item, possibly split by year or month,
    which ROMS reads as one list, so the records of each time variable are pooled
    across them. Each variable must have its first record at or before ``start`` and
    its last at or after ``end``. A climatology (``cycle_length``) is exempt.

    Parameters
    ----------
    paths
        The written or reused files of one forcing item.
    start, end
        The run window.
    reference
        The model reference date the file times count from.
    label
        What the files are, for the error message (e.g. ``"boundary forcing"``).

    Raises
    ------
    ValueError
        If a time variable starts after ``start`` or ends before ``end``.
    """
    files = _as_paths(paths)
    pooled: dict[str, list[tuple[float, float] | None]] = defaultdict(list)
    for path in files:
        for name, span in _time_spans(path).items():
            pooled[name].append(span)

    start_s = (start - reference).total_seconds()
    end_s = (end - reference).total_seconds()
    gaps: list[str] = []
    for name, spans in pooled.items():
        records = [span for span in spans if span is not None]
        if len(records) < len(spans):
            continue  # a climatology: ROMS cycles through it
        first = min(span[0] for span in records)
        last = max(span[1] for span in records)
        if first > start_s + _TOLERANCE_S:
            gaps.append(
                f"the first {name!r} record is {_stamp(reference, first)}, after the "
                "start"
            )
        if last < end_s - _TOLERANCE_S:
            gaps.append(
                f"the last {name!r} record is {_stamp(reference, last)}, before the end"
            )
    if gaps:
        names = ", ".join(path.name for path in files)
        raise ValueError(
            f"The {label} ({names}) does not cover the run window {start} to {end}: "
            + "; ".join(gaps)
            + ". ROMS needs a record at or before the start and one at or after the "
            "end, and stops when its clock passes the last record. Stage source data "
            "that covers the window, or shorten the window. If the window changed "
            f"since these files were written, {_REGENERATE_HINT}."
        )


def initial_time(path: str | Path, reference: datetime) -> datetime:
    """The time an initial-conditions file starts the model at (its ``ocean_time``)."""
    with netCDF4.Dataset(path) as nc:
        var = nc.variables[_INITIAL_TIME]
        var.set_auto_mask(False)
        seconds = float(var[0]) * _SECONDS_PER_UNIT[var.units]
    return _stamp(reference, seconds)


def check_initial_time(
    paths: str | Path | Iterable[str | Path], *, start: datetime, reference: datetime
) -> None:
    """Refuse initial conditions that would start the model at another time than ``start``.

    Raises
    ------
    ValueError
        If an initial-conditions file's ``ocean_time`` is not ``start``.
    """
    for path in _as_paths(paths):
        actual = initial_time(path, reference)
        if abs((actual - start).total_seconds()) > _TOLERANCE_S:
            raise ValueError(
                f"The initial-conditions file {path.name} starts the model at "
                f"{actual}, but the run starts at {start}; ROMS takes its start time "
                "from this file, so every output and forcing time would be offset. "
                "If it was written for another start date, or with flexible start "
                f"time by an earlier C-Star release, {_REGENERATE_HINT}."
            )


def restamp_initial_time(
    path: str | Path, *, start: datetime, reference: datetime
) -> datetime | None:
    """Stamp an initial-conditions file at ``start``, keeping its state.

    With ``allow_flex_time``, roms-tools takes the first source record in
    ``[start, start + 24 h)`` and stamps the file with that record's time. This
    relabels that state as the state at ``start`` and records the record's time in
    the :data:`SOURCE_RECORD_ATTR` global attribute. Call it on the file roms-tools
    wrote (netCDF-4), before any CDF-5 conversion: the edit changes two values and
    two attributes, which never rewrites a netCDF-4 file's data.

    Returns
    -------
    datetime or None
        The source record's time, or ``None`` when the file already starts at
        ``start`` and was left untouched.
    """
    target = (start - reference).total_seconds()
    with netCDF4.Dataset(path, "r+") as nc:
        var = nc.variables[_INITIAL_TIME]
        var.set_auto_mask(False)
        scale = _SECONDS_PER_UNIT[var.units]
        seconds = float(var[0]) * scale
        if abs(seconds - target) <= _TOLERANCE_S:
            return None
        record = _stamp(reference, seconds)
        var[0] = target / scale
        if _ABS_TIME in nc.variables:
            abs_time = nc.variables[_ABS_TIME]
            abs_time.units = f"days since {start:%Y-%m-%d %H:%M:%S}"
            abs_time[0] = 0
        nc.setncattr(SOURCE_RECORD_ATTR, str(record))
    return record
