"""Decide which forcing files a ROMS run needs, from the records they hold.

ucla-roms looks up each forcing variable by name across every file listed in
``frcfiles``, in list order from the first file, and holds two records at a
time (``it1 <= model time < it2``). This module reads each file's time span
cheaply, drops the files that lie wholly outside a run's window, and finds
variables whose files do not cover the window.
"""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Final

from cstar.roms.input_dataset import is_cyclic, recorded_reference_date
from cstar.roms.source_coverage import RECORD_TIME_TOLERANCE

if TYPE_CHECKING:
    import xarray as xr

NON_FORCING_TIME_NAMES: Final = frozenset(
    {
        "ocean_time",
        "abs_time",
        "avg_begin_time",
        "avg_end_time",
        "roms_time",
        "scrum_time",
    }
)
"""Time variables ucla-roms writes for its outputs or initial conditions (in
seconds, or hours since 1900 for ``abs_time``) and never reads as a forcing
time."""


@dataclass(frozen=True)
class FileTimes:
    """The forcing variables of one file and the span of its records."""

    variables: frozenset[str]
    """The forcing variables the file holds (not time variables)."""
    raw_spans: Mapping[str, tuple[float, float]]
    """The first and last value of each forcing time variable as stored (days
    since the file's reference date), by name."""
    reference: datetime | None
    """The reference date the file records, None if it records none."""
    cyclic: bool
    """Whether the file is a climatology that ROMS repeats every cycle."""

    @cached_property
    def spans(self) -> Mapping[str, tuple[datetime, datetime]]:
        """The (first, last) record of each forcing time variable as absolute
        datetimes; empty without a reference date, and leaving out a variable
        whose values do not fit a datetime.
        """
        spans: dict[str, tuple[datetime, datetime]] = {}
        if self.reference is None:
            return spans
        for name, (lo, hi) in self.raw_spans.items():
            try:
                spans[name] = (
                    self.reference + timedelta(days=lo),
                    self.reference + timedelta(days=hi),
                )
            except OverflowError:
                continue
        return spans

    @property
    def placeable(self) -> bool:
        """Whether the records can be placed in time."""
        return bool(self.spans)

    @property
    def first(self) -> datetime:
        """The earliest first record (only meaningful when `placeable`)."""
        return min(first for first, _ in self.spans.values())

    @property
    def last(self) -> datetime:
        """The latest last record (only meaningful when `placeable`)."""
        return max(last for _, last in self.spans.values())


@dataclass(frozen=True)
class UncoveredSpan:
    """Forcing variables whose files together do not cover the run window."""

    files: tuple[int, ...]
    """Indices (into the list passed in) of the files holding the variables."""
    variables: tuple[str, ...]
    """The uncovered variables, sorted."""
    first: datetime
    """The earliest record any of the files holds for these variables."""
    last: datetime
    """The latest record any of the files holds for these variables."""
    late_start: bool
    """Whether even the earliest record comes after the start of the run."""
    early_end: bool
    """Whether even the latest record comes before the end of the run."""


def _unknown(t: FileTimes | None) -> bool:
    """Whether a file's records cannot be placed in time, so it is always kept."""
    return t is None or t.cyclic or not t.placeable


def _is_time_name(name: str) -> bool:
    return name == "time" or name.endswith("_time")


def _is_forcing_time(name: str, var: "xr.Variable") -> bool:
    return (
        _is_time_name(name)
        and name not in NON_FORCING_TIME_NAMES
        and var.ndim == 1
        and var.size > 0
        and var.dtype.kind in "iuf"
    )


def _span_days(var: "xr.Variable") -> tuple[float, float] | None:
    """Return the (min, max) of a time variable, reading as little as possible."""
    import numpy as np

    # Only the two endpoints are read, so the cost does not grow with the record
    # count; a fill value masked to NaN at an end falls back to the whole variable.
    ends = [float(np.asarray(var[i].values)) for i in (0, -1)]
    if not all(np.isfinite(ends)):
        raw = np.asarray(var.values, dtype=float).ravel()
        ends = [float(v) for v in raw[np.isfinite(raw)]]
        if not ends:
            return None
    return min(ends[0], ends[-1]), max(ends[0], ends[-1])


def read_file_times(path: Path) -> FileTimes | None:
    """Read the forcing variables and time span of a netCDF forcing file.

    Only the header and the first and last element of each forcing time
    variable are read, never a data variable, so the cost does not grow with
    the file's size.

    Parameters
    ----------
    path : Path
        The netCDF file to read.

    Returns
    -------
    FileTimes or None
        None if the file cannot be read or has no forcing time variable (e.g.
        tidal forcing). A file that records no reference date is returned with
        `reference` None.
    """
    # Lazy import: only needed when a run is set up
    import xarray as xr

    try:
        with xr.open_dataset(
            path,
            decode_times=False,
            decode_timedelta=False,
            create_default_indexes=False,
        ) as ds:
            recorded = recorded_reference_date(ds)
            cyclic = is_cyclic(ds)
            time_dims: set[object] = set()
            raw_spans: dict[str, tuple[float, float]] = {}
            for name, var in ds.variables.items():
                if not _is_forcing_time(str(name), var):
                    continue
                if (span := _span_days(var)) is None:
                    continue
                time_dims.add(var.dims[0])
                raw_spans[str(name)] = span
            if not raw_spans:
                return None

            variables = frozenset(
                str(name)
                for name, var in ds.variables.items()
                if not _is_time_name(str(name))
                and name not in NON_FORCING_TIME_NAMES
                and var.ndim >= 1
                and var.dims[0] in time_dims
            )
    except Exception:
        # not netCDF, missing or corrupt: the run cannot be placed among its records
        return None
    return FileTimes(
        variables=variables,
        raw_spans=raw_spans,
        reference=None if recorded is None else recorded.date,
        cyclic=cyclic,
    )


def _cyclic_variables(times: Sequence[FileTimes | None]) -> frozenset[str]:
    return frozenset(
        v for t in times if t is not None and t.cyclic for v in t.variables
    )


def _holders(
    times: Sequence[FileTimes | None], skip: Collection[str]
) -> dict[str, list[int]]:
    holders: dict[str, list[int]] = {}
    for i, t in enumerate(times):
        if t is None or _unknown(t):
            continue
        for v in t.variables - set(skip):
            holders.setdefault(v, []).append(i)
    return holders


def _spans(
    times: Sequence[FileTimes | None], i: int
) -> Mapping[str, tuple[datetime, datetime]]:
    t = times[i]
    return {} if t is None else t.spans


def files_in_window(
    times: Sequence[FileTimes | None], start: datetime, end: datetime | None
) -> list[bool]:
    """Flag which forcing files a run over ``[start, end]`` needs.

    Each forcing variable is bracketed on its own: ``L`` is the latest record of
    any file holding it that lies before ``start`` and ``H`` the earliest that
    lies after ``end``. ROMS holds the last record before the start (stepping
    back across a file boundary if need be) and the first record after the end,
    and ``L`` and ``H`` are actual records, so keeping every file that reaches
    into ``[L, H]`` keeps a superset of what ROMS reads. Files are only dropped,
    never reordered. ROMS reads each variable against one of the file's time
    variables and we cannot tell which, so each time-variable name is bracketed
    separately and the bound that is safe for all of them is used. Unreadable, cyclic and unplaceable files are always kept, and a variable
    held by a cyclic file is never trimmed.

    Parameters
    ----------
    times : Sequence[FileTimes | None]
        One entry per forcing file in list order; None for a file that could
        not be read.
    start : datetime
        The start of the run.
    end : datetime or None
        The end of the run; None leaves the end side untrimmed.

    Returns
    -------
    list[bool]
        True for each file to keep, aligned with ``times``.
    """
    holders = _holders(times, _cyclic_variables(times))
    needed: set[int] = set()
    for indices in holders.values():
        if len(indices) == 1:
            needed.add(indices[0])
            continue
        lows: list[datetime | None] = []
        highs: list[datetime | None] = []
        for name in sorted({n for i in indices for n in _spans(times, i)}):
            records = [r for i in indices for r in _spans(times, i).get(name, ())]
            before = [r for r in records if r < start - RECORD_TIME_TOLERANCE]
            after = [
                r
                for r in records
                if end is not None and r > end + RECORD_TIME_TOLERANCE
            ]
            lows.append(max(before) if before else None)
            highs.append(min(after) if after else None)
        low = None if any(x is None for x in lows) else min(x for x in lows if x)
        high = None if any(x is None for x in highs) else max(x for x in highs if x)
        for i in indices:
            t = times[i]
            assert t is not None
            if (low is None or t.last >= low) and (high is None or t.first <= high):
                needed.add(i)

    held = {i for indices in holders.values() for i in indices}
    return [_unknown(t) or i in needed or i not in held for i, t in enumerate(times)]


def uncovered_variables(
    times: Sequence[FileTimes | None],
    kinds: Sequence[str],
    start: datetime,
    end: datetime,
    ignore: Collection[str] = (),
) -> list[UncoveredSpan]:
    """Find forcing variables whose files do not cover ``[start, end]``.

    A variable is skipped when a file of the same kind could not be read (that
    file might hold it, and ROMS could then complete the run), when a file that
    records no usable reference date holds it (its records cannot be placed),
    or when a cyclic file holds it (ROMS wraps those records). A cyclic or
    unplaceable file does not stop the other variables of its kind being
    checked: a Forge kind holds a BGC climatology beside its physics files.

    Parameters
    ----------
    times : Sequence[FileTimes | None]
        One entry per forcing file in list order; None for an unreadable file.
    kinds : Sequence[str]
        The forcing kind of each entry (e.g. ``"surface"``), same length as
        ``times``.
    start : datetime
        The start of the run.
    end : datetime
        The end of the run.
    ignore : Collection[str]
        Variables not to check.

    Returns
    -------
    list[UncoveredSpan]
        One entry per distinct set of files and span, ordered by first file.
    """
    unreadable_kinds = {k for t, k in zip(times, kinds, strict=True) if t is None}
    unplaced = frozenset(
        v
        for t in times
        if t is not None and not t.cyclic and not t.placeable
        for v in t.variables
    )
    holders = _holders(times, _cyclic_variables(times) | unplaced | set(ignore))

    groups: dict[tuple[tuple[int, ...], datetime, datetime, bool, bool], list[str]] = {}
    for v, indices in holders.items():
        if any(kinds[i] in unreadable_kinds for i in indices):
            continue
        held_by = [t for i in indices if (t := times[i]) is not None]
        first = min(t.first for t in held_by)
        last = max(t.last for t in held_by)
        # ROMS reads a variable against one of its files' time variables and we
        # cannot tell which: refuse only if every candidate falls short
        covered = False
        for name in {n for t in held_by for n in t.spans}:
            named = [t.spans[name] for t in held_by if name in t.spans]
            if (
                min(f for f, _ in named) <= start + RECORD_TIME_TOLERANCE
                and max(la for _, la in named) >= end - RECORD_TIME_TOLERANCE
            ):
                covered = True
                break
        if not covered:
            late = first > start + RECORD_TIME_TOLERANCE
            early = last < end - RECORD_TIME_TOLERANCE
            groups.setdefault((tuple(indices), first, last, late, early), []).append(v)

    return [
        UncoveredSpan(
            files=files,
            variables=tuple(sorted(vs)),
            first=first,
            last=last,
            late_start=late,
            early_end=early,
        )
        for (files, first, last, late, early), vs in sorted(
            groups.items(), key=lambda kv: (kv[0][0], sorted(kv[1]))
        )
    ]
