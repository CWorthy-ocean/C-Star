"""Check that forcing files gathered from several sources cover a run without gaps.

ucla-roms reads a forcing file's time values as days since the namelist
reference date, takes the two records that bracket the model time, and
interpolates linearly between them however far apart they are. Forcing a
workplan directive gathers from several sources (`nest-from`,
`carbonate-sensitivity-from`) with one segment missing therefore gives the run
data interpolated across the hole with no message, and forcing that ends before
the run's end stops ROMS only when the model time passes its last record, after
the build and the queue. `check_source_coverage` refuses both before ROMS
starts, reading only each file's time variable.
"""

import math
import statistics
import typing as t
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import pairwise
from pathlib import Path

from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
from cstar.roms.input_dataset import TimeAxis, read_time_axis

if t.TYPE_CHECKING:
    from cstar.roms.outputs import TimestampedOutputFile

log = get_logger(__name__)

SOURCE_GAP_FACTOR: t.Final[float] = 2.0
"""How many times the record spacing two consecutive records may lie apart
before the stretch between them counts as a gap.

The spacing is the median spacing of the whole source, or of either file's own
records where that is longer, so a source whose output period changes from one
segment to the next (an hourly spin-up followed by daily output) passes while a
missing segment does not. Monthly records (28 to 31 days apart) and the records
ucla-roms writes at the start and end of every `_cdrgas` run (half a period
from their neighbours) stay within it; one missing record at the source's own
period (spacing exactly twice) does too, so only longer gaps are refused.
Records at the same time (consecutive runs both write the time they share) are
never a gap."""

RECORD_TIME_TOLERANCE: t.Final[timedelta] = timedelta(seconds=1)
"""Slack when comparing record times: ROMS stores them in days, whose binary
rounding differs from the dates C-Star computes."""

_DAY: t.Final[timedelta] = timedelta(days=1)

_REMEDY: t.Final[str] = (
    "List every segment of the source, separated by `;` (e.g. "
    "`step: parent_1;parent_2;parent_3`, or `path:` with one output directory "
    "per segment), and make sure the source runs cover the step's start_date to "
    "end_date."
)
"""How to fix a source that does not cover the step, appended to the refusal."""


@dataclass(frozen=True)
class _FileRecords:
    """The records read from one whole file of a source."""

    path: Path
    """The file the records were read from (one piece of a partitioned file)."""
    axis: TimeAxis
    """Its time variable and recorded reference date."""


def _whole_files(
    files: Sequence["TimestampedOutputFile"],
) -> list[list["TimestampedOutputFile"]]:
    """Group files into the whole files they make up, in the order given.

    The pieces of a partitioned file share one group, in partition order.

    Parameters
    ----------
    files : Sequence[TimestampedOutputFile]
        The files of a source.

    Returns
    -------
    list[list[TimestampedOutputFile]]
    """
    groups: dict[tuple[Path, str], list[TimestampedOutputFile]] = {}
    for file in files:
        # `<base><SUFFIX>.<ts>.<segment>.nc` -> `<base><SUFFIX>.<ts>`
        name = (
            file.path.name.rsplit(".", 2)[0] if file.is_partitioned else file.path.name
        )
        groups.setdefault((file.path.parent, name), []).append(file)
    return [sorted(group, key=lambda f: f.partition or 0) for group in groups.values()]


def _read(path: Path, variable: str) -> TimeAxis | None:
    """Read `variable` from `path`, naming the file if it cannot be read.

    Parameters
    ----------
    path : Path
        The netCDF file.
    variable : str
        The time variable.

    Returns
    -------
    TimeAxis | None
        None if the file has no such variable.

    Raises
    ------
    CstarError
        If the file cannot be read as netCDF.
    """
    try:
        return read_time_axis(path, variable)
    except Exception as err:
        msg = (
            f"Could not read {variable!r} from {path}: {err}. If the run that "
            "writes it has not finished, wait until it has."
        )
        raise CstarError(msg) from err


def _read_whole_file(
    pieces: Sequence["TimestampedOutputFile"],
    variable: str,
    preferred: int | None,
    missing_hint: str,
) -> tuple[_FileRecords | None, int | None]:
    """Read the time records of one whole file, opening one piece of a partitioned one.

    Every piece of a partitioned ROMS file carries the same records, except that
    a rank without points of a boundary extraction writes none; pieces are tried
    from the partition that held records last time, until one holds records.

    Parameters
    ----------
    pieces : Sequence[TimestampedOutputFile]
        The file, or the pieces of a partitioned file in partition order.
    variable : str
        The time variable.
    preferred : int | None
        The partition to try first.
    missing_hint : str
        Advice appended when a whole file lacks `variable`.

    Returns
    -------
    tuple[_FileRecords | None, int | None]
        The records (None if no piece holds any) and the partition to try first
        for the next file.

    Raises
    ------
    CstarError
        If a whole (not partitioned) file lacks `variable` or cannot be read.
    """
    if not pieces[0].is_partitioned:
        path = pieces[0].path
        if (axis := _read(path, variable)) is None:
            msg = f"{path} has no {variable!r} variable. {missing_hint}".strip()
            raise CstarError(msg)
        return _FileRecords(path, axis), preferred

    for piece in sorted(pieces, key=lambda p: p.partition != preferred):
        if (axis := _read(piece.path, variable)) is not None and axis.values:
            return _FileRecords(piece.path, axis), piece.partition
    return None, preferred


def _median_spacing(values: Sequence[float], tolerance: float) -> float | None:
    """Return the median spacing of distinct sorted `values`, or None if fewer than two.

    Parameters
    ----------
    values : Sequence[float]
        Record times, in days.
    tolerance : float
        Spacings at or below this (in days) are repeats, not spacings.

    Returns
    -------
    float | None
    """
    spacings = [b - a for a, b in pairwise(sorted(values)) if b - a > tolerance]
    return statistics.median(spacings) if spacings else None


def check_source_coverage(
    files: Sequence["TimestampedOutputFile"],
    variable: str,
    start_date: datetime,
    end_date: datetime,
    *,
    source: str,
    missing_hint: str = "",
) -> None:
    """Refuse forcing files whose records leave a gap or do not span a run.

    The records of every file are read from `variable` (the header and that
    variable only; one open per whole file, one piece per partitioned file) and
    sorted by time, whatever order the files were given in. They are placed in
    time with the reference date the files record; a file that records none is
    taken to share the others'. The check refuses when

    - two consecutive records lie more than `SOURCE_GAP_FACTOR` times the
      record spacing apart and the run reads across them (records at the same
      time are allowed, and gaps wholly before or after the run are not its
      concern), or
    - the first record is after `start_date` or the last before `end_date`
      (ROMS needs a record at or before the model time when it starts and one
      at or after it at every later step).

    When no file records a reference date, the run cannot be placed among the
    records, so every gap is checked and the span is not, with a warning.
    Cyclic (climatological) files are not checked: ROMS wraps their records.

    Parameters
    ----------
    files : Sequence[TimestampedOutputFile]
        The files of the source, in the order the run will list them.
    variable : str
        The time variable ROMS reads the files' records by, e.g. `bry_time`.
    start_date : datetime
        The start of the run the files force.
    end_date : datetime
        The end of the run the files force.
    source : str
        What supplied the files, for messages, e.g. "the `nest-from` boundary files".
    missing_hint : str
        Advice appended when a file lacks `variable`.

    Raises
    ------
    CstarError
        If the files leave a gap, do not span `start_date` to `end_date`, hold
        no records, or cannot be read.
    """
    subject = source[:1].upper() + source[1:]
    read: list[_FileRecords] = []
    preferred: int | None = None
    for pieces in _whole_files(files):
        found, preferred = _read_whole_file(pieces, variable, preferred, missing_hint)
        if found is not None and found.axis.values:
            read.append(found)

    if not read:
        msg = f"{subject} hold no {variable!r} records. {_REMEDY}"
        raise CstarError(msg)

    if any(r.axis.reference is not None and r.axis.reference.cyclic for r in read):
        log.debug("%s are cyclic; their coverage is not checked", subject)
        return

    origin = next((r.axis.reference.date for r in read if r.axis.reference), None)

    def shift(r: _FileRecords) -> float:
        """Days from `origin` to the reference date of `r`'s file."""
        if origin is None or r.axis.reference is None:
            return 0.0
        return (r.axis.reference.date - origin) / _DAY

    def when(days: float) -> str:
        """Name the time `days` after `origin`."""
        if origin is not None:
            try:
                return str(origin + timedelta(days=days))
            except OverflowError:
                pass
        return f"day {days:g} after the reference date"

    def span(days: float) -> str:
        """Name a duration of `days`."""
        try:
            return str(timedelta(seconds=round(days * 86400)))
        except OverflowError:
            return f"{days:g} days"

    tolerance = RECORD_TIME_TOLERANCE / _DAY
    # the run window in days after `origin`; unbounded when it cannot be placed
    start = (start_date - origin) / _DAY if origin is not None else -math.inf
    end = (end_date - origin) / _DAY if origin is not None else math.inf
    records = sorted(
        (value + shift(r), i) for i, r in enumerate(read) for value in r.axis.values
    )
    own_spacing = [_median_spacing(r.axis.values, tolerance) for r in read]
    problems: list[str] = []

    if (
        overall := _median_spacing([time for time, _ in records], tolerance)
    ) is not None:
        for (before, i), (after, j) in pairwise(records):
            spacing = after - before
            # a repeated time, or a gap the run never reads across
            if (
                spacing <= tolerance
                or after <= start + tolerance
                or before >= end - tolerance
            ):
                continue
            local = max(
                s for s in (overall, own_spacing[i], own_spacing[j]) if s is not None
            )
            if spacing > SOURCE_GAP_FACTOR * local + tolerance:
                problems.append(
                    f"no record between {when(before)} ({read[i].path}) and "
                    f"{when(after)} ({read[j].path}), {span(spacing)} apart where "
                    f"records are {span(local)} apart"
                )

    if origin is None:
        log.warning(
            "%s record no reference date, so whether they span %s to %s was not checked",
            subject,
            start_date,
            end_date,
        )
    else:
        (first, i), (last, j) = records[0], records[-1]
        if first > start + tolerance:
            problems.append(
                f"the first record is at {when(first)} ({read[i].path}), after "
                f"start_date {start_date}"
            )
        if last < end - tolerance:
            problems.append(
                f"the last record is at {when(last)} ({read[j].path}), before "
                f"end_date {end_date}"
            )

    if problems:
        msg = (
            f"{subject} do not cover the run from {start_date} to "
            f"{end_date} without gaps: {'; '.join(problems)}. ROMS interpolates "
            "linearly across a gap and stops when the model time passes the last "
            f"record. {_REMEDY}"
        )
        raise CstarError(msg)
