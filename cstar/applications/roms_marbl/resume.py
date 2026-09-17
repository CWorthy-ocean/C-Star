"""Resume an interrupted roms_marbl step from its own restart output.

When a step is retried after failing partway through, this module builds a
derived blueprint that continues from the newest *usable* restart the
interrupted attempt left behind, instead of restarting from the blueprint's
original initial conditions.

The search directory follows the same convention `RomsFileSystemManager`
uses for a step's own outputs: a non-ParallelIO run writes partitioned
restart pieces to `temp_output` before `post_run` joins them into `output`,
so a resume must look in `temp_output`; a ParallelIO run writes whole
restart files directly to `output`. A restart is only usable once every
expected piece (or the whole file, for ParallelIO) is present, non-empty,
and opens as netCDF -- a piece written by a process that was killed
mid-write is otherwise indistinguishable from a complete one by name alone.

The original blueprint is never modified: `prepare_resume_blueprint` reads
it, applies the located restart as an `OverrideTransform`, and writes the
result to a new `<stem>.resume.yaml` file in the step's `work` directory.
"""

import re
import typing as t
from collections.abc import Sequence
from pathlib import Path

import xarray as xr

from cstar.applications.roms_marbl.file_system import RomsFileSystemManager
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.applications.roms_marbl.transforms import RestartFile, RestartFileTrxAdapter
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.execution.file_system import local_copy
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.transforms import OverrideTransform

if t.TYPE_CHECKING:
    from datetime import datetime

log = get_logger(__name__)

RESUME_SUFFIX: t.Final[str] = "resume"
"""Suffix appended to the stem of a derived, resume-ready blueprint file."""


def _candidates(search_dir: Path) -> Sequence[RestartFile]:
    """Enumerate restart-file candidates in a directory.

    Mirrors the glob and naming-convention filtering `RestartFile.find` uses,
    factored out here so both can share the same matching rules.

    Parameters
    ----------
    search_dir : Path
        The directory to search.

    Returns
    -------
    Sequence[RestartFile]
    """
    if not search_dir.is_dir():
        return ()

    partitioned_glob = (
        f"*{RestartFile.SUFFIX}.{RestartFile.TS_GLOB}.*.{RestartFile.EXT}"
    )
    joined_glob = f"*{RestartFile.SUFFIX}.{RestartFile.TS_GLOB}.{RestartFile.EXT}"

    found: list[RestartFile] = []
    for glob_pattern in (partitioned_glob, joined_glob):
        found.extend(
            RestartFile(path=match)
            for match in search_dir.rglob(glob_pattern)
            if re.fullmatch(RestartFile.PATTERN_RST, match.name, flags=re.ASCII)
        )
    return found


def _opens_as_netcdf(path: Path) -> bool:
    """Return `True` when `path` is a non-empty file that opens as netCDF.

    Parameters
    ----------
    path : Path
        The file to check.

    Returns
    -------
    bool
    """
    if not path.is_file() or path.stat().st_size == 0:
        return False

    try:
        with xr.open_dataset(path):
            pass
    except Exception:
        return False

    return True


def find_resume_restart(
    search_dir: Path, expected_pieces: int | None
) -> RestartFile | None:
    """Locate the newest usable restart left by an interrupted attempt.

    Candidates are grouped by timestamp and considered newest first. A
    timestamp is usable when `expected_pieces` is an int and exactly that
    many distinct, non-empty, netCDF-readable partition pieces exist for it,
    or when `expected_pieces` is `None` and a non-empty, netCDF-readable
    whole (unpartitioned) file exists for it. An unusable timestamp is
    skipped with one warning and the search continues at the next-newest
    timestamp.

    Parameters
    ----------
    search_dir : Path
        The directory to search (a step's `temp_output` for a non-ParallelIO
        run, or its `output` for a ParallelIO run).
    expected_pieces : int | None
        The number of partition pieces a complete restart must have, or
        `None` when the run is not partitioned (ParallelIO).

    Returns
    -------
    RestartFile | None
        The partition-0 (or whole-file) `RestartFile` for the newest usable
        restart, or `None` if no usable restart was found.
    """
    by_ts: dict[datetime, list[RestartFile]] = {}
    for candidate in _candidates(search_dir):
        by_ts.setdefault(candidate.timestamp, []).append(candidate)

    for ts in sorted(by_ts, reverse=True):
        pieces = by_ts[ts]
        ts_str = pieces[0].formatted_timestamp

        if expected_pieces is not None:
            by_partition = {p.partition: p for p in pieces if p.is_partitioned}
            if not by_partition:
                continue

            if len(by_partition) != expected_pieces:
                log.warning(
                    "restart %s in %s skipped: %d of %d pieces",
                    ts_str,
                    search_dir,
                    len(by_partition),
                    expected_pieces,
                )
                continue

            ordered = sorted(by_partition.values(), key=lambda p: p.partition or 0)
            bad = next((p for p in ordered if not _opens_as_netcdf(p.path)), None)
            if bad is not None:
                log.warning(
                    "restart %s in %s skipped: unreadable piece %s",
                    ts_str,
                    search_dir,
                    bad.path.name,
                )
                continue

            return ordered[0]

        whole = next((p for p in pieces if not p.is_partitioned), None)
        if whole is None:
            continue

        if not _opens_as_netcdf(whole.path):
            log.warning("restart %s in %s skipped: unreadable file", ts_str, search_dir)
            continue

        return whole

    return None


def prepare_resume_blueprint(blueprint_uri: str) -> str:
    """Build a resume-ready blueprint from an interrupted step's own output.

    Reads the blueprint at `blueprint_uri`, locates the newest usable
    restart the step's own working directory holds, and -- when one is
    found and the run has not already finished -- writes a derived
    blueprint that continues from it. The original blueprint file is left
    untouched.

    Parameters
    ----------
    blueprint_uri : str
        The URI (local path or remote resource) of the blueprint to resume.

    Returns
    -------
    str
        The path to the derived, resume-ready blueprint, or `blueprint_uri`
        unchanged when no usable restart was found.

    Raises
    ------
    CstarExpectationFailed
        If a non-ParallelIO blueprint lacks the processor grid needed to
        know how many restart pieces to expect, or if the located restart
        is at or after `runtime_params.end_date` (the run appears to have
        already finished).
    """
    with local_copy(blueprint_uri) as bp_path:
        bp = deserialize(bp_path, RomsMarblBlueprint)

    fs = RomsFileSystemManager(bp.working_dir)
    use_pio = bp.partitioning.use_pio

    if use_pio:
        search_dir = fs.output_dir
        expected_pieces = None
    else:
        n_procs_x, n_procs_y = bp.partitioning.n_procs_x, bp.partitioning.n_procs_y
        if n_procs_x is None or n_procs_y is None:
            msg = (
                f"Blueprint {bp.name!r} cannot be resumed: partitioning.n_procs_x "
                "and n_procs_y must both be set to know how many restart pieces "
                "to expect"
            )
            raise CstarExpectationFailed(msg)
        search_dir = fs.temp_output_dir
        expected_pieces = n_procs_x * n_procs_y

    restart = find_resume_restart(search_dir, expected_pieces)

    if restart is None:
        log.warning(
            "No usable restart found in %s; resuming %s from its original "
            "initial conditions",
            search_dir,
            bp.name,
        )
        return blueprint_uri

    if restart.timestamp >= bp.runtime_params.end_date:
        msg = (
            f"last restart {restart.path.name} is at or after end_date "
            f"{bp.runtime_params.end_date}; the run appears to have finished -- "
            f"inspect {search_dir} or re-run with --clobber"
        )
        raise CstarExpectationFailed(msg)

    overrides = RestartFileTrxAdapter.adapt(restart)
    new_bp = OverrideTransform(sys_overrides=overrides, replace_lists=True).apply(bp)

    out = (
        fs.run_dir
        / Path(bp_path).with_stem(f"{Path(bp_path).stem}.{RESUME_SUFFIX}").name
    )
    fs.run_dir.mkdir(parents=True, exist_ok=True)
    serialize(out, new_bp)

    log.info(
        "Resuming %s from restart %s (%s); blueprint written to %s",
        bp.name,
        restart.path,
        restart.timestamp,
        out,
    )
    return str(out)
