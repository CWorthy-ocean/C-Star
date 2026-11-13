"""Resume an interrupted ROMS step from its own restart output.

When a step is retried after failing partway through, this module builds a
derived blueprint that continues from the newest *usable* restart the
interrupted attempt left behind, instead of restarting from the blueprint's
original initial conditions.

The search directory follows the run's file layout, as `RomsFileSystemManager`
spells it for a step's own outputs: a partitioned run (non-ParallelIO
ucla_roms) writes partitioned restart pieces to `temp_output` before
`post_run` joins them into `output`, so a resume must look in `temp_output`; a
run with a joined layout (ParallelIO, or a model that always writes whole
files) writes whole restart files directly to `output`. A restart is only
usable once every expected piece (or the whole file, for a joined layout) is
present, non-empty, and opens as netCDF -- a piece written by a process that
was killed mid-write is otherwise indistinguishable from a complete one by
name alone.

The original blueprint is never modified: `resume_blueprint_from_restart`
reads it, applies the located restart as an `OverrideTransform`, and writes the
result to a new `<stem>.resume.yaml` file in the step's `work` directory. An
application supplies its blueprint class and where its restarts are
(`prepare_resume_blueprint` does so for ucla_roms).
"""

import typing as t
from pathlib import Path

import xarray as xr

from cstar.applications.ucla_roms.models import UclaRomsBlueprint
from cstar.applications.ucla_roms.transforms import RestartFileTrxAdapter
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.execution.file_system import is_remote_resource, local_copy
from cstar.orchestration.serialization import load_blueprint, serialize
from cstar.orchestration.transforms import OverrideTransform
from cstar.roms.file_system import RomsFileSystemManager
from cstar.roms.outputs import RestartFile

if t.TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cstar.orchestration.models import Blueprint

log = get_logger(__name__)

TBp = t.TypeVar("TBp", bound="Blueprint")

RESUME_SUFFIX: t.Final[str] = "resume"
"""Suffix appended to the stem of a derived, resume-ready blueprint file."""


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
    if search_dir.is_dir():
        for partitioned in (True, False):
            for candidate in RestartFile.candidates(
                search_dir, partitioned=partitioned
            ):
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


def _locate_ucla_roms_restarts(
    bp: UclaRomsBlueprint, fs: RomsFileSystemManager
) -> tuple[Path, int | None]:
    """Say where a ucla_roms run left its restarts and how many pieces each has.

    Parameters
    ----------
    bp : UclaRomsBlueprint
        The blueprint of the interrupted run.
    fs : RomsFileSystemManager
        The file layout of the run's working directory.

    Returns
    -------
    tuple[Path, int | None]
        The directory to search, and the number of partition pieces a complete
        restart has (`None` for whole files, written by a ParallelIO run).

    Raises
    ------
    CstarExpectationFailed
        If a non-ParallelIO blueprint lacks the processor grid needed to
        know how many restart pieces to expect.
    """
    if bp.partitioning.use_pio:
        return fs.output_dir, None

    n_procs_x, n_procs_y = bp.partitioning.n_procs_x, bp.partitioning.n_procs_y
    if n_procs_x is None or n_procs_y is None:
        msg = (
            f"Blueprint {bp.name!r} cannot be resumed: partitioning.n_procs_x "
            "and n_procs_y must both be set to know how many restart pieces "
            "to expect"
        )
        raise CstarExpectationFailed(msg)
    return fs.temp_output_dir, n_procs_x * n_procs_y


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
    return resume_blueprint_from_restart(
        blueprint_uri,
        UclaRomsBlueprint,
        _locate_ucla_roms_restarts,
        end_date=lambda bp: bp.runtime_params.end_date,
    )


def resume_blueprint_from_restart(
    blueprint_uri: str,
    bp_type: "type[TBp]",
    locate: "Callable[[TBp, RomsFileSystemManager], tuple[Path, int | None]]",
    *,
    end_date: "Callable[[TBp], datetime]",
) -> str:
    """Build a resume-ready blueprint for any ROMS application's interrupted step.

    Reads the blueprint at `blueprint_uri`, locates the newest usable restart
    the step's own working directory holds, and -- when one is found and the
    run has not already finished -- writes a derived blueprint that continues
    from it. The original blueprint file is left untouched. The restart is
    applied as the overrides `RestartFileTrxAdapter` builds, so the blueprint
    must have `runtime_params.start_date` and `initial_conditions` like a
    ucla_roms one.

    Parameters
    ----------
    blueprint_uri : str
        The URI (local path or remote resource) of the blueprint to resume.
    bp_type : type[TBp]
        The blueprint class the application reads.
    locate : Callable[[TBp, RomsFileSystemManager], tuple[Path, int | None]]
        Says where the run's restarts are: given the blueprint and the file
        layout of its working directory, returns the directory to search and
        the number of partition pieces a complete restart has (`None` when
        restarts are whole files). May raise `CstarExpectationFailed` when the
        blueprint does not say enough to tell.
    end_date : Callable[[TBp], datetime]
        Reads the end of the run from the blueprint, to tell a run that
        stopped early from one that finished.

    Returns
    -------
    str
        The path to the derived, resume-ready blueprint, or `blueprint_uri`
        unchanged when no usable restart was found.

    Raises
    ------
    CstarExpectationFailed
        If `locate` does, or if the located restart is at or after the end of
        the run (the run appears to have already finished).
    """
    with local_copy(blueprint_uri) as bp_path:
        bp = load_blueprint(
            bp_path, bp_type, anchor=not is_remote_resource(blueprint_uri)
        )

    fs = RomsFileSystemManager(bp.effective_working_dir)
    search_dir, expected_pieces = locate(bp, fs)

    restart = find_resume_restart(search_dir, expected_pieces)

    if restart is None:
        log.warning(
            "No usable restart found in %s; resuming %s from its original "
            "initial conditions",
            search_dir,
            bp.name,
        )
        return blueprint_uri

    run_end = end_date(bp)
    if restart.timestamp >= run_end:
        msg = (
            f"last restart {restart.path.name} is at or after end_date "
            f"{run_end}; the run appears to have finished -- "
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
