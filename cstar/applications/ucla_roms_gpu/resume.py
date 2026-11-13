"""Resume an interrupted GPU ROMS step from its own restart output.

The GPU model's writer ranks write whole restart files straight into `output`,
with no partitioned pieces to join afterwards, so a resume looks there for the
newest restart that opens as netCDF, and never in `temp_output`. The search, the
derived `<stem>.resume.yaml` blueprint and the checks that guard them are the
ones every ROMS application shares
(`cstar.applications.ucla_roms.resume.resume_blueprint_from_restart`).
"""

from pathlib import Path

from cstar.applications.ucla_roms.resume import resume_blueprint_from_restart
from cstar.applications.ucla_roms_gpu.models import UclaRomsGpuBlueprint
from cstar.roms.file_system import RomsFileSystemManager


def _locate_gpu_restarts(
    bp: UclaRomsGpuBlueprint, fs: RomsFileSystemManager
) -> tuple[Path, int | None]:
    """Say where a GPU run leaves its restarts and how many pieces each has.

    Parameters
    ----------
    bp : UclaRomsGpuBlueprint
        The blueprint of the interrupted run.
    fs : RomsFileSystemManager
        The file layout of the run's working directory.

    Returns
    -------
    tuple[Path, int | None]
        The run's `output` directory, and `None` for the number of pieces: a GPU
        run writes whole restart files.
    """
    return fs.output_dir, None


def prepare_resume_blueprint(blueprint_uri: str) -> str:
    """Build a resume-ready blueprint from an interrupted GPU step's own output.

    Reads the blueprint at `blueprint_uri`, locates the newest usable restart in
    the step's `output` directory and -- when one is found and the run has not
    already finished -- writes a derived blueprint that continues from it. The
    original blueprint file is left untouched.

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
        If the located restart is at or after `runtime_params.end_date` (the run
        appears to have already finished).
    """
    return resume_blueprint_from_restart(
        blueprint_uri,
        UclaRomsGpuBlueprint,
        _locate_gpu_restarts,
        end_date=lambda bp: bp.runtime_params.end_date,
    )
