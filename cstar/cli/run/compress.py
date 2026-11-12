"""`cstar run compress`: compress the data files of a finished run in place.

The command reads the header of every data file of the run, groups the files
into types (`FileTypeDetector`), and asks the same few questions about each
type. The settings are written as a `compress` blueprint, which can be run here
(`--now`), submitted as a SLURM job (`--submit`, through a one-step workplan
carrying the job's resources), or reused on another run (`--blueprint`).
"""

import math
import os
import shutil
import subprocess
import sys
import typing as t
from datetime import datetime
from pathlib import Path

import typer
import yaml

from cstar.applications.compress.blueprint import (
    APP_NAME,
    ChunkSettings,
    CompressBlueprint,
    FileTypeSettings,
)
from cstar.applications.compress.detect import FileType, FileTypeDetector
from cstar.applications.compress.run import (
    compress_run,
    default_settings,
    login_node_warning,
    run_data_files,
    settings_for,
)
from cstar.applications.core import ArtifactKind, get_application
from cstar.base.exceptions import CstarError
from cstar.base.netcdf_compress import VERIFIED_RECORDS, matches_any
from cstar.cli.common import try_get_from_ctxmap
from cstar.cli.run.shared import resolve_by_handle
from cstar.cli.workplan.shared import HELP_RUN_HANDLE, KEY_RUN
from cstar.entrypoint.utils import (
    ARG_BLUEPRINT,
    ARG_BLUEPRINT_HELP,
    ARG_DRY_RUN,
    ARG_DRY_RUN_HELP,
    ARG_NOW,
    ARG_NOW_HELP,
    ARG_SUBMIT,
    ARG_SUBMIT_HELP,
    ARG_WORKERS,
    ARG_WORKERS_HELP,
    ARG_YES,
)
from cstar.orchestration.manifest import fold_step_records, read_run_manifest
from cstar.orchestration.models import Step, Workplan
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.tracking import WorkplanRun, require_writable
from cstar.orchestration.utils import ENV_CSTAR_SLURM_ACCOUNT, ENV_CSTAR_SLURM_QUEUE
from cstar.roms.outputs import OUTPUT_STREAMS
from cstar.system.manager import get_sysmgr, outside_scheduler_job

app = typer.Typer()

COMMAND_COMPRESS: t.Final[str] = "compress"
"""Name of the command that compresses a run's data files."""
COMPRESS_DIR_NAME: t.Final[str] = "compress"
"""The run subdirectory the blueprints and workplans of `cstar run compress` are written to."""
DEFAULT_DIGITS: t.Final[int] = 4
"""The significant digits proposed for quantization."""
DEFAULT_CORES: t.Final[int] = 8
"""The cores proposed for a submitted job: one worker each."""
_THROUGHPUT_BYTES_PER_S: t.Final[float] = 50e6
"""A conservative per-worker compression rate for the walltime estimate (GPFS, CDF-5 in)."""
_MEMORY_PER_RECORD: t.Final[float] = 3.5
"""Peak worker memory as a multiple of the largest record it copies (measured:
the record, its cast copy and netCDF-C's buffers)."""
_SHARED_QUEUE_BYTES_PER_CORE: t.Final[int] = 2 * 1024**3
"""What a shared queue typically allots per core, for the memory warning."""
_INPUT_LIKE_KINDS: t.Final[frozenset[ArtifactKind]] = frozenset(
    {
        ArtifactKind.restart,
        ArtifactKind.boundary_forcing,
        ArtifactKind.gas_exchange,
        ArtifactKind.grid,
        ArtifactKind.initial_conditions,
        ArtifactKind.surface_forcing,
        ArtifactKind.tidal_forcing,
        ArtifactKind.river_forcing,
        ArtifactKind.cdr_forcing,
    }
)
"""Product kinds a ROMS run reads as input, where the file format matters:
restarts (`continue-from`), nesting boundaries (`nest-from`), carbonate
sensitivities (`carbonate-sensitivity-from`) and every generated input. Output
read only by Python steps (`_cdr` by CDR metrics, `_uscl` by the upscaler) is
not here: those read netCDF-4, and the variables they need are protected."""

HELP_SHORT: t.Final[str] = "Compress the data files of a finished run in place."
HELP_LONG: t.Final[str] = f"""{HELP_SHORT}

Every data file of the run's completed steps is read for its header, and files
are grouped into types by file name and variables. For each type you choose
whether to compress it (lossless chunked, deflated netCDF-4), whether to cast
variables that are not protected to float32, whether to also quantize them, and
the chunk shape. Protected variables (those an application names, such as the
CDR metrics inputs and carbonate perturbation pairs of UCLA-ROMS) are kept exact.

The settings are saved as a `compress` blueprint in `<run>/compress/`; reuse
them with `{ARG_BLUEPRINT}`. Each file is verified against its original on
{VERIFIED_RECORDS} before it replaces it. Compressed restart, boundary and forge
files are netCDF-4, which a CPU ROMS build with PIO cannot read as input.
"""


def _yn(question: str, default: bool, yes: bool) -> bool:
    """Ask a yes/no question; `--yes` takes the default."""
    return default if yes else typer.confirm(f"  {question}", default=default)


def _human(nbytes: float) -> str:
    """Format bytes with a decimal unit."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1000 or unit == "TB":
            return f"{nbytes:.0f} {unit}" if unit == "B" else f"{nbytes:.1f} {unit}"
        nbytes /= 1000
    return ""


def _input_like(file_type: FileType) -> bool:
    """Whether a ROMS run reads files of this type as input (restarts, nesting
    boundaries, carbonate sensitivities, forge inputs).

    The product kinds the step record gives the files decide; files recorded
    without a kind (a step without products) fall back to the ROMS output
    stream named by the file, and forge files are always inputs.
    """
    if file_type.application == "forge":
        return True
    if file_type.kinds:
        return any(kind in _INPUT_LIKE_KINDS for kind in file_type.kinds)
    name = Path(file_type.match.filename).name
    return any(
        name.startswith(f"{stem}.")
        for stem, kind, _ in OUTPUT_STREAMS
        if kind in _INPUT_LIKE_KINDS
    )


def _describe(file_type: FileType, settings: FileTypeSettings) -> str:
    """One line describing a file type."""
    n3, n2 = file_type.counts()
    spatial = file_type.sample.spatial_variables()
    protected = sum(1 for n in spatial if matches_any(n, settings.protected))
    return (
        f"{file_type.name} [{file_type.application}] {file_type.match.filename}: "
        f"{len(file_type.files)} file(s), {_human(file_type.total_bytes)}, "
        f"{n3} 3-D + {n2} 2-D variables, {protected} protected"
    )


def _ask_chunks(file_type: FileType, yes: bool) -> ChunkSettings:
    """Ask for a chunk override, showing the derived default as x,y,z."""
    default = file_type.default_chunks()
    shown = ",".join(str(v) for v in (default.x, default.y, default.z) if v is not None)
    if yes:
        return ChunkSettings()
    answer = typer.prompt(
        f"  Chunking (x,y,z) [default {shown}]", default="", show_default=False
    )
    if not answer.strip():
        return ChunkSettings()
    values = [int(v) for v in answer.replace(" ", "").split(",") if v]
    return ChunkSettings(**dict(zip(("x", "y", "z"), values)))


def _ask_type(
    file_type: FileType, settings: FileTypeSettings, yes: bool
) -> FileTypeSettings:
    """Ask the questions of one file type."""
    typer.echo(f"\n{_describe(file_type, settings)}")
    input_like = _input_like(file_type)
    if input_like:
        typer.echo(
            "  Note: other runs read these as input; compressed (netCDF-4) they cannot "
            "feed a CPU ROMS build with PIO."
        )
    settings.compress = _yn(f"Compress {file_type.name}?", not input_like, yes)
    if not settings.compress:
        return settings
    if settings.protected == ["*"]:
        typer.echo("  Every variable is protected: lossless compression only.")
    else:
        settings.float32 = _yn("Float32-compress non-protected variables?", False, yes)
        if settings.float32 and _yn(
            "Additionally bit-reduce (quantize) them?", False, yes
        ):
            settings.significant_digits = (
                DEFAULT_DIGITS
                if yes
                else typer.prompt(
                    "  Significant digits to keep", default=DEFAULT_DIGITS, type=int
                )
            )
    settings.chunks = _ask_chunks(file_type, yes)
    return settings


def _protected(application: str) -> list[str]:
    """The variables an application asks compression to keep exact.

    An application this build does not know protects nothing, and says so: the
    user is then choosing float32 for variables nobody vouched for.
    """
    try:
        return list(get_application(application).protected_variables())
    except ValueError:
        typer.echo(
            f"  Warning: application {application!r} is not known to this build; "
            "no variable is protected."
        )
        return []


def _selected(
    types: t.Sequence[FileType], settings: t.Sequence[FileTypeSettings]
) -> list[FileType]:
    """The types the run will compress: those whose most specific match says so,
    the way `plan_jobs` resolves each file.
    """
    return [
        t_
        for t_ in types
        if getattr(settings_for(t_.files[0], settings), "compress", False)
    ]


def _worker_memory(selected: t.Sequence[FileType]) -> int:
    """The memory one worker needs, from the largest record of the selected types."""
    largest = max((t_.sample.largest_record_bytes() for t_ in selected), default=0)
    return int(largest * _MEMORY_PER_RECORD)


def _memory_line(per_worker: int, workers: int) -> str:
    """Describe the job's memory: per worker and in total, with a shared-queue hint."""
    total = per_worker * workers
    line = f"  Memory: about {_human(per_worker)} per worker, {_human(total)} for {workers}"
    if total > workers * _SHARED_QUEUE_BYTES_PER_CORE:
        line += (
            f" (more than the ~{_human(_SHARED_QUEUE_BYTES_PER_CORE)} per core a shared "
            "queue allots: use fewer workers, or a whole node)"
        )
    return line


def _compress_seconds(total_bytes: int, workers: int) -> float:
    """The estimated time to compress `total_bytes` with `workers` workers."""
    return total_bytes / (max(1, workers) * _THROUGHPUT_BYTES_PER_S)


def _duration(seconds: float) -> str:
    """Format a duration: minutes under an hour, else hours with one decimal."""
    if seconds < 3600:
        return f"{max(1, math.ceil(seconds / 60))} min"
    return f"{seconds / 3600:.1f} h"


def _job_settings(
    total_bytes: int, per_worker: int, yes: bool, cpus: int | None
) -> dict[str, t.Any]:
    """Ask for the SLURM resources of a submitted job; returns `compute_overrides.slurm`.

    `cpus` skips the question about the cores.
    """
    typer.echo(
        f"  Each worker compresses one file at a time and needs about {_human(per_worker)}."
    )
    if cpus is None:
        cpus = (
            DEFAULT_CORES
            if yes
            else typer.prompt(
                "  Cores (files compressed at once)", default=DEFAULT_CORES, type=int
            )
        )
    typer.echo(_memory_line(per_worker, max(1, cpus)))
    hours = max(1, math.ceil(1.5 * _compress_seconds(total_bytes, cpus) / 3600))
    walltime = f"{hours:02d}:00:00"
    if not yes:
        walltime = typer.prompt("  Walltime (HH:MM:SS)", default=walltime)
    queue = os.getenv(ENV_CSTAR_SLURM_QUEUE, "")
    account = os.getenv(ENV_CSTAR_SLURM_ACCOUNT, "")
    if not yes:
        queue = typer.prompt("  Queue", default=queue or "shared")
        account = typer.prompt("  Account", default=account, show_default=bool(account))
    slurm: dict[str, t.Any] = {
        "num_cpus": cpus,
        "max_walltime": walltime,
        "single_node": True,
    }
    if queue:
        slurm["queue_name"] = queue
    if account:
        slurm["account_name"] = account
    return slurm


def _resolve_run(context: typer.Context, target: str) -> Path:
    """The run directory of a run handle; a tracked run must be writable.

    Raises
    ------
    typer.BadParameter
        Raised when the handle names no run, or the run is attached read-only.
    """

    def tracked(run: WorkplanRun) -> Path:
        if not run.output_path.is_dir():
            msg = f"The directory recorded for run {run.run_id!r} does not exist: {run.output_path}"
            raise typer.BadParameter(msg)
        return run.output_path

    root = resolve_by_handle(
        context,
        target,
        verb=COMMAND_COMPRESS,
        directory=Path,
        tracked=tracked,
        indexed=Path,
    )
    if run := try_get_from_ctxmap(context, KEY_RUN, WorkplanRun):
        try:
            require_writable(run, action=COMMAND_COMPRESS)
        except CstarError as ex:
            raise typer.BadParameter(str(ex)) from ex
    return root.expanduser().resolve()


@app.command(name=COMMAND_COMPRESS, help=HELP_LONG, short_help=HELP_SHORT)
def compress(
    context: typer.Context,
    run: t.Annotated[str, typer.Argument(help=HELP_RUN_HANDLE)],
    blueprint_path: t.Annotated[
        Path | None, typer.Option(ARG_BLUEPRINT, help=ARG_BLUEPRINT_HELP)
    ] = None,
    submit: t.Annotated[bool, typer.Option(ARG_SUBMIT, help=ARG_SUBMIT_HELP)] = False,
    now: t.Annotated[bool, typer.Option(ARG_NOW, help=ARG_NOW_HELP)] = False,
    yes: t.Annotated[bool, typer.Option(ARG_YES, help="Accept every default.")] = False,
    workers: t.Annotated[
        int | None, typer.Option(ARG_WORKERS, help=ARG_WORKERS_HELP, min=1)
    ] = None,
    dry_run: t.Annotated[
        bool, typer.Option(ARG_DRY_RUN, help=ARG_DRY_RUN_HELP)
    ] = False,
) -> None:
    """Compress the data files of a finished run in place."""
    if submit and now:
        raise typer.BadParameter(f"{ARG_SUBMIT} cannot be combined with {ARG_NOW}")
    root = _resolve_run(context, run)
    manifest = read_run_manifest(root)
    if manifest is None:
        raise typer.BadParameter(
            f"{str(root)!r} holds no run manifest (cstar-run.yaml)"
        )
    if manifest.attempts and manifest.attempts[-1].finished_at is None:
        raise typer.BadParameter(
            f"the latest attempt of {manifest.run_id!r} has not finished"
        )
    fold_step_records(manifest, root)

    typer.echo(f"Reading the headers of the data files of {manifest.run_id!r}...")
    detector = FileTypeDetector()
    types = detector.detect(run_data_files(root, manifest, detector))
    if not types:
        typer.echo("No data files with 2-D or 3-D variables were found.")
        raise typer.Exit(0)

    saved: CompressBlueprint | None = None
    if blueprint_path is not None:
        saved = deserialize(blueprint_path, CompressBlueprint)

    settings: list[FileTypeSettings] = []
    for file_type in types:
        known = (
            settings_for(file_type.files[0], saved.file_types)
            if saved is not None
            else None
        )
        if known is not None:
            typer.echo(
                f"\n{_describe(file_type, known)}\n  (settings from {blueprint_path})"
            )
            if known not in settings:
                settings.append(known)
            continue
        proposed = default_settings(file_type, _protected(file_type.application))
        settings.append(_ask_type(file_type, proposed, yes))

    chosen = _selected(types, settings)
    total = sum(t_.total_bytes for t_ in chosen)
    typer.echo(
        f"\n{len(chosen)} of {len(types)} file type(s) selected, {_human(total)} to compress."
    )
    per_worker = _worker_memory(chosen)

    originals: t.Literal["delete", "keep"] = (
        saved.originals
        if saved is not None
        else (
            "delete"
            if _yn(
                "Delete each original once its compressed copy is verified?", False, yes
            )
            else "keep"
        )
    )

    if not submit and not now:
        has_slurm = shutil.which("sbatch") is not None
        submit = _yn("Submit as a SLURM job?", has_slurm, yes) if has_slurm else False
    slurm = _job_settings(total, per_worker, yes, workers) if submit else {}
    n_workers = int(slurm.get("num_cpus", 0))
    if not n_workers:
        if (
            chosen
            and not submit
            and not dry_run
            and (where := outside_scheduler_job(get_sysmgr()))
        ):
            hint = f"submit it as a job ({ARG_SUBMIT})"
            typer.echo("  Warning: " + login_node_warning(where, hint))
        if workers is not None:
            n_workers = workers
        elif saved is not None:
            n_workers = saved.workers
        elif yes:
            n_workers = 1
        else:
            typer.echo(
                f"  Each worker compresses one file at a time and needs about {_human(per_worker)}."
            )
            n_workers = typer.prompt("  Files to compress at once", default=1, type=int)
        if not submit:
            typer.echo(_memory_line(per_worker, max(1, n_workers)))
            typer.echo(
                f"  Time: about {_duration(_compress_seconds(total, n_workers))} "
                f"with {n_workers} worker(s)"
            )

    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    name = f"compress_{manifest.run_id}_{stamp}"
    blueprint = CompressBlueprint(
        name=name,
        description=f"Compress the data files of run {manifest.run_id}",
        run=root,
        file_types=settings,
        originals=originals,
        workers=n_workers,
        working_dir=root / COMPRESS_DIR_NAME / name,
    )
    # every setting written out, defaults included: the file is the record of the answers
    bp_path = root / COMPRESS_DIR_NAME / f"B_{name}.yaml"
    bp_path.parent.mkdir(parents=True, exist_ok=True)
    bp_path.write_text(
        yaml.safe_dump(
            blueprint.model_dump(
                mode="json", exclude={"state", "provenance"}, exclude_none=True
            ),
            sort_keys=False,
        )
    )
    typer.echo(f"Wrote {bp_path}")

    workplan_path: Path | None = None
    if submit:
        workplan = Workplan(
            name=name,
            description=blueprint.description,
            steps=[
                Step(
                    name=COMMAND_COMPRESS,
                    application=APP_NAME,
                    blueprint=str(bp_path),
                    compute_overrides={"slurm": slurm},
                )
            ],
        )
        workplan_path = root / COMPRESS_DIR_NAME / f"W_{name}.yaml"
        serialize(workplan_path, workplan)
        typer.echo(f"Wrote {workplan_path}")

    if dry_run or not chosen:
        typer.echo(
            "Nothing compressed." if not chosen else "Dry run: nothing compressed."
        )
        return

    if workplan_path is not None:
        cmd = [
            sys.executable,
            "-c",
            "import sys; from cstar.cli.cli import main; sys.argv[0] = 'cstar'; sys.exit(main())",
        ]
        typer.echo(f"Submitting: cstar workplan submit {workplan_path}")
        raise typer.Exit(
            subprocess.run([*cmd, "workplan", "submit", str(workplan_path)]).returncode
        )

    outcomes = compress_run(blueprint)
    failed = [o for o in outcomes if o.status == "failed"]
    before = sum(o.size_before for o in outcomes if o.status == "compressed")
    after = sum(o.size_after for o in outcomes if o.status == "compressed")
    typer.echo(
        f"Compressed {len(outcomes) - len(failed)} file(s): {_human(before)} -> {_human(after)}"
        + (
            f"; {len(failed)} failed and were kept (see compress-report.yaml)"
            if failed
            else ""
        )
    )
    if failed:
        raise typer.Exit(1)
