"""`cstar run compress`: compress the data files of a finished run in place.

The command reads the header of every data file of the run, groups the files
into types (`FileTypeDetector`), and asks the same few questions about each
type. The answers are written as a `compress` blueprint, which can be run here
(`--now`), submitted as a SLURM job (`--submit`, through a one-step workplan
carrying the job's resources), or kept and replayed on another run (`--answers`).
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

from cstar.applications.core import get_application
from cstar.archive.compress import (
    APP_NAME,
    ChunkSettings,
    CompressBlueprint,
    FileTypeSettings,
    compress_run,
    default_settings,
    run_data_files,
    settings_for,
)
from cstar.archive.detect import FileType, FileTypeDetector
from cstar.entrypoint.utils import (
    ARG_ANSWERS,
    ARG_ANSWERS_HELP,
    ARG_DRY_RUN,
    ARG_DRY_RUN_HELP,
    ARG_NOW,
    ARG_NOW_HELP,
    ARG_SUBMIT,
    ARG_SUBMIT_HELP,
    ARG_YES,
)
from cstar.orchestration.manifest import fold_step_records, read_run_manifest
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.utils import ENV_CSTAR_SLURM_ACCOUNT, ENV_CSTAR_SLURM_QUEUE
from cstar.roms.outputs import OUTPUT_STREAMS

app = typer.Typer()

COMMAND_COMPRESS: t.Final[str] = "compress"
"""Name of the command that compresses a run's data files."""
COMPRESS_DIR_NAME: t.Final[str] = "compress"
"""The run subdirectory the blueprints and workplans of `cstar run compress` are written to."""
DEFAULT_DIGITS: t.Final[int] = 4
"""The significant digits proposed for quantization."""
_THROUGHPUT_BYTES_PER_S: t.Final[float] = 50e6
"""A conservative per-worker compression rate for the walltime estimate (GPFS, CDF-5 in)."""
_INPUT_LIKE_KINDS: t.Final[frozenset[str]] = frozenset({"restart", "boundary_forcing"})
"""Output kinds other runs read as input (continue-from, nesting)."""

HELP_SHORT: t.Final[str] = "Compress the data files of a finished run in place."
HELP_LONG: t.Final[str] = f"""{HELP_SHORT}

Every data file of the run's completed steps is read for its header, and files
are grouped into types by file name and variables. For each type you choose
whether to compress it (lossless chunked, deflated netCDF-4), whether to cast
variables that are not protected to float32, whether to also quantize them, and
the chunk shape. Protected variables (those an application names, such as the
CDR metrics inputs and carbonate perturbation pairs of UCLA-ROMS) are kept exact.

The answers are saved as a `compress` blueprint in `<run>/compress/`; replay
them with `{ARG_ANSWERS}`. Each file is verified against its original before it
replaces it. Compressed restart, boundary and forge files are netCDF-4, which a
CPU ROMS build with PIO cannot read as input.
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
    """Whether other runs read files of this type as input (restarts, nesting, forge)."""
    if file_type.application == "forge":
        return True
    name = Path(file_type.match.filename).name
    return any(
        name.startswith(f"{stem}.")
        for stem, kind, _ in OUTPUT_STREAMS
        if kind.value in _INPUT_LIKE_KINDS
    )


def _describe(file_type: FileType, settings: FileTypeSettings) -> str:
    """One line describing a file type."""
    n3, n2 = file_type.counts()
    spatial = file_type.sample.spatial_variables()
    from cstar.base.netcdf_compress import matches_any

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
    """The variables an application asks compression to keep exact."""
    try:
        return list(get_application(application).protected_variables())
    except Exception:  # an unknown application protects nothing it named
        return []


def _job_settings(total_bytes: int, yes: bool) -> dict[str, t.Any]:
    """Ask for the SLURM resources of a submitted job; returns `compute_overrides.slurm`."""
    cpus = (
        8
        if yes
        else typer.prompt("  Cores (files compressed at once)", default=8, type=int)
    )
    hours = max(
        1,
        math.ceil(1.5 * total_bytes / (max(1, cpus) * _THROUGHPUT_BYTES_PER_S) / 3600),
    )
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


def _write_yaml(path: Path, data: dict[str, t.Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path


@app.command(name=COMMAND_COMPRESS, help=HELP_LONG, short_help=HELP_SHORT)
def compress(
    run: t.Annotated[
        Path, typer.Argument(help="The run directory (holding cstar-run.yaml).")
    ],
    answers: t.Annotated[
        Path | None, typer.Option(ARG_ANSWERS, help=ARG_ANSWERS_HELP)
    ] = None,
    submit: t.Annotated[bool, typer.Option(ARG_SUBMIT, help=ARG_SUBMIT_HELP)] = False,
    now: t.Annotated[bool, typer.Option(ARG_NOW, help=ARG_NOW_HELP)] = False,
    yes: t.Annotated[bool, typer.Option(ARG_YES, help="Accept every default.")] = False,
    dry_run: t.Annotated[
        bool, typer.Option(ARG_DRY_RUN, help=ARG_DRY_RUN_HELP)
    ] = False,
) -> None:
    """Compress the data files of a finished run in place."""
    if submit and now:
        raise typer.BadParameter(f"{ARG_SUBMIT} cannot be combined with {ARG_NOW}")
    root = run.expanduser().resolve()
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
    files = run_data_files(root, manifest, FileTypeDetector())
    types = FileTypeDetector().detect(files)
    if not types:
        typer.echo("No data files with 2-D or 3-D variables were found.")
        raise typer.Exit(0)

    saved: CompressBlueprint | None = None
    if answers is not None:
        saved = deserialize(answers, CompressBlueprint)

    settings: list[FileTypeSettings] = []
    for file_type in types:
        known = (
            settings_for(file_type.files[0], saved.file_types)
            if saved is not None
            else None
        )
        if known is not None:
            typer.echo(f"\n{_describe(file_type, known)}\n  (settings from {answers})")
            if known not in settings:
                settings.append(known)
            continue
        proposed = default_settings(file_type, _protected(file_type.application))
        settings.append(_ask_type(file_type, proposed, yes))

    chosen = [s for s in settings if s.compress]
    total = sum(
        t_.total_bytes
        for t_ in types
        for s in chosen
        if s.match == t_.match and s.application == t_.application
    )
    typer.echo(
        f"\n{len(chosen)} of {len(settings)} file type(s) selected, {_human(total)} to compress."
    )

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
    slurm = _job_settings(total, yes) if submit else {}
    workers = int(slurm.get("num_cpus", 0)) or (
        saved.workers
        if saved is not None
        else 1
        if yes
        else typer.prompt("  Files to compress at once", default=1, type=int)
    )

    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    name = f"compress_{manifest.run_id}_{stamp}"
    blueprint = CompressBlueprint(
        name=name,
        description=f"Compress the data files of run {manifest.run_id}",
        run=root,
        file_types=settings,
        originals=originals,
        workers=workers,
        working_dir=root / COMPRESS_DIR_NAME / name,
    )
    bp_path = _write_yaml(
        root / COMPRESS_DIR_NAME / f"B_{name}.yaml",
        blueprint.model_dump(
            mode="json", exclude={"state", "provenance"}, exclude_none=True
        ),
    )
    typer.echo(f"Wrote {bp_path}")

    workplan_path: Path | None = None
    if submit:
        workplan_path = _write_yaml(
            root / COMPRESS_DIR_NAME / f"W_{name}.yaml",
            {
                "name": name,
                "description": blueprint.description,
                "steps": [
                    {
                        "name": "compress",
                        "application": APP_NAME,
                        "blueprint": str(bp_path),
                        "compute_overrides": {"slurm": slurm},
                    }
                ],
            },
        )
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
