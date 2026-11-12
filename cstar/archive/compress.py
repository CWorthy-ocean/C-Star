"""The `compress` blueprint and the work of compressing a run's data files in place.

A `CompressBlueprint` holds the answers of `cstar run compress`: one
`FileTypeSettings` per detected file type, each with a portable `FileTypeMatch`,
so the same blueprint can be applied to another run. `compress_run` matches the
run's data files to those settings, compresses them in a process pool (each into
`<name>.partial`, verified against its source before it replaces it), and
records the new sizes in the run's step records.
"""

import os
import typing as t
from collections.abc import Iterator, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from pydantic import Field

from cstar.archive.detect import DataFile, FileType, FileTypeDetector, FileTypeMatch
from cstar.base.application_names import canonical_application
from cstar.base.log import get_logger
from cstar.base.netcdf_compress import (
    DEFAULT_DEFLATE_LEVEL,
    ChunkSpec,
    Encoding,
    compress_netcdf,
    verify_compressed,
)
from cstar.orchestration.manifest import (
    RunManifest,
    fold_step_records,
    manifest_lock,
    product_digest,
    read_run_manifest,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import Blueprint, ConfiguredBaseModel

log = get_logger(__name__)

APP_NAME: t.Final[str] = "compress"
"""The unique identifier for the compress application type."""
PARTIAL_SUFFIX: t.Final[str] = ".partial"
"""Appended to a file being written; renamed away once verified."""
ORIGINALS_DIR_NAME: t.Final[str] = "uncompressed_originals"
"""Where originals are kept, under the run root, when they are not deleted."""
REPORT_NAME: t.Final[str] = "compress-report.yaml"
"""The per-file report written to the run root."""
COMPRESSION_KEY: t.Final[str] = "compression"
"""The product metadata key recording how a file was compressed."""
_SKIPPED_DIRS: t.Final[frozenset[str]] = frozenset(
    {"input", "temp_output", ORIGINALS_DIR_NAME}
)
"""Step directories never searched for data files: staged inputs, per-rank
pieces, and kept originals."""


class ChunkSettings(ConfiguredBaseModel):
    """A chunk-shape override along x, y and z; an unset axis is derived."""

    x: int | None = Field(default=None, gt=0)
    y: int | None = Field(default=None, gt=0)
    z: int | None = Field(default=None, gt=0)


class FileTypeSettings(ConfiguredBaseModel):
    """How to compress one type of data file."""

    name: str
    """A label for the type, e.g. `output_cdr`."""
    application: str
    """The application of the steps whose files are this type."""
    match: FileTypeMatch
    """Which files are this type."""
    compress: bool = False
    """Rewrite the files as chunked, deflated netCDF-4 (lossless on its own)."""
    float32: bool = False
    """Cast float64 variables that are not protected to float32."""
    significant_digits: int | None = Field(default=None, gt=0)
    """With `float32`, also quantize those variables (BitGroom) to this many
    significant decimal digits."""
    chunks: ChunkSettings = Field(default_factory=ChunkSettings)
    """Chunk-shape overrides; unset axes are derived from each variable."""
    protected: list[str] = Field(default_factory=list)
    """Variables (names or globs) kept exact; filled from the application's
    `protected_variables` and editable."""

    def encoding(self, deflate_level: int) -> Encoding:
        """The engine encoding these settings describe."""
        return Encoding(
            float32=self.float32,
            significant_digits=self.significant_digits if self.float32 else None,
            deflate_level=deflate_level,
            chunks=ChunkSpec(x=self.chunks.x, y=self.chunks.y, z=self.chunks.z),
            protected=tuple(self.protected),
        )


class CompressBlueprint(Blueprint):
    """A blueprint compressing the data files of a finished run in place."""

    application: str = APP_NAME
    """The application identifier."""
    run: Path
    """The root directory of the run to compress."""
    file_types: list[FileTypeSettings] = Field(min_length=1)
    """The settings of each file type, tried in order; a file matching none is left as it is."""
    originals: t.Literal["delete", "keep"] = "keep"
    """What happens to an original once its compressed copy is verified: deleted,
    or moved to `<run>/uncompressed_originals/`."""
    deflate_level: int = Field(default=DEFAULT_DEFLATE_LEVEL, ge=0, le=9)
    """The zlib deflate level."""
    workers: int = Field(default=1, ge=1)
    """Files compressed at once, each in its own process."""


@dataclass
class FileJob:
    """One file to compress, as handed to a worker process."""

    step: str
    step_root: Path
    relative_path: str
    file_type: str
    encoding: Encoding
    originals_dir: Path | None
    """Where to move the original; `None` deletes it."""


@dataclass
class FileOutcome:
    """What happened to one file."""

    step: str
    relative_path: str
    file_type: str
    status: t.Literal["compressed", "failed"]
    size_before: int
    size_after: int = 0
    reduced: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)


def _canonical(manifest: RunManifest) -> dict[str, str]:
    """The canonical application of each step, by step name."""
    return {e.name: canonical_application(e.application) for e in manifest.steps}


def _walk_data_candidates(step_root: Path) -> Iterator[tuple[str, int]]:
    """The `*.nc` files under a step root that has recorded no products.

    Symlinks and the staged-input, per-rank and kept-originals directories are skipped.
    """
    for dirpath, dirnames, filenames in os.walk(step_root):
        here = Path(dirpath)
        dirnames[:] = sorted(
            d
            for d in dirnames
            if d not in _SKIPPED_DIRS and not (here / d).is_symlink()
        )
        for name in sorted(filenames):
            path = here / name
            if name.endswith(".nc") and not path.is_symlink():
                yield path.relative_to(step_root).as_posix(), path.stat().st_size


def run_data_files(
    root: Path, manifest: RunManifest, detector: FileTypeDetector
) -> list[DataFile]:
    """The data files of a run's completed steps.

    A step's recorded products are its files; a step recorded without products
    (C-Star 0.17 and earlier) is searched for `*.nc` files instead.

    Parameters
    ----------
    root : Path
        The run root.
    manifest : RunManifest
        The run manifest, with step records folded in.
    detector : FileTypeDetector
        Reads the headers.

    Returns
    -------
    list[DataFile]
    """
    applications = _canonical(manifest)
    candidates: list[tuple[str, str, Path, str, int]] = []
    for entry in manifest.steps:
        if entry.record is None or entry.record.outcome != "completed":
            log.warning(
                f"Step {entry.name!r} did not complete; its files are left as they are"
            )
            continue
        step_root = root / entry.working_dir
        products = entry.record.products
        if products:
            found = [
                (p.path, p.size_bytes or (step_root / p.path).stat().st_size)
                for p in products
                if (step_root / p.path).is_file()
            ]
        else:
            found = list(_walk_data_candidates(step_root))
        candidates.extend(
            (entry.name, applications[entry.name], step_root, rel, size)
            for rel, size in found
        )
    return detector.data_files(candidates)


def settings_for(
    data_file: DataFile, file_types: Sequence[FileTypeSettings]
) -> FileTypeSettings | None:
    """The first settings whose application and match fit a file, or `None`."""
    for settings in file_types:
        if canonical_application(settings.application) == data_file.application and (
            settings.match.matches(data_file.relative_path, data_file.header)
        ):
            return settings
    return None


def default_settings(file_type: FileType, protected: Sequence[str]) -> FileTypeSettings:
    """The settings `cstar run compress` proposes for a detected type, before any answer."""
    return FileTypeSettings(
        name=file_type.name,
        application=file_type.application,
        match=file_type.match,
        protected=list(protected),
    )


def _compress_one(job: FileJob) -> FileOutcome:
    """Compress, verify and replace one file (runs in a worker process)."""
    src = job.step_root / job.relative_path
    partial = src.with_name(src.name + PARTIAL_SUFFIX)
    outcome = FileOutcome(
        job.step, job.relative_path, job.file_type, "failed", src.stat().st_size
    )
    try:
        result = compress_netcdf(src, partial, job.encoding)
        problems = verify_compressed(src, partial, job.encoding)
    except Exception as ex:  # one bad file must not stop the others
        problems = [f"{type(ex).__name__}: {ex}"]
        result = None
    if problems:
        partial.unlink(missing_ok=True)
        outcome.problems = problems
        return outcome

    if job.originals_dir is not None:
        kept = job.originals_dir / job.step / job.relative_path
        kept.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src, kept)
    os.replace(partial, src)
    outcome.status = "compressed"
    outcome.size_after = src.stat().st_size
    outcome.reduced = list(result.reduced) if result else []
    return outcome


def plan_jobs(
    blueprint: CompressBlueprint, files: Sequence[DataFile]
) -> tuple[list[FileJob], list[DataFile]]:
    """Match the run's files to the blueprint's settings.

    Returns
    -------
    tuple[list[FileJob], list[DataFile]]
        The files to compress, and the files no settings match.
    """
    root = blueprint.run
    originals = None if blueprint.originals == "delete" else root / ORIGINALS_DIR_NAME
    jobs, unmatched = [], []
    for data_file in files:
        settings = settings_for(data_file, blueprint.file_types)
        if settings is None:
            unmatched.append(data_file)
        elif settings.compress and data_file.header.deflated:
            log.info(
                f"{data_file.step}/{data_file.relative_path} is already compressed"
            )
        elif settings.compress:
            jobs.append(
                FileJob(
                    step=data_file.step,
                    step_root=data_file.step_root,
                    relative_path=data_file.relative_path,
                    file_type=settings.name,
                    encoding=settings.encoding(blueprint.deflate_level),
                    originals_dir=originals,
                )
            )
    return jobs, unmatched


def _record(root: Path, run_id: str, outcomes: Sequence[FileOutcome]) -> int:
    """Record the compressed files' sizes, digests and compression in the step records.

    A product whose values changed (float32) gets a fresh digest; a lossless
    rewrite keeps its digest, which does not depend on the encoding.

    Returns
    -------
    int
        The number of products updated.
    """
    done = {(o.step, o.relative_path): o for o in outcomes if o.status == "compressed"}
    if not done:
        return 0
    with manifest_lock(run_id):
        manifest = read_run_manifest(root)
        if manifest is None:
            return 0
        fold_step_records(manifest, root)
        updated = 0
        for entry in manifest.steps:
            if entry.record is None:
                continue
            step_root = root / entry.working_dir
            changed = False
            for product in entry.record.products:
                outcome = done.get((entry.name, product.path))
                if outcome is None:
                    continue
                product.size_bytes = outcome.size_after
                if outcome.reduced and product.digest:
                    product.digest = product_digest(step_root / product.path)
                product.metadata[COMPRESSION_KEY] = (
                    f"netcdf4;float32 x{len(outcome.reduced)}"
                    if outcome.reduced
                    else "netcdf4;lossless"
                )
                changed = True
                updated += 1
            if changed:
                write_step_record(step_root, entry.record)
        if updated:
            write_run_manifest(root, manifest)
    return updated


def _write_report(
    root: Path, outcomes: Sequence[FileOutcome], unmatched: Sequence[DataFile]
) -> Path:
    """Write the per-file report to the run root."""
    before = sum(o.size_before for o in outcomes if o.status == "compressed")
    after = sum(o.size_after for o in outcomes if o.status == "compressed")
    report = {
        "summary": {
            "compressed": sum(o.status == "compressed" for o in outcomes),
            "failed": sum(o.status == "failed" for o in outcomes),
            "unmatched": len(unmatched),
            "bytes_before": before,
            "bytes_after": after,
            "ratio": round(before / after, 2) if after else None,
        },
        "files": [
            {
                "step": o.step,
                "path": o.relative_path,
                "type": o.file_type,
                "status": o.status,
                "bytes_before": o.size_before,
                "bytes_after": o.size_after,
                "float32_variables": len(o.reduced),
                **({"problems": o.problems} if o.problems else {}),
            }
            for o in outcomes
        ],
        "unmatched": [f"{f.step}/{f.relative_path}" for f in unmatched],
    }
    path = root / REPORT_NAME
    path.write_text(yaml.safe_dump(report, sort_keys=False))
    return path


def compress_run(blueprint: CompressBlueprint) -> list[FileOutcome]:
    """Compress the data files of a run as the blueprint says.

    Parameters
    ----------
    blueprint : CompressBlueprint
        The run and the per-type settings.

    Returns
    -------
    list[FileOutcome]
        One outcome per file compressed or attempted.
    """
    root = blueprint.run
    manifest = read_run_manifest(root)
    if manifest is None:
        msg = f"{str(root)!r} holds no run manifest"
        raise FileNotFoundError(msg)
    fold_step_records(manifest, root)

    files = run_data_files(root, manifest, FileTypeDetector())
    jobs, unmatched = plan_jobs(blueprint, files)
    for f in unmatched:
        log.warning(
            f"No file type in the blueprint matches {f.step}/{f.relative_path}; left as it is"
        )
    log.info(f"Compressing {len(jobs)} file(s) with {blueprint.workers} worker(s)")

    outcomes: list[FileOutcome] = []
    with ProcessPoolExecutor(max_workers=blueprint.workers) as pool:
        for outcome in pool.map(_compress_one, jobs):
            outcomes.append(outcome)
            if outcome.status == "compressed":
                log.info(
                    f"{outcome.step}/{outcome.relative_path}: "
                    f"{outcome.size_before:,} -> {outcome.size_after:,} bytes"
                )
            else:
                log.error(
                    f"{outcome.step}/{outcome.relative_path} kept: {outcome.problems}"
                )

    _record(root, manifest.run_id, outcomes)
    report = _write_report(root, outcomes, unmatched)
    log.info(f"Wrote {report}")
    return outcomes
