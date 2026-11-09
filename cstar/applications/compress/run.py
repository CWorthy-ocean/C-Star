"""Compress a run's data files in place, as a `CompressBlueprint` says.

`compress_run` matches the run's data files to the blueprint's settings,
compresses them in a process pool (each into `<name>.partial`, verified against
its source before it replaces it), and records the new sizes in the run's step
records. Whatever happens to one file, the outcomes gathered so far are always
recorded and reported.
"""

import os
import typing as t
from collections.abc import Iterator, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from cstar.applications.compress.blueprint import CompressBlueprint, FileTypeSettings
from cstar.applications.compress.detect import (
    Candidate,
    DataFile,
    FileType,
    FileTypeDetector,
)
from cstar.base.application_names import canonical_application
from cstar.base.log import get_logger
from cstar.base.netcdf_compress import (
    VERIFIED_RECORDS,
    Encoding,
    compress_netcdf,
    verify_compressed,
)
from cstar.orchestration.manifest import (
    Product,
    RunManifest,
    fold_step_records,
    manifest_lock,
    product_digest,
    read_run_manifest,
    update_products,
)

log = get_logger(__name__)

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

    A step's recorded products are its files, each with its recorded kind; a
    step recorded without products (C-Star 0.17 and earlier) is searched for
    `*.nc` files instead.

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
    candidates: list[Candidate] = []
    for entry in manifest.steps:
        if entry.record is None or entry.record.outcome != "completed":
            log.warning(
                f"Step {entry.name!r} did not complete; its files are left as they are"
            )
            continue
        step_root = root / entry.working_dir
        application = applications[entry.name]
        if entry.record.products:
            candidates.extend(
                Candidate(
                    entry.name,
                    application,
                    step_root,
                    p.path,
                    p.size_bytes or (step_root / p.path).stat().st_size,
                    p.kind,
                )
                for p in entry.record.products
                if (step_root / p.path).is_file()
            )
        else:
            candidates.extend(
                Candidate(entry.name, application, step_root, rel, size)
                for rel, size in _walk_data_candidates(step_root)
            )
    return detector.data_files(candidates)


def settings_for(
    data_file: DataFile, file_types: Sequence[FileTypeSettings]
) -> FileTypeSettings | None:
    """The most specific settings whose application and match fit a file, or `None`.

    Among the settings that match, the one requiring the most variables wins
    (a type split from the same template requires the variables that tell it
    from its siblings); ties go to the earlier entry.
    """
    matching = [
        s
        for s in file_types
        if canonical_application(s.application) == data_file.application
        and s.match.matches(data_file.relative_path, data_file.header)
    ]
    return max(matching, key=lambda s: len(s.match.requires_variables), default=None)


def default_settings(file_type: FileType, protected: Sequence[str]) -> FileTypeSettings:
    """The settings `cstar run compress` proposes for a detected type, before any answer."""
    return FileTypeSettings(
        name=file_type.name,
        application=file_type.application,
        match=file_type.match,
        protected=list(protected),
    )


def _compress_one(job: FileJob) -> FileOutcome:
    """Compress, verify and replace one file (runs in a worker process).

    Whatever goes wrong, from reading the source to the final rename, becomes a
    `failed` outcome: one bad file must not stop the others, and the caller
    must learn of every file it touched. An original already moved aside when
    the rename fails is moved back, so a failed file is always where it was.
    """
    src = job.step_root / job.relative_path
    partial = src.with_name(src.name + PARTIAL_SUFFIX)
    kept = (
        None
        if job.originals_dir is None
        else job.originals_dir / job.step / job.relative_path
    )
    outcome = FileOutcome(job.step, job.relative_path, job.file_type, "failed", 0)
    try:
        outcome.size_before = src.stat().st_size
        result = compress_netcdf(src, partial, job.encoding)
        problems = verify_compressed(src, partial, job.encoding)
        if problems:
            partial.unlink(missing_ok=True)
            outcome.problems = problems
            return outcome
        if kept is not None:
            kept.parent.mkdir(parents=True, exist_ok=True)
            os.replace(src, kept)
        try:
            os.replace(partial, src)
        except OSError:
            if kept is not None and not src.exists():
                os.replace(kept, src)
            raise
        outcome.status = "compressed"
        outcome.size_after = src.stat().st_size
        outcome.reduced = list(result.reduced)
    except Exception as ex:
        partial.unlink(missing_ok=True)
        outcome.problems.append(f"{type(ex).__name__}: {ex}")
        if not src.exists():
            where = (
                f"moved to {kept}" if kept is not None and kept.exists() else "missing"
            )
            outcome.problems.append(f"original {where}")
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
    rewrite keeps its digest, which does not depend on the encoding. Digests
    (a bounded read per file) are computed before the manifest lock is taken.

    Returns
    -------
    int
        The number of products updated.
    """
    done = {(o.step, o.relative_path): o for o in outcomes if o.status == "compressed"}
    if not done:
        return 0
    manifest = read_run_manifest(root)
    if manifest is None:
        return 0
    step_dirs = {e.name: e.working_dir for e in manifest.steps}
    digests = {
        key: product_digest(root / step_dirs[key[0]] / key[1])
        for key, o in done.items()
        if o.reduced and key[0] in step_dirs
    }

    def update(step: str, product: Product) -> bool:
        outcome = done.get((step, product.path))
        if outcome is None:
            return False
        product.size_bytes = outcome.size_after
        if product.digest and (step, product.path) in digests:
            product.digest = digests[step, product.path]
        product.metadata[COMPRESSION_KEY] = (
            f"netcdf4;float32 x{len(outcome.reduced)}"
            if outcome.reduced
            else "netcdf4;lossless"
        )
        return True

    with manifest_lock(run_id):
        return update_products(root, update)


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
            "verified": VERIFIED_RECORDS,
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

    The step records and the report cover every file whose outcome is known,
    even when the pool is interrupted part way.

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
    try:
        with ProcessPoolExecutor(max_workers=blueprint.workers) as pool:
            # results are taken as they finish, so a broken pool loses none
            futures = [pool.submit(_compress_one, job) for job in jobs]
            for future in as_completed(futures):
                outcome = future.result()
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
    finally:
        # record-keeping never stops the run, nor the report of what happened
        try:
            if outcomes:
                _record(root, manifest.run_id, outcomes)
        except Exception as ex:
            log.warning(
                f"The step records of {manifest.run_id!r} were not updated: {ex}"
            )
        report = _write_report(root, outcomes, unmatched)
        log.info(f"Wrote {report}")
    return outcomes
