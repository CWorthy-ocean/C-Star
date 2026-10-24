"""The run manifest and the step record: what a run was and what it produced.

The run manifest `cstar-run.yaml` is one document per run directory, written by
the processes that schedule or observe the run. The step record `cstar-step.yaml`
is one document per step working directory, written by the step process at
shutdown. This module holds the formats and their IO only; it knows nothing of
the launchers that decide when they are written.

Every path in either document is a POSIX string relative to the run root (or, in
a step record, to the step root), so a run directory can be moved or copied
without invalidating them.
"""

import enum
import threading
import time
import typing as t
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import AfterValidator, Field, model_validator

from cstar.applications.core import ArtifactKind, ArtifactSpec, get_application
from cstar.base.digest import (
    HASH_SCHEME,
    KNOWN_SCHEMES,
    SHA256_SCHEME,
    hash_netcdf_contents,
    sha256_digest,
)
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.base.provenance import canonical_hash, package_versions, system_name
from cstar.base.utils import slugify, utc_now
from cstar.execution.file_system import (
    LockTimeoutError,
    StateDirectoryManager,
    is_remote_resource,
    local_copy,
    locked,
    write_text_atomic,
)
from cstar.orchestration.models import (
    BlueprintIdentity,
    CatalogSpecRef,
    ConfiguredBaseModel,
    DeferredBlueprintRef,
    InlineBlueprintRef,
)
from cstar.orchestration.orchestration import Status
from cstar.orchestration.serialization import (
    anchor_relative_path,
    load_blueprint,
    model_to_yaml,
    read_document,
)
from cstar.system.migration import CstarSchemaTooNewError, version_key

if t.TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from cstar.orchestration.models import Blueprint, RunRef, Step, Workplan

log = get_logger(__name__)

DOCUMENT_READ_ERRORS: t.Final[tuple[type[Exception], ...]] = (CstarError, OSError)
"""What reading a manifest or a step record raises for a document that cannot be
used: unreadable, invalid, newer than this build reads, or out of reach."""

RUN_MANIFEST_NAME: t.Final[str] = "cstar-run.yaml"
"""The file name of the run manifest, in the run root."""

STEP_RECORD_NAME: t.Final[str] = "cstar-step.yaml"
"""The file name of the step record, in the step's working directory."""

MANIFEST_LOCK_NAME: t.Final[str] = "manifest.lock"
"""The name of the lock file serializing run-manifest writers, in the run's state directory."""

MANIFEST_LOCK_TIMEOUT: t.Final[float] = 60.0
"""Seconds a writer waits for the manifest lock before it gives up: keeping the
record of a run never holds up the run for longer."""

_SUBMISSION_WRITERS: t.Final = threading.Lock()
"""Serializes this process's writers of submissions, which an orchestrator runs on
several threads at once: they queue here rather than poll the manifest lock."""

ATTEMPTS_DIR_NAME: t.Final[str] = "attempts"
"""The directory, in the run root, holding the inputs each attempt carried."""

ATTEMPT_DIR_FORMAT: t.Final[str] = "%Y%m%dT%H%M%S"
"""The `strftime` format naming an attempt's directory after its UTC start."""

ATTEMPT_WORKPLAN_NAME: t.Final[str] = "workplan.yaml"
"""The authored workplan, carried verbatim in an attempt's directory."""

ATTEMPT_VARIABLES_NAME: t.Final[str] = "workplan.vars"
"""The runtime variables of the attempt, in its directory, when any were given."""

ATTEMPT_TRANSFORMED_NAME: t.Final[str] = "workplan_transformed.yaml"
"""A copy of the transformed workplan the attempt prepared, in its directory."""

TRANSFORMED_WORKPLAN_SUFFIX: t.Final[str] = "_transformed"
"""What the transformed workplan's file stem adds to the authored workplan's."""

TRANSFORMED_WORKPLAN_GLOB: t.Final[str] = f"*{TRANSFORMED_WORKPLAN_SUFFIX}.yaml"
"""The transformed workplan a run directory holds, by name; one per run."""


def transformed_workplan_path(root: Path, authored: Path) -> Path:
    """The transformed workplan a run writes into its root for an authored workplan.

    The plan is named `.yaml` whatever the authored suffix.

    Parameters
    ----------
    root : Path
        The run root.
    authored : Path
        The authored workplan, as given when the run was scheduled.

    Returns
    -------
    Path
    """
    return root / f"{authored.stem}{TRANSFORMED_WORKPLAN_SUFFIX}.yaml"


ATTEMPT_BLUEPRINTS_DIR_NAME: t.Final[str] = "blueprints"
"""The directory, in an attempt's directory, holding each step's blueprint by step slug."""

_M = t.TypeVar("_M", bound=ConfiguredBaseModel)
_F = t.TypeVar("_F")


def _whole_second(moment: datetime) -> datetime:
    """Drop the microseconds of a moment: the documents record whole seconds."""
    return moment.replace(microsecond=0)


def _now() -> datetime:
    """The current UTC time, to the second."""
    return _whole_second(utc_now())


Moment = t.Annotated[datetime, AfterValidator(_whole_second)]
"""A timestamp recorded in the documents, kept to whole seconds."""


def carried_digest(path: Path) -> str:
    """Digest a carried file, in the form `Attempt.carried` records.

    Carried files are workplans, variables and blueprints, small by
    construction, so the whole file is read.

    Parameters
    ----------
    path : Path
        The file to digest.

    Returns
    -------
    str
        `sha256:<hex>` of the file's bytes.
    """
    return sha256_digest(path)


def product_digest(path: Path, *, scheme: str = "") -> str:
    """Digest a product's content, in the form `Product.digest` records.

    Parameters
    ----------
    path : Path
        The file to digest.
    scheme : str
        The scheme to compute (`sha256` or `nc-sample-v2`), as an existing digest
        names it. By default a netCDF file (a `.nc` suffix) takes `nc-sample-v2`,
        a bounded read, and any other file `sha256`, a read of the whole file.

    Returns
    -------
    str
        `<scheme>:<hex>`.

    Raises
    ------
    ValueError
        If `scheme` is not one this build computes.
    """
    if not scheme:
        scheme = HASH_SCHEME if path.suffix.casefold() == ".nc" else SHA256_SCHEME
    if scheme == HASH_SCHEME:
        return hash_netcdf_contents(path)
    if scheme == SHA256_SCHEME:
        return sha256_digest(path)
    known = " and ".join(repr(known) for known in KNOWN_SCHEMES)
    raise ValueError(f"unknown digest scheme {scheme!r}; this build computes {known}")


def create_attempt_dir(root: Path, start_at: datetime) -> Path:
    """Create the directory an attempt carries its inputs in.

    The directory is named after the UTC start of the attempt, with `-2`, `-3`,
    ... appended when an earlier attempt of the same second holds the name. A
    name is claimed by creating its directory, so concurrent preparers never
    share one. This is the only place the name is derived from the time;
    readers read `Attempt.directory`.

    Parameters
    ----------
    root : Path
        The run root.
    start_at : datetime
        When the attempt was scheduled.

    Returns
    -------
    Path
        The new, empty directory, `<root>/attempts/<name>`.
    """
    attempts = root / ATTEMPTS_DIR_NAME
    attempts.mkdir(parents=True, exist_ok=True)

    name = start_at.astimezone(UTC).strftime(ATTEMPT_DIR_FORMAT)
    suffix = 1
    while True:
        candidate = attempts / (name if suffix == 1 else f"{name}-{suffix}")
        try:
            candidate.mkdir()
        except FileExistsError:
            suffix += 1
        else:
            return candidate


class AttemptStatus(enum.StrEnum):
    """Where an attempt to run a workplan stands."""

    scheduled = "scheduled"
    running = "running"
    done = "done"
    failed = "failed"
    cancelled = "cancelled"


class Product(ConfiguredBaseModel):
    """A file or directory a step produced."""

    kind: str = Field(json_schema_extra={"examples": [k.value for k in ArtifactKind]})
    """The classification of the product: one of `ArtifactKind` when written by
    this release. Kept as text on disk so a record written with a kind this
    build no longer knows still loads; `known_kind` maps it for consumers."""

    path: str
    """The location of the product, relative to the step root."""

    size_bytes: int = Field(default=0)
    """The size of the product in bytes; zero when not measured."""

    sha256: str = Field(default="")
    """A bare sha256 of the product's bytes, written by no current release: kept so
    records that predate `digest` load, and folded into `digest` on load."""

    digest: str = Field(default="")
    """The content digest of the product, `<scheme>:<hex>`: `nc-sample-v2` for a
    netCDF file (structure plus a bounded sample of its data, independent of the
    on-disk encoding), `sha256` of the bytes otherwise. Empty when not computed;
    `cstar run check --record-digests` records it."""

    metadata: dict[str, str | int | float | list[str]] = Field(default_factory=dict)
    """Model-specific detail, such as an output stream."""

    @model_validator(mode="after")
    def _fold_sha256(self) -> "Product":
        """Read a bare `sha256` of an older record as the `digest` it stands for."""
        if self.sha256 and not self.digest:
            self.digest = f"{SHA256_SCHEME}:{self.sha256}"
        return self

    @property
    def known_kind(self) -> ArtifactKind:
        """The product's kind in this build's vocabulary, `other` when unknown.

        Returns
        -------
        ArtifactKind
        """
        try:
            return ArtifactKind(self.kind)
        except ValueError:
            return ArtifactKind.other


class ExecutionFacts(ConfiguredBaseModel):
    """What ran a step: the scheduler and the allocation it was granted, read from
    the step's own environment.

    These are the granted resources, never the request: the request is in the
    transformed workplan and the batch script. They describe the step's own
    process; a model run the step submits as a job of its own is not described.
    """

    scheduler: str = Field(default="")
    """`slurm` when the step ran inside a SLURM job, else `local`; empty in a
    record written before schema 1.2.0."""

    job_id: str = Field(default="")
    """The id of the SLURM job the step ran in; empty outside SLURM."""

    host: str = Field(default="")
    """The host the step process ran on; under SLURM, the batch node."""

    pid: int = Field(default=0)
    """The id of the step process."""

    nodes: int = Field(default=0)
    """The nodes granted to the job; zero outside SLURM or when not reported."""

    tasks: int = Field(default=0)
    """The tasks granted to the job; zero outside SLURM or when not reported."""

    cpus: int = Field(default=0)
    """The CPUs granted per node, on the node the step process ran on; zero outside
    SLURM or when not reported."""

    partition: str = Field(default="")
    """The partition the job ran in; empty outside SLURM or when not reported."""

    account: str = Field(default="")
    """The account the job was charged to; empty outside SLURM or when not reported."""


class StepRecord(ConfiguredBaseModel):
    """What a step did: written by the step process, in its working directory."""

    schema_version: str = Field(default="1.3.0")
    """The schema version for the document."""

    run_id: str = Field(default="")
    """The run the step belongs to; empty for a standalone `cstar blueprint run`."""

    step: str
    """The name of the step."""

    application: str
    """The application the step ran."""

    blueprint: BlueprintIdentity
    """The blueprint the step ran."""

    blueprint_schema_version: str = Field(default="")
    """The schema version of the blueprint the step ran."""

    blueprint_file: str = Field(default="")
    """The blueprint file the step ran, such as `work/<stem>.ovrd.yaml`, relative to
    the step root; empty when it lies outside the step root."""

    outcome: str
    """The runner's execution status as its lowercase name, e.g. `completed`."""

    start_at: Moment | None = Field(default=None)
    """When the step process started; `None` when it never started."""

    finished_at: Moment
    """When the step process recorded its outcome."""

    system: str = Field(default="")
    """The C-Star system the step ran on."""

    versions: dict[str, str] = Field(default_factory=dict)
    """The versions of the packages the step ran with."""

    execution: ExecutionFacts = Field(default_factory=ExecutionFacts)
    """What ran the step."""

    products: list[Product] = Field(default_factory=list)
    """What the step produced."""

    errors: list[str] = Field(default_factory=list)
    """The errors the step reported."""


class WorkplanIdentity(ConfiguredBaseModel):
    """Identifies the workplan a run was scheduled from."""

    name: str
    """The name of the workplan."""

    content_hash: str
    """A fingerprint of the workplan as authored."""

    path: str
    """The workplan file, as given on the command line."""


class Attempt(ConfiguredBaseModel):
    """One scheduling of a workplan into a run; a resumed run has several."""

    start_at: Moment
    """When the attempt was scheduled."""

    system: str = Field(default="")
    """The C-Star system the attempt was scheduled on."""

    versions: dict[str, str] = Field(default_factory=dict)
    """The versions of the packages the attempt was scheduled with."""

    workplan_content_hash: str = Field(default="")
    """A fingerprint of the workplan as this attempt ran it."""

    clobbered: list[str] = Field(default_factory=list)
    """The steps the attempt cleared and re-ran."""

    resumed: list[str] = Field(default_factory=list)
    """The steps the attempt resumed in place."""

    pre_run: bool = Field(default=False)
    """Whether the attempt stopped every step before its model launch."""

    status: AttemptStatus = Field(default=AttemptStatus.scheduled)
    """Where the attempt stands."""

    finished_at: Moment | None = Field(default=None)
    """Set once every step reached a terminal status: the completion marker."""

    directory: str = Field(default="")
    """The directory of the inputs the attempt carried, relative to the run root;
    empty when it carried none (a re-entry replaying the recorded plan, or an
    attempt recorded before schema 1.1.0)."""

    carried: dict[str, str] = Field(default_factory=dict)
    """The digest of each file the attempt carried, as `sha256:<hex>`, by its path
    relative to `directory`."""


class ExternalRun(ConfiguredBaseModel):
    """Another workplan run this run reads from."""

    alias: str
    """The alias the workplan refers to the run by."""

    run_id: str
    """The id of the run: the referenced run's once it was resolved, otherwise
    the reference as authored."""

    id: str = Field(default="")
    """The uuid of the run as resolved when the workplan was scheduled; empty when
    it was not resolved."""

    start_at: datetime | None = Field(default=None)
    """The restart time the run was referenced at, when pinned."""


class SourceDataRef(ConfiguredBaseModel):
    """Source data a blueprint reads."""

    location: str
    """Where the data lives."""

    name: str = Field(default="")
    """The dataset's name, when it has one."""

    hash: str = Field(default="")
    """The content hash of the data, when recorded."""


class CodebaseRef(ConfiguredBaseModel):
    """An external codebase a blueprint builds."""

    location: str
    """Where the code lives."""

    commit: str = Field(default="")
    """The commit the blueprint pins."""

    branch: str = Field(default="")
    """The branch the blueprint pins."""


class ExternalDependencies(ConfiguredBaseModel):
    """Everything a run depends on that lives outside the run itself."""

    runs: list[ExternalRun] = Field(default_factory=list)
    """Other workplan runs."""

    source_data: list[SourceDataRef] = Field(default_factory=list)
    """Source datasets."""

    codebases: list[CodebaseRef] = Field(default_factory=list)
    """External codebases."""

    catalog_entries: list[CatalogSpecRef] = Field(default_factory=list)
    """Catalog specs the blueprints were derived from."""

    blueprints: list[BlueprintIdentity] = Field(default_factory=list)
    """Blueprints the blueprints were derived from."""

    def merged(self, other: "ExternalDependencies") -> "ExternalDependencies":
        """Combine two sets of dependencies.

        Parameters
        ----------
        other : ExternalDependencies
            The dependencies to add to these.

        Returns
        -------
        ExternalDependencies
            The union, with duplicates dropped and order preserved.
        """
        return ExternalDependencies(
            **{
                name: _unique([*getattr(self, name), *getattr(other, name)])
                for name in type(self).model_fields
            }
        )


class StepEntry(ConfiguredBaseModel):
    """A step of the run, as the manifest knows it."""

    name: str
    """The name of the step."""

    application: str
    """The application the step runs."""

    working_dir: str
    """The step's working directory relative to the run root; `.` for a standalone run."""

    definition_hash: str = Field(default="")
    """A fingerprint of what the step computes (see `describe_steps`)."""

    blueprint: BlueprintIdentity | None = Field(default=None)
    """The blueprint the step runs; `None` until a deferred step's record names it."""

    carried_blueprint: str = Field(default="")
    """The copy of the step's blueprint file the latest attempt with a directory
    carried, relative to the run root; empty when that attempt carried none for
    the step. Not the file the step ran: that is its record's `blueprint_file`."""

    launcher: str = Field(default="")
    """The launcher that last submitted the step (`slurm` or `local`), which says
    what `job_id` is; empty when never submitted by an orchestrator."""

    job_id: str = Field(default="")
    """The id of the step's latest submission: a SLURM job id under `slurm`, the
    process id of the step's launch script under `local`; empty when never
    submitted."""

    submitted_at: Moment | None = Field(default=None)
    """When the latest submission was made; `None` when never submitted."""

    status: str = Field(default="")
    """The orchestration status name as of `observed_at`; empty when never observed."""

    record: StepRecord | None = Field(default=None)
    """The step's record, folded in once the step has written it."""


CANONICAL_NAMES_SINCE: t.Final[str] = "1.5.0"
"""The first manifest schema whose step definition hashes use current application
names. A hash recorded under an older manifest may use an earlier name (see
`cstar.base.application_names`), so `read_run_manifest` drops it rather than let
the re-entry guard compare it. A separate constant on purpose: it stays `1.5.0`
when the manifest schema later moves on."""


class RunManifest(ConfiguredBaseModel):
    """The description of a run: written to its run root."""

    schema_version: str = Field(default="1.5.0")
    """The schema version for the document."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    """A unique identifier minted when the manifest is first written."""

    run_id: str
    """The run's id: the name of its directory."""

    name: str
    """The name of the workplan or blueprint the run was made from."""

    kind: t.Literal["workplan", "blueprint"]
    """Whether the run is a workplan or a standalone blueprint."""

    system: str = Field(default="")
    """The C-Star system the run was first scheduled on."""

    created_at: Moment
    """When the manifest was first written."""

    workplan: WorkplanIdentity | None = Field(default=None)
    """The workplan the run was scheduled from; `None` for a standalone blueprint."""

    attempts: list[Attempt]
    """Every scheduling of the run, oldest first."""

    steps: list[StepEntry]
    """The steps of the run."""

    external: ExternalDependencies = Field(default_factory=ExternalDependencies)
    """What the run depends on outside itself."""

    observed_at: Moment | None = Field(default=None)
    """When the statuses and records were last folded in."""

    def step(self, name: str) -> StepEntry | None:
        """Find a step entry by name.

        Parameters
        ----------
        name : str
            The name of the step.

        Returns
        -------
        StepEntry | None
        """
        return next((entry for entry in self.steps if entry.name == name), None)


def _unique(items: "Sequence[_M]") -> list[_M]:
    """Drop the repeats from a sequence of models, keeping first occurrences in order.

    Parameters
    ----------
    items : Sequence[_M]
        The items to reduce; compared by equality.

    Returns
    -------
    list[_M]
    """
    unique: list[_M] = []
    for item in items:
        if item not in unique:
            unique.append(item)
    return unique


def expand_artifacts(root: Path, specs: "Sequence[ArtifactSpec]") -> list[Product]:
    """Expand declared artifact patterns into the products found under a step root.

    Walks nothing but the patterns given, and never raises: a pattern or a file
    that cannot be read is logged at debug and skipped.

    Parameters
    ----------
    root : Path
        The step root the patterns are relative to.
    specs : Sequence[ArtifactSpec]
        The declarations, in priority order: a file matched by several is
        claimed by the first.

    Returns
    -------
    list[Product]
        One product per matching regular file, sorted by path.
    """
    found: dict[str, Product] = {}
    for spec in specs:
        try:
            matches = sorted(root.glob(spec.pattern))
        except OSError as ex:
            log.debug(f"Unable to expand the pattern {spec.pattern!r}: {ex}")
            continue
        for match in matches:
            path = match.relative_to(root).as_posix()
            if path in found:
                continue
            try:
                if not match.is_file():
                    continue
                size = match.stat().st_size
            except OSError as ex:
                log.debug(f"Unable to inspect {str(match)!r}: {ex}")
                continue
            found[path] = Product(
                kind=spec.kind.value,
                path=path,
                size_bytes=size,
                metadata=dict(spec.metadata),
            )
    return sorted(found.values(), key=lambda product: product.path)


def merge_products(
    declared: "Sequence[Product]", reported: "Sequence[Product]"
) -> list[Product]:
    """Merge the products found by declared patterns with those a runtime hook reported.

    Declarations own kind and size, hooks own metadata.

    Parameters
    ----------
    declared : Sequence[Product]
        The products expanded from the application's declarations.
    reported : Sequence[Product]
        The products reported while the run executed.

    Returns
    -------
    list[Product]
        The declared products, then the reported products at undeclared paths.
    """
    merged = {product.path: product for product in declared}
    undeclared: list[Product] = []
    for product in reported:
        known = merged.get(product.path)
        if known is None:
            undeclared.append(product)
        else:
            merged[product.path] = known.model_copy(
                update={"metadata": {**known.metadata, **product.metadata}}
            )
    return [*merged.values(), *undeclared]


def _read(path: Path, klass: type[_M], label: str) -> _M | None:
    """Read a persisted document into its model.

    Parameters
    ----------
    path : Path
        The file holding the document.
    klass : type[_M]
        The model of the document.
    label : str
        What the document is called in messages.

    Returns
    -------
    _M | None
        The document, or `None` when the file does not exist.

    Raises
    ------
    CstarSchemaTooNewError
        If the document's schema is newer than this build reads.
    CstarError
        If the file cannot be read or is not a valid document.
    """
    target = klass.model_fields["schema_version"].default
    try:
        raw = read_document(path)
        found = str(raw.get("schema_version", "1.0.0"))
        newer = version_key(found) > version_key(target)
        if not newer:
            return klass.model_validate(raw)
    except FileNotFoundError:
        return None
    except Exception as ex:
        msg = f"Unable to read the {label} at {path}: {ex}"
        raise CstarError(msg) from ex

    raise CstarSchemaTooNewError(label, found, target)


def read_run_manifest(root: Path) -> RunManifest | None:
    """Read a run's manifest.

    Parameters
    ----------
    root : Path
        The run root.

    Returns
    -------
    RunManifest | None
        The manifest, or `None` when the run has none. A manifest older than
        `CANONICAL_NAMES_SINCE` comes back with its steps' definition hashes
        blank: the hashes it recorded may use an earlier application name, so
        they are not comparable with a current one. Every writer restamps the
        manifest, so the hashes are dropped here, where its recorded schema
        version is still known; the next attempt that describes the steps
        records them again.

    Raises
    ------
    CstarSchemaTooNewError
        If the manifest's schema is newer than this build reads.
    CstarError
        If the manifest cannot be read or is invalid.
    """
    manifest = _read(root / RUN_MANIFEST_NAME, RunManifest, "run manifest")
    if manifest is None or version_key(manifest.schema_version) >= version_key(
        CANONICAL_NAMES_SINCE
    ):
        return manifest

    recorded = [entry for entry in manifest.steps if entry.definition_hash]
    for entry in recorded:
        entry.definition_hash = ""
    if recorded:
        log.debug(
            f"Dropped the step definition hashes of the run manifest at {root}: "
            f"recorded under schema {manifest.schema_version}, before "
            f"{CANONICAL_NAMES_SINCE}"
        )
    return manifest


def read_step_record(step_root: Path) -> StepRecord | None:
    """Read a step's record.

    Parameters
    ----------
    step_root : Path
        The step's working directory.

    Returns
    -------
    StepRecord | None
        The record, or `None` when the step has not written one.

    Raises
    ------
    CstarSchemaTooNewError
        If the record's schema is newer than this build reads.
    CstarError
        If the record cannot be read or is invalid.
    """
    return _read(step_root / STEP_RECORD_NAME, StepRecord, "step record")


def product_paths(step_root: Path, kind: ArtifactKind, *, role: str = "") -> list[Path]:
    """List the files a step's record says it produced, of one kind.

    An empty list means the record has nothing of the kind to offer, so the
    caller searches the step's files itself: a step that has not written a
    record has no products, a record written by C-Star 0.17.0 lists none, and
    an application may not classify its files under the kind. The files are
    not looked at: a caller confirms the ones it reads with
    `require_recorded_files`, so pruning files it does not read is harmless.

    Parameters
    ----------
    step_root : Path
        The step's working directory.
    kind : ArtifactKind
        The kind of product to list.
    role : str
        The `role` recorded in the products' metadata; products recorded
        without a role match the empty default.

    Returns
    -------
    list[Path]
        The products' locations, ordered by their recorded path.

    Raises
    ------
    CstarSchemaTooNewError
        If the record's schema is newer than this build reads.
    CstarError
        If the record cannot be read or is invalid.
    """
    record = read_step_record(step_root)
    if record is None:
        return []

    listed = sorted(
        (
            product
            for product in record.products
            if product.known_kind is kind and product.metadata.get("role", "") == role
        ),
        key=lambda product: product.path,
    )
    return [step_root / product.path for product in listed]


def recorded_files_in(
    directory: Path,
    kind: ArtifactKind,
    parse: "Callable[[Path], _F]",
    *,
    role: str = "",
) -> list[_F]:
    """List the files of one kind that a step recorded in one of its directories.

    The record is read from `directory`'s parent, the step's working directory
    (e.g. that of the step whose `output` directory `directory` is). Products
    recorded elsewhere in the step are left out, and so are those whose names
    `parse` rejects. As with `product_paths`, an empty list means the caller
    searches the directory itself, and the files are not looked at: the caller
    confirms the ones it reads with `require_recorded_files`. Nothing is
    resolved; resolve `directory` first if a linked directory should find the
    record of the step it links to.

    Parameters
    ----------
    directory : Path
        The directory of the step holding the files.
    kind : ArtifactKind
        The kind of product to list.
    parse : Callable[[Path], F]
        Makes the caller's file reference from a product's location, raising
        `ValueError` for a name that is not one.
    role : str
        The `role` recorded in the products' metadata; products recorded
        without a role match the empty default.

    Returns
    -------
    list[F]
        The parsed products, ordered by their recorded path.

    Raises
    ------
    CstarSchemaTooNewError
        If the record's schema is newer than this build reads.
    CstarError
        If the record cannot be read or is invalid.
    """
    files: list[_F] = []
    misnamed: list[str] = []
    for path in product_paths(directory.parent, kind, role=role):
        if path.parent != directory:
            continue
        try:
            files.append(parse(path))
        except ValueError:
            misnamed.append(path.name)
    if misnamed:
        log.debug(
            f"The record in {directory.parent} lists {', '.join(misnamed)} in "
            f"{directory.name} as {kind.value}, but the names do not parse; "
            "skipping them"
        )
    return files


def require_recorded_files(step_root: Path, paths: "Sequence[Path]") -> None:
    """Refuse to read recorded products that are missing on disk.

    Parameters
    ----------
    step_root : Path
        The working directory of the step whose record listed `paths`.
    paths : Sequence[Path]
        The products about to be read.

    Raises
    ------
    CstarError
        If one of `paths` does not exist; the message names the first.
    """
    if missing := next((path for path in paths if not path.exists()), None):
        msg = (
            f"The record of the step at {step_root} lists {missing.name}, which is "
            f"missing from {missing.parent}; restore it, or run `cstar run check` "
            "to see which of the step's products are missing."
        )
        raise CstarError(msg)


def _write(path: Path, document: "RunManifest | StepRecord") -> Path:
    """Write a document atomically, stamped with the schema version this build writes.

    A document read from an older file may now hold fields of the newer schema,
    so it is restamped: an older build then refuses it as too new, rather than
    failing on fields it does not know.

    Parameters
    ----------
    path : Path
        The file to write.
    document : RunManifest | StepRecord
        The manifest or record; its `schema_version` is updated in place.

    Returns
    -------
    Path
        The file written.
    """
    document.schema_version = type(document).model_fields["schema_version"].default
    write_text_atomic(path, model_to_yaml(document))
    return path


def write_run_manifest(root: Path, manifest: RunManifest) -> Path:
    """Write a run's manifest, replacing any previous one atomically.

    Parameters
    ----------
    root : Path
        The run root.
    manifest : RunManifest
        The manifest to write.

    Returns
    -------
    Path
        The file written.
    """
    return _write(root / RUN_MANIFEST_NAME, manifest)


def write_step_record(step_root: Path, record: StepRecord) -> Path:
    """Write a step's record, replacing any previous one atomically.

    Parameters
    ----------
    step_root : Path
        The step's working directory.
    record : StepRecord
        The record to write.

    Returns
    -------
    Path
        The file written.
    """
    return _write(step_root / STEP_RECORD_NAME, record)


@contextmanager
def manifest_lock(
    run_id: str, *, timeout: float = MANIFEST_LOCK_TIMEOUT
) -> Iterator[None]:
    """Hold the exclusive lock serializing writers of a run's manifest.

    The lock file lives in the run's state directory. A standalone blueprint run
    has one writer and never takes it. A writer waits a bounded time, so one
    that is stalled while it holds the lock (a suspended `cstar run status`, a
    hung file system) cannot hold up the scheduling of a run.

    Parameters
    ----------
    run_id : str
        The run whose manifest is to be written.
    timeout : float
        Seconds to wait for another writer to release the lock.

    Yields
    ------
    None
        While the lock is held.

    Raises
    ------
    LockTimeoutError
        If another writer still holds the lock after `timeout` seconds.
    LockUnsupportedError
        If the file system holding the state directory does not support `flock`.
    """
    lock_path = StateDirectoryManager.run_state_dir(run_id=run_id) / MANIFEST_LOCK_NAME
    with locked(lock_path, timeout=timeout):
        yield


def workplan_content_hash(workplan: "Workplan") -> str:
    """Fingerprint a workplan.

    Parameters
    ----------
    workplan : Workplan
        The workplan to fingerprint.

    Returns
    -------
    str
        The SHA-256 hex digest of the workplan's serialized form.
    """
    return canonical_hash(workplan.model_dump(mode="json", by_alias=True))


@dataclass(frozen=True)
class StepFacts:
    """What is known about a step before it runs."""

    definition_hash: str
    """A fingerprint of what the step computes."""

    blueprint: BlueprintIdentity | None
    """The blueprint the step runs, when it can be loaded at this point."""

    external: ExternalDependencies
    """What the step's blueprint depends on outside itself."""


@dataclass(frozen=True)
class CarriedInputs:
    """What an attempt carried into its run directory."""

    start_at: datetime
    """When the attempt was scheduled; it names the attempt's directory."""

    directory: str
    """The attempt's directory, as a POSIX path relative to the run root."""

    carried: "Mapping[str, str]"
    """The digest of each carried file, by its path relative to `directory`."""

    carried_blueprints: "Mapping[str, str]"
    """The carried blueprint of each step, by step name, as a POSIX path
    relative to the run root."""


def _text(mapping: "Mapping[str, t.Any]", key: str) -> str:
    """Read a string value from a mapping, or an empty string.

    Parameters
    ----------
    mapping : Mapping[str, Any]
        The mapping to read.
    key : str
        The key to read.

    Returns
    -------
    str
    """
    value = mapping.get(key)
    return value if isinstance(value, str) else ""


def _walk(node: t.Any, found: ExternalDependencies) -> ExternalDependencies:
    """Collect the codebases and source data in a dumped blueprint.

    Parameters
    ----------
    node : Any
        The JSON-able value to search.
    found : ExternalDependencies
        The dependencies found so far.

    Returns
    -------
    ExternalDependencies
        `found` with the dependencies under `node` added.
    """
    if isinstance(node, list):
        for item in node:
            found = _walk(item, found)
        return found
    if not isinstance(node, dict):
        return found

    location = _text(node, "location")
    if location and (_text(node, "commit") or _text(node, "branch")):
        ref = CodebaseRef(
            location=location,
            commit=_text(node, "commit"),
            branch=_text(node, "branch"),
        )
        return found.merged(ExternalDependencies(codebases=[ref]))
    if "dataset_key" in node:
        data = SourceDataRef(
            location=_text(node, "url") or _text(node, "dataset_id"),
            name=_text(node, "dataset_key"),
        )
        return found.merged(ExternalDependencies(source_data=[data]))
    if location:
        data = SourceDataRef(
            location=location,
            hash=_text(node, "hash") or _text(node, "content_hash"),
        )
        return found.merged(ExternalDependencies(source_data=[data]))

    for value in node.values():
        found = _walk(value, found)
    return found


def collect_dependencies(blueprint: "Blueprint") -> ExternalDependencies:
    """Find what a blueprint depends on outside itself.

    Walks the serialized blueprint: a mapping naming a `location` and a `commit`
    or `branch` is a codebase, a mapping with a `dataset_key` or any other
    `location` is source data, and the blueprint's provenance supplies the
    catalog entries and blueprints it was derived from.

    Parameters
    ----------
    blueprint : Blueprint
        The blueprint to search.

    Returns
    -------
    ExternalDependencies
    """
    data = blueprint.model_dump(mode="json")
    data.pop("provenance", None)
    found = _walk(data, ExternalDependencies())

    derived = blueprint.provenance.derived_from
    return found.merged(
        ExternalDependencies(
            catalog_entries=[ref for ref in derived if isinstance(ref, CatalogSpecRef)],
            blueprints=[ref for ref in derived if isinstance(ref, BlueprintIdentity)],
        )
    )


def _blueprint_data(step: "Step") -> t.Any:
    """Express a step's blueprint reference as JSON-able data.

    Parameters
    ----------
    step : Step
        The step whose blueprint reference to express.

    Returns
    -------
    Any
        The authored string of a path, or the dumped deferred or inline reference.
    """
    value = step.blueprint_path
    if isinstance(value, DeferredBlueprintRef | InlineBlueprintRef):
        return value.model_dump(mode="json")
    return str(value)


def _load_step_blueprint(step: "Step", base_dir: Path) -> "Blueprint | None":
    """Load the blueprint a step names by file, when it can be loaded now.

    Parameters
    ----------
    step : Step
        The step whose blueprint to load.
    base_dir : Path
        The directory a relative blueprint path is relative to.

    Returns
    -------
    Blueprint | None
        The blueprint, or `None` for a reference holding a placeholder or a
        sentinel URI, which only exist once the workplan is prepared.

    Raises
    ------
    CstarExpectationFailed
        If a local blueprint file does not exist.
    """
    text = str(step.blueprint_path).strip()
    klass = get_application(step.application).blueprint

    if is_remote_resource(text):
        with local_copy(text) as path:
            return load_blueprint(path, klass, anchor=False)  # type: ignore[no-any-return]

    # A placeholder is filled, and a sentinel resolved, only at prepare time.
    if "{{" in text or urlsplit(text).scheme:
        return None

    anchored = anchor_relative_path(text, base_dir)
    path = anchored or Path(text).expanduser()
    if not path.exists():
        shown = text if anchored is None else f"{text} (looked for {anchored})"
        msg = f"Step {step.name!r}: blueprint file not found: {shown}"
        raise CstarExpectationFailed(msg)
    return load_blueprint(path, klass)  # type: ignore[no-any-return]


def describe_steps(workplan: "Workplan", base_dir: Path) -> dict[str, StepFacts]:
    """Fingerprint what each step of a workplan computes, before it runs.

    A step's `definition_hash` covers its name, application, blueprint reference,
    dependencies, `blueprint_overrides`, `directives` and the content of the
    blueprint it names. It leaves out `compute_overrides` and `workflow_overrides`:
    resources and clobber or resume choices do not change what a step that reached
    `Done` computed. A deferred step takes its producer's `definition_hash` as its
    blueprint content, so a change upstream moves every consumer.

    Parameters
    ----------
    workplan : Workplan
        The workplan to describe.
    base_dir : Path
        The directory a relative blueprint path is relative to.

    Returns
    -------
    dict[str, StepFacts]
        The facts of each step, by step name, producers before consumers.

    Raises
    ------
    CstarExpectationFailed
        If a local blueprint file does not exist.
    """
    by_name = {step.name: step for step in workplan.steps}
    facts: dict[str, StepFacts] = {}
    visiting: set[str] = set()

    def describe(name: str) -> None:
        if name in facts or name in visiting:
            return
        visiting.add(name)
        step = by_name[name]

        blueprint: Blueprint | None = None
        if isinstance(step.blueprint_path, DeferredBlueprintRef):
            producer = step.blueprint_path.from_step
            if producer in by_name:
                describe(producer)
            content_hash = facts[producer].definition_hash if producer in facts else ""
        elif isinstance(step.blueprint_path, InlineBlueprintRef):
            content_hash = ""
        else:
            blueprint = _load_step_blueprint(step, base_dir)
            content_hash = blueprint.content_hash() if blueprint else ""

        definition_hash = canonical_hash(
            {
                "name": step.name,
                "application": step.application,
                "blueprint": _blueprint_data(step),
                "depends_on": sorted(step.depends_on),
                "blueprint_overrides": step.blueprint_overrides,
                "directives": step.directives,
                "blueprint_content_hash": content_hash,
            }
        )
        facts[name] = StepFacts(
            definition_hash=definition_hash,
            blueprint=BlueprintIdentity.of(blueprint) if blueprint else None,
            external=(
                collect_dependencies(blueprint) if blueprint else ExternalDependencies()
            ),
        )
        visiting.discard(name)

    for name in by_name:
        describe(name)
    return facts


def new_attempt(
    workplan: "Workplan",
    *,
    workplan_hash: str,
    carried: CarriedInputs | None = None,
) -> Attempt:
    """Describe an attempt that is being scheduled now.

    Parameters
    ----------
    workplan : Workplan
        The workplan being scheduled.
    workplan_hash : str
        The fingerprint of the workplan as this attempt runs it.
    carried : CarriedInputs | None
        What the attempt carried into the run directory, when it prepared the
        workplan; the attempt then starts when the inputs were carried. `None`
        for an attempt that replays the recorded plan, which starts now and
        carries nothing.

    Returns
    -------
    Attempt
    """
    return Attempt(
        start_at=carried.start_at if carried else _now(),
        system=system_name(),
        versions=package_versions(),
        workplan_content_hash=workplan_hash,
        clobbered=[step.name for step in workplan.steps if step.clobber],
        resumed=[step.name for step in workplan.steps if step.resume],
        pre_run=workplan.pre_run,
        directory=carried.directory if carried else "",
        carried=dict(carried.carried) if carried else {},
    )


def ensure_run_manifest(
    root: Path,
    *,
    run_id: str,
    name: str,
    kind: t.Literal["workplan", "blueprint"],
    workplan: WorkplanIdentity | None,
    attempt: Attempt,
    steps: "Mapping[str, tuple[str, str, StepFacts | None]]",
    runs: "Mapping[str, RunRef]",
    carried_blueprints: "Mapping[str, str] | None" = None,
) -> RunManifest:
    """Create a run's manifest, or add an attempt to the one it has, and write it.

    The caller holds `manifest_lock` for workplan runs.

    Parameters
    ----------
    root : Path
        The run root.
    run_id : str
        The run's id.
    name : str
        The name of the workplan or blueprint.
    kind : {"workplan", "blueprint"}
        What the run was made from.
    workplan : WorkplanIdentity | None
        The workplan the run was scheduled from.
    attempt : Attempt
        The attempt being scheduled.
    steps : Mapping[str, tuple[str, str, StepFacts | None]]
        Each step's name mapped to its application, its working directory
        relative to the run root and, when known, its facts. A step already in
        the manifest keeps its status, record and submission; given facts replace
        its definition hash, working directory and (when they carry one)
        blueprint. A step the attempt clobbers or resumes loses its status,
        record and submission: it certainly runs again.
    runs : Mapping[str, RunRef]
        The external runs the workplan refers to, by alias.
    carried_blueprints : Mapping[str, str] | None
        The blueprint each step's attempt carried, by step name, relative to the
        run root. When given, every step in `steps` is pointed at its entry, or
        at none when it has no entry: each attempt that prepares the workplan
        re-points the steps at its own copies. `None` leaves the recorded
        pointers as they are, for an attempt that replays the recorded plan and
        for a standalone blueprint run.

    Returns
    -------
    RunManifest
        The manifest as written.

    Raises
    ------
    CstarError
        If the existing manifest cannot be read.
    """
    # minted with the first attempt: a value equal to "now" would be dropped
    # as a default when the document is written
    manifest = read_run_manifest(root) or RunManifest(
        run_id=run_id,
        name=name,
        kind=kind,
        system=attempt.system,
        created_at=attempt.start_at,
        workplan=workplan,
        attempts=[],
        steps=[],
    )
    manifest.workplan = workplan or manifest.workplan

    external = manifest.external
    for step_name, (application, working_dir, facts) in steps.items():
        entry = manifest.step(step_name)
        if entry is None:
            entry = StepEntry(
                name=step_name, application=application, working_dir=working_dir
            )
            manifest.steps.append(entry)
        if facts is not None:
            entry.working_dir = working_dir
            entry.definition_hash = facts.definition_hash
            entry.blueprint = facts.blueprint or entry.blueprint
            external = external.merged(facts.external)
        if carried_blueprints is not None:
            entry.carried_blueprint = carried_blueprints.get(step_name, "")

    aliases = [
        ExternalRun(alias=alias, run_id=ref.run_id, id=ref.id, start_at=ref.start_at)
        for alias, ref in runs.items()
    ]
    manifest.external = external.merged(ExternalDependencies(runs=aliases))
    manifest.attempts.append(attempt)

    # a step the attempt clobbers or resumes certainly runs again: what was
    # observed or submitted of its earlier run no longer stands
    rerun = {*attempt.clobbered, *attempt.resumed}
    for entry in manifest.steps:
        if entry.name in rerun:
            entry.status = ""
            entry.record = None
            entry.launcher = ""
            entry.job_id = ""
            entry.submitted_at = None

    write_run_manifest(root, manifest)
    return manifest


def record_submission(
    root: Path,
    run_id: str,
    step: str,
    *,
    launcher: str,
    job_id: str,
    submitted_at: datetime,
) -> bool:
    """Record in a run's manifest that a step was submitted.

    Takes `manifest_lock` itself: it is for workplan runs, whose steps are
    submitted by an orchestrator alongside other writers of the manifest. An
    entry that already holds the submission is not rewritten, so an adopted
    sentinel, which carries the submission recorded when it was made, writes
    nothing.

    The submissions this process records at once queue for each other first,
    and all of them share one deadline of `MANIFEST_LOCK_TIMEOUT` seconds for
    both locks: a writer stalled on the manifest lock costs the steps launched
    together that long, not that long per step.

    Parameters
    ----------
    root : Path
        The run root.
    run_id : str
        The run's id, which names the lock.
    step : str
        The name of the step that was submitted.
    launcher : str
        The launcher that submitted the step.
    job_id : str
        The id of the submission, as the launcher knows it.
    submitted_at : datetime
        When the submission was made.

    Returns
    -------
    bool
        Whether the manifest was written. `False` when the run has no manifest,
        the manifest has no entry for `step`, or the entry already holds the
        submission.

    Raises
    ------
    LockTimeoutError
        If the deadline passes before both locks are held.
    LockUnsupportedError
        If the file system holding the state directory does not support `flock`.
    CstarSchemaTooNewError
        If the manifest's schema is newer than this build reads.
    CstarError
        If the manifest cannot be read or is invalid.
    """
    submitted_at = _whole_second(submitted_at)
    deadline = time.monotonic() + MANIFEST_LOCK_TIMEOUT

    if not _SUBMISSION_WRITERS.acquire(timeout=MANIFEST_LOCK_TIMEOUT):
        msg = f"Other submissions of run {run_id!r} kept the manifest busy"
        raise LockTimeoutError(msg)
    try:
        remaining = max(0.0, deadline - time.monotonic())
        with manifest_lock(run_id, timeout=remaining):
            return _record_submission(
                root, step, launcher=launcher, job_id=job_id, submitted_at=submitted_at
            )
    finally:
        _SUBMISSION_WRITERS.release()


def _record_submission(
    root: Path, step: str, *, launcher: str, job_id: str, submitted_at: datetime
) -> bool:
    """Set a step's submission in the manifest, under the caller's locks.

    Returns
    -------
    bool
        Whether the manifest was written.
    """
    manifest = read_run_manifest(root)
    entry = manifest.step(step) if manifest else None
    if manifest is None or entry is None:
        log.debug(f"The manifest at {root} has no entry for step {step!r}")
        return False

    if (entry.launcher, entry.job_id, entry.submitted_at) == (
        launcher,
        job_id,
        submitted_at,
    ):
        return False

    entry.launcher = launcher
    entry.job_id = job_id
    entry.submitted_at = submitted_at
    write_run_manifest(root, manifest)
    return True


def latest_attempt_dir(manifest: RunManifest, root: Path) -> Path | None:
    """Locate the inputs the latest preparing attempt carried.

    An attempt that re-enters a run replays the recorded plan and carries
    nothing, so it is skipped: the latest attempt that has a directory is the one
    whose inputs the run is running. The directory is not checked to exist,
    since older attempt directories may be deleted by hand.

    Parameters
    ----------
    manifest : RunManifest
        The run's manifest.
    root : Path
        The run root.

    Returns
    -------
    Path | None
        The directory, or `None` when no attempt carried inputs.
    """
    for attempt in reversed(manifest.attempts):
        if attempt.directory:
            return root / attempt.directory
    return None


_OUTCOME_OF_STATUS: t.Final[dict[str, str]] = {
    Status.Done.name: "completed",
    Status.Failed.name: "failed",
    Status.Cancelled.name: "cancelled",
}
"""The step record outcome each terminal orchestration status agrees with."""

_STATUS_OF_OUTCOME: t.Final[dict[str, str]] = {
    outcome: status for status, outcome in _OUTCOME_OF_STATUS.items()
}
"""The terminal orchestration status each step record outcome agrees with."""


def status_of_record(record: StepRecord) -> str:
    """The orchestration status a step's record stands for.

    Parameters
    ----------
    record : StepRecord
        The step's record.

    Returns
    -------
    str
        The status name (`Done`, `Failed` or `Cancelled`), or an empty string
        when the record's outcome is none of those.
    """
    return _STATUS_OF_OUTCOME.get(record.outcome, "")


def recorded_step_status(root: Path, manifest: RunManifest, name: str) -> str:
    """The status a run's own records say a step has, for a run read as found.

    A run with no sentinels here (not scheduled on this machine) is `Done` only
    when the step's own record says it completed: a manifest's snapshot that
    says `Done` is no evidence, as a copy of the run without its
    `cstar-step.yaml` files, or one cut short in transfer, carries the same
    snapshot. Any other status the snapshot holds is reported as it stands.
    Nothing is written.

    Parameters
    ----------
    root : Path
        The run root.
    manifest : RunManifest
        The run's manifest.
    name : str
        The name of the step.

    Returns
    -------
    str
        The status name, or an empty string when the manifest has no such
        step, or the step has no status or only the unconfirmed snapshot
        `Done`.
    """
    entry = manifest.step(name)
    if entry is None:
        return ""

    try:
        record = read_step_record(root / entry.working_dir)
    except DOCUMENT_READ_ERRORS as ex:
        log.warning(
            f"Ignoring the record of step {name!r} of run {manifest.run_id!r}: {ex}"
        )
        record = None
    if record is not None:
        return status_of_record(record)
    return "" if entry.status == Status.Done.name else entry.status


def fold_step_records(
    manifest: RunManifest,
    root: Path,
    statuses: "Mapping[str, str] | None" = None,
) -> bool:
    """Fold what the steps have recorded, and their statuses, into a manifest.

    A record is folded only when it agrees with the observed status: `Done` with
    `completed`, `Failed` with `failed`, `Cancelled` with `cancelled`. A record
    that disagrees, or that a step still running or waiting has on disk, is left
    over from an earlier run of the step, and is not reported. When `statuses`
    does not name a step, its record is folded as found.

    When every step is terminal, the latest attempt is closed. The manifest is
    changed in memory only; the caller holds the lock and decides whether to
    write.

    Parameters
    ----------
    manifest : RunManifest
        The manifest to update.
    root : Path
        The run root.
    statuses : Mapping[str, str] | None
        The orchestration status name of each step, by step name.

    Returns
    -------
    bool
        Whether anything but `observed_at` changed.
    """
    before = manifest.model_dump(mode="json", exclude={"observed_at"})

    for entry in manifest.steps:
        try:
            record = read_step_record(root / entry.working_dir)
        except CstarError as ex:
            log.warning(f"Ignoring the record of step {entry.name!r}: {ex}")
            record = None
        if record is not None:
            entry.record = record
        if statuses is not None and entry.name in statuses:
            entry.status = statuses[entry.name]
            outcome = entry.record.outcome if entry.record else None
            if entry.record and _OUTCOME_OF_STATUS.get(entry.status) != outcome:
                log.debug(
                    f"Step {entry.name!r} is {entry.status} but its record says "
                    f"{outcome!r}; the record is from an earlier run"
                )
                entry.record = None
        if entry.record:
            entry.blueprint = entry.blueprint or entry.record.blueprint

    terminal = {state.name for state in Status.terminal_states()}
    if manifest.attempts and all(entry.status in terminal for entry in manifest.steps):
        # A step re-run from scratch is relaunched by the process that is still
        # running its workplan, before it can observe the stale terminal status
        # of the step's earlier run, so a re-entry is not closed early by it.
        attempt = manifest.attempts[-1]
        if attempt.finished_at is None:
            names = {entry.status for entry in manifest.steps}
            if Status.Failed.name in names:
                attempt.status = AttemptStatus.failed
            elif Status.Cancelled.name in names:
                attempt.status = AttemptStatus.cancelled
            else:
                attempt.status = AttemptStatus.done
            recorded = [e.record.finished_at for e in manifest.steps if e.record]
            attempt.finished_at = max([*recorded, attempt.start_at])

    manifest.observed_at = _now()
    return before != manifest.model_dump(mode="json", exclude={"observed_at"})


def compute_product_digests(
    root: Path, manifest: RunManifest
) -> dict[tuple[str, str], str]:
    """Compute the digest of each product that has none.

    Each digest is `product_digest` of the file, a bounded read for a netCDF file
    and a read of the whole file otherwise. A product whose path is not a regular
    file is skipped, and one that cannot be digested is skipped with a warning:
    recording is bookkeeping and never stops a run.

    Nothing is locked or written, and the reads may take long; hand the result to
    `record_product_digests`.

    Parameters
    ----------
    root : Path
        The run root.
    manifest : RunManifest
        The manifest, with the step records folded in; not changed.

    Returns
    -------
    dict[tuple[str, str], str]
        The digest of each product, by its step name and its path.
    """
    digests: dict[tuple[str, str], str] = {}
    for entry in manifest.steps:
        if entry.record is None:
            continue
        step_root = root / entry.working_dir
        for product in entry.record.products:
            file = step_root / product.path
            if product.digest or not file.is_file():
                continue
            try:
                digests[entry.name, product.path] = product_digest(file)
            except Exception as ex:  # bookkeeping: an unreadable file is skipped
                log.warning(f"No digest recorded for {file}: {ex}")
    return digests


def record_product_digests(root: Path, digests: "Mapping[tuple[str, str], str]") -> int:
    """Record computed product digests in the step records and the manifest.

    The manifest is read again from the run, so what another writer recorded
    since the digests were computed is kept: a product that has a digest by now
    keeps it. The step records and the manifest are written
    only when a digest was recorded.

    The caller holds `manifest_lock` around this call only; computing the digests
    is not done under it.

    Parameters
    ----------
    root : Path
        The run root.
    digests : Mapping[tuple[str, str], str]
        The digests from `compute_product_digests`, by step name and product path.

    Returns
    -------
    int
        The number of digests recorded.
    """
    manifest = read_run_manifest(root)
    if manifest is None:
        log.debug(f"No digest recorded: {root} holds no run manifest")
        return 0
    fold_step_records(manifest, root)

    recorded = 0
    for entry in manifest.steps:
        if entry.record is None:
            continue
        count = 0
        for product in entry.record.products:
            digest = digests.get((entry.name, product.path))
            if digest and not product.digest:
                product.digest = digest
                count += 1
        if count:
            write_step_record(root / entry.working_dir, entry.record)
            recorded += count

    if recorded:
        write_run_manifest(root, manifest)
    return recorded


def finalize_standalone_run(
    blueprint: "Blueprint", *, started_at: datetime
) -> RunManifest:
    """Record a standalone blueprint run as a one-step run and write its manifest.

    A standalone `cstar blueprint run` has no orchestrator, so the CLI that ran
    it is the one process left to record the attempt. The step record the
    runner wrote is the authoritative outcome; a missing record, or one older
    than `started_at`, counts as failed. There is no lock: a standalone run has a single writer.

    Parameters
    ----------
    blueprint : Blueprint
        The blueprint that ran.
    started_at : datetime
        When the run was started.

    Returns
    -------
    RunManifest
        The manifest as written.
    """
    root = blueprint.effective_working_dir
    started_at = _whole_second(started_at)
    attempt = Attempt(
        start_at=started_at,
        system=system_name(),
        versions=package_versions(),
    )
    facts = StepFacts(
        definition_hash=blueprint.content_hash(),
        blueprint=BlueprintIdentity.of(blueprint),
        external=collect_dependencies(blueprint),
    )
    manifest = ensure_run_manifest(
        root,
        run_id=slugify(blueprint.name),
        name=blueprint.name,
        kind="blueprint",
        workplan=None,
        attempt=attempt,
        steps={blueprint.name: (blueprint.application, ".", facts)},
        runs={},
    )

    # a record older than this run is an earlier run's: this one died without
    # reaching shutdown
    record = read_step_record(root)
    if record and record.finished_at < started_at:
        record = None
    if record and record.outcome == "completed":
        status = Status.Done
    elif record and record.outcome == "cancelled":
        status = Status.Cancelled
    else:
        status = Status.Failed
    fold_step_records(manifest, root, statuses={blueprint.name: status.name})
    write_run_manifest(root, manifest)
    return manifest
