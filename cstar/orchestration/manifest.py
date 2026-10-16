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
import fcntl
import typing as t
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import Field

from cstar.applications.core import ArtifactKind, get_application
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.base.provenance import canonical_hash, package_versions, system_name
from cstar.base.utils import slugify, utc_now
from cstar.execution.file_system import (
    StateDirectoryManager,
    is_remote_resource,
    local_copy,
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
    read_document,
    render_model,
)
from cstar.system.migration import CstarSchemaTooNewError, version_key

if t.TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from cstar.orchestration.models import Blueprint, RunRef, Step, Workplan

log = get_logger(__name__)

RUN_MANIFEST_NAME: t.Final[str] = "cstar-run.yaml"
"""The file name of the run manifest, in the run root."""

STEP_RECORD_NAME: t.Final[str] = "cstar-step.yaml"
"""The file name of the step record, in the step's working directory."""

MANIFEST_LOCK_NAME: t.Final[str] = "manifest.lock"
"""The name of the lock file serializing run-manifest writers, in the run's state directory."""

_M = t.TypeVar("_M", bound=ConfiguredBaseModel)


class AttemptStatus(enum.StrEnum):
    """Where an attempt to run a workplan stands."""

    scheduled = "scheduled"
    running = "running"
    done = "done"
    failed = "failed"
    cancelled = "cancelled"


class Product(ConfiguredBaseModel):
    """A file or directory a step produced."""

    kind: ArtifactKind
    """The classification of the product."""

    path: str
    """The location of the product, relative to the step root."""

    size_bytes: int = Field(default=0)
    """The size of the product in bytes; zero when not measured."""

    sha256: str = Field(default="")
    """The content digest of the product; empty when not computed."""

    metadata: dict[str, str | int | float | list[str]] = Field(default_factory=dict)
    """Model-specific detail, such as an output stream."""


class StepRecord(ConfiguredBaseModel):
    """What a step did: written by the step process, in its working directory."""

    schema_version: str = Field(default="1.0.0")
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

    outcome: str
    """The runner's execution status as its lowercase name, e.g. `completed`."""

    start_at: datetime | None = Field(default=None)
    """The start of the simulated period, when the step has one."""

    finished_at: datetime
    """When the step process recorded its outcome."""

    system: str = Field(default="")
    """The C-Star system the step ran on."""

    versions: dict[str, str] = Field(default_factory=dict)
    """The versions of the packages the step ran with."""

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

    start_at: datetime
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

    finished_at: datetime | None = Field(default=None)
    """Set once every step reached a terminal status: the completion marker."""


class ExternalRun(ConfiguredBaseModel):
    """Another workplan run this run reads from."""

    alias: str
    """The alias the workplan refers to the run by."""

    run_id: str
    """The id of the run."""

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

    status: str = Field(default="")
    """The orchestration status name as of `observed_at`; empty when never observed."""

    record: StepRecord | None = Field(default=None)
    """The step's record, folded in once the step has written it."""


class RunManifest(ConfiguredBaseModel):
    """The description of a run: written to its run root."""

    schema_version: str = Field(default="1.0.0")
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

    created_at: datetime = Field(default_factory=utc_now)
    """When the manifest was first written."""

    workplan: WorkplanIdentity | None = Field(default=None)
    """The workplan the run was scheduled from; `None` for a standalone blueprint."""

    attempts: list[Attempt]
    """Every scheduling of the run, oldest first."""

    steps: list[StepEntry]
    """The steps of the run."""

    external: ExternalDependencies = Field(default_factory=ExternalDependencies)
    """What the run depends on outside itself."""

    observed_at: datetime | None = Field(default=None)
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
        The manifest, or `None` when the run has none.

    Raises
    ------
    CstarSchemaTooNewError
        If the manifest's schema is newer than this build reads.
    CstarError
        If the manifest cannot be read or is invalid.
    """
    return _read(root / RUN_MANIFEST_NAME, RunManifest, "run manifest")


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
    path = root / RUN_MANIFEST_NAME
    write_text_atomic(path, render_model(manifest))
    return path


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
    path = step_root / STEP_RECORD_NAME
    write_text_atomic(path, render_model(record))
    return path


@contextmanager
def manifest_lock(run_id: str) -> Iterator[None]:
    """Hold the exclusive lock serializing writers of a run's manifest.

    The lock file lives in the run's state directory. A standalone blueprint run
    has one writer and never takes it.

    Parameters
    ----------
    run_id : str
        The run whose manifest is to be written.

    Yields
    ------
    None
        While the lock is held.
    """
    lock_path = StateDirectoryManager.run_state_dir(run_id=run_id) / MANIFEST_LOCK_NAME
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


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


def new_attempt(workplan: "Workplan", *, workplan_hash: str) -> Attempt:
    """Describe an attempt that is being scheduled now.

    Parameters
    ----------
    workplan : Workplan
        The workplan being scheduled.
    workplan_hash : str
        The fingerprint of the workplan as this attempt runs it.

    Returns
    -------
    Attempt
    """
    return Attempt(
        start_at=utc_now(),
        system=system_name(),
        versions=package_versions(),
        workplan_content_hash=workplan_hash,
        clobbered=[step.name for step in workplan.steps if step.clobber],
        resumed=[step.name for step in workplan.steps if step.resume],
        pre_run=workplan.pre_run,
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
        the manifest keeps its status and record; given facts replace its
        definition hash, working directory and (when they carry one) blueprint.
    runs : Mapping[str, RunRef]
        The external runs the workplan refers to, by alias.

    Returns
    -------
    RunManifest
        The manifest as written.

    Raises
    ------
    CstarError
        If the existing manifest cannot be read.
    """
    manifest = read_run_manifest(root) or RunManifest(
        run_id=run_id,
        name=name,
        kind=kind,
        system=attempt.system,
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

    aliases = [
        ExternalRun(alias=alias, run_id=ref.run_id, start_at=ref.start_at)
        for alias, ref in runs.items()
    ]
    manifest.external = external.merged(ExternalDependencies(runs=aliases))
    manifest.attempts.append(attempt)

    # a step the attempt clobbers or resumes certainly runs again: what was
    # observed of its earlier run no longer stands
    rerun = {*attempt.clobbered, *attempt.resumed}
    for entry in manifest.steps:
        if entry.name in rerun:
            entry.status = ""
            entry.record = None

    write_run_manifest(root, manifest)
    return manifest


_OUTCOME_OF_STATUS: t.Final[dict[str, str]] = {
    Status.Done.name: "completed",
    Status.Failed.name: "failed",
    Status.Cancelled.name: "cancelled",
}
"""The step record outcome each terminal orchestration status agrees with."""


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

    manifest.observed_at = utc_now()
    return before != manifest.model_dump(mode="json", exclude={"observed_at"})


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
