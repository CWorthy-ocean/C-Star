"""Blueprints emitted by runs of a catalog's entries.

A producing application's run can publish a blueprint for a downstream
application (a ``forge`` run publishes the ``roms_marbl`` blueprint
``B_<name>.yaml``). :func:`find_blueprint_outputs` finds those blueprints, and
:class:`BlueprintOutput` says how each one's recorded producer compares with the
catalog entry it came from. Reached through
:meth:`cstar.catalog.domain_catalog.LayeredCatalog.blueprint_outputs`; imported
there on first use so that ``import cstar.catalog`` stays light.
"""

import asyncio
import logging
import typing as t
from collections.abc import Collection, Mapping, Sequence
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import Field, ValidationError

from cstar.orchestration.models import (
    BlueprintCore,
    BlueprintRef,
    ConfiguredBaseModel,
    Provenance,
    RequiredString,
)

if t.TYPE_CHECKING:
    from cstar.catalog.domain_catalog import LayeredCatalog
    from cstar.orchestration.orchestration import LiveWorkplan
    from cstar.orchestration.tracking import WorkplanRun

logger = logging.getLogger(__name__)


class OutputStatus(StrEnum):
    """How an emitted blueprint's recorded producer compares with the catalog."""

    CURRENT = "current"
    """The recorded producer equals the catalog entry's current producer reference:
    same name and content hash."""

    CHANGED = "changed"
    """The producer is a catalog entry, but its content hash differs from the recorded
    one: the entry was edited since, or the run applied overrides."""

    UNCATALOGED = "uncataloged"
    """The recorded producer is not an entry in the catalog."""

    UNVERIFIED = "unverified"
    """The output cannot be checked against the catalog: it records no producer
    (e.g. it was emitted before provenance existed), or the catalog entry of its
    recorded producer could not be loaded."""


class BlueprintOutput(ConfiguredBaseModel):
    """A blueprint emitted by a run of a catalog entry's application."""

    path: Path
    """The emitted blueprint file."""

    application: RequiredString
    """The emitted blueprint's own application, e.g. `roms_marbl`."""

    producer: BlueprintRef
    """The blueprint that produced it: the one its provenance records or, when it
    records none, the one it was found under (a guess, as `status` says)."""

    status: OutputStatus
    """How `producer` compares with the catalog; read it rather than re-comparing."""

    generated_at: datetime | None = Field(default=None)
    """When the emitted blueprint was generated; `None` if it records no time."""

    run_id: str = Field(default="")
    """The workplan run that emitted it; empty for a standalone run."""

    step: str = Field(default="")
    """The workplan step that emitted it; empty for a standalone run."""


class _Candidate(t.NamedTuple):
    """A file that may be an emitted blueprint, and where it was found.

    `producer_name` names the producer when the file records none: set for a
    standalone run, where the catalog entry says; empty for a workplan step.
    """

    path: Path
    run_id: str
    step: str
    producer_name: str


def _log_skipped(reason: str, skipped: Sequence[object]) -> None:
    """Log the skipped items in one debug message; log nothing if there are none."""
    if skipped:
        logger.debug("%d %s: %s", len(skipped), reason, ", ".join(map(str, skipped)))


def _recency(output: BlueprintOutput) -> tuple[bool, datetime]:
    """Order outputs newest first (as a reversed sort key), undated ones last.

    A naive timestamp is taken as UTC, so it compares with an aware one.
    """
    moment = output.generated_at
    if moment is None:
        return False, datetime.min.replace(tzinfo=UTC)
    return True, moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _entry_candidates(
    catalog: "LayeredCatalog", application: str
) -> tuple[dict[str, BlueprintRef], list[_Candidate], frozenset[str]]:
    """Read the catalog's ``application`` entries.

    Returns
    -------
    tuple[dict[str, BlueprintRef], list[_Candidate], frozenset[str]]
        The current producer reference of each entry that emits a blueprint, keyed
        by the name outputs record it under; the blueprint files that exist where a
        standalone run of such an entry publishes it; and the names of the entries
        that could not be loaded (as the catalog names them, since an unreadable
        entry has no blueprint name to give).
    """
    from cstar.applications.core import get_application
    from cstar.execution.file_system import JobFileSystemManager
    from cstar.orchestration.serialization import deserialize

    app = get_application(application)
    current: dict[str, BlueprintRef] = {}
    candidates: list[_Candidate] = []
    unloadable: list[str] = []
    unlocatable: list[str] = []

    for name in catalog.blueprint_names(application):
        try:
            blueprint = deserialize(
                catalog.blueprint_path(application, name), app.blueprint
            )
        except Exception:  # a file the user edits: any way it can be unreadable
            unloadable.append(name)
            continue

        # Not guarded: a failing hook is a broken application or environment, which
        # must not look like an entry that emitted nothing.
        emitted = app.emitted_blueprint(blueprint)
        if emitted is None:
            continue
        current[emitted.producer.name] = emitted.producer

        try:
            published = (
                JobFileSystemManager(blueprint.effective_working_dir).output_dir
                / emitted.filename
            )
            exists = published.is_file()
        except (OSError, ValueError):  # e.g. an unreadable or oddly named directory
            unlocatable.append(name)
            continue
        if exists:
            candidates.append(_Candidate(published, "", "", emitted.producer.name))

    _log_skipped("catalog entries could not be loaded and were skipped", unloadable)
    _log_skipped(
        "catalog entries have no readable working directory; their standalone "
        "runs were skipped",
        unlocatable,
    )
    return current, candidates, frozenset(unloadable)


def _step_candidates(
    runs: Sequence["WorkplanRun"],
    workplans: Sequence["LiveWorkplan | None"],
    application: str,
) -> tuple[list[_Candidate], list[str]]:
    """List the blueprint files in the output directories of the ``application``
    steps of the runs' workplans, in the order of ``runs``.

    ``workplans`` are the runs' own, in the same order; ``None`` marks one that
    could not be read.

    Returns
    -------
    tuple[list[_Candidate], list[str]]
        The files found, and the ids of the runs whose workplan could not be read.
    """
    candidates: list[_Candidate] = []
    unreadable: list[str] = []
    for run, workplan in zip(runs, workplans, strict=True):
        if workplan is None:  # e.g. the run was purged
            unreadable.append(run.run_id)
            continue
        for step in workplan.steps:
            if step.application != application:
                continue
            output_dir = step.fsm.output_dir
            files = sorted([*output_dir.glob("*.yaml"), *output_dir.glob("*.yml")])
            candidates.extend(
                _Candidate(path, run.run_id, step.name, "") for path in files
            )
    return candidates, unreadable


async def _run_candidates(application: str) -> list[_Candidate]:
    """Find the blueprint files in the output directories of the ``application``
    steps of every tracked workplan run, in run-id order.
    """
    from cstar.base.env import max_concurrency
    from cstar.orchestration.orchestration import LiveWorkplan
    from cstar.orchestration.serialization import deserialize_all
    from cstar.orchestration.tracking import TrackingRepository

    runs = sorted(
        await TrackingRepository().list_latest_runs(), key=lambda run: run.run_id
    )
    workplans = await deserialize_all(
        [run.trx_workplan_path for run in runs], LiveWorkplan, max_concurrency()
    )

    # Listing each step's directory blocks, so it runs off the event loop.
    candidates, unreadable = await asyncio.to_thread(
        _step_candidates, runs, workplans, application
    )

    _log_skipped("runs have no readable workplan and were skipped", unreadable)
    return candidates


def _describe(
    candidates: Sequence[_Candidate],
    application: str,
    current: Mapping[str, BlueprintRef],
    unloadable: Collection[str],
) -> list[BlueprintOutput]:
    """Describe the candidates that are blueprints, one output per file.

    A recorded producer that is not among the ``current`` ones reads as
    uncataloged, unless it names one of the ``unloadable`` entries: that one is in
    the catalog, so the output is unverified.
    """
    outputs: list[BlueprintOutput] = []
    seen: set[Path] = set()
    not_blueprints: list[Path] = []
    malformed: list[Path] = []

    for candidate in candidates:
        resolved = candidate.path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)

        try:
            data = yaml.safe_load(candidate.path.read_text(encoding="utf-8"))
            core = BlueprintCore.model_validate(data)
        except (OSError, ValueError, yaml.YAMLError):
            not_blueprints.append(candidate.path)
            continue

        # The provenance is read apart, so a malformed one costs only the
        # file's verification, not the file.
        try:
            provenance = Provenance.model_validate(data.get("provenance") or {})
        except ValidationError:
            malformed.append(candidate.path)
            provenance = Provenance()

        recorded = next(
            (
                ref
                for ref in provenance.derived_from
                if isinstance(ref, BlueprintRef) and ref.application == application
            ),
            None,
        )
        if recorded is None:
            producer = BlueprintRef(
                kind="Blueprint",
                application=application,
                name=candidate.producer_name or core.name,
            )
            status = OutputStatus.UNVERIFIED
        else:
            producer = recorded
            if recorded.name in current:
                same = recorded == current[recorded.name]
                status = OutputStatus.CURRENT if same else OutputStatus.CHANGED
            elif recorded.name in unloadable:
                status = OutputStatus.UNVERIFIED
            else:
                status = OutputStatus.UNCATALOGED

        outputs.append(
            BlueprintOutput(
                path=candidate.path,
                application=core.application,
                producer=producer,
                status=status,
                generated_at=provenance.generated_at,
                run_id=candidate.run_id,
                step=candidate.step,
            )
        )

    _log_skipped("files are not blueprints and were skipped", not_blueprints)
    _log_skipped("blueprints have a malformed provenance and are unverified", malformed)
    return sorted(outputs, key=_recency, reverse=True)


async def find_blueprint_outputs(
    catalog: "LayeredCatalog", application: str
) -> list[BlueprintOutput]:
    """Find the blueprints emitted by runs of the catalog's ``application`` entries.

    Read-only, and without walking directories: a standalone run is found where
    the application publishes the blueprint under an entry's working directory,
    and a workplan run through run tracking, in the output directory of each of
    its ``application`` steps. Nothing is written into the catalog.

    Parameters
    ----------
    catalog : LayeredCatalog
        The catalog whose entries are the producers.
    application : str
        The producing application, e.g. ``forge``.

    Returns
    -------
    list[BlueprintOutput]
        The emitted blueprints, newest first; those that record no time last.
        Equally recent ones keep the order they were found in: standalone runs,
        then workplan runs by run id. A file found more than once is listed once,
        as its first finder describes it.

    Raises
    ------
    ValueError
        If ``application`` is not a registered application.

    Notes
    -----
    Entries are matched to outputs by the producer name the application's
    ``emitted_blueprint`` reports. An entry that cannot be loaded, a run whose
    workplan cannot be read (it was purged, say) and a file that is not a
    blueprint are skipped, each kind noted in one debug message. An output whose
    recorded producer is an entry that could not be loaded (matched by the entry's
    name in the catalog, as its own blueprint name is unreadable) reads as
    `OutputStatus.UNVERIFIED`, not `OutputStatus.UNCATALOGED`: the entry exists,
    only the output cannot be checked against it. An error in the application's
    ``emitted_blueprint`` is not skipped but propagates.
    """
    # Loading the entries (the application's first use imports its dependencies)
    # and reading the files block, so they run off the event loop: the wizard
    # awaits this on the kernel's loop. The two scans do not depend on each other,
    # so they overlap.
    (current, candidates, unloadable), run_candidates = await asyncio.gather(
        asyncio.to_thread(_entry_candidates, catalog, application),
        _run_candidates(application),
    )
    candidates.extend(run_candidates)
    return await asyncio.to_thread(
        _describe, candidates, application, current, unloadable
    )
