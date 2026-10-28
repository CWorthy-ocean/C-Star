"""Run directories that only a manifest names: tracking has no record of them."""

import typing as t
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from cstar.base.utils import slugify
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    Attempt,
    RunManifest,
    StepEntry,
    WorkplanIdentity,
    write_run_manifest,
)
from cstar.orchestration.models import Step
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.serialization import serialize

STARTED: t.Final[datetime] = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
"""When the runs `build_manifest_run` makes started, unless a test says otherwise."""


@dataclass(frozen=True)
class BuiltRun:
    """A run directory that tracking knows nothing about."""

    root: Path
    """The run directory."""
    run_id: str
    uuid: str
    """The uuid its manifest holds."""
    steps: Sequence[str]
    """The names of the steps, whose directories are `step_dir(name)`."""

    def step_dir(self, name: str) -> Path:
        """The working directory of a step."""
        return self.root / "tasks" / slugify(name)

    @property
    def manifest_bytes(self) -> bytes:
        """The manifest as it is on disk."""
        return (self.root / RUN_MANIFEST_NAME).read_bytes()


def build_manifest_run(
    root: Path,
    run_id: str,
    blueprint: Path,
    *,
    uuid: str | None = None,
    kind: t.Literal["workplan", "blueprint"] = "workplan",
    steps: Sequence[str] = ("Spin Up", "run"),
    start_at: datetime = STARTED,
    attempts: int = 1,
) -> BuiltRun:
    """Build a run directory with a manifest, as the run left it.

    A workplan run holds a transformed workplan, a directory for each step and
    a log in the first; a blueprint run holds its manifest alone.

    Parameters
    ----------
    root : Path
        The run directory.
    run_id : str
        The run-id; also the name of the run and of its transformed workplan.
    blueprint : Path
        The blueprint file the steps name.
    uuid : str | None
        The uuid of the manifest; a fresh one by default.
    kind : Literal["workplan", "blueprint"]
        The kind of run.
    steps : Sequence[str]
        The names of the steps.
    start_at : datetime
        When the first attempt started.
    attempts : int
        How many attempts the manifest holds, one hour apart.

    Returns
    -------
    BuiltRun
    """
    built = BuiltRun(root, run_id, uuid or str(uuid4()), steps)
    root.mkdir(parents=True, exist_ok=True)

    if kind == "workplan":
        live = [
            LiveStep.from_step(
                Step(
                    name=name,
                    application="hello_world",
                    blueprint=blueprint.as_posix(),
                ),
                update={"working_dir": built.step_dir(name)},
            )
            for name in steps
        ]
        workplan = LiveWorkplan(
            name=run_id, description="a built run", steps=live, root=root
        )
        assert serialize(root / f"{run_id}_transformed.yaml", workplan)
        for step in live:
            step.working_dir.mkdir(parents=True)
        first = live[0]
        first.log_path.parent.mkdir(parents=True, exist_ok=True)
        first.log_path.write_text(f"{first.name} message")

    write_run_manifest(
        root,
        RunManifest(
            id=built.uuid,
            run_id=run_id,
            name=run_id,
            kind=kind,
            created_at=start_at,
            attempts=[
                Attempt(start_at=start_at + timedelta(hours=n)) for n in range(attempts)
            ],
            steps=[
                StepEntry(
                    name=name,
                    application="hello_world",
                    working_dir="."
                    if kind == "blueprint"
                    else f"tasks/{slugify(name)}",
                )
                for name in steps
            ],
        ),
    )
    return built


def write_identity(
    root: Path,
    run_id: str,
    *,
    workplan: Path | str | None = None,
    name: str | None = None,
    start_at: datetime = STARTED,
) -> RunManifest:
    """Write a bare manifest into `root`, so a tracking record of the run derives
    its name, start and workplan paths from it.

    A thin `WorkplanRun` holds none of these: `name` and `start_at` are the
    manifest's, `workplan_path` is its `workplan.path` and `trx_workplan_path`
    is `<root>/<stem of workplan.path>_transformed.yaml` when that file exists.

    Parameters
    ----------
    root : Path
        The run directory; created when absent.
    run_id : str
        The run-id the manifest holds.
    workplan : Path | str | None
        The authored workplan the run was scheduled from; the manifest records
        none by default.
    name : str | None
        The name of the run; the run-id by default.
    start_at : datetime
        When the one attempt the manifest holds started; `created_at` too.

    Returns
    -------
    RunManifest
    """
    manifest = RunManifest(
        run_id=run_id,
        name=name if name is not None else run_id,
        kind="workplan",
        created_at=start_at,
        workplan=(
            WorkplanIdentity(name=run_id, content_hash="", path=str(workplan))
            if workplan is not None
            else None
        ),
        attempts=[Attempt(start_at=start_at)],
        steps=[],
    )
    root.mkdir(parents=True, exist_ok=True)
    write_run_manifest(root, manifest)
    return manifest
