"""Tier 2: the run manifest and step records a run leaves behind.

A two-step ``hello_world`` workplan runs to completion through the real CLI, so no ROMS
is compiled and the run takes seconds. The tests read ``cstar-run.yaml`` and each
``cstar-step.yaml`` the way a user would, with ``cstar run status`` and
``cstar run show``, and check a standalone ``cstar blueprint run`` leaves both files
in its working directory.

The tests share one run and are ordered: each builds on the state the previous one
left in the run directory.
"""

import json
import shutil
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from cstar.base.utils import slugify
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    STEP_RECORD_NAME,
    AttemptStatus,
    RunManifest,
    read_run_manifest,
    read_step_record,
)
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    kill_run,
    make_cli_env,
    run_cstar,
    run_log_tails,
    wait_for_terminal,
)
from cstar.tests.integration_tests.e2e.test_workplan_shapes import write_workplan

RUN_ID = "manifest-e2e"
SHAPE = {"A": [], "B": ["A"]}
"""Each step maps to the steps it depends on."""
RUN_TIMEOUT = 180.0
POLL_INTERVAL = 0.5
STANDALONE_NAME = "standalone hello"
PRODUCT_HEADINGS = ("Products missing", "Products whose size")
TRANSFORMED_WORKPLAN_NAME = "workplan_transformed.yaml"


@dataclass(frozen=True)
class ManifestRun:
    """A finished workplan run and the manifest as it stood right after scheduling."""

    root: Path
    """The directory holding the blueprints, workplan and C-Star homes."""
    env: dict[str, str]
    workplan: Path
    scheduled: RunManifest
    """The manifest read as soon as ``cstar workplan run`` returned."""

    @property
    def data_home(self) -> Path:
        """The ``CSTAR_DATA_HOME`` of the run."""
        return self.root / "data"

    @property
    def run_root(self) -> Path:
        """The run's directory."""
        return self.data_home / "workplan_runs" / RUN_ID

    def step_root(self, step: str) -> Path:
        """The working directory of ``step``."""
        return self.run_root / "tasks" / slugify(step)

    def manifest(self) -> RunManifest:
        """Read the run manifest from disk."""
        manifest = read_run_manifest(self.run_root)
        assert manifest is not None, f"no {RUN_MANIFEST_NAME} in {self.run_root}"
        return manifest


@pytest.fixture(scope="module")
def manifest_run(
    tmp_path_factory: pytest.TempPathFactory, cstar_shim: Path
) -> Iterator[ManifestRun]:
    """Run the workplan once and wait for every step to be terminal."""
    root = tmp_path_factory.mktemp("manifest")
    workplan = write_workplan(root, SHAPE)
    env = make_cli_env(root, cstar_shim)
    state_home = root / "state"
    steps = list(SHAPE)

    proc = run_cstar(env, "workplan", "run", "--run-id", RUN_ID, str(workplan))
    try:
        run_root = root / "data" / "workplan_runs" / RUN_ID
        scheduled = read_run_manifest(run_root)
        statuses = wait_for_terminal(
            state_home, RUN_ID, steps, timeout=RUN_TIMEOUT, poll_interval=POLL_INTERVAL
        )
        if proc.returncode != 0 or statuses != dict.fromkeys(steps, DONE):
            pytest.fail(
                f"workplan run exited {proc.returncode} with {statuses}\n"
                f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}\n"
                f"{run_log_tails(root / 'data', RUN_ID)}"
            )
        assert scheduled is not None, f"no {RUN_MANIFEST_NAME} after scheduling"

        yield ManifestRun(root, env, workplan, scheduled)
    finally:
        kill_run(state_home, RUN_ID, steps)


def _output(proc: subprocess.CompletedProcess[str]) -> str:
    """The captured streams of a finished ``cstar`` process, for assertion messages."""
    return f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


def test_scheduling_writes_the_manifest(manifest_run: ManifestRun) -> None:
    """The manifest exists as soon as the run is scheduled and names every step."""
    manifest = manifest_run.scheduled

    assert manifest.kind == "workplan"
    assert manifest.run_id == RUN_ID
    assert manifest.id
    assert manifest.workplan is not None
    assert manifest.workplan.name == "shapes"
    assert len(manifest.attempts) == 1

    entries = {entry.name: entry for entry in manifest.steps}
    assert set(entries) == set(SHAPE)
    for name, entry in entries.items():
        assert entry.working_dir == f"tasks/{slugify(name)}"
        assert entry.definition_hash
        assert entry.application == "hello_world"


def test_each_step_writes_its_record(manifest_run: ManifestRun) -> None:
    """Every step process leaves a ``cstar-step.yaml`` in its working directory."""
    for step in SHAPE:
        root = manifest_run.step_root(step)
        assert (root / STEP_RECORD_NAME).is_file(), run_log_tails(
            manifest_run.data_home, RUN_ID
        )
        record = read_step_record(root)
        assert record is not None
        assert record.outcome == "completed"
        assert record.run_id == RUN_ID
        assert record.application == "hello_world"


def test_each_record_names_its_step(manifest_run: ManifestRun) -> None:
    """A step record's ``step`` is the name of the workplan step that wrote it."""
    for step in SHAPE:
        record = read_step_record(manifest_run.step_root(step))
        assert record is not None
        assert record.step == step


def test_status_closes_the_attempt(manifest_run: ManifestRun) -> None:
    """``cstar run status`` folds the records in and marks the attempt done."""
    proc = run_cstar(manifest_run.env, "run", "status", RUN_ID)
    assert proc.returncode == 0, _output(proc)

    manifest = manifest_run.manifest()
    (attempt,) = manifest.attempts
    assert attempt.status == AttemptStatus.done
    assert attempt.finished_at is not None
    assert manifest.observed_at is not None
    for step in SHAPE:
        entry = manifest.step(step)
        assert entry is not None
        assert entry.status == "Done"
        assert entry.record is not None
        assert entry.record.outcome == "completed"


def test_check_is_clean_and_hello_world_records_no_products(
    manifest_run: ManifestRun,
) -> None:
    """``hello_world`` declares no products, and ``cstar run check`` finds none to fault.

    The exit code is not asserted: the workplan the harness writes names its
    blueprints by absolute path, so ``workplan_transformed.yaml`` is reported under
    the absolute-path check, which is not a product finding.
    """
    proc = run_cstar(manifest_run.env, "run", "check", RUN_ID)
    assert "Skipped products" not in proc.stdout, _output(proc)
    assert not any(heading in proc.stdout for heading in PRODUCT_HEADINGS), _output(
        proc
    )
    findings = [line for line in proc.stdout.splitlines() if line.startswith("  ")]
    assert all(TRANSFORMED_WORKPLAN_NAME in line for line in findings), _output(proc)

    for step in SHAPE:
        record = read_step_record(manifest_run.step_root(step))
        assert record is not None
        assert record.products == []


def test_reentry_appends_an_attempt(manifest_run: ManifestRun) -> None:
    """Re-entering the run keeps the manifest's identity and adds an attempt.

    Every step is ``Done`` and reused, so the next ``status`` closes the new attempt
    at once.
    """
    before = manifest_run.manifest()

    proc = run_cstar(
        manifest_run.env,
        "workplan",
        "run",
        "--run-id",
        RUN_ID,
        str(manifest_run.workplan),
    )
    assert proc.returncode == 0, _output(proc)

    reentered = manifest_run.manifest()
    assert reentered.id == before.id
    assert len(reentered.attempts) == 2

    proc = run_cstar(manifest_run.env, "run", "status", RUN_ID)
    assert proc.returncode == 0, _output(proc)

    closed = manifest_run.manifest()
    assert closed.id == before.id
    assert [a.status for a in closed.attempts] == [AttemptStatus.done] * 2
    assert all(a.finished_at is not None for a in closed.attempts)


def test_show_reads_a_copied_run(manifest_run: ManifestRun, tmp_path: Path) -> None:
    """A copy of the run directory is shown from its manifest alone."""
    copy = tmp_path / "copy"
    shutil.copytree(manifest_run.run_root, copy, symlinks=True)

    proc = run_cstar(manifest_run.env, "run", "show", str(copy), "--format", "json")
    assert proc.returncode == 0, _output(proc)
    shown = json.loads(proc.stdout)
    assert shown["id"] == manifest_run.manifest().id
    assert {entry["name"] for entry in shown["steps"]} == set(SHAPE)

    proc = run_cstar(manifest_run.env, "run", "show", str(copy))
    assert proc.returncode == 0, _output(proc)

    # the exit code is not asserted: the copy's transformed workplan may name
    # absolute paths, which is a different check
    proc = run_cstar(manifest_run.env, "run", "check", str(copy), "--no-refresh")
    assert not any(heading in proc.stdout for heading in PRODUCT_HEADINGS), _output(
        proc
    )


def test_standalone_blueprint_run_leaves_both_files(
    manifest_run: ManifestRun,
) -> None:
    """``cstar blueprint run`` writes a manifest and a record in its working directory."""
    blueprint = manifest_run.root / "standalone.yaml"
    blueprint.write_text(
        yaml.safe_dump(
            {
                "name": STANDALONE_NAME,
                "description": "a standalone hello",
                "application": "hello_world",
                "state": "draft",
                "target": "standalone",
                "schema_version": "1.0.0",
            },
            sort_keys=False,
        )
    )

    proc = run_cstar(manifest_run.env, "blueprint", "run", str(blueprint))
    assert proc.returncode == 0, _output(proc)

    root = (
        manifest_run.data_home
        / "blueprint_runs"
        / "hello_world"
        / slugify(STANDALONE_NAME)
    )
    assert (root / STEP_RECORD_NAME).is_file(), _output(proc)
    manifest = read_run_manifest(root)
    assert manifest is not None, _output(proc)
    assert manifest.kind == "blueprint"
    assert manifest.workplan is None
    (attempt,) = manifest.attempts
    assert attempt.status == AttemptStatus.done
    (entry,) = manifest.steps
    assert entry.working_dir == "."
    assert entry.record is not None
    assert entry.record.outcome == "completed"

    proc = run_cstar(manifest_run.env, "run", "show", str(root), "--format", "yaml")
    assert proc.returncode == 0, _output(proc)
    assert yaml.safe_load(proc.stdout)["kind"] == "blueprint"
