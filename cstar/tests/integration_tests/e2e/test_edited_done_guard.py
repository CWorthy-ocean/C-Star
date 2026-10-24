"""Tier 2: re-entering a run refuses a ``Done`` step edited since it ran.

A two-step ``hello_world`` workplan runs to completion through the real CLI, so no ROMS
is compiled and the run takes seconds. The tests then edit step ``B``'s blueprint and
re-enter the run with ``--run-id``: the edit is refused unless ``--clobber B`` names
the step, in which case ``B`` alone is re-run.

The tests share one run and are ordered: each builds on the state the previous one
left in the run directory.
"""

import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from cstar.base.utils import slugify
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    RunManifest,
    StepRecord,
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

RUN_ID = "edited-done-guard"
SHAPE = {"A": [], "B": ["A"]}
"""Each step maps to the steps it depends on."""
RUN_TIMEOUT = 180.0
POLL_INTERVAL = 0.5
EDITED_STEP = "B"
EDITED_TARGET = "B edited"


@dataclass(frozen=True)
class GuardedRun:
    """A finished workplan run and the manifest and records as the first run left them."""

    root: Path
    """The directory holding the blueprints, workplan and C-Star homes."""
    env: dict[str, str]
    workplan: Path
    manifest: RunManifest
    """The manifest read after ``cstar run status`` closed the first attempt."""
    records: dict[str, StepRecord]
    """Each step's record, read once the first run finished."""

    @property
    def data_home(self) -> Path:
        """The ``CSTAR_DATA_HOME`` of the run."""
        return self.root / "data"

    @property
    def state_home(self) -> Path:
        """The ``CSTAR_STATE_HOME`` of the run."""
        return self.root / "state"

    @property
    def run_root(self) -> Path:
        """The run's directory."""
        return self.data_home / "workplan_runs" / RUN_ID

    def step_root(self, step: str) -> Path:
        """The working directory of ``step``."""
        return self.run_root / "tasks" / slugify(step)

    def current_manifest(self) -> RunManifest:
        """Read the run manifest from disk."""
        manifest = read_run_manifest(self.run_root)
        assert manifest is not None, f"no {RUN_MANIFEST_NAME} in {self.run_root}"
        return manifest

    def transformed_files(self) -> list[str]:
        """The names of the transformed workplan and its rotated copies."""
        return sorted(p.name for p in self.run_root.glob("workplan_transformed.yaml*"))

    def rerun(self, *args: str) -> subprocess.CompletedProcess[str]:
        """Re-enter the run with the workplan, passing ``args`` to ``workplan submit``."""
        return run_cstar(
            self.env,
            "workplan",
            "submit",
            "--run-id",
            RUN_ID,
            *args,
            str(self.workplan),
        )


@pytest.fixture(scope="module")
def guarded_run(
    tmp_path_factory: pytest.TempPathFactory, cstar_shim: Path
) -> Iterator[GuardedRun]:
    """Run the workplan once, wait for every step to be done and close the attempt."""
    root = tmp_path_factory.mktemp("guard")
    workplan = write_workplan(root, SHAPE)
    env = make_cli_env(root, cstar_shim)
    state_home = root / "state"
    steps = list(SHAPE)

    proc = run_cstar(env, "workplan", "submit", "--run-id", RUN_ID, str(workplan))
    try:
        statuses = wait_for_terminal(
            state_home, RUN_ID, steps, timeout=RUN_TIMEOUT, poll_interval=POLL_INTERVAL
        )
        if proc.returncode != 0 or statuses != dict.fromkeys(steps, DONE):
            pytest.fail(
                f"workplan submit exited {proc.returncode} with {statuses}\n"
                f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}\n"
                f"{run_log_tails(root / 'data', RUN_ID)}"
            )

        # closes the first attempt and records each step's status in the manifest
        status = run_cstar(env, "run", "status", RUN_ID)
        assert status.returncode == 0, _output(status)

        run_root = root / "data" / "workplan_runs" / RUN_ID
        manifest = read_run_manifest(run_root)
        assert manifest is not None, f"no {RUN_MANIFEST_NAME} in {run_root}"
        records: dict[str, StepRecord] = {}
        for step in steps:
            record = read_step_record(run_root / "tasks" / slugify(step))
            assert record is not None, f"no step record for {step!r}"
            records[step] = record

        yield GuardedRun(root, env, workplan, manifest, records)
    finally:
        kill_run(state_home, RUN_ID, steps)


def _output(proc: subprocess.CompletedProcess[str]) -> str:
    """The captured streams of a finished ``cstar`` process, for assertion messages."""
    return f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


def _edit_blueprint(run: GuardedRun, step: str, target: str) -> None:
    """Rewrite the blueprint of ``step`` with a different ``target``."""
    path = run.root / f"{slugify(step)}.yaml"
    document = yaml.safe_load(path.read_text())
    document["target"] = target
    path.write_text(yaml.safe_dump(document, sort_keys=False))


def _wait_for_new_record(run: GuardedRun, step: str) -> StepRecord:
    """Wait for ``step`` to write a record newer than the one the first run left."""
    old = run.records[step].finished_at
    deadline = time.monotonic() + RUN_TIMEOUT
    while True:
        record = read_step_record(run.step_root(step))
        if record is not None and record.finished_at != old:
            return record
        if time.monotonic() > deadline:
            pytest.fail(
                f"step {step!r} wrote no new record within {RUN_TIMEOUT}s\n"
                f"{run_log_tails(run.data_home, RUN_ID)}"
            )
        time.sleep(POLL_INTERVAL)


def test_unchanged_reentry_is_accepted(guarded_run: GuardedRun) -> None:
    """Re-entering the run with nothing edited is accepted and adds an attempt."""
    assert len(guarded_run.manifest.attempts) == 1

    proc = guarded_run.rerun()
    assert proc.returncode == 0, _output(proc)

    assert len(guarded_run.current_manifest().attempts) == 2


def test_edited_done_step_is_refused(guarded_run: GuardedRun) -> None:
    """Editing a ``Done`` step's blueprint refuses re-entry and writes nothing."""
    assert slugify(EDITED_STEP) == "b"
    _edit_blueprint(guarded_run, EDITED_STEP, EDITED_TARGET)
    attempts = len(guarded_run.current_manifest().attempts)
    transformed = guarded_run.transformed_files()
    assert transformed, f"no transformed workplan in {guarded_run.run_root}"

    proc = guarded_run.rerun()

    assert proc.returncode == 1, _output(proc)
    assert "An invalid request was made" in proc.stdout, _output(proc)
    assert repr(EDITED_STEP) in proc.stdout, _output(proc)
    assert f"--clobber {EDITED_STEP}" in proc.stdout, _output(proc)
    assert "--run-id" in proc.stdout, _output(proc)
    assert repr("A") not in proc.stdout, _output(proc)
    assert len(guarded_run.current_manifest().attempts) == attempts
    assert guarded_run.transformed_files() == transformed


def test_clobbered_edited_step_reruns(guarded_run: GuardedRun) -> None:
    """``--clobber B`` re-runs the edited step alone and records the clobber."""
    proc = guarded_run.rerun("--clobber", EDITED_STEP)
    assert proc.returncode == 0, _output(proc)

    record = _wait_for_new_record(guarded_run, EDITED_STEP)
    statuses = wait_for_terminal(
        guarded_run.state_home,
        RUN_ID,
        list(SHAPE),
        timeout=RUN_TIMEOUT,
        poll_interval=POLL_INTERVAL,
    )
    assert statuses == dict.fromkeys(SHAPE, DONE), run_log_tails(
        guarded_run.data_home, RUN_ID
    )

    status = run_cstar(guarded_run.env, "run", "status", RUN_ID)
    assert status.returncode == 0, _output(status)

    manifest = guarded_run.current_manifest()
    assert manifest.attempts[-1].clobbered == [EDITED_STEP]
    assert record.finished_at > guarded_run.records[EDITED_STEP].finished_at

    untouched = read_step_record(guarded_run.step_root("A"))
    assert untouched == guarded_run.records["A"]
