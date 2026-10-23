"""Tier 2: a finished workplan run keeps working after its data home is copied.

A two-step ``hello_world`` workplan runs to completion through the real CLI, so no ROMS
is compiled and the run takes seconds. The whole data home is then copied, the original
is moved out of the way and ``CSTAR_DATA_HOME`` is repointed at the copy, with
``CSTAR_STATE_HOME`` left alone. The run record in the state home still names the
original directories; the verbs must follow it to the copy, and the transformed
workplan, which records the run root it was written under, must rebase its paths onto
the directory it now sits in.
"""

import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from cstar.base.env import ENV_CSTAR_DATA_HOME
from cstar.base.utils import slugify
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    kill_run,
    make_cli_env,
    run_cstar,
    run_log_tails,
    sentinel_path,
    wait_for_terminal,
)
from cstar.tests.integration_tests.e2e.test_workplan_shapes import write_workplan

RUN_ID = "relocation-e2e"
SHAPE = {"A": [], "B": ["A"]}
"""Each step maps to the steps it depends on."""
RUN_TIMEOUT = 180.0
POLL_INTERVAL = 0.5
OUTPUT_NAME = "hello.txt"
"""A file the first step is given in its ``output`` directory, to be gathered."""


@dataclass(frozen=True)
class RelocatedRun:
    """A finished run, its original data home and the copy the CLI now points at."""

    env: dict[str, str]
    """The environment of the CLI: the original's, with only the data home repointed."""
    original: Path
    """Where the data home was moved to; nothing refers to the run here any more."""
    moved: Path
    """The copied data home."""
    state_home: Path

    @property
    def run_dir(self) -> Path:
        """The run's directory inside the copy."""
        return self.moved / "workplan_runs" / RUN_ID

    @property
    def transformed(self) -> Path:
        """The transformed workplan inside the copy."""
        return self.run_dir / "workplan_transformed.yaml"

    def step_dir(self, step: str) -> Path:
        """The working directory of ``step`` inside the copy."""
        return self.run_dir / "tasks" / slugify(step)

    def sentinel(self, step: str) -> Path:
        """The sentinel recording the status of ``step`` in the state home."""
        return sentinel_path(self.state_home, RUN_ID, step)

    def recorded_output_path(self) -> Path:
        """The run directory the persisted run record names, read straight from disk."""
        (record,) = self.state_home.rglob(f"latest/{RUN_ID}.yaml")
        return Path(yaml.safe_load(record.read_text())["output_path"])


@pytest.fixture(scope="module")
def relocated_run(
    tmp_path_factory: pytest.TempPathFactory, cstar_shim: Path
) -> Iterator[RelocatedRun]:
    """Run the workplan to completion, then copy its data home and repoint at the copy."""
    root = tmp_path_factory.mktemp("relocation")
    workplan = write_workplan(root, SHAPE)
    env = make_cli_env(root, cstar_shim)
    data_home, state_home = root / "data", root / "state"
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
                f"{run_log_tails(data_home, RUN_ID)}"
            )

        # the first step produced an output file, as a model step would
        output = data_home / "workplan_runs" / RUN_ID / "tasks" / "a" / "output"
        output.mkdir(parents=True, exist_ok=True)
        (output / OUTPUT_NAME).write_text("hello\n")

        moved, original = root / "moved-data", root / "original-data"
        shutil.copytree(data_home, moved, symlinks=True)
        data_home.rename(original)

        relocated = RelocatedRun(
            env={**env, ENV_CSTAR_DATA_HOME: str(moved)},
            original=original,
            moved=moved,
            state_home=state_home,
        )

        # the scenario must not be vacuous: the record and the copied workplan still
        # name directories that exist nowhere now
        original_run_dir = data_home / "workplan_runs" / RUN_ID
        assert relocated.recorded_output_path() == original_run_dir
        persisted = yaml.safe_load(relocated.transformed.read_text())
        assert Path(persisted["root"]) == original_run_dir
        assert not original_run_dir.exists()

        yield relocated
    finally:
        kill_run(state_home, RUN_ID, steps)


def test_status_reads_the_copy(relocated_run: RelocatedRun) -> None:
    """``workplan status`` loads the copied workplan and tabulates every step.

    A load failure is printed as a traceback and still exits 0, so the table rows,
    which only a loaded workplan produces, are what is asserted.
    """
    # a wide terminal keeps rich from truncating the step rows
    proc = run_cstar(
        {**relocated_run.env, "COLUMNS": "200"}, "workplan", "status", RUN_ID
    )

    output = f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    assert proc.returncode == 0, output
    assert "Traceback" not in output
    rows = [line for line in proc.stdout.splitlines() if "(hello_world)" in line]
    assert len(rows) == len(SHAPE), output
    for step, row in zip(SHAPE, rows, strict=True):
        assert f"{slugify(step)} (hello_world)" in row, output


def test_path_reports_directories_in_the_copy(relocated_run: RelocatedRun) -> None:
    """``workplan path`` prints the run and step directories under the copy."""
    run_proc = run_cstar(relocated_run.env, "workplan", "path", RUN_ID)
    step_proc = run_cstar(relocated_run.env, "workplan", "path", RUN_ID, "B")

    assert run_proc.returncode == 0, run_proc.stderr
    assert run_proc.stdout == f"{relocated_run.run_dir}\n"
    assert step_proc.returncode == 0, step_proc.stderr
    assert step_proc.stdout == f"{relocated_run.step_dir('B')}\n"


def test_gather_links_output_in_the_copy(relocated_run: RelocatedRun) -> None:
    """``workplan gather`` finds a step's output through the rebased working directory
    and builds the gathered directory inside the copy.
    """
    proc = run_cstar(relocated_run.env, "workplan", "gather", RUN_ID)

    assert proc.returncode == 0, proc.stderr
    gathered = relocated_run.run_dir / "gathered_output" / OUTPUT_NAME
    assert gathered.is_symlink()
    assert (
        gathered.resolve()
        == (relocated_run.step_dir("A") / "output" / OUTPUT_NAME).resolve()
    )


def test_resume_relaunches_nothing_and_repoints_the_record(
    relocated_run: RelocatedRun,
) -> None:
    """``workplan submit --resume`` from the copy launches no step, because every step
    is ``Done``, and the record it writes names the copy.

    A launch writes a new ``script.sh``, ``directives.yaml`` and step log, and records
    a new process and start time in the sentinel; the resume rewrites each sentinel
    with the same values, so a sentinel is compared by content.
    """
    launch_artifacts = [
        path
        for step in SHAPE
        for path in (
            relocated_run.step_dir(step) / "script.sh",
            relocated_run.step_dir(step) / "work" / "directives.yaml",
            relocated_run.step_dir(step) / "logs" / f"{slugify(step)}.out",
        )
    ]
    before = {p: p.stat().st_mtime_ns for p in launch_artifacts}
    sentinels_before = {
        step: yaml.safe_load(relocated_run.sentinel(step).read_text()) for step in SHAPE
    }

    proc = run_cstar(
        {**relocated_run.env, "COLUMNS": "400"},
        "workplan",
        "submit",
        "--run-id",
        RUN_ID,
        "--resume",
    )

    output = f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    assert proc.returncode == 0, output
    assert "run scheduling has completed" in proc.stdout, output
    assert str(relocated_run.original) not in proc.stdout, output
    assert str(relocated_run.run_dir) in proc.stdout, output

    assert {p: p.stat().st_mtime_ns for p in launch_artifacts} == before, output
    sentinels_after = {
        step: yaml.safe_load(relocated_run.sentinel(step).read_text()) for step in SHAPE
    }
    assert sentinels_after == sentinels_before, output
    assert all(s["status"] == DONE for s in sentinels_after.values())

    # the record written by the resume names the copy, not the original
    assert relocated_run.recorded_output_path() == relocated_run.run_dir

    # and what reads the record afterwards finds the copy
    proc = run_cstar(relocated_run.env, "workplan", "path", RUN_ID, "A")
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == f"{relocated_run.step_dir('A')}\n"
