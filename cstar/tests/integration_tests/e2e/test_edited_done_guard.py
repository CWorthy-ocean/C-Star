"""Tier 2: re-entering a run refuses a ``Done`` step edited since it ran.

A two-step ``hello_world`` workplan runs to completion through the real CLI, so no ROMS
is compiled and the run takes seconds. The tests then edit step ``B``'s blueprint and
re-enter the run with ``--run-id``: the edit is refused unless ``--clobber B`` names
the step, in which case ``B`` alone is re-run.

The tests share one run and are ordered: each builds on the state the previous one
left in the run directory.

The last section covers steps that are still queued or running instead. Each of its
tests makes its own run and holds step ``A`` back at a gate: the steps run a shim that
waits for a file, so ``A`` stays ``Running`` and ``B``, queued behind it, ``Submitted``,
while the submitting CLI itself starts at once. The tests then re-enter the run to see
an edit to a queued step refused, ``--clobber`` cancel the old attempt's processes
before the step is submitted again, and a queued dependent follow a resubmitted
dependency without being named.
"""

import contextlib
import subprocess
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

import psutil
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
from cstar.orchestration.orchestration import Status
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    flat_output,
    kill_run,
    make_cli_env,
    make_shim,
    read_status,
    run_cstar,
    run_log_tails,
    sentinel_path,
    step_root,
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


def _set_target(root: Path, step: str, target: str) -> None:
    """Rewrite the blueprint of ``step``, written under ``root``, with a different
    ``target``.
    """
    path = root / f"{slugify(step)}.yaml"
    document = yaml.safe_load(path.read_text())
    document["target"] = target
    path.write_text(yaml.safe_dump(document, sort_keys=False))


def _edit_blueprint(run: GuardedRun, step: str, target: str) -> None:
    """Rewrite the blueprint of ``step`` with a different ``target``."""
    _set_target(run.root, step, target)


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


def test_reload_by_run_id_does_not_clobber_again(guarded_run: GuardedRun) -> None:
    """Re-entering by the run-id alone, which replays the recorded plan and its
    ``--clobber B``, leaves ``B`` as the previous attempt re-ran it.
    """
    record = read_step_record(guarded_run.step_root(EDITED_STEP))
    sentinel = sentinel_path(guarded_run.state_home, RUN_ID, EDITED_STEP)
    pid = yaml.safe_load(sentinel.read_text())["pid"]

    proc = run_cstar(guarded_run.env, "workplan", "submit", "--run-id", RUN_ID)

    assert proc.returncode == 0, _output(proc)
    assert guarded_run.current_manifest().attempts[-1].clobbered == []
    assert yaml.safe_load(sentinel.read_text())["pid"] == pid
    assert read_step_record(guarded_run.step_root(EDITED_STEP)) == record


# --- steps that are still queued or running ---------------------------------------

GATE_TIMEOUT = 60.0
"""Seconds the first submit is given to leave ``A`` running and ``B`` queued."""
EXIT_TIMEOUT = 15.0
"""Seconds a cancelled process is given to be gone once the re-entry has returned."""
QUEUED_SHAPE = {"A": [], "B": ["A"]}
DEPENDENCY_FAILED = "ended with status"
"""Part of the message a proxy logs when a dependency did not finish ``Done``."""


@dataclass(frozen=True)
class GatedRun:
    """A run set up to submit with its steps held at a gate, and re-enter without one."""

    run_id: str
    root: Path
    workplan: Path
    gate: Path
    """The file the gated shim waits to see exist; create it to let steps start."""
    fast_shim: Path
    gated_shim: Path
    slow_env: dict[str, str]
    """The environment whose ``PATH`` holds the gated shim, so steps block at the gate."""
    fast_env: dict[str, str]
    """The environment of the CLI and of the steps a re-entry launches: not gated."""

    @property
    def state_home(self) -> Path:
        """The ``CSTAR_STATE_HOME`` of the run."""
        return self.root / "state"

    @property
    def data_home(self) -> Path:
        """The ``CSTAR_DATA_HOME`` of the run."""
        return self.root / "data"

    def submit(self) -> subprocess.CompletedProcess[str]:
        """Submit the workplan with the gated shim on the steps' ``PATH``.

        The submitting CLI runs the fast shim by its path, so only the steps it
        schedules wait at the gate.
        """
        return run_cstar(
            self.slow_env,
            "workplan",
            "submit",
            "--run-id",
            self.run_id,
            str(self.workplan),
            shim=self.fast_shim,
        )

    def reenter(self, *args: str) -> subprocess.CompletedProcess[str]:
        """Re-enter the run with the fast shim, passing ``args`` to ``workplan submit``."""
        return run_cstar(
            self.fast_env,
            "workplan",
            "submit",
            "--run-id",
            self.run_id,
            *args,
            str(self.workplan),
        )

    def attempts(self) -> int:
        """The number of attempts the run's manifest records."""
        manifest = read_run_manifest(self.data_home / "workplan_runs" / self.run_id)
        assert manifest is not None, f"no {RUN_MANIFEST_NAME} for run {self.run_id!r}"
        return len(manifest.attempts)

    def sentinel(self, step: str) -> Path:
        """Path of the sentinel of ``step``."""
        return sentinel_path(self.state_home, self.run_id, step)

    def pid(self, step: str) -> int | None:
        """The pid ``step``'s sentinel records, or ``None`` while it is unreadable."""
        try:
            return int(yaml.safe_load(self.sentinel(step).read_text())["pid"])
        except (OSError, KeyError, TypeError, ValueError, yaml.YAMLError):
            return None

    def log(self, step: str) -> Path:
        """Path of the stdout log of ``step``."""
        return (
            step_root(self.data_home, self.run_id, step)
            / "logs"
            / f"{slugify(step)}.out"
        )

    def diagnostics(self) -> str:
        """The sentinels and step logs, for the message of a failed wait."""
        sentinels = "\n".join(
            f"--- {self.sentinel(step)} ---\n{_read(self.sentinel(step))}"
            for step in QUEUED_SHAPE
        )
        return f"{sentinels}\n{run_log_tails(self.data_home, self.run_id)}"


@pytest.fixture
def gated_run(
    request: pytest.FixtureRequest, tmp_path: Path, cstar_shim: Path
) -> Iterator[GatedRun]:
    """A run with its blueprints written, steps gated, and its leftovers cleaned up.

    The gate is created and every step still alive terminated on teardown, so a test
    that fails midway leaves no process behind. The run id is the test's name.
    """
    gate = tmp_path / "release"
    gated = make_shim(tmp_path / "gated_bin", gate=gate)
    run = GatedRun(
        run_id=slugify(request.node.name),
        root=tmp_path,
        workplan=write_workplan(tmp_path, QUEUED_SHAPE),
        gate=gate,
        fast_shim=cstar_shim,
        gated_shim=gated,
        slow_env=make_cli_env(tmp_path, gated),
        fast_env=make_cli_env(tmp_path, cstar_shim),
    )
    try:
        yield run
    finally:
        gate.touch()
        kill_run(run.state_home, run.run_id, list(QUEUED_SHAPE))
        for step in QUEUED_SHAPE:
            if (pid := run.pid(step)) is not None:
                _kill_tree(pid)


def _read(path: Path) -> str:
    """The text of ``path``, or a note that it is unreadable."""
    try:
        return path.read_text(errors="replace")
    except OSError as ex:
        return f"unreadable: {ex}"


def _kill_tree(pid: int) -> None:
    """Kill the process ``pid`` and everything under it; a process already gone is
    skipped. SIGKILL also ends a process a test left stopped, which SIGTERM would not.
    """
    with contextlib.suppress(psutil.Error):
        process = psutil.Process(pid)
        for child in [*process.children(recursive=True), process]:
            with contextlib.suppress(psutil.Error):
                child.kill()


def _alive(process: psutil.Process) -> bool:
    """Whether ``process`` still runs.

    `psutil.Process` compares the creation time, so a pid recycled since the process
    was captured does not count as it; a zombie, which has exited, does not either.
    """
    try:
        return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except psutil.Error:
        return False


def _wait_until(
    condition: Callable[[], bool], what: str, run: GatedRun, timeout: float
) -> None:
    """Poll ``condition`` until it holds, or fail with the run's state after ``timeout``."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail(f"{what} within {timeout:g}s\n{run.diagnostics()}")
        time.sleep(0.2)


def _wait_until_gone(
    processes: Sequence[psutil.Process], what: str, run: GatedRun
) -> None:
    """Wait for every one of ``processes`` to have exited, or fail naming the survivors."""
    _wait_until(
        lambda: not any(_alive(p) for p in processes),
        f"{what} did not exit",
        run,
        EXIT_TIMEOUT,
    )


def _submit_gated(run: GatedRun) -> tuple[psutil.Process, psutil.Process]:
    """Submit the run gated and wait for ``A`` to be running and ``B`` queued behind it.

    The wait also covers ``A``'s processes reaching the gate, so the processes a later
    test records under ``A`` include the ones doing the work.

    Returns
    -------
    tuple[psutil.Process, psutil.Process]
        The processes of ``A``'s and ``B``'s proxies, as their sentinels record them.
    """
    proc = run.submit()
    assert proc.returncode == 0, _output(proc)

    def queued() -> bool:
        return (
            read_status(run.sentinel("A")) == int(Status.Running)
            and read_status(run.sentinel("B")) == int(Status.Submitted)
            and run.pid("A") is not None
            and run.pid("B") is not None
        )

    _wait_until(queued, "A was not Running with B Submitted", run, GATE_TIMEOUT)
    proxy_a, proxy_b = (psutil.Process(run.pid(step)) for step in ("A", "B"))
    _wait_until(
        lambda: _at_gate(proxy_a, run),
        "A's step did not reach the gate",
        run,
        GATE_TIMEOUT,
    )
    return proxy_a, proxy_b


def _at_gate(proxy: psutil.Process, run: GatedRun) -> bool:
    """Whether a process under ``proxy`` runs the gated shim."""
    with contextlib.suppress(psutil.Error):
        for child in proxy.children(recursive=True):
            with contextlib.suppress(psutil.Error):
                if str(run.gated_shim) in child.cmdline():
                    return True
    return False


def _edit_b(run: GatedRun) -> None:
    """Edit step ``B``'s blueprint, which the run recorded when it was submitted."""
    _set_target(run.root, "B", EDITED_TARGET)


def test_edited_queued_step_is_refused_then_clobber_replaces_it(
    gated_run: GatedRun,
) -> None:
    """An edit to a queued step is refused; ``--clobber B`` cancels its proxy, submits
    it again behind the same running ``A``, and the new ``B`` prints the edit.
    """
    run = gated_run
    proxy_a, proxy_b = _submit_gated(run)
    pid_a, pid_b = proxy_a.pid, proxy_b.pid
    attempts = run.attempts()

    _edit_b(run)
    refused = run.reenter()

    assert refused.returncode == 1, _output(refused)
    assert "An invalid request was made" in refused.stdout, _output(refused)
    assert repr("B") in refused.stdout, _output(refused)
    assert "--clobber B" in refused.stdout, _output(refused)
    assert "queued or running" in flat_output(refused.stdout), _output(refused)
    assert "--run-id" in refused.stdout, _output(refused)
    assert repr("A") not in refused.stdout, _output(refused)
    assert (run.pid("A"), run.pid("B")) == (pid_a, pid_b), run.diagnostics()
    assert _alive(proxy_a) and _alive(proxy_b), run.diagnostics()
    assert run.attempts() == attempts

    replaced = run.reenter("--clobber", "B")

    assert replaced.returncode == 0, _output(replaced) + run.diagnostics()
    assert f"Cancelled job {pid_b} of step 'B'" in replaced.stdout, _output(replaced)
    _wait_until_gone([proxy_b], f"B's old proxy {pid_b}", run)
    new_pid_b = run.pid("B")
    assert new_pid_b not in (None, pid_b), run.diagnostics()
    assert run.pid("A") == pid_a and _alive(proxy_a), run.diagnostics()
    assert read_status(run.sentinel("B")) == int(Status.Submitted), run.diagnostics()

    # by the run-id alone, the recorded --clobber B is spent: B is left queued
    reloaded = run_cstar(run.fast_env, "workplan", "submit", "--run-id", run.run_id)

    assert reloaded.returncode == 0, _output(reloaded) + run.diagnostics()
    assert "Cancelled job" not in reloaded.stdout, _output(reloaded)
    assert run.pid("B") == new_pid_b, run.diagnostics()

    run.gate.touch()
    statuses = wait_for_terminal(
        run.state_home,
        run.run_id,
        list(QUEUED_SHAPE),
        timeout=RUN_TIMEOUT,
        poll_interval=POLL_INTERVAL,
    )
    assert statuses == dict.fromkeys(QUEUED_SHAPE, DONE), run.diagnostics()
    assert "Hello, A" in _read(run.log("A"))
    log_b = _read(run.log("B"))
    assert f"Hello, {EDITED_TARGET}" in log_b
    # the new proxy waited on the pid of the A that kept running
    assert f"Awaiting process {pid_a}" in log_b


def test_clobber_of_a_running_step_requeues_its_queued_dependent(
    gated_run: GatedRun,
) -> None:
    """``--clobber A`` cancels the running ``A`` and every process under it, and also
    the queued ``B`` waiting on it, with no edit and without naming ``B``; the new
    ``A`` and ``B``, launched by the fast shim, then run in order.
    """
    run = gated_run
    proxy_a, proxy_b = _submit_gated(run)
    pid_a, pid_b = proxy_a.pid, proxy_b.pid
    under_a = proxy_a.children(recursive=True)
    assert under_a, "A's proxy has no process under it"
    # B's proxy polls A's pid every couple of seconds, and would record B Failed on
    # finding A gone before the re-entry reaches B, which then needs no cancel;
    # stopped, it cannot, so the cancel of a queued dependent is always exercised
    proxy_b.suspend()

    proc = run.reenter("--clobber", "A")

    assert proc.returncode == 0, _output(proc) + run.diagnostics()
    for step, pid in (("A", pid_a), ("B", pid_b)):
        assert f"Cancelled job {pid} of step '{step}'" in proc.stdout, _output(proc)
    _wait_until_gone([proxy_a, *under_a, proxy_b], "the old attempts", run)
    assert run.pid("A") not in (None, pid_a), run.diagnostics()
    assert run.pid("B") not in (None, pid_b), run.diagnostics()

    # the gate stays closed: the new attempts were launched by the fast shim
    statuses = wait_for_terminal(
        run.state_home,
        run.run_id,
        list(QUEUED_SHAPE),
        timeout=RUN_TIMEOUT,
        poll_interval=POLL_INTERVAL,
    )
    assert statuses == dict.fromkeys(QUEUED_SHAPE, DONE), run.diagnostics()
    assert "Hello, A" in _read(run.log("A"))
    assert "Hello, B" in _read(run.log("B"))
    assert DEPENDENCY_FAILED not in _read(run.log("B"))
    done_at = run.sentinel("A").stat().st_mtime
    assert done_at <= run.log("B").stat().st_mtime, "B printed before A was done"
