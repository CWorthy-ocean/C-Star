"""Tier 2: workplan DAG shapes and failure propagation through the ``cstar`` CLI.

Steps are ``hello_world`` blueprints, so no ROMS is compiled and a run takes seconds.
``cstar workplan run`` schedules each step as a detached ``sh`` proxy that waits for
its dependencies and then runs ``cstar blueprint run``; the tests poll the step
sentinels and read the step logs to check ordering and propagation.
"""

import typing as t
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from cstar.base.utils import slugify
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    FAILED,
    kill_run,
    make_cli_env,
    make_shim,
    read_status,
    run_cstar,
    sentinel_path,
    step_root,
    wait_for_terminal,
)

SCHEDULED_MESSAGE = "run scheduling has completed"
DEPENDENCY_FAILED = "ended with status"
"""Part of the proxy's message when a dependency did not finish ``Done``."""
WALLTIME = "00:05:00"
ONE_SECOND = "00:00:01"
STARTUP_DELAY = 3.0
RUN_TIMEOUT = 180.0
POLL_INTERVAL = 0.5

SHAPES: dict[str, dict[str, list[str]]] = {
    "single": {"A": []},
    "linear": {"A": [], "B": ["A"], "C": ["B"]},
    "fanout": {"A": [], "B1": ["A"], "B2": ["A"], "B3": ["A"]},
    "parallel": {"A": [], "B": [], "C": []},
}
"""Each shape maps a step name to the steps it depends on."""


@dataclass(frozen=True)
class ShapeRun:
    """A scheduled workplan and where its artifacts live."""

    run_id: str
    env: dict[str, str]
    data_home: Path
    state_home: Path
    proc_stdout: str
    steps: tuple[str, ...]

    def sentinel(self, step: str) -> Path:
        """Path of the sentinel file holding ``step``'s status."""
        return sentinel_path(self.state_home, self.run_id, step)

    def log(self, step: str) -> Path:
        """Path of the stdout log of ``step``."""
        root = step_root(self.data_home, self.run_id, step)
        return root / "logs" / f"{slugify(step)}.out"

    def log_text(self, step: str) -> str:
        """The contents of the stdout log of ``step``."""
        return self.log(step).read_text(errors="replace")


def write_workplan(
    root: Path,
    shape: dict[str, list[str]],
    walltimes: dict[str, str] | None = None,
) -> Path:
    """Write a ``hello_world`` blueprint per step and the workplan that runs them.

    Every step sets ``max_walltime`` because a ``local`` override is what wraps the
    step command in ``timeout``; without one a hung step never ends. Each blueprint's
    ``target`` is its step name so the step logs are distinguishable.

    Parameters
    ----------
    root : Path
        The directory to write the blueprints and workplan into.
    shape : dict[str, list[str]]
        Step names mapped to the steps each depends on.
    walltimes : dict[str, str] | None
        Per-step ``max_walltime`` replacing the default.

    Returns
    -------
    Path
        The workplan's path.
    """
    walltimes = walltimes or {}
    steps = []
    for name, depends_on in shape.items():
        blueprint = root / f"{slugify(name)}.yaml"
        blueprint.write_text(
            yaml.safe_dump(
                {
                    "name": f"hello {name}",
                    "description": f"says hello to {name}",
                    "application": "hello_world",
                    "state": "draft",
                    "target": name,
                    "schema_version": "1.0.0",
                },
                sort_keys=False,
            )
        )
        step: dict[str, t.Any] = {
            "name": name,
            "application": "hello_world",
            "blueprint": str(blueprint),
            "compute_overrides": {
                "local": {"max_walltime": walltimes.get(name, WALLTIME)}
            },
        }
        if depends_on:
            step["depends_on"] = depends_on
        steps.append(step)

    workplan = root / "workplan.yaml"
    workplan.write_text(
        yaml.safe_dump(
            {"name": "shapes", "description": "dag shapes", "steps": steps},
            sort_keys=False,
        )
    )
    return workplan


def schedule(
    root: Path,
    shim: Path,
    run_id: str,
    shape: dict[str, list[str]],
    walltimes: dict[str, str] | None = None,
) -> tuple[ShapeRun, int, str]:
    """Run ``cstar workplan run`` and poll until every step is terminal.

    Returns
    -------
    tuple[ShapeRun, int, str]
        The run, the exit code of ``workplan run``, and its stdout and stderr.
    """
    workplan = write_workplan(root, shape, walltimes)
    env = make_cli_env(root, shim)
    proc = run_cstar(env, "workplan", "run", "--run-id", run_id, str(workplan))
    run = ShapeRun(
        run_id=run_id,
        env=env,
        data_home=root / "data",
        state_home=root / "state",
        proc_stdout=proc.stdout,
        steps=tuple(shape),
    )
    wait_for_terminal(
        run.state_home,
        run_id,
        run.steps,
        timeout=RUN_TIMEOUT,
        poll_interval=POLL_INTERVAL,
    )
    return run, proc.returncode, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


@pytest.fixture
def shape_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A fresh directory holding one run's blueprints, workplan and C-Star homes."""
    return tmp_path_factory.mktemp("shape")


@pytest.mark.parametrize("shape_name", SHAPES)
def test_shape_runs_in_dependency_order(
    shape_name: str, shape_root: Path, cstar_shim: Path
) -> None:
    """Every step ends ``Done`` and prints its hello, and runs after its dependencies."""
    shape = SHAPES[shape_name]
    run_id = f"shape-{shape_name}"
    run, returncode, output = schedule(shape_root, cstar_shim, run_id, shape)
    try:
        assert returncode == 0, f"workplan run exited {returncode}\n{output}"
        assert SCHEDULED_MESSAGE in run.proc_stdout, output

        statuses = {s: read_status(run.sentinel(s)) for s in shape}
        assert statuses == dict.fromkeys(shape, DONE), output

        for step in shape:
            assert f"Hello, {step}" in run.log_text(step), step

        for step, dependencies in shape.items():
            assert DEPENDENCY_FAILED not in run.log_text(step)
            # a sentinel is last written at Done, a log when the step printed its hello
            for dependency in dependencies:
                done_at = run.sentinel(dependency).stat().st_mtime
                printed_at = run.log(step).stat().st_mtime
                assert done_at <= printed_at, f"{step} printed before {dependency}"
    finally:
        kill_run(run.state_home, run_id, run.steps)


def test_dependency_failure_propagates(shape_root: Path) -> None:
    """A step killed by its walltime fails its dependent without running it.

    The shim delays every ``cstar`` start by `STARTUP_DELAY`, longer than step A's
    one-second walltime, so GNU ``timeout`` kills A however fast the CLI imports. The
    exit code of ``workplan run`` is not asserted: the DAG runner reports the failure
    only if A had already failed when B was submitted.
    """
    shape = {"A": [], "B": ["A"], "C": []}
    run_id = "shape-failure"
    slow_shim = make_shim(shape_root / "slow_bin", startup_delay=STARTUP_DELAY)
    run, _, output = schedule(
        shape_root, slow_shim, run_id, shape, walltimes={"A": ONE_SECOND}
    )
    try:
        statuses = {s: read_status(run.sentinel(s)) for s in shape}
        assert statuses == {"A": FAILED, "B": FAILED, "C": DONE}, output

        assert "Hello, A" not in run.log_text("A")
        assert DEPENDENCY_FAILED in run.log_text("B")
        assert "Hello, B" not in run.log_text("B")
        assert "Hello, C" in run.log_text("C")

        # a wide terminal keeps rich from truncating step names; it prints their slugs
        proc = run_cstar({**run.env, "COLUMNS": "200"}, "workplan", "status", run_id)
        assert proc.returncode == 0, proc.stderr
        assert all(slugify(step) in proc.stdout for step in shape), proc.stdout
    finally:
        kill_run(run.state_home, run_id, run.steps)
