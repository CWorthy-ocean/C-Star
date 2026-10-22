"""Tier 2: a finished run copied into a project's runs area is found and depended on.

A two-step ``hello_world`` workplan runs through the real CLI under one set of homes;
its run directory is copied into ``<project>/cstar/runs`` and the original is deleted.
Under fresh homes, which track nothing, a second workplan reaches the copy through
the index by its ``run://<uuid>`` handle and by its directory, ``cstar run
ls|show|status`` treat it as a run known only by its manifest, and a step whose record
says it failed is refused as a dependency. No ROMS is compiled; the module takes seconds.
"""

import json
import re
import shutil
import subprocess
import typing as t
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from cstar.base.env import ENV_CSTAR_PROJECT_HOME
from cstar.base.utils import slugify
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    STEP_RECORD_NAME,
    read_run_manifest,
)
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    kill_run,
    make_cli_env,
    run_cstar,
    run_log_tails,
    sentinel_path,
    step_root,
    wait_for_terminal,
)
from cstar.tests.integration_tests.e2e.test_workplan_shapes import (
    WALLTIME,
    write_workplan,
)

BASE_RUN_ID = "index-base"
BASE_SHAPE = {"first": [], "second": ["first"]}
"""Each step of the producing run maps to the steps it depends on."""
STEP = "next"
"""The only step of a consuming workplan."""
RUN_TIMEOUT = 180.0
POLL_INTERVAL = 0.5
PRODUCER = "second"
"""The step of the base run the consuming workplans depend on."""
OUTPUT_PLACEHOLDER = f"{{{{output_dir: {PRODUCER}@base}}}}"
"""A placeholder that needs the producer's location to be filled."""


def _output(proc: subprocess.CompletedProcess[str]) -> str:
    """The captured streams of a finished ``cstar`` process, for assertion messages."""
    return f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


def _flat(text: str) -> str:
    """The text on one line, without the borders of rich's panels."""
    return re.sub(r"\s+", " ", re.sub(r"[│╭╮╰╯─]", " ", text))


def make_env(root: Path, shim: Path, project: Path | None = None) -> dict[str, str]:
    """Build the environment of a set of homes rooted at ``root``.

    Parameters
    ----------
    root : Path
        The directory holding the set's data, state, cache and config homes.
    shim : Path
        The shim written by ``make_shim``.
    project : Path | None
        The project home of the set; none by default, so a ``$PROJECT`` of the
        developer's own cannot reach the test.

    Returns
    -------
    dict[str, str]
    """
    env = make_cli_env(root, shim)
    env.pop("PROJECT", None)
    env.pop(ENV_CSTAR_PROJECT_HOME, None)
    if project is not None:
        env[ENV_CSTAR_PROJECT_HOME] = str(project)
    return env


def write_consumer(root: Path, runs_ref: str) -> Path:
    """Write the workplan whose one step depends on the producer of the base run.

    The step's ``target`` is the producer's output directory, so its log says where
    the run's files were found.

    Parameters
    ----------
    root : Path
        The directory to write the blueprint and workplan into.
    runs_ref : str
        What the alias ``base`` names: a run-id, ``run://<uuid>`` or a directory.

    Returns
    -------
    Path
        The workplan's path.
    """
    root.mkdir(parents=True, exist_ok=True)
    blueprint = root / f"{STEP}.yaml"
    blueprint.write_text(
        yaml.safe_dump(
            {
                "name": "hello next",
                "description": "says hello where the base run kept its output",
                "application": "hello_world",
                "state": "draft",
                "target": "placeholder",
                "schema_version": "1.0.0",
            },
            sort_keys=False,
        )
    )
    workplan = root / "workplan.yaml"
    workplan.write_text(
        yaml.safe_dump(
            {
                "name": "consumer",
                "description": "depends on a step of a run copied into the project",
                "runs": {"base": runs_ref},
                "steps": [
                    {
                        "name": STEP,
                        "application": "hello_world",
                        "blueprint": str(blueprint),
                        "blueprint_overrides": {"target": OUTPUT_PLACEHOLDER},
                        "depends_on": [f"{PRODUCER}@base"],
                        "compute_overrides": {"local": {"max_walltime": WALLTIME}},
                    }
                ],
            },
            sort_keys=False,
        )
    )
    return workplan


@dataclass(frozen=True)
class SharedRun:
    """A finished run whose only copy is in a project's runs area."""

    root: Path
    """The directory holding everything of the module."""
    shim: Path
    uuid: str
    """The ``id`` in the run's manifest."""
    project: Path
    """The project home holding the copy."""

    @property
    def handle(self) -> str:
        """The ``run://`` handle of the run."""
        return f"run://{self.uuid}"

    @property
    def copy(self) -> Path:
        """The run directory in the project's runs area."""
        return self.project / "cstar" / "runs" / BASE_RUN_ID

    def producer_output(self, copy: Path | None = None) -> Path:
        """The output directory of the producer step in a run directory."""
        return (copy or self.copy) / "tasks" / slugify(PRODUCER) / "output"

    def env(self, name: str, project: Path | None) -> dict[str, str]:
        """The environment of a fresh set of homes rooted at ``<root>/<name>``."""
        return make_env(self.root / name, self.shim, project)

    def second_copy(self) -> tuple[Path, Path]:
        """Copy the run into a second project home.

        Returns
        -------
        tuple[Path, Path]
            The project home and the run directory in it.
        """
        project = self.root / "project-failed"
        copy = project / "cstar" / "runs" / BASE_RUN_ID
        shutil.copytree(self.copy, copy, symlinks=True)
        return project, copy


@pytest.fixture(scope="module")
def shared_run(
    tmp_path_factory: pytest.TempPathFactory, cstar_shim: Path
) -> Iterator[SharedRun]:
    """Run the base workplan, copy its directory into a project and delete the original."""
    root = tmp_path_factory.mktemp("run-index")
    producer = root / "producer"
    producer.mkdir()
    workplan = write_workplan(producer, BASE_SHAPE)
    env = make_env(producer, cstar_shim)
    data_home, state_home = producer / "data", producer / "state"
    steps = list(BASE_SHAPE)

    proc = run_cstar(env, "workplan", "run", "--run-id", BASE_RUN_ID, str(workplan))
    try:
        statuses = wait_for_terminal(
            state_home,
            BASE_RUN_ID,
            steps,
            timeout=RUN_TIMEOUT,
            poll_interval=POLL_INTERVAL,
        )
        if proc.returncode != 0 or statuses != dict.fromkeys(steps, DONE):
            pytest.fail(
                f"workplan run exited {proc.returncode} with {statuses}\n"
                f"{_output(proc)}\n{run_log_tails(data_home, BASE_RUN_ID)}"
            )

        original = data_home / "workplan_runs" / BASE_RUN_ID
        manifest = read_run_manifest(original)
        assert manifest is not None, f"no {RUN_MANIFEST_NAME} in {original}"

        project = root / "project"
        shared = SharedRun(root, cstar_shim, manifest.id, project)
        shared.copy.parent.mkdir(parents=True)
        shutil.copytree(original, shared.copy, symlinks=True)
        # anything still pointing at the original now fails loudly
        shutil.rmtree(original)
        assert not original.exists()

        yield shared
    finally:
        kill_run(state_home, BASE_RUN_ID, steps)


@dataclass(frozen=True)
class ConsumerRun:
    """A consuming workplan scheduled under homes that do not track the base run."""

    run_id: str
    env: dict[str, str]
    root: Path
    returncode: int
    output: str

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
        return self.data_home / "workplan_runs" / self.run_id

    def transformed(self) -> dict[str, t.Any]:
        """The transformed workplan the run persisted."""
        (path,) = self.run_root.glob("*_transformed.yaml")
        return yaml.safe_load(path.read_text())

    def log_text(self) -> str:
        """The contents of the stdout log of the consuming step."""
        log = step_root(self.data_home, self.run_id, STEP) / "logs" / f"{STEP}.out"
        return log.read_text(errors="replace")


def schedule_consumer(
    shared: SharedRun,
    name: str,
    runs_ref: str,
    project: Path | None,
    *,
    expect_done: bool = True,
) -> ConsumerRun:
    """Schedule a consuming workplan under a fresh set of homes.

    Parameters
    ----------
    shared : SharedRun
        The base run.
    name : str
        The name of the set of homes, which is also the run-id of the consumer.
    runs_ref : str
        What the alias ``base`` names.
    project : Path | None
        The project home of the set; none when the run is named by its directory.
    expect_done : bool
        Whether to wait for the step to be terminal and require it ``Done``.

    Returns
    -------
    ConsumerRun
    """
    root = shared.root / name
    workplan = write_consumer(root, runs_ref)
    env = shared.env(name, project)
    proc = run_cstar(env, "workplan", "run", "--run-id", name, str(workplan))
    run = ConsumerRun(name, env, root, proc.returncode, _output(proc))
    if not expect_done:
        return run

    try:
        statuses = wait_for_terminal(
            run.state_home,
            name,
            [STEP],
            timeout=RUN_TIMEOUT,
            poll_interval=POLL_INTERVAL,
        )
        if proc.returncode != 0 or statuses != {STEP: DONE}:
            pytest.fail(
                f"workplan run exited {proc.returncode} with {statuses}\n"
                f"{run.output}\n{run_log_tails(run.data_home, name)}"
            )
    finally:
        kill_run(run.state_home, name, [STEP])
    return run


def _assert_follows_the_copy(shared: SharedRun, run: ConsumerRun) -> None:
    """The consumer ran, and the producer's location it was given is in the copy."""
    manifest = read_run_manifest(run.run_root)
    assert manifest is not None, f"no {RUN_MANIFEST_NAME} in {run.run_root}"
    assert [(ref.alias, ref.run_id, ref.id) for ref in manifest.external.runs] == [
        ("base", BASE_RUN_ID, shared.uuid)
    ]

    (step,) = run.transformed()["steps"]
    overrides = step["directives"]["apply-overrides"]["overrides"]
    assert overrides["target"] == str(shared.producer_output()), overrides
    assert f"Hello, {shared.producer_output()}" in run.log_text()


@pytest.fixture(scope="module")
def by_uuid(shared_run: SharedRun) -> ConsumerRun:
    """A workplan that depends on the base run by ``run://<uuid>``, run to completion.

    Its homes are the ones the ``cstar run`` verbs are tried under: they track this
    run and nothing else.
    """
    return schedule_consumer(
        shared_run, "follow-uuid", shared_run.handle, shared_run.project
    )


def test_workplan_depends_on_a_run_by_uuid(
    shared_run: SharedRun, by_uuid: ConsumerRun
) -> None:
    """A run known only by its manifest is a dependency by ``run://<uuid>``.

    The scheduler pins the run into the persisted workplan, by uuid and run-id and
    not by directory, and the producer's output directory is the copy's.
    """
    pinned = by_uuid.transformed()["runs"]["base"]
    assert pinned["run_id"] == BASE_RUN_ID
    assert pinned["id"] == shared_run.uuid
    assert not pinned.get("path")
    assert pinned["start_at"]
    _assert_follows_the_copy(shared_run, by_uuid)


def test_workplan_depends_on_a_run_by_directory(shared_run: SharedRun) -> None:
    """A run directory is a dependency too, pinned with its path.

    The homes have no project, so the directory alone has to find the run.
    """
    run = schedule_consumer(shared_run, "follow-path", str(shared_run.copy), None)

    pinned = run.transformed()["runs"]["base"]
    assert pinned["run_id"] == BASE_RUN_ID
    assert pinned["id"] == shared_run.uuid
    assert Path(pinned["path"]).resolve() == shared_run.copy.resolve()
    _assert_follows_the_copy(shared_run, run)


def test_run_ls_lists_the_shared_run(by_uuid: ConsumerRun) -> None:
    """``run ls --shared`` lists the project's runs, and ``--all`` the tracked ones too."""
    shared = run_cstar(by_uuid.env, "run", "ls", "--shared", "--format", "json")
    assert shared.returncode == 0, _output(shared)
    assert [
        (row["run_id"], row["where"]) for row in json.loads(shared.stdout)["data"]
    ] == [(BASE_RUN_ID, "project")], _output(shared)

    everything = run_cstar(by_uuid.env, "run", "ls", "--all", "--format", "json")
    assert everything.returncode == 0, _output(everything)
    assert {
        (row["run_id"], row["where"]) for row in json.loads(everything.stdout)["data"]
    } == {
        (BASE_RUN_ID, "project"),
        (by_uuid.run_id, "local"),
    }, _output(everything)


def test_run_show_names_the_steps_of_the_run(
    shared_run: SharedRun, by_uuid: ConsumerRun
) -> None:
    """``run show run://<uuid>`` reads the copy's manifest and names its steps."""
    proc = run_cstar(by_uuid.env, "run", "show", shared_run.handle, "--format", "json")
    assert proc.returncode == 0, _output(proc)
    shown = json.loads(proc.stdout)
    assert shown["id"] == shared_run.uuid
    assert {entry["name"] for entry in shown["steps"]} == set(BASE_SHAPE)

    table = run_cstar(by_uuid.env, "run", "show", shared_run.handle)
    assert table.returncode == 0, _output(table)
    # a row of the steps table: the name cell, then the application
    for step in BASE_SHAPE:
        assert re.search(rf"│\s*{step}\s*│\s*hello_world\s*│", table.stdout), _output(
            table
        )


def test_run_status_refuses_a_run_that_is_not_tracked(
    shared_run: SharedRun, by_uuid: ConsumerRun
) -> None:
    """``run status`` reads live state, so it refuses a uuid and points at ``show``."""
    proc = run_cstar(by_uuid.env, "run", "status", shared_run.handle)

    assert proc.returncode == 2, _output(proc)
    assert f"cstar run show {shared_run.handle}" in _flat(_output(proc)), _output(proc)


def test_a_producer_that_is_not_done_is_refused(shared_run: SharedRun) -> None:
    """A step whose record says it failed cannot be depended on by a run not tracked here.

    The copy is a second one, in another project, so the other tests keep a run whose
    steps are done.
    """
    project, copy = shared_run.second_copy()
    record = copy / "tasks" / slugify(PRODUCER) / STEP_RECORD_NAME
    text = record.read_text()
    assert "outcome: completed" in text
    record.write_text(text.replace("outcome: completed", "outcome: failed"))

    run = schedule_consumer(
        shared_run, "follow-failed", shared_run.handle, project, expect_done=False
    )
    try:
        assert run.returncode != 0, run.output
        assert "done steps only" in _flat(run.output), run.output
        assert f"step {PRODUCER!r} of run {BASE_RUN_ID!r}" in _flat(run.output), (
            run.output
        )
        # the step was refused before it was submitted
        assert not sentinel_path(run.state_home, run.run_id, STEP).exists()
    finally:
        kill_run(run.state_home, run.run_id, [STEP])
