"""Tier 2: a finished run is promoted to the project's runs area and stays usable.

A two-step ``hello_world`` workplan runs through the real CLI under one set of homes,
which track it. ``cstar run promote`` copies it into a project's runs area, where the
copy is listed, shown, checked and found by uuid; a dry run writes nothing, no project
home needs ``--to``, a link is replaced by a copy with ``--mode materialize``, a move
removes the source and tracks the copy as attached, ``--verify`` records digests in the
copy only, and a gathered run has its ``gathered_output`` rebuilt in the copy.
``hello_world`` produces no files, so the module gives its ``second`` step one product
to carry. No ROMS is compiled; the module takes seconds.
"""

import json
import shutil
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from cstar.base.env import ENV_CSTAR_PROJECT_HOME
from cstar.base.utils import slugify
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    STEP_RECORD_NAME,
    Product,
    read_run_manifest,
    read_step_record,
    write_step_record,
)
from cstar.orchestration.run_check import TRANSFORMED_WORKPLAN_GLOB
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    flat_output,
    kill_run,
    run_cstar,
    run_log_tails,
    step_root,
    wait_for_terminal,
)
from cstar.tests.integration_tests.e2e.test_run_index import (
    PRODUCER,
    STEP,
    make_env,
    write_consumer,
)
from cstar.tests.integration_tests.e2e.test_workplan_shapes import write_workplan

RUN_ID = "promote-base"
SHAPE = {"first": [], "second": ["first"]}
"""Each step of the producing run maps to the steps it depends on."""
RUN_TIMEOUT = 180.0
POLL_INTERVAL = 0.5
PRODUCT = "output/result.dat"
"""The file the producer step is given, relative to its step root."""
CONTENT = "hello product\n"
TRANSFORMED_WORKPLAN_NAME = "workplan_transformed.yaml"
ABSOLUTE_HEADING = "Absolute paths "
"""The start of both absolute-path headings: outside the run, and inside it."""
VERIFIED = "Compared 1 product(s) with their recorded digest: 1 matched."


def _output(proc: subprocess.CompletedProcess[str]) -> str:
    """The captured streams of a finished ``cstar`` process, for assertion messages."""
    return f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


@dataclass(frozen=True)
class PromoteRun:
    """A finished run, tracked under its original homes, with one product."""

    root: Path
    """The directory holding everything of the module."""
    shim: Path
    uuid: str
    """The ``id`` in the run's manifest."""
    original_env: dict[str, str]
    """The homes the run ran under, which track it."""
    original: Path
    """The run directory under those homes."""
    pristine: Path
    """A copy of the run as it finished, which no test changes."""

    @property
    def handle(self) -> str:
        """The ``run://`` handle of the run."""
        return f"run://{self.uuid}"

    def project(self, name: str) -> Path:
        """The project home ``<root>/<name>/project``, which does not exist yet."""
        return self.root / name / "project"

    def runs_area(self, name: str) -> Path:
        """The project runs area of ``project(name)``."""
        return self.project(name) / "cstar" / "runs"

    def env(self, name: str, project: bool = True) -> dict[str, str]:
        """The environment of fresh homes rooted at ``<root>/<name>``.

        Parameters
        ----------
        name : str
            The name of the set of homes.
        project : bool
            Whether the set has the project home ``project(name)``.
        """
        return make_env(
            self.root / name, self.shim, self.project(name) if project else None
        )

    def original_with_project(self, name: str) -> dict[str, str]:
        """The original homes, which track the run, with the project home ``project(name)``."""
        return {**self.original_env, ENV_CSTAR_PROJECT_HOME: str(self.project(name))}

    def copy(self, name: str) -> Path:
        """Copy the pristine run into ``<root>/<name>/runs`` and return its directory."""
        copy = self.root / name / "runs" / RUN_ID
        shutil.copytree(self.pristine, copy, symlinks=True)
        return copy


def _product(run_dir: Path) -> Path:
    """The product of the producer step in a run directory."""
    return run_dir / "tasks" / slugify(PRODUCER) / PRODUCT


@pytest.fixture(scope="module")
def promote_run(
    tmp_path_factory: pytest.TempPathFactory, cstar_shim: Path
) -> Iterator[PromoteRun]:
    """Run the base workplan, give its second step a product and copy the directory."""
    root = tmp_path_factory.mktemp("run-promote")
    producer = root / "producer"
    producer.mkdir()
    workplan = write_workplan(producer, SHAPE)
    env = make_env(producer, cstar_shim)
    data_home, state_home = producer / "data", producer / "state"
    steps = list(SHAPE)

    proc = run_cstar(env, "workplan", "submit", "--run-id", RUN_ID, str(workplan))
    try:
        statuses = wait_for_terminal(
            state_home,
            RUN_ID,
            steps,
            timeout=RUN_TIMEOUT,
            poll_interval=POLL_INTERVAL,
        )
        if proc.returncode != 0 or statuses != dict.fromkeys(steps, DONE):
            pytest.fail(
                f"workplan submit exited {proc.returncode} with {statuses}\n"
                f"{_output(proc)}\n{run_log_tails(data_home, RUN_ID)}"
            )

        original = data_home / "workplan_runs" / RUN_ID
        manifest = read_run_manifest(original)
        assert manifest is not None, f"no {RUN_MANIFEST_NAME} in {original}"

        # hello_world writes nothing: record one file as the step's product
        product = _product(original)
        product.write_text(CONTENT)
        step = original / "tasks" / slugify(PRODUCER)
        record = read_step_record(step)
        assert record is not None, f"no {STEP_RECORD_NAME} in {step}"
        record.products = [
            Product(kind="other", path=PRODUCT, size_bytes=len(CONTENT.encode()))
        ]
        write_step_record(step, record)

        # every step is terminal: `status` closes the latest attempt, which a move needs
        status = run_cstar(env, "run", "status", RUN_ID)
        assert status.returncode == 0, _output(status)

        pristine = root / "pristine" / RUN_ID
        pristine.parent.mkdir()
        shutil.copytree(original, pristine, symlinks=True)

        yield PromoteRun(root, cstar_shim, manifest.id, env, original, pristine)
    finally:
        kill_run(state_home, RUN_ID, steps)


def _promote(
    env: dict[str, str], target: str | Path, *options: str
) -> subprocess.CompletedProcess[str]:
    """Run ``cstar run promote`` on a run-id, handle or directory."""
    return run_cstar(env, "run", "promote", str(target), *options)


def _ls_shared(env: dict[str, str]) -> list[dict[str, str]]:
    """The rows of ``cstar run ls --shared --format json``.

    The listing is printed through rich, which wraps a document at the terminal's
    width and so can break a string in two: the width is set wide.
    """
    proc = run_cstar(
        {**env, "COLUMNS": "400"}, "run", "ls", "--shared", "--format", "json"
    )
    assert proc.returncode == 0, _output(proc)
    rows: list[dict[str, str]] = json.loads(proc.stdout)["data"]
    return rows


def _tree(run_dir: Path) -> dict[str, int]:
    """Every file of a directory with its size, for comparing a run before and after.

    The manifest is left out: promoting a run tracked here refreshes it, as
    ``cstar run show`` does.
    """
    return {
        str(path.relative_to(run_dir)): path.lstat().st_size
        for path in sorted(run_dir.rglob("*"))
        if path.name != RUN_MANIFEST_NAME
    }


def _assert_only_inherited_findings(proc: subprocess.CompletedProcess[str]) -> None:
    """Assert a ``cstar run check`` report names only the transformed workplan.

    The workplan the harness writes names its blueprints by absolute path, so the
    transformed workplan is reported under the absolute-path checks, which are
    inherited from the source and are not a problem of the copy.
    """
    lines = proc.stdout.splitlines()
    headings = [i for i, line in enumerate(lines) if line.startswith(ABSOLUTE_HEADING)]
    findings = []
    for heading in headings:
        for line in lines[heading + 1 :]:
            if not line.startswith("  "):
                break
            findings.append(line)
    assert all(TRANSFORMED_WORKPLAN_NAME in line for line in findings), _output(proc)


def _assert_complete_copy(promote_run: PromoteRun, copy: Path) -> None:
    """Assert a run directory holds what a promoted copy carries."""
    manifest = read_run_manifest(copy)
    assert manifest is not None, f"no {RUN_MANIFEST_NAME} in {copy}"
    assert manifest.id == promote_run.uuid
    assert [path.name for path in copy.glob(TRANSFORMED_WORKPLAN_GLOB)] == [
        TRANSFORMED_WORKPLAN_NAME
    ]
    for step in SHAPE:
        task = copy / "tasks" / slugify(step)
        assert task.is_dir(), step
        assert (task / STEP_RECORD_NAME).is_file(), step
    assert _product(copy).read_text() == CONTENT


def test_promote_copies_the_run_into_the_project_runs_area(
    promote_run: PromoteRun,
) -> None:
    """A copy lands in the project's runs area, is listed as ``project`` and is promoted once."""
    name = "copies"
    env = promote_run.original_with_project(name)
    before = _tree(promote_run.original)

    proc = _promote(env, RUN_ID)
    assert proc.returncode == 0, _output(proc)
    assert "Promoted run" in flat_output(proc.stdout), _output(proc)
    assert "products of second" in flat_output(proc.stdout), _output(proc)

    copy = promote_run.runs_area(name) / RUN_ID
    _assert_complete_copy(promote_run, copy)
    assert _tree(promote_run.original) == before

    # the project area holds one run, found as `project`
    assert [(row["run_id"], row["where"]) for row in _ls_shared(env)] == [
        (RUN_ID, "project")
    ]

    shown = run_cstar(env, "run", "show", str(copy), "--format", "json")
    assert shown.returncode == 0, _output(shown)
    assert json.loads(shown.stdout)["id"] == promote_run.uuid

    checked = run_cstar(env, "run", "check", str(copy))
    assert checked.returncode in (0, 1), _output(checked)
    _assert_only_inherited_findings(checked)

    again = _promote(env, RUN_ID)
    assert again.returncode == 2, _output(again)
    assert "already promoted" in flat_output(_output(again)), _output(again)


def test_promote_dry_run_writes_nothing(promote_run: PromoteRun) -> None:
    """``--dry-run`` prints the plan and leaves the destination absent."""
    name = "dry-run"
    env = promote_run.original_with_project(name)

    proc = _promote(env, RUN_ID, "--dry-run")
    assert proc.returncode == 0, _output(proc)
    flat = flat_output(proc.stdout)
    assert "Would copy" in flat, _output(proc)
    assert "Total:" in flat, _output(proc)
    assert not (promote_run.runs_area(name) / RUN_ID).exists()
    assert not promote_run.project(name).exists()


def test_promote_needs_a_project_home_or_to(promote_run: PromoteRun) -> None:
    """Without a project home ``promote`` names ``CSTAR_PROJECT_HOME`` and ``--to``, and ``--to`` takes the copy."""
    name = "needs-to"
    run_dir = promote_run.copy(name)
    env = promote_run.env(name, project=False)
    attached = run_cstar(env, "run", "attach", str(run_dir))
    assert attached.returncode == 0, _output(attached)

    refused = _promote(env, RUN_ID)
    assert refused.returncode == 2, _output(refused)
    message = flat_output(_output(refused))
    assert ENV_CSTAR_PROJECT_HOME in message, _output(refused)
    assert "--to" in message, _output(refused)

    target = promote_run.root / name / "elsewhere"
    proc = _promote(env, RUN_ID, "--to", str(target))
    assert proc.returncode == 0, _output(proc)
    _assert_complete_copy(promote_run, target / RUN_ID)


def test_link_then_materialize(promote_run: PromoteRun) -> None:
    """A linked run is a symlink to the run, and ``--mode materialize`` replaces it with a copy."""
    name = "linked"
    env = promote_run.original_with_project(name)
    destination = promote_run.runs_area(name) / RUN_ID

    linked = _promote(env, RUN_ID, "--mode", "link")
    assert linked.returncode == 0, _output(linked)
    assert destination.is_symlink(), _output(linked)
    assert destination.resolve() == promote_run.original.resolve()
    assert [(row["run_id"], row["where"]) for row in _ls_shared(env)] == [
        (RUN_ID, "project, linked")
    ]

    refused = _promote(env, RUN_ID, "--mode", "copy")
    assert refused.returncode == 2, _output(refused)
    assert "materialize" in flat_output(_output(refused)), _output(refused)

    proc = _promote(env, RUN_ID, "--mode", "materialize")
    assert proc.returncode == 0, _output(proc)
    assert not destination.is_symlink(), _output(proc)
    assert destination.is_dir()
    _assert_complete_copy(promote_run, destination)
    assert (promote_run.original / RUN_MANIFEST_NAME).is_file()
    assert _product(promote_run.original).read_text() == CONTENT


def test_a_workplan_depends_on_the_promoted_run_by_uuid(
    promote_run: PromoteRun,
) -> None:
    """A consuming workplan names the promoted run by ``run://<uuid>`` and reads the copy's output."""
    name = "consumer"
    project = promote_run.project(name)
    promoted = _promote(promote_run.original_with_project(name), RUN_ID)
    assert promoted.returncode == 0, _output(promoted)

    # fresh homes track nothing: the copy is found in the project's runs area
    env = promote_run.env(name)
    root = promote_run.root / name
    workplan = write_consumer(root / "plan", promote_run.handle)
    proc = run_cstar(env, "workplan", "submit", "--run-id", name, str(workplan))
    state_home, data_home = root / "state", root / "data"
    try:
        statuses = wait_for_terminal(
            state_home,
            name,
            [STEP],
            timeout=RUN_TIMEOUT,
            poll_interval=POLL_INTERVAL,
        )
        if proc.returncode != 0 or statuses != {STEP: DONE}:
            pytest.fail(
                f"workplan submit exited {proc.returncode} with {statuses}\n"
                f"{_output(proc)}\n{run_log_tails(data_home, name)}"
            )
    finally:
        kill_run(state_home, name, [STEP])

    log = step_root(data_home, name, STEP) / "logs" / f"{STEP}.out"
    producer_output = (
        project / "cstar" / "runs" / RUN_ID / "tasks" / slugify(PRODUCER) / "output"
    )
    assert f"Hello, {producer_output}" in log.read_text(errors="replace")


def test_move_updates_the_tracking_record_and_relaunch_is_refused(
    promote_run: PromoteRun,
) -> None:
    """A move tracks the copy as attached and read-only, and a read-only run is not moved."""
    name = "move"
    run_dir = promote_run.copy(name)
    env = promote_run.env(name)
    attached = run_cstar(env, "run", "attach", str(run_dir))
    assert attached.returncode == 0, _output(attached)
    destination = promote_run.runs_area(name) / RUN_ID

    proc = _promote(env, RUN_ID, "--mode", "move")
    assert proc.returncode == 0, _output(proc)
    assert not run_dir.exists(), _output(proc)
    _assert_complete_copy(promote_run, destination)
    assert "it is tracked here as attached (read-only)" in flat_output(proc.stdout), (
        _output(proc)
    )

    path = run_cstar(env, "run", "path", RUN_ID)
    assert path.returncode == 0, _output(path)
    assert Path(path.stdout.strip()).resolve() == destination.resolve(), _output(path)

    status = run_cstar(env, "run", "status", RUN_ID)
    assert status.returncode == 0, _output(status)

    relaunched = run_cstar(env, "workplan", "submit", "--run-id", RUN_ID)
    assert relaunched.returncode == 2, _output(relaunched)
    assert "attached runs can be inspected and referenced" in flat_output(
        _output(relaunched)
    ), _output(relaunched)

    gathered = run_cstar(env, "run", "gather", RUN_ID)
    assert gathered.returncode == 2, _output(gathered)

    # a run attached read-only is not removed
    name = "move-read-only"
    run_dir = promote_run.copy(name)
    env = promote_run.env(name)
    attached = run_cstar(env, "run", "attach", str(run_dir), "--read-only")
    assert attached.returncode == 0, _output(attached)
    refused = _promote(env, RUN_ID, "--mode", "move")
    assert refused.returncode == 2, _output(refused)
    assert (run_dir / RUN_MANIFEST_NAME).is_file(), _output(refused)
    assert not (promote_run.runs_area(name) / RUN_ID).exists()


def test_verify_records_digests_in_the_copy(promote_run: PromoteRun) -> None:
    """``--verify`` records the source's digests in the copy and never in the source."""
    name = "verify"
    env = promote_run.original_with_project(name)
    destination = promote_run.runs_area(name) / RUN_ID

    proc = _promote(env, RUN_ID, "--verify")
    assert proc.returncode == 0, _output(proc)
    assert "Recorded 1 digest(s) in the copy." in flat_output(proc.stdout), _output(
        proc
    )
    assert VERIFIED in proc.stdout, _output(proc)

    step = Path("tasks") / slugify(PRODUCER)
    copied = read_step_record(destination / step)
    assert copied is not None
    (product,) = copied.products
    assert product.digest.startswith("sha256:"), copied

    source = read_step_record(promote_run.original / step)
    assert source is not None
    assert [p.digest for p in source.products] == [""], source

    checked = run_cstar(env, "run", "check", str(destination), "--verify")
    assert checked.returncode in (0, 1), _output(checked)
    assert VERIFIED in checked.stdout, _output(checked)
    _assert_only_inherited_findings(checked)


def test_gathered_output_is_rebuilt_in_the_copy(promote_run: PromoteRun) -> None:
    """The copy's ``gathered_output`` links to the copy's products, not the source's."""
    name = "gathered"
    gathered = run_cstar(promote_run.original_env, "run", "gather", RUN_ID)
    assert gathered.returncode == 0, _output(gathered)
    assert (promote_run.original / "gathered_output").is_dir(), _output(gathered)

    target = promote_run.root / name / "elsewhere"
    proc = _promote(promote_run.original_env, RUN_ID, "--to", str(target))
    assert proc.returncode == 0, _output(proc)

    copy = target / RUN_ID
    link = copy / "gathered_output" / "result.dat"
    assert link.is_symlink(), _output(proc)
    assert link.resolve() == _product(copy).resolve(), _output(proc)
    assert link.read_text() == CONTENT
