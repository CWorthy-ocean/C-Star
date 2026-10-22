"""Tier 2: a finished run copied to this machine is attached, used and kept intact.

A two-step ``hello_world`` workplan runs through the real CLI under one set of homes;
its run directory is copied elsewhere and attached under fresh homes, which track
nothing, with ``cstar run attach``. The ``cstar run`` verbs then find the copy by its
run-id and by ``run://<uuid>``, an owned run may be gathered and repaired while a
read-only one is never written into, ``--verify`` compares the digests the original
recorded, and a workplan depends on the attached run by its run-id. ``hello_world``
produces no files, so the module gives its ``second`` step one product to check. No
ROMS is compiled; the module takes seconds.
"""

import json
import re
import shutil
import subprocess
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from cstar.base.feature import ENV_FF_DEVELOPER_MODE
from cstar.base.utils import slugify
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    STEP_RECORD_NAME,
    Product,
    read_run_manifest,
    read_step_record,
    write_step_record,
)
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    kill_run,
    run_cstar,
    run_log_tails,
    step_root,
    wait_for_terminal,
)
from cstar.tests.integration_tests.e2e.test_run_index import (
    OUTPUT_PLACEHOLDER,
    PRODUCER,
    STEP,
    _flat,
    make_env,
    write_consumer,
)
from cstar.tests.integration_tests.e2e.test_workplan_shapes import write_workplan

RUN_ID = "attach-base"
SHAPE = {"first": [], "second": ["first"]}
"""Each step of the producing run maps to the steps it depends on."""
RUN_TIMEOUT = 180.0
POLL_INTERVAL = 0.5
PRODUCT = "output/result.dat"
"""The file the producer step is given, relative to its step root."""
CONTENT = "hello product\n"
FLIPPED = "hellp product\n"
"""The content of the product with one byte changed: the size is the same."""
TRUNCATED = "hello\n"
DIGEST_HEADING = "Products whose content differs from the recorded digest"
INCOMPLETE = "the transfer of"


def _output(proc: subprocess.CompletedProcess[str]) -> str:
    """The captured streams of a finished ``cstar`` process, for assertion messages."""
    return f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


@dataclass(frozen=True)
class AttachRun:
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

    def env(self, name: str) -> dict[str, str]:
        """The environment of a fresh set of homes rooted at ``<root>/<name>``."""
        return make_env(self.root / name, self.shim)

    def copy(self, name: str, source: Path | None = None) -> Path:
        """Copy a run into ``<root>/<name>/runs`` and return the copy's directory."""
        copy = self.root / name / "runs" / RUN_ID
        shutil.copytree(source or self.pristine, copy, symlinks=True)
        return copy


def _product(run_dir: Path) -> Path:
    """The product of the producer step in a run directory."""
    return run_dir / "tasks" / slugify(PRODUCER) / PRODUCT


@pytest.fixture(scope="module")
def attach_run(
    tmp_path_factory: pytest.TempPathFactory, cstar_shim: Path
) -> Iterator[AttachRun]:
    """Run the base workplan, give its second step a product and copy the directory."""
    root = tmp_path_factory.mktemp("run-attach")
    producer = root / "producer"
    producer.mkdir()
    workplan = write_workplan(producer, SHAPE)
    env = make_env(producer, cstar_shim)
    data_home, state_home = producer / "data", producer / "state"
    steps = list(SHAPE)

    proc = run_cstar(env, "workplan", "run", "--run-id", RUN_ID, str(workplan))
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
                f"workplan run exited {proc.returncode} with {statuses}\n"
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

        pristine = root / "pristine" / RUN_ID
        pristine.parent.mkdir()
        shutil.copytree(original, pristine, symlinks=True)

        yield AttachRun(root, cstar_shim, manifest.id, env, original, pristine)
    finally:
        kill_run(state_home, RUN_ID, steps)


def _attach(
    env: dict[str, str], run_dir: Path, *options: str
) -> subprocess.CompletedProcess[str]:
    """Run ``cstar run attach`` on a run directory."""
    return run_cstar(env, "run", "attach", str(run_dir), *options)


def _attached(
    attach_run: AttachRun, name: str, *options: str
) -> tuple[dict[str, str], Path]:
    """Copy the run and attach the copy under fresh homes, which must succeed.

    Returns
    -------
    tuple[dict[str, str], Path]
        The homes' environment and the attached directory.
    """
    run_dir = attach_run.copy(name)
    env = attach_run.env(name)
    proc = _attach(env, run_dir, *options)
    assert proc.returncode == 0, _output(proc)
    return env, run_dir


def _ls_all(env: dict[str, str]) -> list[dict[str, str]]:
    """The rows of ``cstar run ls --all --format json``.

    The listing is printed through rich, which wraps a document at the terminal's
    width and so can break a string in two: the width is set wide.
    """
    proc = run_cstar(
        {**env, "COLUMNS": "400"}, "run", "ls", "--all", "--format", "json"
    )
    assert proc.returncode == 0, _output(proc)
    rows: list[dict[str, str]] = json.loads(proc.stdout)["data"]
    return rows


def test_attach_registers_a_copied_run(attach_run: AttachRun) -> None:
    """A copied run is attached as owned, and the verbs find it by run-id and by uuid."""
    env = attach_run.env("registers")
    run_dir = attach_run.copy("registers")

    # nothing tracks the run until it is attached
    assert run_cstar(env, "run", "path", RUN_ID).returncode == 2

    attached = _attach(env, run_dir)
    assert attached.returncode == 0, _output(attached)
    flat = _flat(attached.stdout)
    assert f"Attached run '{RUN_ID}' ({attach_run.handle})" in flat, _output(attached)
    assert "as owned" in flat, _output(attached)

    status = run_cstar(env, "run", "status", RUN_ID)
    assert status.returncode == 0, _output(status)
    assert RUN_ID in _flat(status.stdout), _output(status)
    for step in SHAPE:
        assert step in status.stdout, _output(status)

    for handle in (RUN_ID, attach_run.handle):
        shown = run_cstar(env, "run", "show", handle, "--format", "json")
        assert shown.returncode == 0, _output(shown)
        manifest = json.loads(shown.stdout)
        assert manifest["id"] == attach_run.uuid, handle
        assert {entry["name"] for entry in manifest["steps"]} == set(SHAPE), handle

    path = run_cstar(env, "run", "path", RUN_ID)
    assert path.returncode == 0, _output(path)
    assert Path(path.stdout.strip()).resolve() == run_dir.resolve(), _output(path)

    assert [(row["run_id"], row["where"]) for row in _ls_all(env)] == [
        (RUN_ID, "attached")
    ]

    # attaching again changes nothing
    again = _attach(env, run_dir)
    assert again.returncode == 0, _output(again)
    assert "already attached" in _flat(again.stdout), _output(again)


def test_attach_verify_compares_recorded_digests(attach_run: AttachRun) -> None:
    """``--verify`` accepts the recorded content, flags a changed one, and refuses a truncated one."""
    recorded = run_cstar(
        attach_run.original_env, "run", "check", RUN_ID, "--record-digests"
    )
    # the original is checked in place, where its workplan's absolute paths are found
    assert "Recorded 1 digest(s)." in _flat(recorded.stdout), _output(recorded)
    record = read_step_record(attach_run.original / "tasks" / slugify(PRODUCER))
    assert record is not None
    (product,) = record.products
    assert product.digest.startswith("sha256:"), record

    intact = attach_run.copy("verify-intact", attach_run.original)
    env = attach_run.env("verify-intact")
    proc = _attach(env, intact, "--verify")
    assert proc.returncode == 0, _output(proc)
    assert DIGEST_HEADING not in _flat(proc.stdout), _output(proc)

    # a byte of the product differs and its size does not: attached, with a problem
    changed = attach_run.copy("verify-changed", attach_run.original)
    _product(changed).write_text(FLIPPED)
    env = attach_run.env("verify-changed")
    proc = _attach(env, changed, "--verify")
    assert proc.returncode == 1, _output(proc)
    assert DIGEST_HEADING in _flat(proc.stdout), _output(proc)
    path = run_cstar(env, "run", "path", RUN_ID)
    assert path.returncode == 0, _output(path)

    # without --verify the sizes agree and the run is attached clean
    env = attach_run.env("verify-skipped")
    proc = _attach(env, changed)
    assert proc.returncode == 0, _output(proc)

    # a product shorter than recorded is a transfer that did not finish: not attached
    truncated = attach_run.copy("verify-truncated", attach_run.original)
    _product(truncated).write_text(TRUNCATED)
    env = attach_run.env("verify-truncated")
    proc = _attach(env, truncated, "--verify")
    assert proc.returncode == 1, _output(proc)
    assert INCOMPLETE in _flat(_output(proc)), _output(proc)
    assert "Not attached" in _flat(proc.stderr), _output(proc)
    path = run_cstar(env, "run", "path", RUN_ID)
    assert path.returncode == 2, _output(path)


def test_owned_attached_run_may_be_gathered_and_repaired_but_not_relaunched(
    attach_run: AttachRun,
) -> None:
    """An owned attached run takes ``gather`` and ``check --repair`` and refuses ``workplan run``."""
    env, run_dir = _attached(attach_run, "owned")

    gathered = run_cstar(env, "run", "gather", RUN_ID)
    assert gathered.returncode == 0, _output(gathered)
    assert (run_dir / "gathered_output").is_dir(), _output(gathered)

    repaired = run_cstar(env, "run", "check", RUN_ID, "--repair")
    assert repaired.returncode == 0, _output(repaired)

    relaunched = run_cstar(env, "workplan", "run", "--run-id", RUN_ID)
    assert relaunched.returncode == 2, _output(relaunched)
    assert "attached runs can be inspected and referenced" in _flat(
        _output(relaunched)
    ), _output(relaunched)


def test_read_only_attached_run_is_never_written_into(attach_run: AttachRun) -> None:
    """A read-only attached run refuses ``gather`` and ``check --repair``, and ``admin clean`` keeps its directory."""
    env, run_dir = _attached(attach_run, "read-only", "--read-only")

    assert [(row["run_id"], row["where"]) for row in _ls_all(env)] == [
        (RUN_ID, "attached, read-only")
    ]

    gathered = run_cstar(env, "run", "gather", RUN_ID)
    assert gathered.returncode == 2, _output(gathered)
    assert not (run_dir / "gathered_output").exists()

    repaired = run_cstar(env, "run", "check", RUN_ID, "--repair")
    assert repaired.returncode == 2, _output(repaired)
    assert "--own" in _flat(_output(repaired)), _output(repaired)

    # reading is unaffected
    assert run_cstar(env, "run", "check", RUN_ID).returncode == 0

    # `admin clean` is a developer-mode command
    cleaner = {**env, ENV_FF_DEVELOPER_MODE: "1"}
    cleaned = run_cstar(cleaner, "admin", "clean", "--run-id", RUN_ID, "--yes")
    assert cleaned.returncode == 0, _output(cleaned)
    assert (run_dir / RUN_MANIFEST_NAME).is_file(), _output(cleaned)
    assert _product(run_dir).read_text() == CONTENT
    gone = run_cstar(env, "run", "path", RUN_ID)
    assert gone.returncode == 2, _output(gone)


def test_a_workplan_depends_on_an_attached_run_by_its_run_id(
    attach_run: AttachRun,
) -> None:
    """A consuming workplan names the attached run by its bare run-id and reads its output."""
    env, run_dir = _attached(attach_run, "consumer")
    root = attach_run.root / "consumer"
    consumer_id = "consumer"
    workplan = write_consumer(root / "plan", RUN_ID)
    assert OUTPUT_PLACEHOLDER in workplan.read_text()

    proc = run_cstar(env, "workplan", "run", "--run-id", consumer_id, str(workplan))
    state_home, data_home = root / "state", root / "data"
    try:
        statuses = wait_for_terminal(
            state_home,
            consumer_id,
            [STEP],
            timeout=RUN_TIMEOUT,
            poll_interval=POLL_INTERVAL,
        )
        if proc.returncode != 0 or statuses != {STEP: DONE}:
            pytest.fail(
                f"workplan run exited {proc.returncode} with {statuses}\n"
                f"{_output(proc)}\n{run_log_tails(data_home, consumer_id)}"
            )
    finally:
        kill_run(state_home, consumer_id, [STEP])

    log = step_root(data_home, consumer_id, STEP) / "logs" / f"{STEP}.out"
    producer_output = run_dir / "tasks" / slugify(PRODUCER) / "output"
    assert f"Hello, {producer_output}" in log.read_text(errors="replace")


def test_attach_as_another_run_id(attach_run: AttachRun) -> None:
    """A run-id tracked for another run is refused, and ``--as`` attaches the run under a free one."""
    env, _ = _attached(attach_run, "as-first")

    other = attach_run.copy("as-second")
    other_uuid = str(uuid.uuid4())
    manifest_file = other / RUN_MANIFEST_NAME
    text = manifest_file.read_text()
    assert f"id: {attach_run.uuid}\n" in text
    manifest_file.write_text(
        text.replace(f"id: {attach_run.uuid}\n", f"id: {other_uuid}\n", 1)
    )

    refused = _attach(env, other)
    assert refused.returncode == 2, _output(refused)
    assert "--as" in _flat(_output(refused)), _output(refused)
    assert run_cstar(env, "run", "show", f"run://{other_uuid}").returncode == 2

    renamed = _attach(env, other, "--as", "other-id")
    assert renamed.returncode == 0, _output(renamed)
    assert re.search(r"Attached run 'other-id'", _flat(renamed.stdout)), _output(
        renamed
    )

    shown = run_cstar(env, "run", "show", f"run://{other_uuid}", "--format", "json")
    assert shown.returncode == 0, _output(shown)
    assert json.loads(shown.stdout)["id"] == other_uuid

    # the first run is still the one the run-id names
    first = run_cstar(env, "run", "show", RUN_ID, "--format", "json")
    assert first.returncode == 0, _output(first)
    assert json.loads(first.stdout)["id"] == attach_run.uuid
