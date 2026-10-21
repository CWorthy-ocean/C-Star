"""Tests for the run manifest and step record."""

import fcntl
import hashlib
import logging
import threading
import time
import typing as t
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

import cstar.orchestration.manifest as manifest_module
from cstar.applications.core import ArtifactKind, ArtifactSpec
from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.utils import slugify
from cstar.execution.file_system import (
    LOCK_POLL_INTERVAL,
    LockTimeoutError,
    StateDirectoryManager,
)
from cstar.orchestration.manifest import (
    ATTEMPT_DIR_FORMAT,
    ATTEMPTS_DIR_NAME,
    MANIFEST_LOCK_NAME,
    RUN_MANIFEST_NAME,
    STEP_RECORD_NAME,
    Attempt,
    AttemptStatus,
    CarriedInputs,
    CodebaseRef,
    ExecutionFacts,
    ExternalDependencies,
    ExternalRun,
    Product,
    RunManifest,
    SourceDataRef,
    StepEntry,
    StepFacts,
    StepRecord,
    WorkplanIdentity,
    _now,
    carried_digest,
    collect_dependencies,
    create_attempt_dir,
    describe_steps,
    ensure_run_manifest,
    expand_artifacts,
    finalize_standalone_run,
    fold_step_records,
    latest_attempt_dir,
    manifest_lock,
    merge_products,
    new_attempt,
    product_paths,
    read_run_manifest,
    read_step_record,
    record_submission,
    recorded_files_in,
    require_recorded_files,
    workplan_content_hash,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import (
    BlueprintIdentity,
    CatalogSpecRef,
    Provenance,
    RunRef,
    Step,
    Workplan,
)
from cstar.orchestration.serialization import load_blueprint
from cstar.system.migration import CstarSchemaTooNewError

NOW = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)

CURRENT = RunManifest.model_fields["schema_version"].default
"""The schema version this build writes, which both documents carry."""

_MAJOR, _MINOR, _ = CURRENT.split(".")
NEWER_MINOR = f"{_MAJOR}.{int(_MINOR) + 10}.0"
"""A newer minor with an extra digit, which a text comparison would misorder."""

NEWER_MAJOR = f"{int(_MAJOR) + 1}.0.0"
"""A newer major."""


def _identity(name: str = "bp", content_hash: str = "abc") -> BlueprintIdentity:
    return BlueprintIdentity(
        kind="Blueprint",
        application="hello_world",
        name=name,
        content_hash=content_hash,
    )


def _record(
    step: str = "one", finished_at: datetime = NOW, **kwargs: t.Any
) -> StepRecord:
    return StepRecord(
        step=step,
        application="hello_world",
        blueprint=_identity(),
        outcome="completed",
        finished_at=finished_at,
        **kwargs,
    )


def _manifest(*names: str) -> RunManifest:
    return RunManifest(
        run_id="run",
        name="run",
        kind="workplan",
        created_at=NOW,
        attempts=[Attempt(start_at=NOW)],
        steps=[
            StepEntry(name=n, application="hello_world", working_dir=f"tasks/{n}")
            for n in names
        ],
    )


def _write_blueprint(path: Path, target: str = "world") -> Path:
    bp = HelloWorldBlueprint(name="hello", description="says hello", target=target)
    path.write_text(
        f"name: {bp.name}\ndescription: {bp.description}\n"
        f"application: hello_world\nschema_version: '1.0.0'\n"
        f"target: {target}\n"
    )
    return path


def _workplan(*steps: Step) -> Workplan:
    return Workplan(name="plan", description="a plan", steps=steps)


def _step(blueprint: t.Any, name: str = "one", **kwargs: t.Any) -> Step:
    return Step(name=name, application="hello_world", blueprint=blueprint, **kwargs)


class TestRoundTrip:
    def test_run_manifest(self, tmp_path: Path) -> None:
        manifest = _manifest("one")
        manifest.workplan = WorkplanIdentity(name="p", content_hash="h", path="p.yaml")
        manifest.steps[0].record = _record(
            products=[
                Product(
                    kind=ArtifactKind.grid,
                    path="output/grid.nc",
                    metadata={"stream": "his", "n": 1, "x": 0.5, "l": ["a"]},
                )
            ],
            start_at=NOW,
        )
        manifest.external = ExternalDependencies(
            runs=[ExternalRun(alias="a", run_id="r", start_at=NOW)]
        )
        manifest.observed_at = NOW

        path = write_run_manifest(tmp_path / "nested", manifest)

        assert path == tmp_path / "nested" / RUN_MANIFEST_NAME
        assert read_run_manifest(tmp_path / "nested") == manifest

    def test_created_at_survives_being_now(self, tmp_path: Path) -> None:
        """A timestamp equal to "now" is written, not dropped as a default."""
        manifest = _manifest("one")
        manifest.created_at = _now()

        write_run_manifest(tmp_path, manifest)

        assert "created_at:" in (tmp_path / RUN_MANIFEST_NAME).read_text()
        assert read_run_manifest(tmp_path) == manifest

    def test_step_record(self, tmp_path: Path) -> None:
        record = _record(versions={"cstar-ocean": "1.0"}, errors=["boom"])

        path = write_step_record(tmp_path / "step", record)

        assert path == tmp_path / "step" / STEP_RECORD_NAME
        assert read_step_record(tmp_path / "step") == record

    def test_default_execution_facts_are_not_written(self, tmp_path: Path) -> None:
        """A record that says nothing of what ran it writes no `execution` key, and
        reads back with the defaults.
        """
        write_step_record(tmp_path, _record())

        assert "execution" not in (tmp_path / STEP_RECORD_NAME).read_text()
        record = read_step_record(tmp_path)
        assert record is not None
        assert record.execution == ExecutionFacts()

    def test_execution_facts_round_trip(self, tmp_path: Path) -> None:
        facts = ExecutionFacts(
            scheduler="slurm",
            job_id="4242",
            host="node-1",
            pid=777,
            nodes=2,
            tasks=8,
            cpus=4,
            partition="debug",
            account="m1234",
        )
        write_step_record(tmp_path, _record(execution=facts))

        record = read_step_record(tmp_path)

        assert record is not None
        assert record.execution == facts
        assert "execution:" in (tmp_path / STEP_RECORD_NAME).read_text()

    def test_a_submission_round_trips_in_whole_seconds(self, tmp_path: Path) -> None:
        manifest = _manifest("one")
        manifest.steps[0].launcher = "slurm"
        manifest.steps[0].job_id = "4242"
        manifest.steps[0].submitted_at = NOW + timedelta(milliseconds=1500)

        write_run_manifest(tmp_path, manifest)

        loaded = read_run_manifest(tmp_path)
        assert loaded is not None
        assert loaded.steps[0].submitted_at == NOW + timedelta(seconds=1)
        assert (loaded.steps[0].launcher, loaded.steps[0].job_id) == ("slurm", "4242")

    def test_unknown_product_kind_still_loads(self, tmp_path: Path) -> None:
        """A kind this build does not know is kept as text and read as `other`."""
        record = _record(products=[Product(kind="holo_grid", path="x.nc")])
        write_step_record(tmp_path, record)

        loaded = read_step_record(tmp_path)

        assert loaded is not None
        assert loaded.products[0].kind == "holo_grid"
        assert loaded.products[0].known_kind is ArtifactKind.other
        assert (
            Product(kind=ArtifactKind.grid, path="g.nc").known_kind is ArtifactKind.grid
        )

    def test_missing_is_none(self, tmp_path: Path) -> None:
        assert read_run_manifest(tmp_path) is None
        assert read_step_record(tmp_path) is None

    def test_documents_carry_the_current_schema(self, tmp_path: Path) -> None:
        write_run_manifest(tmp_path, _manifest("one"))
        write_step_record(tmp_path, _record())

        assert StepRecord.model_fields["schema_version"].default == CURRENT
        text = f"schema_version: {CURRENT}"
        assert text in (tmp_path / RUN_MANIFEST_NAME).read_text()
        assert text in (tmp_path / STEP_RECORD_NAME).read_text()

    def test_a_1_0_0_manifest_and_record_still_load(self, tmp_path: Path) -> None:
        """The carried-input, execution and submission fields are additive: older
        files read with defaults.
        """
        write_run_manifest(tmp_path, _manifest("one"))
        write_step_record(tmp_path, _record())
        for name in (RUN_MANIFEST_NAME, STEP_RECORD_NAME):
            path = tmp_path / name
            path.write_text(path.read_text().replace(CURRENT, "1.0.0", 1))

        manifest = read_run_manifest(tmp_path)
        record = read_step_record(tmp_path)

        assert manifest is not None
        assert record is not None
        assert manifest.schema_version == "1.0.0"
        assert (manifest.attempts[0].directory, manifest.attempts[0].carried) == (
            "",
            {},
        )
        assert manifest.steps[0].carried_blueprint == ""
        assert (
            manifest.steps[0].launcher,
            manifest.steps[0].job_id,
            manifest.steps[0].submitted_at,
        ) == ("", "", None)
        assert record.blueprint_file == ""
        assert record.execution == ExecutionFacts()

    def test_an_older_document_is_restamped_when_written(self, tmp_path: Path) -> None:
        """A document read from an older file may gain this build's fields, so it is
        written under this build's schema version, which an older build refuses as
        too new rather than failing on fields it does not know.
        """
        write_run_manifest(tmp_path, _manifest("one"))
        write_step_record(tmp_path, _record())
        for name in (RUN_MANIFEST_NAME, STEP_RECORD_NAME):
            path = tmp_path / name
            path.write_text(path.read_text().replace(CURRENT, "1.0.0", 1))
        manifest = read_run_manifest(tmp_path)
        record = read_step_record(tmp_path)
        assert manifest is not None
        assert record is not None
        manifest.attempts[0].directory = "attempts/20261015T120000"

        write_run_manifest(tmp_path, manifest)
        write_step_record(tmp_path, record)

        assert manifest.schema_version == record.schema_version == CURRENT
        text = f"schema_version: {CURRENT}"
        assert text in (tmp_path / RUN_MANIFEST_NAME).read_text()
        assert text in (tmp_path / STEP_RECORD_NAME).read_text()

    def test_carried_inputs_round_trip(self, tmp_path: Path) -> None:
        manifest = _manifest("one")
        manifest.attempts[0].directory = "attempts/20261015T120000"
        manifest.attempts[0].carried = {"workplan.yaml": "sha256:ab"}
        manifest.steps[
            0
        ].carried_blueprint = "attempts/20261015T120000/blueprints/one/b.yaml"
        write_run_manifest(tmp_path, manifest)
        write_step_record(tmp_path, _record(blueprint_file="work/b.ovrd.yaml"))

        assert read_run_manifest(tmp_path) == manifest
        record = read_step_record(tmp_path)
        assert record is not None
        assert record.blueprint_file == "work/b.ovrd.yaml"

    def test_newer_schema_refused(self, tmp_path: Path) -> None:
        write_run_manifest(tmp_path, _manifest("one"))
        path = tmp_path / RUN_MANIFEST_NAME
        path.write_text(path.read_text().replace(CURRENT, NEWER_MINOR, 1))

        with pytest.raises(
            CstarSchemaTooNewError, match=f"run manifest schema {NEWER_MINOR}"
        ):
            read_run_manifest(tmp_path)

    def test_newer_step_record_refused(self, tmp_path: Path) -> None:
        write_step_record(tmp_path, _record())
        path = tmp_path / STEP_RECORD_NAME
        path.write_text(path.read_text().replace(CURRENT, NEWER_MAJOR, 1))

        with pytest.raises(CstarSchemaTooNewError, match="step record"):
            read_step_record(tmp_path)

    @pytest.mark.parametrize("text", ["- not\n- a mapping\n", "{{{", "run_id: x\n"])
    def test_corrupt_names_the_path(self, tmp_path: Path, text: str) -> None:
        (tmp_path / RUN_MANIFEST_NAME).write_text(text)

        with pytest.raises(CstarError, match=str(tmp_path / RUN_MANIFEST_NAME)):
            read_run_manifest(tmp_path)


def test_manifest_lock_creates_the_lock_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CSTAR_STATE_HOME", str(tmp_path / "state"))

    with manifest_lock("my-run"):
        lock = tmp_path / "state" / "run_state" / "my-run" / MANIFEST_LOCK_NAME
        assert lock.exists()

    with manifest_lock("my-run"):  # released, so it can be taken again
        pass


@contextmanager
def held_manifest_lock(run_id: str) -> t.Iterator[None]:
    """Hold a run's manifest lock as another process would: through a file of its own."""
    path = StateDirectoryManager.run_state_dir(run_id=run_id) / MANIFEST_LOCK_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def test_manifest_lock_gives_up_on_a_writer_that_keeps_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a writer stalled while holding the lock makes the next one fail
    after its timeout, rather than wait as long as the stall lasts.
    """
    monkeypatch.setenv("CSTAR_STATE_HOME", str(tmp_path / "state"))

    with held_manifest_lock("my-run"), pytest.raises(LockTimeoutError):
        with manifest_lock("my-run", timeout=0.1):
            pytest.fail("the lock was taken while another writer held it")


def test_workplan_content_hash_is_stable(hello_world_bp_path: Path) -> None:
    plan = _workplan(_step(hello_world_bp_path.as_posix()))
    redumped = Workplan.model_validate(plan.model_dump(mode="json", by_alias=True))

    assert workplan_content_hash(plan) == workplan_content_hash(redumped)
    assert workplan_content_hash(plan) != workplan_content_hash(
        _workplan(_step(hello_world_bp_path.as_posix(), name="other"))
    )


class TestDescribeSteps:
    def _hash(self, tmp_path: Path, **kwargs: t.Any) -> str:
        bp = _write_blueprint(tmp_path / "bp.yaml")
        plan = _workplan(_step(bp.as_posix(), **kwargs))
        return describe_steps(plan, tmp_path)["one"].definition_hash

    def test_loaded_blueprint(self, tmp_path: Path) -> None:
        bp = _write_blueprint(tmp_path / "bp.yaml")

        facts = describe_steps(_workplan(_step(bp.as_posix())), tmp_path)["one"]

        assert facts.blueprint is not None
        assert facts.blueprint.name == "hello"
        assert facts.external == ExternalDependencies()

    def test_relative_path_anchored_to_base_dir(self, tmp_path: Path) -> None:
        _write_blueprint(tmp_path / "bp.yaml")

        facts = describe_steps(_workplan(_step("bp.yaml")), tmp_path)["one"]

        assert facts.blueprint is not None

    def test_blueprint_content_moves_the_hash(self, tmp_path: Path) -> None:
        bp = tmp_path / "bp.yaml"
        _write_blueprint(bp, "world")
        before = describe_steps(_workplan(_step(bp.as_posix())), tmp_path)
        _write_blueprint(bp, "moon")
        after = describe_steps(_workplan(_step(bp.as_posix())), tmp_path)

        assert before["one"].definition_hash != after["one"].definition_hash

    def test_blueprint_overrides_move_the_hash(self, tmp_path: Path) -> None:
        assert self._hash(tmp_path) != self._hash(
            tmp_path, blueprint_overrides={"target": "moon"}
        )

    def test_resources_and_clobber_do_not(self, tmp_path: Path) -> None:
        base = self._hash(tmp_path)

        assert base == self._hash(
            tmp_path, compute_overrides={"slurm": {"num_cpus": 4}}
        )
        assert base == self._hash(tmp_path, workflow_overrides={"clobber": True})

    def test_deferred_consumer_follows_its_producer(self, tmp_path: Path) -> None:
        def hashes(overrides: dict[str, t.Any]) -> dict[str, StepFacts]:
            bp = _write_blueprint(tmp_path / "bp.yaml")
            consumer = _step(
                {"from_step": "producer"}, name="consumer", depends_on=["producer"]
            )
            producer = _step(
                bp.as_posix(), name="producer", blueprint_overrides=overrides
            )
            # Consumer listed first: producers must still be described first.
            return describe_steps(_workplan(consumer, producer), tmp_path)

        before, after = hashes({}), hashes({"target": "moon"})

        assert before["consumer"].blueprint is None
        assert before["consumer"].definition_hash != after["consumer"].definition_hash
        assert before["producer"].definition_hash != after["producer"].definition_hash

    def test_deferred_with_unknown_producer(self, tmp_path: Path) -> None:
        step = _step({"from_step": "up@ext"}, depends_on=["up@ext"])
        plan = Workplan(
            name="plan",
            description="a plan",
            runs={"ext": RunRef(run_id="spin")},
            steps=[step],
        )

        facts = describe_steps(plan, tmp_path)["one"]

        assert facts.definition_hash
        assert facts.blueprint is None

    def test_inline_has_no_blueprint(self, tmp_path: Path) -> None:
        facts = describe_steps(
            _workplan(_step("inline", blueprint_overrides={"target": "x"})), tmp_path
        )["one"]

        assert facts.blueprint is None
        assert facts.external == ExternalDependencies()

    def test_placeholder_has_no_blueprint(self, tmp_path: Path) -> None:
        facts = describe_steps(_workplan(_step("{{where}}/bp.yaml")), tmp_path)["one"]

        assert facts.blueprint is None

    def test_missing_file_names_the_step(self, tmp_path: Path) -> None:
        plan = _workplan(_step("missing.yaml", name="spin-up"))

        with pytest.raises(CstarExpectationFailed, match="spin-up") as err:
            describe_steps(plan, tmp_path)

        assert "missing.yaml" in str(err.value)
        assert str(tmp_path.resolve()) in str(err.value)


class _DepsBlueprint(HelloWorldBlueprint):
    """A blueprint with nested mappings of the shapes real blueprints use."""

    codebase: dict[str, t.Any]
    data: list[dict[str, t.Any]]


class TestCollectDependencies:
    def _blueprint(self, **kwargs: t.Any) -> _DepsBlueprint:
        return _DepsBlueprint(
            name="deps",
            description="d",
            target="x",
            codebase={
                "roms": {"location": "https://git/roms.git", "commit": "c0ffee"},
                "marbl": {"location": "https://git/marbl.git", "branch": "main"},
            },
            data=[
                {"location": "/data/grid.nc", "hash": "h1"},
                {"location": "/data/grid.nc", "hash": "h1"},
                {"location": "/data/ic.nc", "content_hash": "h2"},
                {"dataset_key": "GLORYS", "dataset_id": "id-1", "location": "ignored"},
                {"dataset_key": "ERA5", "url": "https://era5", "x": 1},
                {"location": ""},
            ],
            **kwargs,
        )

    def test_walk(self) -> None:
        found = collect_dependencies(self._blueprint())

        assert found.codebases == [
            CodebaseRef(location="https://git/roms.git", commit="c0ffee"),
            CodebaseRef(location="https://git/marbl.git", branch="main"),
        ]
        assert found.source_data == [
            SourceDataRef(location="/data/grid.nc", hash="h1"),
            SourceDataRef(location="/data/ic.nc", hash="h2"),
            SourceDataRef(location="id-1", name="GLORYS"),
            SourceDataRef(location="https://era5", name="ERA5"),
        ]

    def test_provenance_is_split_not_walked(self) -> None:
        spec = CatalogSpecRef(kind="DomainSpec", name="na", origin="catalog")
        upstream = _identity("upstream")
        found = collect_dependencies(
            self._blueprint(provenance=Provenance(derived_from=[spec, upstream]))
        )

        assert found.catalog_entries == [spec]
        assert found.blueprints == [upstream]
        assert len(found.codebases) == 2

    def test_plain_blueprint_has_none(self) -> None:
        bp = HelloWorldBlueprint(name="hello", description="d", target="x")

        assert collect_dependencies(bp) == ExternalDependencies()


class TestEnsureRunManifest:
    def _ensure(
        self,
        root: Path,
        steps: dict[str, tuple[str, str, StepFacts | None]],
        **kwargs: t.Any,
    ) -> RunManifest:
        return ensure_run_manifest(
            root,
            run_id="run",
            name="run",
            kind="workplan",
            workplan=None,
            attempt=kwargs.pop("attempt", Attempt(start_at=NOW, system="anvil")),
            steps=steps,
            runs=kwargs.pop("runs", {}),
            carried_blueprints=kwargs.pop("carried_blueprints", None),
        )

    def test_mints_once_and_appends_attempts(self, tmp_path: Path) -> None:
        steps: dict[str, tuple[str, str, StepFacts | None]] = {
            "one": ("hello_world", "tasks/one", None)
        }

        first = self._ensure(tmp_path, steps)
        second = self._ensure(tmp_path, steps)

        assert second.id == first.id
        assert second.system == "anvil"
        assert len(second.attempts) == 2
        assert [s.name for s in second.steps] == ["one"]
        assert read_run_manifest(tmp_path) == second

    def test_upsert_keeps_status_and_record(self, tmp_path: Path) -> None:
        none: dict[str, tuple[str, str, StepFacts | None]] = {
            "one": ("hello_world", "tasks/one", None)
        }
        manifest = self._ensure(tmp_path, none)
        manifest.steps[0].status = "Done"
        manifest.steps[0].record = _record()
        write_run_manifest(tmp_path, manifest)
        facts = StepFacts(
            definition_hash="h",
            blueprint=_identity("known"),
            external=ExternalDependencies(),
        )

        updated = self._ensure(
            tmp_path,
            {
                "one": ("hello_world", "tasks/moved", facts),
                "two": ("hello_world", "tasks/two", None),
            },
        )

        one = updated.step("one")
        assert one is not None
        assert (one.status, one.record) == ("Done", _record())
        assert (one.definition_hash, one.working_dir) == ("h", "tasks/moved")
        assert one.blueprint == _identity("known")
        assert [s.name for s in updated.steps] == ["one", "two"]

        # Facts without a blueprint (deferred) keep what a record set.
        deferred = StepFacts("h2", None, ExternalDependencies())
        again = self._ensure(
            tmp_path, {"one": ("hello_world", "tasks/moved", deferred)}
        )
        assert again.step("one") is not None
        assert again.step("one").blueprint == _identity("known")  # type: ignore[union-attr]
        assert again.step("one").definition_hash == "h2"  # type: ignore[union-attr]

    def test_rerun_steps_forget_their_earlier_observation(self, tmp_path: Path) -> None:
        """A clobbered or resumed step certainly runs again: its status and record go."""
        steps: dict[str, tuple[str, str, StepFacts | None]] = {
            n: ("hello_world", f"tasks/{n}", None) for n in ("one", "two", "three")
        }
        manifest = self._ensure(tmp_path, steps)
        for entry in manifest.steps:
            entry.status = "Done"
            entry.record = _record(entry.name)
        write_run_manifest(tmp_path, manifest)

        updated = self._ensure(
            tmp_path,
            steps,
            attempt=Attempt(start_at=NOW, clobbered=["one"], resumed=["two"]),
        )

        assert [(s.status, s.record is None) for s in updated.steps] == [
            ("", True),
            ("", True),
            ("Done", False),
        ]

    def test_rerun_steps_forget_their_submission(self, tmp_path: Path) -> None:
        """A clobbered or resumed step is submitted again: the earlier job goes, and
        a step the attempt leaves alone keeps its own.
        """
        steps: dict[str, tuple[str, str, StepFacts | None]] = {
            n: ("hello_world", f"tasks/{n}", None) for n in ("one", "two", "three")
        }
        manifest = self._ensure(tmp_path, steps)
        for entry in manifest.steps:
            entry.launcher = "slurm"
            entry.job_id = f"job-{entry.name}"
            entry.submitted_at = NOW
        write_run_manifest(tmp_path, manifest)

        updated = self._ensure(
            tmp_path,
            steps,
            attempt=Attempt(start_at=NOW, clobbered=["one"], resumed=["two"]),
        )

        assert [(s.launcher, s.job_id, s.submitted_at) for s in updated.steps] == [
            ("", "", None),
            ("", "", None),
            ("slurm", "job-three", NOW),
        ]

    def test_upsert_keeps_the_submission(self, tmp_path: Path) -> None:
        steps: dict[str, tuple[str, str, StepFacts | None]] = {
            "one": (
                "hello_world",
                "tasks/one",
                StepFacts("h", None, ExternalDependencies()),
            )
        }
        manifest = self._ensure(tmp_path, steps)
        manifest.steps[0].launcher = "local"
        manifest.steps[0].job_id = "99"
        manifest.steps[0].submitted_at = NOW
        write_run_manifest(tmp_path, manifest)

        updated = self._ensure(tmp_path, steps)

        entry = updated.steps[0]
        assert (entry.launcher, entry.job_id, entry.submitted_at) == (
            "local",
            "99",
            NOW,
        )

    def test_blueprint_files_repoint_every_listed_step(self, tmp_path: Path) -> None:
        """Each preparing attempt points the steps at its own copies, and a step
        it carried nothing for is cleared.
        """
        steps: dict[str, tuple[str, str, StepFacts | None]] = {
            n: ("hello_world", f"tasks/{n}", None) for n in ("one", "two")
        }
        first = self._ensure(
            tmp_path,
            steps,
            carried_blueprints={"one": "attempts/a/blueprints/one/b.yaml", "two": "x"},
        )
        assert [s.carried_blueprint for s in first.steps] == [
            "attempts/a/blueprints/one/b.yaml",
            "x",
        ]

        second = self._ensure(
            tmp_path,
            steps,
            carried_blueprints={"one": "attempts/b/blueprints/one/b.yaml"},
        )

        assert [s.carried_blueprint for s in second.steps] == [
            "attempts/b/blueprints/one/b.yaml",
            "",
        ]

    def test_blueprint_files_never_name_a_step_outside_the_attempt(
        self, tmp_path: Path
    ) -> None:
        """A pre-run prunes steps; a pointer for a step not scheduled is ignored."""
        steps: dict[str, tuple[str, str, StepFacts | None]] = {
            "one": ("hello_world", "tasks/one", None)
        }

        manifest = self._ensure(
            tmp_path, steps, carried_blueprints={"one": "p", "pruned": "q"}
        )

        assert [(s.name, s.carried_blueprint) for s in manifest.steps] == [("one", "p")]

    def test_no_blueprint_files_leave_the_pointers_as_recorded(
        self, tmp_path: Path
    ) -> None:
        """A re-entry replaying the recorded plan carries nothing."""
        steps: dict[str, tuple[str, str, StepFacts | None]] = {
            "one": ("hello_world", "tasks/one", None)
        }
        self._ensure(tmp_path, steps, carried_blueprints={"one": "attempts/a/b.yaml"})

        reentered = self._ensure(tmp_path, steps)

        assert reentered.steps[0].carried_blueprint == "attempts/a/b.yaml"
        assert len(reentered.attempts) == 2

    def test_a_pointer_survives_facts_and_a_clobber(self, tmp_path: Path) -> None:
        steps: dict[str, tuple[str, str, StepFacts | None]] = {
            "one": (
                "hello_world",
                "tasks/one",
                StepFacts("h", None, ExternalDependencies()),
            )
        }
        self._ensure(tmp_path, steps, carried_blueprints={"one": "attempts/a/b.yaml"})

        updated = self._ensure(
            tmp_path, steps, attempt=Attempt(start_at=NOW, clobbered=["one"])
        )

        assert updated.steps[0].carried_blueprint == "attempts/a/b.yaml"

    def test_merges_external_without_duplicates(self, tmp_path: Path) -> None:
        facts = StepFacts(
            "h",
            None,
            ExternalDependencies(source_data=[SourceDataRef(location="/a")]),
        )
        steps: dict[str, tuple[str, str, StepFacts | None]] = {
            "one": ("hello_world", "tasks/one", facts)
        }
        runs = {"up": RunRef(run_id="spin", start_at=NOW)}

        self._ensure(tmp_path, steps, runs=runs)
        merged = self._ensure(tmp_path, steps, runs=runs)

        assert merged.external.source_data == [SourceDataRef(location="/a")]
        assert merged.external.runs == [
            ExternalRun(alias="up", run_id="spin", start_at=NOW)
        ]

    def test_merged_preserves_order(self) -> None:
        a, b, c = (SourceDataRef(location=x) for x in "abc")

        union = ExternalDependencies(source_data=[a, b]).merged(
            ExternalDependencies(source_data=[b, c])
        )

        assert union.source_data == [a, b, c]


def test_new_attempt(hello_world_bp_path: Path) -> None:
    plan = _workplan(
        _step(hello_world_bp_path.as_posix(), workflow_overrides={"clobber": True}),
        _step(
            hello_world_bp_path.as_posix(),
            name="two",
            workflow_overrides={"resume": True},
        ),
    )

    attempt = new_attempt(plan, workplan_hash="wh")

    assert attempt.clobbered == ["one"]
    assert attempt.resumed == ["two"]
    assert attempt.workplan_content_hash == "wh"
    assert attempt.status == AttemptStatus.scheduled
    assert attempt.finished_at is None
    assert attempt.pre_run is False
    assert (attempt.directory, attempt.carried) == ("", {})


def test_new_attempt_with_carried_inputs(hello_world_bp_path: Path) -> None:
    plan = _workplan(_step(hello_world_bp_path.as_posix()))
    carried = CarriedInputs(
        start_at=NOW - timedelta(days=1),
        directory="attempts/20261014T120000",
        carried={"workplan.yaml": "sha256:ab"},
        carried_blueprints={"one": "attempts/20261014T120000/blueprints/one/b.yaml"},
    )

    attempt = new_attempt(plan, workplan_hash="wh", carried=carried)

    assert attempt.start_at == NOW - timedelta(days=1)
    assert attempt.directory == "attempts/20261014T120000"
    assert attempt.carried == {"workplan.yaml": "sha256:ab"}
    assert attempt.carried is not carried.carried


class TestCreateAttemptDir:
    def test_named_after_the_utc_start(self, tmp_path: Path) -> None:
        created = create_attempt_dir(tmp_path, NOW)

        assert created == tmp_path / ATTEMPTS_DIR_NAME / "20261015T120000"
        assert created.is_dir()
        assert NOW.strftime(ATTEMPT_DIR_FORMAT) == created.name

    def test_a_zoned_start_is_named_in_utc(self, tmp_path: Path) -> None:
        zoned = datetime(2026, 10, 15, 23, 30, 5, tzinfo=timezone(timedelta(hours=-5)))

        created = create_attempt_dir(tmp_path, zoned)

        assert created.name == "20261016T043005"

    def test_the_same_second_takes_a_numeric_suffix(self, tmp_path: Path) -> None:
        names = [create_attempt_dir(tmp_path, NOW).name for _ in range(3)]

        assert names == ["20261015T120000", "20261015T120000-2", "20261015T120000-3"]

    def test_a_later_second_is_a_new_name(self, tmp_path: Path) -> None:
        create_attempt_dir(tmp_path, NOW)

        later = create_attempt_dir(tmp_path, NOW + timedelta(seconds=1))

        assert later.name == "20261015T120001"

    def test_a_deleted_directory_frees_its_name(self, tmp_path: Path) -> None:
        first = create_attempt_dir(tmp_path, NOW)
        first.rmdir()

        assert create_attempt_dir(tmp_path, NOW) == first


def test_carried_digest_is_the_sha256_of_the_bytes(tmp_path: Path) -> None:
    path = tmp_path / "workplan.yaml"
    path.write_bytes(b"name: plan  # a comment\n")

    assert carried_digest(path) == (
        "sha256:" + hashlib.sha256(b"name: plan  # a comment\n").hexdigest()
    )


class TestLatestAttemptDir:
    def _with(self, *directories: str) -> RunManifest:
        manifest = _manifest("one")
        manifest.attempts = [
            Attempt(start_at=NOW, directory=directory) for directory in directories
        ]
        return manifest

    def test_the_last_attempt_with_a_directory(self, tmp_path: Path) -> None:
        manifest = self._with("attempts/a", "attempts/b")

        assert latest_attempt_dir(manifest, tmp_path) == tmp_path / "attempts/b"

    def test_a_re_entry_does_not_hide_the_preparing_attempt(
        self, tmp_path: Path
    ) -> None:
        manifest = self._with("attempts/a", "", "")

        assert latest_attempt_dir(manifest, tmp_path) == tmp_path / "attempts/a"

    def test_none_when_no_attempt_carried_inputs(self, tmp_path: Path) -> None:
        assert latest_attempt_dir(self._with("", ""), tmp_path) is None
        assert latest_attempt_dir(self._with(), tmp_path) is None

    def test_existence_is_not_checked(self, tmp_path: Path) -> None:
        directory = latest_attempt_dir(self._with("attempts/gone"), tmp_path)

        assert directory is not None
        assert not directory.exists()


class TestRecordSubmission:
    SUBMITTED = NOW + timedelta(seconds=5, milliseconds=250)

    def _submit(self, root: Path, step: str = "one", **kwargs: t.Any) -> bool:
        arguments: dict[str, t.Any] = {
            "launcher": "slurm",
            "job_id": "123",
            "submitted_at": self.SUBMITTED,
        } | kwargs
        return record_submission(root, "run", step, **arguments)

    def test_writes_the_submission_of_the_step(self, tmp_path: Path) -> None:
        write_run_manifest(tmp_path, _manifest("one", "two"))

        assert self._submit(tmp_path)

        manifest = read_run_manifest(tmp_path)
        assert manifest is not None
        one, two = manifest.step("one"), manifest.step("two")
        assert one is not None
        assert two is not None
        assert (one.launcher, one.job_id) == ("slurm", "123")
        assert one.submitted_at == NOW + timedelta(seconds=5)
        assert (two.launcher, two.job_id, two.submitted_at) == ("", "", None)

    def test_holds_the_lock_while_it_reads_and_writes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_run_manifest(tmp_path, _manifest("one"))
        events: list[str] = []
        read = manifest_module.read_run_manifest
        write = manifest_module.write_run_manifest

        @contextmanager
        def lock(run_id: str, **_: t.Any) -> t.Iterator[None]:
            events.append(f"lock {run_id}")
            yield
            events.append("unlock")

        def read_recorded(root: Path) -> RunManifest | None:
            events.append("read")
            return read(root)

        def write_recorded(root: Path, manifest: RunManifest) -> Path:
            events.append("write")
            return write(root, manifest)

        monkeypatch.setattr(manifest_module, "manifest_lock", lock)
        monkeypatch.setattr(manifest_module, "read_run_manifest", read_recorded)
        monkeypatch.setattr(manifest_module, "write_run_manifest", write_recorded)

        assert self._submit(tmp_path)

        assert events == ["lock run", "read", "write", "unlock"]

    def test_submissions_of_one_run_are_serialized(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a second submission cannot read the manifest while the first holds
        it: the orchestrator records the steps of one pass on separate threads, and
        a read-modify-write that overlapped would lose one of them.
        """
        write_run_manifest(tmp_path, _manifest("one", "two"))
        read = manifest_module.read_run_manifest
        reads: list[str] = []
        first_read, release = threading.Event(), threading.Event()

        def read_and_hold(root: Path) -> RunManifest | None:
            manifest = read(root)
            reads.append(threading.current_thread().name)
            first_read.set()
            assert release.wait(timeout=5)
            return manifest

        monkeypatch.setattr(manifest_module, "read_run_manifest", read_and_hold)
        threads = [
            threading.Thread(
                target=self._submit,
                args=(tmp_path, step),
                kwargs={"job_id": step},
                name=step,
            )
            for step in ("one", "two")
        ]

        threads[0].start()
        assert first_read.wait(timeout=5)
        threads[1].start()
        time.sleep(0.2)  # an unserialized second submission would have read by now
        assert reads == ["one"]
        release.set()
        for thread in threads:
            thread.join(timeout=5)

        manifest = read(tmp_path)
        assert manifest is not None
        assert [(s.name, s.job_id) for s in manifest.steps] == [
            ("one", "one"),
            ("two", "two"),
        ]

    def test_a_stalled_writer_makes_the_submission_give_up(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a submission gives up once the deadline passes while another writer
        keeps the manifest lock, and leaves the manifest as it was.
        """
        write_run_manifest(tmp_path, _manifest("one"))
        before = (tmp_path / RUN_MANIFEST_NAME).read_bytes()
        monkeypatch.setattr(manifest_module, "MANIFEST_LOCK_TIMEOUT", 0.2)

        with held_manifest_lock("run"), pytest.raises(LockTimeoutError):
            self._submit(tmp_path)

        assert (tmp_path / RUN_MANIFEST_NAME).read_bytes() == before

    def test_submissions_launched_together_share_one_deadline(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify submissions recorded at once behind a stalled writer all give up
        at the one deadline, not one deadline after another.
        """
        write_run_manifest(tmp_path, _manifest("one", "two", "three"))
        timeout = 1.0
        monkeypatch.setattr(manifest_module, "MANIFEST_LOCK_TIMEOUT", timeout)
        failures: list[BaseException] = []

        def submit(step: str) -> None:
            try:
                self._submit(tmp_path, step)
            except LockTimeoutError as ex:
                failures.append(ex)

        threads = [
            threading.Thread(target=submit, args=(s,)) for s in ("one", "two", "three")
        ]
        with held_manifest_lock("run"):
            start = time.monotonic()
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
            elapsed = time.monotonic() - start

        assert len(failures) == 3
        assert elapsed < 2 * timeout

    def test_submissions_launched_together_do_not_poll_each_other(
        self, tmp_path: Path
    ) -> None:
        """Verify submissions recorded at once queue for each other instead of
        polling the manifest lock, so all of them land without delay.
        """
        steps = [f"step-{n}" for n in range(12)]
        write_run_manifest(tmp_path, _manifest(*steps))
        threads = [
            threading.Thread(
                target=self._submit, args=(tmp_path, s), kwargs={"job_id": s}
            )
            for s in steps
        ]

        start = time.monotonic()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        elapsed = time.monotonic() - start

        manifest = read_run_manifest(tmp_path)
        assert manifest is not None
        assert [entry.job_id for entry in manifest.steps] == steps
        assert elapsed < LOCK_POLL_INTERVAL

    def test_a_later_submission_replaces_the_earlier(self, tmp_path: Path) -> None:
        write_run_manifest(tmp_path, _manifest("one"))
        self._submit(tmp_path)

        later = NOW + timedelta(hours=1)
        assert self._submit(tmp_path, launcher="local", job_id="55", submitted_at=later)

        manifest = read_run_manifest(tmp_path)
        assert manifest is not None
        entry = manifest.steps[0]
        assert (entry.launcher, entry.job_id, entry.submitted_at) == (
            "local",
            "55",
            later,
        )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"submitted_at": SUBMITTED + timedelta(milliseconds=500)},  # same second
        ],
        ids=["identical", "same-whole-second"],
    )
    def test_an_unchanged_submission_is_not_rewritten(
        self, tmp_path: Path, kwargs: dict[str, t.Any]
    ) -> None:
        """An adopted sentinel carries the submission recorded when it was made."""
        write_run_manifest(tmp_path, _manifest("one"))
        assert self._submit(tmp_path)
        path = tmp_path / RUN_MANIFEST_NAME
        before = (path.read_bytes(), path.stat().st_mtime_ns)

        assert not self._submit(tmp_path, **kwargs)

        assert (path.read_bytes(), path.stat().st_mtime_ns) == before

    def test_no_manifest_writes_nothing(self, tmp_path: Path) -> None:
        assert not self._submit(tmp_path)

        assert not (tmp_path / RUN_MANIFEST_NAME).exists()

    def test_an_unknown_step_writes_nothing(self, tmp_path: Path) -> None:
        write_run_manifest(tmp_path, _manifest("one"))
        path = tmp_path / RUN_MANIFEST_NAME
        before = (path.read_bytes(), path.stat().st_mtime_ns)

        assert not self._submit(tmp_path, step="elsewhere")

        assert (path.read_bytes(), path.stat().st_mtime_ns) == before

    def test_a_manifest_that_cannot_be_read_propagates(self, tmp_path: Path) -> None:
        (tmp_path / RUN_MANIFEST_NAME).write_text("- not\n- a mapping\n")

        with pytest.raises(CstarError, match=str(tmp_path / RUN_MANIFEST_NAME)):
            self._submit(tmp_path)

    def test_a_manifest_from_a_newer_build_propagates(self, tmp_path: Path) -> None:
        write_run_manifest(tmp_path, _manifest("one"))
        path = tmp_path / RUN_MANIFEST_NAME
        path.write_text(path.read_text().replace(CURRENT, NEWER_MAJOR, 1))

        with pytest.raises(CstarSchemaTooNewError):
            self._submit(tmp_path)


class TestFoldStepRecords:
    def test_open_while_a_step_is_not_terminal(self, tmp_path: Path) -> None:
        manifest = _manifest("one", "two")

        changed = fold_step_records(
            manifest, tmp_path, {"one": "Done", "two": "Running"}
        )

        assert changed
        assert manifest.attempts[-1].finished_at is None
        assert manifest.attempts[-1].status == AttemptStatus.scheduled
        assert manifest.observed_at is not None

    @pytest.mark.parametrize(
        ("statuses", "expected"),
        [
            (["Done", "Done"], AttemptStatus.done),
            (["Done", "Cancelled"], AttemptStatus.cancelled),
            (["Failed", "Cancelled"], AttemptStatus.failed),
            (["Done", "Failed"], AttemptStatus.failed),
        ],
    )
    def test_closes_with_precedence(
        self, tmp_path: Path, statuses: list[str], expected: AttemptStatus
    ) -> None:
        manifest = _manifest("one", "two")

        fold_step_records(manifest, tmp_path, dict(zip(["one", "two"], statuses)))

        assert manifest.attempts[-1].status == expected
        assert manifest.attempts[-1].finished_at is not None

    def test_finish_time_is_the_latest_record(self, tmp_path: Path) -> None:
        manifest = _manifest("one", "two")
        later = NOW + timedelta(hours=2)
        write_step_record(tmp_path / "tasks/one", _record("one", NOW))
        write_step_record(tmp_path / "tasks/two", _record("two", later))

        fold_step_records(manifest, tmp_path, {"one": "Done", "two": "Done"})

        assert manifest.attempts[-1].finished_at == later
        assert manifest.step("one").record == _record("one", NOW)  # type: ignore[union-attr]

    def test_deferred_entry_takes_the_recorded_blueprint(self, tmp_path: Path) -> None:
        manifest = _manifest("one")
        write_step_record(tmp_path / "tasks/one", _record("one"))
        assert manifest.steps[0].blueprint is None

        fold_step_records(manifest, tmp_path)

        assert manifest.steps[0].blueprint == _identity()

    def test_a_known_blueprint_is_kept(self, tmp_path: Path) -> None:
        manifest = _manifest("one")
        manifest.steps[0].blueprint = _identity("authored")
        write_step_record(tmp_path / "tasks/one", _record("one"))

        fold_step_records(manifest, tmp_path)

        assert manifest.steps[0].blueprint == _identity("authored")

    def test_corrupt_record_leaves_the_entry(self, tmp_path: Path) -> None:
        manifest = _manifest("one")
        (tmp_path / "tasks/one").mkdir(parents=True)
        (tmp_path / "tasks/one" / STEP_RECORD_NAME).write_text("- nope\n")

        fold_step_records(manifest, tmp_path, {"one": "Done"})

        assert manifest.steps[0].record is None
        assert manifest.steps[0].status == "Done"

    def test_finished_at_is_kept_once_set(self, tmp_path: Path) -> None:
        manifest = _manifest("one")
        fold_step_records(manifest, tmp_path, {"one": "Failed"})
        closed = manifest.attempts[-1].model_copy()

        fold_step_records(manifest, tmp_path, {"one": "Done"})

        assert manifest.attempts[-1] == closed

    def test_record_disagreeing_with_the_status_is_dropped(
        self, tmp_path: Path
    ) -> None:
        """A `completed` record beside a step observed `Failed` is an earlier run's."""
        manifest = _manifest("one")
        write_step_record(tmp_path / "tasks/one", _record("one"))

        fold_step_records(manifest, tmp_path, {"one": "Failed"})

        assert manifest.step("one").record is None  # type: ignore[union-attr]
        assert manifest.attempts[-1].status == AttemptStatus.failed

    def test_record_of_a_step_running_again_is_dropped(self, tmp_path: Path) -> None:
        manifest = _manifest("one")
        manifest.steps[0].record = _record("one")
        write_step_record(tmp_path / "tasks/one", _record("one"))

        fold_step_records(manifest, tmp_path, {"one": "Running"})

        assert manifest.step("one").record is None  # type: ignore[union-attr]
        assert manifest.attempts[-1].finished_at is None

    def test_record_is_folded_as_found_without_statuses(self, tmp_path: Path) -> None:
        manifest = _manifest("one")
        write_step_record(tmp_path / "tasks/one", _record("one"))

        fold_step_records(manifest, tmp_path)

        assert manifest.step("one").record == _record("one")  # type: ignore[union-attr]

    def test_finish_time_never_precedes_the_attempt(self, tmp_path: Path) -> None:
        """A re-entry that only reused Done steps closes at its own start."""
        manifest = _manifest("one")
        later = NOW + timedelta(hours=3)
        manifest.attempts = [Attempt(start_at=later)]
        write_step_record(tmp_path / "tasks/one", _record("one", NOW))

        fold_step_records(manifest, tmp_path, {"one": "Done"})

        assert manifest.attempts[-1].finished_at == later

    def test_the_submission_is_not_touched(self, tmp_path: Path) -> None:
        """Folding observes statuses and records: what was submitted stays."""
        manifest = _manifest("one")
        entry = manifest.steps[0]
        entry.launcher, entry.job_id, entry.submitted_at = "slurm", "7", NOW
        write_step_record(tmp_path / "tasks/one", _record("one"))

        fold_step_records(manifest, tmp_path, {"one": "Done"})

        assert (entry.launcher, entry.job_id, entry.submitted_at) == ("slurm", "7", NOW)

    def test_unchanged_returns_false(self, tmp_path: Path) -> None:
        manifest = _manifest("one")
        statuses = {"one": "Done"}
        assert fold_step_records(manifest, tmp_path, statuses)

        assert not fold_step_records(manifest, tmp_path, statuses)


class TestFinalizeStandaloneRun:
    @pytest.fixture
    def blueprint(self, hello_world_bp_path: Path) -> HelloWorldBlueprint:
        return load_blueprint(hello_world_bp_path, HelloWorldBlueprint)

    @pytest.mark.parametrize(
        ("outcome", "status", "attempt"),
        [
            ("completed", "Done", AttemptStatus.done),
            ("cancelled", "Cancelled", AttemptStatus.cancelled),
            ("failed", "Failed", AttemptStatus.failed),
        ],
    )
    def test_outcome_decides_the_attempt(
        self,
        blueprint: HelloWorldBlueprint,
        outcome: str,
        status: str,
        attempt: AttemptStatus,
    ) -> None:
        root = blueprint.effective_working_dir
        write_step_record(
            root, _record(blueprint.name).model_copy(update={"outcome": outcome})
        )

        manifest = finalize_standalone_run(blueprint, started_at=NOW)

        assert manifest.steps[0].status == status
        assert manifest.attempts[0].status == attempt
        assert manifest.attempts[0].start_at == NOW
        assert read_run_manifest(root) == manifest

    def test_no_record_counts_as_failed(self, blueprint: HelloWorldBlueprint) -> None:
        manifest = finalize_standalone_run(blueprint, started_at=NOW)

        assert manifest.steps[0].status == "Failed"
        assert manifest.steps[0].record is None
        assert manifest.attempts[0].status == AttemptStatus.failed

    def test_a_record_older_than_the_run_counts_as_failed(
        self, blueprint: HelloWorldBlueprint
    ) -> None:
        """A re-run that died before shutdown leaves the earlier run's record behind."""
        root = blueprint.effective_working_dir
        write_step_record(root, _record(blueprint.name, NOW))

        manifest = finalize_standalone_run(
            blueprint, started_at=NOW + timedelta(hours=1)
        )

        assert manifest.attempts[-1].status == AttemptStatus.failed
        assert manifest.steps[0].record is None

    def test_second_run_adds_an_attempt(self, blueprint: HelloWorldBlueprint) -> None:
        root = blueprint.effective_working_dir
        write_step_record(
            root, _record(blueprint.name).model_copy(update={"outcome": "failed"})
        )
        first = finalize_standalone_run(blueprint, started_at=NOW)
        write_step_record(root, _record(blueprint.name, NOW + timedelta(hours=2)))

        second = finalize_standalone_run(blueprint, started_at=NOW + timedelta(hours=1))

        assert second.run_id == first.run_id == slugify(blueprint.name)
        assert second.kind == "blueprint"
        assert [a.status for a in second.attempts] == [
            AttemptStatus.failed,
            AttemptStatus.done,
        ]
        assert len(second.steps) == 1
        assert second.steps[0].working_dir == "."
        assert second.steps[0].definition_hash == blueprint.content_hash()


class TestExpandArtifacts:
    def test_first_spec_wins_on_overlap(self, tmp_path: Path) -> None:
        """A file matched by several patterns is claimed by the earliest."""
        (tmp_path / "output").mkdir()
        (tmp_path / "output" / "output_rst.1.nc").write_bytes(b"r")
        (tmp_path / "output" / "output_his.1.nc").write_bytes(b"h")

        products = expand_artifacts(
            tmp_path,
            [
                ArtifactSpec(
                    kind=ArtifactKind.restart, pattern="output/output_rst.*.nc"
                ),
                ArtifactSpec(kind=ArtifactKind.output, pattern="output/*.nc"),
            ],
        )

        assert [(p.path, p.kind) for p in products] == [
            ("output/output_his.1.nc", "output"),
            ("output/output_rst.1.nc", "restart"),
        ]

    def test_directories_are_not_products(self, tmp_path: Path) -> None:
        (tmp_path / "output" / "sub").mkdir(parents=True)
        (tmp_path / "output" / "a.nc").write_bytes(b"a")

        products = expand_artifacts(
            tmp_path, [ArtifactSpec(kind=ArtifactKind.output, pattern="output/*")]
        )

        assert [p.path for p in products] == ["output/a.nc"]

    def test_sorted_by_path_with_file_sizes(self, tmp_path: Path) -> None:
        (tmp_path / "b.log").write_bytes(b"12345")
        (tmp_path / "a.log").write_bytes(b"123")

        products = expand_artifacts(
            tmp_path, [ArtifactSpec(kind=ArtifactKind.log, pattern="*.log")]
        )

        assert [(p.path, p.size_bytes) for p in products] == [
            ("a.log", 3),
            ("b.log", 5),
        ]

    def test_sorted_across_specs(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_bytes(b"")
        (tmp_path / "z.nc").write_bytes(b"")

        products = expand_artifacts(
            tmp_path,
            [
                ArtifactSpec(kind=ArtifactKind.output, pattern="*.nc"),
                ArtifactSpec(kind=ArtifactKind.log, pattern="*.log"),
            ],
        )

        assert [p.path for p in products] == ["a.log", "z.nc"]

    def test_each_product_owns_its_metadata(self, tmp_path: Path) -> None:
        (tmp_path / "a.nc").write_bytes(b"")
        (tmp_path / "b.nc").write_bytes(b"")
        spec = ArtifactSpec(
            kind=ArtifactKind.output, pattern="*.nc", metadata={"stream": "his"}
        )

        first, second = expand_artifacts(tmp_path, [spec])
        first.metadata["stream"] = "avg"

        assert second.metadata == {"stream": "his"}
        assert spec.metadata == {"stream": "his"}

    def test_missing_directory_yields_nothing(self, tmp_path: Path) -> None:
        products = expand_artifacts(
            tmp_path, [ArtifactSpec(kind=ArtifactKind.output, pattern="nope/*.nc")]
        )

        assert products == []

    def test_recursive_glob(self, tmp_path: Path) -> None:
        (tmp_path / "a" / "b").mkdir(parents=True)
        (tmp_path / "top.nc").write_bytes(b"")
        (tmp_path / "a" / "b" / "deep.nc").write_bytes(b"")

        products = expand_artifacts(
            tmp_path, [ArtifactSpec(kind=ArtifactKind.output, pattern="**/*.nc")]
        )

        assert [p.path for p in products] == ["a/b/deep.nc", "top.nc"]

    def test_no_specs(self, tmp_path: Path) -> None:
        assert expand_artifacts(tmp_path, []) == []


class TestMergeProducts:
    def test_reported_metadata_wins_per_key(self) -> None:
        declared = Product(
            kind="output",
            path="o.nc",
            size_bytes=7,
            metadata={"stream": "his", "keep": 1},
        )
        reported = Product(
            kind="other", path="o.nc", metadata={"stream": "avg", "new": "x"}
        )

        (merged,) = merge_products([declared], [reported])

        assert merged.metadata == {"stream": "avg", "keep": 1, "new": "x"}

    def test_declarations_own_kind_and_size(self) -> None:
        declared = Product(kind="restart", path="r.nc", size_bytes=7)
        reported = Product(kind="other", path="r.nc", size_bytes=99)

        (merged,) = merge_products([declared], [reported])

        assert (merged.kind, merged.size_bytes) == ("restart", 7)

    def test_undeclared_reported_product_follows_declared(self) -> None:
        declared = [
            Product(kind="log", path="z.log"),
            Product(kind="log", path="y.log"),
        ]
        extra = Product(kind="build", path="a.exe", size_bytes=3, metadata={"k": "v"})

        merged = merge_products(declared, [extra])

        assert [p.path for p in merged] == ["z.log", "y.log", "a.exe"]
        assert merged[-1] == extra

    def test_empty_inputs(self) -> None:
        product = Product(kind="log", path="a.log")

        assert merge_products([], []) == []
        assert merge_products([product], []) == [product]
        assert merge_products([], [product]) == [product]

    def test_inputs_are_not_mutated(self) -> None:
        declared = Product(kind="log", path="a.log", metadata={"a": 1})
        reported = Product(kind="log", path="a.log", metadata={"b": 2})

        merge_products([declared], [reported])

        assert declared.metadata == {"a": 1}
        assert reported.metadata == {"b": 2}


def _record_with(root: Path, *products: Product) -> None:
    """Write a record of `products` into `root`, creating the files they name."""
    for product in products:
        target = root / product.path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x")
    write_step_record(root, _record(products=list(products)))


class TestProductPaths:
    def test_filters_by_kind_and_orders_by_path(self, tmp_path: Path) -> None:
        _record_with(
            tmp_path,
            Product(kind=ArtifactKind.restart, path="output/output_rst.2.nc"),
            Product(kind=ArtifactKind.history, path="output/output_his.1.nc"),
            Product(kind=ArtifactKind.restart, path="output/output_rst.1.nc"),
        )

        assert product_paths(tmp_path, ArtifactKind.restart) == [
            tmp_path / "output" / "output_rst.1.nc",
            tmp_path / "output" / "output_rst.2.nc",
        ]

    @pytest.mark.parametrize(
        ("role", "expected"),
        [
            pytest.param("child", ["output/child_bry.1.nc"], id="child role"),
            pytest.param("", ["output/other_bry.1.nc"], id="no role"),
            pytest.param("parent", [], id="role nothing is recorded under"),
        ],
    )
    def test_filters_by_role(
        self, tmp_path: Path, role: str, expected: list[str]
    ) -> None:
        _record_with(
            tmp_path,
            Product(
                kind=ArtifactKind.boundary_forcing,
                path="output/child_bry.1.nc",
                metadata={"role": "child"},
            ),
            Product(kind=ArtifactKind.boundary_forcing, path="output/other_bry.1.nc"),
        )

        found = product_paths(tmp_path, ArtifactKind.boundary_forcing, role=role)

        assert found == [tmp_path / path for path in expected]

    def test_no_record_lists_nothing(self, tmp_path: Path) -> None:
        assert product_paths(tmp_path, ArtifactKind.restart) == []

    def test_record_without_products_lists_nothing(self, tmp_path: Path) -> None:
        """A record written by 0.17.0 lists no products."""
        write_step_record(tmp_path, _record())

        assert product_paths(tmp_path, ArtifactKind.restart) == []

    def test_unlisted_kind_lists_nothing(self, tmp_path: Path) -> None:
        _record_with(
            tmp_path, Product(kind=ArtifactKind.history, path="output/output_his.1.nc")
        )

        assert product_paths(tmp_path, ArtifactKind.restart) == []

    def test_missing_products_are_still_listed(self, tmp_path: Path) -> None:
        _record_with(
            tmp_path,
            Product(kind=ArtifactKind.restart, path="output/output_rst.1.nc"),
            Product(kind=ArtifactKind.restart, path="output/output_rst.2.nc"),
        )
        (tmp_path / "output" / "output_rst.1.nc").unlink()

        assert product_paths(tmp_path, ArtifactKind.restart) == [
            tmp_path / "output" / "output_rst.1.nc",
            tmp_path / "output" / "output_rst.2.nc",
        ]

    def test_corrupt_record_is_refused(self, tmp_path: Path) -> None:
        (tmp_path / STEP_RECORD_NAME).write_text("{{{")

        with pytest.raises(CstarError, match=STEP_RECORD_NAME):
            product_paths(tmp_path, ArtifactKind.restart)


def _restart(path: Path) -> Path:
    """Accept a restart file name, the `parse` of the `recorded_files_in` tests."""
    if "_rst." not in path.name:
        raise ValueError(path.name)
    return path


class TestRecordedFilesIn:
    @pytest.fixture
    def output(self, tmp_path: Path) -> Path:
        return tmp_path / "output"

    def test_lists_the_parsed_files_of_the_directory(self, output: Path) -> None:
        _record_with(
            output.parent,
            Product(kind=ArtifactKind.restart, path="output/output_rst.2.nc"),
            Product(kind=ArtifactKind.history, path="output/output_his.1.nc"),
            Product(kind=ArtifactKind.restart, path="output/output_rst.1.nc"),
        )

        found = recorded_files_in(output, ArtifactKind.restart, lambda p: p.name)

        assert found == ["output_rst.1.nc", "output_rst.2.nc"]

    def test_leaves_out_files_recorded_elsewhere_in_the_step(
        self, output: Path
    ) -> None:
        _record_with(
            output.parent,
            Product(kind=ArtifactKind.restart, path="output/output_rst.1.nc"),
            Product(kind=ArtifactKind.restart, path="output/sub/output_rst.2.nc"),
            Product(kind=ArtifactKind.restart, path="input/output_rst.3.nc"),
            Product(kind=ArtifactKind.restart, path="output_rst.4.nc"),
        )

        found = recorded_files_in(output, ArtifactKind.restart, _restart)

        assert found == [output / "output_rst.1.nc"]

    def test_filters_by_role(self, output: Path) -> None:
        _record_with(
            output.parent,
            Product(
                kind=ArtifactKind.boundary_forcing,
                path="output/child_bry.1.nc",
                metadata={"role": "child"},
            ),
            Product(kind=ArtifactKind.boundary_forcing, path="output/other_bry.1.nc"),
        )

        found = recorded_files_in(
            output, ArtifactKind.boundary_forcing, Path, role="child"
        )

        assert found == [output / "child_bry.1.nc"]

    def test_skips_rejected_names_in_one_message(
        self, output: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cstar.orchestration.manifest")
        _record_with(
            output.parent,
            Product(kind=ArtifactKind.restart, path="output/output_rst.1.nc"),
            Product(kind=ArtifactKind.restart, path="output/notes_a.nc"),
            Product(kind=ArtifactKind.restart, path="output/notes_b.nc"),
        )

        found = recorded_files_in(output, ArtifactKind.restart, _restart)

        assert found == [output / "output_rst.1.nc"]
        skipped = [
            r.getMessage() for r in caplog.records if "skipping" in r.getMessage()
        ]
        assert len(skipped) == 1
        assert "notes_a.nc, notes_b.nc" in skipped[0]

    def test_missing_files_are_still_listed(self, output: Path) -> None:
        _record_with(
            output.parent,
            Product(kind=ArtifactKind.restart, path="output/output_rst.1.nc"),
        )
        (output / "output_rst.1.nc").unlink()

        found = recorded_files_in(output, ArtifactKind.restart, _restart)

        assert found == [output / "output_rst.1.nc"]

    def test_no_record_lists_nothing(self, output: Path) -> None:
        output.mkdir()

        assert recorded_files_in(output, ArtifactKind.restart, _restart) == []


class TestRequireRecordedFiles:
    def test_present_files_pass(self, tmp_path: Path) -> None:
        present = tmp_path / "output_rst.1.nc"
        present.write_bytes(b"x")

        require_recorded_files(tmp_path, [present])

    def test_missing_file_is_refused(self, tmp_path: Path) -> None:
        present = tmp_path / "output_rst.1.nc"
        present.write_bytes(b"x")
        missing = tmp_path / "output" / "output_rst.2.nc"

        with pytest.raises(CstarError, match="output_rst.2.nc") as error:
            require_recorded_files(tmp_path, [present, missing])

        assert str(tmp_path) in str(error.value)
        assert "cstar run check" in str(error.value)
