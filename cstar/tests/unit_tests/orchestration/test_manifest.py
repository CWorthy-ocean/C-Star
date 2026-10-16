"""Tests for the run manifest and step record."""

import typing as t
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cstar.applications.core import ArtifactKind
from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.utils import slugify
from cstar.orchestration.manifest import (
    MANIFEST_LOCK_NAME,
    RUN_MANIFEST_NAME,
    STEP_RECORD_NAME,
    Attempt,
    AttemptStatus,
    CodebaseRef,
    ExternalDependencies,
    ExternalRun,
    Product,
    RunManifest,
    SourceDataRef,
    StepEntry,
    StepFacts,
    StepRecord,
    WorkplanIdentity,
    collect_dependencies,
    describe_steps,
    ensure_run_manifest,
    finalize_standalone_run,
    fold_step_records,
    manifest_lock,
    new_attempt,
    read_run_manifest,
    read_step_record,
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

    def test_step_record(self, tmp_path: Path) -> None:
        record = _record(versions={"cstar-ocean": "1.0"}, errors=["boom"])

        path = write_step_record(tmp_path / "step", record)

        assert path == tmp_path / "step" / STEP_RECORD_NAME
        assert read_step_record(tmp_path / "step") == record

    def test_missing_is_none(self, tmp_path: Path) -> None:
        assert read_run_manifest(tmp_path) is None
        assert read_step_record(tmp_path) is None

    def test_newer_schema_refused(self, tmp_path: Path) -> None:
        write_run_manifest(tmp_path, _manifest("one"))
        path = tmp_path / RUN_MANIFEST_NAME
        path.write_text(path.read_text().replace("1.0.0", "1.10.0", 1))

        with pytest.raises(CstarSchemaTooNewError, match="run manifest schema 1.10.0"):
            read_run_manifest(tmp_path)

    def test_newer_step_record_refused(self, tmp_path: Path) -> None:
        write_step_record(tmp_path, _record())
        path = tmp_path / STEP_RECORD_NAME
        path.write_text(path.read_text().replace("1.0.0", "2.0.0", 1))

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
