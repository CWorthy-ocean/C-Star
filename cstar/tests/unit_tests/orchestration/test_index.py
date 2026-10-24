"""Tests for the run index and the run resolver."""

import logging
import re
import typing as t
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock
from uuid import uuid4

import pytest

import cstar.orchestration.index as index_module
import cstar.orchestration.manifest as manifest_module
from cstar.base.exceptions import CstarError
from cstar.execution.file_system import DirectoryManager, StateDirectoryManager
from cstar.orchestration.index import (
    WHERE_LINKED,
    WHERE_PROJECT,
    WHERE_UNTRACKED,
    Durability,
    HandleKind,
    IndexRoot,
    ManifestRecord,
    Origin,
    RunHandle,
    RunIndex,
    RunLocation,
    RunRecord,
    default_index,
    is_path_like,
    parse_run_handle,
    resolve_run,
    run_uri,
    scan_roots,
    tracked_by_uuid,
    untracked_locations,
    where_kept,
)
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    Attempt,
    RunManifest,
    StepEntry,
    StepRecord,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.tracking import (
    KEY_RUN_UUID,
    TrackingRepository,
    WorkplanRun,
)
from cstar.tests.unit_tests.orchestration.run_tree import link

NOW = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)

CURRENT = RunManifest.model_fields["schema_version"].default
"""The schema version this build writes."""

NEWER_MAJOR = f"{int(CURRENT.split('.')[0]) + 1}.0.0"
"""A schema version this build refuses as too new."""


@pytest.fixture(autouse=True)
def project_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the project home at a temporary directory; return its runs area.

    The directory does not exist until a test makes it: no project area is
    the default. `PROJECT` is cleared so no test can reach a real one.
    """
    monkeypatch.delenv("PROJECT", raising=False)
    project = (tmp_path / "project").resolve()
    monkeypatch.setenv("CSTAR_PROJECT_HOME", str(project))
    return project / "cstar" / "runs"


def make_run(
    root: Path,
    run_id: str,
    *,
    uuid: str | None = None,
    kind: t.Literal["workplan", "blueprint"] = "workplan",
    steps: t.Sequence[str] = ("one",),
    attempts: int = 1,
    status: str = "",
) -> RunManifest:
    """Write the manifest of a run into `root` and return it."""
    manifest = RunManifest(
        id=uuid or str(uuid4()),
        run_id=run_id,
        name=run_id,
        kind=kind,
        created_at=NOW,
        attempts=[Attempt(start_at=NOW + timedelta(hours=n)) for n in range(attempts)],
        steps=[
            StepEntry(
                name=name,
                application="hello_world",
                working_dir="." if kind == "blueprint" else f"tasks/{name}",
                status=status,
            )
            for name in steps
        ],
    )
    root.mkdir(parents=True, exist_ok=True)
    write_run_manifest(root, manifest)
    return manifest


def local_run(run_id: str, **kwargs: t.Any) -> Path:
    """Make a workplan run in the data home; return its directory."""
    root = StateDirectoryManager.workplan_runs_dir() / run_id
    make_run(root, run_id, **kwargs)
    return root


def local_manifest_id(root: Path) -> str:
    """The uuid of the manifest in a run directory."""
    manifest = index_module.read_run_manifest(root)
    assert manifest is not None
    return manifest.id


def track(
    root: Path, run_id: str, *, uuid: str = "", attached: bool = False
) -> WorkplanRun:
    """Record a run in tracking, stamped with `uuid` when given."""
    metadata = {KEY_RUN_UUID: uuid} if uuid else {}
    run = WorkplanRun(
        output_path=root,
        run_id=run_id,
        metadata=metadata,
        attached=attached,
    )
    TrackingRepository().put_workplan_run_sync(run)
    return run


def finished_record(outcome: str = "completed") -> StepRecord:
    """A step record with the given outcome."""
    return StepRecord(
        step="one",
        application="hello_world",
        blueprint=BlueprintIdentity(
            kind="Blueprint", application="hello_world", name="bp", content_hash="abc"
        ),
        outcome=outcome,
        finished_at=NOW,
    )


def too_new_text(manifest_file: Path) -> str:
    """The text of a manifest file with a schema this build refuses as too new."""
    return re.sub(
        r"^schema_version: .*$",
        f"schema_version: {NEWER_MAJOR}",
        manifest_file.read_text(),
        count=1,
        flags=re.MULTILINE,
    )


def warnings_of(caplog: pytest.LogCaptureFixture) -> list[str]:
    """The text of the warnings logged so far."""
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.fixture
def opened(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Record the directories the index reads a manifest from."""
    calls: list[Path] = []
    real = index_module.read_run_manifest

    def spy(root: Path) -> RunManifest | None:
        calls.append(root)
        return real(root)

    monkeypatch.setattr(index_module, "read_run_manifest", spy)
    return calls


class TestGrammar:
    @pytest.mark.parametrize("text", ["my-run", "  my-run \n", "Mixed Case"])
    def test_run_id(self, text: str) -> None:
        handle = parse_run_handle(text)

        assert handle.kind is HandleKind.run_id
        # not normalized: callers slugify run-ids themselves
        assert handle.value == text.strip()
        assert str(handle) == text.strip()

    def test_uuid(self) -> None:
        wanted = str(uuid4())

        handle = parse_run_handle(f"run://{wanted}")

        assert handle == RunHandle(HandleKind.uuid, wanted)
        assert str(handle) == run_uri(wanted) == f"run://{wanted}"

    def test_scheme_is_case_insensitive_and_whitespace_is_tolerated(self) -> None:
        wanted = str(uuid4())

        handle = parse_run_handle(f"  RUN://{wanted.upper()}  ")

        assert handle.kind is HandleKind.uuid
        assert handle.value == wanted

    @pytest.mark.parametrize(
        "text", ["run://", "run://nonsense", "run://1234", "run://a/b"]
    )
    def test_bad_uuid(self, text: str) -> None:
        with pytest.raises(ValueError, match=r"run://<uuid>"):
            parse_run_handle(text)

    @pytest.mark.parametrize("text", ["", "   ", "\n"])
    def test_empty(self, text: str) -> None:
        with pytest.raises(ValueError, match="empty"):
            parse_run_handle(text)

    @pytest.mark.parametrize(
        "text", ["./x", "~/x", "a/b", "/abs/run", "../up", ".hidden", " ./x "]
    )
    def test_path(self, text: str) -> None:
        handle = parse_run_handle(text)

        assert handle.kind is HandleKind.path
        assert handle.value == text.strip()
        assert is_path_like(handle.value)

    def test_a_run_id_is_not_path_like(self) -> None:
        assert not is_path_like("my-run")


class TestScan:
    def test_roots_put_local_before_project(self, project_runs: Path) -> None:
        assert scan_roots() == [
            IndexRoot(StateDirectoryManager.workplan_runs_dir(), Origin.local, 1),
            IndexRoot(StateDirectoryManager.blueprint_runs_dir(), Origin.local, 2),
            IndexRoot(project_runs, Origin.project, 1),
        ]

    def test_no_project_home_means_no_project_root(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("CSTAR_PROJECT_HOME")

        assert DirectoryManager.project_runs_home() is None
        assert [root.origin for root in scan_roots()] == [Origin.local, Origin.local]
        assert RunIndex().locations() == []

    def test_finds_every_kind_of_run(
        self, tmp_path: Path, project_runs: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        alpha, beta = local_run("alpha"), local_run("beta")
        gamma = StateDirectoryManager.blueprint_runs_dir() / "hello_world" / "gamma"
        make_run(gamma, "gamma", kind="blueprint")
        delta = project_runs / "delta"
        make_run(delta, "delta")
        archive = tmp_path / "archive" / "epsilon"
        make_run(archive, "epsilon")
        epsilon = link(project_runs / "epsilon", archive)

        with caplog.at_level(logging.DEBUG):
            found = RunIndex().locations()

        assert {loc.root: loc for loc in found}.keys() == {
            alpha,
            beta,
            gamma,
            delta,
            epsilon,
        }
        by_run = {loc.run_id: loc for loc in found}
        assert [by_run[r].origin for r in ("alpha", "beta", "gamma")] == [
            Origin.local
        ] * 3
        assert [by_run[r].durability for r in ("alpha", "beta", "gamma")] == [
            Durability.none
        ] * 3
        assert (by_run["delta"].origin, by_run["delta"].durability) == (
            Origin.project,
            Durability.materialized,
        )
        assert (by_run["epsilon"].origin, by_run["epsilon"].durability) == (
            Origin.project,
            Durability.linked,
        )
        # the link is not followed in the location, and the order is the roots'
        assert by_run["epsilon"].root == epsilon
        assert [loc.origin for loc in found] == sorted(
            (loc.origin for loc in found), key=lambda o: o is Origin.project
        )
        assert by_run["alpha"].uuid == local_manifest_id(alpha)
        assert by_run["alpha"].name == "alpha"
        assert by_run["alpha"].kind == "workplan"
        assert by_run["gamma"].kind == "blueprint"
        assert by_run["alpha"].start_at == NOW
        # a scan keeps what identifies a run, not the manifests
        assert not hasattr(by_run["alpha"], "manifest")
        assert not warnings_of(caplog)

    def test_a_timestamp_with_no_zone_is_taken_as_utc(self) -> None:
        naive = datetime(2026, 10, 15, 12, 0)
        root = StateDirectoryManager.workplan_runs_dir() / "naive"
        make_run(root, "naive")
        manifest = index_module.read_run_manifest(root)
        assert manifest is not None
        manifest.attempts[0].start_at = naive
        write_run_manifest(root, manifest)

        (found,) = RunIndex().locations()

        assert found.start_at == NOW
        assert found.start_at.tzinfo is not None

    def test_skips_what_is_not_a_run_silently(
        self, project_runs: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        wanted = local_run("kept")
        # a run that predates the manifest
        (StateDirectoryManager.workplan_runs_dir() / "legacy" / "tasks" / "x").mkdir(
            parents=True
        )
        # a stray file, a run-like directory inside a run, a blueprint app with no runs
        (StateDirectoryManager.workplan_runs_dir() / "stray.txt").write_text("x")
        make_run(wanted / "tasks" / "inner", "inner")
        (StateDirectoryManager.blueprint_runs_dir() / "hello_world").mkdir(parents=True)
        (StateDirectoryManager.blueprint_runs_dir() / "stray.txt").write_text("x")
        project_runs.mkdir(parents=True)
        link(project_runs / "dangling", project_runs / "nowhere")

        with caplog.at_level(logging.DEBUG):
            found = RunIndex().locations()

        assert [loc.root for loc in found] == [wanted]
        assert not warnings_of(caplog)

    def test_skips_unreadable_and_too_new_manifests_with_one_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        good = local_run("good")
        broken = StateDirectoryManager.workplan_runs_dir() / "broken"
        broken.mkdir(parents=True)
        (broken / RUN_MANIFEST_NAME).write_text("run_id: [unclosed\n")
        future = local_run("future")
        manifest_file = future / RUN_MANIFEST_NAME
        manifest_file.write_text(too_new_text(manifest_file))

        with caplog.at_level(logging.WARNING):
            found = RunIndex().locations()

        assert [loc.root for loc in found] == [good]
        (warning,) = warnings_of(caplog)
        assert "2 run manifest(s)" in warning
        assert str(broken) in warning
        assert str(future) in warning
        assert "Unable to read the run manifest" in warning

    def test_scans_once_and_returns_copies(self, opened: list[Path]) -> None:
        local_run("alpha")
        index = RunIndex()
        assert not index.scanned

        first = index.locations()
        reads = len(opened)
        first.clear()

        assert index.scanned
        assert len(index.locations()) == 1
        assert len(opened) == reads

    def test_by_id(self) -> None:
        local_run("alpha")
        wanted = str(uuid4())
        beta = local_run("beta", uuid=wanted)
        index = RunIndex()

        assert [loc.root for loc in index.by_id(wanted)] == [beta]
        assert index.by_id(str(uuid4())) == []

    def test_depth_two_roots_hold_application_directories(self, tmp_path: Path) -> None:
        make_run(tmp_path / "bp" / "hello_world" / "one", "one")
        make_run(tmp_path / "bp" / "other_app" / "one", "one")
        make_run(tmp_path / "bp" / "stray", "stray")

        found = RunIndex([IndexRoot(tmp_path / "bp", Origin.local, 2)]).locations()

        assert [loc.root.relative_to(tmp_path / "bp").as_posix() for loc in found] == [
            "hello_world/one",
            "other_app/one",
        ]


class TestByRunId:
    def test_before_a_scan_opens_only_directories_of_that_name(
        self,
        opened: list[Path],
        caplog: pytest.LogCaptureFixture,
        project_runs: Path,
    ) -> None:
        wanted = local_run("target")
        local_run("other")
        broken = StateDirectoryManager.workplan_runs_dir() / "broken"
        broken.mkdir(parents=True)
        (broken / RUN_MANIFEST_NAME).write_text("run_id: [unclosed\n")
        also = project_runs / "target"
        make_run(also, "target")
        make_run(
            StateDirectoryManager.blueprint_runs_dir() / "hello_world" / "target",
            "target",
            kind="blueprint",
        )
        index = RunIndex()

        with caplog.at_level(logging.DEBUG):
            found = index.by_run_id("target")

        assert {loc.root.name for loc in found} == {"target"}
        assert len(found) == 3
        assert found[0].root == wanted
        assert found[-1].root == also
        assert not index.scanned
        assert not warnings_of(caplog)
        assert {path.name for path in opened} == {"target"}

    def test_keeps_only_manifests_of_that_run_id(self, tmp_path: Path) -> None:
        # a directory renamed after the run was made
        renamed = tmp_path / "runs" / "renamed"
        make_run(renamed, "original")
        index = RunIndex([IndexRoot(tmp_path / "runs", Origin.local, 1)])

        assert index.by_run_id("renamed") == []
        assert index.by_run_id("original") == []
        assert [loc.root for loc in index.locations()] == [renamed]
        assert [loc.root for loc in index.by_run_id("original")] == [renamed]

    def test_after_a_scan_filters_what_was_found(
        self, opened: list[Path], caplog: pytest.LogCaptureFixture
    ) -> None:
        local_run("target")
        local_run("other")
        broken = StateDirectoryManager.workplan_runs_dir() / "broken"
        broken.mkdir(parents=True)
        (broken / RUN_MANIFEST_NAME).write_text("run_id: [unclosed\n")
        index = RunIndex()
        with caplog.at_level(logging.WARNING):
            index.locations()
        reads = len(opened)
        caplog.clear()

        found = index.by_run_id("target")

        assert [loc.run_id for loc in found] == ["target"]
        assert len(opened) == reads
        assert not warnings_of(caplog)

    def test_a_bad_manifest_of_that_name_is_warned_about(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        broken = StateDirectoryManager.workplan_runs_dir() / "target"
        broken.mkdir(parents=True)
        (broken / RUN_MANIFEST_NAME).write_text("run_id: [unclosed\n")

        with caplog.at_level(logging.WARNING):
            assert RunIndex().by_run_id("target") == []

        (warning,) = warnings_of(caplog)
        assert str(broken) in warning

    @pytest.mark.parametrize("run_id", ["", "../alpha", "a/b", ".", ".."])
    def test_what_is_no_run_id_finds_nothing(
        self, run_id: str, opened: list[Path]
    ) -> None:
        local_run("alpha")

        assert RunIndex().by_run_id(run_id) == []
        assert opened == []

    def test_missing_roots_find_nothing(self, tmp_path: Path) -> None:
        index = RunIndex([IndexRoot(tmp_path / "nowhere", Origin.local, 2)])

        assert index.by_run_id("x") == []
        assert index.locations() == []


class TestManifestRecord:
    @staticmethod
    def record(root: Path, **kwargs: t.Any) -> ManifestRecord:
        run_id = kwargs.pop("run_id", "run")
        make_run(root, run_id, **kwargs)
        (location,) = RunIndex([IndexRoot(root.parent, Origin.local, 1)]).locations()
        return ManifestRecord(location)

    def test_identity(self, tmp_path: Path) -> None:
        record = self.record(tmp_path / "run", run_id="named")

        assert record.run_id == "named"
        assert record.uuid == local_manifest_id(tmp_path / "run")
        assert record.output_path == tmp_path / "run"

    def test_start_is_the_latest_attempt(self, tmp_path: Path) -> None:
        record = self.record(tmp_path / "run", attempts=3)

        assert record.start_at == NOW + timedelta(hours=2)

    def test_start_without_attempts_is_the_manifest_creation(
        self, tmp_path: Path
    ) -> None:
        record = self.record(tmp_path / "run", attempts=0)

        assert record.start_at == NOW

    def test_manifest_is_read_when_asked_for_and_kept(
        self, tmp_path: Path, opened: list[Path]
    ) -> None:
        record = self.record(tmp_path / "run", steps=("one", "two"))
        opened.clear()

        manifest = record.manifest

        assert [step.name for step in manifest.steps] == ["one", "two"]
        assert manifest.id == record.uuid
        assert record.manifest is manifest
        assert opened == [tmp_path / "run"]

    def test_manifest_that_is_gone(self, tmp_path: Path) -> None:
        record = self.record(tmp_path / "run")
        (tmp_path / "run" / RUN_MANIFEST_NAME).unlink()

        with pytest.raises(CstarError, match="no longer holds") as raised:
            _ = record.manifest

        assert str(tmp_path / "run") in str(raised.value)

    @pytest.mark.parametrize("too_new", [False, True])
    def test_manifest_that_cannot_be_read(self, tmp_path: Path, too_new: bool) -> None:
        record = self.record(tmp_path / "run")
        manifest_file = tmp_path / "run" / RUN_MANIFEST_NAME
        manifest_file.write_text(
            too_new_text(manifest_file) if too_new else "run_id: [unclosed\n"
        )

        with pytest.raises(CstarError, match="Unable to read") as raised:
            _ = record.manifest

        assert str(tmp_path / "run") in str(raised.value)
        assert ("newer than this build" in str(raised.value)) is too_new

    def test_manifest_of_another_run(self, tmp_path: Path) -> None:
        record = self.record(tmp_path / "run")
        other = make_run(tmp_path / "run", "run")

        with pytest.raises(CstarError, match="is now") as raised:
            _ = record.manifest

        assert run_uri(other.id) in str(raised.value)
        assert run_uri(record.uuid) in str(raised.value)

    def test_transformed_workplan(self, tmp_path: Path) -> None:
        record = self.record(tmp_path / "run")
        (tmp_path / "run" / "plan_transformed.yaml").write_text("x")
        (tmp_path / "run" / "plan.yaml").write_text("x")

        assert record.trx_workplan_path == tmp_path / "run" / "plan_transformed.yaml"

    def test_blueprint_run_has_no_workplan(self, tmp_path: Path) -> None:
        record = self.record(tmp_path / "run", run_id="solo", kind="blueprint")

        with pytest.raises(CstarError, match="standalone blueprint run") as raised:
            _ = record.trx_workplan_path

        assert "'solo'" in str(raised.value)
        assert str(tmp_path / "run") in str(raised.value)

    def test_no_transformed_workplan(self, tmp_path: Path) -> None:
        record = self.record(tmp_path / "run")

        with pytest.raises(CstarError, match="no transformed workplan"):
            _ = record.trx_workplan_path

    def test_several_transformed_workplans(self, tmp_path: Path) -> None:
        record = self.record(tmp_path / "run")
        for name in ("a_transformed.yaml", "b_transformed.yaml"):
            (tmp_path / "run" / name).write_text("x")

        with pytest.raises(CstarError, match="more than one") as raised:
            _ = record.trx_workplan_path

        assert "a_transformed.yaml, b_transformed.yaml" in str(raised.value)

    @pytest.mark.parametrize(
        ("outcome", "status"),
        [("completed", "Done"), ("failed", "Failed"), ("cancelled", "Cancelled")],
    )
    def test_status_of_a_step_with_a_record(
        self, tmp_path: Path, outcome: str, status: str
    ) -> None:
        record = self.record(tmp_path / "run", steps=("one", "two"))
        write_step_record(tmp_path / "run" / "tasks" / "one", finished_record(outcome))

        assert record.step_status("one") == status

    def test_a_record_on_disk_beats_the_snapshot(self, tmp_path: Path) -> None:
        record = self.record(tmp_path / "run", status="Running")
        write_step_record(tmp_path / "run" / "tasks" / "one", finished_record())

        assert record.step_status("one") == "Done"

    def test_a_record_that_disagrees_with_the_snapshot_wins(
        self, tmp_path: Path
    ) -> None:
        record = self.record(tmp_path / "run", status="Done")
        write_step_record(tmp_path / "run" / "tasks" / "one", finished_record("failed"))

        assert record.step_status("one") == "Failed"

    def test_without_a_record_the_snapshot_is_used(self, tmp_path: Path) -> None:
        record = self.record(tmp_path / "run", steps=("one", "two"), status="Running")

        assert record.step_status("two") == "Running"

    def test_a_snapshot_that_says_done_is_not_evidence(self, tmp_path: Path) -> None:
        """A copy without the step's own record, or one cut short in transfer, holds
        the same snapshot a finished run does.
        """
        record = self.record(tmp_path / "run", status="Done")

        assert record.step_status("one") == ""

    def test_a_step_that_is_not_in_the_manifest_has_no_status(
        self, tmp_path: Path
    ) -> None:
        record = self.record(tmp_path / "run")

        assert record.step_status("nine") == ""

    def test_a_step_record_is_read_for_the_step_asked_about_only(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        record = self.record(tmp_path / "run", steps=("one", "two", "three"))
        read: list[Path] = []
        real = manifest_module.read_step_record

        def spy(step_root: Path) -> StepRecord | None:
            read.append(step_root)
            return real(step_root)

        monkeypatch.setattr(manifest_module, "read_step_record", spy)

        record.step_status("two")
        record.step_status("two")

        assert read == [tmp_path / "run" / "tasks" / "two"]

    def test_nothing_is_written(self, tmp_path: Path) -> None:
        root = tmp_path / "run"
        record = self.record(root, steps=("one", "two"), status="Running")
        write_step_record(root / "tasks" / "one", finished_record())
        before_bytes = (root / RUN_MANIFEST_NAME).read_bytes()
        before_tree = sorted(root.rglob("*"))

        assert record.step_status("one") == "Done"
        assert record.step_status("two") == "Running"

        assert (root / RUN_MANIFEST_NAME).read_bytes() == before_bytes
        assert sorted(root.rglob("*")) == before_tree

    def test_a_step_is_read_once(self, tmp_path: Path) -> None:
        """The first question about a step fixes what the record says of it."""
        root = tmp_path / "run"
        record = self.record(root)
        step_dir = root / "tasks" / "one"
        write_step_record(step_dir, finished_record())
        assert record.step_status("one") == "Done"

        write_step_record(step_dir, finished_record("failed"))

        assert record.step_status("one") == "Done"

    @pytest.mark.parametrize(
        ("snapshot", "expected"), [("Running", "Running"), ("Done", "")]
    )
    def test_a_step_record_that_cannot_be_read_is_a_warning_and_no_evidence(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        snapshot: str,
        expected: str,
    ) -> None:
        root = tmp_path / "run"
        record = self.record(root, status=snapshot)
        (root / "tasks" / "one").mkdir(parents=True)
        (root / "tasks" / "one" / "cstar-step.yaml").write_text("step: [unclosed\n")

        with caplog.at_level(logging.WARNING):
            assert record.step_status("one") == expected
            assert record.step_status("one") == expected

        (warning,) = warnings_of(caplog)
        assert "'one'" in warning
        assert "'run'" in warning

    def test_a_manifest_that_is_gone_is_a_warning_and_no_status(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        record = self.record(tmp_path / "run")
        (tmp_path / "run" / RUN_MANIFEST_NAME).unlink()

        with caplog.at_level(logging.WARNING):
            assert record.step_status("one") == ""

        assert "no longer holds" in "".join(warnings_of(caplog))

    def test_is_a_run_record(self, tmp_path: Path) -> None:
        record: RunRecord = self.record(tmp_path / "run")

        assert record.run_id == "run"


class TestLive:
    def test_a_manifest_record_is_never_live(self, tmp_path: Path) -> None:
        root = tmp_path / "kept"
        make_run(root, "kept")

        assert resolve_run(parse_run_handle(str(root))).live is False

    def test_a_tracked_record_is_live_unless_attached(self, tmp_path: Path) -> None:
        root = tmp_path / "kept"
        make_run(root, "kept")

        assert track(root, "scheduled").live is True
        assert track(root, "attached", attached=True).live is False


class TestResolveRunId:
    def test_a_tracked_run_wins(self) -> None:
        root = local_run("tracked")
        track(root, "tracked")  # not stamped: its manifest names the uuid

        found = resolve_run(parse_run_handle("tracked"))

        assert isinstance(found, WorkplanRun)
        assert found.output_path == root
        assert found.uuid == local_manifest_id(root)
        assert not default_index().scanned

    def test_a_tracked_run_costs_one_miss_in_the_project_area(
        self, project_runs: Path, opened: list[Path]
    ) -> None:
        root = local_run("tracked")
        track(root, "tracked", uuid=str(uuid4()))

        found = resolve_run(parse_run_handle("tracked"))

        assert found.output_path == root
        assert opened == [project_runs / "tracked"]

    def test_a_project_run_of_another_uuid_only_warns(
        self, project_runs: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        root = local_run("shared")
        mine = str(uuid4())
        track(root, "shared", uuid=mine)
        theirs = make_run(project_runs / "shared", "shared")

        with caplog.at_level(logging.WARNING):
            found = resolve_run(parse_run_handle("shared"))

        assert isinstance(found, WorkplanRun)
        assert found.output_path == root
        (warning,) = warnings_of(caplog)
        assert run_uri(mine) in warning
        assert str(root) in warning
        assert run_uri(theirs.id) in warning
        assert str(project_runs / "shared") in warning

    def test_a_project_copy_of_the_same_run_is_no_shadow(
        self, project_runs: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        root = local_run("shared")
        track(root, "shared", uuid=local_manifest_id(root))
        make_run(project_runs / "shared", "shared", uuid=local_manifest_id(root))

        with caplog.at_level(logging.WARNING):
            found = resolve_run(parse_run_handle("shared"))

        assert isinstance(found, WorkplanRun)
        assert not warnings_of(caplog)

    def test_an_unreadable_shadow_is_not_a_warning(
        self, project_runs: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        root = local_run("shared")
        track(root, "shared", uuid=str(uuid4()))
        (project_runs / "shared").mkdir(parents=True)
        (project_runs / "shared" / RUN_MANIFEST_NAME).write_text("run_id: [bad\n")

        with caplog.at_level(logging.WARNING):
            found = resolve_run(parse_run_handle("shared"))

        assert isinstance(found, WorkplanRun)
        assert not warnings_of(caplog)

    def test_a_tracked_run_whose_directory_is_gone_uses_its_copy(
        self, project_runs: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        wanted = str(uuid4())
        gone = StateDirectoryManager.workplan_runs_dir() / "purged"
        track(gone, "purged", uuid=wanted)
        copy = project_runs / "purged"
        make_run(copy, "purged", uuid=wanted)

        with (
            caplog.at_level(logging.WARNING),
            mock.patch.object(index_module.log, "info") as info,
        ):
            found = resolve_run(parse_run_handle("purged"))

        assert isinstance(found, ManifestRecord)
        assert found.output_path == copy
        assert found.uuid == wanted
        info.assert_called_once()
        assert str(gone) in info.call_args.args[0]
        assert str(copy) in info.call_args.args[0]
        assert not warnings_of(caplog)

    def test_a_copy_of_another_run_does_not_replace_a_purged_one(
        self, project_runs: Path
    ) -> None:
        gone = StateDirectoryManager.workplan_runs_dir() / "purged"
        track(gone, "purged", uuid=str(uuid4()))
        make_run(project_runs / "purged", "purged")

        found = resolve_run(parse_run_handle("purged"))

        # the callers report the missing directory, as they do today
        assert isinstance(found, WorkplanRun)
        assert found.output_path == gone

    def test_a_purged_run_with_no_copy_is_still_the_tracked_one(self) -> None:
        gone = StateDirectoryManager.workplan_runs_dir() / "purged"
        track(gone, "purged")

        found = resolve_run(parse_run_handle("purged"))

        assert isinstance(found, WorkplanRun)
        assert found.output_path == gone

    def test_an_untracked_run_is_known_by_its_manifest(
        self, project_runs: Path
    ) -> None:
        copy = project_runs / "kept"
        manifest = make_run(copy, "kept")

        found = resolve_run(parse_run_handle("kept"))

        assert isinstance(found, ManifestRecord)
        assert found.uuid == manifest.id
        assert found.output_path == copy
        assert not default_index().scanned

    def test_a_blueprint_run_is_known_by_its_manifest(self) -> None:
        root = StateDirectoryManager.blueprint_runs_dir() / "hello_world" / "solo"
        make_run(root, "solo", kind="blueprint")

        found = resolve_run(parse_run_handle("solo"))

        assert isinstance(found, ManifestRecord)
        assert found.output_path == root
        with pytest.raises(CstarError, match="standalone blueprint run"):
            _ = found.trx_workplan_path

    def test_several_runs_of_one_run_id_are_an_error_that_lists_them(
        self, project_runs: Path
    ) -> None:
        mine = local_run("dup")
        theirs = make_run(project_runs / "dup", "dup")

        with pytest.raises(CstarError, match="More than one run") as raised:
            resolve_run(parse_run_handle("dup"))

        message = str(raised.value)
        assert run_uri(local_manifest_id(mine)) in message
        assert f"at {mine} (local)" in message
        assert f"{run_uri(theirs.id)} at {project_runs / 'dup'} (project)" in message
        assert "run://<uuid>" in message

    def test_copies_of_one_run_are_not_a_conflict(self, project_runs: Path) -> None:
        mine = local_run("dup")
        make_run(project_runs / "dup", "dup", uuid=local_manifest_id(mine))

        found = resolve_run(parse_run_handle("dup"))

        assert isinstance(found, ManifestRecord)
        assert found.output_path == mine

    def test_a_pinned_uuid_picks_one_of_several(self, project_runs: Path) -> None:
        local_run("dup")
        theirs = make_run(project_runs / "dup", "dup")

        found = resolve_run(parse_run_handle("dup"), uuid=theirs.id)

        assert isinstance(found, ManifestRecord)
        assert found.uuid == theirs.id
        assert found.output_path == project_runs / "dup"

    def test_a_pinned_uuid_finds_a_renamed_directory(self, project_runs: Path) -> None:
        renamed = make_run(project_runs / "dup-2026", "dup")

        found = resolve_run(parse_run_handle("dup"), uuid=renamed.id)

        assert isinstance(found, ManifestRecord)
        assert found.output_path == project_runs / "dup-2026"
        assert default_index().scanned

    def test_a_pinned_uuid_does_not_take_a_tracked_run_of_another_uuid(
        self, project_runs: Path
    ) -> None:
        track(local_run("dup"), "dup", uuid=str(uuid4()))
        theirs = make_run(project_runs / "dup", "dup")

        found = resolve_run(parse_run_handle("dup"), uuid=theirs.id)

        assert isinstance(found, ManifestRecord)
        assert found.uuid == theirs.id

    def test_a_pinned_uuid_takes_the_tracked_run_that_matches(self) -> None:
        root = local_run("dup")
        mine = local_manifest_id(root)
        track(root, "dup", uuid=mine)

        found = resolve_run(parse_run_handle("dup"), uuid=mine)

        assert isinstance(found, WorkplanRun)

    def test_a_pinned_uuid_accepts_a_tracked_run_that_cannot_say_its_own(
        self,
    ) -> None:
        root = StateDirectoryManager.workplan_runs_dir() / "bare"
        root.mkdir(parents=True)
        track(root, "bare")

        found = resolve_run(parse_run_handle("bare"), uuid=str(uuid4()))

        assert isinstance(found, WorkplanRun)
        assert found.output_path == root

    @pytest.mark.parametrize("spelling", ["Spin Up", " SPIN-UP ", "spin up"])
    def test_a_run_id_is_slugified_as_tracking_does(
        self, project_runs: Path, spelling: str
    ) -> None:
        copy = project_runs / "spin-up"
        make_run(copy, "spin-up")

        found = resolve_run(parse_run_handle(spelling))

        assert isinstance(found, ManifestRecord)
        assert found.output_path == copy

    def test_the_shadow_of_a_tracked_run_is_looked_up_by_its_slug(
        self, project_runs: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        root = local_run("spin-up")
        track(root, "spin-up", uuid=local_manifest_id(root))
        theirs = make_run(project_runs / "spin-up", "spin-up")

        with caplog.at_level(logging.WARNING):
            found = resolve_run(parse_run_handle("Spin Up"))

        assert isinstance(found, WorkplanRun)
        (warning,) = warnings_of(caplog)
        assert run_uri(theirs.id) in warning

    def test_a_purged_run_of_an_unknown_identity_reports_the_copies(
        self, tmp_path: Path, project_runs: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A record scheduled before the uuid was cached cannot say which run it
        was once its directory, and the manifest in it, are gone: no copy is
        guessed.
        """
        gone = StateDirectoryManager.workplan_runs_dir() / "purged"
        track(gone, "purged")
        mine = make_run(project_runs / "purged", "purged")
        archive = tmp_path / "archive" / "purged"
        theirs = make_run(archive, "purged")
        link(StateDirectoryManager.workplan_runs_dir() / "purged-2", archive)

        with caplog.at_level(logging.WARNING):
            found = resolve_run(parse_run_handle("purged"))

        assert isinstance(found, WorkplanRun)
        assert found.output_path == gone
        (warning,) = warnings_of(caplog)
        assert f"{run_uri(mine.id)} at {project_runs / 'purged'}" in warning
        assert "cannot be read" in warning
        assert "run:// handle" in warning
        assert theirs.id not in warning
        assert not default_index().scanned

    def test_a_purged_run_of_an_unknown_identity_with_no_copy_is_quiet(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        gone = StateDirectoryManager.workplan_runs_dir() / "purged"
        track(gone, "purged")

        with caplog.at_level(logging.WARNING):
            found = resolve_run(parse_run_handle("purged"))

        assert isinstance(found, WorkplanRun)
        assert not warnings_of(caplog)

    def test_a_pinned_uuid_that_replaced_the_tracked_run_says_so(
        self, project_runs: Path
    ) -> None:
        root = local_run("replaced")
        mine = local_manifest_id(root)
        track(root, "replaced", uuid=mine)
        asked = str(uuid4())

        with pytest.raises(CstarError, match="tracked here as") as raised:
            resolve_run(parse_run_handle("replaced"), uuid=asked)

        message = str(raised.value)
        assert f"tracked here as {run_uri(mine)}, not the {run_uri(asked)}" in message
        assert "replaced since" in message
        assert "No copy of the asked-for run" in message
        assert str(project_runs) in message
        assert "not tracked here" not in message

    def test_an_unknown_run_id(self) -> None:
        local_run("something-else")

        with pytest.raises(
            CstarError, match="No run found for run-id nothing"
        ) as raised:
            resolve_run(parse_run_handle("nothing"))

        assert "cstar run ls --all" in str(raised.value)
        assert str(StateDirectoryManager.workplan_runs_dir()) in str(raised.value)

    def test_a_run_id_that_is_no_slug(self) -> None:
        with pytest.raises(CstarError, match="Unable to look up run '!!!'"):
            resolve_run(RunHandle(HandleKind.run_id, "!!!"))

    def test_the_given_index_is_the_one_used(self, tmp_path: Path) -> None:
        make_run(tmp_path / "elsewhere" / "far", "far")
        index = RunIndex([IndexRoot(tmp_path / "elsewhere", Origin.local, 1)])

        found = resolve_run(parse_run_handle("far"), index=index)

        assert found.output_path == tmp_path / "elsewhere" / "far"
        assert not default_index().scanned


class TestResolveUuid:
    def test_an_untracked_run(self, project_runs: Path) -> None:
        manifest = make_run(project_runs / "kept", "kept")

        found = resolve_run(parse_run_handle(run_uri(manifest.id)))

        assert isinstance(found, ManifestRecord)
        assert found.output_path == project_runs / "kept"
        assert found.uuid == manifest.id

    def test_a_tracked_run(self) -> None:
        root = local_run("tracked")
        wanted = local_manifest_id(root)
        track(root, "tracked", uuid=wanted)

        found = resolve_run(parse_run_handle(run_uri(wanted)))

        assert isinstance(found, WorkplanRun)
        assert found.output_path == root

    def test_a_tracked_run_of_another_uuid_is_not_the_run(
        self, project_runs: Path
    ) -> None:
        track(local_run("dup"), "dup", uuid=str(uuid4()))
        theirs = make_run(project_runs / "dup", "dup")

        found = resolve_run(parse_run_handle(run_uri(theirs.id)))

        assert isinstance(found, ManifestRecord)
        assert found.output_path == project_runs / "dup"

    def test_a_copy_of_a_tracked_run_is_the_tracked_run(
        self, project_runs: Path
    ) -> None:
        root = local_run("shared")
        wanted = local_manifest_id(root)
        track(root, "shared", uuid=wanted)
        make_run(project_runs / "shared", "shared", uuid=wanted)

        found = resolve_run(parse_run_handle(run_uri(wanted)))

        assert isinstance(found, WorkplanRun)

    def test_a_blueprint_run_is_never_tracked(self) -> None:
        root = StateDirectoryManager.blueprint_runs_dir() / "hello_world" / "solo"
        manifest = make_run(root, "solo", kind="blueprint")

        found = resolve_run(parse_run_handle(run_uri(manifest.id)))

        assert isinstance(found, ManifestRecord)
        assert found.output_path == root

    def test_an_unknown_uuid(self) -> None:
        local_run("alpha")
        unknown = str(uuid4())

        with pytest.raises(
            CstarError, match=f"No run has the uuid {unknown}"
        ) as raised:
            resolve_run(parse_run_handle(run_uri(unknown)))

        assert str(StateDirectoryManager.workplan_runs_dir()) in str(raised.value)

    def test_another_pinned_uuid_is_refused(self) -> None:
        handle = parse_run_handle(run_uri(str(uuid4())))

        with pytest.raises(CstarError, match="another uuid"):
            resolve_run(handle, uuid=str(uuid4()))


class TestTrackedByUuid:
    def test_a_run_outside_every_index_root(self, tmp_path: Path) -> None:
        wanted = str(uuid4())
        root = tmp_path / "colleague" / "their-run"
        make_run(root, "their-run", uuid=wanted)
        track(root, "their-run", uuid=wanted, attached=True)

        found = tracked_by_uuid(wanted)

        assert isinstance(found, WorkplanRun)
        assert found.output_path == root
        assert not any(root == loc.root for loc in RunIndex().locations())

    def test_no_record_holds_the_uuid(self, tmp_path: Path) -> None:
        track(tmp_path / "other", "other", uuid=str(uuid4()))
        (tmp_path / "other").mkdir()

        assert tracked_by_uuid(str(uuid4())) is None

    def test_a_record_whose_directory_is_gone_is_not_found(
        self, tmp_path: Path
    ) -> None:
        wanted = str(uuid4())
        track(tmp_path / "purged", "purged", uuid=wanted)

        assert tracked_by_uuid(wanted) is None

    def test_several_records_are_an_error_that_lists_them(self, tmp_path: Path) -> None:
        wanted = str(uuid4())
        for name in ("first", "second"):
            (tmp_path / name).mkdir()
            track(tmp_path / name, name, uuid=wanted)

        with pytest.raises(CstarError, match="More than one tracked run") as raised:
            tracked_by_uuid(wanted)

        message = str(raised.value)
        assert "'first'" in message
        assert "'second'" in message
        assert str(tmp_path / "first") in message
        assert "run-id" in message

    def test_a_record_that_does_not_cache_its_uuid_is_never_read_for_it(
        self, tmp_path: Path, opened: list[Path], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = tmp_path / "unstamped"
        manifest = make_run(root, "unstamped")
        track(root, "unstamped")
        read: list[Path] = []
        real = manifest_module.read_run_manifest

        def spy(path: Path) -> RunManifest | None:
            read.append(path)
            return real(path)

        monkeypatch.setattr("cstar.orchestration.tracking.read_run_manifest", spy)

        assert tracked_by_uuid(manifest.id) is None

        assert opened == []
        assert read == []

    def test_unreadable_records_are_skipped(self, tmp_path: Path) -> None:
        wanted = str(uuid4())
        root = tmp_path / "good"
        root.mkdir()
        track(root, "good", uuid=wanted)
        repo = TrackingRepository()
        (repo.latest_dir / "broken.yaml").write_text("run_id: [unclosed\n")

        found = tracked_by_uuid(wanted)

        assert found is not None
        assert found.run_id == "good"

    def test_a_repository_that_cannot_be_read_finds_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def refuse(self: TrackingRepository) -> t.NoReturn:
            raise OSError("unreadable")

        monkeypatch.setattr(TrackingRepository, "list_latest_runs_sync", refuse)

        assert tracked_by_uuid(str(uuid4())) is None

    def test_it_works_inside_a_running_event_loop(self, tmp_path: Path) -> None:
        import asyncio

        wanted = str(uuid4())
        root = tmp_path / "in-loop"
        root.mkdir()
        track(root, "in-loop", uuid=wanted)

        async def ask() -> WorkplanRun | None:
            return tracked_by_uuid(wanted)

        found = asyncio.run(ask())

        assert found is not None
        assert found.run_id == "in-loop"


class TestAttachedRuns:
    def test_a_uuid_finds_the_attached_run_without_a_scan(self, tmp_path: Path) -> None:
        wanted = str(uuid4())
        root = tmp_path / "colleague" / "their-run"
        make_run(root, "their-run", uuid=wanted)
        track(root, "their-run", uuid=wanted, attached=True)
        index = RunIndex()

        found = resolve_run(parse_run_handle(run_uri(wanted)), index=index)

        assert isinstance(found, WorkplanRun)
        assert found.attached
        assert found.output_path == root
        assert not index.scanned

    def test_a_path_finds_the_run_attached_under_another_run_id(
        self, tmp_path: Path
    ) -> None:
        root = local_run("original")
        wanted = local_manifest_id(root)
        track(root, "renamed", uuid=wanted, attached=True)

        found = resolve_run(parse_run_handle(str(root)))

        assert isinstance(found, WorkplanRun)
        assert found.run_id == "renamed"

    def test_the_manifest_run_id_finds_the_run_attached_under_another(
        self, tmp_path: Path
    ) -> None:
        root = local_run("original")
        wanted = local_manifest_id(root)
        track(root, "renamed", uuid=wanted, attached=True)

        found = resolve_run(parse_run_handle("original"))

        assert isinstance(found, WorkplanRun)
        assert found.run_id == "renamed"

    def test_a_record_of_the_run_id_with_another_uuid_does_not_hide_it(self) -> None:
        root = local_run("shared")
        wanted = local_manifest_id(root)
        track(root, "shared-copy", uuid=wanted, attached=True)
        other = StateDirectoryManager.workplan_runs_dir() / "decoy"
        make_run(other, "decoy")
        track(other, "shared", uuid=str(uuid4()))

        found = resolve_run(parse_run_handle(str(root)))

        assert isinstance(found, WorkplanRun)
        assert found.run_id == "shared-copy"

    def test_a_directory_named_by_the_user_is_found_though_the_uuid_is_ambiguous(
        self, tmp_path: Path
    ) -> None:
        root = local_run("original")
        wanted = local_manifest_id(root)
        for name in ("first", "second"):
            (tmp_path / name).mkdir()
            track(tmp_path / name, name, uuid=wanted, attached=True)

        by_path = resolve_run(parse_run_handle(str(root)))

        assert isinstance(by_path, ManifestRecord)
        assert by_path.output_path == root
        with pytest.raises(CstarError, match="More than one tracked run"):
            resolve_run(parse_run_handle(run_uri(wanted)))

    def test_a_directory_named_by_the_user_must_be_the_attached_one(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "attached"
        wanted = str(uuid4())
        make_run(root, "attached", uuid=wanted)
        track(root, "attached", uuid=wanted, attached=True)
        copy = tmp_path / "copy"
        make_run(copy, "attached", uuid=wanted)

        found = resolve_run(parse_run_handle(str(copy)))

        assert isinstance(found, ManifestRecord)
        assert found.output_path == copy


class TestResolvePath:
    def test_an_untracked_run(self, tmp_path: Path) -> None:
        root = tmp_path / "anywhere" / "kept"
        manifest = make_run(root, "kept")

        found = resolve_run(parse_run_handle(str(root)))

        assert isinstance(found, ManifestRecord)
        assert found.uuid == manifest.id
        assert found.output_path == root
        assert found.location.origin is Origin.local
        assert found.location.durability is Durability.none

    def test_a_relative_path_and_a_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = tmp_path / "base" / "kept"
        make_run(root, "kept")
        monkeypatch.chdir(tmp_path / "base")

        assert resolve_run(parse_run_handle("./kept")).output_path == root
        assert resolve_run(parse_run_handle("../base/kept/")).output_path == root

        monkeypatch.setenv("HOME", str(tmp_path / "base"))
        assert resolve_run(parse_run_handle("~/kept")).output_path == root

    def test_a_run_in_the_project_area(self, project_runs: Path) -> None:
        copy = project_runs / "kept"
        make_run(copy, "kept")

        found = resolve_run(parse_run_handle(str(copy)))

        assert isinstance(found, ManifestRecord)
        assert found.location.origin is Origin.project
        assert found.location.durability is Durability.materialized

    def test_a_link_in_the_project_area_and_its_target(
        self, tmp_path: Path, project_runs: Path
    ) -> None:
        target = tmp_path / "archive" / "kept"
        make_run(target, "kept")
        linked = link(project_runs / "kept", target)

        by_link = resolve_run(parse_run_handle(str(linked)))
        by_target = resolve_run(parse_run_handle(str(target)))

        assert by_link.output_path == linked
        assert isinstance(by_link, ManifestRecord)
        assert (by_link.location.origin, by_link.location.durability) == (
            Origin.project,
            Durability.linked,
        )
        assert isinstance(by_target, ManifestRecord)
        assert (by_target.location.origin, by_target.location.durability) == (
            Origin.local,
            Durability.none,
        )

    def test_the_tracked_directory_is_the_tracked_run(self) -> None:
        root = local_run("tracked")
        track(root, "tracked", uuid=local_manifest_id(root))

        found = resolve_run(parse_run_handle(str(root)))

        assert isinstance(found, WorkplanRun)

    def test_a_copy_of_a_tracked_run_is_the_copy_that_was_named(
        self, project_runs: Path
    ) -> None:
        root = local_run("tracked")
        wanted = local_manifest_id(root)
        track(root, "tracked", uuid=wanted)
        copy = project_runs / "tracked"
        make_run(copy, "tracked", uuid=wanted)

        found = resolve_run(parse_run_handle(str(copy)))

        assert isinstance(found, ManifestRecord)
        assert found.output_path == copy

    def test_a_pinned_uuid_that_the_directory_does_not_hold(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "kept"
        manifest = make_run(root, "kept")
        asked = str(uuid4())

        with pytest.raises(
            CstarError, match="not the run that was asked for"
        ) as raised:
            resolve_run(parse_run_handle(str(root)), uuid=asked)

        assert f"{root} is {run_uri(manifest.id)}, not {run_uri(asked)}" in str(
            raised.value
        )

    def test_a_pinned_uuid_that_it_does_hold(self, tmp_path: Path) -> None:
        root = tmp_path / "kept"
        manifest = make_run(root, "kept")

        found = resolve_run(parse_run_handle(str(root)), uuid=manifest.id)

        assert found.uuid == manifest.id

    def test_not_a_directory(self, tmp_path: Path) -> None:
        (tmp_path / "file").write_text("x")

        with pytest.raises(CstarError, match="is not a directory"):
            resolve_run(parse_run_handle(str(tmp_path / "file")))
        with pytest.raises(CstarError, match="is not a directory"):
            resolve_run(parse_run_handle(str(tmp_path / "nowhere")))

    def test_no_manifest(self, tmp_path: Path) -> None:
        (tmp_path / "empty").mkdir()

        with pytest.raises(CstarError, match="holds no run manifest") as raised:
            resolve_run(parse_run_handle(str(tmp_path / "empty")))

        assert "predates" not in str(raised.value)

    def test_a_run_that_predates_the_manifest(self, tmp_path: Path) -> None:
        (tmp_path / "old" / "tasks").mkdir(parents=True)

        with pytest.raises(CstarError, match="predates the manifest") as raised:
            resolve_run(parse_run_handle(str(tmp_path / "old")))

        assert "cstar workplan submit --run-id <run-id>" in str(raised.value)

    @pytest.mark.parametrize("too_new", [False, True])
    def test_a_manifest_that_cannot_be_read(
        self, tmp_path: Path, too_new: bool
    ) -> None:
        root = tmp_path / "bad"
        make_run(root, "bad")
        manifest_file = root / RUN_MANIFEST_NAME
        manifest_file.write_text(
            too_new_text(manifest_file) if too_new else "run_id: [unclosed\n"
        )

        with pytest.raises(
            CstarError, match="newer than this build" if too_new else "Unable"
        ):
            resolve_run(parse_run_handle(str(root)))


class TestWhereKept:
    @pytest.mark.parametrize(
        ("origin", "durability", "expected"),
        [
            (Origin.local, Durability.none, WHERE_UNTRACKED),
            (Origin.project, Durability.materialized, WHERE_PROJECT),
            (Origin.project, Durability.linked, WHERE_LINKED),
        ],
    )
    def test_labels(
        self, tmp_path: Path, origin: Origin, durability: Durability, expected: str
    ) -> None:
        location = RunLocation(
            root=tmp_path,
            origin=origin,
            durability=durability,
            uuid=str(uuid4()),
            run_id="run",
            name="run",
            kind="workplan",
            start_at=NOW,
        )

        assert where_kept(location) == expected


class TestUntrackedLocations:
    """Which of the runs the index finds are not tracked here."""

    @staticmethod
    def locations(*roots: Path) -> list[RunLocation]:
        by_root = {loc.root: loc for loc in RunIndex().locations()}
        return [by_root[root] for root in roots]

    @pytest.fixture
    def uuid_reads(self) -> t.Iterator[mock.PropertyMock]:
        """Count the times a tracked run is asked for its uuid."""
        with mock.patch.object(
            WorkplanRun, "uuid", new_callable=mock.PropertyMock, return_value=""
        ) as reads:
            yield reads

    def test_a_run_in_a_tracked_directory_is_tracked(
        self, uuid_reads: mock.PropertyMock
    ) -> None:
        mine = local_run("mine")
        other = local_run("other")
        found = self.locations(mine, other)

        left = untracked_locations(found, [track(mine, "mine")])

        assert [loc.root for loc in left] == [other]
        uuid_reads.assert_not_called()

    def test_a_copy_of_a_tracked_run_is_that_run(
        self, project_runs: Path, uuid_reads: mock.PropertyMock
    ) -> None:
        mine = local_run("mine")
        copy = project_runs / "mine"
        make_run(copy, "mine", uuid=local_manifest_id(mine))
        theirs = project_runs / "theirs"
        make_run(theirs, "mine")

        left = untracked_locations(
            self.locations(mine, copy, theirs), [track(mine, "mine")]
        )

        # the uuid is the one the scan read where the run is; the record is not asked
        assert [loc.root for loc in left] == [theirs]
        uuid_reads.assert_not_called()

    def test_nothing_left_asks_nothing(self, uuid_reads: mock.PropertyMock) -> None:
        mine = local_run("mine")

        assert untracked_locations(self.locations(mine), [track(mine, "mine")]) == []
        uuid_reads.assert_not_called()

    def test_a_purged_run_does_not_hide_its_copy(self, project_runs: Path) -> None:
        wanted = str(uuid4())
        gone = StateDirectoryManager.workplan_runs_dir() / "purged"
        copy = project_runs / "purged"
        make_run(copy, "purged", uuid=wanted)

        left = untracked_locations(
            self.locations(copy), [track(gone, "purged", uuid=wanted)]
        )

        assert [loc.root for loc in left] == [copy]

    def test_a_tracked_run_the_scan_did_not_find_is_asked_for_its_uuid(
        self, tmp_path: Path, project_runs: Path
    ) -> None:
        # kept where no index root looks, so the scan did not read its manifest
        elsewhere = tmp_path / "elsewhere" / "mine"
        wanted = make_run(elsewhere, "mine").id
        copy = project_runs / "mine"
        make_run(copy, "mine", uuid=wanted)
        unstamped = track(elsewhere, "mine")

        with mock.patch.object(
            WorkplanRun, "uuid", new_callable=mock.PropertyMock, return_value=wanted
        ) as reads:
            left = untracked_locations(self.locations(copy), [unstamped])

        assert left == []
        reads.assert_called_once()

    def test_the_order_of_the_locations_is_kept(self) -> None:
        roots = [local_run(name) for name in ("c", "a", "b")]

        left = untracked_locations(self.locations(*roots), [])

        assert [loc.root for loc in left] == roots
