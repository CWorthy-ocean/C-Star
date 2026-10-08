"""Tests for the read-only discovery of blueprints emitted by runs of catalog entries."""

import logging
import subprocess
import sys
import threading
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

import cstar.catalog.outputs as discovery
from cstar.applications.forge.app import ForgeApplication
from cstar.applications.forge.blueprint import ForgeBlueprint, producer_ref
from cstar.catalog.domain_catalog import (
    _DEFAULT_CATALOG_ROOT,
    DomainCatalog,
    LayeredCatalog,
)
from cstar.catalog.outputs import (
    BlueprintOutput,
    OutputStatus,
    find_blueprint_outputs,
)
from cstar.execution.file_system import JobFileSystemManager, StateDirectoryManager
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.serialization import serialize
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun

FORGE = "forge"
_BUNDLED_ENTRY = _DEFAULT_CATALOG_ROOT / "blueprints" / FORGE / "wio-toy-simple.yaml"
_NEWER = datetime(2026, 10, 6, 12, tzinfo=UTC)
_OLDER = datetime(2026, 10, 5, 12, tzinfo=UTC)
_LOGGER = "cstar.catalog.outputs"


def _entry(
    root: Path, name: str, working_dir: Path | None, *, extra_days: int = 0
) -> tuple[Path, BlueprintIdentity]:
    """Write the forge entry ``name`` into the catalog at ``root``.

    ``working_dir`` is what the entry declares; ``None`` declares none.
    ``extra_days`` lengthens the run window, which changes the entry's content hash.

    Returns
    -------
    tuple[Path, BlueprintIdentity]
        The entry file and the producer reference a run of it records.
    """
    blueprint = ForgeBlueprint.from_yaml(_BUNDLED_ENTRY)
    run = blueprint.run.model_copy(
        update={"end_date": blueprint.run.end_date + timedelta(days=extra_days)}
    )
    path = root / "blueprints" / FORGE / f"{name}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    blueprint.model_copy(
        update={"name": name, "working_dir": working_dir, "run": run}
    ).to_yaml(path)
    return path, producer_ref(ForgeBlueprint.from_yaml(path))


def _catalog(root: Path) -> LayeredCatalog:
    """Scan the catalog at ``root``, so write its entries first."""
    root.mkdir(parents=True, exist_ok=True)
    return LayeredCatalog([DomainCatalog(root, suppress_validation=True)])


def _emit(
    path: Path,
    *,
    name: str = "emitted",
    derived_from: Sequence[Mapping[str, Any]] | None = None,
    generated_at: datetime | None = None,
) -> Path:
    """Write a minimal ``roms_marbl`` blueprint, with a provenance block if given."""
    data: dict[str, Any] = {"name": name, "application": "roms_marbl"}
    if derived_from is not None or generated_at is not None:
        provenance: dict[str, Any] = {"derived_from": list(derived_from or [])}
        if generated_at is not None:
            provenance["generated_at"] = generated_at
        data["provenance"] = provenance
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path


def _run(
    tmp_path: Path,
    run_id: str,
    entry: Path,
    *,
    working_dir: Path | None = None,
    application: str = FORGE,
    purged: bool = False,
) -> LiveStep:
    """Track a workplan run of one ``application`` step, as it is left on disk.

    ``purged`` deletes the transformed workplan, leaving only the tracking record.
    """
    step = LiveStep(
        name=f"{application}-step",
        application=application,
        blueprint=entry.as_posix(),
        working_dir=working_dir or tmp_path / "runs" / run_id / "step",
    )
    plan = LiveWorkplan(
        name=f"{run_id}-plan", description="A fabricated run.", steps=[step]
    )
    trx_path = tmp_path / "trx" / f"{run_id}.yaml"
    assert serialize(trx_path, plan)
    TrackingRepository().put_workplan_run_sync(
        WorkplanRun(
            workplan_path=tmp_path / f"{run_id}.yaml",
            trx_workplan_path=trx_path,
            output_path=tmp_path,
            run_id=run_id,
        )
    )
    if purged:
        trx_path.unlink()
    return step


def _debug_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name == _LOGGER]


# ---------------------------------------------------------------------------
# status of an output against the catalog
# ---------------------------------------------------------------------------
async def test_current_standalone_output(tmp_path: Path) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    _, producer = _entry(root, "wio", work)
    emitted = _emit(
        work / "output" / "B_wio.yaml",
        name="wio",
        derived_from=[producer.model_dump()],
        generated_at=_NEWER,
    )

    outputs = await _catalog(root).blueprint_outputs(FORGE)

    assert outputs == [
        BlueprintOutput(
            path=emitted,
            application="roms_marbl",
            producer=producer,
            status=OutputStatus.CURRENT,
            generated_at=_NEWER,
        )
    ]
    assert outputs == await find_blueprint_outputs(_catalog(root), FORGE)


async def test_output_of_an_edited_entry_is_changed(tmp_path: Path) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    _, recorded = _entry(root, "wio", work)
    _emit(
        work / "output" / "B_wio.yaml",
        name="wio",
        derived_from=[recorded.model_dump()],
    )
    _, edited = _entry(root, "wio", work, extra_days=1)
    assert edited.content_hash != recorded.content_hash

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert output.status is OutputStatus.CHANGED
    assert output.producer == recorded


async def test_standalone_output_without_provenance_is_unverified(
    tmp_path: Path,
) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    _entry(root, "wio", work)
    emitted = _emit(work / "output" / "B_wio.yaml")

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert output == BlueprintOutput(
        path=emitted,
        application="roms_marbl",
        producer=BlueprintIdentity(kind="Blueprint", application=FORGE, name="wio"),
        status=OutputStatus.UNVERIFIED,
    )


async def test_workplan_output_carries_its_run_and_step(tmp_path: Path) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    entry, producer = _entry(root, "wio", work)
    step = _run(tmp_path, "run-a", entry)
    emitted = _emit(
        step.fsm.output_dir / "B_wio.yaml",
        name="wio",
        derived_from=[producer.model_dump()],
        generated_at=_NEWER,
    )

    outputs = await _catalog(root).blueprint_outputs(FORGE)

    assert outputs == [
        BlueprintOutput(
            path=emitted,
            application="roms_marbl",
            producer=producer,
            status=OutputStatus.CURRENT,
            generated_at=_NEWER,
            run_id="run-a",
            step=step.name,
        )
    ]


async def test_standalone_output_under_the_default_working_dir(
    tmp_path: Path,
) -> None:
    root = tmp_path / "catalog"
    _, producer = _entry(root, "wio", None)
    default_dir = StateDirectoryManager.blueprint_run_dir(FORGE, "wio")
    emitted = _emit(
        JobFileSystemManager(default_dir).output_dir / "B_wio.yaml",
        derived_from=[producer.model_dump()],
    )

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert output.path == emitted
    assert output.status is OutputStatus.CURRENT


async def test_yml_outputs_are_found_too(tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    entry, producer = _entry(root, "wio", tmp_path / "work")
    step = _run(tmp_path, "run-a", entry)
    emitted = _emit(
        step.fsm.output_dir / "B_wio.yml", derived_from=[producer.model_dump()]
    )

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert output.path == emitted
    assert output.status is OutputStatus.CURRENT


async def test_output_of_a_producer_outside_the_catalog_is_uncataloged(
    tmp_path: Path,
) -> None:
    root = tmp_path / "catalog"
    entry, _ = _entry(root, "wio", tmp_path / "work")
    step = _run(tmp_path, "run-a", entry)
    gone = BlueprintIdentity(
        kind="Blueprint", application=FORGE, name="gone", content_hash="ab" * 32
    )
    _emit(step.fsm.output_dir / "B_gone.yaml", derived_from=[gone.model_dump()])

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert output.status is OutputStatus.UNCATALOGED
    assert output.producer == gone


async def test_output_of_an_entry_that_cannot_be_loaded_is_unverified(
    tmp_path: Path,
) -> None:
    """The producer is in the catalog, only unreadable: the output cannot be
    verified against it, which is not the same as the producer being missing.
    """
    root = tmp_path / "catalog"
    # lacks everything a forge blueprint requires beyond its name
    broken = root / "blueprints" / FORGE / "wio.yaml"
    broken.parent.mkdir(parents=True)
    broken.write_text("name: wio\napplication: forge\n")
    out = _run(tmp_path, "run-a", broken).fsm.output_dir
    recorded = BlueprintIdentity(
        kind="Blueprint", application=FORGE, name="wio", content_hash="ab" * 32
    )
    gone = BlueprintIdentity(
        kind="Blueprint", application=FORGE, name="gone", content_hash="cd" * 32
    )
    _emit(out / "B_wio.yaml", derived_from=[recorded.model_dump()])
    _emit(out / "B_gone.yaml", derived_from=[gone.model_dump()])

    outputs = await _catalog(root).blueprint_outputs(FORGE)

    assert {o.path.name: (o.status, o.producer) for o in outputs} == {
        "B_wio.yaml": (OutputStatus.UNVERIFIED, recorded),
        "B_gone.yaml": (OutputStatus.UNCATALOGED, gone),
    }


async def test_producer_is_the_first_blueprint_reference_of_the_application(
    tmp_path: Path,
) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    _, producer = _entry(root, "wio", work)
    downstream = BlueprintIdentity(
        kind="Blueprint", application="roms_marbl", name="wio", content_hash="x"
    )
    _emit(
        work / "output" / "B_wio.yaml",
        name="wio",
        derived_from=[
            {"kind": "DomainSpec", "name": "wio-toy", "origin": "catalog"},
            downstream.model_dump(),
            producer.model_dump(),
        ],
    )

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert output.status is OutputStatus.CURRENT
    assert output.producer == producer


async def test_outputs_match_entries_by_blueprint_name_not_file_name(
    tmp_path: Path,
) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    entry, producer = _entry(root, "wio", work)
    entry.rename(entry.with_name("renamed.yaml"))
    emitted = _emit(
        work / "output" / "B_wio.yaml",
        name="wio",
        derived_from=[producer.model_dump()],
    )
    catalog = _catalog(root)
    assert catalog.blueprint_names(FORGE) == ["renamed"]

    [output] = await catalog.blueprint_outputs(FORGE)
    assert output.status is OutputStatus.CURRENT

    emitted.write_text("name: wio\napplication: roms_marbl\n")
    [output] = await catalog.blueprint_outputs(FORGE)
    assert output.status is OutputStatus.UNVERIFIED
    assert output.producer.name == "wio"


async def test_catalog_spec_references_alone_leave_the_output_unverified(
    tmp_path: Path,
) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    _entry(root, "wio", work)
    _emit(
        work / "output" / "B_wio.yaml",
        name="wio",
        derived_from=[{"kind": "DomainSpec", "name": "wio-toy", "origin": "catalog"}],
        generated_at=_OLDER,
    )

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert output.status is OutputStatus.UNVERIFIED
    assert output.producer.name == "wio"
    assert output.generated_at == _OLDER


# ---------------------------------------------------------------------------
# what is skipped
# ---------------------------------------------------------------------------
async def test_entry_without_outputs_and_no_runs_finds_nothing(tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    _entry(root, "wio", tmp_path / "work")

    assert await _catalog(root).blueprint_outputs(FORGE) == []


async def test_purged_run_is_skipped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    root = tmp_path / "catalog"
    entry, producer = _entry(root, "wio", tmp_path / "work")
    _run(tmp_path, "run-a", entry, purged=True)
    kept = _run(tmp_path, "run-b", entry)
    _emit(kept.fsm.output_dir / "B_wio.yaml", derived_from=[producer.model_dump()])
    assert not (tmp_path / "trx" / "run-a.yaml").exists()

    with caplog.at_level(logging.DEBUG, logger=_LOGGER):
        outputs = await _catalog(root).blueprint_outputs(FORGE)

    assert [output.run_id for output in outputs] == ["run-b"]
    [message] = [m for m in _debug_messages(caplog) if "run-a" in m]
    assert "run-b" not in message


async def test_steps_of_other_applications_are_ignored(tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    entry, producer = _entry(root, "wio", tmp_path / "work")
    other = _run(tmp_path, "run-a", entry, application="hello_world")
    _emit(other.fsm.output_dir / "B_wio.yaml", derived_from=[producer.model_dump()])

    assert await _catalog(root).blueprint_outputs(FORGE) == []


async def test_unloadable_entries_are_skipped_and_named_in_one_message(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    _, producer = _entry(root, "wio", work)
    for name in ("broken-a", "broken-b"):
        # lacks everything a forge blueprint requires beyond its name
        (root / "blueprints" / FORGE / f"{name}.yaml").write_text(
            f"name: {name}\napplication: forge\n"
        )
    _emit(work / "output" / "B_wio.yaml", derived_from=[producer.model_dump()])

    with caplog.at_level(logging.DEBUG, logger=_LOGGER):
        outputs = await _catalog(root).blueprint_outputs(FORGE)

    assert [output.status for output in outputs] == [OutputStatus.CURRENT]
    [message] = [m for m in _debug_messages(caplog) if "broken-a" in m]
    assert "broken-b" in message


async def test_a_failing_emitted_blueprint_hook_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "catalog"
    _entry(root, "wio", tmp_path / "work")

    def broken(self: ForgeApplication, blueprint: ForgeBlueprint) -> None:
        raise ImportError("roms_tools is not installed")

    monkeypatch.setattr(ForgeApplication, "emitted_blueprint", broken)

    with pytest.raises(ImportError, match="roms_tools"):
        await _catalog(root).blueprint_outputs(FORGE)


async def test_unreadable_working_dir_costs_the_entry_only_its_standalone_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    entry, producer = _entry(root, "wio", work)
    standalone = work / "output" / "B_wio.yaml"
    step = _run(tmp_path, "run-a", entry)
    _emit(step.fsm.output_dir / "B_wio.yaml", derived_from=[producer.model_dump()])
    is_file = Path.is_file

    def denied(self: Path) -> bool:
        if self == standalone:
            raise PermissionError(13, "Permission denied", str(self))
        return is_file(self)

    monkeypatch.setattr(Path, "is_file", denied)

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert (output.run_id, output.status) == ("run-a", OutputStatus.CURRENT)


async def test_files_that_are_not_blueprints_are_skipped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    root = tmp_path / "catalog"
    entry, producer = _entry(root, "wio", tmp_path / "work")
    step = _run(tmp_path, "run-a", entry)
    # found twice, yet each skipped file is counted once
    _run(tmp_path, "run-b", entry, working_dir=step.working_dir)
    out = step.fsm.output_dir
    out.mkdir(parents=True)
    (out / "unclosed.yaml").write_text("key: [unclosed")
    (out / "a-list.yaml").write_text("- a\n- b\n")
    (out / "two-documents.yml").write_text("a: 1\n---\nb: 2\n")
    (out / "empty.yaml").write_text("")
    (out / "binary.yaml").write_bytes(b"\xff\xfe\x00")
    (out / "a-directory.yaml").mkdir()
    (out / "notes.txt").write_text("name: x\napplication: roms_marbl\n")
    emitted = _emit(out / "B_wio.yaml", derived_from=[producer.model_dump()])

    with caplog.at_level(logging.DEBUG, logger=_LOGGER):
        outputs = await _catalog(root).blueprint_outputs(FORGE)

    assert [output.path for output in outputs] == [emitted]
    [message] = [m for m in _debug_messages(caplog) if "not blueprints" in m]
    assert message.startswith("6 files")
    assert "notes.txt" not in message


async def test_malformed_provenance_reads_as_unverified(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    entry, _ = _entry(root, "wio", work)
    step = _run(tmp_path, "run-a", entry)
    unknown_kind = work / "output" / "B_wio.yaml"
    unknown_kind.parent.mkdir(parents=True)
    unknown_kind.write_text(
        "name: emitted\napplication: roms_marbl\nprovenance:\n  derived_from:\n"
        "  - kind: Nope\n"
    )
    not_a_mapping = step.fsm.output_dir / "B_other.yaml"
    not_a_mapping.parent.mkdir(parents=True)
    not_a_mapping.write_text("name: other\napplication: roms_marbl\nprovenance: x\n")

    with caplog.at_level(logging.DEBUG, logger=_LOGGER):
        outputs = await _catalog(root).blueprint_outputs(FORGE)

    assert {o.path: (o.status, o.producer.name) for o in outputs} == {
        unknown_kind: (OutputStatus.UNVERIFIED, "wio"),
        not_a_mapping: (OutputStatus.UNVERIFIED, "other"),
    }
    [message] = [m for m in _debug_messages(caplog) if "malformed" in m]
    assert str(unknown_kind) in message
    assert str(not_a_mapping) in message


# ---------------------------------------------------------------------------
# order and de-duplication
# ---------------------------------------------------------------------------
async def test_outputs_are_newest_first_with_undated_last(tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    entry, _ = _entry(root, "wio", tmp_path / "work")
    out = _run(tmp_path, "run-a", entry).fsm.output_dir
    _emit(out / "B_oldest.yaml", generated_at=_OLDER)
    _emit(out / "B_newest.yaml", generated_at=_NEWER)
    # naive, so taken as UTC: between the two aware ones
    _emit(out / "B_naive.yaml", generated_at=datetime(2026, 10, 5, 18))
    _emit(out / "B_undated_2.yaml")
    _emit(out / "B_undated_1.yaml")

    outputs = await _catalog(root).blueprint_outputs(FORGE)

    assert [output.path.name for output in outputs] == [
        "B_newest.yaml",
        "B_naive.yaml",
        "B_oldest.yaml",
        "B_undated_1.yaml",
        "B_undated_2.yaml",
    ]


async def test_file_found_by_a_standalone_run_and_a_workplan_run_is_listed_once(
    tmp_path: Path,
) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    entry, producer = _entry(root, "wio", work)
    _emit(work / "output" / "B_wio.yaml", derived_from=[producer.model_dump()])
    _run(tmp_path, "run-a", entry, working_dir=work)

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert output.run_id == ""


async def test_file_found_by_two_runs_is_listed_under_the_first_run_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    list_latest_runs = TrackingRepository.list_latest_runs

    async def last_run_id_first(
        self: TrackingRepository, run_id_filter: str = ""
    ) -> Sequence[WorkplanRun]:
        runs = await list_latest_runs(self, run_id_filter)
        return sorted(runs, key=lambda run: run.run_id, reverse=True)

    monkeypatch.setattr(TrackingRepository, "list_latest_runs", last_run_id_first)
    root = tmp_path / "catalog"
    entry, producer = _entry(root, "wio", tmp_path / "work")
    shared = tmp_path / "shared"
    _run(tmp_path, "run-b", entry, working_dir=shared)
    step = _run(tmp_path, "run-a", entry, working_dir=shared)
    _emit(step.fsm.output_dir / "B_wio.yaml", derived_from=[producer.model_dump()])

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert (output.run_id, output.step) == ("run-a", step.name)


# ---------------------------------------------------------------------------
# the event loop and the scans
# ---------------------------------------------------------------------------
async def test_the_entry_scan_and_the_run_scan_overlap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Neither scan needs the other, so one must not wait for the other to finish."""
    root, work = tmp_path / "catalog", tmp_path / "work"
    _, producer = _entry(root, "wio", work)
    _emit(work / "output" / "B_wio.yaml", derived_from=[producer.model_dump()])
    run_scan_started = threading.Event()
    scan_entries, scan_runs = discovery._entry_candidates, discovery._run_candidates

    def entries(catalog: LayeredCatalog, application: str) -> Any:
        # Awaited first, this scan would end before the run scan ever started.
        assert run_scan_started.wait(timeout=5), "the run scan did not start"
        return scan_entries(catalog, application)

    async def runs(application: str) -> Any:
        run_scan_started.set()
        return await scan_runs(application)

    monkeypatch.setattr(discovery, "_entry_candidates", entries)
    monkeypatch.setattr(discovery, "_run_candidates", runs)

    [output] = await _catalog(root).blueprint_outputs(FORGE)

    assert output.status is OutputStatus.CURRENT


async def test_run_output_directories_are_read_off_the_event_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The wizard awaits discovery on the kernel's event loop, which a directory
    listing must not block.
    """
    root = tmp_path / "catalog"
    entry, producer = _entry(root, "wio", tmp_path / "work")
    step = _run(tmp_path, "run-a", entry)
    _emit(step.fsm.output_dir / "B_wio.yaml", derived_from=[producer.model_dump()])
    catalog = _catalog(root)
    loop_thread = threading.get_ident()
    globbed_in: list[int] = []
    glob = Path.glob

    def recording_glob(self: Path, *args: Any, **kwargs: Any) -> Any:
        if self == step.fsm.output_dir:
            globbed_in.append(threading.get_ident())
        return glob(self, *args, **kwargs)

    monkeypatch.setattr(Path, "glob", recording_glob)

    [output] = await catalog.blueprint_outputs(FORGE)

    assert output.run_id == "run-a"
    assert globbed_in
    assert loop_thread not in globbed_in


# ---------------------------------------------------------------------------
# the catalog and the application
# ---------------------------------------------------------------------------
async def test_discovery_writes_nothing_into_the_catalog(tmp_path: Path) -> None:
    root, work = tmp_path / "catalog", tmp_path / "work"
    entry, producer = _entry(root, "wio", work)
    _emit(work / "output" / "B_wio.yaml", derived_from=[producer.model_dump()])
    step = _run(tmp_path, "run-a", entry)
    _emit(step.fsm.output_dir / "B_wio.yaml", derived_from=[producer.model_dump()])
    catalog = _catalog(root)

    def snapshot() -> dict[Path, int]:
        return {p: p.stat().st_mtime_ns for p in root.rglob("*")}

    before = snapshot()
    assert len(await catalog.blueprint_outputs(FORGE)) == 2
    assert snapshot() == before


async def test_application_that_emits_no_blueprint_finds_nothing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "catalog"
    root.mkdir()
    stack = LayeredCatalog(
        [DomainCatalog(root, suppress_validation=True), DomainCatalog(read_only=True)]
    )
    assert "wales-toy" in stack.blueprint_names("roms_marbl")

    assert await stack.blueprint_outputs("roms_marbl") == []


async def test_unregistered_application_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="No application for 'no-such-app'"):
        await _catalog(tmp_path / "catalog").blueprint_outputs("no-such-app")


def test_importing_the_catalog_does_not_import_discovery() -> None:
    code = (
        "import sys\n"
        "import cstar.catalog\n"
        "loaded = [m for m in sys.modules if m.startswith('cstar.')]\n"
        "assert 'cstar.catalog.outputs' not in loaded, loaded\n"
        "assert not [m for m in loaded if m.startswith('cstar.applications')], loaded\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout
