"""Tests for `cstar run attach`: registering a copied run in this machine's tracking.

The runs are built elsewhere than the data home, as a copy from another machine
would be, with a completed step record and a product for the first step.
"""

import os
import shutil
import typing as t
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from click.testing import Result
from typer.testing import CliRunner

from cstar.cli.run import app as app_run
from cstar.cli.run.attach import default_access
from cstar.orchestration.index import parse_run_handle, resolve_run, run_uri
from cstar.orchestration.manifest import (
    Product,
    StepRecord,
    WorkplanIdentity,
    read_run_manifest,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.tracking import (
    KEY_RUN_UUID,
    Access,
    TrackingRepository,
    WorkplanRun,
)
from cstar.tests.unit_tests.orchestration.manifest_runs import STARTED, BuiltRun

EXIT_BAD_ARGS: t.Final[int] = 2
STEP: t.Final[str] = "Spin Up"
PRODUCT: t.Final[str] = "output/only.nc"
CONTENT: t.Final[str] = "data"


def invoke(*args: str) -> Result:
    """Run a `cstar run` command against the tracking of the test's data home."""
    return CliRunner().invoke(app_run, list(args), color=False)


def tracked(run_id: str) -> WorkplanRun | None:
    """The tracking record of a run-id, as the repository finds it."""
    return TrackingRepository().get_workplan_run_sync(run_id)


def tracked_path(run_id: str) -> Path:
    """The file holding the latest record of a run-id."""
    return TrackingRepository().latest_path(run_id).resolve()


def history_files(run_id: str) -> list[Path]:
    """The history records written for a run-id."""
    return sorted(TrackingRepository().run_history_dir(run_id).glob("*"))


@pytest.fixture
def elsewhere(
    tmp_path: Path, build_run: t.Callable[..., BuiltRun]
) -> t.Callable[..., BuiltRun]:
    """Build a finished run that arrived from another machine, in a directory of its own."""

    def build(
        run_id: str = "arrived",
        sha256: str = "",
        dest: Path | None = None,
        **kwargs: t.Any,
    ) -> BuiltRun:
        built = build_run(tmp_path / "origin" / run_id, run_id, **kwargs)
        step = built.step_dir(STEP)
        (step / PRODUCT).parent.mkdir()
        (step / PRODUCT).write_text(CONTENT)
        write_step_record(
            step,
            StepRecord(
                step=STEP,
                application="hello_world",
                blueprint=BlueprintIdentity(
                    kind="Blueprint",
                    application="hello_world",
                    name="bp",
                    content_hash="abc",
                ),
                outcome="completed",
                finished_at=STARTED,
                products=[
                    Product(
                        kind="output",
                        path=PRODUCT,
                        size_bytes=len(CONTENT),
                        sha256=sha256,
                    )
                ],
            ),
        )
        # moved, so that the files that name the run's directory name the old one
        arrived = dest or tmp_path / "elsewhere" / run_id
        shutil.copytree(built.root, arrived, symlinks=True)
        shutil.rmtree(built.root)
        return replace(built, root=arrived)

    return build


def test_a_new_record_holds_what_the_manifest_says(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify attach writes one thin record: the run, its directory, its uuid and
    attachment, with the name and start read from the manifest rather than stored.
    """
    built = elsewhere(attempts=2)

    result = invoke("attach", str(built.root))

    assert result.exit_code == 0, result.output
    record = tracked(built.run_id)
    assert record is not None
    assert record.run_id == built.run_id
    assert record.output_path == built.root
    assert record.name == built.run_id
    assert record.trx_workplan_path == built.root / f"{built.run_id}_transformed.yaml"
    assert record.workplan_path == record.trx_workplan_path
    assert record.start_at == STARTED + timedelta(hours=1)
    assert record.environment == {}
    assert record.user_variables == {}
    assert record.sentinels == set()
    assert record.metadata == {KEY_RUN_UUID: built.uuid}
    assert record.attached is True
    assert record.access is Access.owned
    assert record.legacy.name == ""
    stored = yaml.safe_load(tracked_path(built.run_id).read_text())
    assert not {"workplan_path", "trx_workplan_path", "start_at"} & set(stored)
    assert "name" not in stored["metadata"]
    assert f"Attached run {built.run_id!r} ({run_uri(built.uuid)})" in result.output
    assert len(history_files(built.run_id)) == 1


def test_attach_writes_nothing_into_the_run(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify the run directory is as it was after the run is attached."""
    built = elsewhere()
    before = sorted(built.root.rglob("*"))
    manifest = built.manifest_bytes

    assert invoke("attach", str(built.root)).exit_code == 0

    assert sorted(built.root.rglob("*")) == before
    assert built.manifest_bytes == manifest


def test_the_path_keeps_its_symlinks(
    elsewhere: t.Callable[..., BuiltRun], tmp_path: Path
) -> None:
    """Verify the recorded directory is the one named, not the one it resolves to."""
    built = elsewhere()
    link = tmp_path / "link"
    link.symlink_to(built.root, target_is_directory=True)

    result = invoke("attach", str(link))

    assert result.exit_code == 0, result.output
    record = tracked(built.run_id)
    assert record is not None
    assert record.output_path == link


def test_dry_run_writes_no_record_and_prints_the_plan(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify `--dry-run` says what it would do and leaves tracking alone."""
    built = elsewhere()

    result = invoke("attach", str(built.root), "--dry-run")

    assert result.exit_code == 0, result.output
    assert (
        f"Would attach run {built.run_id!r} ({built.uuid}) at {built.root} as owned "
        "[new record]"
    ) in result.output
    assert tracked(built.run_id) is None
    assert not history_files(built.run_id)


def test_dry_run_exits_as_the_real_run_would(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify `--dry-run` of an incomplete transfer exits 1, and of a taken run-id 2."""
    incomplete = elsewhere("incomplete")
    (incomplete.step_dir(STEP) / PRODUCT).unlink()
    other = elsewhere("other")
    assert invoke("attach", str(other.root)).exit_code == 0
    clash = elsewhere("other-copy")
    # the same run-id, another run
    manifest = read_run_manifest(clash.root)
    assert manifest is not None
    manifest.run_id = "other"
    write_run_manifest(clash.root, manifest)

    assert invoke("attach", str(incomplete.root), "--dry-run").exit_code == 1
    assert invoke("attach", str(clash.root), "--dry-run").exit_code == EXIT_BAD_ARGS


def test_a_directory_without_a_manifest_is_refused(
    tmp_path: Path, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a directory that predates the manifest is refused, with the way to get one."""
    root = tmp_path / "elsewhere" / "old"
    (root / "tasks").mkdir(parents=True)

    result = invoke("attach", str(root))

    assert result.exit_code == EXIT_BAD_ARGS
    text = flatten_cli_output(result.output + result.stderr)
    assert "holds no run manifest" in text
    assert "cstar workplan submit --run-id" in text
    assert tracked("old") is None


def test_a_missing_directory_is_refused(tmp_path: Path) -> None:
    """Verify a path that is no directory is refused."""
    result = invoke("attach", str(tmp_path / "elsewhere" / "nothing"))

    assert result.exit_code == EXIT_BAD_ARGS


@pytest.mark.parametrize("handle", ["run-id", "uuid"])
def test_a_run_that_is_known_by_name_is_refused(
    local_run: t.Callable[..., BuiltRun],
    flatten_cli_output: t.Callable[[str], str],
    handle: str,
) -> None:
    """Verify attach takes a directory: a run-id and a `run://<uuid>` are refused."""
    built = local_run("known")
    text = built.run_id if handle == "run-id" else run_uri(built.uuid)

    result = invoke("attach", text)

    assert result.exit_code == EXIT_BAD_ARGS
    assert "attach takes the directory of a run" in flatten_cli_output(
        result.output + result.stderr
    )
    assert tracked(built.run_id) is None


def test_a_blueprint_run_is_refused(
    build_run: t.Callable[..., BuiltRun],
    tmp_path: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a standalone blueprint run, which is never tracked, is refused."""
    built = build_run(
        tmp_path / "elsewhere" / "lone", "lone", kind="blueprint", steps=("only",)
    )

    result = invoke("attach", str(built.root))

    assert result.exit_code == EXIT_BAD_ARGS
    assert "standalone blueprint run" in flatten_cli_output(
        result.output + result.stderr
    )
    assert tracked("lone") is None


@pytest.mark.parametrize("damage", ["missing", "truncated"])
def test_an_incomplete_transfer_is_refused_and_nothing_is_written(
    elsewhere: t.Callable[..., BuiltRun], damage: str
) -> None:
    """Verify a product that is missing or cut short stops the attach, exit 1."""
    built = elsewhere()
    product = built.step_dir(STEP) / PRODUCT
    if damage == "missing":
        product.unlink()
    else:
        product.write_text(CONTENT[:1])

    result = invoke("attach", str(built.root))

    assert result.exit_code == 1
    assert "Not attached" in result.output + result.stderr
    assert "incomplete" in result.output + result.stderr
    assert tracked(built.run_id) is None
    assert not history_files(built.run_id)


def test_a_run_attached_again_from_another_directory_is_re_attached(
    elsewhere: t.Callable[..., BuiltRun], tmp_path: Path
) -> None:
    """Verify the same run in a new place updates the record rather than refusing."""
    built = elsewhere()
    assert invoke("attach", str(built.root)).exit_code == 0
    moved = tmp_path / "elsewhere" / "moved"
    shutil.copytree(built.root, moved, symlinks=True)
    shutil.rmtree(built.root)

    result = invoke("attach", str(moved))

    assert result.exit_code == 0, result.output
    assert f"Re-attached run {built.run_id!r}" in result.output
    record = tracked(built.run_id)
    assert record is not None
    assert record.output_path == moved
    assert record.trx_workplan_path.parent == moved
    assert record.workplan_path.parent == moved
    assert record.attached is True
    assert record.metadata[KEY_RUN_UUID] == built.uuid


def test_re_attaching_keeps_what_the_record_held(
    elsewhere: t.Callable[..., BuiltRun], tmp_path: Path
) -> None:
    """Verify the environment and variables of a record survive a re-attach."""
    built = elsewhere()
    assert invoke("attach", str(built.root)).exit_code == 0
    repository = TrackingRepository()

    def keep(run: WorkplanRun) -> None:
        run.environment = {"KEPT": "yes"}
        run.user_variables = {"var": "1"}
        run.sentinels = {tmp_path / "stale"}

    repository.update_workplan_run_sync(built.run_id, keep)
    moved = tmp_path / "elsewhere" / "moved"
    shutil.copytree(built.root, moved, symlinks=True)

    assert invoke("attach", str(moved)).exit_code == 0

    record = tracked(built.run_id)
    assert record is not None
    assert record.output_path == moved
    assert record.environment == {"KEPT": "yes"}
    assert dict(record.user_variables) == {"var": "1"}
    assert record.sentinels == set()


def test_a_run_that_is_already_attached_changes_nothing(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify attaching twice exits 0 and writes no second history record."""
    built = elsewhere()
    assert invoke("attach", str(built.root)).exit_code == 0
    before = history_files(built.run_id)
    contents = [p.read_bytes() for p in before]

    result = invoke("attach", str(built.root))

    assert result.exit_code == 0, result.output
    assert f"Run {built.run_id!r} is already attached here at" in " ".join(
        result.output.split()
    )
    assert history_files(built.run_id) == before
    assert [p.read_bytes() for p in before] == contents


def test_a_run_that_is_already_attached_keeps_its_mode_unless_asked(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify attaching again without a mode flag keeps the recorded mode, and a flag changes only that."""
    built = elsewhere()
    assert invoke("attach", str(built.root), "--read-only").exit_code == 0

    again = invoke("attach", str(built.root))
    assert again.exit_code == 0
    record = tracked(built.run_id)
    assert record is not None
    assert record.access is Access.read_only

    changed = invoke("attach", str(built.root), "--own")

    assert changed.exit_code == 0, changed.output
    record = tracked(built.run_id)
    assert record is not None
    assert record.access is Access.owned
    assert record.output_path == built.root
    assert record.attached


def test_the_run_id_of_another_run_is_refused(
    elsewhere: t.Callable[..., BuiltRun],
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a run-id tracked for another run is refused, naming both and the way out."""
    first = elsewhere("same-name")
    assert invoke("attach", str(first.root)).exit_code == 0
    second = elsewhere("same-name-copy")
    manifest = read_run_manifest(second.root)
    assert manifest is not None
    manifest.run_id = "same-name"
    write_run_manifest(second.root, manifest)

    result = invoke("attach", str(second.root))

    assert result.exit_code == EXIT_BAD_ARGS
    text = flatten_cli_output(result.output + result.stderr)
    assert run_uri(first.uuid) in text
    assert run_uri(second.uuid) in text
    assert "--as" in text
    record = tracked("same-name")
    assert record is not None
    assert record.output_path == first.root


def test_as_attaches_the_run_under_another_run_id(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify `--as` records the run under the run-id given, and its uuid finds it."""
    first = elsewhere("same-name")
    assert invoke("attach", str(first.root)).exit_code == 0
    second = elsewhere("same-name-copy")
    manifest = read_run_manifest(second.root)
    assert manifest is not None
    manifest.run_id = "same-name"
    write_run_manifest(second.root, manifest)

    result = invoke("attach", str(second.root), "--as", "Other Id")

    assert result.exit_code == 0, result.output
    record = tracked("other-id")
    assert record is not None
    assert record.output_path == second.root
    assert record.metadata[KEY_RUN_UUID] == second.uuid
    found = resolve_run(parse_run_handle(run_uri(second.uuid)))
    assert isinstance(found, WorkplanRun)
    assert found.run_id == "other-id"
    first_record = tracked("same-name")
    assert first_record is not None
    assert first_record.output_path == first.root


def test_as_naming_the_run_id_of_a_third_run_is_refused(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify `--as` is refused when it names a record of another run."""
    first = elsewhere("first")
    second = elsewhere("second")
    assert invoke("attach", str(first.root)).exit_code == 0

    result = invoke("attach", str(second.root), "--as", "first")

    assert result.exit_code == EXIT_BAD_ARGS
    assert tracked("second") is None
    record = tracked("first")
    assert record is not None
    assert record.output_path == first.root


def test_a_record_of_unknown_uuid_is_refused(
    elsewhere: t.Callable[..., BuiltRun], tmp_path: Path
) -> None:
    """Verify a record that predates uuids, whose directory is gone, is not taken over."""
    first = elsewhere("old-record")
    assert invoke("attach", str(first.root)).exit_code == 0

    def forget(run: WorkplanRun) -> None:
        run.metadata.pop(KEY_RUN_UUID)
        run.output_path = tmp_path / "gone"

    TrackingRepository().update_workplan_run_sync("old-record", forget)

    result = invoke("attach", str(first.root))

    assert result.exit_code == EXIT_BAD_ARGS
    assert "unknown uuid" in " ".join((result.output + result.stderr).split())


def test_a_run_elsewhere_that_i_own_is_owned(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify a run in a directory of the user's own, outside the project, is owned."""
    built = elsewhere()

    assert default_access(built.root) is Access.owned
    assert invoke("attach", str(built.root)).exit_code == 0
    record = tracked(built.run_id)
    assert record is not None
    assert record.access is Access.owned


def test_a_run_in_the_project_area_is_read_only(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a run in the project's runs area is attached read-only, and says so."""
    built = elsewhere("shared-run", dest=project_runs / "shared-run")

    result = invoke("attach", str(built.root))

    assert result.exit_code == 0, result.output
    record = tracked(built.run_id)
    assert record is not None
    assert record.access is Access.read_only
    assert "as read-only" in flatten_cli_output(result.output)
    assert "Nothing will be written into it" in flatten_cli_output(result.output)


def test_a_run_of_another_owner_is_read_only(
    elsewhere: t.Callable[..., BuiltRun], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a run in a directory another user owns is attached read-only."""
    built = elsewhere()
    monkeypatch.setattr(os, "geteuid", lambda: built.root.stat().st_uid + 1)

    result = invoke("attach", str(built.root))

    assert result.exit_code == 0, result.output
    record = tracked(built.run_id)
    assert record is not None
    assert record.access is Access.read_only


def test_own_overrides_the_default(
    elsewhere: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify `--own` takes a run in the project area as the user's."""
    built = elsewhere("shared-run", dest=project_runs / "shared-run")

    result = invoke("attach", str(built.root), "--own")

    assert result.exit_code == 0, result.output
    record = tracked(built.run_id)
    assert record is not None
    assert record.access is Access.owned
    assert "Nothing will be written" not in result.output


def test_read_only_overrides_the_default(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify `--read-only` takes a run of the user's own as read-only."""
    built = elsewhere()

    result = invoke("attach", str(built.root), "--read-only")

    assert result.exit_code == 0, result.output
    record = tracked(built.run_id)
    assert record is not None
    assert record.access is Access.read_only


def test_both_access_flags_are_refused(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify `--own` with `--read-only` is a bad argument and writes nothing."""
    built = elsewhere()

    result = invoke("attach", str(built.root), "--own", "--read-only")

    assert result.exit_code == EXIT_BAD_ARGS
    assert tracked(built.run_id) is None


def test_verify_reports_a_digest_mismatch_and_still_attaches(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify a product whose recorded sha256 differs is a problem (exit 1) that does not block the attach."""
    built = elsewhere(sha256="0" * 64)

    plain = invoke("attach", str(built.root), "--dry-run")
    verified = invoke("attach", str(built.root), "--verify")

    assert plain.exit_code == 0, plain.output
    assert verified.exit_code == 1
    assert "differs from the record" in verified.output
    summary = "Compared 1 product(s) with their recorded digest: 0 matched, 1 differed."
    assert summary in verified.output
    assert "Compared" not in plain.output
    written = tracked(built.run_id)
    assert written is not None
    assert written.output_path == built.root


def test_the_authored_workplan_is_the_manifests(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify `workplan_path` is the one the manifest records, however the run's
    transformed workplan is named, and that attach stores neither.
    """
    built = elsewhere()
    authored = "/where/it/was/authored/plan.yaml"
    manifest = read_run_manifest(built.root)
    assert manifest is not None
    manifest.workplan = WorkplanIdentity(name="plan", content_hash="", path=authored)
    write_run_manifest(built.root, manifest)

    result = invoke("attach", str(built.root))

    assert result.exit_code == 0, result.output
    record = tracked(built.run_id)
    assert record is not None
    assert record.workplan_path == Path(authored)
    # the run holds one transformed workplan, under the run-id's name rather than plan's
    assert record.trx_workplan_path == built.root / f"{built.run_id}_transformed.yaml"
    stored = yaml.safe_load(tracked_path(built.run_id).read_text())
    assert "workplan_path" not in stored


def test_a_yml_transformed_workplan_of_cstar_0_17_is_found(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify a run whose plan 0.17 named `<stem>_transformed.yml` attaches, and
    its record answers with that plan.
    """
    built = elsewhere()
    plan = built.root / f"{built.run_id}_transformed.yaml"
    yml = plan.rename(plan.with_suffix(".yml"))

    result = invoke("attach", str(built.root))

    assert result.exit_code == 0, result.output
    record = tracked(built.run_id)
    assert record is not None
    assert record.trx_workplan_path == yml


def test_attached_runs_are_known_by_their_run_id(
    elsewhere: t.Callable[..., BuiltRun],
) -> None:
    """Verify a verb that takes a run-id finds the run once it is attached."""
    built = elsewhere()
    before = invoke("path", built.run_id)
    assert before.exit_code == EXIT_BAD_ARGS

    assert invoke("attach", str(built.root)).exit_code == 0

    after = invoke("path", built.run_id)
    assert after.exit_code == 0, after.output
    assert after.stdout == f"{built.root}\n"
