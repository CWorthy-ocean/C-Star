import asyncio
import logging
import os
import threading
import time
import typing as t
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock
from uuid import uuid4

import pytest
import yaml

from cstar.base.env import ENV_CSTAR_STATE_HOME
from cstar.base.exceptions import CstarError
from cstar.base.utils import slugify
from cstar.execution.file_system import StateDirectoryManager
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    Attempt,
    RunManifest,
    StepEntry,
    StepRecord,
    WorkplanIdentity,
    read_run_manifest,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import (
    LEGACY_TRANSFORMED_NAME_SUFFIX,
    BlueprintIdentity,
    Step,
    Workplan,
)
from cstar.orchestration.orchestration import (
    LiveStep,
    LiveWorkplan,
    ProcessHandle,
    Status,
)
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.state import StateRepository
from cstar.orchestration.tracking import (
    KEY_RUN_UUID,
    UNKNOWN_SIZE,
    Access,
    TrackingRepository,
    WorkplanRun,
    measure_step_sizes,
    require_writable,
)

if t.TYPE_CHECKING:
    from cstar.orchestration.index import RunRecord


@pytest.mark.asyncio
async def test_tracking_create(tmp_path: Path) -> None:
    """Verify that run-tracking writes to the expected location.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    """
    state_dir = tmp_path / "state"
    output_path = tmp_path / "output"
    run_id = "test-tracking-create-run-id"

    repo = TrackingRepository()
    wp_run = WorkplanRun(
        output_path=output_path,
        run_id=run_id,
    )

    with mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}):
        latest_dir = repo.latest_dir
        history_dir = repo.history_dir
        latest_link = repo.latest_path(run_id)

        persisted_to = await repo.put_workplan_run(wp_run)

    # confirm the record is persisted
    assert persisted_to.exists()

    # confirm the history path is returned (not the path to the latest use of the run-id)
    assert "latest" not in persisted_to.as_posix()

    # confirm latest and history are not overlapping
    assert latest_dir.as_posix() != history_dir.as_posix()

    # confirm the `put` operation saves to both the latest and run-tracking history
    found_files = list(latest_dir.rglob("*.yaml"))
    assert found_files

    # the latest record is a link into the history, relative so the state
    # directory can be relocated as a unit
    assert latest_link.is_symlink()
    assert not latest_link.readlink().is_absolute()
    assert latest_link.resolve() == persisted_to.resolve()

    found_files = list(history_dir.rglob("*.yaml"))
    assert found_files


@pytest.mark.parametrize(
    "run_id",
    [
        "",
        " ",
        "\t \n",
    ],
)
@pytest.mark.asyncio
async def test_tracking_retrieve_no_runid(run_id: str) -> None:
    """Verify that run-tracking reports an invalid run-id.

    Parameters
    ----------
    run_id : str
        Parameterized "bad values" for the run-id that should result in exceptions.
    """
    repo = TrackingRepository()

    with pytest.raises(ValueError, match="run-id was not provided"):
        await repo.get_workplan_run(run_id=run_id)


@pytest.mark.asyncio
async def test_tracking_retrieve(tmp_path: Path) -> None:
    """Verify that run-tracking retrieves a persisted record and deserializes it
    as expected.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    """
    output_path = tmp_path / "output"
    run_id = "test-tracking-retrieve-run-id"
    recorded_at = datetime.now(tz=UTC)
    captured_env = {"foo": "foo-value"}

    wp_run = WorkplanRun(
        output_path=output_path,
        run_id=run_id,
        recorded_at=recorded_at,
        environment=captured_env,
    )

    repo = TrackingRepository()
    _ = await repo.put_workplan_run(wp_run)

    # use public API to retrieve using "latest" record (by passing only run_id)
    latest = await repo.get_workplan_run(run_id=run_id)
    assert latest
    assert latest.__dict__ == wp_run.__dict__  # confirm same stored values

    # use public API to retrieve using "history" record (by passing run_id & run_date)
    history = await repo.get_workplan_run(run_id=run_id, run_date=wp_run.recorded_at)
    assert history
    assert history.__dict__ == wp_run.__dict__  # confirm same stored values

    assert history.output_path == output_path
    assert history.run_id == run_id
    assert history.recorded_at == recorded_at
    assert history.environment == captured_env


@pytest.mark.asyncio
async def test_tracking_retrieve_variant(
    tmp_path: Path,
) -> None:
    """Verify that run-tracking retrieves a persisted record from the `history`
    if start date is supplied and from `latest` if start date is not supplied.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    # single_step_workplan: Workplan
    #     Fixture returning a simple workplan
    """
    state_dir = tmp_path / "state"
    output_path = tmp_path / "output"
    run_id = "test-tracking-retrieve-run-id"

    repo = TrackingRepository()
    wp_run = WorkplanRun(
        output_path=output_path,
        run_id=run_id,
    )

    # manually serialize so i have access to a known, working path for mocks
    mock_doc_path = tmp_path / repo._runfile_name(datetime.now())  # type: ignore
    assert serialize(mock_doc_path, wp_run)

    with (
        mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}),
        mock.patch.object(repo, "history_path", return_value=mock_doc_path) as hp_fn,
        mock.patch.object(repo, "latest_path", return_value=mock_doc_path) as lp_fn,
    ):
        latest = await repo.get_workplan_run(wp_run.run_id, None)
        assert latest

        # confirm the latest search was used
        lp_fn.assert_called()
        hp_fn.assert_not_called()

        history = await repo.get_workplan_run(wp_run.run_id, wp_run.recorded_at)
        assert history

        # confirm the history search was used
        hp_fn.assert_called_once()


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
async def test_default_run_id(
    wp_templates_dir: Path,
) -> None:
    """Verify the default run id matches the workplan safe name.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    wp_templates_dir : Path
        Fixture returning the path to the directory containing workplan template files
    """
    wp_path = wp_templates_dir / "workplan.yaml"

    # load the sample workplan from disk to verify the default name provenance
    wp = deserialize(wp_path, Workplan)

    # generate the default run id from the source path
    run_id = WorkplanRun.get_default_run_id(wp_path.as_posix())

    # verify it matches the supplied workplan; a valid name is its own slug
    assert run_id == slugify(wp.name)
    assert run_id == wp.name


@pytest.mark.usefixtures("read_yaml_intercept")
def test_default_run_id_rejects_non_slug_name(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify a workplan whose name is not a slug yields no default run id and
    the error names the slug to use.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    wp_templates_dir : Path
        Fixture returning the path to the directory containing workplan template files
    """
    content = (wp_templates_dir / "workplan.yaml").read_text()
    wp_path = tmp_path / "workplan.yaml"
    wp_path.write_text(
        content.replace("name: sample-workplan", "name: Sample Workplan")
    )

    with pytest.raises(ValueError, match="use 'sample-workplan'"):
        WorkplanRun.get_default_run_id(wp_path.as_posix())


@pytest.mark.parametrize(
    "persisted",
    ["sample-workplan", f"Sample Workplan{LEGACY_TRANSFORMED_NAME_SUFFIX}"],
)
def test_default_run_id_of_a_transformed_workplan(
    tmp_path: Path,
    fake_blueprint_path: Path,
    persisted: str,
) -> None:
    """Verify the transformed workplan a run persists, which a plain `Workplan`
    rejects for its step `working_dir`, yields the run-id of that run, also
    under the name an earlier release gave it.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    persisted : str
        The name as written into the transformed workplan
    """
    step = LiveStep.from_step(
        Step(name="a", application="hello_world", blueprint=fake_blueprint_path)
    )
    trx_path = tmp_path / "workplan_transformed.yaml"
    live = LiveWorkplan(name="sample-workplan", description="d", steps=[step])
    assert serialize(trx_path, live), "serializing live workplan failed in test"
    document = yaml.safe_load(trx_path.read_text()) | {"name": persisted}
    trx_path.write_text(yaml.safe_dump(document))

    assert WorkplanRun.get_default_run_id(trx_path.as_posix()) == "sample-workplan"


def test_default_run_id_of_a_transformed_workplan_in_a_run_directory(
    fake_blueprint_path: Path,
) -> None:
    """Verify a transformed workplan inside a run directory yields that run's
    id, which an explicit `--run-id` may have set to something other than the
    workplan name.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    step = LiveStep.from_step(
        Step(name="a", application="hello_world", blueprint=fake_blueprint_path)
    )
    run_dir = StateDirectoryManager.data_dir("custom-id")
    run_dir.mkdir(parents=True)
    trx_path = run_dir / "workplan_transformed.yaml"
    live = LiveWorkplan(name="sample-workplan", description="d", steps=[step])
    assert serialize(trx_path, live), "serializing live workplan failed in test"

    assert WorkplanRun.get_default_run_id(trx_path.as_posix()) == "custom-id"


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
async def test_default_run_id_remote(
    remote_workplan_uri: str,
) -> None:
    """Verify the default run id matches the workplan safe name when
    the workplan is a remote resource.

    Parameters
    ----------
    remote_workplan_uri : str
        A fixture returning a known URI to a sample workplan
    """
    expected_name = "sample-workplan"

    # generate the default run id from the source URI
    run_id = WorkplanRun.get_default_run_id(remote_workplan_uri)

    # verify it matches the supplied workplan
    assert run_id == slugify(expected_name)


@pytest.mark.parametrize("num_runs", [1, 2, 4, 8])
@pytest.mark.asyncio
async def test_tracking_list_latest_run_id(tmp_path: Path, num_runs: int) -> None:
    """Verify that using the `list_latest_runs` filter finds the
    expected set of persisted records.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    """
    state_dir = tmp_path / "state"
    output_path = tmp_path / "output"
    base_run_id = "test-tracking-list-latest-run-id"
    captured_env = {"foo": "foo-value"}

    with mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}):
        repo = TrackingRepository()

        # insert some runs to test filtering...
        run_ids: list[str] = []

        for i in range(num_runs):
            current_run_id = f"{base_run_id}-{i}"
            run_ids.append(current_run_id)

            _ = await repo.put_workplan_run(
                WorkplanRun(
                    output_path=output_path,
                    run_id=current_run_id,
                    environment=captured_env,
                )
            )

        # confirm per-run-id retrieval meets expectations
        for run_id in set(run_ids):
            wp_runs = await repo.list_latest_runs(run_id)

            # confirm each unique run-id returns a list
            assert wp_runs

            # confirm only the desired, exact run is returned
            assert len(wp_runs) == 1

        # confirm that a non-exact search finds all the expected things
        base_matches = await repo.list_latest_runs(base_run_id)

        # more than one run should be found when using base-run-id as filter
        assert len(base_matches) == num_runs


@pytest.mark.parametrize(
    ("num_runs", "num_unique"), [(2, 1), (2, 2), (2, 3), (3, 3), (4, 2)]
)
@pytest.mark.asyncio
async def test_tracking_list_history_run_id(
    tmp_path: Path, num_runs: int, num_unique: int
) -> None:
    """Verify that using the `list_history_runs` filter finds the
    expected set of persisted records when a run-id is re-used.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    num_runs : int
        The number of runs to insert for a given run id
    num_unique : int
        The number of unique run ids to generate
    """
    state_dir = tmp_path / "state"
    output_path = tmp_path / "output"
    base_run_id = "test-tracking-list-history-run-id"
    captured_env = {"foo": "foo-value"}

    with mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}):
        repo = TrackingRepository()
        coros = []

        # insert some run-ids that won't conform to the search and should be omitted
        for _ in range(3):
            coros.append(
                repo.put_workplan_run(
                    WorkplanRun(
                        output_path=output_path,
                        run_id=str(uuid4()),
                        environment=captured_env,
                    )
                )
            )

        await asyncio.gather(*coros)
        target_run_ids = [f"{base_run_id}-{i}" for i in range(num_unique)]

        # NOTE: this is NOT thread-safe and assumes a single user env
        for current_run_id in target_run_ids:
            # insert some runs to test filtering...
            for _ in range(num_runs):
                await repo.put_workplan_run(
                    WorkplanRun(
                        output_path=output_path,
                        run_id=current_run_id,
                        environment=captured_env,
                    )
                )

        # run a new loop to ensure multiple unique IDs have been inserted
        # and a bug can present itself
        for current_run_id in target_run_ids:
            # confirm that re-using a run-id does not increase the number of "latest" runs
            wp_runs = await repo.list_latest_runs(current_run_id)

            # confirm that the run-id was re-used
            assert len(wp_runs) == 1

        # confirm that a search finds all the individual runs
        base_matches = await repo.list_history_runs(base_run_id)

        # confirm 2 separate run-ids had the same number of runs inserted.
        assert len(base_matches) == num_unique * num_runs


@pytest.mark.parametrize(
    ("num_runs", "all_history", "exp_num_paths"),
    [
        (1, True, 1),
        (1, False, 1),
        (2, True, 2),
        (2, False, 1),
        (4, True, 4),
        (4, False, 1),
    ],
)
@pytest.mark.asyncio
async def test_tracking_get_run_paths(
    tmp_path: Path,
    num_runs: int,
    all_history: bool,
    exp_num_paths: int,
) -> None:
    """Verify that using the `get_run_paths` method locates
    all directories for the specified run-id

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    num_runs : int
        The number of runs to insert for a given run id
    all_history : bool
        Pass `True` to test retrieval of all historical run paths.
    """
    output_path = tmp_path / "output"
    run_id = "test-get-run-paths-run-id"
    captured_env = {"foo": "foo-value"}

    repo = TrackingRepository()
    expected_paths = [repo.latest_path(run_id=run_id)]  # type: ignore

    # insert some runs that re-use the same run-id
    record_times = [datetime(2020, 1, 1 + i, tzinfo=UTC) for i in range(num_runs)]
    inserts = [
        repo.put_workplan_run(
            WorkplanRun(
                output_path=output_path,
                run_id=run_id,
                environment=captured_env,
                recorded_at=recorded_at,
            )
        )
        for recorded_at in record_times
    ]
    run_paths = await asyncio.gather(*inserts)
    expected_paths.extend(run_paths)

    # retrieve the list of run paths
    actual_paths = repo.list_runtracking_paths(run_id, all_history)

    # confirm the latest symlink is included
    actual_latest = repo.latest_path(run_id)

    # confirm the results are de-duped to only the history paths
    latest_paths = [p for p in actual_paths if "latest" in str(p)]
    assert len(latest_paths) == 0
    assert actual_latest not in set(actual_paths)

    # get the history paths
    history = set(p for p in actual_paths if "history" in str(p))

    assert len(history) == exp_num_paths


@pytest.mark.asyncio
async def test_tracking_retrieve_mixed_case_runid(tmp_path: Path) -> None:
    """Verify a mixed-case run-id query resolves to the slugified run record.

    Run records are persisted with slugified (lowercase) run-ids; the lookup
    must normalize the same way so `cstar run status MYRUN` finds the
    record created for `myrun`.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    """
    wp_run = WorkplanRun(
        output_path=tmp_path / "output",
        run_id="mixed-case-run-id",
    )

    repo = TrackingRepository()
    _ = await repo.put_workplan_run(wp_run)

    found = await repo.get_workplan_run(run_id="MIXED-Case-Run-ID")
    assert found
    assert found.run_id == "mixed-case-run-id"

    found_sync = repo.get_workplan_run_sync(run_id="MIXED-Case-Run-ID")
    assert found_sync
    assert found_sync.run_id == "mixed-case-run-id"


def _make_workplan_run(tmp_path: Path, run_id: str, **overrides) -> WorkplanRun:
    """Build a minimal `WorkplanRun` for use in tests.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory used to build placeholder paths.
    run_id : str
        The run-id to assign to the record.
    """
    defaults: dict = {
        "output_path": tmp_path / "output",
        "run_id": run_id,
    }
    defaults.update(overrides)
    return WorkplanRun(**defaults)


def test_workplanrun_size_mb_unknown_when_empty(tmp_path: Path) -> None:
    """Verify `size_mb` reports `UNKNOWN_SIZE` when no steps have been measured."""
    run = _make_workplan_run(tmp_path, "size-mb-empty")

    assert run.size_mb == UNKNOWN_SIZE


def test_workplanrun_size_mb_sums_step_sizes(tmp_path: Path) -> None:
    """Verify `size_mb` sums all recorded per-step sizes."""
    run = _make_workplan_run(
        tmp_path, "size-mb-sum", step_sizes={"step-a": 3, "step-b": 5}
    )

    assert run.size_mb == 8


@pytest.mark.asyncio
async def test_measure_step_sizes_stores_successes_and_skips_failures(
    tmp_path: Path,
) -> None:
    """Verify successful measurements are stored, failures leave prior values
    untouched, and `size_measured_at` is set once measurement is attempted.
    """
    run = _make_workplan_run(
        tmp_path, "measure-step-sizes", step_sizes={"stale-step": 99}
    )
    step_dirs = {
        "step-a": tmp_path / "step-a",
        "step-b": tmp_path / "step-b",
        "stale-step": tmp_path / "stale-step",
    }

    async def fake_step_disk_usage(step_root: Path) -> int:
        return UNKNOWN_SIZE if step_root.name != "step-a" else 7

    with mock.patch(
        "cstar.orchestration.tracking.step_disk_usage",
        side_effect=fake_step_disk_usage,
    ):
        measured = await measure_step_sizes(run, step_dirs, asyncio.Semaphore(2))

    assert measured == {"step-a": 7}
    assert run.step_sizes["step-a"] == 7
    assert "step-b" not in run.step_sizes
    # a failed re-measurement leaves the previously recorded value intact
    assert run.step_sizes["stale-step"] == 99
    assert run.size_measured_at is not None


@pytest.mark.asyncio
async def test_measure_step_sizes_no_steps_leaves_measured_at_unset(
    tmp_path: Path,
) -> None:
    """Verify that measuring an empty set of steps does not set `size_measured_at`."""
    run = _make_workplan_run(tmp_path, "measure-step-sizes-empty")

    await measure_step_sizes(run, {}, asyncio.Semaphore(2))

    assert run.size_measured_at is None
    assert run.step_sizes == {}


@pytest.mark.asyncio
async def test_tracking_list_latest_runs_skips_corrupt_record(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a corrupt run record is skipped, with exactly one collapsed warning.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    caplog : pytest.LogCaptureFixture
        Fixture used to capture log output
    """
    state_dir = tmp_path / "state"

    with mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}):
        repo = TrackingRepository()

        good_run = _make_workplan_run(tmp_path, "good-run")
        await repo.put_workplan_run(good_run)

        corrupt_path = repo.latest_dir / "corrupt-run.yaml"
        corrupt_path.write_text("{}\n")

        with caplog.at_level(logging.WARNING):
            runs = await repo.list_latest_runs()

    assert [r.run_id for r in runs] == ["good-run"]

    warnings = [rec for rec in caplog.records if rec.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "corrupt-run.yaml" in warnings[0].message
    assert "1 run record(s)" in warnings[0].message


@pytest.mark.asyncio
async def test_tracking_bound_caps_concurrency(tmp_path: Path) -> None:
    """Verify that `bound(limit)` caps the number of concurrent reads performed
    by `list_latest_runs`.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    """
    limit = 2
    lock = threading.Lock()
    state = {"in_flight": 0, "peak": 0}

    def fake_try_deserialize(path, klass, mode=None):
        with lock:
            state["in_flight"] += 1
            state["peak"] = max(state["peak"], state["in_flight"])
        time.sleep(0.05)
        with lock:
            state["in_flight"] -= 1

    state_dir = tmp_path / "state"

    with mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}):
        async with TrackingRepository.bound(limit) as repo:
            for i in range(6):
                (repo.latest_dir / f"run-{i}.yaml").touch()

            with mock.patch(
                "cstar.orchestration.serialization.try_deserialize",
                side_effect=fake_try_deserialize,
            ):
                await repo.list_latest_runs()

    assert state["peak"] <= limit


@pytest.mark.asyncio
async def test_tracking_update_workplan_run_is_atomic_under_concurrency(
    tmp_path: Path,
) -> None:
    """Verify concurrent updates to one run each land, instead of the last
    writer discarding the others' changes.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    """
    state_dir = tmp_path / "state"

    with mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}):
        repo = TrackingRepository()
        run = _make_workplan_run(tmp_path, "atomic-update")
        await repo.put_workplan_run(run)

        def _add(name: str, size: int) -> t.Callable[[WorkplanRun], None]:
            def _mutate(latest: WorkplanRun) -> None:
                latest.record_step_sizes({name: size})
                latest.sentinels.add(tmp_path / f"{name}.sentinel")

            return _mutate

        await asyncio.gather(
            *(
                repo.update_workplan_run("atomic-update", _add(f"step-{i}", i))
                for i in range(8)
            )
        )

        persisted = await repo.get_workplan_run("atomic-update")

    assert persisted is not None
    assert persisted.step_sizes == {f"step-{i}": i for i in range(8)}
    assert len(persisted.sentinels) == 8


@pytest.mark.asyncio
async def test_tracking_update_workplan_run_unknown_run_returns_none(
    tmp_path: Path,
) -> None:
    """Verify updating a run-id with no record returns `None` without writing."""
    state_dir = tmp_path / "state"

    with mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}):
        repo = TrackingRepository()
        result = await repo.update_workplan_run("never-ran", lambda r: None)

    assert result is None


def _record(run_id: str, output_path: Path) -> WorkplanRun:
    """Build a record of the run in `output_path`."""
    return WorkplanRun(output_path=output_path, run_id=run_id)


@pytest.mark.asyncio
async def test_tracking_follows_a_run_whose_directory_moved(tmp_path: Path) -> None:
    """Verify a record naming a run directory that is gone is followed to the
    directory the current data home keeps for the run-id, wherever the record is read.
    """
    run_id = "moved-run"
    recorded = tmp_path / "old-data" / "workplan_runs" / run_id
    current = StateDirectoryManager.data_dir(run_id)
    _write_manifest(current, run_id, workplan_path=tmp_path / "workplan.yaml")
    (current / "workplan_transformed.yaml").touch()
    repo = TrackingRepository()
    await repo.put_workplan_run(_record(run_id, recorded))

    found = await repo.get_workplan_run(run_id)
    listed = await repo.list_latest_runs()

    assert found is not None
    assert found.output_path == current
    assert found.trx_workplan_path == current / "workplan_transformed.yaml"
    assert found.workplan_path == tmp_path / "workplan.yaml"
    assert [r.output_path for r in listed] == [current]

    # an update persists the followed paths
    updated = await repo.update_workplan_run(run_id, lambda run: None)
    assert updated is not None
    assert deserialize(repo.latest_path(run_id), WorkplanRun).output_path == current


@pytest.mark.asyncio
async def test_tracking_keeps_a_run_whose_directory_exists(tmp_path: Path) -> None:
    """Verify a record naming an existing run directory is left alone, even when the
    current data home also holds a directory for the run-id.
    """
    run_id = "kept-run"
    recorded = tmp_path / "other-data" / "workplan_runs" / run_id
    _write_manifest(recorded, run_id, workplan_path=tmp_path / "workplan.yaml")
    (recorded / "workplan_transformed.yaml").touch()
    StateDirectoryManager.data_dir(run_id).mkdir(parents=True)
    repo = TrackingRepository()
    await repo.put_workplan_run(_record(run_id, recorded))

    found = await repo.get_workplan_run(run_id)

    assert found is not None
    assert found.output_path == recorded
    assert found.trx_workplan_path == recorded / "workplan_transformed.yaml"


@pytest.mark.asyncio
async def test_tracking_keeps_a_run_it_cannot_inspect(tmp_path: Path) -> None:
    """Verify a record whose directories cannot be probed is returned as recorded,
    rather than failing the read.
    """
    run_id = "unreadable-run"
    recorded = tmp_path / "old-data" / "workplan_runs" / run_id
    StateDirectoryManager.data_dir(run_id).mkdir(parents=True)
    repo = TrackingRepository()
    await repo.put_workplan_run(_record(run_id, recorded))

    real_exists = Path.exists

    def exists(self: Path, **kwargs: t.Any) -> bool:
        """Deny only the recorded run directory, as a permission error would."""
        if self == recorded:
            raise PermissionError("denied")
        return real_exists(self, **kwargs)

    with mock.patch.object(Path, "exists", exists):
        found = await repo.get_workplan_run(run_id)
        listed = await repo.list_latest_runs()

    assert found is not None
    assert found.output_path == recorded
    assert [r.output_path for r in listed] == [recorded]


@pytest.mark.asyncio
async def test_tracking_lists_runs_following_them_off_the_event_loop(
    tmp_path: Path,
) -> None:
    """Verify listing records follows moved runs in the worker threads that read them."""
    run_id = "listed-run"
    StateDirectoryManager.data_dir(run_id).mkdir(parents=True)
    repo = TrackingRepository()
    await repo.put_workplan_run(
        _record(run_id, tmp_path / "old-data" / "workplan_runs" / run_id)
    )
    threads: list[threading.Thread] = []
    reanchored = WorkplanRun.reanchored

    def spy(self: WorkplanRun) -> WorkplanRun:
        threads.append(threading.current_thread())
        return reanchored(self)

    with mock.patch.object(WorkplanRun, "reanchored", spy):
        listed = await repo.list_latest_runs()

    assert [r.output_path for r in listed] == [StateDirectoryManager.data_dir(run_id)]
    assert threads
    assert threading.main_thread() not in threads


MANIFEST_START = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
"""The start of the attempt `_write_manifest` records."""


def _write_manifest(
    root: Path, run_id: str, *, workplan_path: Path | str | None = None
) -> RunManifest:
    """Write the manifest of a one-step run into `root`.

    The run is named `run_id`, started at `MANIFEST_START`, and made from the
    workplan at `workplan_path` when one is given.
    """
    manifest = RunManifest(
        run_id=run_id,
        name=run_id,
        kind="workplan",
        created_at=MANIFEST_START,
        attempts=[Attempt(start_at=MANIFEST_START)],
        workplan=(
            WorkplanIdentity(name=run_id, content_hash="", path=str(workplan_path))
            if workplan_path is not None
            else None
        ),
        steps=[
            StepEntry(name="one", application="hello_world", working_dir="tasks/one")
        ],
    )
    root.mkdir(parents=True, exist_ok=True)
    write_run_manifest(root, manifest)
    return manifest


def test_workplanrun_uuid_is_the_cached_one(tmp_path: Path) -> None:
    """Verify the uuid the run was stamped with is used without reading the manifest."""
    manifest = _write_manifest(tmp_path / "output", "uuid-cached")
    stamped = str(uuid4())
    run = _make_workplan_run(tmp_path, "uuid-cached", metadata={KEY_RUN_UUID: stamped})

    with mock.patch("cstar.orchestration.tracking.read_run_manifest") as read:
        assert run.uuid == stamped

    read.assert_not_called()
    assert stamped != manifest.id


def test_workplanrun_uuid_falls_back_to_the_manifest(tmp_path: Path) -> None:
    """Verify a run recorded before the uuid was stamped takes it from its manifest."""
    manifest = _write_manifest(tmp_path / "output", "uuid-fallback")
    run = _make_workplan_run(tmp_path, "uuid-fallback")

    assert KEY_RUN_UUID not in run.metadata
    assert run.uuid == manifest.id


def test_workplanrun_uuid_is_empty_without_a_readable_manifest(
    tmp_path: Path,
) -> None:
    """Verify a run with no manifest, or one that cannot be read, has no uuid."""
    run = _make_workplan_run(tmp_path, "uuid-none")
    assert run.uuid == ""

    (tmp_path / "output").mkdir()
    (tmp_path / "output" / "cstar-run.yaml").write_text("run_id: [unclosed\n")
    assert run.uuid == ""


def test_workplanrun_step_status_reads_the_sentinel(tmp_path: Path) -> None:
    """Verify a step's status is the one its sentinel holds, and empty without one."""
    run = _make_workplan_run(tmp_path, "step-status")
    path = StateRepository.sentinel_path("Spin Up", run_id="step-status")
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = ProcessHandle(
        pid="1", name="Spin Up", run_id="step-status", status=Status.Done
    )
    assert serialize(path, handle)

    assert run.step_status("Spin Up") == "Done"
    assert run.step_status("no-such-step") == ""
    assert run.step_status("") == ""


def test_workplanrun_is_a_run_record(tmp_path: Path) -> None:
    """Verify (for the type checker, too) that tracking records are run records."""
    record: RunRecord = _make_workplan_run(tmp_path, "is-a-record")

    assert record.run_id == "is-a-record"


def _step_record(outcome: str = "completed") -> StepRecord:
    """A record of the step `_write_manifest` names, with the given outcome."""
    return StepRecord(
        step="one",
        application="hello_world",
        blueprint=BlueprintIdentity(
            kind="Blueprint", application="hello_world", name="bp", content_hash="abc"
        ),
        outcome=outcome,
        finished_at=datetime(2026, 10, 15, 12, 0, tzinfo=UTC),
    )


def test_workplanrun_is_not_attached_by_default(tmp_path: Path) -> None:
    """Verify a record that says nothing is one this machine scheduled, and owned."""
    run = _make_workplan_run(tmp_path, "scheduled-here")

    assert not run.attached
    assert run.access is Access.owned


@pytest.mark.parametrize("attached", [True, False])
def test_workplanrun_attached_is_the_flag_set(tmp_path: Path, attached: bool) -> None:
    """Verify the `attached` field is the flag the record was made with."""
    run = _make_workplan_run(tmp_path, "flagged", attached=attached)

    assert run.attached is attached


@pytest.mark.parametrize("access", list(Access))
def test_workplanrun_access_is_the_value_recorded(
    tmp_path: Path, access: Access
) -> None:
    """Verify the access a record holds is the one it reads back."""
    run = _make_workplan_run(tmp_path, "accessed", access=access)

    assert run.access is access


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spelling",
    [
        pytest.param({"access": "rw"}, id="typed"),
        pytest.param({"metadata": {"access": "rw"}}, id="legacy-metadata"),
    ],
)
async def test_workplanrun_unknown_access_is_read_as_read_only(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, spelling: dict
) -> None:
    """Verify a record whose access this release does not know loads as read-only
    with a warning, refuses writes, and stays in the listing.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    caplog : pytest.LogCaptureFixture
        Captures the warning.
    spelling : dict
        The record fields that carry the unknown access, typed or legacy.
    """
    repo = TrackingRepository()
    await repo.put_workplan_run(_make_workplan_run(tmp_path, "even-access"))
    odd = repo.latest_dir / "odd-access.yaml"
    odd.write_text(
        yaml.safe_dump(
            {"output_path": str(tmp_path), "run_id": "odd-access", **spelling}
        )
    )

    with caplog.at_level(logging.WARNING):
        loaded = deserialize(odd, WorkplanRun)

    assert loaded.access is Access.read_only
    assert "records an access this release does not know" in caplog.text
    assert "'odd-access'" in caplog.text
    assert "'rw'" in caplog.text
    with pytest.raises(CstarError, match="'odd-access' is attached read-only"):
        require_writable(loaded, action="gather")
    listed = await repo.list_latest_runs()

    assert sorted(r.run_id for r in listed) == ["even-access", "odd-access"]
    assert {r.run_id: r.access for r in listed}["odd-access"] is Access.read_only


def test_workplanrun_known_access_is_read_without_a_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify a record whose access is one of the known values warns of nothing.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    caplog : pytest.LogCaptureFixture
        Captures any warning.
    """
    with caplog.at_level(logging.WARNING):
        run = _make_workplan_run(tmp_path, "known-access", access="read-only")

    assert run.access is Access.read_only
    assert "does not know" not in caplog.text


def test_workplanrun_keeps_attachment_through_the_repository(tmp_path: Path) -> None:
    """Verify the typed fields ride through persistence and `reanchored`."""
    run = _make_workplan_run(
        tmp_path,
        "persisted",
        output_path=tmp_path,
        attached=True,
        access=Access.read_only,
    )
    repo = TrackingRepository()
    repo.put_workplan_run_sync(run)

    found = repo.get_workplan_run_sync("persisted")

    assert found is not None
    assert found.reanchored().attached
    assert found.reanchored().access is Access.read_only
    assert found.metadata == {}


def test_workplanrun_defaults_are_not_written(tmp_path: Path) -> None:
    """Verify an owned, scheduled-here record writes neither `attached` nor `access`,
    and an attached read-only one writes them as fields rather than metadata.
    """
    repo = TrackingRepository()
    plain = repo.put_workplan_run_sync(_make_workplan_run(tmp_path, "plain"))
    flagged = repo.put_workplan_run_sync(
        _make_workplan_run(tmp_path, "flagged", attached=True, access=Access.read_only)
    )

    plain_doc = yaml.safe_load(plain.read_text())
    flagged_doc = yaml.safe_load(flagged.read_text())

    assert "attached" not in plain_doc
    assert "access" not in plain_doc
    assert "legacy" not in plain_doc
    assert "legacy" not in flagged_doc
    assert flagged_doc["attached"] is True
    assert flagged_doc["access"] == "read-only"
    assert "attached" not in flagged_doc.get("metadata", {})


def test_require_writable_lets_an_owned_run_through(tmp_path: Path) -> None:
    """Verify a run with no access recorded, or an owned one, may be written into."""
    require_writable(_make_workplan_run(tmp_path, "mine"), action="gather")
    owned = _make_workplan_run(tmp_path, "mine-too", access=Access.owned)

    require_writable(owned, action="gather")


def test_require_writable_refuses_a_read_only_run(tmp_path: Path) -> None:
    """Verify the refusal names the run, the action, the directory and `--own`."""
    run = _make_workplan_run(tmp_path, "theirs", access=Access.read_only)

    with pytest.raises(CstarError, match="'theirs' is attached read-only") as raised:
        require_writable(run, action="--repair")

    message = str(raised.value)
    assert "--repair writes into" in message
    assert str(run.output_path) in message
    assert "--own" in message


def test_workplanrun_manifest_is_read_on_every_access(tmp_path: Path) -> None:
    """Verify the manifest is the one in the run directory now, not a kept one."""
    manifest = _write_manifest(tmp_path / "output", "manifest-read")
    run = _make_workplan_run(tmp_path, "manifest-read")

    assert run.manifest.id == manifest.id

    changed = manifest.model_copy(update={"name": "renamed"})
    write_run_manifest(tmp_path / "output", changed)

    assert run.manifest.name == "renamed"


def test_attached_workplanrun_manifest_is_read_once(tmp_path: Path) -> None:
    """Verify an attached run's manifest, which this machine never refreshes, is
    parsed once however many steps ask for their status.
    """
    root = tmp_path / "output"
    _write_manifest(root, "attached-once")
    write_step_record(root / "tasks" / "one", _step_record())
    run = _make_workplan_run(tmp_path, "attached-once", attached=True)

    with mock.patch(
        "cstar.orchestration.tracking.read_run_manifest", wraps=read_run_manifest
    ) as read:
        assert run.step_status("one") == "Done"
        assert run.step_status("one") == "Done"

    read.assert_called_once()


def test_scheduled_workplanrun_manifest_is_read_again(tmp_path: Path) -> None:
    """Verify a run scheduled here, whose manifest changes, is never kept."""
    _write_manifest(tmp_path / "output", "scheduled-again")
    run = _make_workplan_run(tmp_path, "scheduled-again")

    with mock.patch(
        "cstar.orchestration.tracking.read_run_manifest", wraps=read_run_manifest
    ) as read:
        run.manifest
        run.manifest

    assert read.call_count == 2


@pytest.mark.parametrize(("attached", "live"), [(False, True), (True, False)])
def test_workplanrun_is_live_unless_attached(
    tmp_path: Path, attached: bool, live: bool
) -> None:
    """Verify only an attached run is not live: its sentinels are elsewhere."""
    run = _make_workplan_run(tmp_path, "liveness", attached=attached)

    assert run.live is live


def test_workplanrun_manifest_that_is_missing_or_unreadable(tmp_path: Path) -> None:
    """Verify a run directory with no usable manifest raises a `CstarError`."""
    run = _make_workplan_run(tmp_path, "no-manifest")

    with pytest.raises(CstarError, match="'no-manifest' has no run manifest at"):
        run.manifest

    (tmp_path / "output").mkdir()
    (tmp_path / "output" / "cstar-run.yaml").write_text("run_id: [unclosed\n")
    with pytest.raises(CstarError, match="Unable to read the manifest"):
        run.manifest


def test_attached_workplanrun_step_status_reads_the_step_record(
    tmp_path: Path,
) -> None:
    """Verify an attached run, which has no sentinels, reports what its records say."""
    root = tmp_path / "output"
    _write_manifest(root, "attached-status")
    run = _make_workplan_run(tmp_path, "attached-status", attached=True)
    # a sentinel here would be a lie about another machine's run: it is not read
    path = StateRepository.sentinel_path("one", run_id="attached-status")
    path.parent.mkdir(parents=True, exist_ok=True)
    assert serialize(
        path,
        ProcessHandle(
            pid="1", name="one", run_id="attached-status", status=Status.Failed
        ),
    )

    assert run.step_status("one") == ""  # the snapshot holds no status, no record

    write_step_record(root / "tasks" / "one", _step_record())

    assert run.step_status("one") == "Done"
    assert run.step_status("no-such-step") == ""


def test_attached_workplanrun_step_status_without_a_manifest_is_empty(
    tmp_path: Path,
) -> None:
    """Verify an attached run whose manifest is gone has no step status, quietly."""
    run = _make_workplan_run(tmp_path, "attached-bare", attached=True)

    assert run.step_status("one") == ""


LEGACY_START = datetime(2026, 9, 1, 10, 30, 15, 123456, tzinfo=UTC)
"""When the records written by hand below say their run was triggered."""

LEGACY_KEYS = ("workplan_path", "trx_workplan_path", "start_at")
"""The top-level keys of a record written by cstar 0.17.0 that this release drops."""


def _write_legacy_record(
    repo: TrackingRepository, run_id: str, output_path: Path, **metadata: str
) -> Path:
    """Write a record in the shape cstar 0.17.0 wrote, where `latest_path` points.

    The record names the workplan, transformed workplan and start time itself,
    and holds the workplan name in its metadata beside `metadata`.
    """
    document = {
        "workplan_path": str(output_path.parent / "authored.yaml"),
        "trx_workplan_path": str(output_path / "legacy_transformed.yaml"),
        "output_path": str(output_path),
        "run_id": run_id,
        "start_at": LEGACY_START.isoformat(),
        "environment": {"FOO": "bar"},
        "metadata": {"name": "legacy-plan (transformed)", **metadata},
    }
    path = repo.latest_path(run_id)
    path.write_text(yaml.safe_dump(document))
    return path


def test_workplanrun_reads_a_record_written_by_cstar_0_17(tmp_path: Path) -> None:
    """Verify a 0.17.0 record loads, folding its metadata marks into the typed
    fields, keeping its identity as the fallback and its start time as `recorded_at`.
    """
    run_id = "legacy-attached"
    output_path = tmp_path / "output"
    repo = TrackingRepository()
    stamped = str(uuid4())
    _write_legacy_record(
        repo,
        run_id,
        output_path,
        uuid=stamped,
        attached="1",
        access="read-only",
    )

    found = repo.get_workplan_run_sync(run_id)

    assert found is not None
    assert found.attached is True
    assert found.live is False
    assert found.access is Access.read_only
    assert found.metadata == {KEY_RUN_UUID: stamped}
    assert found.recorded_at == LEGACY_START
    assert found.environment == {"FOO": "bar"}
    # no manifest in the run directory: the record's own words are used
    assert found.name == "legacy-plan"
    assert found.start_at == LEGACY_START
    assert found.workplan_path == tmp_path / "authored.yaml"
    assert found.trx_workplan_path == output_path / "legacy_transformed.yaml"
    assert found.legacy.name == "legacy-plan (transformed)"


@pytest.mark.asyncio
async def test_workplanrun_of_cstar_0_17_is_rewritten_in_place(tmp_path: Path) -> None:
    """Verify updating a 0.17.0 record rewrites its own history entry, in this
    release's shape: the old keys move under `legacy`, which a record read back
    from the rewrite still answers from when the run has no manifest.
    """
    run_id = "legacy-updated"
    output_path = tmp_path / "output"
    repo = TrackingRepository()
    _write_legacy_record(repo, run_id, output_path, uuid=str(uuid4()), attached="1")
    legacy_history = repo.history_path(run_id, LEGACY_START)

    updated = await repo.update_workplan_run(
        run_id, lambda run: run.sentinels.add(tmp_path / "a.sentinel")
    )

    assert updated is not None
    assert legacy_history.exists()
    assert list(repo.run_history_dir(run_id).iterdir()) == [legacy_history]
    assert repo.latest_path(run_id).resolve() == legacy_history.resolve()

    document = yaml.safe_load(legacy_history.read_text())
    assert not set(LEGACY_KEYS) & set(document)
    assert "name" not in document["metadata"]
    assert "access" not in document["metadata"]
    assert document["attached"] is True
    assert document["metadata"] == {KEY_RUN_UUID: updated.uuid}
    held = document["legacy"]
    assert held["name"] == "legacy-plan (transformed)"
    assert datetime.fromisoformat(str(held["start_at"])) == LEGACY_START
    assert Path(held["workplan_path"]) == tmp_path / "authored.yaml"
    assert Path(held["trx_workplan_path"]) == output_path / "legacy_transformed.yaml"

    reread = await repo.get_workplan_run(run_id)
    assert reread is not None
    assert not (output_path / RUN_MANIFEST_NAME).exists()
    assert reread.attached is True
    assert reread.recorded_at == LEGACY_START
    assert reread.sentinels == {tmp_path / "a.sentinel"}
    # no manifest in the run directory: what the old record said is still answered
    assert reread.name == "legacy-plan"
    assert reread.start_at == LEGACY_START
    assert reread.workplan_path == tmp_path / "authored.yaml"
    assert reread.trx_workplan_path == output_path / "legacy_transformed.yaml"


def test_workplanrun_keeps_an_existing_legacy_block_over_the_old_keys(
    tmp_path: Path,
) -> None:
    """Verify the values of a nested `legacy` block win over the 0.17.0 top-level
    keys, which fill only what the block leaves empty.
    """
    document = {
        "output_path": str(tmp_path),
        "run_id": "both-spellings",
        "workplan_path": str(tmp_path / "old-authored.yaml"),
        "start_at": LEGACY_START.isoformat(),
        "metadata": {"name": "old-name"},
        "legacy": {"name": "kept-name", "workplan_path": str(tmp_path / "kept.yaml")},
    }

    run = WorkplanRun.model_validate(document)

    assert run.legacy.name == "kept-name"
    assert run.legacy.workplan_path == tmp_path / "kept.yaml"
    assert run.legacy.start_at == LEGACY_START


def test_workplanrun_of_a_run_before_the_manifest_takes_its_identity_from_the_record(
    tmp_path: Path,
) -> None:
    """Verify a run scheduled before `cstar-run.yaml` existed, recorded in the
    0.17.0 shape without a uuid or attachment marks, answers from the record.
    """
    run_id = "pre-manifest"
    output_path = tmp_path / "output"
    (output_path / "tasks").mkdir(parents=True)
    trx = output_path / "workplan_transformed.yaml"
    trx.touch()
    document = {
        "workplan_path": str(tmp_path / "authored.yaml"),
        "trx_workplan_path": str(trx),
        "output_path": str(output_path),
        "run_id": run_id,
        "start_at": LEGACY_START.isoformat(),
        "metadata": {"name": "old-plan (transformed)"},
    }
    repo = TrackingRepository()
    repo.latest_path(run_id).write_text(yaml.safe_dump(document))

    found = repo.get_workplan_run_sync(run_id)

    assert found is not None
    assert not (output_path / RUN_MANIFEST_NAME).exists()
    assert found.name == "old-plan"
    assert found.start_at == LEGACY_START
    assert found.trx_workplan_path == trx
    assert found.workplan_path == tmp_path / "authored.yaml"
    assert found.attached is False
    assert found.live is True
    assert found.access is Access.owned
    assert found.uuid == ""
    assert found.metadata == {}


def test_workplanrun_derives_its_identity_from_the_manifest(tmp_path: Path) -> None:
    """Verify name, start, workplan paths come from the run's manifest, and none
    of them is written into the record.
    """
    run_id = "thin"
    output_path = tmp_path / "output"
    _write_manifest(output_path, run_id, workplan_path="/some/where/plan.yaml")
    trx = output_path / "plan_transformed.yaml"
    trx.touch()
    repo = TrackingRepository()
    persisted = repo.put_workplan_run_sync(
        _make_workplan_run(tmp_path, run_id, metadata={KEY_RUN_UUID: str(uuid4())})
    )

    document = yaml.safe_load(persisted.read_text())
    assert not set(LEGACY_KEYS) & set(document)
    assert "name" not in document["metadata"]

    found = repo.get_workplan_run_sync(run_id)

    assert found is not None
    assert found.name == run_id
    assert found.start_at == MANIFEST_START
    assert found.start_at.tzinfo is not None
    assert found.workplan_path == Path("/some/where/plan.yaml")
    assert found.trx_workplan_path == trx
    assert found.legacy.name == ""
    assert found.legacy.trx_workplan_path is None


def test_workplanrun_start_is_the_latest_attempts_else_the_manifests_creation(
    tmp_path: Path,
) -> None:
    """Verify `start_at` follows the latest attempt, falls back to the manifest's
    creation without one, and is aware even when the manifest holds a naive time.
    """
    root = tmp_path / "output"
    manifest = _write_manifest(root, "attempts")
    later = datetime(2026, 10, 16, 8, 30, tzinfo=UTC)

    resumed = manifest.model_copy(
        update={"attempts": [*manifest.attempts, Attempt(start_at=later)]}
    )
    write_run_manifest(root, resumed)
    assert _make_workplan_run(tmp_path, "attempts").start_at == later

    write_run_manifest(root, manifest.model_copy(update={"attempts": []}))
    created = _make_workplan_run(tmp_path, "attempts").start_at
    assert created == MANIFEST_START
    assert created.tzinfo is not None


def test_workplanrun_transformed_workplan_is_named_for_the_authored_stem(
    tmp_path: Path,
) -> None:
    """Verify the transformed workplan is `<stem>_transformed.yaml`, falling back
    to the only `*_transformed.yaml` of the run directory, and failing when there
    is none, or several and no way to choose.
    """
    root = tmp_path / "output"
    _write_manifest(root, "trx", workplan_path="/some/where/plan.yaml")

    with pytest.raises(CstarError, match="holds no transformed workplan"):
        _make_workplan_run(tmp_path, "trx").trx_workplan_path

    other = root / "other_transformed.yaml"
    other.touch()
    assert _make_workplan_run(tmp_path, "trx").trx_workplan_path == other

    named = root / "plan_transformed.yaml"
    named.touch()
    assert _make_workplan_run(tmp_path, "trx").trx_workplan_path == named

    named.unlink()
    (root / "third_transformed.yaml").touch()
    with pytest.raises(CstarError, match="more than one transformed workplan"):
        _make_workplan_run(tmp_path, "trx").trx_workplan_path


def test_workplanrun_workplan_path_falls_back_to_the_transformed_workplan(
    tmp_path: Path,
) -> None:
    """Verify a run whose manifest records no authored workplan answers with the
    transformed one.
    """
    root = tmp_path / "output"
    _write_manifest(root, "no-workplan")
    trx = root / "plan_transformed.yaml"
    trx.touch()

    assert _make_workplan_run(tmp_path, "no-workplan").workplan_path == trx


def test_workplanrun_without_manifest_or_legacy_falls_back_to_the_record(
    tmp_path: Path,
) -> None:
    """Verify a thin record whose run has no manifest starts when it was recorded
    and has no name.
    """
    recorded = datetime(2026, 8, 3, 4, 5, 6, tzinfo=UTC)
    run = _make_workplan_run(tmp_path, "bare", recorded_at=recorded)

    assert run.name == ""
    assert run.start_at == recorded
    assert run.legacy.start_at is None
    with pytest.raises(CstarError, match="holds no transformed workplan"):
        run.workplan_path


def test_workplanrun_reads_the_manifest_once_for_what_it_derives(
    tmp_path: Path,
) -> None:
    """Verify name, start and both workplan paths cost one manifest read per
    instance, even for a run scheduled here whose `manifest` is otherwise read afresh.
    """
    root = tmp_path / "output"
    _write_manifest(root, "once", workplan_path="/some/where/plan.yaml")
    (root / "plan_transformed.yaml").touch()
    run = _make_workplan_run(tmp_path, "once")

    with mock.patch(
        "cstar.orchestration.tracking.read_run_manifest", wraps=read_run_manifest
    ) as read:
        assert run.name == "once"
        assert run.start_at == MANIFEST_START
        assert run.workplan_path == Path("/some/where/plan.yaml")
        assert run.trx_workplan_path == root / "plan_transformed.yaml"
        assert run.name == "once"

    read.assert_called_once()


def test_workplanrun_does_not_keep_a_failed_manifest_read(tmp_path: Path) -> None:
    """Verify a manifest that cannot be read yet is asked for again later: the
    fallbacks are used meanwhile, and the manifest once it is there.
    """
    root = tmp_path / "output"
    run = _make_workplan_run(tmp_path, "late")
    assert run.name == ""

    _write_manifest(root, "late")

    assert run.name == "late"
    assert run.start_at == MANIFEST_START


def test_reanchored_workplanrun_derives_the_transformed_workplan_from_the_new_directory(
    tmp_path: Path,
) -> None:
    """Verify a record that follows a moved run reads that run's manifest and finds
    the transformed workplan beside it, however the old directory was named.
    """
    run_id = "relocated"
    recorded = tmp_path / "old-data" / "workplan_runs" / run_id
    current = StateDirectoryManager.data_dir(run_id)
    _write_manifest(current, run_id, workplan_path="/some/where/plan.yaml")
    (current / "plan_transformed.yaml").touch()
    run = _make_workplan_run(tmp_path, run_id, output_path=recorded)

    # reading the old (absent) directory first must not be remembered
    assert run.name == ""
    moved = run.reanchored()

    assert moved.output_path == current
    assert moved.name == run_id
    assert moved.trx_workplan_path == current / "plan_transformed.yaml"
    assert moved.workplan_path == Path("/some/where/plan.yaml")
