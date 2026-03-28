import asyncio
import os
import unittest.mock as mock
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from cstar.base.env import ENV_CSTAR_STATE_HOME
from cstar.base.utils import slugify
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun


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
    wp_path = tmp_path / "fake_workplan.yaml"
    wp_trx_path = tmp_path / "mock_transformed_workplan.yaml"
    run_id = "test-tracking-create-run-id"

    repo = TrackingRepository()
    wp_run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=wp_trx_path,
        output_path=output_path,
        run_id=run_id,
    )

    with mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}):
        latest_dir = repo.latest_dir
        history_dir = repo.history_dir

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

    found_files = list(history_dir.rglob("*.yaml"))
    assert found_files


@pytest.mark.asyncio
async def test_tracking_retrieve(tmp_path: Path) -> None:
    """Verify that run-tracking retrieves a persisted record and deserializes it
    as expected.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    """
    state_dir = tmp_path / "state"
    output_path = tmp_path / "output"
    wp_path = tmp_path / "fake_workplan.yaml"
    wp_trx_path = tmp_path / "mock_transformed_workplan.yaml"
    run_id = "test-tracking-retrieve-run-id"
    start_at = datetime.now(tz=timezone.utc)
    captured_env = {"foo": "foo-value"}

    wp_run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=wp_trx_path,
        output_path=output_path,
        run_id=run_id,
        start_at=start_at,
        environment=captured_env,
    )

    with mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}):
        repo = TrackingRepository()
        _ = await repo.put_workplan_run(wp_run)

    # use public API to retrieve using "latest" record (by passing only run_id)
    latest = await repo.get_workplan_run(run_id=run_id)
    assert latest
    assert latest.__dict__ == wp_run.__dict__  # confirm same stored values

    # use public API to retrieve using "history" record (by passing run_id & run_date)
    history = await repo.get_workplan_run(run_id=run_id, run_date=wp_run.start_at)
    assert history
    assert history.__dict__ == wp_run.__dict__  # confirm same stored values

    assert history.output_path == output_path
    assert history.workplan_path == wp_path
    assert history.trx_workplan_path == wp_trx_path
    assert history.run_id == run_id
    assert history.start_at == start_at
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
    wp_path = tmp_path / "fake_workplan.yaml"
    wp_trx_path = tmp_path / "mock_transformed_workplan.yaml"
    run_id = "test-tracking-retrieve-run-id"

    repo = TrackingRepository()
    wp_run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=wp_trx_path,
        output_path=output_path,
        run_id=run_id,
    )

    # manually serialize so i have access to a known, working path for mocks
    mock_doc_path = tmp_path / repo._runfile_name(run_id)
    assert serialize(mock_doc_path, wp_run)

    with (
        mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}),
        mock.patch.object(repo, "_history_path", return_value=mock_doc_path) as hp_fn,
        mock.patch.object(repo, "_latest_path", return_value=mock_doc_path) as lp_fn,
    ):
        latest = await repo.get_workplan_run(wp_run.run_id, None)
        assert latest

        # confirm the latest search was used
        lp_fn.assert_called_once()
        hp_fn.assert_not_called()

        history = await repo.get_workplan_run(wp_run.run_id, wp_run.start_at)
        assert history

        # confirm the history search was used
        hp_fn.assert_called_once()
        lp_fn.assert_called_once()  # no new call made


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

    # verify it matches the supplied workplan
    assert run_id == slugify(wp.name)


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
    expected_name = "Sample Workplan"

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
    wp_path = tmp_path / "fake_workplan.yaml"
    wp_trx_path = tmp_path / "mock_transformed_workplan.yaml"
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
                    workplan_path=wp_path,
                    trx_workplan_path=wp_trx_path,
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
    wp_path = tmp_path / "fake_workplan.yaml"
    wp_trx_path = tmp_path / "mock_transformed_workplan.yaml"
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
                        workplan_path=wp_path,
                        trx_workplan_path=wp_trx_path,
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
                        workplan_path=wp_path,
                        trx_workplan_path=wp_trx_path,
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
