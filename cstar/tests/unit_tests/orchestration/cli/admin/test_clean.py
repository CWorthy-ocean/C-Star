import itertools
import os
from collections.abc import Generator
from pathlib import Path
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_CLI_DRY_RUN, FLAG_ON
from cstar.cli.admin.clean import (
    ARG_YES,
    CleanupAction,
    app,
    get_default_cleanup_actions,
    get_run_action,
    perform_actions,
)
from cstar.entrypoint.utils import ARG_DRY_RUN
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.models import Workplan
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun


@pytest.fixture(autouse=True)
def prefect_path(tmp_path: Path) -> Generator[Path]:
    """Replace the function returning the path to the prefect storage location
    to avoid wiping out "real data" during tests.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs

    Returns
    -------
    Path
    """
    path = tmp_path / "prefect-test-storage"
    path.mkdir(parents=True, exist_ok=True)

    with mock.patch(
        "cstar.cli.admin.clean.get_prefect_storage_path",
        mock.Mock(return_value=path),
    ):
        yield path


def test_cli_admin_clean_get_default_cleanup_actions(prefect_path: Path) -> None:
    """Verify that the default cleanup actions include all 4 XDG-compliant directories.

    Parameters
    ----------
    prefect_path : Path
        The path returned by the mocked `get_prefect_storage_path` function.
    """
    default_actions = get_default_cleanup_actions()

    default_paths = set(
        itertools.chain.from_iterable(a.asset_paths for a in default_actions)
    )

    assert DirectoryManager.cache_home() in default_paths
    assert DirectoryManager.config_home() in default_paths
    assert DirectoryManager.data_home() in default_paths
    assert DirectoryManager.state_home() in default_paths
    assert prefect_path in default_paths


@pytest.mark.parametrize(
    ("existing", "missing", "exp_mitigated"),
    [
        pytest.param(["f0"], [], False, id="File exists, no DNE"),
        pytest.param(["d0"], [], False, id="Dir exists, no DNE"),
        pytest.param(["f0", "d0"], [], False, id="File & dir exist, no DNE"),
        pytest.param(["f0", "d0"], ["f1"], False, id="File & dir exist, file DNE"),
        pytest.param(["f0", "d0"], ["d1"], False, id="File & dir exist, dir DNE"),
        pytest.param(
            ["f0", "d0"], ["d1", "f1"], False, id="File & dir exist, file & dir DNE"
        ),
        pytest.param([], ["f0"], True, id="Empty, file DNE"),
        pytest.param([], ["d0"], True, id="Empty, dir DNE"),
        pytest.param([], ["f0", "d0"], True, id="Empty, file & dir DNE"),
    ],
)
def test_cli_admin_cleanupaction_mitigated(
    tmp_path: Path,
    existing: list[str],
    missing: list[str],
    exp_mitigated: bool,
) -> None:
    """Verify that the cleanup action correctly determines the right
    result in `CleanupAction.mitigated`

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    existing : str
        Asset paths that will be created and available to be cleaned up
    missing : str
        Asset paths that will be contained in the actions but don't exist.

        Used to confirm no bad behavior if file/dir paths aren't found.
    exp_mitigated : bool
        Boolean indicating if the action is expected to evaluate as mitigated.
    """
    all_assets = [tmp_path / e for e in existing]
    for asset in all_assets:
        if asset.name.startswith("f"):
            asset.touch()
        if asset.name.startswith("d"):
            asset.mkdir(parents=True, exist_ok=True)

    # add any paths that are already "cleaned up"
    all_assets.extend(tmp_path / e for e in missing)

    action = CleanupAction(
        name="action",
        asset_paths=all_assets,
    )

    assert action.mitigated() == exp_mitigated


@pytest.mark.parametrize(
    "num_actions",
    range(5),
)
async def test_cli_admin_clean_perform_actions(
    tmp_path: Path,
    num_actions: int,
) -> None:
    """Verify that the correct action is performed.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    num_actions : int
        The number of cleanup actions to create and execute via `perform_action`
    """
    actions = [
        CleanupAction(
            name=f"action-{i}",
            asset_paths=[tmp_path / f"action-{i}-{j}" for j in range(3)],
        )
        for i in range(num_actions)
    ]

    await perform_actions(actions)

    for action in actions:
        # confirm the cleanup action says cleanup is done
        assert action.mitigated()


@pytest.mark.parametrize(
    "num_assets",
    range(5),
)
async def test_cli_admin_clean_perform_actions_dryrun(
    tmp_path: Path,
    num_actions: int,
) -> None:
    """Verify that no actions are performed if dry-run is specified.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test assets
    num_actions : int
        The number of cleanup actions to create and execute via `perform_action`
    """
    action = CleanupAction(
        name="action",
        asset_paths=[tmp_path / str(i) for i in range(num_actions)],
    )
    for i, path in enumerate(action.asset_paths):
        if i % 2:
            path.touch()
        else:
            path.mkdir(parents=True, exist_ok=False)

    with mock.patch.dict(os.environ, {ENV_CSTAR_CLI_DRY_RUN: FLAG_ON}):
        await perform_actions([action])

    # confirm the deletions didn't occur
    assert all(path.exists() for path in action.asset_paths)


@pytest.mark.asyncio
async def test_cli_admin_clean_get_run_action(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that `get_run_action` results in the population of the typer
    context object with a `CleanupRequest` for the specified run.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, run_id = executed_workplan
    repo = TrackingRepository()
    run = await repo.get_workplan_run(run_id)
    mock_ctx = mock.MagicMock(spec=typer.Context)
    mock_ctx.obj = {"run": run, "workplan": wp}

    # ensure `get_run_action` handles whitespace
    run_id = f" {run_id}\n\t"

    assert run is not None

    with (
        mock.patch(
            "cstar.cli.admin.clean.get_from_ctxmap", return_value=run
        ) as mock_get,
        mock.patch("cstar.cli.admin.clean.preload_run", return_value=run_id),
    ):
        actual_run_id = get_run_action(mock_ctx, run_id)

    # confirm the run-id is returned with any callback cleaning appleid
    mock_get.assert_called_once_with(mock_ctx, "run", WorkplanRun)
    assert actual_run_id == run_id.strip()

    # confirm the action was set
    action: CleanupAction | None = mock_ctx.obj.get("action", None)
    assert isinstance(action, CleanupAction)
    assert run.state_dir in action.asset_paths

    # confirm only run-specific asset paths (none of the defaults)
    assets = [str(p) for p in action.asset_paths]
    assert all(actual_run_id in p for p in assets)


@pytest.mark.parametrize(
    ("dry_run"),
    [
        pytest.param(True, id="dry-run enabled"),
        pytest.param(False, id="dry-run disabled"),
    ],
)
def test_cli_admin_clean_default_cleanup(dry_run: bool) -> None:
    """Verify that the default _nuclear option_ cleanup is executed when
    a run-id is not specified.

    Parameters
    ----------
    dry_run : bool
        dynamically configures dry-run mode.
    """
    # create an asset in each of the default locations
    actions = get_default_cleanup_actions()
    asset_idx = 0
    for action in actions:
        for path in action.asset_paths:
            asset_path = path / f"{asset_idx}.mock"
            asset_path.touch()
            asset_idx += 1

    # use non-interactive mode
    args: list[str] = [ARG_YES]
    if dry_run:
        args.append(ARG_DRY_RUN)

    runner = CliRunner()
    result = runner.invoke(app, args, color=False)

    key = "will remove" if dry_run else "removed"

    assert key in result.stdout.lower()
