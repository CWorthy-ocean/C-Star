import itertools
import os
import typing as t
from pathlib import Path
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_RUNID
from cstar.base.exceptions import CstarError
from cstar.cli.admin.clean import (
    ARG_YES,
    CleanupStatus,
    FileSystemCleanupAction,
    app,
    get_default_cleanup_actions,
    get_run_actions,
    perform_actions,
    runid_callback,
)
from cstar.entrypoint.utils import ARG_DRY_RUN
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.models import Workplan
from cstar.orchestration.tracking import (
    KEY_ACCESS,
    KEY_ATTACHED,
    Access,
    TrackingRepository,
    WorkplanRun,
)


def test_cli_admin_clean_get_default_cleanup_actions() -> None:
    """Verify that the default cleanup actions include all 4 XDG-compliant directories."""
    default_actions = t.cast(
        "list[FileSystemCleanupAction]", get_default_cleanup_actions()
    )

    default_paths = set(
        itertools.chain.from_iterable(a.asset_paths for a in default_actions)
    )

    assert DirectoryManager.cache_home() in default_paths
    assert DirectoryManager.config_home() in default_paths
    assert DirectoryManager.data_home() in default_paths
    assert DirectoryManager.state_home() in default_paths


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

    action = FileSystemCleanupAction(
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
        FileSystemCleanupAction(
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
    "num_actions",
    range(1, 5),
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
    action = FileSystemCleanupAction(
        name="action",
        asset_paths=[tmp_path / str(i) for i in range(num_actions)],
    )
    for i, path in enumerate(action.asset_paths):
        if i % 2:
            path.touch()
        else:
            path.mkdir(parents=True, exist_ok=False)

    await perform_actions([action], dry_run=True)

    # confirm the deletions didn't occur
    assert all(path.exists() for path in action.asset_paths)


@pytest.mark.asyncio
async def test_cli_admin_clean_runid_callback(
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

    actual_run_id = runid_callback(mock_ctx, run_id)

    # confirm the run-id is returned with any callback cleaning appleid
    assert actual_run_id == run_id.strip()


def test_cli_admin_clean_normalizes_mixed_case_runid(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify a mixed-case run-id is normalized before it reaches the
    environment, so cleanup targets the directories the run actually used.

    Regression test: the clean command previously placed the raw user-typed
    run-id in the environment, so a mixed-case run-id resolved cleanup paths
    in a nonexistent (wrong-case) run directory.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    *_, run_id = executed_workplan

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--run-id", run_id.upper(), ARG_YES, ARG_DRY_RUN],
        color=False,
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert os.environ[ENV_CSTAR_RUNID] == run_id


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
    actions = t.cast("list[FileSystemCleanupAction]", get_default_cleanup_actions())
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


@pytest.fixture
def data_home(tmp_path: Path) -> Path:
    """Create a data directory holding a cache, a catalog, and removable content."""
    home = tmp_path / "data"
    outside = tmp_path / "outside"
    for d in (home / "source-data", home / "catalog", home / "run1", outside):
        d.mkdir(parents=True)
    (home / "source-data" / "glorys.nc").touch()
    (home / "catalog" / "domains.yaml").touch()
    (home / "run1" / "out.nc").touch()
    (home / "file.txt").touch()
    (home / "dirlink").symlink_to(outside, target_is_directory=True)
    (outside / "precious.txt").touch()
    return home


def _keep_action(home: Path, *keeps: Path) -> FileSystemCleanupAction:
    return FileSystemCleanupAction(
        name="data", asset_paths=[home], keep_paths=list(keeps)
    )


def test_cli_admin_clean_keep_paths_survive(data_home: Path, tmp_path: Path) -> None:
    """Verify everything but the keep paths is removed, and symlinks are unlinked."""
    keeps = [data_home / "source-data", data_home / "catalog"]
    action = _keep_action(data_home, *keeps)

    assert not action.mitigated()
    results = action.execute()

    assert {p.name for p in data_home.iterdir()} == {"source-data", "catalog"}
    assert (data_home / "source-data" / "glorys.nc").exists()
    assert (data_home / "catalog" / "domains.yaml").exists()
    assert (tmp_path / "outside" / "precious.txt").exists()  # link target untouched
    assert list(results.ledger) == [str(data_home)]
    assert results.status == CleanupStatus.DONE
    assert action.mitigated()


def test_cli_admin_clean_keep_path_nested(tmp_path: Path) -> None:
    """Verify ancestors of a nested keep path are kept and their siblings removed."""
    home = tmp_path / "data"
    keep = home / "a" / "b" / "keep"
    for d in (keep, home / "a" / "b" / "drop", home / "a" / "sibling", home / "other"):
        d.mkdir(parents=True)
    (keep / "x.nc").touch()
    (home / "a" / "b" / "f.txt").touch()

    action = _keep_action(home, keep)
    assert not action.mitigated()
    action.execute()

    assert [p for p in sorted(home.rglob("*"))] == [
        home / "a",
        home / "a" / "b",
        keep,
        keep / "x.nc",
    ]
    assert action.mitigated()


def test_cli_admin_clean_keep_path_symlink(tmp_path: Path) -> None:
    """Verify a keep path that is a symlink is neither followed nor removed."""
    home = tmp_path / "data"
    legacy = tmp_path / "legacy-cache"
    home.mkdir()
    legacy.mkdir()
    (legacy / "glorys.nc").touch()
    link = home / "source-data"
    link.symlink_to(legacy, target_is_directory=True)
    (home / "run1").mkdir()

    action = _keep_action(home, link)
    action.execute()

    assert link.is_symlink()
    assert (legacy / "glorys.nc").exists()
    assert [p.name for p in home.iterdir()] == ["source-data"]
    assert action.mitigated()


@pytest.mark.asyncio
async def test_cli_admin_clean_keep_paths_dry_run(data_home: Path) -> None:
    """Verify dry-run removes nothing when keep paths are configured."""
    before = sorted(data_home.rglob("*"))
    action = _keep_action(data_home, data_home / "source-data")

    await perform_actions([action], dry_run=True)

    assert sorted(data_home.rglob("*")) == before
    assert not action.mitigated()


def test_cli_admin_clean_keep_path_equal_to_asset_or_outside(tmp_path: Path) -> None:
    """Verify keep paths that are the asset itself, or outside it, change nothing."""
    asset, elsewhere = tmp_path / "asset", tmp_path / "elsewhere"
    asset.mkdir()
    elsewhere.mkdir()
    (asset / "f").touch()
    action = _keep_action(asset, asset, elsewhere)

    assert action.display() == f"data\n{asset}"
    action.execute()

    assert not asset.exists()
    assert elsewhere.exists()


def test_cli_admin_clean_display_lists_keeps(data_home: Path, tmp_path: Path) -> None:
    """Verify display lists the kept paths that are inside an asset."""
    keep = data_home / "source-data"
    action = _keep_action(data_home, keep, tmp_path / "elsewhere")

    lines = action.display().splitlines()

    assert lines == ["data", str(data_home), f"* keeps {keep}"]


def _isolate_env(
    monkeypatch: pytest.MonkeyPatch, home: Path, catalog: str | None
) -> Path:
    """Point C-Star's data, home, and scratch resolution at `home`."""
    data = home / "cstar"
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("CSTAR_DATA_HOME", str(data))
    for var in (
        "CSTAR_PROJECT_HOME",
        "PROJECT",
        "SCRATCH",
        "SCRATCH_DIR",
        "LOCAL_SCRATCH",
        "CSTAR_CATALOG",
    ):
        monkeypatch.delenv(var, raising=False)
    if catalog is not None:
        monkeypatch.setenv("CSTAR_CATALOG", catalog)
    monkeypatch.setattr(
        "cstar.system.manager.get_system_context",
        mock.Mock(side_effect=CstarError("no system")),
    )
    return data


def test_cli_admin_clean_default_data_action_keeps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the data action carries the source-data cache and user catalog."""
    home = tmp_path.resolve()
    data = _isolate_env(monkeypatch, home, catalog=None)

    actions = t.cast("list[FileSystemCleanupAction]", get_default_cleanup_actions())
    (action,) = (a for a in actions if a.name == "C-Star Data")

    assert action.keep_paths == [
        DirectoryManager.source_data_home(),
        home / "cstar" / "catalog",
    ]
    assert action.keep_paths[0].resolve() == (data / "source-data").resolve()


def test_cli_admin_clean_default_data_action_keeps_local_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify only the cache is kept when the catalog has no writable layer."""
    _isolate_env(monkeypatch, tmp_path.resolve(), catalog="local")

    actions = t.cast("list[FileSystemCleanupAction]", get_default_cleanup_actions())
    (action,) = (a for a in actions if a.name == "C-Star Data")

    assert action.keep_paths == [DirectoryManager.source_data_home()]


def _track(root: Path, run_id: str, **metadata: str) -> WorkplanRun:
    """Make the run directory `root` and track it under `run_id`."""
    root.mkdir(parents=True)
    (root / "data.nc").write_text("data")
    run = WorkplanRun(
        workplan_path=root / "workplan.yaml",
        trx_workplan_path=root / "workplan_transformed.yaml",
        output_path=root,
        run_id=run_id,
        metadata=metadata,
    )
    TrackingRepository().put_workplan_run_sync(run)
    return run


def _names(actions: list[t.Any]) -> list[str]:
    """The names of the actions, without the run-id they are about."""
    return [action.name.split(" ", 1)[1] for action in actions]


def test_cli_admin_clean_run_actions_of_a_scheduled_run_include_its_data(
    tmp_path: Path,
) -> None:
    """Verify a run this machine scheduled has its state and its data removed."""
    run = _track(tmp_path / "scheduled", "scheduled")

    actions = get_run_actions("scheduled")

    assert _names(actions) == ["State", "Data"]
    data = t.cast("FileSystemCleanupAction", actions[1])
    assert data.asset_paths == [run.output_path]


@pytest.mark.parametrize(
    "metadata",
    [
        {KEY_ATTACHED: "1", KEY_ACCESS: Access.owned.value},
        {KEY_ATTACHED: "1"},
    ],
)
def test_cli_admin_clean_run_actions_of_an_owned_attached_run_include_its_data(
    tmp_path: Path, metadata: dict[str, str]
) -> None:
    """Verify an attached run the user owns is cleaned like any other."""
    _track(tmp_path / "colleague" / "owned", "owned", **metadata)

    assert _names(get_run_actions("owned")) == ["State", "Data"]


def test_cli_admin_clean_run_actions_keep_the_data_of_a_read_only_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify the data of a run attached read-only is left alone, the tracking
    record is still removed, and the user is told why.
    """
    _track(
        tmp_path / "colleague" / "frozen",
        "frozen",
        **{KEY_ATTACHED: "1", KEY_ACCESS: Access.read_only.value},
    )

    actions = get_run_actions("frozen")

    assert _names(actions) == ["State"]
    out = " ".join(capsys.readouterr().out.split())
    assert "'frozen' is attached read-only" in out
    assert "admin clean" in out
    assert "--own" in out


def test_cli_admin_clean_run_actions_keep_the_data_of_a_project_area_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify a tracked run whose directory is in the project's runs area keeps
    its data, and the user is told to remove it by hand.
    """
    project = (tmp_path / "project").resolve()
    monkeypatch.setenv("CSTAR_PROJECT_HOME", str(project))
    root = project / "cstar" / "runs" / "shared"
    _track(root, "shared")

    actions = get_run_actions("shared")

    assert _names(actions) == ["State"]
    out = " ".join(capsys.readouterr().out.split())
    assert "project runs area" in out
    assert "remove it by hand" in out


def test_cli_admin_clean_removes_the_tracking_of_a_read_only_run_but_not_its_data(
    tmp_path: Path,
) -> None:
    """Verify the command removes the record and leaves the run directory."""
    run = _track(
        tmp_path / "colleague" / "frozen",
        "frozen",
        **{KEY_ATTACHED: "1", KEY_ACCESS: Access.read_only.value},
    )
    latest = TrackingRepository().latest_path("frozen")
    assert latest.exists()

    result = CliRunner().invoke(
        app, ["--run-id", "frozen", ARG_YES], color=False, catch_exceptions=False
    )

    assert result.exit_code == 0, result.output
    assert not latest.exists()
    assert TrackingRepository().get_workplan_run_sync("frozen") is None
    assert (run.output_path / "data.nc").read_text() == "data"


def test_cli_admin_clean_removes_the_data_of_a_scheduled_run(tmp_path: Path) -> None:
    """Verify a run scheduled here is removed whole, as before."""
    run = _track(tmp_path / "scheduled", "scheduled")

    result = CliRunner().invoke(
        app, ["--run-id", "scheduled", ARG_YES], color=False, catch_exceptions=False
    )

    assert result.exit_code == 0, result.output
    assert not run.output_path.exists()
