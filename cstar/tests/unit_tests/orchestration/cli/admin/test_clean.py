import itertools
import os
import typing as t
from pathlib import Path
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.base.env import (
    ENV_CSTAR_CACHE_HOME,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_RUNID,
    ENV_CSTAR_STATE_HOME,
    REMOVED_SETTINGS,
)
from cstar.base.exceptions import CstarError
from cstar.base.feature import ENV_FF_DEVELOPER_MODE
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
from cstar.cli.cli import app as root_app
from cstar.entrypoint.utils import ARG_ALL, ARG_DRY_RUN
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import ProcessHandle
from cstar.orchestration.serialization import serialize
from cstar.orchestration.state import StateRepository
from cstar.orchestration.tracking import (
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
    args: list[str] = [ARG_ALL, ARG_YES]
    if dry_run:
        args.append(ARG_DRY_RUN)

    runner = CliRunner()
    result = runner.invoke(app, args, color=False)

    key = "will remove" if dry_run else "removed"

    assert key in result.stdout.lower()


def _seed_roots() -> list[Path]:
    """Put an asset in each of the four C-Star roots and return the assets."""
    roots = [
        Path(os.environ[name])
        for name in (
            ENV_CSTAR_CACHE_HOME,
            ENV_CSTAR_CONFIG_HOME,
            ENV_CSTAR_DATA_HOME,
            ENV_CSTAR_STATE_HOME,
        )
    ]
    assets = [root / "asset.mock" for root in roots]
    for asset in assets:
        asset.touch()
    return assets


@pytest.mark.parametrize("extra", [[], [ARG_YES], [ARG_YES, ARG_DRY_RUN]])
def test_cli_admin_clean_requires_all_or_run_id(extra: list[str]) -> None:
    """Verify neither `--all` nor `--run-id` is a usage error that names both
    choices and removes nothing, even with `--yes`.
    """
    assets = _seed_roots()

    result = CliRunner().invoke(app, extra, color=False)

    assert result.exit_code == 2
    assert "--all" in result.output
    assert "--run-id" in result.output
    assert all(asset.exists() for asset in assets)


def test_cli_admin_clean_rejects_all_with_run_id() -> None:
    """Verify `--all` together with `--run-id` is a usage error removing nothing."""
    assets = _seed_roots()

    result = CliRunner().invoke(
        app, [ARG_ALL, "--run-id", "some-run", ARG_YES], color=False
    )

    assert result.exit_code == 2
    assert all(asset.exists() for asset in assets)


def test_cli_admin_clean_all_dry_run_lists_the_four_roots() -> None:
    """Verify `--all --dry-run` lists exactly the four roots in the environment
    and removes nothing.
    """
    assets = _seed_roots()
    roots = {
        os.environ[name]
        for name in (
            ENV_CSTAR_CACHE_HOME,
            ENV_CSTAR_CONFIG_HOME,
            ENV_CSTAR_DATA_HOME,
            ENV_CSTAR_STATE_HOME,
        )
    }

    result = CliRunner().invoke(
        app,
        [ARG_ALL, ARG_DRY_RUN],
        color=False,
        catch_exceptions=False,
        env={"COLUMNS": "1000"},  # keep rich from wrapping the long tmp paths
    )

    assert result.exit_code == 0, result.output
    listed = {
        line
        for line in (ln.strip() for ln in result.output.splitlines())
        if line.startswith("/") and "keeps" not in line
    }
    assert listed == roots
    assert all(asset.exists() for asset in assets)


def test_cli_admin_clean_all_keeps_the_source_cache_and_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify `--all` removes everything under the data home except the shared
    source-data cache and the user catalog.
    """
    data = _isolate_env(monkeypatch, tmp_path.resolve(), catalog=None)
    source_data = DirectoryManager.source_data_home()
    catalog = data / "catalog"
    run = data / "runs" / "run1"
    for d in (source_data, catalog, run):
        d.mkdir(parents=True, exist_ok=True)
    (source_data / "glorys.nc").touch()
    (catalog / "domains.yaml").touch()
    (run / "out.nc").touch()
    state = Path(os.environ[ENV_CSTAR_STATE_HOME]) / "state.mock"
    state.touch()

    result = CliRunner().invoke(
        app, [ARG_ALL, ARG_YES], color=False, catch_exceptions=False
    )

    assert result.exit_code == 0, result.output
    assert (source_data / "glorys.nc").exists()
    assert (catalog / "domains.yaml").exists()
    assert not (data / "runs").exists()
    assert not state.exists()


@pytest.mark.parametrize("blank", [" ", "\n\t "])
def test_cli_admin_clean_blank_runid_is_explained(blank: str) -> None:
    """Verify a whitespace run-id gives a message, not a silent exit 1."""
    assets = _seed_roots()

    result = CliRunner().invoke(app, ["--run-id", blank, ARG_YES], color=False)

    assert result.exit_code == 2
    assert "blank" in " ".join(result.output.split())
    assert all(asset.exists() for asset in assets)


def test_cli_admin_clean_unknown_runid_exits_zero() -> None:
    """Verify a run-id with nothing to clean says so and exits 0."""
    result = CliRunner().invoke(
        app, ["--run-id", "no-such-run", ARG_YES], color=False, catch_exceptions=False
    )

    assert result.exit_code == 0, result.output
    assert "No cleanup actions to perform" in result.output


def test_cli_admin_clean_is_mounted_without_developer_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `cstar admin clean` is reachable from the root app with the
    developer-mode flag off.
    """
    monkeypatch.delenv(ENV_FF_DEVELOPER_MODE, raising=False)
    assets = _seed_roots()

    result = CliRunner().invoke(
        root_app,
        ["admin", "clean", ARG_ALL, ARG_DRY_RUN],
        color=False,
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "will remove" in result.output.lower()
    assert all(asset.exists() for asset in assets)


def test_cli_admin_clean_refuses_a_removed_dry_run_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `admin clean` removes nothing and exits 2 while a removed setting
    that used to make it a dry run is still exported.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Fixture used to export the setting.
    """
    name = next(n for n, removed in REMOVED_SETTINGS.items() if removed.refused)
    monkeypatch.setenv(name, "1")
    actions = t.cast("list[FileSystemCleanupAction]", get_default_cleanup_actions())
    assets = [path / "asset.mock" for action in actions for path in action.asset_paths]
    for asset in assets:
        asset.touch()

    result = CliRunner().invoke(app, [ARG_ALL, ARG_YES], color=False)

    assert result.exit_code == 2
    assert all(asset.exists() for asset in assets)


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


def _track(root: Path, run_id: str, **fields: t.Any) -> WorkplanRun:
    """Make the run directory `root` and track it under `run_id`."""
    root.mkdir(parents=True)
    (root / "data.nc").write_text("data")
    run = WorkplanRun(
        output_path=root,
        run_id=run_id,
        **fields,
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


@pytest.mark.parametrize("fields", [{"access": Access.owned}, {}])
def test_cli_admin_clean_run_actions_of_an_owned_attached_run_include_its_data(
    tmp_path: Path, fields: dict[str, Access]
) -> None:
    """Verify an attached run the user owns is cleaned like any other."""
    _track(tmp_path / "colleague" / "owned", "owned", attached=True, **fields)

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
        attached=True,
        access=Access.read_only,
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
        attached=True,
        access=Access.read_only,
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


def _leave_sentinel(run_id: str) -> Path:
    """Write a sentinel for a step of `run_id`; return the run's state directory."""
    sentinel = StateRepository.sentinel_path("step", run_id=run_id)
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    assert serialize(sentinel, ProcessHandle(pid="1", name="step", run_id=run_id))
    return sentinel.parent


def test_cli_admin_clean_removes_the_step_state_of_a_run_whose_directory_is_gone(
    tmp_path: Path,
) -> None:
    """Verify the run's sentinels go with its tracking record when its
    directory was already removed, so a later run of the id starts afresh.
    """
    run = _track(tmp_path / "purged", "purged")
    state_dir = _leave_sentinel("purged")
    for path in run.output_path.iterdir():
        path.unlink()
    run.output_path.rmdir()

    result = CliRunner().invoke(
        app, ["--run-id", "purged", ARG_YES], color=False, catch_exceptions=False
    )

    assert result.exit_code == 0, result.output
    assert not state_dir.exists()
    assert TrackingRepository().get_workplan_run_sync("purged") is None


def test_cli_admin_clean_run_actions_of_an_untracked_run_include_its_step_state(
    tmp_path: Path,
) -> None:
    """Verify a run-id with nothing left but its sentinels still has them removed."""
    state_dir = _leave_sentinel("forgotten")

    actions = get_run_actions("forgotten")

    assert _names(actions) == ["State"]
    state = t.cast("FileSystemCleanupAction", actions[0])
    assert state.asset_paths == [state_dir]
