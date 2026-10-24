import errno
import fcntl
import logging
import os
import pickle
import shlex
import stat
from pathlib import Path
from unittest import mock

import pytest
import requests

from cstar.applications.roms_marbl.file_system import RomsFileSystemManager
from cstar.base.env import ENV_CSTAR_DATA_HOME, ENV_CSTAR_RUNID
from cstar.base.exceptions import CstarError
from cstar.execution.file_system import (
    UNKNOWN_SIZE,
    DirectoryManager,
    JobFileSystemManager,
    LockTimeoutError,
    LockUnsupportedError,
    StateDirectoryManager,
    get_backup_path,
    is_remote_resource,
    local_copy,
    local_copy_async,
    locked,
    remove_files,
    rotate_file,
    step_disk_usage,
    symlink_relative,
    write_bytes_atomic,
    write_local_copy,
    write_text_atomic,
)
from cstar.orchestration.models import Step
from cstar.orchestration.orchestration import LiveStep


@pytest.fixture
def populated_output_dir(tmp_path: Path) -> tuple[Path, list[Path]]:
    """Create a populated asset directory."""
    asset_root = tmp_path / "my_output_dir"
    fs = RomsFileSystemManager(asset_root)
    fs.prepare()

    files = [
        fs.input_dir / "some_input_file",
        fs.output_dir / "some_output_file",
        fs.temp_output_dir / "some_temp_file",
    ]

    for f in files:
        f.touch()

    return asset_root, files


def test_file_system_root() -> None:
    """Test that a user path is expanded to an absolute path."""
    user_path = Path("~/cstar-directory")
    fsm = JobFileSystemManager(user_path)

    assert fsm.root_dir.is_absolute()
    assert "~" not in str(fsm.root_dir)


def test_file_system_prepare(
    tmp_path: Path,
) -> None:
    """
    Test that fs.prepare correctly creates the expected set of directories.

    Parameters
    ----------
    tmp_path : Path
        The path to temporary test outputs.
    """
    output_dir = tmp_path / "my_output_dir"
    fs = RomsFileSystemManager(output_dir)
    fs.prepare()

    assert fs.output_dir.exists()
    assert fs.input_dir.exists()
    assert fs.temp_output_dir.exists()
    assert fs.codebases_dir.exists()
    assert fs.root_dir.exists()


def test_file_system_clear(
    populated_output_dir: tuple[Path, list[Path]],
) -> None:
    """
    Test that fs.clear correctly clears the working directory.

    Parameters
    ----------
    populated_output_dir : Path
        Fixture providing tmp_path and fake files
    """
    working_dir, _ = populated_output_dir

    fs = RomsFileSystemManager(working_dir)
    fs.prepare()

    (fs.compile_time_code_dir / "a.txt").touch()
    (fs.runtime_code_dir / "b.txt").touch()
    (fs.input_datasets_dir / "c.txt").touch()
    (fs.temp_output_dir / "d.txt").touch()
    (fs.run_dir / "e.txt").touch()
    (fs.run_dir / "f.yaml").touch()
    (fs.run_dir / "g.yml").touch()

    fs.clear()

    assert not fs.compile_time_code_dir.exists()
    assert not fs.runtime_code_dir.exists()
    assert not fs.input_datasets_dir.exists()
    assert not fs.temp_output_dir.exists()

    # confirm yaml in work_dir is not removed.
    assert not (fs.run_dir / "e.txt").exists()
    assert (fs.run_dir / "f.yaml").exists()
    assert (fs.run_dir / "g.yml").exists()


def test_file_system_pickle_job_fs(tmp_path: Path) -> None:
    """Verify the attributes of the file-system object match pre- and post-pickling."""
    fsm = JobFileSystemManager(tmp_path)

    pickled_fsm = pickle.dumps(fsm)
    unpickled: JobFileSystemManager = pickle.loads(pickled_fsm)

    for k in fsm.__dict__:
        assert getattr(fsm, k) == getattr(unpickled, k)


def test_file_system_pickle_roms_fs(tmp_path: Path) -> None:
    """Verify the attributes of the file-system object match pre- and post-pickling."""
    fsm = RomsFileSystemManager(tmp_path)

    pickled_fsm = pickle.dumps(fsm)
    unpickled: RomsFileSystemManager = pickle.loads(pickled_fsm)

    for k in fsm.__dict__:
        assert getattr(fsm, k) == getattr(unpickled, k)


def test_live_step(tmp_path: Path) -> None:
    """Verify parent-child traversal results in the correct root directories."""
    bp = tmp_path / "bp.yml"
    bp.touch()

    run_id = "fake-run-id"

    step_1 = Step(name="A", application="roms_marbl", blueprint=bp)
    step_2 = Step(name="B", application="roms_marbl", blueprint=bp)
    step_3 = Step(name="C", application="roms_marbl", blueprint=bp)

    with mock.patch.dict(
        os.environ,
        {
            ENV_CSTAR_RUNID: run_id,
        },
        clear=True,
    ):
        # use plan "Step" as parent (expect step_1 parent dir to step_2)
        ls_1 = LiveStep.from_step(step_1)
        # use "LiveStep" as parent
        ls_2 = LiveStep.from_step(step_2, update={"parent": step_1})
        # step from another live step (one with parent, one as base task)
        ls_3 = LiveStep.from_step(step_3, update={"parent": ls_2})
        ls_4 = LiveStep.from_step(ls_3)

        # updating name should change the dir name
        ls_5_name = "child of ls 2"
        ls_5 = LiveStep.from_step(
            ls_1, update={"name": ls_5_name}
        )  # override attributes

        task_dir_name = JobFileSystemManager._TASKS_NAME  # type: ignore
        actual = ls_1.working_dir

        directory_mgr = DirectoryManager()
        data_home = directory_mgr.data_home()
        expected = data_home / "workplan_runs" / run_id / task_dir_name / ls_1.safe_name
        assert actual == expected

        actual = ls_2.working_dir
        expected = ls_1.working_dir / task_dir_name / ls_2.safe_name  # type: ignore
        assert actual == expected

        actual = ls_3.working_dir
        expected = ls_2.working_dir / task_dir_name / ls_3.safe_name  # type: ignore
        assert actual == expected

        actual = ls_4.working_dir
        # confirm the parent carries over and ls4 is a child of ls2
        assert ls_2.working_dir
        expected = ls_2.working_dir / task_dir_name / ls_4.safe_name
        assert actual == expected

        actual = ls_5.working_dir
        expected = data_home / "workplan_runs" / run_id / task_dir_name / ls_5.safe_name
        assert actual == expected
        assert ls_5.name == ls_5_name  # confirm the update was honored


def test_blueprint_run_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the default blueprint working directory layout under the data home."""
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, str(tmp_path))

    actual = StateDirectoryManager.blueprint_run_dir("roms_marbl", "NA 8x8 / Test")

    assert actual == tmp_path / "blueprint_runs" / "ucla_roms" / "na-8x8-test"


@pytest.mark.parametrize(
    ("existing", "expected"),
    [
        pytest.param([], "ucla_roms/my-run", id="nothing-exists"),
        pytest.param(["roms_marbl/my-run"], "roms_marbl/my-run", id="legacy-run-kept"),
        pytest.param(
            ["roms_marbl/my-run", "ucla_roms/my-run"],
            "ucla_roms/my-run",
            id="canonical-wins",
        ),
        pytest.param(
            ["roms_marbl/other-run"], "ucla_roms/my-run", id="legacy-other-run-ignored"
        ),
    ],
)
@pytest.mark.parametrize("application", ["ucla_roms", "roms_marbl"])
def test_blueprint_run_dir_legacy_application_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    application: str,
    existing: list[str],
    expected: str,
) -> None:
    """A run made under an earlier application name is found where it ran.

    The application is canonicalised first, so the earlier name resolves
    exactly as the current one.
    """
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, str(tmp_path))
    for rel_path in existing:
        (tmp_path / "blueprint_runs" / rel_path).mkdir(parents=True)

    actual = StateDirectoryManager.blueprint_run_dir(application, "My Run")

    assert actual == tmp_path / "blueprint_runs" / expected


def test_blueprint_run_dir_legacy_file_is_not_a_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stray file at the earlier location is not adopted as a run directory."""
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, str(tmp_path))
    legacy_parent = tmp_path / "blueprint_runs" / "roms_marbl"
    legacy_parent.mkdir(parents=True)
    (legacy_parent / "my-run").write_text("not a directory")

    actual = StateDirectoryManager.blueprint_run_dir("ucla_roms", "my-run")

    assert actual == tmp_path / "blueprint_runs" / "ucla_roms" / "my-run"


def test_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify a workplan run's directory layout under the data home."""
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, str(tmp_path))

    actual = StateDirectoryManager.data_dir("my-run")

    assert actual == tmp_path / "workplan_runs" / "my-run"


@pytest.mark.parametrize(
    ("existing", "expected"),
    [
        pytest.param(["my-run/tasks"], "my-run", id="legacy-run-root-kept"),
        pytest.param(["my-run"], "workplan_runs/my-run", id="legacy-dir-without-tasks"),
        pytest.param(
            ["my-run/tasks", "workplan_runs/my-run"],
            "workplan_runs/my-run",
            id="new-root-wins",
        ),
    ],
)
def test_data_dir_legacy_run_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    existing: list[str],
    expected: str,
) -> None:
    """A run started under the old layout keeps its root directly in the data home."""
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, str(tmp_path))
    for rel_path in existing:
        (tmp_path / rel_path).mkdir(parents=True)

    actual = StateDirectoryManager.data_dir("my-run")

    assert actual == tmp_path / expected


def test_blueprint_run_dir_rejects_name_without_slug(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A name with no path-safe characters must not collapse onto the application directory."""
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, str(tmp_path))

    with pytest.raises(ValueError, match="empty"):
        StateDirectoryManager.blueprint_run_dir("roms_marbl", "---")


@pytest.mark.parametrize(
    "uri",
    [
        "http://example.com/resource",
        "https://example.com/resource",
        "ftp://example.com/resource",
        "sftp://example.com/resource",
        "smb://host/share/resource",
        "s3://example-bucket.s3.amazonaws.com/folder/resource",
    ],
)
def test_file_system_is_remote_resource_remote(uri: str) -> None:
    """Verify that remote URIs are correctly interpreted as remote resources."""
    assert is_remote_resource(uri), "Remote URI was not properly identified"


@pytest.mark.parametrize(
    "uri",
    ["/", "/foo/bar", "\\\\host\\share\resource"],
)
def test_file_system_is_remote_resource_local(uri: str) -> None:
    """Verify that local URIs are correctly interpreted as remote resources."""
    assert not is_remote_resource(uri), "Local URI was not properly identified"


@pytest.mark.parametrize(
    "uri",
    [
        "",
        " ",
        "  ",
        "\n",
    ],
)
def test_file_system_is_remote_resource_empty(uri: str) -> None:
    """Verify that an empty URI results in an exception"""
    with pytest.raises(ValueError, match="Invalid resource URI"):
        _ = is_remote_resource(uri)


def test_file_system_local_copy_failed_retrieval() -> None:
    """Verify that a failure while retrieving a remote resource is handled as expected."""
    uri = "http://example.com/resource"

    mock_request = mock.Mock()
    mock_sc_prop = mock.PropertyMock(return_value=404)
    type(mock_request).status_code = mock_sc_prop

    mock_req_fn = mock.MagicMock(return_value=mock_request)

    with (
        mock.patch("cstar.execution.file_system.request", new=mock_req_fn),
        pytest.raises(FileNotFoundError, match="Unable to retrieve remote file"),
    ):
        with local_copy(uri):
            assert False

    mock_req_fn.assert_called_once()
    mock_sc_prop.assert_called_once()


@pytest.mark.parametrize(
    "uri",
    [
        "http://example.com/resource",
        "https://example.com/resource",
        "ftp://example.com/resource",
        "sftp://example.com/resource",
        "smb://host/share/resource",
        "s3://example-bucket.s3.amazonaws.com/folder/resource",
    ],
)
def test_file_system_local_copy(uri: str) -> None:
    """Verify that a local copy of a remote resource is correctly written."""
    mock_body = "mocked-resource-body"

    mock_request = mock.Mock()
    mock_sc_prop = mock.PropertyMock(return_value=200)
    mock_text_prop = mock.PropertyMock(return_value=mock_body)
    type(mock_request).status_code = mock_sc_prop
    type(mock_request).text = mock_text_prop

    mock_req_fn = mock.MagicMock(return_value=mock_request)

    with mock.patch("cstar.execution.file_system.request", new=mock_req_fn):
        with local_copy(uri) as local_path:
            assert local_path.exists()
            assert local_path.read_text() == mock_body

        mock_req_fn.assert_called_once()
        mock_sc_prop.assert_called_once()
        mock_text_prop.assert_called_once()


def test_file_system_local_copy_dne(tmp_path: Path) -> None:
    """Verify that a local path that doesn't exist results in an exception."""
    path = tmp_path / "file.txt"

    with pytest.raises(FileNotFoundError, match="File not found"):
        with local_copy(path.as_posix()) as _local_path:
            ...


def test_file_system_local_copy_passthrough(tmp_path: Path) -> None:
    """Verify that a local file is returned directly and is not copied."""
    path = tmp_path / "file.txt"
    content = "yo yo yo "
    path.write_text(content)

    with local_copy(path.as_posix()) as local_path:
        assert local_path == path.expanduser().resolve()
        assert local_path.read_text() == content


@pytest.mark.asyncio
async def test_file_system_local_copy_async() -> None:
    """Verify that a local copy of a remote resource is correctly written."""
    uri = "http://example.com/resource"
    mock_body = "mocked-resource-body"

    mock_request = mock.Mock()
    mock_sc_prop = mock.PropertyMock(return_value=200)
    mock_text_prop = mock.PropertyMock(return_value=mock_body)
    type(mock_request).status_code = mock_sc_prop
    type(mock_request).text = mock_text_prop

    mock_req_fn = mock.MagicMock(return_value=mock_request)

    with mock.patch("cstar.execution.file_system.request", new=mock_req_fn):
        async with local_copy_async(uri) as local_path:
            assert local_path.exists()
            assert local_path.read_text() == mock_body

        mock_req_fn.assert_called_once()
        mock_sc_prop.assert_called_once()
        mock_text_prop.assert_called_once()


def test_file_system_remove_files_path_DNE(tmp_path: Path) -> None:
    """Verify that passing a non-existant path is logged and handled gracefully."""
    path = tmp_path / "dne.txt"

    # nothing was removed - expect `False` as retval
    assert not remove_files(path, "*.txt")


@pytest.mark.asyncio
async def test_file_system_remove_files_file_path(tmp_path: Path) -> None:
    """Verify that passing a path to a file instead of a directory results
    in an exception.
    """
    path = tmp_path / "dne.txt"
    path.touch()

    with pytest.raises(ValueError, match="Directory is required"):
        _ = remove_files(path, "*.txt")


def test_file_system_remove_files_no_content(tmp_path: Path) -> None:
    """Verify that an empty directory results in retval==`False`."""
    path = tmp_path / "content"
    path.mkdir(parents=True, exist_ok=False)

    assert not remove_files(path, "*.txt")


def test_file_system_remove_files_no_matches(tmp_path: Path) -> None:
    """Verify that an empty directory results in retval==`False`."""
    path = tmp_path / "content"
    path.mkdir(parents=True, exist_ok=False)

    log = path / "foo.log"
    log.touch()

    assert not remove_files(path, "*.txt")


def test_file_system_remove_files(tmp_path: Path) -> None:
    """Verify that wildcard matches are removed as expected."""
    path = tmp_path / "content"
    path.mkdir(parents=True, exist_ok=False)

    log = path / "foo.txt"
    log.touch()

    assert remove_files(path, "*.txt")
    assert not log.exists()


def test_file_system_remove_files_directories(tmp_path: Path) -> None:
    """Verify that directories matching the wildcard are deleted."""
    path = tmp_path / "content"
    file_match = tmp_path / "content.log"
    file_match.touch()

    subdir = path / "nested"
    subdir.mkdir(parents=True, exist_ok=False)

    log = subdir / "foo.txt"
    log.touch()

    # /content
    #  - nested
    #    - foo.txt
    # /content.log

    assert remove_files(tmp_path, "content*")
    assert not path.exists()
    assert not file_match.exists()
    assert not log.exists()


def test_file_system_get_backup_path_first_backup(tmp_path: Path) -> None:
    """Verify that the first backup path appends the backup extension to the
    full original file name.
    """
    source = tmp_path / "blueprint.yaml"
    source.touch()

    backup_path = get_backup_path(source)

    assert backup_path == tmp_path / "blueprint.yaml.bak"


def test_file_system_get_backup_path_does_not_require_source(
    tmp_path: Path,
) -> None:
    """Verify that a backup path is produced even if the source does not exist."""
    source = tmp_path / "blueprint.yaml"

    backup_path = get_backup_path(source)

    assert backup_path == tmp_path / "blueprint.yaml.bak"


def test_file_system_get_backup_path_existing_backup(tmp_path: Path) -> None:
    """Verify that an existing backup is never overwritten; a numeric
    suffix is appended instead.
    """
    source = tmp_path / "blueprint.yaml"
    source.touch()
    (tmp_path / "blueprint.yaml.bak").touch()

    backup_path = get_backup_path(source)

    assert backup_path == tmp_path / "blueprint.yaml.bak.001"


def test_file_system_get_backup_path_multiple_existing_backups(
    tmp_path: Path,
) -> None:
    """Verify that the numeric suffix increments past all existing backups."""
    source = tmp_path / "blueprint.yaml"
    source.touch()
    (tmp_path / "blueprint.yaml.bak").touch()
    (tmp_path / "blueprint.yaml.bak.001").touch()
    (tmp_path / "blueprint.yaml.bak.002").touch()

    backup_path = get_backup_path(source)

    assert backup_path == tmp_path / "blueprint.yaml.bak.003"


def test_file_system_get_backup_path_multidot_name(tmp_path: Path) -> None:
    """Verify that a versioned file name keeps its full stem in the backup."""
    source = tmp_path / "blueprint.1.0.0.yaml"
    source.touch()

    backup_path = get_backup_path(source)

    assert backup_path == tmp_path / "blueprint.1.0.0.yaml.bak"


def test_file_system_get_backup_path_no_suffix(tmp_path: Path) -> None:
    """Verify that a file without an extension gets the backup extension."""
    source = tmp_path / "blueprint"
    source.touch()

    backup_path = get_backup_path(source)

    assert backup_path == tmp_path / "blueprint.bak"


@pytest.mark.parametrize("backup_ext", ["orig", ".orig"])
def test_file_system_get_backup_path_custom_ext(
    tmp_path: Path,
    backup_ext: str,
) -> None:
    """Verify that a custom extension is honored, with or without a leading dot."""
    source = tmp_path / "blueprint.yaml"
    source.touch()

    backup_path = get_backup_path(source, backup_ext=backup_ext)

    assert backup_path == tmp_path / "blueprint.yaml.orig"


def test_rotate_file_no_prior_file(tmp_path: Path) -> None:
    """Verify nothing is rotated, and `None` is returned, when the target
    file does not exist.
    """
    target = tmp_path / "step.out"

    assert rotate_file(target) is None
    assert not target.exists()


def test_rotate_file_first_rotation(tmp_path: Path) -> None:
    """Verify an existing file is renamed to `<name>.1` on first rotation."""
    target = tmp_path / "step.out"
    target.write_text("attempt 1")

    rotated = rotate_file(target)

    assert rotated == tmp_path / "step.out.1"
    assert rotated.read_text() == "attempt 1"
    assert not target.exists()


def test_rotate_file_second_rotation_skips_existing(tmp_path: Path) -> None:
    """Verify a prior `.1` rotation is preserved and the new one becomes `.2`."""
    target = tmp_path / "step.out"
    target.write_text("attempt 2")
    (tmp_path / "step.out.1").write_text("attempt 1")

    rotated = rotate_file(target)

    assert rotated == tmp_path / "step.out.2"
    assert rotated.read_text() == "attempt 2"
    assert (tmp_path / "step.out.1").read_text() == "attempt 1"
    assert not target.exists()


async def test_step_disk_usage_missing_dir(tmp_path: Path) -> None:
    """Verify a missing step directory reports zero usage without running `du`."""
    with mock.patch("cstar.execution.file_system._run_cmd") as mock_run_cmd:
        size = await step_disk_usage(tmp_path / "does-not-exist")

    assert size == 0
    mock_run_cmd.assert_not_called()


async def test_step_disk_usage_without_nested_steps(tmp_path: Path) -> None:
    """Verify a step without a `tasks` subdirectory is measured with one `du`."""
    step_root = tmp_path / "step"
    (step_root / "output").mkdir(parents=True)

    with mock.patch(
        "cstar.execution.file_system._run_cmd", return_value=f"7\t{step_root}\n"
    ) as mock_run_cmd:
        size = await step_disk_usage(step_root)

    assert size == 7
    assert mock_run_cmd.call_count == 1
    assert shlex.quote(str(step_root)) in mock_run_cmd.call_args.args[0]


async def test_step_disk_usage_subtracts_tasks_dir(tmp_path: Path) -> None:
    """Verify the nested `tasks` subtree is measured separately and subtracted."""
    step_root = tmp_path / "step"
    tasks_dir = step_root / "tasks"
    (step_root / "output").mkdir(parents=True)
    tasks_dir.mkdir()

    outputs = [f"12\t{step_root}\n", f"4\t{tasks_dir}\n"]

    with mock.patch(
        "cstar.execution.file_system._run_cmd", side_effect=outputs
    ) as mock_run_cmd:
        size = await step_disk_usage(step_root)

    assert size == 8
    assert mock_run_cmd.call_count == 2
    assert shlex.quote(str(tasks_dir)) in mock_run_cmd.call_args_list[1].args[0]


@pytest.mark.parametrize(
    "outputs",
    [
        ["garbage"],
        [""],
        ["12\t/step\n", "nope"],
    ],
    ids=["unparseable-total", "empty-total", "unparseable-nested"],
)
async def test_step_disk_usage_failures_report_unknown(
    tmp_path: Path, outputs: list[str]
) -> None:
    """Verify any unparseable `du` output results in `UNKNOWN_SIZE`."""
    step_root = tmp_path / "step"
    (step_root / "tasks").mkdir(parents=True)

    with mock.patch("cstar.execution.file_system._run_cmd", side_effect=outputs):
        size = await step_disk_usage(step_root)

    assert size == UNKNOWN_SIZE


async def test_step_disk_usage_unreadable_dir_reports_unknown(tmp_path: Path) -> None:
    """Verify an `OSError` while inspecting the step directory yields `UNKNOWN_SIZE`."""
    step_root = tmp_path / "step"
    step_root.mkdir()

    with (
        mock.patch("pathlib.Path.is_dir", side_effect=PermissionError("denied")),
        mock.patch("cstar.execution.file_system._run_cmd") as mock_run_cmd,
    ):
        size = await step_disk_usage(step_root)

    assert size == UNKNOWN_SIZE
    mock_run_cmd.assert_not_called()


class TestSourceDataHome:
    """The source-data cache: ``<root>/cstar/source-data``, root chosen by
    ``CSTAR_PROJECT_HOME`` (default ``$PROJECT``), then scratch, then home.
    """

    @pytest.fixture(autouse=True)
    def _isolated_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in (
            "CSTAR_PROJECT_HOME",
            "PROJECT",
            "SCRATCH",
            "SCRATCH_DIR",
            "LOCAL_SCRATCH",
        ):
            monkeypatch.delenv(var, raising=False)
        (tmp_path / "home").mkdir()
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        self._set_system_scratch(monkeypatch, None)

    @staticmethod
    def _set_system_scratch(monkeypatch: pytest.MonkeyPatch, root: Path | None) -> None:
        """Stub the SystemContext hook; ``None`` makes the system unknown."""

        class _Ctx:
            @classmethod
            def scratch_root(cls) -> Path:
                assert root is not None
                return root

        def _get() -> type:
            if root is None:
                raise CstarError("Unknown system requested: nowhere")
            return _Ctx

        monkeypatch.setattr("cstar.system.manager.get_system_context", _get)

    @staticmethod
    def _expected(root: Path) -> Path:
        return root.resolve() / "cstar" / "source-data"

    @staticmethod
    def _make_legacy(root: Path) -> Path:
        legacy = root / "cstar-forge-data" / "source-data"
        legacy.mkdir(parents=True)
        (legacy / "glorys.nc").write_text("data")
        return legacy

    def test_project_home_wins_over_project_and_scratch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CSTAR_PROJECT_HOME", str(tmp_path / "explicit"))
        monkeypatch.setenv("PROJECT", str(tmp_path / "project"))
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))
        assert DirectoryManager.source_data_home() == self._expected(
            tmp_path / "explicit"
        )

    def test_project_wins_over_scratch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROJECT", str(tmp_path / "project"))
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))
        assert DirectoryManager.source_data_home() == self._expected(
            tmp_path / "project"
        )

    def test_scratch_used_without_project(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))
        assert DirectoryManager.source_data_home() == self._expected(
            tmp_path / "scratch"
        )

    def test_scratch_dir_alone_is_used(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Any CSTAR_SCRATCH_DIRS variable counts, not only SCRATCH."""
        monkeypatch.setenv("SCRATCH_DIR", str(tmp_path / "scratch-dir"))
        assert DirectoryManager.source_data_home() == self._expected(
            tmp_path / "scratch-dir"
        )

    def test_system_scratch_hook_used_without_variables(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._set_system_scratch(monkeypatch, tmp_path / "sys-scratch" / "user")
        assert DirectoryManager.source_data_home() == self._expected(
            tmp_path / "sys-scratch" / "user"
        )

    def test_falls_back_to_home(self, tmp_path: Path) -> None:
        assert DirectoryManager.source_data_home() == self._expected(tmp_path / "home")

    def test_source_data_home_creates_nothing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))
        home = DirectoryManager.source_data_home()
        assert not home.exists()
        assert not (tmp_path / "scratch").exists()

    def test_ensure_adopts_legacy_cache_with_relative_symlink(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        scratch = tmp_path / "scratch"
        self._make_legacy(scratch)
        monkeypatch.setenv("SCRATCH", str(scratch))

        home = DirectoryManager.ensure_source_data_home()

        assert home == self._expected(scratch)
        assert home.is_symlink()
        assert home.readlink() == Path("../cstar-forge-data/source-data")
        assert (home / "glorys.nc").read_text() == "data"

    def test_ensure_adopts_legacy_cache_under_system_scratch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Bouchet-style: the root comes from the SystemContext hook."""
        root = tmp_path / "scratch_pi_abc" / "user"
        self._make_legacy(root)
        self._set_system_scratch(monkeypatch, root)

        home = DirectoryManager.ensure_source_data_home()

        assert home == self._expected(root)
        assert home.is_symlink()
        assert home.readlink() == Path("../cstar-forge-data/source-data")
        assert (home / "glorys.nc").read_text() == "data"

    def _make_symlinked_home(self, tmp_path: Path) -> tuple[Path, Path]:
        """A home directory and legacy cache behind different symlinks:
        ``cstar`` and ``cstar-forge-data`` in the home directory are symlinks
        into different directories elsewhere.

        Returns the fake home and the (real) legacy source-data directory.
        """
        mount = tmp_path / "mnt" / "volume" / "group"
        real_cstar = mount / "user" / "name" / "cstar"
        real_cstar.mkdir(parents=True)
        legacy = self._make_legacy(mount)
        home = tmp_path / "linked-home"
        home.mkdir()
        (home / "cstar").symlink_to(real_cstar)
        (home / "cstar-forge-data").symlink_to(mount / "cstar-forge-data")
        return home, legacy

    def test_ensure_adopts_legacy_cache_behind_symlinks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The link must resolve from the real ``cstar`` directory, not from the
        symlink that points to it.
        """
        root, legacy = self._make_symlinked_home(tmp_path)
        monkeypatch.setenv("SCRATCH", str(root))

        home = DirectoryManager.ensure_source_data_home()

        assert home == self._expected(root)
        assert home.exists()
        assert home.is_symlink()
        assert not home.readlink().is_absolute()
        assert home.resolve() == legacy.resolve()
        assert (home / "glorys.nc").read_text() == "data"

        link_target = home.readlink()
        assert DirectoryManager.ensure_source_data_home() == home
        assert home.is_symlink()
        assert home.readlink() == link_target
        assert home.resolve() == legacy.resolve()

    def test_ensure_repairs_dangling_link_from_old_relpath(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Releases 0.16.0 and 0.16.1 linked with a lexical relpath, which dangles
        behind symlinked parents; that link is replaced instead of refused.
        """
        root, legacy = self._make_symlinked_home(tmp_path)
        monkeypatch.setenv("SCRATCH", str(root))
        standard = self._expected(root)
        standard.symlink_to("../cstar-forge-data/source-data")
        assert standard.is_symlink()
        assert not standard.exists()

        home = DirectoryManager.ensure_source_data_home()

        assert home == standard
        assert home.is_dir()
        assert home.is_symlink()
        assert not home.readlink().is_absolute()
        assert home.resolve() == legacy.resolve()
        assert (home / "glorys.nc").read_text() == "data"

    def test_ensure_does_not_link_when_standard_dir_exists(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        scratch = tmp_path / "scratch"
        self._make_legacy(scratch)
        standard = self._expected(scratch)
        standard.mkdir(parents=True)
        monkeypatch.setenv("SCRATCH", str(scratch))

        home = DirectoryManager.ensure_source_data_home()

        assert home == standard
        assert not home.is_symlink()
        assert not (home / "glorys.nc").exists()

    def test_ensure_creates_plain_directory_without_legacy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))

        home = DirectoryManager.ensure_source_data_home()

        assert home == self._expected(tmp_path / "scratch")
        assert home.is_dir()
        assert not home.is_symlink()

    def test_ensure_refuses_dangling_symlink(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        scratch = tmp_path / "scratch"
        standard = self._expected(scratch)
        standard.parent.mkdir(parents=True)
        standard.symlink_to(tmp_path / "unmounted")
        monkeypatch.setenv("SCRATCH", str(scratch))

        with pytest.raises(CstarError, match="does not exist"):
            DirectoryManager.ensure_source_data_home()

        assert standard.is_symlink()
        assert not (tmp_path / "unmounted").exists()

    def test_ensure_refuses_other_dangling_link_when_legacy_exists(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only the exact link left by the old code is repaired; any other
        dangling link is refused even when a legacy cache could be adopted.
        """
        root, _ = self._make_symlinked_home(tmp_path)
        monkeypatch.setenv("SCRATCH", str(root))
        standard = self._expected(root)
        standard.symlink_to("../unmounted/source-data")

        with pytest.raises(CstarError, match="does not exist"):
            DirectoryManager.ensure_source_data_home()

        assert standard.is_symlink()
        assert standard.readlink() == Path("../unmounted/source-data")

    def test_ensure_refuses_old_relpath_link_without_legacy_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The old release's link text is repaired only when the legacy cache it
        names exists; otherwise the link is refused, not replaced by an empty
        directory.
        """
        scratch = tmp_path / "scratch"
        standard = self._expected(scratch)
        standard.parent.mkdir(parents=True)
        standard.symlink_to("../cstar-forge-data/source-data")
        monkeypatch.setenv("SCRATCH", str(scratch))
        assert not (scratch / "cstar-forge-data").exists()

        with pytest.raises(CstarError, match="does not exist"):
            DirectoryManager.ensure_source_data_home()

        assert standard.is_symlink()
        assert standard.readlink() == Path("../cstar-forge-data/source-data")

    def test_ensure_tolerates_losing_the_link_race(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Another process creating the cache first is not an error."""
        scratch = tmp_path / "scratch"
        self._make_legacy(scratch)
        monkeypatch.setenv("SCRATCH", str(scratch))

        def _lose_race(self: Path, target: object, *args: object, **kwargs: object):
            self.mkdir()
            raise FileExistsError(str(self))

        monkeypatch.setattr(Path, "symlink_to", _lose_race)

        home = DirectoryManager.ensure_source_data_home()

        assert home == self._expected(scratch)
        assert home.is_dir()


class TestProjectHome:
    """The project directory and its runs area: ``CSTAR_PROJECT_HOME`` (default
    ``$PROJECT``), and no fallback when neither is set.
    """

    @pytest.fixture(autouse=True)
    def _isolated_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in ("CSTAR_PROJECT_HOME", "PROJECT"):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))

    def test_explicit_home_wins_over_project(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CSTAR_PROJECT_HOME", str(tmp_path / "explicit"))
        monkeypatch.setenv("PROJECT", str(tmp_path / "project"))

        assert DirectoryManager.project_home() == (tmp_path / "explicit").resolve()
        assert DirectoryManager.project_runs_home() == (
            (tmp_path / "explicit").resolve() / "cstar" / "runs"
        )

    def test_project_variable_alone_is_used(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROJECT", str(tmp_path / "project"))

        assert DirectoryManager.project_home() == (tmp_path / "project").resolve()
        assert DirectoryManager.project_runs_home() == (
            (tmp_path / "project").resolve() / "cstar" / "runs"
        )

    def test_neither_variable_means_no_project(self) -> None:
        """Scratch is no project area: only the source-data root falls back to it."""
        assert DirectoryManager.project_home() is None
        assert DirectoryManager.project_runs_home() is None

    def test_home_is_expanded_and_resolved(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "home").mkdir()
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setenv("CSTAR_PROJECT_HOME", "~/proj/../proj")

        assert DirectoryManager.project_home() == (tmp_path / "home" / "proj").resolve()

    def test_runs_home_creates_nothing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROJECT", str(tmp_path / "project"))

        DirectoryManager.project_runs_home()

        assert not (tmp_path / "project").exists()

    def test_source_data_paths_keep_the_project_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The source-data cache sits beside the runs area, under the same root."""
        monkeypatch.setenv("PROJECT", str(tmp_path / "project"))

        runs = DirectoryManager.project_runs_home()
        assert runs is not None
        assert DirectoryManager.source_data_home() == runs.parent / "source-data"


def test_run_directories_are_under_the_data_home() -> None:
    """The workplan and blueprint runs live in two directories of the data home,
    where `data_dir` and `blueprint_run_dir` put a run.
    """
    data_home = DirectoryManager.data_home()

    assert StateDirectoryManager.workplan_runs_dir() == data_home / "workplan_runs"
    assert StateDirectoryManager.blueprint_runs_dir() == data_home / "blueprint_runs"
    assert StateDirectoryManager.data_dir("a-run").parent == (
        StateDirectoryManager.workplan_runs_dir()
    )
    assert StateDirectoryManager.blueprint_run_dir(
        "hello_world", "A Name"
    ).parent.parent == (StateDirectoryManager.blueprint_runs_dir())


class TestWriteLocalCopy:
    """Tests for `write_local_copy`."""

    def test_writes_response_text(self, tmp_path: Path) -> None:
        """A 200 response is written under the URL's file name."""
        url = "https://example.com/plans/workplan.yaml"
        response = mock.Mock(status_code=200, text="name: wp\n")

        with mock.patch(
            "cstar.execution.file_system.request", return_value=response
        ) as mock_request:
            local_path = write_local_copy(url, tmp_path)

        mock_request.assert_called_once_with("GET", url, timeout=10.0)
        assert local_path == (tmp_path / "workplan.yaml").resolve()
        assert local_path.read_text() == "name: wp\n"

    @pytest.mark.parametrize(
        "error",
        [
            requests.ReadTimeout("Read timed out. (read timeout=10.0)"),
            requests.ConnectionError("Connection aborted."),
        ],
    )
    def test_request_failure_is_file_not_found(
        self, tmp_path: Path, error: requests.RequestException
    ) -> None:
        """A timeout or connection failure is reported like a missing file, so
        callers' existing FileNotFoundError handling covers it.
        """
        url = "https://example.com/plans/workplan.yaml"

        with (
            mock.patch("cstar.execution.file_system.request", side_effect=error),
            pytest.raises(FileNotFoundError, match=type(error).__name__) as exc_info,
        ):
            write_local_copy(url, tmp_path)

        assert exc_info.value.filename == url
        assert not (tmp_path / "workplan.yaml").exists()


def test_symlink_relative_text_is_relative(tmp_path: Path) -> None:
    """The link text is relative, creates missing parents, and resolves to the target."""
    target = tmp_path / "data" / "grid.nc"
    target.parent.mkdir()
    target.write_text("data")
    link = tmp_path / "run" / "input" / "grid.nc"

    symlink_relative(link, target)

    assert link.is_symlink()
    assert link.readlink() == Path("../../data/grid.nc")
    assert link.resolve() == target.resolve()
    assert link.read_text() == "data"


def test_symlink_relative_survives_moving_the_tree(tmp_path: Path) -> None:
    """A link and target moved together keep working, as relative links should."""
    tree = tmp_path / "tree"
    (tree / "a").mkdir(parents=True)
    (tree / "b").mkdir()
    (tree / "b" / "file").write_text("data")
    symlink_relative(tree / "a" / "link", tree / "b" / "file")

    moved = tmp_path / "moved"
    tree.rename(moved)

    assert (moved / "a" / "link").read_text() == "data"


def test_symlink_relative_through_symlinked_parent(tmp_path: Path) -> None:
    """A link directory reached through a symlink is measured from where it really is.

    The kernel resolves `..` in link text from the physical directory, so text
    computed from the symlinked spelling of the path would point elsewhere.
    """
    target = tmp_path / "a" / "file"
    target.parent.mkdir()
    target.write_text("data")
    physical = tmp_path / "b" / "c" / "real"
    physical.mkdir(parents=True)
    (tmp_path / "a" / "alias").symlink_to(physical)

    link = tmp_path / "a" / "alias" / "link"
    symlink_relative(link, target)

    assert link.read_text() == "data"
    assert link.resolve() == target.resolve()


def test_symlink_relative_existing_link_raises(tmp_path: Path) -> None:
    """An existing link is left alone; callers unlink first when they mean to replace."""
    target = tmp_path / "target"
    target.write_text("data")
    link = tmp_path / "link"
    link.symlink_to(target)

    with pytest.raises(FileExistsError):
        symlink_relative(link, target)


def test_symlink_relative_within_keeps_relative_when_both_are_inside(
    tmp_path: Path,
) -> None:
    """A link and target both inside `within` are linked relatively."""
    home = tmp_path / "home"
    target = home / "source-run" / "output" / "file.nc"
    target.parent.mkdir(parents=True)
    target.write_text("data")
    link = home / "this-run" / "input" / "file.nc"

    symlink_relative(link, target, within=home)

    assert link.readlink() == Path("../../source-run/output/file.nc")
    assert link.read_text() == "data"


def test_symlink_relative_within_falls_back_to_absolute_for_outside_target(
    tmp_path: Path,
) -> None:
    """A target outside `within` is linked by absolute path, not a `..` chain into
    another tree.
    """
    home = tmp_path / "home"
    target = tmp_path / "elsewhere" / "file.nc"
    target.parent.mkdir(parents=True)
    target.write_text("data")
    link = home / "this-run" / "input" / "file.nc"

    symlink_relative(link, target, within=home)

    assert link.readlink() == target.resolve()
    assert link.read_text() == "data"


def test_symlink_relative_within_falls_back_to_absolute_for_outside_link(
    tmp_path: Path,
) -> None:
    """A link outside `within`, such as a run kept elsewhere, is absolute even when
    its target is inside.
    """
    home = tmp_path / "home"
    target = home / "source-run" / "file.nc"
    target.parent.mkdir(parents=True)
    target.write_text("data")
    link = tmp_path / "scratch" / "run" / "input" / "file.nc"

    symlink_relative(link, target, within=home)

    assert link.readlink() == target.resolve()
    assert link.read_text() == "data"


def test_write_bytes_atomic_writes_content_and_creates_parents(tmp_path: Path) -> None:
    """Verify the data lands in the file and missing parents are created."""
    target = tmp_path / "a" / "b" / "file.bin"

    write_bytes_atomic(target, b"payload")

    assert target.read_bytes() == b"payload"
    assert [p.name for p in target.parent.iterdir()] == ["file.bin"]


def test_write_bytes_atomic_new_file_honours_the_umask(tmp_path: Path) -> None:
    """Verify a new file gets the umask default without toggling the process umask."""
    target = tmp_path / "fresh.txt"
    previous = os.umask(0o027)
    try:
        with mock.patch.object(os, "umask", side_effect=AssertionError("toggled")):
            write_bytes_atomic(target, b"data")
    finally:
        os.umask(previous)

    assert stat.S_IMODE(target.stat().st_mode) == 0o666 & ~0o027


def test_write_bytes_atomic_without_sync_still_replaces(tmp_path: Path) -> None:
    """Verify `sync=False` skips the fsync and still renames the data into place."""
    target = tmp_path / "state.yaml"
    target.write_bytes(b"old")

    with mock.patch.object(os, "fsync") as fsync:
        write_bytes_atomic(target, b"new", sync=False)

    fsync.assert_not_called()
    assert target.read_bytes() == b"new"


def test_write_bytes_atomic_keeps_existing_mode(tmp_path: Path) -> None:
    """Verify replacing a file keeps its permissions."""
    target = tmp_path / "script.sh"
    target.write_bytes(b"old")
    target.chmod(0o750)

    write_bytes_atomic(target, b"new")

    assert target.read_bytes() == b"new"
    assert stat.S_IMODE(target.stat().st_mode) == 0o750


def test_write_bytes_atomic_follows_symlink(tmp_path: Path) -> None:
    """Verify a symlink at the path keeps its link and its target is replaced."""
    real = tmp_path / "real.txt"
    real.write_bytes(b"old")
    link = tmp_path / "link.txt"
    link.symlink_to(real)

    write_bytes_atomic(link, b"new")

    assert link.is_symlink()
    assert real.read_bytes() == b"new"


def test_write_bytes_atomic_failure_leaves_original_intact(tmp_path: Path) -> None:
    """Verify a failure before the rename keeps the original and leaves no temp file."""
    target = tmp_path / "atomic" / "file.txt"
    target.parent.mkdir()
    target.write_bytes(b"original")

    with (
        mock.patch("cstar.execution.file_system.os.replace", side_effect=OSError("x")),
        pytest.raises(OSError, match="x"),
    ):
        write_bytes_atomic(target, b"replacement")

    assert target.read_bytes() == b"original"
    assert [p.name for p in target.parent.iterdir()] == ["file.txt"]


def test_write_text_atomic_encodes_text(tmp_path: Path) -> None:
    """Verify text is encoded with the requested codec."""
    target = tmp_path / "sub" / "file.txt"

    write_text_atomic(target, "caf\u00e9")
    assert target.read_bytes() == "caf\u00e9".encode()

    write_text_atomic(target, "caf\u00e9", encoding="latin-1")
    assert target.read_bytes() == b"caf\xe9"


def _is_locked(path: Path) -> bool:
    """Whether another holder has an exclusive `flock` on `path`."""
    with path.open("a") as probe:
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(probe, fcntl.LOCK_UN)
        return False


def test_locked_holds_an_exclusive_lock_until_exit(tmp_path: Path) -> None:
    """Verify the lock file is created with its parents and held only inside the block."""
    lock_path = tmp_path / "missing" / "parent" / "entry.lock"

    with locked(lock_path):
        assert lock_path.is_file()
        assert _is_locked(lock_path)

    assert lock_path.is_file()
    assert not _is_locked(lock_path)


def test_locked_with_timeout_logs_the_wait_and_gives_up(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify a held lock is polled, the wait is logged, and the timeout raises."""
    lock_path = tmp_path / "entry.lock"
    caplog.set_level(logging.INFO, logger="cstar.execution.file_system")

    with (
        mock.patch("cstar.execution.file_system.LOCK_POLL_INTERVAL", 0.05),
        locked(lock_path),
        pytest.raises(LockTimeoutError, match="entry.lock"),
        locked(lock_path, timeout=0.2),
    ):
        pytest.fail("the lock was taken twice")

    assert f"Waiting for the lock at {lock_path}" in caplog.text


@pytest.mark.parametrize("code", [errno.ENOLCK, errno.EOPNOTSUPP, errno.ENOSYS])
def test_locked_names_an_unsupported_file_system(tmp_path: Path, code: int) -> None:
    """Verify `flock` errors meaning "cannot lock here" raise `LockUnsupportedError`."""
    lock_path = tmp_path / "entry.lock"

    with (
        mock.patch(
            "cstar.execution.file_system.fcntl.flock",
            side_effect=OSError(code, os.strerror(code)),
        ),
        pytest.raises(LockUnsupportedError) as raised,
        locked(lock_path, timeout=1.0),
    ):
        pytest.fail("the lock was taken")

    assert raised.value.errno == code


def test_locked_reraises_other_flock_errors(tmp_path: Path) -> None:
    """Verify an I/O error from `flock` is not mistaken for missing lock support."""
    with (
        mock.patch(
            "cstar.execution.file_system.fcntl.flock",
            side_effect=OSError(errno.EIO, os.strerror(errno.EIO)),
        ),
        pytest.raises(OSError) as raised,
        locked(tmp_path / "entry.lock"),
    ):
        pytest.fail("the lock was taken")

    assert not isinstance(raised.value, LockUnsupportedError)
    assert raised.value.errno == errno.EIO
