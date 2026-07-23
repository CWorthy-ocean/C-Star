import os
import pickle
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_DATA_HOME, ENV_CSTAR_RUNID
from cstar.execution.file_system import (
    JobFileSystemManager,
    RomsFileSystemManager,
    is_remote_resource,
    local_copy,
    local_copy_async,
    remove_files,
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
        fs.joined_output_dir / "some_joined_file",
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
    assert fs.joined_output_dir.exists()
    assert fs._codebases_dir.exists()
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
    (fs.joined_output_dir / "d.txt").touch()
    (fs.run_dir / "e.txt").touch()
    (fs.run_dir / "f.yaml").touch()
    (fs.run_dir / "g.yml").touch()

    fs.clear()

    assert not fs.compile_time_code_dir.exists()
    assert not fs.runtime_code_dir.exists()
    assert not fs.input_datasets_dir.exists()
    assert not fs.joined_output_dir.exists()

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

    data_home = tmp_path / "data"
    run_id = "fake-run-id"

    step_1 = Step(name="A", application="roms_marbl", blueprint=bp.as_posix())
    step_2 = Step(name="B", application="roms_marbl", blueprint=bp.as_posix())
    step_3 = Step(name="C", application="roms_marbl", blueprint=bp.as_posix())

    with mock.patch.dict(
        os.environ,
        {
            ENV_CSTAR_DATA_HOME: data_home.as_posix(),
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
        expected = data_home / run_id / task_dir_name / ls_1.safe_name  # noqa: SLF001
        assert actual == expected

        actual = ls_2.working_dir
        expected = ls_1.working_dir / task_dir_name / ls_2.safe_name  # type: ignore # noqa: SLF001
        assert actual == expected

        actual = ls_3.working_dir
        expected = ls_2.working_dir / task_dir_name / ls_3.safe_name  # type: ignore # noqa: SLF001
        assert actual == expected

        actual = ls_4.working_dir
        # confirm the parent carries over and ls4 is a child of ls2
        assert ls_2.working_dir
        expected = ls_2.working_dir / task_dir_name / ls_4.safe_name
        assert actual == expected

        actual = ls_5.working_dir
        expected = data_home / run_id / task_dir_name / ls_5.safe_name
        assert actual == expected
        assert ls_5.name == ls_5_name  # confirm the update was honored


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
