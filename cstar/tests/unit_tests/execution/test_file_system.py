import os
import pickle
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_DATA_HOME
from cstar.execution.file_system import JobFileSystemManager, RomsFileSystemManager
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

    assert fsm.working_directory.is_absolute()
    assert "~" not in str(fsm.working_directory)


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
    assert fs.working_directory.exists()


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

    step_1 = Step(name="A", application="roms_marbl", blueprint=bp.as_posix())
    step_2 = Step(name="B", application="roms_marbl", blueprint=bp.as_posix())
    step_3 = Step(name="C", application="roms_marbl", blueprint=bp.as_posix())

    ls_1 = LiveStep.from_step(step_1)
    ls_2 = LiveStep.from_step(step_2, step_1)  # use plan "Step" as parent
    ls_3 = LiveStep.from_step(step_3, ls_2)  # use "LiveStep" as parent
    ls_4 = LiveStep.from_step(ls_3)  # step from another live step

    ls_5_name = "child of ls 2"
    ls_5 = LiveStep.from_step(ls_1, update={"name": ls_5_name})  # override attributes

    with mock.patch.dict(
        os.environ,
        {ENV_CSTAR_DATA_HOME: data_home.as_posix()},
        clear=True,
    ):
        actual = ls_1.get_working_dir
        expected = data_home / JobFileSystemManager._TASKS_NAME / ls_1.safe_name  # noqa: SLF001
        assert actual == expected

        actual = ls_2.get_working_dir
        expected = (
            ls_1.get_working_dir / JobFileSystemManager._TASKS_NAME / ls_2.safe_name
        )  # noqa: SLF001
        assert actual == expected

        actual = ls_3.get_working_dir
        expected = (
            ls_2.get_working_dir / JobFileSystemManager._TASKS_NAME / ls_3.safe_name
        )  # noqa: SLF001
        assert actual == expected

        actual = ls_4.get_working_dir
        # confirm the parent carries over and ls4 is a child of ls2
        expected = (
            ls_2.get_working_dir / JobFileSystemManager._TASKS_NAME / ls_4.safe_name
        )  # noqa: SLF001
        assert actual == expected

        actual = ls_5.get_working_dir
        expected = data_home / JobFileSystemManager._TASKS_NAME / ls_5.safe_name  # noqa: SLF001
        assert actual == expected
        assert ls_5.name == ls_5_name  # confirm the update was honored
