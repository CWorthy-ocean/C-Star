from pathlib import Path

import pytest

from cstar.execution.file_system import RomsFileSystemManager


@pytest.fixture
def populated_output_dir(tmp_path: Path) -> tuple[Path, list[Path]]:
    """Create a populated output directory."""
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
    asset_root = tmp_path / "my_output_dir"
    fs = RomsFileSystemManager(asset_root)
    fs.prepare()

    assert fs.output_dir.exists()
    assert fs.input_dir.exists()
    assert fs.joined_output_dir.exists()
    assert fs.codebases_dir.exists()
    assert fs.root.exists()


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
    output_dir, output_files = populated_output_dir

    fs = RomsFileSystemManager(output_dir)
    fs.clear()

    assert all([not f.exists() for f in output_files])
    assert all([not f.parent.exists() for f in output_files])
