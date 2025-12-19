from pathlib import Path

import pytest

from cstar.orchestration.utils import clear_working_dir


@pytest.fixture
def populated_output_dir(tmp_path: Path) -> tuple[Path, list[Path]]:
    output_dir = tmp_path / "my_output_dir"
    files = [
        (output_dir / "input" / "some_file"),
        (output_dir / "output" / "some_file"),
        (output_dir / "output" / "joined_output" / "some_file"),
    ]

    for f in files:
        f.parent.mkdir(parents=True)
        f.touch()

    return output_dir, files


@pytest.mark.parametrize(
    "env_value,cleared", (("1", True), ("0", False), (None, False))
)
def test_clear_working_directory_env_setting(
    env_value: str | None,
    cleared: bool,
    monkeypatch,
    populated_output_dir: tuple[Path, list[Path]],
):
    """
    Test that clear_working_dir correctly clears the working directory if CSTAR_CLOBBER_WORKING_DIR is set to 1, and
    doesn't clear it if set to 0 or unset.

    Parameters
    ----------
    env_value: value to set our magic env-var to
    cleared: whether the directory should be cleared or not
    monkeypatch: pytest util for setting env-vars in tests
    populated_output_dir: fixture providing tmp_path and fake files

    """
    output_dir, output_files = populated_output_dir

    if env_value is not None:
        monkeypatch.setenv("CSTAR_CLOBBER_WORKING_DIR", env_value)
    else:
        monkeypatch.delenv("CSTAR_CLOBBER_WORKING_DIR", raising=False)

    clear_working_dir(output_dir)
    if cleared:
        assert all([not f.exists() for f in output_files])
        assert all([not f.parent.exists() for f in output_files])

    else:
        assert output_dir.exists()
        assert all([f.exists for f in output_files])
