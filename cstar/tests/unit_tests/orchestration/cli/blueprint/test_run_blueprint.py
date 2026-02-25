from pathlib import Path

import pytest

from cstar.cli.blueprint.run import run


def test_blueprint_run_file_dne(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
) -> None:
    """Verify that a path to a non-existent blueprint fails to be started due
    to validation.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    wp_path = tmp_path / "blueprint-dne.yml"

    run(wp_path)

    assert "not found" in capsys.readouterr().out
