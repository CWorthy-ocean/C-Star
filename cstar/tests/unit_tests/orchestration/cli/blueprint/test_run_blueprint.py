from pathlib import Path
from unittest import mock

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
    bp_path = tmp_path / "blueprint-dne.yml"

    run(bp_path.as_posix())

    assert "not found" in capsys.readouterr().out


def test_blueprint_run_remote_blueprint_dne(
    capsys: pytest.CaptureFixture,
) -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is not executed if the URL is invalid.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    """
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint-X.yaml"

    run(bp_path)

    assert "not found" in capsys.readouterr().out


def test_blueprint_run_remote_blueprint(
    capsys: pytest.CaptureFixture,
) -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is executed.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    """
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml"

    with mock.patch(
        "cstar.cli.blueprint.run.execute_runner",
        return_value=0,
    ) as mock_exec:
        run(bp_path)

    assert "is valid" in capsys.readouterr().out
    mock_exec.assert_called_once()
