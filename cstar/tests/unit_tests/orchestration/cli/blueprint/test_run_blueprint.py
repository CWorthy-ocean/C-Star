from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.cli.blueprint.run import app


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

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix()],
        color=False,
    )

    assert "not found" in result.stderr


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

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path],
        color=False,
    )

    assert "not found" in result.stderr


def test_blueprint_run_remote_blueprint() -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is executed.
    """
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml"

    with mock.patch(
        "cstar.cli.blueprint.run.execute_runner_rm",
        return_value=0,
    ) as mock_exec:
        runner = CliRunner()
        _ = runner.invoke(
            app,
            [bp_path],
            color=False,
        )

    mock_exec.assert_called_once()
