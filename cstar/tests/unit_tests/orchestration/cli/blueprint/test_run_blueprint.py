from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.cli.blueprint.run import app
from cstar.entrypoint.utils import ARG_DIRECTIVES_URI_LONG


def test_blueprint_run_file_dne(tmp_path: Path) -> None:
    """Verify that a path to a non-existent blueprint fails to be started due
    to validation.

    Parameters
    ----------
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


def test_blueprint_run_remote_blueprint_dne() -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is not executed if the URL is invalid.
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
        "cstar.cli.blueprint.run.exec_romsmarbl_runner",
        return_value=0,
    ) as mock_exec:
        runner = CliRunner()
        _ = runner.invoke(
            app,
            [bp_path],
            color=False,
        )

    mock_exec.assert_called_once()


@pytest.mark.parametrize(
    "directive_path",
    [
        "directive-dne.json",
        "https://www.google.com/directive-dne.json",
    ],
)
def test_blueprint_run_apply_directive_dne(tmp_path: Path, directive_path: str) -> None:
    """Verify that an exception is raised if a path to a non-existent directive file is passed."""
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml"

    with mock.patch(
        "cstar.cli.blueprint.run.exec_romsmarbl_runner",
        return_value=0,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                bp_path,
                ARG_DIRECTIVES_URI_LONG,
                directive_path,
            ],
            color=False,
        )

    assert "file not found" in result.stderr


def test_blueprint_run_apply_directive_empty(tmp_path: Path) -> None:
    """Verify that an exception is raised if an empty directive file is passed."""
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml"
    directive_file_path = tmp_path / "directive-dne.json"
    directive_file_path.touch()

    with mock.patch(
        "cstar.cli.blueprint.run.exec_romsmarbl_runner",
        return_value=0,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                bp_path,
                ARG_DIRECTIVES_URI_LONG,
                directive_file_path.as_posix(),
            ],
            color=False,
        )

    assert "malformed" in result.stderr
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is executed.
    """
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml"

    with mock.patch(
        "cstar.cli.blueprint.run.exec_romsmarbl_runner",
        return_value=0,
    ) as mock_exec:
        runner = CliRunner()
        _ = runner.invoke(
            app,
            [bp_path],
            color=False,
        )

    mock_exec.assert_called_once()
