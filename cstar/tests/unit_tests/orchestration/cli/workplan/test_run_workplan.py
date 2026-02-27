from pathlib import Path
from unittest import mock

import pytest

from cstar.cli.workplan.run import run


def test_workplan_run_file_dne(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
) -> None:
    """Verify that a path to a non-existent workplan fails to be started due
    to validation.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    wp_path = tmp_path / "workplan-dne.yml"

    run(wp_path.as_posix(), "test-run-id")

    assert "not found" in capsys.readouterr().out


def test_workplan_run_remote_workplan_dne(
    capsys: pytest.CaptureFixture,
) -> None:
    """Verify that a URL to a remote workplan is handled properly and the
    workplan is not executed if the URL is invalid.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    """
    wp_path = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml_XXX"

    run(wp_path, "my-run-id")

    assert "not found" in capsys.readouterr().out


@pytest.mark.parametrize(
    "wp_uri",
    [
        "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml",
        "HTTPS://raw.githubusercontent.com/cworthy-ocean/c-star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml",
    ],
)
def test_workplan_run_remote_workplan(
    capsys: pytest.CaptureFixture,
    wp_uri: str,
) -> None:
    """Verify that a URL to a remote workplan is handled properly and the
    workplan is executed.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    wp_uri : str
        A working URL referencing a valid workplan
    """
    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        return_value=0,
    ) as mock_exec:
        run(wp_uri, "12345")

    assert "is valid" in capsys.readouterr().out
    mock_exec.assert_called_once()
