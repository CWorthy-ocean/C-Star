from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.applications.core import ApplicationDefinition, get_application
from cstar.applications.roms_marbl.app import RomsMarblRunner
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.cli.blueprint.run import app
from cstar.entrypoint.runner import XBlueprintRunner, XRunnerRequest, XRunnerResult
from cstar.entrypoint.utils import ARG_DIRECTIVES_URI_LONG
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.converter.converter import prepare_directive_file
from cstar.orchestration.models import Application, Blueprint
from cstar.orchestration.orchestration import LiveStep
from cstar.roms.simulation import ROMSSimulation


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

    async def modify_runner(self) -> XRunnerResult[RomsMarblBlueprint]:
        """Mock the main execution method to avoid `real work` and ensure the result
        attribute is updated.
        """
        self._result = XRunnerResult(
            XRunnerRequest(str(bp_path), RomsMarblBlueprint),
            ExecutionStatus.COMPLETED,
        )
        return self._result

    app_config: ApplicationDefinition[Blueprint, XBlueprintRunner[Blueprint]] = (
        get_application("roms_marbl")
    )

    with mock.patch.object(
        app_config.runner,
        "execute",
        side_effect=modify_runner,
        autospec=True,
    ) as mock_exec_runner:
        runner = CliRunner()
        _ = runner.invoke(
            app,
            [bp_path],
            color=False,
        )

    mock_exec_runner.assert_called_once()


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
        "cstar.applications.roms_marbl.app.RomsMarblRunner.execute",
        return_value=XRunnerResult(
            XRunnerRequest(bp_path, RomsMarblBlueprint),
            ExecutionStatus.COMPLETED,
        ),
    ) as mock_exec:
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
    mock_exec.assert_not_called()


def test_blueprint_run_apply_directive_empty(tmp_path: Path) -> None:
    """Verify that an exception is raised if an empty directive file is passed."""
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml"
    directive_file_path = tmp_path / "directive-dne.json"
    directive_file_path.touch()

    with mock.patch.object(RomsMarblRunner, "execute", mock.AsyncMock()):
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


def test_blueprint_run_apply_directives(
    tmp_path: Path, mock_sim_output_dir: tuple[Path, Path, Path]
) -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is executed.
    """
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml"
    _, step_dir, _ = mock_sim_output_dir

    temp_step = LiveStep(
        name="step-for-testing",
        application=Application.ROMS_MARBL.value,
        blueprint=bp_path,
        work_dir=tmp_path,
        directives={
            "continue-from": {"path": step_dir.as_posix()},
        },
    )
    directive_path = prepare_directive_file(temp_step)

    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(self) -> XRunnerResult[RomsMarblBlueprint]:
        """Mock the main execution method to avoid `real work` and ensure the result
        attribute is updated.
        """
        self._result = XRunnerResult(
            XRunnerRequest(str(bp_path), RomsMarblBlueprint),
            ExecutionStatus.COMPLETED,
        )
        return self._result

    with (
        mock.patch.object(
            ROMSSimulation,
            "from_blueprint",
            return_value=mock_sim_instance,
        ),
        mock.patch.object(
            RomsMarblRunner,
            "execute",
            side_effect=modify_runner,
            autospec=True,
        ) as mock_exec_runner,
    ):
        runner = CliRunner()
        _ = runner.invoke(
            app,
            [
                bp_path,
                ARG_DIRECTIVES_URI_LONG,
                directive_path.as_posix(),
            ],
            color=False,
        )

    mock_exec_runner.assert_called_once()
