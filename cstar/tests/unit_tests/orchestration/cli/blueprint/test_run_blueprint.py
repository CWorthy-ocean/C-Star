from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerRequest,
    RunnerResult,
    RunnerState,
    get_application,
)
from cstar.applications.roms_marbl.app import RomsMarblRunner
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.cli.blueprint.run import app
from cstar.entrypoint.runner import BlueprintRunner
from cstar.entrypoint.utils import ARG_DIRECTIVES_URI_LONG
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.adapter import prepare_directive_file
from cstar.orchestration.models import Application, Blueprint
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.transforms import ApplyOverridesDirective
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

    async def modify_runner(
        self: BlueprintRunner[RomsMarblBlueprint],
    ) -> RunnerResult[RomsMarblBlueprint]:
        """Mock the main execution method to avoid `real work` and ensure the result
        attribute is updated.
        """
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    app_config: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
        get_application("roms_marbl")
    )

    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    with (
        mock.patch.object(
            ROMSSimulation,
            "from_blueprint",
            return_value=mock_sim_instance,
        ),
        mock.patch.object(
            app_config.runner,
            "execute",
            side_effect=modify_runner,
            autospec=True,
        ) as mock_exec_runner,
    ):
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
def test_blueprint_run_apply_directive_dne(directive_path: str) -> None:
    """Verify that an exception is raised if a path to a non-existent directive file is passed."""
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml"

    with mock.patch(
        "cstar.applications.roms_marbl.app.RomsMarblRunner.execute",
        return_value=RunnerResult(
            RunnerRequest(bp_path, RomsMarblBlueprint),
            RunnerState(ExecutionStatus.COMPLETED),
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


def test_blueprint_run_apply_directive_empty(
    tmp_path: Path,
    package_path: Path,
) -> None:
    """Verify that an exception is raised if an empty directive file is passed."""
    bp_path = str(package_path / "docs/tutorials/wales_toy_blueprint.yaml")
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
    tmp_path: Path,
    mocked_simulation_outputs: tuple[Path, Path, Path],
    package_path: Path,
) -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is executed.
    """
    bp_path = str(package_path / "docs/tutorials/wales_toy_blueprint.yaml")
    _, step_dir, _ = mocked_simulation_outputs

    temp_step = LiveStep(
        name="step-for-testing",
        application=Application.ROMS_MARBL.value,
        blueprint=bp_path,
        working_dir=tmp_path,
        directives={
            "continue-from": {"path": step_dir.as_posix()},
        },
    )
    directive_path = prepare_directive_file(temp_step)

    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[RomsMarblBlueprint],
    ) -> RunnerResult[RomsMarblBlueprint]:
        """Mock the main execution method to avoid `real work` and ensure the result
        attribute is updated.
        """
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

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
        mock.patch(
            "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
            mock.Mock(
                return_value=LiveWorkplan(
                    name="test-workplan",
                    description="a live workplan used to create a `WorkplanRun` to test directives",
                    steps=[temp_step],
                )
            ),
        ),
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


def test_blueprint_run_deferred_blueprint(
    tmp_path: Path,
    hello_world_bp_content: str,
) -> None:
    """Verify a `step://` URI is resolved against the producing step's output
    directory and the generated blueprint is executed with the packaged
    overrides applied.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_content : str
        The content of a minimal hello-world blueprint.
    """
    producer = LiveStep(
        name="producer",
        application="hello_world",
        blueprint=(tmp_path / "unused.yaml").as_posix(),
        working_dir=tmp_path / "producer",
    )
    producer.fsm.output_dir.mkdir(parents=True, exist_ok=True)
    generated = producer.fsm.output_dir / "generated.yaml"
    generated.write_text(hello_world_bp_content)

    consumer = LiveStep.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "producer", "filename": "generated.yaml"},
            "depends_on": ["producer"],
            "working_dir": tmp_path / "consumer",
            "directives": {
                ApplyOverridesDirective.key(): {
                    ApplyOverridesDirective.KEY_OVERRIDES: {
                        "target": "@overridden-target",
                        "working_dir": (tmp_path / "consumer").as_posix(),
                    },
                    ApplyOverridesDirective.KEY_APPLICATION: "hello_world",
                },
            },
        },
    )
    directive_path = prepare_directive_file(consumer)

    live_plan = LiveWorkplan(
        name="deferred-run",
        description="a live workplan providing producer-step context",
        steps=[producer],
    )

    with mock.patch(
        "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
        mock.Mock(return_value=live_plan),
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                str(consumer.blueprint_path),
                ARG_DIRECTIVES_URI_LONG,
                directive_path.as_posix(),
            ],
            color=False,
        )

    assert result.exit_code == 0, result.output
    assert "Hello, @overridden-target" in result.stdout


def test_blueprint_run_deferred_blueprint_unresolvable(
    tmp_path: Path,
) -> None:
    """Verify a `step://` URI that cannot be resolved fails with a clear
    error and a non-zero exit code.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    producer = LiveStep(
        name="producer",
        application="hello_world",
        blueprint=(tmp_path / "unused.yaml").as_posix(),
        working_dir=tmp_path / "producer",
    )
    producer.fsm.output_dir.mkdir(parents=True, exist_ok=True)

    live_plan = LiveWorkplan(
        name="deferred-run",
        description="a live workplan providing producer-step context",
        steps=[producer],
    )

    with mock.patch(
        "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
        mock.Mock(return_value=live_plan),
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["step://producer/never-generated.yaml"],
            color=False,
        )

    assert result.exit_code == 1
    assert "Unable to resolve deferred blueprint" in result.stdout
