from pathlib import Path
from unittest import mock

import pytest
import yaml
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
from cstar.base.env import (
    ENV_CSTAR_DISABLE_MIGRATION,
    ENV_CSTAR_STATE_HOME,
    FLAG_ON,
)
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

    The published wales_toy blueprint is still schema 2.0.0; automatic
    migration brings it to the current schema during `run`.
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


def _write_2_1_0_blueprint(source: Path, dest_dir: Path) -> tuple[Path, int]:
    """Reshape the (3.0.0) complete blueprint fixture into its 2.1.0 form.

    Returns the path to the written file and the time step it carries in the
    legacy `model_params.time_step` field.
    """
    data = yaml.safe_load(source.read_text())
    data.pop("$schema", None)
    time_step = data.pop("namelist_overrides")["time_stepping"]["dt"]
    use_pio = data["partitioning"].pop("use_pio", False)
    data["schema_version"] = "2.1.0"
    data["model_params"] = {"time_step": time_step, "use_pio": use_pio}

    bp_path = dest_dir / "blueprint_2_1_0.yaml"
    bp_path.write_text(yaml.safe_dump(data))
    return bp_path, time_step


def test_blueprint_run_auto_migrates_2_1_0_blueprint(
    tmp_path: Path,
    complete_blueprint_path: Path,
    mock_xdg_dirs: dict[str, Path],
) -> None:
    """`run` migrates a 2.1.0 blueprint to the current schema by default and
    executes it: `model_params.time_step` lands in
    `namelist_overrides.time_stepping.dt` and `use_pio` moves under
    `partitioning` in the persisted, migrated blueprint.
    """
    bp_path, time_step = _write_2_1_0_blueprint(complete_blueprint_path, tmp_path)

    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[RomsMarblBlueprint],
    ) -> RunnerResult[RomsMarblBlueprint]:
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    app_config: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
        get_application("roms_marbl")
    )

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
            [bp_path.as_posix()],
            color=False,
        )

    mock_exec_runner.assert_called_once()

    state_home = mock_xdg_dirs[ENV_CSTAR_STATE_HOME]
    migrated_path = next(state_home.rglob(f"{bp_path.stem}_3.0.0*"))
    migrated = yaml.safe_load(migrated_path.read_text())
    assert "model_params" not in migrated
    # serialize() excludes model defaults, so validate to see effective values
    bp = RomsMarblBlueprint.model_validate(migrated)
    assert bp.schema_version == "3.0.0"
    assert bp.partitioning.use_pio is False
    assert bp.namelist_overrides["time_stepping"]["dt"] == time_step


def test_blueprint_run_disable_migration_outdated_rejected(
    tmp_path: Path,
    complete_blueprint_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With `CSTAR_DISABLE_MIGRATION` set, an out-of-date blueprint fails
    early instead of being migrated, and is not executed.
    """
    monkeypatch.setenv(ENV_CSTAR_DISABLE_MIGRATION, FLAG_ON)
    bp_path, _ = _write_2_1_0_blueprint(complete_blueprint_path, tmp_path)

    with mock.patch.object(RomsMarblRunner, "execute", mock.AsyncMock()) as mock_exec:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [bp_path.as_posix()],
            color=False,
        )

    assert result.exit_code != 0
    # rich wraps the message at the terminal width, so a line break may land
    # inside the phrase; normalize whitespace before matching.
    assert "migration is disabled" in " ".join(result.stdout.split())
    mock_exec.assert_not_called()


def test_blueprint_run_disable_migration_current_blueprint_ok(
    complete_blueprint_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With `CSTAR_DISABLE_MIGRATION` set, an up-to-date blueprint is
    unaffected and executes normally.
    """
    monkeypatch.setenv(ENV_CSTAR_DISABLE_MIGRATION, FLAG_ON)

    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[RomsMarblBlueprint],
    ) -> RunnerResult[RomsMarblBlueprint]:
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    app_config: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
        get_application("roms_marbl")
    )

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
            [complete_blueprint_path.as_posix()],
            color=False,
        )

    mock_exec_runner.assert_called_once()


def test_blueprint_run_current_blueprint_not_persisted(
    complete_blueprint_path: Path,
    mock_xdg_dirs: dict[str, Path],
) -> None:
    """An up-to-date blueprint runs as-is: no migrated copy is written to the
    state directory.
    """
    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[RomsMarblBlueprint],
    ) -> RunnerResult[RomsMarblBlueprint]:
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    app_config: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
        get_application("roms_marbl")
    )

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
            [complete_blueprint_path.as_posix()],
            color=False,
        )

    mock_exec_runner.assert_called_once()
    state_home = mock_xdg_dirs[ENV_CSTAR_STATE_HOME]
    assert not list(state_home.rglob(f"{complete_blueprint_path.stem}_*"))


def test_blueprint_run_invalid_blueprint_exits_nonzero(
    tmp_path: Path,
    complete_blueprint_path: Path,
) -> None:
    """A blueprint that is schema-current but fails content validation exits
    with a non-zero code and is not executed.
    """
    data = yaml.safe_load(complete_blueprint_path.read_text())
    data.pop("$schema", None)
    # violate the model validator: end_date beyond the valid range
    data["runtime_params"]["end_date"] = "2200-01-01T00:00:00"
    bp_path = tmp_path / "blueprint_invalid.yaml"
    bp_path.write_text(yaml.safe_dump(data))

    with mock.patch.object(RomsMarblRunner, "execute", mock.AsyncMock()) as mock_exec:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [bp_path.as_posix()],
            color=False,
        )

    assert result.exit_code != 0
    mock_exec.assert_not_called()


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

    # Depending on the installed typer/rich versions, usage errors render as a
    # rich panel whose box borders and width-dependent wrapping can split the
    # phrase across lines -- collapse the decoration before matching.
    stderr_flat = " ".join(result.stderr.replace("│", " ").split())
    assert "file not found" in stderr_flat
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
