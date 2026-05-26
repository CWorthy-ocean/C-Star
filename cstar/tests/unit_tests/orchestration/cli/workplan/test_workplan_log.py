from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.cli.workplan.log import app
from cstar.orchestration.models import Workplan


@pytest.mark.asyncio
async def test_cli_workplan_log_step_dne(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that an invalid step name results in an error message.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    *_, fake_run_id = executed_workplan

    runner = CliRunner()
    invalid_step_name = "step-DNE"

    result = runner.invoke(
        app,
        [fake_run_id, invalid_step_name],
        color=False,
    )

    assert f"No log file found for step {invalid_step_name!r}" in result.stdout


@pytest.mark.asyncio
async def test_cli_workplan_log_step_no_logs(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that a step where no logs have been created results in an error message.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    runner = CliRunner()
    step_name = wp.steps[0].name

    result = runner.invoke(
        app,
        [fake_run_id, step_name],
        color=False,
        catch_exceptions=False,
    )

    assert f"No log file found for step {step_name!r}" in result.stdout


@pytest.mark.asyncio
async def test_cli_workplan_log_step_with_logs(
    executed_workplan_with_sideeffects: tuple[Path, Workplan, str],
) -> None:
    """Verify that logs are loaded successfully.

    Parameters
    ----------
    executed_workplan_with_sideeffects : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan_with_sideeffects

    runner = CliRunner()

    for step in wp.steps:
        result = runner.invoke(
            app,
            [fake_run_id, step.name],
            color=False,
            catch_exceptions=False,
        )

        assert f"{step.name} message" in result.stdout
