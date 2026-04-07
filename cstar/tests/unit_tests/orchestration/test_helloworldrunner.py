# ruff: noqa: S101
import os
import sys
import typing as t
from pathlib import Path
from unittest import mock

import pytest
from pytest import CaptureFixture
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_STATE_HOME
from cstar.cli.workplan.run import app
from cstar.entrypoint.worker.app_host import BlueprintRequest as BlueprintRequestV2
from cstar.entrypoint.worker.app_host import (
    BlueprintRunner,
    create_runner,
)
from cstar.entrypoint.worker.hello_app import (
    HelloWorldRunner,
)
from cstar.entrypoint.worker.hello_app import (
    main as hw_main,
)
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize, serialize


@pytest.fixture
def hw_single_step_wp_path(
    tmp_path: Path,
    hello_world_bp_content: str,
    fill_workplan_template: t.Callable[[dict[str, t.Any]], str],
) -> Path:
    """Return the path to a workplan containing a single step that runs the hello_world application."""
    bp_content = hello_world_bp_content
    bp_path = tmp_path / "hw.yaml"
    bp_path.write_text(bp_content)

    expected_name = "hw-workplan"
    expected_description = "This is a test workplan using the hello_world application"

    step_name = "Hello Devloper!"
    app_name = "hello_world"
    draft_state = "draft"

    wp_yaml = fill_workplan_template(
        {
            "name": expected_name,
            "description": expected_description,
            "state": draft_state,
            "steps": [
                {
                    "name": step_name,
                    "application": app_name,
                    "blueprint": bp_path.as_posix(),
                },
            ],
        }
    )
    wp_path = tmp_path / "hw-workplan.yaml"
    wp_path.write_text(wp_yaml)

    return wp_path


@pytest.fixture
def hw_single_step_wp(
    hw_single_step_wp_path: Path,
) -> Workplan:
    """Return a workplan containing a single step that runs the hello_world application."""
    return deserialize(hw_single_step_wp_path, Workplan)


@pytest.mark.asyncio
async def test_hello_world_main(
    blueprint_path: Path,
) -> None:
    """Test the execution of the "hello, {world}!" application entrypoint

    This test verifies that the runner is started successfully and receives
    the set of expected parameters.
    """
    mock_execute = mock.AsyncMock(return_code=0)

    args = [
        "cstar.entrypoint.worker.hello_app",
        "--blueprint-uri",
        str(blueprint_path),
        "--log-level",
        "DEBUG",
    ]

    # don't let it perform any real work; mock out runner.execute
    with (
        mock.patch.object(
            BlueprintRunner,
            "execute",
            mock_execute,
        ),
        mock.patch.object(sys, "argv", args),
    ):
        # This should run the simulation and return a success code
        return_code = await hw_main()

    # Confirm an error code is returned
    assert return_code == 0

    # Confirm the runner ran the simulation
    assert mock_execute.call_count == 1


@pytest.mark.parametrize(
    "target",
    [
        "@ankona",
        "@ScottEilerman",
        "@NoraLoose",
    ],
)
@pytest.mark.asyncio
async def test_hello_world_runner_happy_path(
    tmp_path: Path,
    capsys: CaptureFixture,
    target: str,
    hello_world_bp_content: str,
) -> None:
    """Test the execution of the "hello, {world}!" application

    This test verifies that the runner receives and parses the Blueprint, and
    executes the `Service` lifecycle.

    Parameters
    ----------
    tmp_path : Path
        Temporary output location for writing the test blueprint.
    target : str
        Target to say hello to. Used to verify the runner triggers the
        `on_iteration` callback and that target is correctly parsed.
    hello_world_bp_content : str
        The content of a `HelloWorldBlueprint`.
    """
    bp_content = hello_world_bp_content.replace("@ankona", target)
    bp_path = tmp_path / "hw.yaml"
    bp_path.write_text(bp_content)

    request = BlueprintRequestV2(blueprint_uri=bp_path.as_posix())
    runner = create_runner(request, HelloWorldRunner)

    await runner.execute()

    # Confirm the success disposition is set
    assert runner._result.status == ExecutionStatus.COMPLETED

    captured = capsys.readouterr()
    assert f"hello, {target}".lower() in captured.out.lower()


@pytest.mark.asyncio
async def test_hello_world_workplan(
    tmp_path: Path,
    hello_world_bp_content: str,
    fill_workplan_template: t.Callable[[dict[str, t.Any]], str],
) -> None:
    """Test deserialization of a workplan containing a non ROMS-MARBL application.

    Consider moving to a parametrized test that executes the same evaluation on all
    known application types.

    Parameters
    ----------
    tmp_path : Path
        Temporary output location for writing the test blueprint.
    hello_world_bp_content : str
        The content of a `HelloWorldBlueprint`.
    """
    bp_content = hello_world_bp_content
    bp_path = tmp_path / "hw.yaml"
    bp_path.write_text(bp_content)

    expected_name = "hw-workplan"
    expected_description = "This is a test workplan using the hello_world application"

    step_name = "Hello Devloper!"
    app_name = "hello_world"
    draft_state = "draft"

    wp_yaml = fill_workplan_template(
        {
            "name": expected_name,
            "description": expected_description,
            "state": draft_state,
            "steps": [
                {
                    "name": step_name,
                    "application": app_name,
                    "blueprint": bp_path.as_posix(),
                },
            ],
        }
    )
    wp_path = tmp_path / "hw-workplan.yaml"
    wp_path.write_text(wp_yaml)

    # ensure the read succeeds
    workplan = deserialize(wp_path, Workplan)
    assert workplan.name == expected_name
    assert workplan.description == expected_description
    assert workplan.state == draft_state
    assert len(workplan.steps) == 1
    assert workplan.steps[0].name == step_name
    assert workplan.steps[0].application == app_name
    assert workplan.steps[0].blueprint_path == bp_path.as_posix()

    # ensure a write succeeds
    wp_copy = tmp_path / "hw-workplan-copy.yaml"
    assert serialize(wp_copy, workplan), "Failed to serialize workplan"

    # and sanity check the copy that was loaded
    workplan_copy = deserialize(wp_copy, Workplan)
    assert workplan_copy.name == workplan_copy.name
    assert workplan_copy.description == workplan_copy.description
    assert workplan_copy.state == workplan_copy.state
    assert workplan_copy.steps[0].name == workplan_copy.steps[0].name
    assert workplan_copy.steps[0].application == workplan_copy.steps[0].application
    assert (
        workplan_copy.steps[0].blueprint_path == workplan_copy.steps[0].blueprint_path
    )


def test_hello_world_workplan_dry_run(
    tmp_path: Path,
    hw_single_step_wp_path: Path,
) -> None:
    """Test the preparation of a workplan containing a non ROMS-MARBL application (--dry-run).

    Parameters
    ----------
    tmp_path : Path
        Temporary output location for writing the test workplan and outputs from the run.
    hw_single_step_wp_path : Path
        The path to the workplan containing a single step that runs the hello_world application.
    """
    state_dir = tmp_path / "state"
    runner = CliRunner()

    with (
        mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}),
    ):
        result = runner.invoke(
            app,
            [
                "--dry-run",
                hw_single_step_wp_path.as_posix(),
            ],
            color=False,
        )

    assert result.exit_code == 0
    assert "hw-workplan" in result.stdout
