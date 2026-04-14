# ruff: noqa: S101
from pathlib import Path

import pytest
from pytest import CaptureFixture

from cstar.base.log import LogLevelChoices
from cstar.entrypoint.config import get_job_config, get_service_config
from cstar.entrypoint.service import ServiceConfiguration
from cstar.entrypoint.worker.hello_app import (
    HelloWorldBlueprint,
    HelloWorldRunner,
)
from cstar.entrypoint.xrunner import XRunnerRequest
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.converter.converter import get_command_mapping
from cstar.orchestration.models import Application
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import deserialize, serialize


@pytest.mark.asyncio
async def test_hello_world_blueprint_serialization(
    tmp_path: Path,
    hello_world_bp_content: str,
) -> None:
    """Verify that a well-formed HW blueprint is serialized and
    deserialized correctly.

    Parameters
    ----------
    tmp_path : Path
        Temporary output location for writing the test blueprint.
    hello_world_bp_content : str
        Valid yaml for a hello world blueprint.
    """
    target = "@ankona"
    bp_path = tmp_path / "helloworld.yaml"
    bp_path.write_text(hello_world_bp_content)

    bp = deserialize(bp_path.as_posix(), HelloWorldBlueprint)
    assert bp.application == Application.HELLO_WORLD
    assert bp.target == target
    assert bp.name == "Say hello to my little friend!"
    assert bp.description == "This blueprint says hello to a very nice guy"
    assert bp.state == "draft"
    assert bp.cpus_needed == 1  # until it's moved, cpus_needed is hardcoded

    persist_to = tmp_path / "hwserialized.yaml"
    assert serialize(persist_to, bp), "Failed to serialize blueprint"
    assert persist_to.exists(), "Serialized blueprint not found"

    bp2 = deserialize(persist_to, HelloWorldBlueprint)
    assert bp2.name == bp.name
    assert bp2.description == bp.description
    assert bp2.application == bp.application
    assert bp2.state == bp.state
    assert bp2.target == bp.target
    assert bp2.cpus_needed == bp.cpus_needed


@pytest.mark.asyncio
async def test_hello_world_runner_execute_xrunner(
    capsys: CaptureFixture,
    hello_world_bp_path: Path,
    hello_world_default_target: str,
) -> None:
    """Verify that a well-formed HW blueprint executes correctly.

    This tests the integration of `BlueprintRunner` and `HelloWorldRunner` classes
    without the service lifecycle being invoked.

    Parameters
    ----------
    capsys : CaptureFixture
        Fixture for capturing stdout and stderr.
    hello_world_bp_path : Path
        Path to the hello world blueprint.
    """
    request = XRunnerRequest(str(hello_world_bp_path), HelloWorldBlueprint)
    svc_cfg = ServiceConfiguration()
    job_cfg = get_job_config()

    runner = HelloWorldRunner(request, job_cfg, svc_cfg)
    result = await runner.execute_xrunner()

    assert result is not None
    # Confirm the success disposition is set
    assert result.status == ExecutionStatus.COMPLETED

    captured = capsys.readouterr()
    assert f"hello, {hello_world_default_target}".lower() in captured.out.lower()


@pytest.mark.asyncio
async def test_execute_runner_happy_path(
    capsys: CaptureFixture,
    hello_world_bp_path: Path,
    hello_world_default_target: str,
) -> None:
    """Verify that a well-formed HW blueprint executes correctly.

    This tests the `BlueprintRunner` and `HelloWorldRunner` classes.

    Parameters
    ----------
    capsys : CaptureFixture
        Fixture for capturing stdout and stderr.
    hello_world_bp_path : Path
        Path to the hello world blueprint.
    """
    request = XRunnerRequest(str(hello_world_bp_path), HelloWorldBlueprint)
    job_cfg = get_job_config()
    svc_cfg = get_service_config(
        log_level=LogLevelChoices.INFO,
    )

    runner = HelloWorldRunner(request, job_cfg, svc_cfg)
    await runner.execute()

    # Confirm the success disposition is set
    assert runner.status == ExecutionStatus.COMPLETED

    captured = capsys.readouterr()
    assert f"hello, {hello_world_default_target}".lower() in captured.out.lower()


@pytest.mark.asyncio
async def test_hello_world_command_converter(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify that the command converter produces a working CLI
    command.

    Parameters
    ----------
    tmp_path : Path
        Temporary output location for writing the test blueprint.
    capsys : CaptureFixture
        Fixture for capturing stdout and stderr.
    hello_world_bp_path : Path
        Path to the hello world blueprint.
    """
    work_dir = tmp_path / "work"

    step = LiveStep(
        name=f"{__name__}",
        application=Application.HELLO_WORLD,
        blueprint=hello_world_bp_path.as_posix(),
        work_dir=work_dir,
    )

    # find the registered command converter for this application and launcher type
    cmd_converter = get_command_mapping(Application.HELLO_WORLD.value)
    assert cmd_converter is not None, "Command converter not found"

    # use the converter function to generate the command
    command = cmd_converter(step)

    # confirm the retrieved converter produces the output expected
    # from the function: convert_step_to_blueprint_run_command
    assert "python" in command
    assert "cstar.entrypoint.worker.hello_app" in command
    assert f"-b {hello_world_bp_path.as_posix()}" in command
    # assert command == f"cstar blueprint run {hello_world_bp_path.as_posix()}"
