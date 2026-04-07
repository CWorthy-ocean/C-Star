# ruff: noqa: S101
from pathlib import Path

import pytest
from pytest import CaptureFixture

from cstar.entrypoint.worker.app_host import BlueprintRequest as BlueprintRequestV2
from cstar.entrypoint.worker.app_host import (
    create_runner,
)
from cstar.entrypoint.worker.hello_app import (
    HelloWorldBlueprint,
    HelloWorldRunner,
)
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Application
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
async def test_hello_world_blueprint_execution(
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
    request = BlueprintRequestV2(blueprint_uri=hello_world_bp_path.as_posix())
    runner = create_runner(request, HelloWorldRunner)

    await runner.execute()

    # Confirm the success disposition is set
    assert runner._result.status == ExecutionStatus.COMPLETED

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
    request = BlueprintRequestV2(blueprint_uri=hello_world_bp_path.as_posix())
    runner = create_runner(request, HelloWorldRunner)

    await runner.execute()

    # Confirm the success disposition is set
    assert runner._result.status == ExecutionStatus.COMPLETED

    captured = capsys.readouterr()
    assert f"hello, {hello_world_default_target}".lower() in captured.out.lower()
