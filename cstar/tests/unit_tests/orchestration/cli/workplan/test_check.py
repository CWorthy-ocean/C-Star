import argparse
import typing as t
from pathlib import Path

import pytest

from cstar.cli.workplan.actions.check import create_action, handle
from cstar.cli.workplan.command import create_command_root
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize, serialize


@pytest.fixture
def parser() -> argparse.ArgumentParser:
    """Create a minimal argument parser for the check action."""
    parser = argparse.ArgumentParser(prog="cstar-test-cli")
    commands_sp = parser.add_subparsers(title="commands")

    (command, _), command_fn = create_command_root()
    parser.set_defaults(command=command)
    assert command == "workplan"
    tpl_subparser = t.cast(argparse._SubParsersAction, command_fn(commands_sp))

    (_, action), action_fn = create_action()
    assert action == "check"
    _ = action_fn(tpl_subparser)

    return parser


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workplan_name",
    ["mvp_workplan", "fanout_workplan", "linear_workplan"],
)
async def test_cli_workplan_check_action(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    workplan_name: str,
    parser: argparse.ArgumentParser,
    capsys: pytest.CaptureFixture,
) -> None:
    """Verify that CLI check action validates a properly serialized workplan.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest request used to load fixtures by name
    tmp_path : Path
        Temporary directory for test outputs
    workplan_name : str
        The name of a workplan fixture to use for workplan creation
    """
    workplan = request.getfixturevalue(workplan_name)
    wp_path = tmp_path / f"{workplan_name}.yaml"
    serialize(wp_path, workplan)
    ns = parser.parse_args(["workplan", "check", wp_path.as_posix()])

    await handle(ns)
    captured = capsys.readouterr().out
    assert " valid" in captured


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "template_file",
    ["fanout_workplan.yaml", "linear_workplan.yaml", "mvp_workplan.yaml"],
)
async def test_cli_workplan_check_action_tpl(
    # request: pytest.FixtureRequest,
    # tmp_path: Path,
    template_file: str,
    parser: argparse.ArgumentParser,
    capsys: pytest.CaptureFixture,
) -> None:
    """Verify that CLI check action validates the stored templates.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest request used to load fixtures by name
    tmp_path : Path
        Temporary directory for test outputs
    workplan_name : str
        The name of a workplan fixture to use for workplan creation
    """
    # workplan = request.getfixturevalue(workplan_name)
    templates_dir = Path("/Users/chris/code/cstar/cstar/additional_files/templates")
    wp_path = templates_dir / template_file

    workplan: Workplan = deserialize(wp_path, Workplan)
    assert workplan, "sanity-check deserialize failed."

    ns = parser.parse_args(["workplan", "check", wp_path.as_posix()])
    await handle(ns)
    captured = capsys.readouterr().out
    assert " valid" in captured
