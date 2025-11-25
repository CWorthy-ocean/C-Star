import argparse
import typing as t
from pathlib import Path

import pytest

from cstar.cli.workplan.actions.check import create_action, handle
from cstar.cli.workplan.command import create_command_root


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
    ["fanout", "linear", "parallel", "single_step"],
)
async def test_cli_workplan_check_action_tpl(
    tmp_path: Path,
    workplan_name: str,
    parser: argparse.ArgumentParser,
    capsys: pytest.CaptureFixture,
) -> None:
    """Verify that CLI check action validates the stored templates.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    workplan_name : str
        The name of a workplan template to use for workplan creation
    """
    cstar_dir = Path(__file__).parent.parent.parent.parent.parent.parent
    template_file = f"{workplan_name}.yaml"
    templates_dir = cstar_dir / "additional_files/templates/wp"
    template_path = templates_dir / template_file

    wp_path = tmp_path / template_file
    wp_path.write_text(template_path.read_text())

    ns = parser.parse_args(["workplan", "check", wp_path.as_posix()])
    await handle(ns)
    captured = capsys.readouterr().out
    assert " valid" in captured
