import argparse
import typing as t
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile

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


@contextmanager
def templated_plan(plan_name: str) -> t.Generator[Path, None, None]:
    """Return the path to a temporary file containing a populated workplan template.

    # TODO: convert to a fixture or use existing fixtures (this is duped from
    # tests in dag_runner.py for expediency).
    """
    wp_template_path = (
        Path(__file__).parent.parent.parent.parent.parent.parent
        / f"additional_files/templates/wp/{plan_name}.yaml"
    )
    bp_template_path = (
        Path(__file__).parent.parent.parent.parent.parent.parent
        / "additional_files/templates/bp/blueprint.yaml"
    )

    wp_tpl = wp_template_path.read_text()
    bp_tpl = bp_template_path.read_text()

    with (
        NamedTemporaryFile("w", delete_on_close=False) as wp,
        NamedTemporaryFile("w", delete_on_close=False) as bp,
    ):
        wp_path = Path(wp.name)
        bp_path = Path(bp.name)

        wp_tpl = wp_tpl.replace("{application}", "sleep")
        wp_tpl = wp_tpl.replace("{blueprint_path}", bp_path.as_posix())

        wp.write(wp_tpl)
        bp.write(bp_tpl)
        wp.close()
        bp.close()

        try:
            print(
                f"Temporary workplan located at {wp_path} with blueprint at {bp_path}"
            )
            yield wp_path
        finally:
            print(f"populated workplan template:\n{'#' * 80}\n{wp_tpl}\n{'#' * 80}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "plan_name",
    ["fanout", "single_step", "parallel"],
)
async def test_cli_workplan_check_action_tpl(
    plan_name: str,
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
    # cstar_dir = Path(__file__).parent.parent.parent.parent.parent.parent
    # templates_dir = cstar_dir / "additional_files/templates"
    # wp_path = templates_dir / template_file

    with templated_plan(plan_name) as wp_path:
        workplan: Workplan = deserialize(wp_path, Workplan)
        assert workplan, "sanity-check deserialize failed."

        ns = parser.parse_args(["workplan", "check", wp_path.as_posix()])
        await handle(ns)
    captured = capsys.readouterr().out
    assert " valid" in captured
