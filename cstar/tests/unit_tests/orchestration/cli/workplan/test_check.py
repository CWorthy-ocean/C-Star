from pathlib import Path

import pytest

from cstar.cli.workplan.check import check


@pytest.mark.parametrize(
    "workplan_name",
    ["fanout", "linear", "parallel", "single_step"],
)
def test_cli_workplan_check_action_tpl(
    tmp_path: Path,
    workplan_name: str,
    capsys: pytest.CaptureFixture,
    wp_templates_dir: Path,
) -> None:
    """Verify that CLI check action validates the stored templates.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    workplan_name : str
        The name of a workplan template to use for workplan creation
    wp_templates_dir : Path
        Fixture returning the path to the directory containing workplan template files
    """
    template_file = f"{workplan_name}.yaml"
    template_path = wp_templates_dir / template_file

    wp_path = tmp_path / template_file
    wp_path.write_text(template_path.read_text())

    check(wp_path)
    captured = capsys.readouterr().out
    assert " valid" in captured
