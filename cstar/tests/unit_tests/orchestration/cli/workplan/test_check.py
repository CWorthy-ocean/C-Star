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

    check(wp_path)
    captured = capsys.readouterr().out
    assert " valid" in captured
