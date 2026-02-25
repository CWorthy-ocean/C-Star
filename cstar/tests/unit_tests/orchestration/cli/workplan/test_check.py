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


def test_cli_workplan_check_dne(
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    """Verify that an invalid path fails a validity check.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    workplan_name : str
        The name of a workplan template to use for workplan creation
    wp_templates_dir : Path
        Fixture returning the path to the directory containing workplan template files
    """
    wp_path = tmp_path / "workplan-dne.yaml"

    check(wp_path)
    captured = capsys.readouterr().out
    assert " not found" in captured


def test_cli_workplan_check_file_no_content(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
) -> None:
    """Verify that an empty workplan file fails a validity check.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    wp_path = tmp_path / "empty_workplan.yml"
    wp_path.touch()

    check(wp_path)

    assert "is invalid" in capsys.readouterr().out


@pytest.mark.parametrize(
    "content",
    [" ", "", "\n", '{"foo": "bar"}', "name: Sample Workplan\n"],
)
def test_cli_workplan_check_file_bad_content(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
    content: str,
) -> None:
    """Verify that an invalid/malformed workplan fails a validity check.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    wp_path = tmp_path / "invalid_workplan.yml"
    wp_path.write_text(content)

    check(wp_path)

    assert "is invalid" in capsys.readouterr().out


@pytest.mark.parametrize(
    "repo_relative_path",
    [
        Path("docs/tutorials/workplan_laptop_example.yaml"),
        Path("cstar/additional_files/templates/wp/workplan.yaml"),
    ],
)
def test_cli_workplan_check_valid_input(
    capsys: pytest.CaptureFixture,
    repo_relative_path: Path,
    package_path: Path,
) -> None:
    """Verify that a valid workplan passes the CLI check.

    NOTE: This test also serves a practical purpose of confirming the continued
    validity of tutorial and sample workplans.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    repo_relative_path : Path
        Relative path to a workplan within the c-star repo
    package_path : Path
        Absolute path to the c-star package on disk
    """
    wp_path = package_path / repo_relative_path

    check(Path(wp_path))

    msg = f"`{wp_path}` does not contain a valid workplan"
    assert "is valid" in capsys.readouterr().out, msg


@pytest.mark.parametrize(
    ("start_removal", "end_removal"),
    [
        ("name:", None),
        ("description:", None),
        ("steps:", "<EOF>"),
        ("Prepare", "Ensemble X"),
        ("Ensemble X", "Ensemble Y"),
        ("Ensemble Y", "Aggregate"),
        ("blueprint:", None),
        ("segment_length:", None),
    ],
)
def test_workplan_incomplete_input(
    capsys: pytest.CaptureFixture,
    start_removal: str,
    end_removal: str,
    tmp_path: Path,
    package_path: Path,
) -> None:
    """Verify that an incomplete workplan fails the CLI check.

    Starts with a sample workplan and removes a piece of required information in each test.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    start_removal : Path
        A string that will trigger content skipping to begin when building a test workplan
    end_removal : Path
       A string that will trigger content skipping to end when building a test workplan
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    package_path : Path
        Absolute path to the c-star package on disk
    """
    wp_path = package_path / "cstar/additional_files/templates/wp/workplan.yaml"

    content = wp_path.read_text().splitlines()
    remaining_content = []
    cutting = False
    cut_once = False

    for line in content:
        if start_removal in line and not cut_once:
            cutting = True
            cut_once = True
        elif end_removal and end_removal in line:
            cutting = False

        if not cutting:
            remaining_content.append(line)

        if end_removal is None or end_removal in line:
            cutting = False

    bp_path = tmp_path / "bp.yaml"
    bp_path.write_text("\n".join(remaining_content))

    check(bp_path)

    err_msg = f"{bp_path} should not pass validation"
    assert "is invalid" in capsys.readouterr().out, err_msg


@pytest.mark.parametrize(
    ("start_removal", "end_removal"),
    [
        ("state:", None),
        ("compute_environment:", "runtime_vars:"),
        ("num_nodes:", None),
        ("num_cpus_per_process:", None),
        ("runtime_vars:", None),
        ("Aggregate", None),
        ("workflow_overrides:", "compute_overrides:"),
        ("compute_overrides:", "name:"),
        ("walltime", None),
        ("          num_nodes:", None),
        ("blueprint_overrides:", "workflow_overrides"),
        ("depends_on:", "blueprint"),
    ],
)
def test_workplan_optional_input(
    capsys: pytest.CaptureFixture,
    start_removal: str,
    end_removal: str | None,
    tmp_path: Path,
    package_path: Path,
) -> None:
    """Verify that an incomplete workplan fails the CLI check.

    Starts with a sample workplan and removes a piece of required information in each test.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    start_removal : Path
        A string that will trigger content skipping to begin when building a test workplan
    end_removal : Path
       A string that will trigger content skipping to end when building a test workplan
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    package_path : Path
        Absolute path to the c-star package on disk
    """
    wp_path = package_path / "cstar/additional_files/templates/wp/workplan.yaml"

    content = wp_path.read_text().splitlines()
    remaining_content = []
    cutting = False
    cut_once = False

    for line in content:
        if start_removal in line and not cut_once:
            cutting = True
            cut_once = True
        elif end_removal and end_removal in line:
            cutting = False

        if not cutting:
            remaining_content.append(line)

        if end_removal is None or end_removal in line:
            cutting = False

    bp_path = tmp_path / "bp.yaml"
    bp_path.write_text("\n".join(remaining_content))

    check(bp_path)

    err_msg = f"{bp_path} should not pass validation"
    assert "is valid" in capsys.readouterr().out, err_msg
