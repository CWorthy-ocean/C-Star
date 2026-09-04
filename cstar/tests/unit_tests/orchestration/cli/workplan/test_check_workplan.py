import enum
from pathlib import Path, PosixPath

import pytest
import yaml
from typer.testing import CliRunner

from cstar.cli.workplan.check import app
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import (
    deserialize,
    enum_representer,
    path_representer,
    register_representer,
)


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.parametrize(
    "workplan_name",
    ["fanout", "linear", "parallel", "single_step"],
)
def test_cli_workplan_check_action_tpl(
    workplan_name: str,
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
    wp_path = wp_templates_dir / template_file

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert " valid" in result.stdout


def test_cli_workplan_check_dne(
    tmp_path: Path,
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

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert " not found" in result.stderr


def test_cli_workplan_check_file_no_content(
    tmp_path: Path,
) -> None:
    """Verify that an empty workplan file fails a validity check.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    wp_path = tmp_path / "empty_workplan.yml"
    wp_path.touch()

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert "is invalid" in result.stderr


@pytest.mark.parametrize(
    "content",
    [" ", "", "\n", '{"foo": "bar"}', "name: Sample Workplan\n"],
)
def test_cli_workplan_check_file_bad_content(
    tmp_path: Path,
    content: str,
) -> None:
    """Verify that an invalid/malformed workplan fails a validity check.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    wp_path = tmp_path / "invalid_workplan.yml"
    wp_path.write_text(content)

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert "is invalid" in result.stderr


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.parametrize(
    "repo_relative_path",
    [
        Path("docs/tutorials/workplan_laptop_example.yaml"),
        Path("cstar/additional_files/templates/wp/workplan.yaml"),
    ],
)
def test_cli_workplan_check_valid_input(
    repo_relative_path: Path,
    package_path: Path,
) -> None:
    """Verify that a valid workplan passes the CLI check.

    NOTE: This test also serves a practical purpose of confirming the continued
    validity of tutorial and sample workplans.

    Parameters
    ----------
    repo_relative_path : Path
        Relative path to a workplan within the c-star repo
    package_path : Path
        Absolute path to the c-star package on disk
    """
    wp_path = package_path / repo_relative_path

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    msg = f"`{wp_path}` does not contain a valid workplan"
    assert "is valid" in result.stdout, msg


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
    start_removal: str,
    end_removal: str | None,
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that an incomplete workplan fails the CLI check.

    Starts with a sample workplan and removes a piece of required information in each test.

    Parameters
    ----------
    start_removal : Path
        A string that will trigger content skipping to begin when building a test workplan
    end_removal : Path
       A string that will trigger content skipping to end when building a test workplan
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    package_path : Path
        Absolute path to the c-star package on disk
    """
    wp_path = wp_templates_dir / "workplan.yaml"

    content = wp_path.read_text().splitlines()
    remaining_content: list[str] = []
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

    wp_path = tmp_path / "wp.yaml"
    wp_path.write_text("\n".join(remaining_content))

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    err_msg = f"{wp_path} should not pass validation"
    assert "is invalid" in result.stderr, err_msg


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.parametrize(
    ("field_name", "expect_failure"),
    [
        ("name", True),
        ("description", True),
        ("steps", True),
        ("state", False),
        ("compute_environment", False),
        ("runtime_vars", False),
    ],
)
def test_workplan_optional_input(
    field_name: str,
    expect_failure: bool,
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that an incomplete workplan fails the CLI check.

    Starts with a sample workplan and removes a piece of required information in each test.

    Parameters
    ----------
    field_name : Path
        The field from the Workplan that will be removed.
    expect_failure : bool
        If the field missing will cause a deserialization failure.
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : str
        Directory containing workplan templates
    """
    wp_template = wp_templates_dir / "workplan.yaml"

    wp = deserialize(wp_template, Workplan)
    dumped = wp.model_dump(exclude_defaults=True, by_alias=True)

    # remove the attribute that should cause deserialization to fail
    del dumped[field_name]

    wp_path = tmp_path / "wp.yaml"

    dumper = yaml.Dumper
    dumper.add_multi_representer(enum.Enum, enum_representer)
    register_representer(PosixPath, path_representer)

    with wp_path.open("w") as fp:
        yaml.dump(dumped, fp, sort_keys=False)

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    err_msg = f"{wp_path} should not pass validation"
    is_invalid = "is valid" not in result.stdout
    assert is_invalid == expect_failure, err_msg


def test_workplan_check_remote_workplan_dne() -> None:
    """Verify that a URL to a remote workplan is handled properly and the
    workplan is not executed if the URL is invalid.
    """
    wp_uri = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml_XXX"

    runner = CliRunner()
    result = runner.invoke(app, [wp_uri], color=False)

    assert "not found" in result.stderr


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.parametrize(
    "wp_uri",
    [
        "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml",
        "HTTPS://raw.githubusercontent.com/cworthy-ocean/c-star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml",
    ],
)
def test_workplan_check_remote_workplan(
    wp_uri: str,
) -> None:
    """Verify that a URL to a remote workplan is handled properly and the
    workplan is executed.

    Parameters
    ----------
    wp_uri : str
        A working URL referencing a valid workplan
    """
    runner = CliRunner()
    result = runner.invoke(app, [wp_uri], color=False)

    assert "is valid" in result.stdout
