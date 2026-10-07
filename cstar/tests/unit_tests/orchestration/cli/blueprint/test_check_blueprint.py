import json
import typing as t
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.applications.core import get_application
from cstar.applications.plotter import APP_NAME as APP_PLOTTER
from cstar.cli.blueprint.check import app


def test_blueprint_check_file_dne(
    tmp_path: Path,
) -> None:
    """Verify that a path to a non-existent blueprint fails a validity check.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    blueprint_path = tmp_path / "blueprint-dne.yml"
    args = [str(blueprint_path)]

    runner = CliRunner()
    result = runner.invoke(app, args, color=False)

    assert "not found" in result.stderr


def test_blueprint_check_file_no_content(
    tmp_path: Path,
) -> None:
    """Verify that an empty blueprint file fails a validity check.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    blueprint_path = tmp_path / "empty_blueprint.yml"
    blueprint_path.touch()
    args = [str(blueprint_path)]

    runner = CliRunner()
    result = runner.invoke(app, args, color=False)

    assert "is invalid" in result.stderr


@pytest.mark.parametrize(
    "content",
    [" ", "", "\n", '{"foo": "bar"}'],
)
def test_blueprint_check_file_bad_content(
    tmp_path: Path,
    content: str,
) -> None:
    """Verify that an empty blueprint fails a validity check.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    blueprint_path = tmp_path / "invalid_blueprint.yml"
    blueprint_path.write_text(content)
    args = [str(blueprint_path)]

    runner = CliRunner()
    result = runner.invoke(app, args, color=False)

    assert "is invalid" in result.stderr


@pytest.mark.parametrize(
    "repo_relative_path",
    [
        Path("cstar/tests/integration_tests/blueprints/blueprint_template.yaml"),
        Path("cstar/tests/integration_tests/blueprints/blueprint_complete.yaml"),
        Path("docs/tutorials/wales_toy_blueprint.yaml"),
        Path("docs/tutorials/wio_toy_blueprint.yaml"),
    ],
)
def test_blueprint_valid_input(
    repo_relative_path: Path,
    package_path: Path,
) -> None:
    """Verify that a valid blueprint passes the CLI check.

    NOTE: This test also serves a practical purpose of confirming the continued
    validity of tutorial and sample blueprints.

    Parameters
    ----------
    repo_relative_path : Path
        Relative path to a blueprint within the c-star repo
    package_path : Path
        Absolute path to the c-star package on disk
    """
    blueprint_path = package_path / repo_relative_path
    args = [str(blueprint_path)]

    runner = CliRunner()
    result = runner.invoke(app, args, color=False)

    assert "is valid" in result.stdout, (
        f"`{blueprint_path}` does not contain a valid blueprint"
    )


@pytest.mark.parametrize(
    ("start_removal", "end_removal"),
    [
        ("name:", None),
        ("description:", None),
        ("valid_start_date:", None),
        ("valid_end_date:", None),
        ("code:", "grid:"),
        ("grid:", "initial_conditions:"),
        ("initial_conditions:", "forcing"),
        ("forcing:", "partitioning"),
        ("partitioning:", "namelist_overrides:"),
        ("runtime_params:", "<EOF>"),
        ("run_time:", "compile_time:"),
        ("compile_time:", "grid:"),
        ("location:", None),
        ("branch", None),
        ("roms:", "marbl:"),
        ("directory:", None),
    ],
)
def test_blueprint_incomplete_input(
    start_removal: str,
    end_removal: str | None,
    tests_path: Path,
    tmp_path: Path,
) -> None:
    """Verify that an incomplete blueprint fails the CLI check.

    Starts with a sample blueprint and removes a piece of required information in each test.

    Parameters
    ----------
    start_removal : Path
        A string that will trigger content skipping to begin when building a test blueprint
    end_removal : Path
       A string that will trigger content skipping to end when building a test blueprint
    tests_path : Path
        Path to the c-star tests directory
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    blueprint_path = tests_path / "integration_tests/blueprints/blueprint_complete.yaml"

    content = blueprint_path.read_text().splitlines()
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
            remaining_content.append(line)

    bp_path = tmp_path / "bp.yaml"
    bp_path.write_text("\n".join(remaining_content))
    args = [str(bp_path)]

    runner = CliRunner()
    result = runner.invoke(app, args, color=False)

    err_msg = f"{bp_path} should not pass validation"
    assert "is invalid" in result.stderr, err_msg


@pytest.mark.parametrize(
    ("start_removal", "end_removal"),
    [
        ("state:", None),
        ("filter:", "compile_time"),
        ("files:", "directory:"),
    ],
)
def test_blueprint_optional_input(
    start_removal: str,
    end_removal: str | None,
    tests_path: Path,
    tmp_path: Path,
) -> None:
    """Verify that an incomplete blueprint passes the CLI check if the missing information
    is not required.

    Starts with a sample blueprint and removes a piece of optional information in each test.

    Parameters
    ----------
    start_removal : Path
        A string that will trigger content skipping to begin when building a test blueprint
    end_removal : Path
       A string that will trigger content skipping to end when building a test blueprint
    tests_path : Path
        Path to the c-star tests directory
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    blueprint_path = tests_path / "integration_tests/blueprints/blueprint_complete.yaml"

    content = blueprint_path.read_text().splitlines()
    remaining_content: list[str] = []
    cutting, cut_once = False, False

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
    args = [str(bp_path)]

    runner = CliRunner()
    result = runner.invoke(app, args, color=False)

    err_msg = f"{bp_path} should not pass validation"
    assert "is valid" in result.stdout, err_msg


def test_blueprint_check_remote_blueprint_dne() -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is not executed if the URL is invalid.
    """
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint-X.yaml"
    args = [str(bp_path)]

    runner = CliRunner()
    result = runner.invoke(app, args, color=False)

    assert "not found" in result.stderr


@pytest.mark.xfail(
    reason=(
        "cstar_blueprint_roms_marbl_example's wales_toy_blueprint.yaml is still "
        "schema 2.0.0 (has a bare `model_params` section) and roms_marbl schema "
        "3.0.0 forbids it; remove once the sibling repo publishes a 3.0.0 blueprint"
    ),
    strict=False,
)
@pytest.mark.parametrize(
    "bp_uri",
    [
        "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml",
        "HTTPS://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml",
    ],
)
def test_blueprint_check_remote_blueprint(
    bp_uri: str,
) -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is executed.

    Parameters
    ----------
    bp_uri : str
        A working URL referencing a valid blueprint
    """
    args = [bp_uri]

    runner = CliRunner()
    result = runner.invoke(app, args, color=False)

    assert "is valid" in result.stdout


def test_blueprint_check_too_new(
    tmp_path: Path,
    plotter_v1_0_0_model: dict[str, t.Any],
) -> None:
    """Verify that a blueprint newer than this build reads is refused with the
    upgrade message before its content is validated.
    """
    bp_path = tmp_path / "plotter_9.9.9.json"
    bp_path.write_text(json.dumps({**plotter_v1_0_0_model, "schema_version": "9.9.9"}))

    runner = CliRunner()
    result = runner.invoke(app, [str(bp_path)], color=False)

    latest = get_application(APP_PLOTTER).schema_version
    output = " ".join(result.stdout.split())
    assert result.exit_code == 1
    assert f"Blueprint {str(bp_path)!r}: {APP_PLOTTER} schema 9.9.9 is newer" in output
    assert f"reads ({latest}). Upgrade cstar-ocean" in output
    assert "is valid" not in output


def test_blueprint_check_manual_migration(
    tmp_path: Path,
    plotter_v1_0_0_model: dict[str, t.Any],
) -> None:
    """Verify that a blueprint with no automatic migration is refused."""
    bp_path = tmp_path / "plotter_0.5.0.json"
    bp_path.write_text(json.dumps({**plotter_v1_0_0_model, "schema_version": "0.5.0"}))

    runner = CliRunner()
    result = runner.invoke(app, [str(bp_path)], color=False)

    output = " ".join(result.stdout.split())
    assert result.exit_code == 1
    assert f"Blueprint {str(bp_path)!r}: " in output
    assert "has no automatic migration" in output


def test_blueprint_check_needs_migration(plotter_v1_0_0_bp: Path) -> None:
    """Verify that an older major version with an automatic migration is
    reported as needing migration, naming the command to run.
    """
    latest = get_application(APP_PLOTTER).schema_version

    runner = CliRunner()
    result = runner.invoke(app, [str(plotter_v1_0_0_bp)], color=False)

    output = " ".join(result.stdout.split())
    assert result.exit_code == 1
    assert f"is {APP_PLOTTER} schema 1.0.0 and needs migration to {latest}" in output
    assert f"run: cstar blueprint migrate {plotter_v1_0_0_bp}" in output
    assert "is valid" not in output
