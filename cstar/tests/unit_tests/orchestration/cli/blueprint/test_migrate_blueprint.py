import uuid
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.cli.blueprint.migrate import app
from cstar.entrypoint.utils import ARG_DRY_RUN, ARG_OUTPUT_LONG, ARG_OUTPUT_SHORT
from cstar.orchestration.serialization import deserialize
from cstar.system.migration import hw_bounds, rm_bounds


@pytest.fixture
def blueprint_1_0_0(
    tmp_path: Path,
    bp_templates_dir: Path,
) -> Path:
    work_dir = tmp_path / str(uuid.uuid4())
    work_dir.mkdir(parents=True)

    versioned_file_name = "blueprint.1.0.0.yaml"
    bp_2025_1 = bp_templates_dir / "roms_marbl" / versioned_file_name
    bp_path = work_dir / versioned_file_name
    content = bp_2025_1.read_text()
    bp_path.write_text(content)
    return bp_path


@pytest.fixture
def blueprint_1_0_0_sleep(blueprint_1_0_0: Path) -> Path:
    content = blueprint_1_0_0.read_text()
    content = content.replace("application: sleep", "application: roms_marbl")

    bp_path = blueprint_1_0_0.with_stem(f"{blueprint_1_0_0.stem}_sleep")
    bp_path.write_text(content)
    return bp_path


def test_blueprint_migrate_file_dne(tmp_path: Path) -> None:
    """Verify that a path to a non-existent blueprint does not pass validation.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    bp_path = tmp_path / "blueprint-dne.yml"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix()],
        color=False,
    )

    assert "not found" in result.stderr


def test_blueprint_migrate_remote_blueprint_dne() -> None:
    """Verify that a URL to a non-existent blueprint does not pass validation."""
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint-X.yaml"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path],
        color=False,
    )

    assert "not found" in result.stderr


def test_blueprint_migrate_default_output(blueprint_1_0_0_sleep: Path) -> None:
    """Verify that a request to migrate a blueprint without specifying an output
    path explicitly results in the creation of a file matching the expected naming
    convention `<input_file_stem>_<latest_version>.yaml`
    """
    latest = rm_bounds["max"]
    bp_path = blueprint_1_0_0_sleep
    expected_output_path = Path(f"./{bp_path.stem}_{latest}.yaml")

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix()],
        color=False,
    )

    assert not result.stderr, result.stderr
    assert expected_output_path.exists()
    assert expected_output_path.is_file()

    bp = deserialize(expected_output_path, RomsMarblBlueprint)
    assert bp
    # the test writes to current working directory. clean up.
    expected_output_path.unlink()


def test_blueprint_migrate_unnecessary(hello_world_bp_path: Path) -> None:
    """Verify that the user is informed that no migration is necessary
    when a blueprint has the latest schema version.
    """
    latest = hw_bounds["max"]
    bp_path = hello_world_bp_path

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix()],
        color=False,
    )
    assert "uses the latest" in result.stdout
    assert latest in result.stdout


@pytest.mark.parametrize("output_param", [ARG_OUTPUT_SHORT, ARG_OUTPUT_LONG])
def test_blueprint_migrate_custom_output(
    tmp_path: Path,
    blueprint_1_0_0_sleep: Path,
    output_param: str,
) -> None:
    """Verify that an output path specified by the user is honored."""
    bp_path = blueprint_1_0_0_sleep
    file_name = f"{uuid.uuid4()!s}.yaml"
    expected_output_path = tmp_path / file_name

    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            bp_path.as_posix(),
            output_param,
            expected_output_path.as_posix(),
        ],
        color=False,
    )

    assert not result.stderr, result.stderr
    assert expected_output_path.exists()
    assert expected_output_path.is_file()


def test_blueprint_migrate_dry_run(
    tmp_path: Path,
    blueprint_1_0_0_sleep: Path,
) -> None:
    """Verify that dry run mode does not produce a file and displays the plan
    to the user.
    """
    source = rm_bounds["min"]
    target = rm_bounds["max"]
    bp_path = blueprint_1_0_0_sleep
    expected_output_path = tmp_path / "upgraded.yaml"

    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            bp_path.as_posix(),
            ARG_DRY_RUN,
            ARG_OUTPUT_LONG,
            expected_output_path.as_posix(),
        ],
        color=False,
    )

    assert not result.stderr, result.stderr
    assert not expected_output_path.exists()
    assert f"Migrating {source!r}->{target!r}" in result.stdout
