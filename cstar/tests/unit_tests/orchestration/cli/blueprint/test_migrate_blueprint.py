from pathlib import Path

from typer.testing import CliRunner

from cstar.cli.blueprint.migrate import ARG_OUTPUT_PATH_SHORT, app
from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import deserialize
from cstar.system.migration import BlueprintMigrationManager


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


def test_blueprint_migrate_default_output(
    tmp_path: Path,
    bp_templates_dir: Path,
) -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is executed.
    """
    latest = BlueprintMigrationManager.latest
    work_dir = tmp_path / "subdir"
    work_dir.mkdir(parents=True)

    bp_stem = "source"
    versioned_file_name = "blueprint.2025.1.yaml"
    bp_2025_1 = bp_templates_dir / versioned_file_name
    bp_path = work_dir / f"{bp_stem}.yaml"
    bp_path.write_text(bp_2025_1.read_text())

    expected_output_path = Path(f"./{bp_stem}_{latest}.yaml")

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


def test_blueprint_migrate_custom_output(
    tmp_path: Path,
    bp_templates_dir: Path,
) -> None:
    """Verify that a URL the output path specified by the user is honored."""
    latest = BlueprintMigrationManager.latest
    work_dir = tmp_path / "subdir"
    work_dir.mkdir(parents=True)

    bp_stem = "source"
    versioned_file_name = "blueprint.2025.1.yaml"
    bp_2025_1 = bp_templates_dir / versioned_file_name
    bp_path = work_dir / f"{bp_stem}.yaml"
    bp_path.write_text(bp_2025_1.read_text())

    expected_output_path = work_dir / f"{bp_stem}_{latest}.yaml"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            bp_path.as_posix(),
            ARG_OUTPUT_PATH_SHORT,
            expected_output_path.as_posix(),
        ],
        color=False,
    )

    assert not result.stderr, result.stderr
    assert expected_output_path.exists()
    assert expected_output_path.is_file()
