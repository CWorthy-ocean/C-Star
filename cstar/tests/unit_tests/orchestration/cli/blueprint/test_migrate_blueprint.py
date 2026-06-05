import uuid
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.applications.hello_world_app import HelloWorldSchemaAdapterV1V1
from cstar.applications.plotter_app import APP_NAME as APP_PLOTTER
from cstar.applications.plotter_app import PlotterSchemaAdapterV1V2
from cstar.applications.roms_marbl.app import APP_NAME as APP_ROMS
from cstar.applications.roms_marbl.migration import RomsMarblSchemaAdapter2025v1
from cstar.cli.blueprint.migrate import app
from cstar.entrypoint.utils import ARG_DRY_RUN, ARG_OUTPUT_LONG, ARG_OUTPUT_SHORT
from cstar.system.migration import KEY_APP, identify_bounds


@pytest.fixture
def blueprint_1_0_0(
    tmp_path: Path,
    bp_templates_dir: Path,
) -> Path:
    work_dir = tmp_path / str(uuid.uuid4())
    work_dir.mkdir(parents=True)

    versioned_file_name = "blueprint.1.0.0.yaml"
    bp_2025_1 = bp_templates_dir / APP_ROMS / versioned_file_name
    bp_path = work_dir / versioned_file_name
    content = bp_2025_1.read_text()
    bp_path.write_text(content)
    return bp_path


@pytest.fixture
def blueprint_1_0_0_sleep(blueprint_1_0_0: Path) -> Path:
    content = blueprint_1_0_0.read_text()
    # content = content.replace("application: sleep", f"application: {APP_ROMS}")

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

    assert "Invalid value for 'PATH'" in result.stderr
    assert "was not found" in result.stderr


def test_blueprint_migrate_remote_blueprint_dne() -> None:
    """Verify that a URL to a non-existent blueprint does not pass validation."""
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint-X.yaml"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path],
        color=False,
    )

    assert "Unable to retrieve remote file" in result.stderr


def test_blueprint_migrate_persist_to_default(
    mock_state_dir: Path,
    plotter_v1_0_0_bp: Path,
) -> None:
    """Verify that a request to migrate a blueprint without specifying an output
    path explicitly results in the creation of a file matching the expected naming
    convention `<input_file_stem>_<latest_version>.<ext>` in `$CSTAR_STATE_HOME`
    """
    app_name = APP_PLOTTER
    bounds = identify_bounds([PlotterSchemaAdapterV1V2])[app_name]
    latest = bounds["max"]

    bp_path = plotter_v1_0_0_bp
    expected_output_path = mock_state_dir / f"{bp_path.stem}_{latest}{bp_path.suffix}"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix()],
        color=False,
    )

    assert not result.stderr, result.stderr
    assert expected_output_path.exists()
    assert expected_output_path.is_file()

    # sanity check content is not empty
    content = expected_output_path.read_text()
    assert KEY_APP in content
    assert app_name in content


def test_blueprint_migrate_unnecessary(hello_world_bp_path: Path) -> None:
    """Verify that the user is informed that no migration is necessary
    when a blueprint has the latest schema version.
    """
    bounds = identify_bounds([HelloWorldSchemaAdapterV1V1])
    latest = bounds[HelloWorldSchemaAdapterV1V1.application()]["max"]

    bp_path = hello_world_bp_path

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix()],
        color=False,
    )
    assert "No migration needed" in result.stdout
    assert latest in result.stdout


@pytest.mark.parametrize("output_param", [ARG_OUTPUT_SHORT, ARG_OUTPUT_LONG])
def test_blueprint_migrate_custom_output(
    tmp_path: Path,
    plotter_v1_0_0_bp: Path,
    output_param: str,
) -> None:
    """Verify that an output path specified by the user is honored."""
    bp_path = plotter_v1_0_0_bp  # blueprint_1_0_0_sleep
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
    plotter_v1_0_0_bp: Path,
) -> None:
    """Verify that dry run mode does not produce a file and displays the plan
    to the user.
    """
    bounds = identify_bounds([RomsMarblSchemaAdapter2025v1])[APP_ROMS]
    source = bounds["min"]
    target = bounds["max"]

    bp_path = plotter_v1_0_0_bp
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
