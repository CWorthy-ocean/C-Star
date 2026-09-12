import json
import os
import uuid
from pathlib import Path
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.applications.hello_world import HelloWorldSchemaAdapterV1V1
from cstar.applications.plotter import APP_NAME as APP_PLOTTER
from cstar.applications.plotter import PlotterSchemaAdapterV1V2
from cstar.applications.roms_marbl.app import APP_NAME as APP_ROMS
from cstar.applications.roms_marbl.migration import RomsMarblSchemaAdapter2025v1
from cstar.base.env import (
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_DISABLE_MIGRATION,
    ENV_CSTAR_STATE_HOME,
    FLAG_ON,
)
from cstar.cli.blueprint.migrate import (
    app,
    clobber_output,
    dryrun_notify,
    report_inplace_conflicts,
)
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_DRY_RUN
from cstar.system.migration import KEY_APP, identify_bounds

ARG_INPLACE = "--inplace"


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

    # Normalize away the rich error panel's hard wrapping, which can split
    # the message at any point depending on the tmp path length.
    plain_stderr = " ".join(result.stderr.replace("│", " ").split())
    assert "Invalid value for 'PATH'" in plain_stderr
    assert "was not found" in plain_stderr


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
    state_dir = Path(str(os.getenv(ENV_CSTAR_STATE_HOME, "")))
    expected_output_path = state_dir / f"{bp_path.stem}_{latest}{bp_path.suffix}"

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


def test_blueprint_migrate_disabled(
    plotter_v1_0_0_bp: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that `CSTAR_DISABLE_MIGRATION` blocks migration of an
    out-of-date blueprint with an early error.
    """
    monkeypatch.setenv(ENV_CSTAR_DISABLE_MIGRATION, FLAG_ON)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [plotter_v1_0_0_bp.as_posix()],
        color=False,
    )

    assert result.exit_code != 0
    # rich wraps the message at the terminal width, so a line break may land
    # inside the phrase; normalize whitespace before matching.
    assert "migration is disabled" in " ".join(result.stdout.split())


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


def test_blueprint_migrate_custom_output(
    tmp_path: Path,
    plotter_v1_0_0_bp: Path,
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
            expected_output_path.as_posix(),
        ],
        color=False,
    )

    assert not result.stderr, result.stderr
    assert not expected_output_path.exists()
    assert f"Migrating {source!r}->{target!r}" in result.stdout


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"output": "", "clobber": False},
        {"output": "some/output.yaml", "clobber": True},
    ],
)
def test_report_inplace_conflicts_disabled(params: dict[str, object]) -> None:
    """Verify that the callback is a no-op when in-place mode is not requested,
    regardless of the other parameter values.
    """
    ctx = mock.Mock(spec=typer.Context)
    ctx.params = params

    assert report_inplace_conflicts(ctx, value=False) is False


def test_report_inplace_conflicts_no_conflicts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify that in-place mode without conflicting parameters passes through
    silently.
    """
    ctx = mock.Mock(spec=typer.Context)
    ctx.params = {"output": "", "clobber": False}

    assert report_inplace_conflicts(ctx, value=True) is True
    assert not capsys.readouterr().out


def test_report_inplace_conflicts_output_ignored(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify that the user is informed that an output path is ignored when
    combined with in-place mode.
    """
    ctx = mock.Mock(spec=typer.Context)
    ctx.params = {"output": "out.yaml"}

    assert report_inplace_conflicts(ctx, value=True) is True
    assert "'out.yaml' will be ignored in in-place mode" in capsys.readouterr().out


def test_report_inplace_conflicts_clobber_cancels() -> None:
    """Verify that combining in-place mode with clobber is rejected to avoid
    destroying the input file.
    """
    ctx = mock.Mock(spec=typer.Context)
    ctx.params = {"clobber": True}

    with pytest.raises(typer.BadParameter, match="Cancelling"):
        report_inplace_conflicts(ctx, value=True)


def test_blueprint_migrate_inplace(plotter_v1_0_0_bp: Path) -> None:
    """Verify that in-place migration overwrites the source file with the
    migrated content and preserves the original in a backup file.
    """
    bp_path = plotter_v1_0_0_bp
    original_content = bp_path.read_text()

    bounds = identify_bounds([PlotterSchemaAdapterV1V2])[APP_PLOTTER]
    latest = bounds["max"]

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix(), ARG_INPLACE],
        color=False,
    )

    assert not result.stderr, result.stderr
    assert result.exit_code == 0, result.output

    # the source file now holds the migrated content
    migrated_content = bp_path.read_text()
    assert latest in migrated_content
    assert migrated_content != original_content

    # the original content is preserved in a backup next to the source
    backup_path = bp_path.with_suffix(f"{bp_path.suffix}.bak")
    assert backup_path.exists()
    assert backup_path.read_text() == original_content

    # rich wraps long paths at the terminal width, so strip all whitespace
    # before matching the persisted-to message
    squashed_stdout = "".join(result.stdout.split())
    assert f"persistedto{bp_path.as_posix()!r}" in squashed_stdout


def test_blueprint_migrate_inplace_ignores_output(
    tmp_path: Path,
    plotter_v1_0_0_bp: Path,
) -> None:
    """Verify that an output path is ignored when in-place mode is requested."""
    bp_path = plotter_v1_0_0_bp
    output_path = tmp_path / f"{uuid.uuid4()!s}.yaml"

    bounds = identify_bounds([PlotterSchemaAdapterV1V2])[APP_PLOTTER]
    latest = bounds["max"]

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix(), output_path.as_posix(), ARG_INPLACE],
        color=False,
    )

    assert not result.stderr, result.stderr
    assert result.exit_code == 0, result.output

    # the migration lands in the source file, not the requested output
    assert not output_path.exists()
    assert latest in bp_path.read_text()


def test_blueprint_migrate_inplace_clobber_conflict(
    plotter_v1_0_0_bp: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that requesting in-place migration with clobber enabled is
    rejected and leaves the source file untouched.
    """
    # register the env var with monkeypatch so the flag set by the clobber
    # callback during invocation is rolled back after the test
    monkeypatch.setenv(ENV_CSTAR_CLOBBER_WORKING_DIR, "0")

    bp_path = plotter_v1_0_0_bp
    original_content = bp_path.read_text()

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix(), ARG_INPLACE, ARG_CLOBBER],
        color=False,
    )

    assert result.exit_code != 0
    plain_stderr = " ".join(result.stderr.replace("│", " ").split())
    assert "Clobbering in-place will result in loss of the input file" in plain_stderr

    # the source file was not modified and no backup was produced
    assert bp_path.read_text() == original_content
    assert not bp_path.with_suffix(f"{bp_path.suffix}.bak").exists()


def test_dryrun_notify_output_ignored(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify that the user is informed that an output path is ignored when
    combined with dry-run mode.
    """
    ctx = mock.Mock(spec=typer.Context)
    ctx.params = {"output": "out.yaml"}

    assert dryrun_notify(ctx, value=True) is True
    assert "'out.yaml' will be ignored during dry-run" in capsys.readouterr().out


def test_dryrun_notify_disabled(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify that the callback is silent when dry-run mode is not requested."""
    ctx = mock.Mock(spec=typer.Context)
    ctx.params = {"output": "out.yaml"}

    assert dryrun_notify(ctx, value=False) is False
    assert not capsys.readouterr().out


def test_clobber_output_removes_existing(tmp_path: Path) -> None:
    """Verify that an existing output file is removed when clobber is enabled."""
    output_path = tmp_path / "output.yaml"
    output_path.touch()

    ctx = mock.Mock(spec=typer.Context)
    ctx.params = {"output": output_path.as_posix()}

    assert clobber_output(ctx, value=True) is True
    assert not output_path.exists()


def test_clobber_output_missing_output(tmp_path: Path) -> None:
    """Verify that a non-existent output file is tolerated when clobber is
    enabled.
    """
    output_path = tmp_path / "output.yaml"

    ctx = mock.Mock(spec=typer.Context)
    ctx.params = {"output": output_path.as_posix()}

    assert clobber_output(ctx, value=True) is True


def test_clobber_output_disabled(tmp_path: Path) -> None:
    """Verify that an existing output file is kept when clobber is disabled."""
    output_path = tmp_path / "output.yaml"
    output_path.touch()

    ctx = mock.Mock(spec=typer.Context)
    ctx.params = {"output": output_path.as_posix()}

    assert clobber_output(ctx, value=False) is False
    assert output_path.exists()


def test_blueprint_migrate_unplannable(
    tmp_path: Path,
    plotter_v1_0_0_bp: Path,
) -> None:
    """Verify that a blueprint whose schema version has no migration path
    reports the failure to produce a plan and exits with code 2.
    """
    model = json.loads(plotter_v1_0_0_bp.read_text())
    model["schema_version"] = "9.9.9"

    bp_path = tmp_path / "plotter_9.9.9.json"
    bp_path.write_text(json.dumps(model))

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix()],
        color=False,
    )

    assert result.exit_code == 2
    assert "Migration failed to produce a plan." in result.stdout
