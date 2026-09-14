"""Tests for shared CLI helpers in ``cstar.cli.common``."""

import json
import typing as t
from pathlib import Path
from unittest import mock

import pytest
import typer

from cstar.cli.common import (
    checkmark,
    colored,
    execute_migration,
    id_label,
    italic,
    label,
    localize_and_migrate,
    present,
    set_ctxmap,
)
from cstar.system.migration import (
    BlueprintMigration,
    CstarMigrationError,
    MigrationRequest,
)


@pytest.fixture
def unsupported_version_bp(
    tmp_path: Path,
    plotter_v1_0_0_model: dict[str, t.Any],
) -> Path:
    """A blueprint with a schema version no adapter can migrate from."""
    model = {**plotter_v1_0_0_model, "schema_version": "9.9.9"}

    bp_path = tmp_path / "plotter_9.9.9.json"
    bp_path.write_text(json.dumps(model))
    return bp_path


def test_colored() -> None:
    assert colored("msg") == "[cyan]msg[/cyan]"
    assert colored("msg", "red") == "[red]msg[/red]"


def test_italic() -> None:
    assert italic("msg") == "[italic]msg[/italic]"


def test_checkmark() -> None:
    assert checkmark("green") == "[green]:heavy_check_mark:[/green]"


def test_present() -> None:
    assert present("name", "value") == "name: [cyan]value[/cyan]"
    assert present("name", "value", "red", 6) == "  name: [red]value[/red]"


def test_label() -> None:
    assert label("step", "plotter") == "step ([italic]plotter[/italic])"
    assert label("step", None) == "step"


def test_id_label() -> None:
    assert id_label(3, "step", "plotter") == "3. step ([italic]plotter[/italic])"
    assert id_label(3, "step", None) == "3. step"


def test_execute_migration_unplannable(unsupported_version_bp: Path) -> None:
    """Verify that a blueprint without a migration path produces an error
    result pointing at the unmodified source instead of raising.
    """
    request = MigrationRequest(path=unsupported_version_bp)

    result = execute_migration(request)

    assert "Unable to plan migration" in result.migration_result.error
    assert result.migration_result.plan is None
    # target == source indicates no change occurred
    assert result.target == request.source


def test_execute_migration_failed_migration(
    plotter_v1_0_0_bp: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    """Verify that a failure while executing a planned migration reports the
    error to the user and exits with a non-zero code.
    """
    request = MigrationRequest(path=plotter_v1_0_0_bp)

    with (
        mock.patch.object(
            BlueprintMigration,
            "migrate",
            side_effect=CstarMigrationError("kaboom"),
        ),
        pytest.raises(typer.Exit) as exc_info,
    ):
        execute_migration(request)

    assert exc_info.value.exit_code == 1
    assert "Unable to complete migration: kaboom" in capsys.readouterr().out


def test_localize_and_migrate_error(
    unsupported_version_bp: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    """Verify that a migration error during localization is reported to the
    user and exits with a non-zero code.
    """
    with pytest.raises(typer.Exit) as exc_info:
        localize_and_migrate(unsupported_version_bp.as_posix())

    assert exc_info.value.exit_code == 1
    assert "Unable to plan migration" in capsys.readouterr().out


def test_set_ctxmap_overwrite(capsys: pytest.CaptureFixture) -> None:
    """Verify that overwriting an existing context value warns the user."""
    ctx = mock.Mock(spec=typer.Context)
    ctx.obj = {"key": "original"}

    set_ctxmap(ctx, "key", "updated")

    assert ctx.obj["key"] == "updated"
    assert "will be overwritten" in capsys.readouterr().out
