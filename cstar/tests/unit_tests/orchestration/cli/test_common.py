"""Tests for shared CLI helpers in ``cstar.cli.common``."""

import json
import os
import typing as t
from pathlib import Path
from unittest import mock

import pytest
import typer
from pydantic import Field

from cstar.applications import core
from cstar.applications.hello_world import (
    HelloWorldApplication,
    HelloWorldBlueprint,
)
from cstar.base.env import ENV_CSTAR_DISABLE_MIGRATION, FLAG_ON
from cstar.cli.common import (
    checkmark,
    colored,
    execute_migration,
    id_label,
    italic,
    label,
    localize_and_migrate,
    present,
    schema_message,
    set_ctxmap,
)
from cstar.execution.file_system import DirectoryManager
from cstar.system.migration import (
    BlueprintMigration,
    CstarManualMigrationError,
    CstarMigrationError,
    CstarSchemaTooNewError,
    MigrationRequest,
)

APP_MINOR = "hello_world_minor"
"""Name of the test application whose schema is 1.1.0."""


class MinorBlueprint(HelloWorldBlueprint):
    """A blueprint whose schema gained a minor version without needing an adapter."""

    application: str = APP_MINOR
    schema_version: str = Field(default="1.1.0")


class MinorApplication(HelloWorldApplication):
    """A test application whose current schema is 1.1.0 and has no migrations."""

    name = APP_MINOR
    blueprint = MinorBlueprint
    migrations = ()


@pytest.fixture
def minor_app() -> t.Iterator[None]:
    """Register `MinorApplication` for the duration of a test.

    The registry has no unregister, so the entry is patched in and removed again.
    """
    with mock.patch.dict(core._registry, {APP_MINOR: MinorApplication}):
        yield


@pytest.fixture
def older_minor_bp(tmp_path: Path, minor_app: None) -> Path:
    """A blueprint at 1.0.0 for an application whose current schema is 1.1.0."""
    model = {
        "name": "Older minor",
        "description": "compatible with the current schema",
        "application": APP_MINOR,
        "state": "draft",
        "target": "world",
        "schema_version": "1.0.0",
    }
    bp_path = tmp_path / "older_minor.json"
    bp_path.write_text(json.dumps(model))
    return bp_path


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


def test_schema_message(tmp_path: Path) -> None:
    """Verify the planner's text is prefixed with the blueprint path."""
    ex = CstarSchemaTooNewError("plotter", "9.9.9", "2.0.0")

    msg = schema_message(tmp_path / "bp.yaml", ex)

    assert msg == f"Blueprint '{tmp_path / 'bp.yaml'}': {ex}"


def test_execute_migration_too_new(unsupported_version_bp: Path) -> None:
    """Verify that a blueprint newer than this build reads raises instead of
    producing an error result.
    """
    request = MigrationRequest(path=unsupported_version_bp)

    with pytest.raises(CstarSchemaTooNewError, match="Upgrade cstar-ocean"):
        execute_migration(request)


def test_execute_migration_manual(
    tmp_path: Path,
    plotter_v1_0_0_model: dict[str, t.Any],
) -> None:
    """Verify that an older major version without an automatic migration raises."""
    bp_path = tmp_path / "plotter_0.5.0.json"
    bp_path.write_text(json.dumps({**plotter_v1_0_0_model, "schema_version": "0.5.0"}))

    with pytest.raises(CstarManualMigrationError, match="no automatic migration"):
        execute_migration(MigrationRequest(path=bp_path))


@mock.patch.dict(os.environ, {ENV_CSTAR_DISABLE_MIGRATION: FLAG_ON})
def test_execute_migration_compatible_not_disabled(older_minor_bp: Path) -> None:
    """Verify that an older minor version is compatible as-is: nothing is
    persisted and `CSTAR_DISABLE_MIGRATION` does not refuse it.
    """
    request = MigrationRequest(path=older_minor_bp)
    state_home = DirectoryManager.state_home()
    before = set(state_home.glob("*"))

    result = execute_migration(request)

    assert result.target == request.source
    plan = result.migration_result.plan
    assert plan is not None
    assert plan.is_compatible
    assert (plan.source, plan.target) == ("1.0.0", "1.1.0")
    assert set(state_home.glob("*")) == before


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


def test_localize_and_migrate_error(unsupported_version_bp: Path) -> None:
    """Verify that a schema error during localization propagates to the caller,
    which reports it with the blueprint path.
    """
    with pytest.raises(CstarSchemaTooNewError):
        localize_and_migrate(unsupported_version_bp.as_posix())


def test_localize_and_migrate_failed_migration(plotter_v1_0_0_bp: Path) -> None:
    """Verify that a failure while executing a planned migration is reported
    as a bad parameter rather than a planner error.
    """
    with (
        mock.patch.object(
            BlueprintMigration,
            "migrate",
            side_effect=CstarMigrationError("kaboom"),
        ),
        pytest.raises(typer.Exit),
    ):
        localize_and_migrate(plotter_v1_0_0_bp.as_posix())


def test_set_ctxmap_overwrite(capsys: pytest.CaptureFixture) -> None:
    """Verify that overwriting an existing context value warns the user."""
    ctx = mock.Mock(spec=typer.Context)
    ctx.obj = {"key": "original"}

    set_ctxmap(ctx, "key", "updated")

    assert ctx.obj["key"] == "updated"
    assert "will be overwritten" in capsys.readouterr().out
