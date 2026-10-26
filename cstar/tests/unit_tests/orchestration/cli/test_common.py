"""Tests for shared CLI helpers in ``cstar.cli.common``."""

import json
import os
import typing as t
from collections.abc import Callable
from pathlib import Path
from unittest import mock

import pytest
import typer
import yaml
from pydantic import Field
from typer.testing import CliRunner

from cstar.applications import core
from cstar.applications.hello_world import (
    HelloWorldApplication,
    HelloWorldBlueprint,
)
from cstar.base.env import ENV_CSTAR_DISABLE_MIGRATION, FLAG_ON
from cstar.cli.cli import app as root_app
from cstar.cli.common import (
    checkmark,
    colored,
    execute_migration,
    get_persist_to,
    id_label,
    italic,
    label,
    localize_and_migrate,
    persist_migration,
    present,
    schema_message,
    set_ctxmap,
)
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.serialization import read_blueprint_raw
from cstar.orchestration.utils import ENV_CSTAR_SLURM_ACCOUNT
from cstar.system.migration import (
    BlueprintMigration,
    CstarManualMigrationError,
    CstarMigrationError,
    CstarSchemaTooNewError,
    MigrateResult,
    MigrationPlan,
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


_PLAN = MigrationPlan("1.0.0", "2.0.0", ())
"""A plan whose target names the version in a migrated copy's filename."""


def _write_bp(path: Path, content: str = "name: bp\n") -> Path:
    """Write a stand-in source file; `get_persist_to` reads only its bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def test_get_persist_to_names_a_copy_in_state_home(tmp_path: Path) -> None:
    """Verify a copy without an explicit target lands in the state home, named
    after the source's stem and the target version.
    """
    bp_path = _write_bp(tmp_path / "blueprint.yaml")

    persist_to = get_persist_to(MigrationRequest(path=bp_path), _PLAN)

    assert persist_to.parent == DirectoryManager.state_home().resolve()
    assert persist_to.name.startswith("blueprint-")
    assert persist_to.name.endswith("_2.0.0.yaml")


def test_get_persist_to_is_stable_for_one_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify an unchanged source maps to one copy, however its path is spelled."""
    bp_path = _write_bp(tmp_path / "blueprint.yaml")
    monkeypatch.chdir(tmp_path)

    absolute = get_persist_to(MigrationRequest(path=bp_path), _PLAN)
    relative = get_persist_to(MigrationRequest(path=Path("blueprint.yaml")), _PLAN)

    assert absolute == relative


def test_get_persist_to_separates_same_named_sources(tmp_path: Path) -> None:
    """Verify identical files sharing a filename in different directories get
    separate copies, since relative locations anchor to each one's directory.
    """
    first = _write_bp(tmp_path / "first" / "blueprint.yaml")
    second = _write_bp(tmp_path / "second" / "blueprint.yaml")

    assert get_persist_to(MigrationRequest(path=first), _PLAN) != get_persist_to(
        MigrationRequest(path=second), _PLAN
    )


def test_get_persist_to_separates_edits_of_one_source(tmp_path: Path) -> None:
    """Verify editing a source names a new copy instead of replacing the copy an
    earlier run may still read.
    """
    bp_path = _write_bp(tmp_path / "blueprint.yaml")
    before = get_persist_to(MigrationRequest(path=bp_path), _PLAN)

    _write_bp(bp_path, "name: edited\n")

    assert get_persist_to(MigrationRequest(path=bp_path), _PLAN) != before


def test_get_persist_to_honours_an_explicit_target(tmp_path: Path) -> None:
    """Verify an explicit target (`--output`) is used as given."""
    bp_path = _write_bp(tmp_path / "blueprint.yaml")
    target = tmp_path / "out" / "migrated.yaml"

    assert get_persist_to(MigrationRequest(path=bp_path, output=target), _PLAN) == (
        target.resolve()
    )


@pytest.fixture
def relative_location_bp(tmp_path: Path, bp_templates_dir: Path) -> Path:
    """A UCLA-ROMS blueprint, in its own directory, with a relative location."""
    bp_dir = tmp_path / "blueprints"
    bp_dir.mkdir()
    bp_path = bp_dir / "bp.yaml"
    bp_path.write_text(
        (bp_templates_dir / "blueprint.yaml")
        .read_text()
        .replace("http://mockdoc.com/partitioning1.nc", "../data/boundary.nc")
    )
    return bp_path


def _persist_with_target(bp_path: Path, target: Path) -> dict[str, t.Any]:
    """Persist a (no-op) migration of `bp_path` to `target` and read the result back."""
    migrated = yaml.safe_load(bp_path.read_text())
    plan = MigrationPlan(migrated["schema_version"], migrated["schema_version"], ())
    request = MigrationRequest(path=bp_path, output=target)

    persisted = persist_migration(request, MigrateResult(migrated, migrated, plan=plan))

    assert persisted == target.resolve()
    return yaml.safe_load(persisted.read_text())


def test_persist_migration_anchors_relative_locations_when_moving(
    relative_location_bp: Path, tmp_path: Path
) -> None:
    """Verify a migrated copy written to another directory still names the files the
    original named, rather than resolving them against its new directory.
    """
    persisted = _persist_with_target(
        relative_location_bp, tmp_path / "state" / "migrated.yaml"
    )

    location = persisted["forcing"]["boundary"]["data"][0]["location"]
    assert location == (tmp_path / "data" / "boundary.nc").as_posix()


def test_persist_migration_keeps_relative_locations_beside_the_source(
    relative_location_bp: Path,
) -> None:
    """Verify a migrated copy written next to its source keeps the authored text."""
    persisted = _persist_with_target(
        relative_location_bp, relative_location_bp.with_name("migrated.yaml")
    )

    location = persisted["forcing"]["boundary"]["data"][0]["location"]
    assert location == "../data/boundary.nc"


def test_persist_migration_keeps_relative_locations_for_a_relative_target(
    relative_location_bp: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify `--output migrated.yaml`, given from the source's own directory, is
    beside the source and so keeps the authored text.
    """
    monkeypatch.chdir(relative_location_bp.parent)
    migrated = yaml.safe_load(relative_location_bp.read_text())
    plan = MigrationPlan(migrated["schema_version"], migrated["schema_version"], ())
    request = MigrationRequest(path=relative_location_bp, output=Path("migrated.yaml"))

    persisted = persist_migration(request, MigrateResult(migrated, migrated, plan=plan))

    location = yaml.safe_load(persisted.read_text())["forcing"]["boundary"]["data"][0][
        "location"
    ]
    assert persisted.parent == relative_location_bp.parent.resolve()
    assert location == "../data/boundary.nc"


def test_persist_migration_rejects_cwd_only_locations(
    relative_location_bp: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a location that exists only relative to the working directory is
    reported, not silently carried to a directory it would not resolve in.
    """
    (tmp_path / "cwd" / "data").mkdir(parents=True)
    (tmp_path / "cwd" / "data" / "boundary.nc").touch()
    relative_location_bp.write_text(
        relative_location_bp.read_text().replace(
            "../data/boundary.nc", "data/boundary.nc"
        )
    )
    monkeypatch.chdir(tmp_path / "cwd")

    with pytest.raises(typer.BadParameter, match="directory of the blueprint file"):
        _persist_with_target(relative_location_bp, tmp_path / "state" / "migrated.yaml")


def test_localize_and_migrate_anchors_a_remote_blueprint_to_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bp_templates_dir: Path
) -> None:
    """Verify the local copy of a downloaded blueprint holds its relative locations
    as absolute paths against the working directory, so file-based steps that read
    the copy later do not anchor them to its temporary directory.
    """
    text = (
        (bp_templates_dir / "blueprint.yaml")
        .read_text()
        .replace("http://mockdoc.com/partitioning1.nc", "../data/boundary.nc")
    )
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    response = mock.Mock(status_code=200, text=text)

    with mock.patch("cstar.execution.file_system.request", return_value=response):
        localized, migrated = localize_and_migrate("https://example.com/bp.yaml")

    assert not migrated
    expected = (tmp_path / "data" / "boundary.nc").resolve().as_posix()
    written = yaml.safe_load(localized.read_text())
    assert written["forcing"]["boundary"]["data"][0]["location"] == expected
    # a later read of the copy, which anchors to its own directory, agrees
    assert (
        read_blueprint_raw(localized)["forcing"]["boundary"]["data"][0]["location"]
        == expected
    )


def test_root_callback_skips_unknown_and_removed_settings(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a configuration file naming a removed setting and one from a newer
    version runs the command, applying the rest of the file and warning once
    about each skipped line.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    isolate_user_config.write_text(
        f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n"
        "CSTAR_ORCH_TRX_FREQ=1D\n"
        "CSTAR_FROM_A_NEWER_VERSION=1\n"
    )

    result = CliRunner().invoke(root_app, ["env", "show"], color=False)

    assert result.exit_code == 0, result.stderr
    assert result.stderr.count("Ignoring a line in") == 2
    assert "CSTAR_ORCH_TRX_FREQ has no effect" in result.stderr
    assert "CSTAR_FROM_A_NEWER_VERSION is not a setting C-Star" in result.stderr
    assert f"{ENV_CSTAR_SLURM_ACCOUNT}: m1234 [config]" in flatten_cli_output(
        result.stdout
    )


@pytest.fixture
def unreadable_config(isolate_user_config: Path) -> Path:
    """A configuration file the loader refuses: it cannot be decoded."""
    isolate_user_config.write_bytes(b"\xff\xfeCSTAR_LOG_LEVEL=DEBUG\n")
    return isolate_user_config


def test_root_callback_exits_on_an_unreadable_configuration_file(
    unreadable_config: Path,
) -> None:
    """Verify a configuration file the loader cannot read stops an ordinary
    command with exit code 2 and the error on stderr.

    Parameters
    ----------
    unreadable_config : Path
        The configuration file the loader refuses.
    """
    result = CliRunner().invoke(root_app, ["blueprint", "--help"], color=False)

    assert result.exit_code == 2
    assert str(unreadable_config) in result.stderr.replace("\n", "")
    assert "cannot be read" in result.stderr


@pytest.mark.parametrize(
    "args",
    [
        ["env", "show"],
        ["env", "set", "CSTAR_LOG_LEVEL", "DEBUG"],
        ["env", "unset", "CSTAR_LOG_LEVEL"],
        ["init", "--yes"],
    ],
)
def test_root_callback_lets_the_repair_commands_run(
    unreadable_config: Path, args: list[str]
) -> None:
    """Verify a configuration file the loader refuses does not stop the commands
    that manage it: each gets past the root callback (which reports the error)
    and fails, if at all, on its own terms rather than with exit code 2.

    Parameters
    ----------
    unreadable_config : Path
        The configuration file the loader refuses.
    args : list[str]
        The command to run.
    """
    result = CliRunner().invoke(root_app, args, color=False)

    assert result.exit_code != 2, result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "cannot be read" in result.stderr
    if args[:2] == ["env", "show"]:
        assert result.exit_code == 0
        assert "C-Star Environment Configuration" in result.stdout
    else:
        # a file that cannot be read is never rewritten
        assert result.exit_code == 1
        assert unreadable_config.read_bytes().startswith(b"\xff\xfe")


def test_root_callback_loads_the_configuration_file(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a subcommand run through the root app sees a setting the
    configuration file supplies.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    result = CliRunner().invoke(root_app, ["env", "show"], color=False)

    assert result.exit_code == 0, result.stderr
    assert f"{ENV_CSTAR_SLURM_ACCOUNT}: m1234 [config]" in flatten_cli_output(
        result.stdout
    )
    assert os.environ[ENV_CSTAR_SLURM_ACCOUNT] == "m1234"
