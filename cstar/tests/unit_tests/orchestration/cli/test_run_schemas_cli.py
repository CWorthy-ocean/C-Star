"""Tests for the `cstar run schemas` command and the shared schema writer."""

import json
from pathlib import Path

import typer
from typer.testing import CliRunner

from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.cli.blueprint.schemas import generate_schema
from cstar.cli.common import write_model_schema
from cstar.cli.run.schemas import CMD_NAME
from cstar.cli.run.schemas import app as app_schemas
from cstar.orchestration.manifest import RunManifest, StepRecord

# a sub-app with one command is invoked without its name; a group needs a parent
app_run = typer.Typer()
app_run.add_typer(app_schemas)


def test_run_schemas_writes_both_schemas(tmp_path: Path) -> None:
    """Verify both schemas are written, versioned, as JSON schema documents."""
    output = tmp_path / "out" / "run"

    result = CliRunner().invoke(app_run, [CMD_NAME, output.as_posix()], color=False)

    assert result.exit_code == 0, result.output
    expected = {
        f"run_manifest_schema.{RunManifest.model_fields['schema_version'].default}.json",
        f"step_record_schema.{StepRecord.model_fields['schema_version'].default}.json",
    }
    assert {p.name for p in output.iterdir()} == expected
    for name in expected:
        assert name in " ".join(result.output.split())
        schema = json.loads((output / name).read_text())
        assert "properties" in schema


def test_write_model_schema_creates_the_directory(tmp_path: Path) -> None:
    """Verify the writer makes missing parents and ends the file with a newline."""
    path = tmp_path / "a" / "b" / "schema.json"

    assert write_model_schema(RunManifest, path) == path
    assert path.read_text().endswith("}\n")


def test_generate_schema_keeps_its_layout(tmp_path: Path) -> None:
    """Verify the blueprint schema is still written under its application's directory."""
    path = generate_schema(HelloWorldBlueprint, tmp_path)

    assert path.parent == tmp_path / "hello_world"
    assert path.name.startswith("hello_world_schema.")
    assert "properties" in json.loads(path.read_text())
