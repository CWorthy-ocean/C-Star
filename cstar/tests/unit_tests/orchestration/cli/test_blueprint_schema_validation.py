"""Verify that each blueprint C-Star writes validates against its published schema."""

import json
import typing as t
from collections.abc import Callable
from pathlib import Path

import jsonschema
import pytest
import yaml

import cstar
from cstar.applications.cdr_metrics import CdrMetricsBlueprint
from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.applications.nest_ic import NestIcBlueprint
from cstar.applications.plotter import PlotterBlueprint
from cstar.applications.ucla_roms.models import UclaRomsBlueprint
from cstar.applications.upscaler import UpscalerBlueprint
from cstar.cli.blueprint.schemas import all_applications
from cstar.orchestration.models import Blueprint
from cstar.orchestration.serialization import serialize

SCHEMA_ROOT = Path(cstar.__file__).resolve().parents[1] / "docs" / "schemas" / "bp"
"""The directory holding the published blueprint schemas."""

COMPLETE_BLUEPRINT = (
    Path(__file__).resolve().parents[3]
    / "integration_tests"
    / "blueprints"
    / "blueprint_complete.yaml"
)
"""A fully-populated UCLA ROMS blueprint."""


def _plotter(tmp_path: Path) -> Blueprint:
    return PlotterBlueprint(
        name="test-plotter",
        description="A test plotter blueprint.",
        working_dir=tmp_path / "work",
        input_dir=tmp_path / "input",
        variables=["temp"],
        time_indices=[0],
        s_indices=[0],
        grid_file_path=tmp_path / "grid.nc",
    )


def _ucla_roms(tmp_path: Path) -> Blueprint:
    data = yaml.safe_load(COMPLETE_BLUEPRINT.read_text())
    data.pop("$schema", None)
    data.pop("schema_version", None)
    return UclaRomsBlueprint.model_validate(data)


def _hello_world(tmp_path: Path) -> Blueprint:
    return HelloWorldBlueprint(
        name="test-hello-world",
        description="A test hello_world blueprint.",
        working_dir=tmp_path / "work",
        target="world",
    )


def _nest_ic(tmp_path: Path) -> Blueprint:
    return NestIcBlueprint(
        name="test-nest-ic",
        description="A test nest_ic blueprint.",
        working_dir=tmp_path,
        parent_rst=tmp_path / "parent_rst.nc",
        parent_grid=tmp_path / "parent_grid.nc",
        child_grid=tmp_path / "child_grid.nc",
    )


def _upscaler(tmp_path: Path) -> Blueprint:
    uscl_dir = tmp_path / "uscl"
    uscl_dir.mkdir()
    (uscl_dir / "roms_uscl.20240101000000.nc").touch()
    return UpscalerBlueprint(
        name="test-upscaler",
        description="A test upscaler blueprint.",
        working_dir=tmp_path / "work",
        uscl_file_location=str(uscl_dir),
    )


def _cdr_metrics(tmp_path: Path) -> Blueprint:
    segment = tmp_path / "seg-01"
    (segment / "output").mkdir(parents=True)
    (segment / "output" / "x_cdr.20120101000000.nc").touch()
    return CdrMetricsBlueprint.model_validate(
        {
            "name": "test-cdr-metrics",
            "description": "A test CDR metrics blueprint.",
            "working_dir": str(tmp_path / "work"),
            "domains": {
                "wio": {
                    "grid": {"location": str(tmp_path / "grid.nc")},
                    "sources": [{"location": str(segment)}],
                },
            },
        }
    )


FACTORIES: dict[str, Callable[[Path], Blueprint]] = {
    "plotter": _plotter,
    "ucla_roms": _ucla_roms,
    "hello_world": _hello_world,
    "nest_ic": _nest_ic,
    "upscaler": _upscaler,
    "cdr_metrics": _cdr_metrics,
}
"""Factories building a minimal valid blueprint at its current schema version."""


def _as_json(data: dict[str, t.Any]) -> dict[str, t.Any]:
    # yaml.safe_load yields datetimes for timestamps, which JSON schema sees as strings
    return t.cast(
        "dict[str, t.Any]",
        json.loads(json.dumps(data, default=lambda v: v.isoformat())),
    )


def test_every_application_has_a_factory() -> None:
    """Verify that every built-in application is covered by this module."""
    assert set(FACTORIES) == set(all_applications())


@pytest.mark.parametrize("application", all_applications())
def test_blueprint_validates_against_published_schema(
    application: str, tmp_path: Path
) -> None:
    """Verify that the published schema is the model's, and that a blueprint, as
    dumped and as serialized, matches it.
    """
    bp = FACTORIES[application](tmp_path)
    assert bp.schema_version == type(bp).model_fields["schema_version"].default

    schema_path = SCHEMA_ROOT / bp.schema_ref.split("/docs/schemas/bp/")[1]
    assert schema_path.exists(), f"no published schema at {schema_path}"
    schema = json.loads(schema_path.read_text())
    assert schema == json.loads(json.dumps(type(bp).model_json_schema())), (
        f"{schema_path.name} differs from the model's schema; bump schema_version"
    )

    dumped = bp.model_dump(mode="json")
    assert dumped["$schema"] == bp.schema_ref
    jsonschema.validate(dumped, schema)

    path = tmp_path / "bp.yaml"
    serialize(path, bp)
    written = yaml.safe_load(path.read_text())
    assert "$schema" not in written
    jsonschema.validate(_as_json(written), schema)
