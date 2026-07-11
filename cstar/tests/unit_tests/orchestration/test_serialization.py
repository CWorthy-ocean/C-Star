import textwrap
import typing as t
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest
from pydantic import BaseModel, Field, ValidationError

from cstar.applications.plotter_app import PlotterBlueprint
from cstar.orchestration.launch.slurm import SlurmHandle
from cstar.orchestration.models import Application, Workplan
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import (
    PersistenceMode,
    deserialize,
    read_json_to_raw,
    read_raw,
    read_yaml_to_raw,
    serialize,
)
from cstar.orchestration.state import StateRepository


def test_serialization_json_aliased_fields(tmp_path: Path) -> None:
    """Verify that an aliased field is serialized and deserialized."""
    exp_value = "foo"

    class FakeModel(BaseModel):
        value: str = Field(alias="the_value")

    model = FakeModel(the_value=exp_value)
    assert model.value == exp_value

    serialize_to = tmp_path / "fake.json"

    serialize(serialize_to, model, mode=PersistenceMode.auto)
    reloaded = deserialize(serialize_to, FakeModel)

    assert reloaded.value == model.value


def test_serialization_workplan_no_data(
    tmp_path: Path,
) -> None:
    """Verify that an empty workplan yaml file fails to deserialize."""
    wp_yaml = ""
    yaml_path = tmp_path / "workplan.yaml"
    yaml_path.write_text(wp_yaml)

    with pytest.raises(ValidationError):
        _ = deserialize(yaml_path, Workplan)


@pytest.mark.parametrize(
    "attr_to_exclude",
    [
        "name",
        "description",
        "state",
        pytest.param(
            "steps",
            marks=pytest.mark.skip(
                reason="step template population not implemented in test fixture",
            ),
        ),
    ],
)
def test_serialization_workplan_required_fields(
    tmp_path: Path,
    fill_workplan_template: Callable[[dict[str, t.Any]], str],
    attr_to_exclude: str,
    complete_workplan_template_input: dict[str, t.Any],
    empty_workplan_template_input: dict[str, t.Any],
) -> None:
    """Verify the required fields are checked during workplan deserialization."""
    data = complete_workplan_template_input

    # remove the variable under test by replacing with an empty value
    data[attr_to_exclude] = empty_workplan_template_input[attr_to_exclude]

    for i, step in enumerate(data.get("steps", [])):
        empty_bp_path = tmp_path / f"blueprint-{i:00}.yaml"
        empty_bp_path.touch()
        step["blueprint"] = empty_bp_path.as_posix()

    wp_yaml = fill_workplan_template(data)
    yaml_path = tmp_path / "workplan.yaml"
    yaml_path.write_text(wp_yaml)

    with pytest.raises(ValidationError) as ex:
        _ = deserialize(yaml_path, Workplan)

    assert attr_to_exclude in str(ex)


@pytest.mark.parametrize(
    "attr_to_empty",
    [
        "compute_environment",
        "runtime_vars",
    ],
)
def test_serialization_workplan_optional_fields(
    tmp_path: Path,
    fill_workplan_template: Callable[[dict[str, t.Any]], str],
    attr_to_empty: str,
    complete_workplan_template_input: dict[str, t.Any],
    empty_workplan_template_input: dict[str, t.Any],
) -> None:
    """Verify that missing optional fields do not break deserialization."""
    data = complete_workplan_template_input

    # remove the variable under test by replacing with an empty value
    data[attr_to_empty] = empty_workplan_template_input[attr_to_empty]

    for i, step in enumerate(data.get("steps", [])):
        empty_bp_path = tmp_path / f"blueprint-{i:00}.yaml"
        empty_bp_path.touch()
        step["blueprint"] = empty_bp_path.as_posix()

    wp_yaml = fill_workplan_template(data)
    yaml_path = tmp_path / "workplan.yaml"
    yaml_path.write_text(wp_yaml)

    workplan = deserialize(yaml_path, Workplan)

    assert workplan.name == data["name"]


def test_serialization_workplan_happy_path(
    tmp_path: Path,
    fill_workplan_template: Callable[[dict[str, t.Any]], str],
    complete_workplan_template_input: dict[str, t.Any],
) -> None:
    """Verify that a fully-populated workplan can be deserialized."""
    data = complete_workplan_template_input

    for i, step in enumerate(data.get("steps", [])):
        empty_bp_path = tmp_path / f"blueprint-{i:00}.yaml"
        empty_bp_path.touch()
        step["blueprint"] = empty_bp_path.as_posix()

    wp_yaml = fill_workplan_template(data)
    yaml_path = tmp_path / "workplan.yaml"
    yaml_path.write_text(wp_yaml)

    workplan = deserialize(yaml_path, Workplan)

    assert workplan.name == "Test Workplan"
    assert workplan.steps[0].name == "Test Step"
    assert not workplan.steps[0].depends_on
    assert workplan.steps[0].application == Application.SLEEP
    assert workplan.steps[0].blueprint_overrides == {}
    assert workplan.steps[0].workflow_overrides["segment_length"] in {16, 16.0}
    assert workplan.steps[0].compute_overrides["walltime"] == "00:10:00"
    assert workplan.steps[0].compute_overrides["num_nodes"] == 4


def test_serialization_polymorphic_workplan(
    tmp_path: Path,
    wp_templates_dir: Path,
    bp_templates_dir: Path,
    default_blueprint_path: str,
) -> None:
    """Verify that a workplan serialized with steps of a subclass of `Step` results
    in deserialization to the correct subclass/type.
    """
    template_file = "workplan.yaml"
    template_path = wp_templates_dir / template_file

    bp_path = tmp_path / "blueprint.yaml"
    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    bp_path.write_text(bp_tpl_path.read_text())

    wp_content = template_path.read_text()
    wp_content = wp_content.replace(default_blueprint_path, bp_path.as_posix())

    wp_path = tmp_path / template_file
    wp_path.write_text(wp_content)

    wp = deserialize(wp_path, Workplan)

    # convert steps to live-steps and store on a workplan
    all_steps = [LiveStep.from_step(s) for s in wp.steps]

    poly_path = tmp_path / "live_workplan.yaml"
    poly_wp = Workplan(**wp.model_dump(exclude={"steps"}), steps=all_steps)

    assert serialize(poly_path, poly_wp)

    # confirm that the subclass LiveStep was serialized
    wp_content = poly_path.read_text()
    print(wp_content)
    assert "working_dir" in wp_content


def test_serialization_workplan_compute_env(
    tmp_path: Path,
    fill_workplan_template: Callable[[dict[str, t.Any]], str],
    complete_workplan_template_input: dict[str, t.Any],
) -> None:
    """Verify that dynamically added compute environment attributes are parsed."""
    data = complete_workplan_template_input

    nodes_var = "num_nodes"
    nodes_val = 4

    cpus_var = "num_cpus_per_process"
    cpus_val = 16

    data["compute_environment"] = {
        nodes_var: nodes_val,
        cpus_var: cpus_val,
    }

    for i, step in enumerate(data.get("steps", [])):
        empty_bp_path = tmp_path / f"blueprint-{i:00}.yaml"
        empty_bp_path.touch()
        step["blueprint"] = empty_bp_path.as_posix()

    wp_yaml = fill_workplan_template(data)
    yaml_path = tmp_path / "workplan.yaml"
    yaml_path.write_text(wp_yaml)

    workplan = deserialize(yaml_path, Workplan)

    compute_env = workplan.compute_environment

    assert compute_env[nodes_var] == nodes_val
    assert compute_env[cpus_var] == cpus_val


def test_serialization_workplan_runtime_vars(
    tmp_path: Path,
    fill_workplan_template: Callable[[dict[str, t.Any]], str],
    complete_workplan_template_input: dict[str, t.Any],
) -> None:
    """Verify that dynamically added runtime vars are parsed."""
    data = complete_workplan_template_input

    var0, var1, var2 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    expected_vars = [var0, var1, var2]

    data["runtime_vars"] = expected_vars

    for i, step in enumerate(data.get("steps", [])):
        empty_bp_path = tmp_path / f"blueprint-{i:00}.yaml"
        empty_bp_path.touch()
        step["blueprint"] = empty_bp_path.as_posix()

    wp_yaml = fill_workplan_template(data)
    yaml_path = tmp_path / "workplan.yaml"
    yaml_path.write_text(wp_yaml)

    workplan = deserialize(yaml_path, Workplan)

    actual = set(workplan.runtime_vars)
    expected = set(expected_vars)

    assert actual == expected


def test_serialization_workplan_steps_with_directives(
    tmp_path: Path,
    preprocessable_workplan_path: Path,
) -> None:
    """Verify that steps containing directives are serialized correctly.

    Parameters
    ----------
    tmp_path : Path
        Used to write some temporary workplans to disk
    preprocessable_workplan_path : Path
        Path to a workplan that contains directives.
    """
    write_to = preprocessable_workplan_path

    # load the new workplan
    wp = deserialize(write_to, Workplan)

    step = wp.steps[-1]

    # confirm the directive has been loaded
    directives = t.cast("dict[str, dict[str, str]]", step.directives)
    assert directives
    assert "continue-from" in directives
    assert "path" in directives["continue-from"]

    # confirm the directives can be written
    serialize_to = tmp_path / "reserialized.yaml"
    assert serialize(serialize_to, wp)

    # confirm the serialization step wrote the directives
    wp2 = deserialize(serialize_to, Workplan)
    assert wp2.steps[-1].directives == wp.steps[-1].directives


@pytest.mark.parametrize(
    "mode",
    [
        PersistenceMode.json,
        PersistenceMode.yaml,
    ],
)
def test_serialization_handle(mode: PersistenceMode) -> None:
    """Verify that a task can be properly serialized to disk and reloaded."""
    pid, job_name, run_id = "test-pid", "abc123", "fake-run-id"
    handle = SlurmHandle(pid=pid, name=job_name, run_id=run_id)

    persist_as = StateRepository.sentinel_path(handle, mode=mode)

    # confirm the parametrized serialization mode is supported
    nbytes = serialize(
        persist_as,
        handle,
        mode,
    )
    assert nbytes > 0

    print(f"Task persisted to: {persist_as}")
    print(persist_as.read_text(encoding="utf-8"))

    # confirm the output from `serialize` is valid and can be deserialized
    dhandle: SlurmHandle = deserialize(
        persist_as,
        SlurmHandle,
        mode,
    )

    assert dhandle.name == handle.name
    assert dhandle.pid == handle.pid
    assert dhandle.run_id == run_id


def test_serializaton_json_schema(plotter_v2_0_0_bp: Path, tmp_path: Path) -> None:
    """Verify a json-serialized file contains a schema reference."""
    target = tmp_path / "output.json"

    bp = deserialize(plotter_v2_0_0_bp, PlotterBlueprint)

    assert serialize(target, bp, mode=PersistenceMode.auto)

    content = target.read_text()
    assert '"$schema":"http' in content
    assert f"{bp.application}_schema.{bp.schema_version}.json" in content


def test_serializaton_yaml_schema(plotter_v2_0_0_bp: Path, tmp_path: Path) -> None:
    """Verify a YAML-serialized file contains a schema reference."""
    target = tmp_path / "output.yaml"

    bp = deserialize(plotter_v2_0_0_bp, PlotterBlueprint)

    assert serialize(target, bp, mode=PersistenceMode.auto)

    content = target.read_text()
    assert "# yaml-language-server: $schema=" in content
    assert f"{bp.application}_schema.{bp.schema_version}.json" in content


@pytest.fixture
def person_yaml(tmp_path: Path) -> Path:
    document = textwrap.dedent("""\
        person:
          name: unit test
          age: 42
          address:
            street: "101 main st"
            city: anywhere
            state: ND
          pets:
          - Waffles
          - Grits
    """)
    document_path = tmp_path / "doc.yaml"
    document_path.write_text(document)
    return document_path


@pytest.fixture
def person_yml(person_yaml: Path) -> Path:
    name = person_yaml.with_suffix(".yml")
    return person_yaml.rename(name)


@pytest.fixture
def person_json(tmp_path: Path) -> Path:
    document = textwrap.dedent("""\
        {
            "person": {
                "name": "unit test",
                "address": {
                    "street": "101 main st",
                    "city": "anywhere",
                    "state": "ND"
                },
                "age": 42,
                "pets": [
                    "Waffles",
                    "Grits"
                ]
            }
        }
    """)
    document_path = tmp_path / "doc.json"
    document_path.write_text(document)
    return document_path


@pytest.mark.parametrize(
    "source_fixture",
    ["person_yaml", "person_yml"],
)
def test_read_yaml_to_raw(request: pytest.FixtureRequest, source_fixture: str) -> None:
    """Verify yaml parsing results in the expected dictionary."""
    person_doc = request.getfixturevalue(source_fixture)
    d = read_yaml_to_raw(person_doc)

    person = d.get("person", {})
    assert person
    assert person["name"] == "unit test"
    assert person["age"] == 42

    address = person.get("address", {})
    assert address
    assert address["street"] == "101 main st"

    pets = person.get("pets", [])
    assert "Waffles" in pets
    assert "Grits" in pets


def test_read_json_to_raw(person_json: Path) -> None:
    """Verify json parsing results in the expected dictionary."""
    d = read_json_to_raw(person_json)

    person = d.get("person", {})
    assert person
    assert person["name"] == "unit test"
    assert person["age"] == 42

    address = person.get("address", {})
    assert address
    assert address["street"] == "101 main st"

    pets = person.get("pets", [])
    assert "Waffles" in pets
    assert "Grits" in pets


@pytest.mark.parametrize(
    ("source_fixture", "mode"),
    [
        pytest.param("person_yaml", PersistenceMode.yaml, id="mode:correct:yaml"),
        pytest.param("person_yaml", PersistenceMode.auto, id="mode:auto:yaml"),
        pytest.param("person_yaml", PersistenceMode.json, id="mode:fallback:yaml"),
        pytest.param("person_yml", PersistenceMode.yaml, id="mode:correct:yml"),
        pytest.param("person_yml", PersistenceMode.auto, id="mode:auto:yml"),
        pytest.param("person_yml", PersistenceMode.json, id="mode:fallback:yml"),
        pytest.param("person_json", PersistenceMode.json, id="mode:correct:json"),
        pytest.param("person_json", PersistenceMode.auto, id="mode:auto:json"),
        pytest.param("person_json", PersistenceMode.yaml, id="mode:fallback:json"),
    ],
)
def test_read_raw(
    request: pytest.FixtureRequest, source_fixture: str, mode: PersistenceMode
) -> None:
    """Verify the automatic read mode selects the right reader and
    failures are recovered by falling back to the alternate reader.
    """
    path = request.getfixturevalue(source_fixture)
    d = read_raw(path, mode=mode)

    person = d.get("person", {})
    assert person
    assert person["name"] == "unit test"
    assert person["age"] == 42

    address = person.get("address", {})
    assert address
    assert address["street"] == "101 main st"

    pets = person.get("pets", [])
    assert "Waffles" in pets
    assert "Grits" in pets


def test_read_raw_no_auto(
    person_yaml: Path,
    person_json: Path,
) -> None:
    """Verify that auto-mode does not take precedence over supplied mode value.

    This test renames the file(s) so auto-mode would fail.
    """
    renamed0 = person_yaml.rename(person_yaml.with_suffix(".stuff"))
    d0 = read_raw(renamed0, mode=PersistenceMode.yaml)

    renamed1 = person_json.rename(person_json.with_suffix(".other"))
    d1 = read_raw(renamed1, mode=PersistenceMode.json)

    for d in [d0, d1]:
        person = d.get("person", {})
        assert person
        assert person["name"] == "unit test"
        assert person["age"] == 42

        address = person.get("address", {})
        assert address
        assert address["street"] == "101 main st"

        pets = person.get("pets", [])
        assert "Waffles" in pets
        assert "Grits" in pets
