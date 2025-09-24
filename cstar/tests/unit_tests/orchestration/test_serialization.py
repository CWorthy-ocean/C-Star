import typing as t
import uuid
from pathlib import Path

import pytest
from pydantic import ValidationError

from cstar.orchestration.models import Application, Workplan


def test_workplan_no_data(
    tmp_path: Path,
    load_workplan: t.Callable[[Path], Workplan],
) -> None:
    """Verify that an empty workplan yaml file fails to deserialize."""
    wp_yaml = ""
    yaml_path = tmp_path / "workplan.yaml"
    yaml_path.write_text(wp_yaml)

    with pytest.raises(ValidationError):
        _ = load_workplan(yaml_path)


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
def test_workplan_required_fields(
    tmp_path: Path,
    load_workplan: t.Callable[[Path], Workplan],
    fill_workplan_template: t.Callable[[dict[str, t.Any]], str],
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
        _ = load_workplan(yaml_path)

    assert attr_to_exclude in str(ex)


@pytest.mark.parametrize(
    "attr_to_empty",
    [
        "compute_environment",
        "runtime_vars",
    ],
)
def test_workplan_optional_fields(
    tmp_path: Path,
    load_workplan: t.Callable[[Path], Workplan],
    fill_workplan_template: t.Callable[[dict[str, t.Any]], str],
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

    workplan = load_workplan(yaml_path)

    assert workplan.name == data["name"]


def test_workplan_happy_path(
    tmp_path: Path,
    load_workplan: t.Callable[[Path], Workplan],
    fill_workplan_template: t.Callable[[dict[str, t.Any]], str],
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

    workplan = load_workplan(yaml_path)

    assert workplan.name == "Test Workplan"
    assert workplan.steps[0].name == "Test Step"
    assert not workplan.steps[0].depends_on
    assert workplan.steps[0].application == Application.SLEEP
    assert workplan.steps[0].blueprint_overrides == {}
    assert workplan.steps[0].workflow_overrides["segment_length"] in {16, 16.0}
    assert workplan.steps[0].compute_overrides["walltime"] == "00:10:00"
    assert workplan.steps[0].compute_overrides["num_nodes"] == 4


def test_workplan_compute_env(
    tmp_path: Path,
    load_workplan: t.Callable[[Path], Workplan],
    fill_workplan_template: t.Callable[[dict[str, t.Any]], str],
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

    workplan = load_workplan(yaml_path)

    compute_env = workplan.compute_environment

    assert compute_env[nodes_var] == nodes_val
    assert compute_env[cpus_var] == cpus_val


def test_workplan_runtime_vars(
    tmp_path: Path,
    load_workplan: t.Callable[[Path], Workplan],
    fill_workplan_template: t.Callable[[dict[str, t.Any]], str],
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

    workplan = load_workplan(yaml_path)

    actual = set(workplan.runtime_vars)
    expected = set(expected_vars)

    assert actual == expected
