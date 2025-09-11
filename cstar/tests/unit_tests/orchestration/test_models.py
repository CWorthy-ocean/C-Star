# ruff: noqa: S101

import json
import pathlib
import typing as t
import uuid
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

from cstar.orchestration.models import KeyValueStore, Step, Workplan, WorkplanState


def test_step_defaults(fake_blueprint_path: Path) -> None:
    """Verify attributes and default values are set as expected.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    step_name = f"test-step-{uuid.uuid4()}"
    app_name = f"test-app-{uuid.uuid4()}"
    step = Step(
        name=step_name,
        application=app_name,
        blueprint=fake_blueprint_path,
    )

    assert step.name == step_name
    assert step.application == app_name
    assert step.blueprint == fake_blueprint_path

    assert not step.depends_on
    assert "foo" not in step.depends_on  # ensure non-null

    assert not step.blueprint_overrides
    assert "foo" not in step.blueprint_overrides  # ensure non-null

    assert not step.compute_overrides
    assert "foo" not in step.compute_overrides  # ensure non-null

    assert not step.workflow_overrides
    assert "foo" not in step.workflow_overrides  # ensure non-null


@pytest.mark.parametrize(
    "invalid_value",
    [
        "",
        "   ",
        None,
    ],
)
def test_step_name_validation(
    fake_blueprint_path: Path,
    invalid_value: str | None,
) -> None:
    """Verify the name field is populated with a non-null, non-whitespace string.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    invalid_value : str
        A test value that should trigger a validation error
    """
    app_name = f"test-app-{uuid.uuid4()}"

    with pytest.raises(ValidationError) as error:
        _ = Step(
            name=invalid_value,  # type: ignore[arg-type]
            application=app_name,
            blueprint=fake_blueprint_path,
        )

    assert "name" in str(error)


@pytest.mark.parametrize(
    "invalid_value",
    [
        "",
        "   ",
        None,
        "\n",
        "  \n  ",
    ],
)
def test_step_application_validation(
    fake_blueprint_path: Path,
    invalid_value: str | None,
) -> None:
    """Verify the application field is populated with a non-null, non-whitespace string.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    invalid_value : str
        A test value that should trigger a validation error
    """
    step_name = f"test-step-{uuid.uuid4()}"

    with pytest.raises(ValidationError) as error:
        _ = Step(
            name=step_name,
            application=invalid_value,  # type: ignore[arg-type]
            blueprint=fake_blueprint_path,
        )

    assert "application" in str(error)


@pytest.mark.parametrize(
    "invalid_value",
    [
        Path("does-not-exist.yml"),
        None,
    ],
)
def test_step_path_validation(invalid_value: Path | None) -> None:
    """Verify the path field is populated with a path to a file that exists.

    Parameters
    ----------
    invalid_value : str
        A test value that should trigger a validation error
    """
    step_name = f"test-step-{uuid.uuid4()}"
    app_name = f"test-app-{uuid.uuid4()}"

    with pytest.raises(ValidationError) as error:
        _ = Step(
            name=step_name,
            application=app_name,
            blueprint=invalid_value,  # type: ignore[arg-type]
        )

    assert "blueprint" in str(error)


@pytest.mark.parametrize(
    "invalid_value",
    [
        [""],
        [" ", "bbb"],
        ["\n"],
        ["  \n  "],
    ],
)
def test_step_dependson_validation(
    fake_blueprint_path: Path,
    invalid_value: set[str],
) -> None:
    """Verify that step.depends_on does not allow invalid values.

    - confirms that empty strings are not allowed
    - confirms that whitespace is stripped

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    invalid_value: set[str]
        A test value that should trigger a validation error
    """
    with pytest.raises(ValidationError) as error:
        _ = Step(
            name="test-step",
            application="test-app",
            blueprint=fake_blueprint_path,
            depends_on=invalid_value,  # type: ignore[arg-type]
        )

    assert "depends_on" in str(error)


def test_step_dependson_set(
    fake_blueprint_path: pathlib.Path,
) -> None:
    """Verify that the Step does not allow the depends_on reference to change.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).

    """
    depends_on = ["a", "b", "c"]

    step_name = f"test-step-{uuid.uuid4()}"
    app_name = f"test-app-{uuid.uuid4()}"
    step = Step(
        name=step_name,
        application=app_name,
        blueprint=fake_blueprint_path,
        depends_on=depends_on,
    )

    new_values = ["d", "e"]
    depends_on.extend(new_values)

    # sanity check that list is distinct
    with pytest.raises(ValidationError) as error:
        step.depends_on = depends_on

    assert "depends_on" in str(error)


@pytest.mark.parametrize(
    "depends_on",
    [
        [],
        ["aaa"],
        ["aaa", "bbb"],
    ],
)
def test_step_dependson(
    fake_blueprint_path: Path,
    depends_on: list[str],
) -> None:
    """Verify that step depends_on value is stored as expected.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    depends_on: set[str]
        A set of dependencies for the step
    """
    step = Step(
        name="test-step",
        application="test-app",
        blueprint=fake_blueprint_path,
        depends_on=depends_on,
    )

    assert step.depends_on == depends_on


@pytest.mark.parametrize(
    "depends_on",
    [
        [],
        ["aaa"],
        ["aaa", "bbb"],
    ],
)
def test_step_dependson_copy(
    fake_blueprint_path: Path,
    depends_on: list[str],
) -> None:
    """Verify that step depends_on value is deep copied

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    depends_on: set[str]
        A set of dependencies for the step
    """
    step = Step(
        name="test-step",
        application="test-app",
        blueprint=fake_blueprint_path,
        depends_on=depends_on,
    )

    new_values = ["1", "2", "3"]
    og_value = set(depends_on)
    depends_on.extend(new_values)

    # sanity check that list is distinct
    attr_value = step.depends_on
    assert len(attr_value) == len(depends_on) - len(new_values)

    # confirm vars are functionally the same
    assert og_value == set(attr_value)

    # confirm post-init changes are not propagated
    assert not set(new_values).intersection(set(attr_value))


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"key1": "value1"},
        {"key1": "value1", "key2": "value2"},
        {"key1": "value1", "key2": 1},
    ],
)
def test_step_blueprint_overrides(
    fake_blueprint_path: Path,
    overrides: KeyValueStore,
) -> None:
    """Verify that step blueprint overrides value is stored as expected.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    overrides: dict[str, str | int]
        A set of overrides for the step
    """
    step = Step(
        name="test-step",
        application="test-app",
        blueprint=fake_blueprint_path,
        blueprint_overrides=overrides,
    )

    assert step.blueprint_overrides == overrides


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"key1": "value1"},
        {"key1": "value1", "key2": "value2"},
        {"key1": "value1", "key2": 1},
    ],
)
def test_step_compute_overrides(
    fake_blueprint_path: Path,
    overrides: KeyValueStore,
) -> None:
    """Verify that step compute overrides value is stored as expected.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    overrides: dict[str, str | int]
        A set of overrides for the step
    """
    step = Step(
        name="test-step",
        application="test-app",
        blueprint=fake_blueprint_path,
        compute_overrides=overrides,
    )

    assert step.compute_overrides == overrides


def test_step_compute_overrides_set(
    fake_blueprint_path: pathlib.Path,
) -> None:
    """Verify that the Step does not allow the compute overrides reference to change.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).

    """
    overrides = {"a": 1, "b": 2, "c": "xyz"}

    step_name = f"test-step-{uuid.uuid4()}"
    app_name = f"test-app-{uuid.uuid4()}"
    step = Step(
        name=step_name,
        application=app_name,
        blueprint=fake_blueprint_path,
        compute_overrides=overrides,  # type: ignore[arg-type]
    )

    new_values: KeyValueStore = {"CSTAR_XYZ": 42, "CSTAR_PQR": "xxx"}

    # sanity check that list is distinct
    with pytest.raises(ValidationError) as error:
        step.compute_overrides = new_values

    assert "compute_overrides" in str(error)


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"key1": "value1"},
        {"key1": "value1", "key2": "value2"},
        {"key1": "value1", "key2": 1},
    ],
)
def test_step_workflow_overrides(
    fake_blueprint_path: Path,
    overrides: KeyValueStore,
) -> None:
    """Verify that step workflow overrides value is stored as expected.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    overrides: dict[str, str | int]
        A set of overrides for the step
    """
    step = Step(
        name="test-step",
        application="test-app",
        blueprint=fake_blueprint_path,
        workflow_overrides=overrides,
    )

    assert step.workflow_overrides == overrides


def test_step_workflow_overrides_set(
    fake_blueprint_path: pathlib.Path,
) -> None:
    """Verify that the Step does not allow the workflow overrides reference to change.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).

    """
    overrides: KeyValueStore = {"a": 1, "b": 2, "c": "xyz"}

    step_name = f"test-step-{uuid.uuid4()}"
    app_name = f"test-app-{uuid.uuid4()}"
    step = Step(
        name=step_name,
        application=app_name,
        blueprint=fake_blueprint_path,
        workflow_overrides=overrides,
    )

    new_values: KeyValueStore = {"CSTAR_XYZ": 42, "CSTAR_PQR": "xxx"}

    # sanity check that list is distinct
    with pytest.raises(ValidationError) as error:
        step.workflow_overrides = new_values

    assert "workflow_overrides" in str(error)


@pytest.mark.parametrize(
    "dict_prop",
    [
        "blueprint_overrides",
        "compute_overrides",
        "workflow_overrides",
    ],
)
def test_step_all_overrides_copy(
    fake_blueprint_path: pathlib.Path,
    dict_prop: str,
) -> None:
    """Verify that the Step's copy of the attribute specified in `dict_prop` is distinct

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    dict_prop: str
        The name of the attribute on the Step object that should be tested.

    """
    overrides: dict[str, str | int] = {"a": 1, "b": 2, "c": "xyz"}

    step_name = f"test-step-{uuid.uuid4()}"
    app_name = f"test-app-{uuid.uuid4()}"
    step = Step(
        name=step_name,
        application=app_name,
        blueprint=fake_blueprint_path,
        **{
            dict_prop: overrides,  # type: ignore[arg-type]
        },
    )

    og_env = dict(**overrides)
    new_values: dict[str, str | int] = {"CSTAR_XYZ": 42, "CSTAR_PQR": "xxx"}

    overrides.update(new_values)

    # sanity check that list is distinct
    attr_value = getattr(step, dict_prop)
    assert len(attr_value) == len(overrides) - len(new_values)

    # confirm vars are functionally the same
    assert og_env == attr_value

    # confirm post-init changes are not propagated
    assert not set(new_values).intersection(set(attr_value))


def test_step_blueprint_overrides_set(
    fake_blueprint_path: pathlib.Path,
) -> None:
    """Verify that the Step does not allow the blueprint overrides reference to change.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).

    """
    overrides: KeyValueStore = {"a": 1, "b": 2, "c": "xyz"}

    step_name = f"test-step-{uuid.uuid4()}"
    app_name = f"test-app-{uuid.uuid4()}"
    step = Step(
        name=step_name,
        application=app_name,
        blueprint=fake_blueprint_path,
        blueprint_overrides=overrides,
    )

    new_values: KeyValueStore = {"CSTAR_XYZ": 42, "CSTAR_PQR": "xxx"}

    # sanity check that list is distinct
    with pytest.raises(ValidationError) as error:
        step.blueprint_overrides = new_values

    assert "blueprint_overrides" in str(error)


def test_workplan_defaults(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify attributes and default values are set as expected.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    """
    steps = list(gen_fake_steps(5))
    name = f"test-plan-{uuid.uuid4()}"
    description = f"test-desc-{uuid.uuid4()}"

    plan = Workplan(
        name=name,
        description=description,
        steps=steps,
        state=WorkplanState.Draft,
    )

    assert plan.name == name
    assert plan.description == description
    assert len(plan.steps) == len(steps)
    assert {step.name for step in plan.steps} == {step.name for step in steps}
    assert plan.state == WorkplanState.Draft

    assert not plan.compute_environment
    assert "foo" not in plan.compute_environment  # ensure non-null
    assert not plan.runtime_vars
    assert "foo" not in plan.compute_environment  # ensure non-null


@pytest.mark.parametrize(
    "invalid_value",
    [
        "",
        "    ",
        None,
    ],
)
def test_workplan_name_validation(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
    invalid_value: str | None,
) -> None:
    """Verify name validation works as expected.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    invalid_value : str
        A test value that should trigger a validation error
    """
    steps = list(gen_fake_steps(5))
    description = f"test-desc-{uuid.uuid4()}"

    with pytest.raises(ValidationError) as error:
        _ = Workplan(
            name=invalid_value,  # type: ignore[arg-type]
            description=description,
            steps=steps,
        )

    assert "name" in str(error)


@pytest.mark.parametrize(
    "invalid_value",
    [
        "",
        "    ",
        None,
    ],
)
def test_workplan_description_validation(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
    invalid_value: str | None,
) -> None:
    """Verify description validation works as expected.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    invalid_value : str
        A test value that should trigger a validation error
    """
    steps = list(gen_fake_steps(5))
    name = f"test-plan-{uuid.uuid4()}"

    with pytest.raises(ValidationError) as error:
        _ = Workplan(
            name=name,
            description=invalid_value,  # type: ignore[arg-type]
            steps=steps,
        )

    assert "description" in str(error)


def test_workplan_state_validation(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify description validation works as expected.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    """
    steps = list(gen_fake_steps(5))
    name = f"test-plan-{uuid.uuid4()}"
    description = f"test-desc-{uuid.uuid4()}"

    with pytest.raises(ValidationError) as error:
        _ = Workplan(
            name=name,
            description=description,
            steps=steps,
            state=f"{WorkplanState.Draft}y",  # type: ignore[arg-type]
        )

    assert "state" in str(error)


@pytest.mark.parametrize(
    "invalid_value",
    [
        [],
        None,
        [None],
    ],
)
def test_workplan_steps_validation(invalid_value: list | None) -> None:
    """Verify steps validation works as expected.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    """
    name = f"test-plan-{uuid.uuid4()}"
    description = f"test-desc-{uuid.uuid4()}"

    with pytest.raises(ValidationError) as error:
        _ = Workplan(
            name=name,
            description=description,
            steps=invalid_value,  # type: ignore[arg-type]
        )

    assert "steps" in str(error)


@pytest.mark.parametrize(
    "compute_env",
    [
        {},
        {"key1": "value1"},
        {"key1": "value1", "key2": "value2"},
        {"key1": "value1", "key2": 1},
    ],
)
def test_workplan_compute_environment(
    compute_env: KeyValueStore,
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify the compute environment of the workplan is set as expected.

    Parameters
    ----------
    overrides: dict[str, str | int]
        A set of overrides for the step
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    """
    steps = list(gen_fake_steps(5))
    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=steps,
        compute_environment=compute_env,
    )

    assert plan.compute_environment == compute_env


def test_workplan_json_serialize(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify that the model is json serializable.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    fake_steps = list(gen_fake_steps(1))
    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=fake_steps,
    )

    json = plan.model_dump_json()

    assert "name" in json
    assert "description" in json
    assert "steps" in json
    assert "state" in json
    assert "compute_environment" in json
    assert "application" in json
    assert "blueprint" in json
    assert "depends_on" in json
    assert "blueprint_overrides" in json
    assert "compute_overrides" in json
    assert "workflow_overrides" in json


def test_workplan_yaml_serialize(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
    tmp_path: pathlib.Path,
    serialize_workplan: t.Callable[[Workplan, Path], str],
) -> None:
    """Verify that the model serializes to YAML without errors.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    tmp_path : Path
        Temporarily write a yaml document to disk for manual test review of failures.

    """
    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=list(gen_fake_steps(1)),
        state=WorkplanState.Draft,
    )

    schema = Workplan.model_json_schema()
    schema_path = tmp_path / "schema.json"
    with schema_path.open("w") as fp:
        fp.write(json.dumps(schema))

    yaml_path = tmp_path / "test.yaml"
    yaml_doc = serialize_workplan(plan, yaml_path)

    assert "name" in yaml_doc
    assert "description" in yaml_doc
    assert "steps" in yaml_doc
    assert "state" in yaml_doc
    assert "compute_environment" not in yaml_doc
    assert "application" in yaml_doc
    assert "blueprint" in yaml_doc
    assert "depends_on" not in yaml_doc
    assert "blueprint_overrides" not in yaml_doc
    assert "compute_overrides" not in yaml_doc
    assert "workflow_overrides" not in yaml_doc


def test_workplan_yaml_deserialize(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
    tmp_path: pathlib.Path,
    serialize_workplan: t.Callable[[BaseModel, Path], str],
    deserialize_model: t.Callable[[Path, type], BaseModel],
) -> None:
    """Verify that the model deserializes from YAML without errors.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    tmp_path : Path
        Temporarily write a yaml document to disk to ensure deserialization.

    """
    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=list(gen_fake_steps(1)),
    )

    yaml_path = tmp_path / "test.yaml"
    _ = serialize_workplan(plan, yaml_path)

    plan2 = t.cast("Workplan", deserialize_model(yaml_path, Workplan))

    assert plan == plan2


def test_workplan_step_copy(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan deep-copies steps.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    steps = list(gen_fake_steps(2))
    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=steps,
    )

    step_names = {s.name for s in steps}

    new_name = "changed-name"
    steps[0].name = new_name

    new_steps = list(gen_fake_steps(1))
    steps.extend(new_steps)

    # sanity check that list is copied
    assert len(plan.steps) == len(steps) - len(new_steps)

    # confirm steps are functionally the same
    assert set(step_names) == {s.name for s in plan.steps}

    # confirm post-init changes are not propagated
    assert new_name not in {s.name for s in plan.steps}


def test_workplan_step_set(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan does not allow the steps list reference to change.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    steps = list(gen_fake_steps(2))
    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=list(gen_fake_steps(1)),
    )

    with pytest.raises(ValidationError) as error:
        plan.steps = steps

    assert "steps" in str(error)


def test_workplan_runtimevars_copy(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan copy of runtime_vars is distinct

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    runtime_vars = ["a", "b", "c"]

    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=list(gen_fake_steps(2)),
        runtime_vars=runtime_vars,
    )

    og_vars = set(runtime_vars)
    new_vars = ["d", "e"]

    runtime_vars.extend(new_vars)

    # sanity check that list is distinct
    assert len(plan.runtime_vars) == len(runtime_vars) - len(new_vars)

    # confirm vars are functionally the same
    assert og_vars == set(plan.runtime_vars)

    # confirm post-init changes are not propagated
    assert not set(new_vars).intersection(set(plan.runtime_vars))


def test_workplan_runtimevars_set(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan does not allow the runtime_vars list reference to change.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    runtime_vars = ["a", "b", "c"]

    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=list(gen_fake_steps(2)),
    )

    with pytest.raises(ValidationError) as error:
        plan.runtime_vars = runtime_vars

    assert "runtime_vars" in str(error)


def test_workplan_runtimevars_duplicates(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan identifies duplicate runtime_vars

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    runtime_vars = ["a", "a", "c"]

    with pytest.raises(ValidationError) as error:
        _ = Workplan(
            name="test-plan",
            description="test-description",
            steps=list(gen_fake_steps(2)),
            runtime_vars=runtime_vars,
        )

    assert "runtime_vars" in str(error)


def test_workplan_computeenv_copy(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan copy of compute_environment is distinct

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    compute_env: KeyValueStore = {"a": 1, "b": 2, "c": "xyz"}

    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=list(gen_fake_steps(2)),
        compute_environment=compute_env,
    )

    og_env = dict(**compute_env)
    new_values: KeyValueStore = {"CSTAR_XYZ": 42, "CSTAR_PQR": "xxx"}

    compute_env.update(new_values)

    # sanity check that list is distinct
    assert len(plan.compute_environment) == len(compute_env) - len(new_values)

    # confirm vars are functionally the same
    assert og_env == plan.compute_environment

    # confirm post-init changes are not propagated
    assert not set(new_values).intersection(set(plan.compute_environment))


def test_workplan_computeenv_set(
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan does not allow the compute env reference to change.

    Parameters
    ----------
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    compute_env: KeyValueStore = {"a": 1, "b": 2, "c": "xyz"}

    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=list(gen_fake_steps(2)),
        compute_environment=compute_env,
    )

    new_values: KeyValueStore = {"CSTAR_XYZ": 42, "CSTAR_PQR": "xxx"}
    compute_env.update(new_values)

    with pytest.raises(ValidationError) as error:
        plan.compute_environment = compute_env

    assert "compute_environment" in str(error)
