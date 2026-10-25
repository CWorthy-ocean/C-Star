import json
import pathlib
import typing as t
import uuid
from collections.abc import Callable, Generator, Sequence
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.base.env import ENV_CSTAR_DATA_HOME
from cstar.execution.file_system import StateDirectoryManager
from cstar.orchestration.models import (
    KEY_PRE_RUN,
    KEY_RESUME,
    BlueprintCore,
    DeferredBlueprintRef,
    InlineBlueprintRef,
    KeyValueStore,
    PathLocation,
    RunRef,
    Step,
    StepRef,
    Workplan,
    WorkplanState,
)
from cstar.orchestration.orchestration import (
    LiveStep,
    LiveWorkplan,
    is_transformed_workplan,
    read_workplan,
)
from cstar.orchestration.serialization import deserialize, model_to_yaml, serialize
from cstar.system.migration import (
    TOLERATE_UNKNOWN,
    CstarSchemaTooNewError,
    for_writing,
)

CURRENT_WORKPLAN = Workplan.model_fields["schema_version"].default
"""The schema version this build writes in a workplan."""


def _unwritable(model: object) -> bool:
    """Whether a writer refuses the model: it holds keys a newer file set."""
    try:
        for_writing(model)
    except CstarSchemaTooNewError:
        return True
    return False


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
    assert step.blueprint_path == fake_blueprint_path

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
        pytest.param("all", id="reserved keyword"),
        pytest.param(" all ", id="reserved keyword w/ws"),
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


@pytest.mark.parametrize("authored", ["roms_marbl", "  roms_marbl  "])
def test_step_application_loads_earlier_name_as_current(
    fake_blueprint_path: Path,
    authored: str,
) -> None:
    """Verify a step authored with an earlier application name carries the current one."""
    step = Step(
        name="run",
        application=authored,
        blueprint=fake_blueprint_path,
    )

    assert step.application == "ucla_roms"


def test_blueprint_core_loads_earlier_application_name_as_current() -> None:
    """Verify metadata-only loading of a blueprint reports the current application name."""
    core = BlueprintCore.model_validate({"name": "bp", "application": "roms_marbl"})

    assert core.application == "ucla_roms"


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
        {"slurm": {"num_cpus": 16}},
        {"local": {"max_walltime": "00:10:00"}},
        {"slurm": {"num_cpus": 16}, "local": {"max_walltime": "00:10:00"}},
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


@pytest.mark.parametrize(
    ("overrides", "bad_keys"),
    [
        pytest.param({"cpus": 10}, "['cpus']", id="unnested key"),
        pytest.param({"walltime": "00:10:00"}, "['walltime']", id="stale flat key"),
        pytest.param(
            {"slurm": {"num_cpus": 16}, "cpus": 10, "sluurm": {}},
            "['cpus', 'sluurm']",
            id="all bad keys reported",
        ),
    ],
)
def test_step_compute_overrides_unknown_namespace(
    fake_blueprint_path: Path,
    overrides: KeyValueStore,
    bad_keys: str,
) -> None:
    """Verify that compute overrides not nested under a known launcher namespace are
    rejected (they would otherwise be silently ignored at submission), and that all
    offending keys are reported in one error.
    """
    with pytest.raises(ValidationError) as error:
        Step(
            name="test-step",
            application="test-app",
            blueprint=fake_blueprint_path,
            compute_overrides=overrides,
        )

    assert f"unknown compute_overrides key(s) {bad_keys}" in str(error.value)


def test_step_compute_overrides_set(
    fake_blueprint_path: pathlib.Path,
) -> None:
    """Verify that the Step does not allow the compute overrides reference to change.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).

    """
    overrides: KeyValueStore = {"slurm": {"a": 1, "b": 2, "c": "xyz"}}

    step_name = f"test-step-{uuid.uuid4()}"
    app_name = f"test-app-{uuid.uuid4()}"
    step = Step(
        name=step_name,
        application=app_name,
        blueprint=fake_blueprint_path,
        compute_overrides=overrides,
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
    # compute_overrides only permits launcher-namespace keys
    overrides: dict[str, t.Any] = (
        {"slurm": {"a": 1, "b": 2, "c": "xyz"}}
        if dict_prop == "compute_overrides"
        else {"a": 1, "b": 2, "c": "xyz"}
    )

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


def test_step_directives(
    tmp_path: Path,
    fake_blueprint_path: pathlib.Path,
) -> None:
    """Verify that a `LiveBlueprint` can be instantiated from the Blueprint
    model and dynamic directives specified on a workplan `Step`.

    Parameters
    ----------
    tmp_path: Path
        A temporary directory for storing testing outputs.
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).

    """
    directive_name = "continue-from"
    directive_kwargs = {"path", "extra"}
    directives: KeyValueStore = {
        directive_name: {
            "path": tmp_path.as_posix(),
            "extra": 12345,
        },
    }

    step_name = f"test-step-{uuid.uuid4()}"
    app_name = f"test-app-{uuid.uuid4()}"
    step = Step(
        name=step_name,
        application=app_name,
        blueprint=fake_blueprint_path,
        directives=directives,
    )

    # confirm directives are available
    assert directive_name in step.directives
    # confirm attributes exist
    directive_params = t.cast("dict[str, t.Any]", step.directives[directive_name])
    assert directive_kwargs.issubset(directive_params.keys())


def test_workplan_defaults(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify attributes and default values are set as expected.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    ("version", "expected"),
    [
        (None, "1.2.0"),
        ("1.0.0", "1.0.0"),
        ("1.1.0", "1.1.0"),
        ("1.2.0", "1.2.0"),
    ],
)
def test_workplan_reads_its_version_and_older_ones(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    version: str | None,
    expected: str,
) -> None:
    """Verify a workplan without a `schema_version` (written before workplans
    were versioned) and one of an older version load unchanged, and that each
    is written at the version this build writes, as a new workplan is.
    """
    data: dict[str, t.Any] = {
        "name": "plan",
        "description": "d",
        "steps": list(gen_fake_steps(1)),
    }
    if version is not None:
        data["schema_version"] = version

    plan = Workplan.model_validate(data)

    assert plan.schema_version == expected
    assert f"schema_version: {CURRENT_WORKPLAN}" in model_to_yaml(plan)


def _workplan_data(
    steps: Sequence[Step], version: str, **extra: t.Any
) -> dict[str, t.Any]:
    """Describe a workplan document of `version` as a reader would see it."""
    return {
        "schema_version": version,
        "name": "plan",
        "description": "d",
        "steps": [step.model_dump(by_alias=True) for step in steps],
        **extra,
    }


@pytest.mark.parametrize("klass", [Workplan, LiveWorkplan])
@pytest.mark.parametrize("version", ["1.2.1", "1.3.0", "1.12.0"])
def test_workplan_reads_a_newer_version_without_new_keys(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    klass: type[Workplan],
    version: str,
) -> None:
    """Verify a workplan a newer minor or patch release wrote loads when it holds
    nothing this build does not know, keeps its version, and is written at it.
    """
    data = _workplan_data(list(gen_fake_steps(1)), version)

    plan = klass.model_validate(data)

    assert plan.schema_version == version
    assert plan.unknown_keys == ()
    assert not _unwritable(plan)
    assert f"schema_version: {version}" in model_to_yaml(plan)


@pytest.mark.filterwarnings("error::UserWarning")
def test_a_newer_workplan_and_blueprint_rebuild_through_their_constructors(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify a newer-minor document that loaded can be rebuilt with `Cls(**dump)`,
    as the transformer and the override transform do: under `__init__`, the
    validator must return the instance pydantic builds.
    """
    plan = Workplan.model_validate(_workplan_data(list(gen_fake_steps(1)), "1.3.0"))
    current = HelloWorldBlueprint.model_fields["schema_version"].default
    major, minor, _ = current.split(".")
    blueprint = HelloWorldBlueprint.model_validate(
        {
            "name": "b",
            "description": "d",
            "target": "t",
            "schema_version": f"{major}.{int(minor) + 1}.0",
        }
    )

    rebuilt_plan = Workplan(**plan.model_dump(by_alias=True))
    rebuilt_blueprint = HelloWorldBlueprint(**blueprint.model_dump(exclude={"$schema"}))

    assert rebuilt_plan == plan
    assert rebuilt_blueprint == blueprint


@pytest.mark.parametrize("klass", [Workplan, LiveWorkplan])
@pytest.mark.parametrize("version", ["1.2.1", "1.3.0"])
def test_workplan_refuses_a_newer_version_with_a_new_key(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    klass: type[Workplan],
    version: str,
) -> None:
    """Verify a workplan a newer release wrote with a key this build does not
    know is refused with the upgrade message naming the key, so the key does not
    surface as a raw validation error or drop silently.
    """
    data = _workplan_data(list(gen_fake_steps(1)), version, a_key_from_the_future=True)

    with pytest.raises(CstarSchemaTooNewError, match="Upgrade cstar-ocean") as caught:
        klass.model_validate(data)

    assert caught.value.found == version
    assert caught.value.target == CURRENT_WORKPLAN
    assert caught.value.unknown == ("a_key_from_the_future",)
    assert "workplan schema" in str(caught.value)
    assert "sets a_key_from_the_future" in str(caught.value)


def test_workplan_refuses_a_newer_version_with_a_new_step_key(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify the refusal reaches the steps: a key this build does not know in a
    step is named by its location.
    """
    data = _workplan_data(list(gen_fake_steps(1)), "1.3.0")
    data["steps"][0]["a_step_key_from_the_future"] = 1

    with pytest.raises(CstarSchemaTooNewError) as caught:
        Workplan.model_validate(data)

    assert caught.value.unknown == ("steps.0.a_step_key_from_the_future",)


@pytest.mark.parametrize("klass", [Workplan, LiveWorkplan])
@pytest.mark.parametrize("extra", [{}, {"a_key_from_the_future": True}])
def test_workplan_refuses_a_newer_major_version(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    klass: type[Workplan],
    extra: dict[str, t.Any],
) -> None:
    """Verify a workplan of a newer major is refused with the upgrade message,
    whether or not it holds anything this build does not know.
    """
    data = _workplan_data(list(gen_fake_steps(1)), "2.0.0", **extra)

    with pytest.raises(CstarSchemaTooNewError, match="Upgrade cstar-ocean") as caught:
        klass.model_validate(data)

    assert caught.value.found == "2.0.0"
    assert caught.value.target == CURRENT_WORKPLAN
    assert caught.value.unknown == ()


@pytest.mark.parametrize("klass", [Workplan, LiveWorkplan])
def test_workplan_tolerates_unknown_keys_when_asked(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    klass: type[Workplan],
) -> None:
    """Verify a tolerant reader loads a newer workplan whose only problems are
    unknown keys, names them, and that the workplan then refuses to be written.
    """
    data = _workplan_data(list(gen_fake_steps(1)), "1.3.0", a_key_from_the_future=True)

    plan = klass.model_validate(data, context={TOLERATE_UNKNOWN: True})

    assert plan.schema_version == "1.3.0"
    assert plan.unknown_keys == ("a_key_from_the_future",)
    assert _unwritable(plan)
    with pytest.raises(CstarSchemaTooNewError, match="to write this file"):
        model_to_yaml(plan)


def test_workplan_tolerance_does_not_cover_other_problems(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify a tolerant reader still refuses a newer workplan that is invalid
    for a reason besides unknown keys.
    """
    data = _workplan_data(list(gen_fake_steps(1)), "1.3.0", a_key_from_the_future=True)
    del data["name"]

    with pytest.raises(CstarSchemaTooNewError, match="name"):
        Workplan.model_validate(data, context={TOLERATE_UNKNOWN: True})


def test_workplan_rejects_a_malformed_version(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify a `schema_version` that is not dotted integers is a validation error."""
    data = {
        "schema_version": "latest",
        "name": "plan",
        "description": "d",
        "steps": list(gen_fake_steps(1)),
    }

    with pytest.raises(ValidationError, match="expected dotted integers"):
        Workplan.model_validate(data)


def test_workplan_templates_carry_the_current_version(wp_templates_dir: Path) -> None:
    """Verify each bundled workplan template states the current version and
    points editors at that version's published schema.
    """
    current = Workplan.model_fields["schema_version"].default
    templates = sorted(wp_templates_dir.glob("*.yaml"))
    assert templates

    for path in templates:
        header = path.read_text().splitlines()[0]
        assert header.endswith(f"/docs/schemas/wp/workplan_schema.{current}.json")
        assert yaml.safe_load(path.read_text())["schema_version"] == current
        assert (
            Path(__file__).parents[4]
            / "docs"
            / "schemas"
            / "wp"
            / header.rsplit("/", 1)[1]
        ).exists()


@pytest.mark.parametrize(
    "invalid_value",
    [
        "",
        "    ",
        None,
    ],
)
def test_workplan_name_validation(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    invalid_value: str | None,
) -> None:
    """Verify name validation works as expected.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    ("name", "slug"),
    [
        ("Sample Workplan", "sample-workplan"),
        ("Fan-out Workplan", "fan-out-workplan"),
        ("UPPER", "upper"),
        ("two  spaces", "two-spaces"),
        ("trailing-", "trailing"),
    ],
)
def test_workplan_name_must_be_a_slug(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    name: str,
    slug: str,
) -> None:
    """Verify a name that is not its own slug is rejected with the slug to use.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    name : str
        A workplan name that differs from its slug
    slug : str
        The slug the error should suggest
    """
    with pytest.raises(ValidationError) as error:
        _ = Workplan(name=name, description="d", steps=list(gen_fake_steps(1)))

    message = str(error.value)
    assert f"workplan name {name!r} is not a valid run-id" in message
    assert f"use {slug!r}" in message


@pytest.mark.parametrize("name", ["sample-workplan", "my_plan", "run-2026", "a"])
def test_workplan_name_slug_accepted(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    name: str,
) -> None:
    """Verify a name that is already a slug is accepted unchanged.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    name : str
        A workplan name that equals its slug
    """
    wp = Workplan(name=name, description="d", steps=list(gen_fake_steps(1)))

    assert wp.name == name


@pytest.mark.parametrize(
    ("persisted", "expected"),
    [
        ("my-plan", "my-plan"),
        ("my-plan (transformed)", "my-plan"),
        ("My Plan (transformed)", "my-plan"),
    ],
)
def test_live_workplan_normalizes_legacy_name(
    tmp_path: Path,
    fake_blueprint_path: Path,
    persisted: str,
    expected: str,
) -> None:
    """Verify a transformed workplan loads as a `LiveWorkplan` under the run-id
    its name produced, also when an earlier release renamed it to
    `<name> (transformed)` or persisted a non-slug name.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to persist the workplan in
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    persisted : str
        The name as written into the transformed workplan
    expected : str
        The name the loaded workplan carries
    """
    step = LiveStep.from_step(
        Step(name="a", application="hello_world", blueprint=fake_blueprint_path)
    )
    path = tmp_path / "trx.yaml"
    assert serialize(path, LiveWorkplan(name="x", description="d", steps=[step])), (
        "Serialization of the transformed workplan failed"
    )
    document = yaml.safe_load(path.read_text()) | {"name": persisted}
    path.write_text(yaml.safe_dump(document))

    live = read_workplan(path)

    assert isinstance(live, LiveWorkplan)
    assert live.name == expected


@pytest.mark.parametrize(
    ("persisted", "complaint"),
    [
        ("Fan-out Workplan", "not a valid run-id"),
        ("計画 (transformed)", "empty"),
    ],
)
def test_live_workplan_refuses_a_name_that_is_not_a_run_id(
    tmp_path: Path,
    fake_blueprint_path: Path,
    persisted: str,
    complaint: str,
) -> None:
    """Verify a transformed workplan whose name is not a run-id is refused: a
    non-slug name without the legacy suffix is not normalized, and a legacy
    name with nothing to slug reports that rather than a slug of the suffix.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to persist the workplan in
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    persisted : str
        The name as written into the transformed workplan
    complaint : str
        A fragment of the expected error
    """
    step = LiveStep.from_step(
        Step(name="a", application="hello_world", blueprint=fake_blueprint_path)
    )
    path = tmp_path / "trx.yaml"
    assert serialize(path, LiveWorkplan(name="x", description="d", steps=[step]))
    document = yaml.safe_load(path.read_text()) | {"name": persisted}
    path.write_text(yaml.safe_dump(document, allow_unicode=True))

    with pytest.raises(ValidationError, match=complaint):
        read_workplan(path)


def test_is_transformed_workplan_requires_a_working_dir_on_every_step(
    tmp_path: Path,
    fake_blueprint_path: Path,
) -> None:
    """Verify only a file whose every step carries a `working_dir` counts as the
    transformed form: an authored workplan does not, nor does one with a stray
    `working_dir` on a single step, nor an unreadable file.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to persist the workplans in
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    steps = [
        Step(name=n, application="hello_world", blueprint=fake_blueprint_path)
        for n in ("a", "b")
    ]
    authored = tmp_path / "wp.yaml"
    assert serialize(authored, Workplan(name="my-plan", description="d", steps=steps))
    assert not is_transformed_workplan(authored)

    stray = tmp_path / "stray.yaml"
    document = yaml.safe_load(authored.read_text())
    document["steps"][0]["working_dir"] = (tmp_path / "somewhere").as_posix()
    stray.write_text(yaml.safe_dump(document))
    assert not is_transformed_workplan(stray)

    transformed = tmp_path / "trx.yaml"
    live = LiveWorkplan(
        name="my-plan", description="d", steps=[LiveStep.from_step(s) for s in steps]
    )
    assert serialize(transformed, live)
    assert is_transformed_workplan(transformed)

    assert not is_transformed_workplan(tmp_path / "missing.yaml")
    garbage = tmp_path / "garbage.yaml"
    garbage.write_text("- just\n- a list\n")
    assert not is_transformed_workplan(garbage)


def test_read_workplan_reads_an_authored_workplan_as_a_workplan(
    tmp_path: Path,
    fake_blueprint_path: Path,
) -> None:
    """Verify an authored workplan, which carries no step `working_dir`, reads as
    a plain `Workplan` and a non-slug name is still refused.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to persist the workplan in
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    step = Step(name="a", application="hello_world", blueprint=fake_blueprint_path)
    path = tmp_path / "wp.yaml"
    assert serialize(path, Workplan(name="my-plan", description="d", steps=[step]))

    assert type(read_workplan(path)) is Workplan

    document = yaml.safe_load(path.read_text()) | {"name": "My Plan"}
    path.write_text(yaml.safe_dump(document))
    with pytest.raises(ValidationError, match="not a valid run-id"):
        read_workplan(path)


@pytest.mark.parametrize(
    "invalid_value",
    [
        "",
        "    ",
        None,
    ],
)
def test_workplan_description_validation(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    invalid_value: str | None,
) -> None:
    """Verify description validation works as expected.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify description validation works as expected.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
def test_workplan_steps_validation(invalid_value: list[str | None] | None) -> None:
    """Verify steps validation works as expected.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    "step_names",
    [
        pytest.param(["S1", "S1"], id="duped name"),
        pytest.param(["S1", "s1"], id="case sensitivity"),
        pytest.param(["S1", " S1"], id="whitespaced name (pre)"),
        pytest.param(["S1", "S1 "], id="whitespaced name (post)"),
        pytest.param(["S1", "S2", "S1 "], id="non-consecutive"),
        pytest.param(["S1", "S1 ", " s1 "], id="many copies"),
        pytest.param(["S1", "S2", "s1 ", " s2"], id="multiple collisions"),
    ],
)
def test_workplan_steps_uniqueness_validation(
    step_names: list[str],
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify step names are unique.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
        A generator function to produce minimally valid test steps
    """
    name = f"test-plan-{uuid.uuid4()}"
    description = f"test-desc-{uuid.uuid4()}"

    steps = list(gen_fake_steps(len(step_names)))
    for i in range(len(step_names)):
        steps[i].name = step_names[i]

    with pytest.raises(ValidationError) as error:
        _ = Workplan(
            name=name,
            description=description,
            steps=steps,
        )

    assert "Step names must be unique" in str(error)


@pytest.mark.parametrize(
    "compute_env",
    [
        {},
        {"launcher": "local"},
        {"launcher": "slurm", "system": "anvil"},
        {"slurm": {"account_name": "x-ees250129", "max_walltime": "04:00:00"}},
        {
            "launcher": "slurm",
            "system": "anvil",
            "slurm": {"queue_name": "wholenode", "num_nodes": 2},
        },
    ],
)
def test_workplan_compute_environment(
    compute_env: KeyValueStore,
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify the compute environment of the workplan is set as expected.

    Parameters
    ----------
    overrides: dict[str, str | int]
        A set of overrides for the step
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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


@pytest.mark.parametrize(
    ("compute_env", "expected"),
    [
        ({"key1": "value1"}, "key1"),
        ({"launcher": "pbs"}, "pbs"),
        ({"slurm": {"max_walltime": "soon"}}, "max_walltime"),
        ({"launcher": "pbs", "bogus": 1}, "bogus"),
    ],
)
def test_workplan_compute_environment_invalid(
    compute_env: KeyValueStore,
    expected: str,
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify an invalid compute environment is rejected, naming the problem."""
    with pytest.raises(ValidationError, match=expected):
        Workplan(
            name="test-plan",
            description="test-description",
            steps=list(gen_fake_steps(2)),
            compute_environment=compute_env,
        )


def test_workplan_json_serialize(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify that the model is json serializable.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    tmp_path: pathlib.Path,
) -> None:
    """Verify that the model serializes to YAML without errors.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    nbytes = serialize(yaml_path, plan)
    assert nbytes > 0

    yaml_doc = yaml_path.read_text()

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
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
    tmp_path: pathlib.Path,
) -> None:
    """Verify that the model deserializes from YAML without errors.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    _ = serialize(yaml_path, plan)

    plan2 = deserialize(yaml_path, Workplan)

    for step in plan.steps:
        step.blueprint_path = str(step.blueprint_path)

    assert plan == plan2


def test_workplan_step_copy(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan deep-copies steps.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan does not allow the steps list reference to change.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan copy of runtime_vars is distinct

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan does not allow the runtime_vars list reference to change.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan identifies duplicate runtime_vars

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
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
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan copy of compute_environment is distinct

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    compute_env: KeyValueStore = {"launcher": "slurm", "system": "xyz"}

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
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify that the Workplan does not allow the compute env reference to change.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    compute_env: KeyValueStore = {"launcher": "slurm", "system": "xyz"}

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


def test_liveworkplan_step_lookup(
    gen_fake_steps: Callable[[int], Generator[Step, None, None]],
) -> None:
    """Verify __contains__ and __getitem__ work as expected.

    Parameters
    ----------
    gen_fake_steps : Callable[[int], Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    steps = list(gen_fake_steps(3))
    step_names = {s.name for s in steps}
    wp = Workplan(
        name="test-plan",
        description="test-description",
        steps=steps,
    )

    live_steps = [LiveStep.from_step(s) for s in steps]
    live_plan = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)

    for s in step_names:
        # confirm __contains__
        assert s in live_plan
        # confirm __getitem__
        assert live_plan[s].name == s


def test_deferred_blueprint_ref_parsing(fake_blueprint_path: Path) -> None:
    """Verify a mapping in the blueprint field parses to a `DeferredBlueprintRef`
    and other value types behave as before.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    deferred = Step.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "producer", "filename": "generated.yaml"},
            "depends_on": ["producer"],
        },
    )
    assert deferred.is_deferred
    assert isinstance(deferred.blueprint_path, DeferredBlueprintRef)
    assert deferred.blueprint_path.from_step == "producer"
    assert deferred.blueprint_path.filename == "generated.yaml"

    concrete = Step(
        name="concrete",
        application="hello_world",
        blueprint=fake_blueprint_path,
    )
    assert not concrete.is_deferred
    assert isinstance(concrete.blueprint_path, Path)

    missing = Step(
        name="missing",
        application="hello_world",
        blueprint="does-not-exist.yaml",
    )
    assert not missing.is_deferred
    assert isinstance(missing.blueprint_path, str)


@pytest.mark.parametrize(
    ("from_step", "filename"),
    [
        ("producer", ""),
        ("producer", "generated.yaml"),
        ("step with spaces", "file with spaces.yaml"),
        ("step/with/slashes", ""),
    ],
)
def test_deferred_blueprint_ref_uri_roundtrip(
    from_step: str,
    filename: str,
) -> None:
    """Verify the `step://` wire form of a deferred reference is reversible.

    Parameters
    ----------
    from_step : str
        The name of the producing step.
    filename : str
        An optional file name within the producing step's output directory.
    """
    ref = DeferredBlueprintRef(from_step=from_step, filename=filename)
    uri = str(ref)

    assert DeferredBlueprintRef.matches(uri)

    parsed = DeferredBlueprintRef.from_uri(uri)
    assert parsed.from_step == from_step
    assert parsed.filename == filename


def test_deferred_blueprint_ref_matches_case_and_whitespace() -> None:
    """Verify scheme matching tolerates case variations and stray whitespace."""
    uri = "  STEP://producer/generated.yaml "

    assert DeferredBlueprintRef.matches(uri)

    parsed = DeferredBlueprintRef.from_uri(uri)
    assert parsed.from_step == "producer"
    assert parsed.filename == "generated.yaml"


def test_deferred_blueprint_ref_from_uri_rejects_other_schemes() -> None:
    """Verify parsing a non-deferred URI raises an error."""
    with pytest.raises(ValueError, match="Not a deferred blueprint URI"):
        _ = DeferredBlueprintRef.from_uri("https://example.com/blueprint.yaml")


def test_workplan_deferred_unknown_producer(fake_blueprint_path: Path) -> None:
    """Verify a deferred blueprint referencing a step that is not in the
    workplan is rejected.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    producer = Step(
        name="producer",
        application="hello_world",
        blueprint=fake_blueprint_path,
    )
    consumer = Step.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "no-such-step"},
            "depends_on": ["producer"],
        },
    )

    with pytest.raises(ValidationError) as error:
        _ = Workplan(
            name="test-plan",
            description="test-description",
            steps=[producer, consumer],
        )

    assert "unknown" in str(error.value)
    assert "no-such-step" in str(error.value)


def test_workplan_deferred_producer_not_dependency(
    fake_blueprint_path: Path,
) -> None:
    """Verify a deferred blueprint whose producer is not listed in the step's
    `depends_on` is rejected.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    producer = Step(
        name="producer",
        application="hello_world",
        blueprint=fake_blueprint_path,
    )
    consumer = Step.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "producer"},
        },
    )

    with pytest.raises(ValidationError) as error:
        _ = Workplan(
            name="test-plan",
            description="test-description",
            steps=[producer, consumer],
        )

    assert "depends_on" in str(error.value)


def test_workplan_dependency_cycle_two_steps_rejected(
    fake_blueprint_path: Path,
) -> None:
    """Verify a direct A <-> B dependency cycle is rejected, naming both steps.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    a = Step(
        name="A",
        application="hello_world",
        blueprint=fake_blueprint_path,
        depends_on=["B"],
    )
    b = Step(
        name="B",
        application="hello_world",
        blueprint=fake_blueprint_path,
        depends_on=["A"],
    )

    with pytest.raises(ValidationError) as error:
        _ = Workplan(
            name="test-plan",
            description="test-description",
            steps=[a, b],
        )

    assert "Dependency cycle" in str(error.value)
    assert "A" in str(error.value)
    assert "B" in str(error.value)


def test_workplan_dependency_cycle_three_steps_rejected(
    fake_blueprint_path: Path,
) -> None:
    """Verify a transitive A -> B -> C -> A dependency cycle is rejected.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    a = Step(
        name="A",
        application="hello_world",
        blueprint=fake_blueprint_path,
        depends_on=["C"],
    )
    b = Step(
        name="B",
        application="hello_world",
        blueprint=fake_blueprint_path,
        depends_on=["A"],
    )
    c = Step(
        name="C",
        application="hello_world",
        blueprint=fake_blueprint_path,
        depends_on=["B"],
    )

    with pytest.raises(ValidationError) as error:
        _ = Workplan(
            name="test-plan",
            description="test-description",
            steps=[a, b, c],
        )

    assert "Dependency cycle" in str(error.value)
    assert "A" in str(error.value)
    assert "B" in str(error.value)
    assert "C" in str(error.value)


def test_workplan_dependency_diamond_accepted(
    fake_blueprint_path: Path,
) -> None:
    """Verify a diamond dependency shape (no cycle) is accepted.

    A <- B, A <- C, {B, C} <- D: two independent paths reconverge on D
    without ever looping back to an ancestor.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    a = Step(name="A", application="hello_world", blueprint=fake_blueprint_path)
    b = Step(
        name="B",
        application="hello_world",
        blueprint=fake_blueprint_path,
        depends_on=["A"],
    )
    c = Step(
        name="C",
        application="hello_world",
        blueprint=fake_blueprint_path,
        depends_on=["A"],
    )
    d = Step(
        name="D",
        application="hello_world",
        blueprint=fake_blueprint_path,
        depends_on=["B", "C"],
    )

    wp = Workplan(
        name="test-plan",
        description="test-description",
        steps=[a, b, c, d],
    )

    assert [step.name for step in wp.steps] == ["A", "B", "C", "D"]


def test_workplan_deferred_roundtrip(
    tmp_path: Path,
    fake_blueprint_path: Path,
) -> None:
    """Verify a workplan with a deferred blueprint serializes and deserializes
    without losing the reference type.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    producer = Step(
        name="producer",
        application="hello_world",
        blueprint=fake_blueprint_path,
    )
    consumer = Step.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "producer", "filename": "generated.yaml"},
            "depends_on": ["producer"],
        },
    )
    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=[producer, consumer],
    )

    wp_path = tmp_path / "deferred_wp.yaml"
    assert serialize(wp_path, plan)

    reloaded = deserialize(wp_path, Workplan)
    reloaded_consumer = next(s for s in reloaded.steps if s.name == "consumer")

    assert reloaded_consumer.is_deferred
    assert isinstance(reloaded_consumer.blueprint_path, DeferredBlueprintRef)
    assert reloaded_consumer.blueprint_path.from_step == "producer"
    assert reloaded_consumer.blueprint_path.filename == "generated.yaml"


@pytest.mark.parametrize("token", ["inline", " Inline ", "INLINE"])
def test_inline_blueprint_ref_parsing(token: str) -> None:
    """Verify the `inline` token parses to an `InlineBlueprintRef`.

    Parameters
    ----------
    token : str
        A spelling of the inline token, varying in case and whitespace.
    """
    step = Step.model_validate(
        {"name": "inline-step", "application": "hello_world", "blueprint": token},
    )

    assert step.is_inline
    assert not step.is_deferred
    assert isinstance(step.blueprint_path, InlineBlueprintRef)


@pytest.mark.parametrize(
    "fields",
    [
        {"blueprint": {}},
        {"blueprint": None},
        {},
    ],
    ids=["empty-mapping", "null", "omitted"],
)
def test_inline_blueprint_ref_rejects_other_forms(fields: dict[str, t.Any]) -> None:
    """Verify only the token declares an inline blueprint and the field stays
    required.

    Parameters
    ----------
    fields : dict[str, Any]
        The `blueprint` entry (if any) to include in the step.
    """
    with pytest.raises(ValidationError):
        _ = Step.model_validate(
            {"name": "inline-step", "application": "hello_world", **fields},
        )


def test_inline_blueprint_ref_not_inferred_from_other_blueprints(
    fake_blueprint_path: Path,
) -> None:
    """Verify path and deferred steps, and a file named `./inline`, are not inline.

    Parameters
    ----------
    fake_blueprint_path : Path
        A path to a file that meets minimum expectations (it exists).
    """
    concrete = Step(
        name="concrete",
        application="hello_world",
        blueprint=fake_blueprint_path,
    )
    deferred = Step.model_validate(
        {
            "name": "deferred",
            "application": "hello_world",
            "blueprint": {"from_step": "producer"},
            "depends_on": ["producer"],
        },
    )
    literal = Step(name="literal", application="hello_world", blueprint="./inline")

    assert not concrete.is_inline
    assert not deferred.is_inline
    assert not literal.is_inline
    assert literal.blueprint_path == "./inline"
    assert isinstance(literal.blueprint_path, str)


def test_inline_blueprint_ref_dump_roundtrip() -> None:
    """Verify a dumped inline step reads `inline` and validates back to inline."""
    step = Step.model_validate(
        {"name": "inline-step", "application": "hello_world", "blueprint": "inline"},
    )

    dumped = step.model_dump(by_alias=True)
    assert dumped["blueprint"] == "inline"

    assert Step.model_validate(dumped).is_inline


def test_inline_blueprint_ref_constructs() -> None:
    """Verify the marker constructs directly and is accepted by a step."""
    step = Step(
        name="inline-step",
        application="hello_world",
        blueprint=InlineBlueprintRef(),
    )

    assert step.is_inline
    assert isinstance(step.blueprint_path, InlineBlueprintRef)


def test_inline_blueprint_ref_str() -> None:
    """Verify the string form of the reference is the token."""
    assert str(InlineBlueprintRef()) == "inline"
    assert InlineBlueprintRef.matches("  Inline ")
    assert not InlineBlueprintRef.matches("./inline")


def test_inline_blueprint_ref_json_schema() -> None:
    """Verify the step JSON schema advertises the token and keeps `blueprint`
    required.
    """
    schema = Step.model_json_schema()

    assert "blueprint" in schema["required"]
    refs = [
        entry["$ref"].rsplit("/", 1)[-1]
        for entry in schema["properties"]["blueprint"]["anyOf"]
        if "$ref" in entry
    ]
    assert any(
        schema["$defs"][name].get("const") == InlineBlueprintRef.TOKEN for name in refs
    )


def test_workplan_inline_roundtrip(tmp_path: Path) -> None:
    """Verify a workplan with an inline blueprint serializes as `blueprint: inline`
    and deserializes without losing the marker.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    inline = Step.model_validate(
        {"name": "inline-step", "application": "hello_world", "blueprint": "inline"},
    )
    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=[inline],
    )

    wp_path = tmp_path / "inline_wp.yaml"
    assert serialize(wp_path, plan)
    assert "blueprint: inline" in wp_path.read_text()

    reloaded = deserialize(wp_path, Workplan)

    assert reloaded.steps[0].is_inline
    assert isinstance(reloaded.steps[0].blueprint_path, InlineBlueprintRef)


@pytest.fixture
def hello_kwargs() -> dict[str, str]:
    """Required fields for a minimal blueprint, named so slugification matters."""
    return {"name": "My Hello Run", "description": "A test", "target": "world"}


def test_blueprint_working_dir_default(hello_kwargs: dict[str, str]) -> None:
    """Verify a blueprint declares no working directory unless told to."""
    bp = HelloWorldBlueprint(**hello_kwargs)

    assert bp.working_dir is None


def test_blueprint_effective_working_dir_default(
    hello_kwargs: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify an omitted working directory resolves to the blueprint run directory."""
    data_home = tmp_path / "data-home"
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, str(data_home))
    bp = HelloWorldBlueprint(**hello_kwargs)

    actual = bp.effective_working_dir

    assert actual.is_relative_to(data_home)
    assert actual == StateDirectoryManager.blueprint_run_dir(bp.application, bp.name)


def test_blueprint_effective_working_dir_relative(
    hello_kwargs: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify an explicit relative working directory is resolved and honored."""
    monkeypatch.chdir(tmp_path)
    bp = HelloWorldBlueprint(**hello_kwargs, working_dir=Path("some/dir"))

    assert bp.working_dir == tmp_path / "some" / "dir"
    assert bp.effective_working_dir == bp.working_dir


def test_blueprint_effective_working_dir_user_home(
    hello_kwargs: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify an explicit working directory beginning with ``~`` is expanded."""
    monkeypatch.setenv("HOME", str(tmp_path))
    bp = HelloWorldBlueprint(**hello_kwargs, working_dir=Path("~/some/dir"))

    assert bp.working_dir == tmp_path / "some" / "dir"
    assert bp.effective_working_dir == bp.working_dir


def test_blueprint_yaml_omits_unset_working_dir(hello_kwargs: dict[str, str]) -> None:
    """Verify a blueprint without a working directory serializes without the key."""
    content = model_to_yaml(HelloWorldBlueprint(**hello_kwargs))

    assert "working_dir" not in content


def test_blueprint_yaml_includes_set_working_dir(
    hello_kwargs: dict[str, str], tmp_path: Path
) -> None:
    """Verify a declared working directory survives serialization."""
    bp = HelloWorldBlueprint(**hello_kwargs, working_dir=tmp_path)

    content = model_to_yaml(bp)

    assert f"working_dir: {tmp_path}" in content


def _pre_run_step(blueprint_path: Path, name: str, pre_run: bool) -> Step:
    """Build a step whose `pre_run` override is set or unset."""
    return Step(
        name=name,
        application="hello_world",
        blueprint=blueprint_path,
        workflow_overrides={KEY_PRE_RUN: True} if pre_run else {},
    )


@pytest.mark.parametrize("pre_run", [True, False])
def test_step_pre_run_reads_workflow_override(
    fake_blueprint_path: Path, pre_run: bool
) -> None:
    """Verify `Step.pre_run` reflects the override and is off by default."""
    step = _pre_run_step(fake_blueprint_path, "step", pre_run)

    assert step.pre_run is pre_run


def test_step_rejects_pre_run_with_resume(fake_blueprint_path: Path) -> None:
    """Verify a step cannot be both prepared without launching and resumed."""
    with pytest.raises(ValidationError, match="mutually exclusive"):
        Step(
            name="step",
            application="hello_world",
            blueprint=fake_blueprint_path,
            workflow_overrides={KEY_PRE_RUN: True, KEY_RESUME: True},
        )


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        pytest.param([True, True], True, id="all"),
        pytest.param([True, False], False, id="some"),
        pytest.param([False, False], False, id="none"),
    ],
)
def test_workplan_pre_run_requires_every_step(
    fake_blueprint_path: Path, flags: list[bool], expected: bool
) -> None:
    """Verify `Workplan.pre_run` is true only when every step is a pre-run step."""
    steps = [
        _pre_run_step(fake_blueprint_path, f"step-{i}", flag)
        for i, flag in enumerate(flags)
    ]
    plan = Workplan(name="test-plan", description="test-description", steps=steps)

    assert plan.pre_run is expected


@pytest.mark.parametrize(
    ("token", "step", "run"),
    [
        ("outer", "outer", ""),
        ("outer@spinup", "outer", "spinup"),
        ("  outer@spinup  ", "outer", "spinup"),
        ("my step@my-run", "my step", "my-run"),
    ],
)
def test_step_ref_parse(token: str, step: str, run: str) -> None:
    """Verify tokens parse to local or external references and round-trip."""
    ref = StepRef.parse(token)

    assert ref.step == step
    assert ref.run == run
    assert ref.is_external is bool(run)
    assert str(ref) == token.strip()
    assert StepRef.parse(str(ref)) == ref


@pytest.mark.parametrize("token", ["", "@spinup", "outer@", "a@b@c", "@", "  @  "])
def test_step_ref_parse_rejects_malformed(token: str) -> None:
    """Verify malformed tokens are rejected with the token in the message."""
    with pytest.raises(ValueError, match="Invalid step reference") as error:
        _ = StepRef.parse(token)

    assert repr(token) in str(error.value)


def test_run_ref_bare_string_coerces() -> None:
    """Verify a bare string is shorthand for a mapping with `run_id`."""
    ref = RunRef.model_validate("my-run-id")

    assert ref.run_id == "my-run-id"
    assert ref.start_at is None


def test_run_ref_mapping_and_placeholder() -> None:
    """Verify the mapping form (and a placeholder run-id) is accepted."""
    ref = RunRef.model_validate({"run_id": "{{spinup_run}}"})

    assert ref.run_id == "{{spinup_run}}"
    assert ref == RunRef.model_validate("{{spinup_run}}")


def test_run_ref_pin_fields_default_empty_and_are_accepted() -> None:
    """Verify a reference is authored without `id` and `path`, which a scheduled
    workplan fills.
    """
    authored = RunRef.model_validate("my-run-id")
    pinned = RunRef.model_validate(
        {"run_id": "my-run-id", "id": "0b9d6a3e", "path": "/runs/my-run-id"}
    )

    assert (authored.id, authored.path) == ("", "")
    assert (pinned.id, pinned.path) == ("0b9d6a3e", "/runs/my-run-id")


@pytest.mark.parametrize(
    "reference",
    [
        "my-run-id",
        "run://0b9d6a3e-1111-4222-8333-444455556666",
        "./runs/spin",
        "../runs/spin",
        "~/runs/spin",
        "/abs/runs/spin",
        "{{where}}/spin",
    ],
)
def test_run_ref_accepts_every_way_of_naming_a_run(
    fake_blueprint_path: Path, reference: str
) -> None:
    """Verify a workplan accepts a run named by run-id, `run://<uuid>` or
    directory, as a bare string or as a mapping.
    """
    wp = _external_workplan(
        fake_blueprint_path,
        runs={"bare": reference, "mapped": {"run_id": reference}},
    )

    assert wp.runs["bare"].run_id == wp.runs["mapped"].run_id == reference


def test_run_ref_rejects_extra_and_empty() -> None:
    """Verify unknown keys and an empty run-id are rejected."""
    with pytest.raises(ValidationError):
        _ = RunRef.model_validate({"run_id": "x", "bogus": 1})
    with pytest.raises(ValidationError):
        _ = RunRef.model_validate("")


def _external_workplan(
    blueprint: Path,
    *,
    runs: dict[str, t.Any] | None = None,
    depends_on: list[str] | None = None,
    consumer_blueprint: t.Any = None,
    extra_steps: list[Step] | None = None,
) -> Workplan:
    """Build a workplan whose `child` step may reference external steps."""
    child = Step.model_validate(
        {
            "name": "child",
            "application": "hello_world",
            "blueprint": consumer_blueprint or str(blueprint),
            "depends_on": depends_on or [],
        },
    )
    return Workplan(
        name="test-plan",
        description="test-description",
        runs=runs or {},
        steps=[child, *(extra_steps or [])],
    )


def test_workplan_runs_default_empty(fake_blueprint_path: Path) -> None:
    """Verify `runs` defaults to empty."""
    assert _external_workplan(fake_blueprint_path).runs == {}


def test_workplan_runs_valid_aliases(fake_blueprint_path: Path) -> None:
    """Verify aliases are accepted and bare strings coerced."""
    wp = _external_workplan(
        fake_blueprint_path,
        runs={"spinup": "run-1", "prior_2": {"run_id": "{{prior}}"}},
    )

    assert wp.runs["spinup"].run_id == "run-1"
    assert wp.runs["prior_2"].run_id == "{{prior}}"


def test_workplan_runs_bad_aliases_reported_together(
    fake_blueprint_path: Path,
) -> None:
    """Verify every bad alias is reported in one error."""
    with pytest.raises(ValidationError) as error:
        _ = _external_workplan(
            fake_blueprint_path,
            runs={"a@b": "r1", "has space": "r2", " ": "r3", "ok": "r4"},
        )

    (detail,) = error.value.errors()
    message = detail["msg"]
    assert "'a@b'" in message
    assert "'has space'" in message
    assert "''" in message
    assert "'ok'" not in message


def test_step_name_rejects_separator(fake_blueprint_path: Path) -> None:
    """Verify a step name may not contain the run-alias separator."""
    with pytest.raises(ValidationError):
        _ = Step(
            name="outer@spinup",
            application="hello_world",
            blueprint=fake_blueprint_path,
        )


def test_workplan_external_dependency_accepted(fake_blueprint_path: Path) -> None:
    """Verify an external dependency needs a declared alias but no local step."""
    wp = _external_workplan(
        fake_blueprint_path,
        runs={"spinup": "run-1"},
        depends_on=["outer@spinup"],
    )

    assert wp.steps[0].depends_on == ["outer@spinup"]


def test_workplan_external_dependency_undeclared_alias(
    fake_blueprint_path: Path,
) -> None:
    """Verify an external dependency with an undeclared alias is rejected."""
    with pytest.raises(ValidationError) as error:
        _ = _external_workplan(
            fake_blueprint_path,
            runs={"other": "run-1"},
            depends_on=["outer@spinup", "inner@spinup"],
        )

    message = str(error.value)
    assert "undeclared run alias" in message
    assert "['spinup']" in message
    assert "['other']" in message


def test_workplan_local_unknown_dependency_wording(fake_blueprint_path: Path) -> None:
    """Verify a local unknown dependency keeps its existing error."""
    with pytest.raises(ValidationError, match="Unknown dependency specified"):
        _ = _external_workplan(fake_blueprint_path, depends_on=["nope"])


def test_workplan_malformed_dependency_reported(fake_blueprint_path: Path) -> None:
    """Verify a malformed dependency token names the step and entry."""
    with pytest.raises(ValidationError) as error:
        _ = _external_workplan(
            fake_blueprint_path,
            runs={"spinup": "run-1"},
            depends_on=["outer@@spinup"],
        )

    message = str(error.value)
    assert "'child'" in message
    assert "outer@@spinup" in message


def test_workplan_cycle_detection_ignores_external(
    fake_blueprint_path: Path,
) -> None:
    """Verify an external dependency neither hides nor creates a cycle."""
    a = Step.model_validate(
        {
            "name": "a",
            "application": "hello_world",
            "blueprint": str(fake_blueprint_path),
            "depends_on": ["b", "a@spinup"],
        },
    )
    b = Step.model_validate(
        {
            "name": "b",
            "application": "hello_world",
            "blueprint": str(fake_blueprint_path),
            "depends_on": ["a"],
        },
    )

    with pytest.raises(ValidationError, match="Dependency cycle detected"):
        _ = Workplan(
            name="test-plan",
            description="test-description",
            runs={"spinup": RunRef(run_id="run-1")},
            steps=[a, b],
        )

    # a step sharing a name with an external step is not a cycle
    ok = _external_workplan(
        fake_blueprint_path,
        runs={"spinup": "run-1"},
        depends_on=["child@spinup"],
    )
    assert ok.steps[0].depends_on == ["child@spinup"]


def test_workplan_deferred_external_producer_accepted(
    fake_blueprint_path: Path,
) -> None:
    """Verify a deferred blueprint may come from an external run's step."""
    wp = _external_workplan(
        fake_blueprint_path,
        runs={"spinup": "run-1"},
        depends_on=["outer@spinup"],
        consumer_blueprint={"from_step": "outer@spinup"},
    )

    blueprint = wp.steps[0].blueprint_path
    assert isinstance(blueprint, DeferredBlueprintRef)
    assert blueprint.from_step == "outer@spinup"


def test_workplan_deferred_external_producer_undeclared_alias(
    fake_blueprint_path: Path,
) -> None:
    """Verify a deferred external producer needs a declared alias."""
    with pytest.raises(ValidationError) as error:
        _ = _external_workplan(
            fake_blueprint_path,
            runs={"spinup": "run-1"},
            depends_on=["outer@spinup"],
            consumer_blueprint={"from_step": "outer@missing"},
        )

    assert "missing" in str(error.value)
    assert "not declared" in str(error.value)


def test_workplan_deferred_external_producer_not_dependency(
    fake_blueprint_path: Path,
) -> None:
    """Verify a deferred external producer must be listed in `depends_on`."""
    with pytest.raises(ValidationError) as error:
        _ = _external_workplan(
            fake_blueprint_path,
            runs={"spinup": "run-1"},
            consumer_blueprint={"from_step": "outer@spinup"},
        )

    assert "depends_on" in str(error.value)
    assert "outer@spinup" in str(error.value)


def test_step_depends_on_is_canonical(fake_blueprint_path: Path) -> None:
    """Verify whitespace around the `@` of an external reference is normalized,
    so every consumer compares the same token.
    """
    wp = _external_workplan(
        fake_blueprint_path,
        runs={"spinup": "run-1"},
        depends_on=["build @ spinup"],
    )

    assert wp.steps[0].depends_on == ["build@spinup"]


def test_deferred_blueprint_from_step_is_canonical_and_matches_dependency(
    fake_blueprint_path: Path,
) -> None:
    """Verify a deferred blueprint written with spaces around the `@` is accepted
    against a dependency written without them.
    """
    wp = _external_workplan(
        fake_blueprint_path,
        runs={"spinup": "run-1"},
        depends_on=["build@spinup"],
        consumer_blueprint={"from_step": "build @ spinup"},
    )

    blueprint = wp.steps[0].blueprint_path
    assert isinstance(blueprint, DeferredBlueprintRef)
    assert blueprint.from_step == "build@spinup"


def test_path_location_expands_user() -> None:
    """Verify `~` in a location is expanded and nothing else is touched: a
    placeholder or a missing relative path is kept as given.
    """
    assert PathLocation(location=Path("~/x")).location == Path("~/x").expanduser()
    assert PathLocation(location="{{output_dir: run}}/output").location == Path(
        "{{output_dir: run}}/output"
    )
    assert PathLocation(location="absent/grid.nc").location == Path("absent/grid.nc")


def test_path_location_rejects_extra_keys() -> None:
    """Verify a mistyped key beside `location` is an error."""
    with pytest.raises(ValidationError, match="extra"):
        PathLocation.model_validate({"location": "grid.nc", "path": "grid.nc"})
