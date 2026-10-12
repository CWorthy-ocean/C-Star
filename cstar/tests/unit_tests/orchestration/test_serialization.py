import asyncio
import errno
import shutil
import textwrap
import threading
import time
import typing as t
import uuid
from collections.abc import Callable
from pathlib import Path
from unittest import mock

import pytest
from pydantic import BaseModel, Field, ValidationError

from cstar.applications.plotter import PlotterBlueprint
from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.launch.slurm import SlurmHandle
from cstar.orchestration.models import Application, Workplan
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.serialization import (
    PersistenceMode,
    _mode_detect,
    anchor_relative_path,
    deserialize,
    deserialize_all,
    load_blueprint,
    read_blueprint_raw,
    read_document,
    read_json_to_raw,
    read_raw,
    read_yaml_to_raw,
    rebase_paths,
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


def test_serialization_yaml_unregistered_enums(tmp_path: Path) -> None:
    """Verify enums without a registered representer serialize by value.

    External applications declare their own enums, which must round-trip
    through yaml without a class-specific representer (e.g. the blueprint
    override files written at workplan-prep time).
    """
    import enum

    class Mode(str, enum.Enum):  # noqa: UP042 -- the str-mixin form is the case under test
        never = "never"

    class Level(enum.IntEnum):
        high = 2

    class FakeModel(BaseModel):
        mode: Mode
        level: Level

    serialize_to = tmp_path / "fake.yaml"
    serialize(serialize_to, FakeModel(mode=Mode.never, level=Level.high))

    assert "python/object" not in serialize_to.read_text()
    reloaded = deserialize(serialize_to, FakeModel)
    assert reloaded.mode == Mode.never
    assert reloaded.level == Level.high


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

    assert workplan.name == "test-workplan"
    assert workplan.steps[0].name == "Test Step"
    assert not workplan.steps[0].depends_on
    assert workplan.steps[0].application == Application.ROMS_MARBL
    assert workplan.steps[0].blueprint_overrides == {}
    assert workplan.steps[0].workflow_overrides["segment_length"] in {16, 16.0}
    slurm_overrides = t.cast(
        "dict[str, t.Any]", workplan.steps[0].compute_overrides["slurm"]
    )
    assert slurm_overrides["max_walltime"] == "00:10:00"
    assert slurm_overrides["num_nodes"] == 4


@pytest.mark.usefixtures("read_yaml_intercept")
def test_serialization_polymorphic_workplan(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that a workplan serialized with steps of a subclass of `Step` results
    in deserialization to the correct subclass/type.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    wp_templates_dir: Path
        Fixture returning the path to the directory containing workplan template files
    """
    template_file = "workplan.yaml"
    wp_path = wp_templates_dir / template_file

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

    # confirm deserialize to Workplan blows up due to strict model validation
    with pytest.raises(ValidationError, match="working_dir"):
        result = deserialize(poly_path, Workplan)

    # now, load and confirm the workplan contains LiveStep; it never recorded a
    # run root, so none is written or invented
    result = LiveWorkplan.load(poly_path)

    assert result.root is None
    for step in result.steps:
        assert isinstance(step, LiveStep)


def test_serialization_workplan_compute_env(
    tmp_path: Path,
    fill_workplan_template: Callable[[dict[str, t.Any]], str],
    complete_workplan_template_input: dict[str, t.Any],
) -> None:
    """Verify that dynamically added compute environment attributes are parsed."""
    data = complete_workplan_template_input

    slurm_defaults = {"num_nodes": 4, "cpus_per_node": 16}

    data["compute_environment"] = {"slurm": slurm_defaults}

    for i, step in enumerate(data.get("steps", [])):
        empty_bp_path = tmp_path / f"blueprint-{i:00}.yaml"
        empty_bp_path.touch()
        step["blueprint"] = empty_bp_path.as_posix()

    wp_yaml = fill_workplan_template(data)
    yaml_path = tmp_path / "workplan.yaml"
    yaml_path.write_text(wp_yaml)

    workplan = deserialize(yaml_path, Workplan)

    compute_env = workplan.compute_environment

    assert compute_env["slurm"] == slurm_defaults


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


def test_mode_detect_unknown_extension_defaults_to_yaml(
    plotter_v2_0_0_bp: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify an unrecognized extension falls back to YAML without printing to stdout."""
    target = tmp_path / "workplan_original.bak"
    bp = deserialize(plotter_v2_0_0_bp, PlotterBlueprint)
    assert serialize(target, bp, mode=PersistenceMode.yaml)
    capsys.readouterr()

    assert _mode_detect(target) == PersistenceMode.yaml
    assert deserialize(target, PlotterBlueprint, mode=PersistenceMode.auto) == bp
    assert capsys.readouterr().out == ""


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


def test_read_raw_file_dne(tmp_path: Path) -> None:
    """Verify reading a non-existent file raises a `FileNotFoundError` that
    carries the missing path in `filename` so callers can report it accurately.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    path = tmp_path / "does-not-exist.yaml"

    with pytest.raises(FileNotFoundError) as exc_info:
        read_raw(path)

    assert exc_info.value.errno == errno.ENOENT
    assert exc_info.value.filename == path


class _SimpleModel(BaseModel):
    """A minimal model used to exercise `deserialize_all`."""

    value: int


def _write_simple_model(path: Path, value: int) -> Path:
    """Write a valid `_SimpleModel` document to disk.

    Parameters
    ----------
    path : Path
        The destination path.
    value : int
        The value to persist.
    """
    serialize(path, _SimpleModel(value=value))
    return path


async def test_deserialize_all_preserves_order(tmp_path: Path) -> None:
    """Verify results are returned in the same order as the input paths."""
    paths = [
        _write_simple_model(tmp_path / f"model-{i}.yaml", value=i) for i in range(5)
    ]

    results = await deserialize_all(paths, _SimpleModel, limit=10)

    assert [r.value if r else None for r in results] == list(range(5))


async def test_deserialize_all_returns_none_for_unreadable(tmp_path: Path) -> None:
    """Verify an unreadable or invalid file yields `None` at its position."""
    good_path = _write_simple_model(tmp_path / "good.yaml", value=1)
    bad_path = tmp_path / "bad.yaml"
    bad_path.write_text("value: not-an-int\n")
    missing_path = tmp_path / "missing.yaml"

    results = await deserialize_all(
        [good_path, bad_path, missing_path], _SimpleModel, limit=10
    )

    assert results[0] is not None
    assert results[0].value == 1
    assert results[1] is None
    assert results[2] is None


async def test_deserialize_all_honours_supplied_semaphore(tmp_path: Path) -> None:
    """Verify an externally supplied semaphore bounds concurrent reads."""
    limit = 2
    lock = threading.Lock()
    state = {"in_flight": 0, "peak": 0}

    def fake_try_deserialize(path, klass, mode=PersistenceMode.auto):
        with lock:
            state["in_flight"] += 1
            state["peak"] = max(state["peak"], state["in_flight"])
        time.sleep(0.05)
        with lock:
            state["in_flight"] -= 1
        return None

    paths = [tmp_path / f"model-{i}.yaml" for i in range(6)]
    for p in paths:
        p.touch()

    sem = asyncio.Semaphore(limit)

    with mock.patch(
        "cstar.orchestration.serialization.try_deserialize",
        side_effect=fake_try_deserialize,
    ):
        # `limit` is intentionally larger than the supplied semaphore's limit
        # to confirm the supplied `sem` takes precedence.
        await deserialize_all(paths, _SimpleModel, limit=len(paths), sem=sem)

    assert state["peak"] <= limit


def _write_transformed_workplan(root: Path, record_root: bool = True) -> Path:
    """Write a one-step transformed workplan whose paths all sit under ``root``.

    The step carries its working directory, a blueprint file, directive values
    nested in a mapping and a list, a string that merely contains the root
    mid-string, and a sibling directory whose name starts with the root's.

    Parameters
    ----------
    root : Path
        The run root the workplan is written into.
    record_root : bool
        Whether the workplan records ``root``, as `prepare_workplan` does.

    Returns
    -------
    Path
        The path to the transformed workplan.
    """
    step_dir = root / "tasks" / "a"
    blueprint = step_dir / "work" / "blueprint.yaml"
    blueprint.parent.mkdir(parents=True)
    blueprint.write_text("name: a\n")

    step = LiveStep(
        name="a",
        application="hello_world",
        blueprint=str(blueprint),
        working_dir=step_dir,
        blueprint_overrides={
            "mid_string": f"--out={root}/mid",
            "sibling": f"{root}-other/x",
        },
        directives={
            "apply-overrides": {
                "working_dir": str(step_dir),
                "extra": [str(step_dir / "x"), "unrelated"],
            }
        },
    )
    workplan = LiveWorkplan(
        name="t",
        description="d",
        steps=[step],
        root=root if record_root else None,
    )
    path = root / "t_transformed.yaml"
    assert serialize(path, workplan)
    return path


def test_live_workplan_load_rebases_a_moved_run(tmp_path: Path) -> None:
    """Verify every string under the recorded root follows the file to its new directory,
    wherever it sits in the document, and strings that only contain it stay put.
    """
    old_root = tmp_path.resolve() / "old"
    _write_transformed_workplan(old_root)
    new_root = tmp_path.resolve() / "new"
    shutil.copytree(old_root, new_root)

    workplan = LiveWorkplan.load(new_root / "t_transformed.yaml")

    (step,) = workplan.steps
    assert workplan.root == new_root
    assert step.working_dir == new_root / "tasks" / "a"
    assert Path(str(step.blueprint_path)) == new_root / "tasks/a/work/blueprint.yaml"
    assert step.directives["apply-overrides"] == {
        "working_dir": str(new_root / "tasks" / "a"),
        "extra": [str(new_root / "tasks" / "a" / "x"), "unrelated"],
    }
    # mid-string and sibling-prefix values are not paths under the root
    assert step.blueprint_overrides == {
        "mid_string": f"--out={old_root}/mid",
        "sibling": f"{old_root}-other/x",
    }


def test_live_workplan_load_without_root_loads_as_written(tmp_path: Path) -> None:
    """Verify a file written before the root was recorded is not rebased."""
    old_root = tmp_path.resolve() / "old"
    _write_transformed_workplan(old_root, record_root=False)
    new_root = tmp_path.resolve() / "new"
    shutil.copytree(old_root, new_root)

    workplan = LiveWorkplan.load(new_root / "t_transformed.yaml")

    assert workplan.root is None
    assert workplan.steps[0].working_dir == old_root / "tasks" / "a"


def test_live_workplan_load_in_place_loads_as_written(tmp_path: Path) -> None:
    """Verify a file still sitting in its recorded root is not rebased."""
    root = tmp_path.resolve() / "run"
    path = _write_transformed_workplan(root)

    assert LiveWorkplan.load(path) == deserialize(path, LiveWorkplan)
    assert LiveWorkplan.load(str(path)).root == root


def test_live_workplan_load_ignores_an_empty_root(tmp_path: Path) -> None:
    """Verify an empty recorded root cannot rebase every absolute path."""
    root = tmp_path.resolve() / "run"
    path = _write_transformed_workplan(root)
    emptied = path.read_text().replace(f"root: {root}", "root: ''")
    assert "root: ''" in emptied
    path.write_text(emptied)

    workplan = LiveWorkplan.load(path)

    assert workplan.steps[0].working_dir == root / "tasks" / "a"


def test_live_workplan_load_missing_file(tmp_path: Path) -> None:
    """Verify a missing file raises the error `deserialize` raises."""
    with pytest.raises(FileNotFoundError):
        LiveWorkplan.load(tmp_path / "absent.yaml")

    assert LiveWorkplan.try_load(tmp_path / "absent.yaml") is None


async def test_live_workplan_load_all_returns_none_for_unreadable(
    tmp_path: Path,
) -> None:
    """Verify the batch loader rebases each file and yields `None` for an unreadable one."""
    old_root = tmp_path.resolve() / "old"
    _write_transformed_workplan(old_root)
    new_root = tmp_path.resolve() / "new"
    shutil.copytree(old_root, new_root)
    bad = tmp_path / "bad.yaml"
    bad.write_text("name: nope\n")

    results = await LiveWorkplan.load_all(
        [new_root / "t_transformed.yaml", bad, tmp_path / "absent.yaml"], limit=2
    )

    assert results[0] is not None
    assert results[0].root == new_root
    assert results[1:] == [None, None]


def test_live_workplan_root_serialization(tmp_path: Path) -> None:
    """Verify a recorded root is written, and an absent one is not."""
    recorded = _write_transformed_workplan(tmp_path.resolve() / "a")
    unrecorded = _write_transformed_workplan(tmp_path.resolve() / "b", False)

    assert f"root: {tmp_path.resolve() / 'a'}" in recorded.read_text()
    assert "root:" not in unrecorded.read_text()


def test_live_workplan_root_field_names_the_root_field() -> None:
    """Verify the key the loader reads is the name of the model's `root` field."""
    assert LiveWorkplan.ROOT_FIELD in LiveWorkplan.model_fields
    assert LiveWorkplan.model_fields[LiveWorkplan.ROOT_FIELD].annotation == Path | None


def test_rebase_paths() -> None:
    """Verify only strings equal to the root, or beneath it, are rebased, at any depth."""
    document = {
        "root": "/old/run",
        "nested": {"dir": "/old/run/tasks/a", "list": ["/old/run/x", "plain", 3]},
        "mid": "--out=/old/run/x",
        "sibling": "/old/run-other/x",
        "/old/run/key": "value",
    }

    rebased = rebase_paths(document, "/old/run", "/new/run")

    assert rebased == {
        "root": "/new/run",
        "nested": {"dir": "/new/run/tasks/a", "list": ["/new/run/x", "plain", 3]},
        "mid": "--out=/old/run/x",
        "sibling": "/old/run-other/x",
        "/old/run/key": "value",
    }
    assert document["root"] == "/old/run"


def test_read_document_drops_the_schema_key(tmp_path: Path) -> None:
    """Verify the raw reader returns the mapping `deserialize` validates."""
    path = tmp_path / "doc.yaml"
    path.write_text("$schema: https://example.org/s.json\nvalue: 1\n")

    assert read_document(path) == {"value": 1}
    assert read_document(str(path), PersistenceMode.yaml) == {"value": 1}
    with pytest.raises(FileNotFoundError):
        read_document(tmp_path / "absent.yaml")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("grid.nc", "grid.nc"),
        ("./grid.nc", "grid.nc"),
        ("sub/../data/grid.nc", "data/grid.nc"),
        ("  grid.nc  ", "grid.nc"),
        ("../shared/grid.nc", "../shared/grid.nc"),
    ],
)
def test_anchor_relative_path_resolves_against_base_dir(
    tmp_path: Path, text: str, expected: str
) -> None:
    """Verify a relative path is made absolute next to the base directory."""
    base_dir = tmp_path / "bp"

    anchored = anchor_relative_path(text, base_dir)

    assert anchored == (base_dir / expected).resolve()
    assert anchored is not None
    assert anchored.is_absolute()


def test_anchor_relative_path_keeps_a_symlink_as_the_name(tmp_path: Path) -> None:
    """Verify a location that is a symlink is not followed: its name stays the one
    written, while symlinked directories on the way are resolved.
    """
    real = tmp_path / "shared" / "v3" / "abc123.nc"
    real.parent.mkdir(parents=True)
    real.touch()
    base_dir = tmp_path / "bp"
    (base_dir / "data").mkdir(parents=True)
    (base_dir / "data" / "grid.nc").symlink_to(real)
    (base_dir / "linked").symlink_to(real.parent)

    assert anchor_relative_path("data/grid.nc", base_dir) == (
        base_dir.resolve() / "data" / "grid.nc"
    )
    assert anchor_relative_path("linked/abc123.nc", base_dir) == real.resolve()
    assert anchor_relative_path("sub/../data", base_dir) == (
        base_dir.resolve() / "data"
    )


@pytest.mark.parametrize(
    "text",
    [
        "",
        "   ",
        "/abs/path/grid.nc",
        "https://example.com/grid.nc",
        "s3://bucket/grid.nc",
        "{{output_dir: producer}}/grid.nc",
        "{{grid}}",
        "cstar-forge:child-domain-initial-conditions-placeholder",
        "placeholder://run_time",
    ],
)
def test_anchor_relative_path_passes_non_relative_values(
    tmp_path: Path, text: str
) -> None:
    """Verify empty, absolute, URI and placeholder values are not anchored."""
    assert anchor_relative_path(text, tmp_path) is None


class _ResourceModel(BaseModel):
    """A stand-in for a blueprint resource."""

    location: str
    partitioned: bool = False


class _DatasetModel(BaseModel):
    """A stand-in for a blueprint dataset holding a list of resources."""

    data: list[_ResourceModel]


class _BlueprintModel(BaseModel):
    """A stand-in for a blueprint with nested and listed resources."""

    name: str
    grid: _DatasetModel
    forcing: dict[str, _DatasetModel] = Field(default_factory=dict)


def _write_blueprint_yaml(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(body))
    return path


def test_load_blueprint_anchors_locations_at_the_files_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify nested and listed relative locations resolve against the blueprint's
    directory, whatever the working directory is.
    """
    bp_dir = tmp_path / "blueprints"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    path = _write_blueprint_yaml(
        bp_dir / "bp.yaml",
        """\
        name: anchored
        grid:
          data:
            - location: ../input_data/grid.nc
        forcing:
          surface:
            data:
              - location: surf_a.nc
              - location: ./sub/surf_b.nc
                partitioned: true
        """,
    )

    bp = load_blueprint(path, _BlueprintModel)

    assert [r.location for r in bp.grid.data] == [
        (tmp_path / "input_data" / "grid.nc").as_posix()
    ]
    assert [r.location for r in bp.forcing["surface"].data] == [
        (bp_dir / "surf_a.nc").as_posix(),
        (bp_dir / "sub" / "surf_b.nc").as_posix(),
    ]
    assert bp.forcing["surface"].data[1].partitioned


def test_load_blueprint_leaves_absolute_remote_and_placeholder_locations(
    tmp_path: Path,
) -> None:
    """Verify absolute, URL and placeholder locations pass through untouched."""
    values = [
        "/abs/grid.nc",
        "https://example.com/grid.nc",
        "{{grid_dir}}/grid.nc",
        "cstar-forge:upscaled-cdr-forcing-placeholder",
    ]
    listing = "\n".join(f"    - location: '{value}'" for value in values)
    path = _write_blueprint_yaml(
        tmp_path / "bp.yaml", f"name: untouched\ngrid:\n  data:\n{listing}\n"
    )

    bp = load_blueprint(path, _BlueprintModel)

    assert [r.location for r in bp.grid.data] == values


def test_load_blueprint_ignores_other_keys_and_non_string_locations(
    tmp_path: Path,
) -> None:
    """Verify only string values under `location` are anchored."""
    path = _write_blueprint_yaml(
        tmp_path / "bp.yaml",
        """\
        name: grid.nc
        grid:
          data:
            - location: grid.nc
        """,
    )

    bp = load_blueprint(path, _BlueprintModel)

    assert bp.name == "grid.nc"
    assert bp.grid.data[0].location == (tmp_path / "grid.nc").as_posix()


def test_load_blueprint_reads_json(tmp_path: Path) -> None:
    """Verify JSON blueprints are anchored like YAML ones."""
    path = tmp_path / "sub" / "bp.json"
    path.parent.mkdir()
    path.write_text(
        '{"name": "j", "grid": {"data": [{"location": "grid.nc"}]}, "$schema": "x"}'
    )

    bp = load_blueprint(path, _BlueprintModel)

    assert bp.grid.data[0].location == (path.parent / "grid.nc").as_posix()


def test_load_blueprint_keeps_a_symlinked_location_by_name(tmp_path: Path) -> None:
    """Verify the staged name of a location that is a symlink is the link's."""
    real = tmp_path / "shared" / "abc123.nc"
    real.parent.mkdir()
    real.touch()
    (tmp_path / "bp").mkdir()
    (tmp_path / "bp" / "grid.nc").symlink_to(real)
    path = _write_blueprint_yaml(
        tmp_path / "bp" / "bp.yaml",
        "name: linked\ngrid:\n  data:\n    - location: grid.nc\n",
    )

    bp = load_blueprint(path, _BlueprintModel)

    assert (
        bp.grid.data[0].location == (tmp_path.resolve() / "bp" / "grid.nc").as_posix()
    )


def test_load_blueprint_anchors_symlinked_blueprint_at_its_target(
    tmp_path: Path,
) -> None:
    """Verify a symlinked blueprint anchors where the real file lives."""
    real = _write_blueprint_yaml(
        tmp_path / "real" / "bp.yaml",
        "name: linked\ngrid:\n  data:\n    - location: grid.nc\n",
    )
    link = tmp_path / "links" / "bp.yaml"
    link.parent.mkdir()
    link.symlink_to(real)

    bp = load_blueprint(link, _BlueprintModel)

    assert bp.grid.data[0].location == (real.resolve().parent / "grid.nc").as_posix()


def test_load_blueprint_without_anchoring_leaves_locations_as_authored(
    tmp_path: Path,
) -> None:
    """Verify `anchor=False`, for a blueprint downloaded to a temporary copy, loads
    exactly as `deserialize` does.
    """
    path = _write_blueprint_yaml(
        tmp_path / "bp.yaml",
        "name: remote\ngrid:\n  data:\n    - location: ../data/grid.nc\n",
    )

    bp = load_blueprint(path, _BlueprintModel, anchor=False)

    assert bp.grid.data[0].location == "../data/grid.nc"
    assert bp == deserialize(path, _BlueprintModel)


def test_load_blueprint_missing_file(tmp_path: Path) -> None:
    """Verify a missing blueprint raises `FileNotFoundError`."""
    with pytest.raises(FileNotFoundError):
        load_blueprint(tmp_path / "missing.yaml", _BlueprintModel)


def test_load_blueprint_relative_location_only_in_cwd_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a location that exists only relative to the working directory, where
    C-Star used to look, fails with an error naming both candidate paths.
    """
    cwd = tmp_path / "cwd"
    (cwd / "data").mkdir(parents=True)
    (cwd / "data" / "grid.nc").touch()
    monkeypatch.chdir(cwd)
    bp_dir = tmp_path / "blueprints"
    path = _write_blueprint_yaml(
        bp_dir / "bp.yaml",
        "name: stranded\ngrid:\n  data:\n    - location: data/grid.nc\n",
    )

    with pytest.raises(CstarExpectationFailed) as exc_info:
        load_blueprint(path, _BlueprintModel)

    message = str(exc_info.value)
    assert (bp_dir / "data" / "grid.nc").as_posix() in message
    assert (cwd / "data" / "grid.nc").as_posix() in message
    assert "directory of the blueprint file" in message
    assert "not the working directory" in message


def test_load_blueprint_reports_every_stranded_location(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify one error lists all the locations that resolve differently."""
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    (cwd / "a.nc").touch()
    (cwd / "b.nc").touch()
    monkeypatch.chdir(cwd)
    path = _write_blueprint_yaml(
        tmp_path / "bp" / "bp.yaml",
        "name: stranded\ngrid:\n  data:\n    - location: a.nc\n    - location: b.nc\n",
    )

    with pytest.raises(CstarExpectationFailed) as exc_info:
        load_blueprint(path, _BlueprintModel)

    message = str(exc_info.value)
    assert "'a.nc'" in message
    assert "'b.nc'" in message


def test_read_blueprint_raw_not_strict_anchors_cwd_only_locations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a knowingly partial document is anchored without the check that
    rejects a location existing only relative to the working directory.
    """
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    (cwd / "a.nc").touch()
    monkeypatch.chdir(cwd)
    path = _write_blueprint_yaml(
        tmp_path / "bp" / "bp.yaml",
        "name: partial\ngrid:\n  data:\n    - location: a.nc\n",
    )

    with pytest.raises(CstarExpectationFailed):
        read_blueprint_raw(path)

    raw = read_blueprint_raw(path, strict=False)

    assert raw["grid"]["data"][0]["location"] == (tmp_path / "bp" / "a.nc").as_posix()


def test_load_blueprint_missing_everywhere_passes_through(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a relative location found in neither place is anchored without error,
    leaving the file's absence to be reported, by path, when it is read.
    """
    monkeypatch.chdir(tmp_path)
    path = _write_blueprint_yaml(
        tmp_path / "bp" / "bp.yaml",
        "name: missing\ngrid:\n  data:\n    - location: nowhere.nc\n",
    )

    bp = load_blueprint(path, _BlueprintModel)

    assert bp.grid.data[0].location == (tmp_path / "bp" / "nowhere.nc").as_posix()


def test_load_blueprint_agrees_with_deserialize_on_absolute_content(
    tmp_path: Path,
) -> None:
    """Verify a blueprint with only absolute locations loads identically either way."""
    path = _write_blueprint_yaml(
        tmp_path / "bp.yaml",
        "name: same\ngrid:\n  data:\n    - location: /abs/grid.nc\n",
    )

    assert load_blueprint(path, _BlueprintModel) == deserialize(path, _BlueprintModel)


def test_read_blueprint_raw_returns_the_anchored_mapping(tmp_path: Path) -> None:
    """Verify the raw reader, used for lenient loads, anchors the same way."""
    path = _write_blueprint_yaml(
        tmp_path / "bp.yaml",
        "name: raw\ngrid:\n  data:\n    - location: grid.nc\n",
    )

    raw = read_blueprint_raw(path)

    assert raw["grid"]["data"][0]["location"] == (tmp_path / "grid.nc").as_posix()
