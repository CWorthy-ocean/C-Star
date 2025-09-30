# ruff: noqa: S101

import json
import pathlib
import textwrap
import typing as t
import uuid
from pathlib import Path

import pytest
import yaml
from pydantic import BaseModel

from cstar.orchestration.models import (
    Application,
    BlueprintState,
    RomsMarblBlueprint,
    Step,
    Workplan,
    WorkplanState,
)


def model_to_yaml(model: BaseModel) -> str:
    """Serialize a model to yaml.

    Parameters
    ----------
    model : BaseModel
        The model to be serialized

    Returns
    -------
    str
        The serialized model
    """
    dumped = model.model_dump(exclude_defaults=True)

    def path_representer(
        dumper: yaml.Dumper,
        data: pathlib.PosixPath,
    ) -> yaml.ScalarNode:
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))

    def application_representer(
        dumper: yaml.Dumper,
        data: Application,
    ) -> yaml.ScalarNode:
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))

    def blueprintstate_representer(
        dumper: yaml.Dumper,
        data: BlueprintState,
    ) -> yaml.ScalarNode:
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))

    def workplanstate_representer(
        dumper: yaml.Dumper,
        data: WorkplanState,
    ) -> yaml.ScalarNode:
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))

    dumper = yaml.Dumper
    dumper.ignore_aliases = lambda *_args: True  # type: ignore[method-assign]

    dumper.add_representer(pathlib.PosixPath, path_representer)
    dumper.add_representer(WorkplanState, workplanstate_representer)
    dumper.add_representer(Application, application_representer)
    dumper.add_representer(BlueprintState, blueprintstate_representer)

    return yaml.dump(dumped, sort_keys=False)


_T = t.TypeVar("_T", bound=BaseModel)


def yaml_to_model(yaml_doc: str, cls: type[_T]) -> _T:
    """Deserialize yaml to a model.

    Parameters
    ----------
    yaml_doc : str
        The serialized model
    cls : type
        The type to deserialize to

    Returns
    -------
    _T
        The deserialized model instance
    """
    loaded_dict = yaml.safe_load(yaml_doc)

    return cls.model_validate(loaded_dict)


@pytest.fixture
def serialize_blueprint(
    blueprint_schema_path: Path,
) -> t.Callable[[RomsMarblBlueprint, Path], str]:
    def _inner(model: RomsMarblBlueprint, path: Path) -> str:
        yaml_doc = model_to_yaml(model)

        schema_directive = (
            f"# yaml-language-server: $schema={blueprint_schema_path.as_posix()}"
        )

        yaml_doc = f"{schema_directive}\n{yaml_doc}"

        print(f"Writing test yaml document to: {path}")
        path.write_text(yaml_doc, encoding="utf-8")

        return yaml_doc

    return _inner


@pytest.fixture
def serialize_workplan(
    workplan_schema_path: Path,
) -> t.Callable[[Workplan, Path], str]:
    def _inner(model: Workplan, path: Path) -> str:
        yaml_doc = model_to_yaml(model)

        schema_directive = (
            f"# yaml-language-server: $schema={workplan_schema_path.as_posix()}"
        )

        yaml_doc = f"{schema_directive}\n{yaml_doc}"

        print(f"Writing test yaml document to: {path}")
        path.write_text(yaml_doc, encoding="utf-8")

        return yaml_doc

    return _inner


@pytest.fixture
def deserialize_model() -> t.Callable[[Path, type], BaseModel]:
    def _inner(path: Path, klass: type) -> BaseModel:
        yaml_content = path.read_text()
        return yaml_to_model(yaml_content, klass)

    return _inner


@pytest.fixture
def fake_blueprint_path(tmp_path: Path) -> Path:
    """Create an empty blueprint yaml file.

    Parameters
    ----------
    tmp_path : Path
        Unique path for test-specific files
    """
    path = tmp_path / "blueprint.yml"
    path.touch()
    return path


@pytest.fixture
def gen_fake_steps(tmp_path: Path) -> t.Callable[[int], t.Generator[Step, None, None]]:
    """Create fake steps for testing purposes.

    Parameters
    ----------
    tmp_path : Path
        Unique path for test-specific files
    """

    def _gen_fake_steps(num_steps: int) -> t.Generator[Step, None, None]:
        """Create `num_steps` fake steps."""
        for _ in range(num_steps):
            step_name = f"test-step-{uuid.uuid4()}"
            app_name = f"test-app-{uuid.uuid4()}"
            path = tmp_path / f"dummy-blueprint-{uuid.uuid4()}.yml"
            path.touch()

            yield Step(
                name=step_name,
                application=app_name,
                blueprint=path,
            )

    return _gen_fake_steps


@pytest.fixture
def load_workplan() -> t.Callable[[Path], Workplan]:
    """Create a function to load workplan yaml."""

    def _data_loader(path: Path) -> Workplan:
        """Deserialize a yaml file and return the resulting Workplan."""
        yaml_doc = path.read_text(encoding="utf-8")
        return yaml_to_model(yaml_doc, Workplan)

    return _data_loader


@pytest.fixture
def load_blueprint() -> t.Callable[[Path], RomsMarblBlueprint]:
    """Create a function to load workplan yaml."""

    def _data_loader(path: Path) -> RomsMarblBlueprint:
        """Deserialize a yaml file and return the resulting Workplan."""
        yaml_doc = path.read_text(encoding="utf-8")
        return yaml_to_model(yaml_doc, RomsMarblBlueprint)

    return _data_loader


@pytest.fixture
def complete_workplan_template_input() -> dict[str, t.Any]:
    """Create a dictionary containing all attributes required to deserialize
    a valid workplan, each populated with a valid value (no steps).
    """
    return {
        "name": "Test Workplan",
        "description": "This is the description of my test workplan",
        "state": "draft",
        "compute_environment": {
            "num_nodes": 4,
            "num_cpus_per_process": 16,
        },
        "runtime_vars": ["var1", "var2"],
        "steps": [],
    }


@pytest.fixture
def empty_workplan_template_input() -> dict[str, t.Any]:
    """Create a dictionary containing all attributes required to deserialize
    a valid workplan, but with each field being an empty or unset value.
    """
    return {
        "name": "",
        "description": "",
        "state": "",
        "compute_environment": {},
        "runtime_vars": [],
        "steps": [],
    }


@pytest.fixture
def complete_blueprint_template_input() -> dict[str, t.Any]:
    """Create a dictionary containing all attributes required to deserialize
    a valid blueprint, each populated with a valid value.
    """
    return {
        "name": "Test Workplan",
        # "description": "This is the description of my test workplan",
        # "state": "draft",
        # "compute_environment": {
        #     "num_nodes": 4,
        #     "num_cpus_per_process": 16,
        # },
        # "runtime_vars": ["var1", "var2"],
        # "steps": [],
    }


@pytest.fixture
def empty_blueprint_template_input() -> dict[str, t.Any]:
    """Create a dictionary containing all attributes required to deserialize
    a valid blueprint, but with each field being an empty or unset value.
    """
    return {
        "name": "",
        # "description": "",
        # "state": "",
        # "compute_environment": {},
        # "runtime_vars": [],
        # "steps": [],
    }


@pytest.fixture
def workplan_schema_path(tmp_path: Path) -> Path:
    """Create a schema file that can be referenced in a yaml document."""
    # yaml-language-server: $schema=/this/path.json
    path = tmp_path / "schema.json"
    schema = json.dumps(Workplan.model_json_schema())
    path.write_text(schema)
    return path


@pytest.fixture
def blueprint_schema_path(tmp_path: Path) -> Path:
    """Create a schema file that can be referenced in a yaml document."""
    # yaml-language-server: $schema=/this/path.json
    path = tmp_path / "schema.json"
    schema = json.dumps(RomsMarblBlueprint.model_json_schema())
    path.write_text(schema)
    return path


@pytest.fixture
def fill_workplan_template(
    empty_workplan_template_input: dict[str, t.Any],
    workplan_schema_path: Path,
) -> t.Callable[[dict[str, t.Any]], str]:
    """Create a function to populate a raw workplan yaml document template."""

    def _get_workplan_template(
        input_data: dict[str, t.Any],
        list_form: int = 0,
    ) -> str:
        """Populate the template with the supplied data."""
        dedent = "            "  # depth of populated dedent below...
        l1_indent = " " * 4
        # l2_indent = l1_indent * 2

        fill_vals = empty_workplan_template_input

        if "name" in input_data:
            fill_vals["name"] = input_data["name"]
        if "description" in input_data:
            fill_vals["description"] = input_data["description"]
        if "state" in input_data:
            fill_vals["state"] = input_data["state"]

        for dict_key in ["compute_environment"]:
            user_supplied = input_data.get(dict_key, "")

            if user_supplied:
                clauses = [
                    f"{dedent}{l1_indent}{k}: {v}" for k, v in user_supplied.items()
                ]
                clause = "\n" + "\n".join(clauses)
            else:
                clause = " {}"

            fill_vals[dict_key] = clause

        rtvs = input_data.get("runtime_vars", [])
        if list_form == 0:
            fill_vals["runtime_vars"] = f"[{', '.join(rtvs)}]"
        elif list_form == 1:
            fill_vals["runtime_vars"] = "\n" + "\n".join(
                f"{dedent}{l1_indent}- {rtv}" for rtv in rtvs
            )

        # NOTE: using __file__ for blueprint path to give an existing path to pass validator
        populated = textwrap.dedent(
            f"""\
            # yaml-language-server: $schema={workplan_schema_path.as_posix()}
            name: {fill_vals["name"]}
            description: {fill_vals["description"]}
            state: {fill_vals["state"]}
            compute_environment: {fill_vals["compute_environment"]}
                # num_nodes: 4
                # num_cpus_per_process: 16

            runtime_vars: {fill_vals.get("runtime_vars", "")}
            steps:
                - name: Test Step
                  application: sleep
                  depends_on: []
                  blueprint: {__file__}
                  blueprint_overrides: {{}}
                  workflow_overrides:
                      segment_length: 16
                  compute_overrides:
                      walltime: 00:10:00
                      num_nodes: 4
                - name: Test Another Step
                  application: sleep
                  depends_on: []
                  blueprint: {__file__}
                  blueprint_overrides: {{}}
                  workflow_overrides:
                      segment_length: 16
                  compute_overrides:
                      walltime: 00:10:00
                      num_nodes: 4
            """,
        )

        print(f"populated workplan template:\n{populated}\n")

        return populated

    return _get_workplan_template


@pytest.fixture
def fill_blueprint_template(
    empty_workplan_template_input: dict[str, t.Any],
    blueprint_schema_path: Path,
) -> t.Callable[[dict[str, t.Any]], str]:
    """Create a function to populate a raw workplan yaml document template."""

    def _get_blueprint_template(
        input_data: dict[str, t.Any],
        list_form: int = 0,
    ) -> str:
        """Populate the template with the supplied data."""
        dedent = "            "  # depth of populated dedent below...
        l1_indent = " " * 4
        # l2_indent = l1_indent * 2

        fill_vals = empty_workplan_template_input

        if "name" in input_data:
            fill_vals["name"] = input_data["name"]
        if "description" in input_data:
            fill_vals["description"] = input_data["description"]
        if "state" in input_data:
            fill_vals["state"] = input_data["state"]

        for dict_key in ["compute_environment"]:
            user_supplied = input_data.get(dict_key, "")

            if user_supplied:
                clauses = [
                    f"{dedent}{l1_indent}{k}: {v}" for k, v in user_supplied.items()
                ]
                clause = "\n" + "\n".join(clauses)
            else:
                clause = " {}"

            fill_vals[dict_key] = clause

        rtvs = input_data.get("runtime_vars", [])
        if list_form == 0:
            fill_vals["runtime_vars"] = f"[{', '.join(rtvs)}]"
        elif list_form == 1:
            fill_vals["runtime_vars"] = "\n" + "\n".join(
                f"{dedent}{l1_indent}- {rtv}" for rtv in rtvs
            )

        # NOTE: using __file__ for blueprint path to give an existing path to pass validator
        populated = textwrap.dedent(
            f"""\
            # yaml-language-server: $schema={blueprint_schema_path.as_posix()}
            name: {fill_vals["name"]}
            description: This is the description of my test blueprint
            application: sleep
            state: draft
            valid_start_date: 2020-01-01 00:00:00
            valid_end_date: 2020-02-01 00:00:00
            code:
              roms:
                location: http://github.com/ankona/ucla-roms
                branch: main
                filter: null
              run_time:
                location: http://github.com/ankona/ucla-roms
                branch: main
                filter: null
              compile_time:
                location: http://github.com/ankona/ucla-roms
                branch: main
                filter: null
              marbl: null
            forcing:
              boundary:
                data:
                  - location: http://mockdoc.com/partitioning1.nc
                    hash: abc
                  - location: http://mockdoc.com/partitioning2.nc
                    hash: pqr
                  - location: http://mockdoc.com/partitioning3.nc
                    hash: xyz
              surface:
                documentation: http://mockdoc.com/partitioning
                locked: false
                data:
                  - location: http://mockdoc.com/partitioning.nc
                    hash: abc123
              corrections:
                documentation: http://mockdoc.com/partitioning
                locked: false
                data:
                  - location: http://mockdoc.com/partitioning.nc
                    hash: abc123
              tidal:
                documentation: http://mockdoc.com/partitioning
                locked: false
                data:
                  location: http://mockdoc.com/partitioning.nc
                  hash: abc123
              river:
                documentation: http://mockdoc.com/partitioning
                locked: false
                data:
                  location: http://mockdoc.com/partitioning.nc
                  hash: abc123
            partitioning:
              documentation: http://mockdoc.com/partitioning
              hash: null
              locked: false
              n_procs_x: 16
              n_procs_y: 8
            model_params:
              documentation: http://mockdoc.com/model-params
              hash: null
              locked: false
            runtime_params:
              documentation: http://mockdoc.com/runtime-params
              hash: null
              locked: false
              start_date: 2020-01-01 00:00:00
              end_date: 2020-01-02 00:00:00
              checkpoint_frequency: 1d
              output_dir: .
            grid:
              documentation: http://mockdoc.com/model-params
              data:
                location: http://mockdoc.com/grid
            initial_conditions:
              data:
                location: http://mockdoc.com/grid
            model_params:
              time_step: 1
            """,
        )

        print(f"populated blueprint template:\n{populated}\n")

        return populated

    return _get_blueprint_template
