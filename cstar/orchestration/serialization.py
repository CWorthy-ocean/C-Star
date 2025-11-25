import enum
import typing as t
from pathlib import Path, PosixPath

import yaml
from pydantic import BaseModel
from yaml import safe_load

from cstar.orchestration import models


class PersistenceMode(enum.StrEnum):
    """Supported serialization engines."""

    json = enum.auto()
    yaml = enum.auto()
    auto = enum.auto()


_T = t.TypeVar("_T", bound=BaseModel)


def _read_json(path: Path, klass: type[_T]) -> _T:
    """Read and process content as a JSON file.

    Parameters
    ----------
    path : Path
        The path to a file containing content.
    klass : type[_T]
        The type to instantiate from the content.

    Returns
    -------
    _T
    """
    with path.open("r", encoding="utf-8") as fp:
        return klass.model_validate_json(json_data=fp.read())


def _read_yaml(path: Path, klass: type[_T]) -> _T:
    """Read and process content as a YAML file.

    Parameters
    ----------
    path : Path
        The path to a file containing content.
    klass : type[_T]
        The type to instantiate from the content.

    Returns
    -------
    _T
    """
    with path.open("r", encoding="utf-8") as fp:
        model_dict = safe_load(fp)
        return klass.model_validate(model_dict)


def model_to_yaml(model: BaseModel) -> str:
    """Serialize a model to yaml.

    Parameters
    ----------
    model : BaseModel
        The model to be serialized.

    Returns
    -------
    str
        The serialized model
    """
    dumped = model.model_dump(exclude_defaults=True)

    def path_representer(
        dumper: yaml.Dumper,
        data: PosixPath,
    ) -> yaml.ScalarNode:
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))

    def application_representer(
        dumper: yaml.Dumper,
        data: models.Application,
    ) -> yaml.ScalarNode:
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))

    def blueprintstate_representer(
        dumper: yaml.Dumper,
        data: models.BlueprintState,
    ) -> yaml.ScalarNode:
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))

    def workplanstate_representer(
        dumper: yaml.Dumper,
        data: models.WorkplanState,
    ) -> yaml.ScalarNode:
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))

    dumper = yaml.Dumper
    dumper.ignore_aliases = lambda *_args: True  # type: ignore[method-assign]

    dumper.add_representer(PosixPath, path_representer)
    dumper.add_representer(models.WorkplanState, workplanstate_representer)
    dumper.add_representer(models.Application, application_representer)
    dumper.add_representer(models.BlueprintState, blueprintstate_representer)

    return yaml.dump(dumped, sort_keys=False)


_DT = t.TypeVar("_DT", models.RomsMarblBlueprint, models.Workplan)


def deserialize(
    path: Path,
    klass: type[_DT],
    mode: PersistenceMode = PersistenceMode.auto,
) -> _DT:
    """Deserialize a blueprint into a Simulation instance.

    Parameters
    ----------
    path : Path
        The location of the blueprint
    klass : type[_T]
        The type to instantiate.
    mode : t.Literal["json", "yaml", "auto"]
        The type of serializer used to create the file.

        The default value of `auto` selects based on the file extension.

    Raises
    ------
    FileNotFoundError:
        If the blueprint file does not exist
    ValueError
        If the blueprint cannot be deserialized with the desired mode

    Returns
    -------
    Simulation
        The deserialized Simulation instance.

    """
    if not path.exists():
        msg = f"No file found at path `{path}` to deserialize to `{klass.__name__}`"
        raise FileNotFoundError(msg)

    model: _DT | None = None
    ext = path.suffix
    is_auto = mode == PersistenceMode.auto
    use_json = (is_auto and ext == ".json") or mode == PersistenceMode.json
    use_yaml = (is_auto and (ext in {".yaml", ".yml"})) or mode == PersistenceMode.yaml

    if not use_json and not use_yaml:
        print(f"Non-standard file name {path} was provided. Defaulting to YAML.")
        use_yaml = True

    if use_json:
        model = _read_json(path, klass)
    elif use_yaml:
        model = _read_yaml(path, klass)

    if model is None:
        msg = f"Unable to deserialize the `{klass.__name__}` at `{path}` as `{mode}` from: \n{path.read_text()}"
        raise ValueError(msg)

    return model


def serialize(
    path: Path,
    model: BaseModel,
    mode: PersistenceMode = PersistenceMode.yaml,
) -> None:
    """Serialize a model into a file.

    Parameters
    ----------
    path : Path
        The location to store the serialized model in
    model : BaseModel
        The model to serialize
    mode : PersistenceMode
        Specify the type of document to produce (yaml or json)
    """
    if mode == PersistenceMode.auto:
        mode = PersistenceMode.yaml

    if mode == PersistenceMode.json:
        output_document = model.model_dump_json()
    elif mode == PersistenceMode.yaml:
        output_document = model_to_yaml(model)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open(mode="w") as fp:
        fp.write(output_document)
