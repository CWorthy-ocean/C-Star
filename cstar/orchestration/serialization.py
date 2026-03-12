import enum
import typing as t
from pathlib import Path, PosixPath

import yaml
from pydantic import BaseModel
from yaml import safe_load

from cstar.base.log import get_logger

log = get_logger(__name__)


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


def path_representer(
    dumper: yaml.Dumper,
    data: PosixPath,
) -> yaml.ScalarNode:
    """Create a representer for converting PosixPath values to a serialized string."""
    return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))


def strenum_representer(
    dumper: yaml.Dumper,
    data: enum.StrEnum,
) -> yaml.ScalarNode:
    """Create a representer for converting StrEnum values to a serialized string."""
    return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))


def intenum_representer(
    dumper: yaml.Dumper,
    data: enum.IntEnum,
) -> yaml.ScalarNode:
    """Create a representer for converting IntEnum values to a serialized string."""
    return dumper.represent_scalar("tag:yaml.org,2002:int", str(data.value))


_RT = t.TypeVar("_RT", enum.IntEnum, enum.StrEnum, PosixPath)


def register_representer(
    model_type: type[_RT],
    conversion_fn: t.Callable[[yaml.Dumper, _RT], yaml.ScalarNode],
) -> None:
    """Register a yaml representer for the serialization of a specific entity type."""
    dumper = yaml.Dumper
    dumper.add_representer(model_type, conversion_fn)


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
    dumped = model.model_dump(exclude_defaults=True, by_alias=True)

    dumper = yaml.Dumper
    dumper.ignore_aliases = lambda *_args: True  # type: ignore[method-assign]

    register_representer(PosixPath, path_representer)

    return yaml.dump(dumped, sort_keys=False)


def _mode_detect(path: Path) -> PersistenceMode:
    """Use the file extension to select the persistence mode.

    Returns
    -------
    PersistenceMode
    """
    if path.suffix == ".json":
        return PersistenceMode.json

    if path.suffix in {".yaml", ".yml"}:
        return PersistenceMode.yaml

    print("Using default persistence mode `yaml` for file `{path}`")
    return PersistenceMode.yaml


def deserialize(
    path: Path | str,
    klass: type[_T],
    mode: PersistenceMode = PersistenceMode.auto,
) -> _T:
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
    if isinstance(path, str):
        path = Path(path)

    if not path.exists():
        msg = f"No file found at path `{path}` to deserialize to `{klass.__name__}`"
        raise FileNotFoundError(msg)

    if mode == PersistenceMode.auto:
        mode = _mode_detect(path)

    handlers = {
        PersistenceMode.json: _read_json,
        PersistenceMode.yaml: _read_yaml,
    }

    model = handlers[mode](path, klass)

    if model is None:
        msg = f"Unable to deserialize a `{klass.__name__}` at `{path}` as `{mode}` from: \n{path.read_text()}"
        raise ValueError(msg)

    return model


def serialize(
    path: Path,
    model: BaseModel,
    mode: PersistenceMode = PersistenceMode.yaml,
) -> int:
    """Serialize a model into a file.

    Parameters
    ----------
    path : Path
        The location to store the serialized model in
    model : BaseModel
        The model to serialize
    mode : PersistenceMode
        Specify the type of document to produce (yaml or json)

    Returns
    -------
    int
        The number of bytes written.
    """
    if path.exists():
        msg = f"Overwriting existing file at `{path}`"
        log.debug(msg)

    if mode == PersistenceMode.auto:
        mode = _mode_detect(path)

    handlers = {
        PersistenceMode.json: lambda model: model.model_dump_json(by_alias=True),
        PersistenceMode.yaml: model_to_yaml,
    }

    content = handlers[mode](model)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open(mode="w") as fp:
        nbytes = fp.write(content)

    return nbytes
