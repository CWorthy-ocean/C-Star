import enum
import typing as t
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PosixPath

import yaml
from pydantic_core import from_json
from yaml.scanner import ScannerError

from cstar.base.log import get_logger
from cstar.execution.file_system import local_copy

log = get_logger(__name__)


class PersistenceMode(enum.StrEnum):
    """Supported serialization engines."""

    json = enum.auto()
    yaml = enum.auto()
    auto = enum.auto()


class SerializableModel(t.Protocol):
    """Protocol defining API required to serialize and deserialize objects.

    This is a stand-in for pydantic's BaseModel, as it can cause issues
    with metacalases in a protocol.
    """

    def model_dump_json(self, *args, **kwargs) -> str:  # type: ignore  # noqa: ANN002, ANN003, PGH003
        """Return a JSON string representation of the object."""
        ...

    def model_dump(self, *args, **kwargs) -> dict[str, t.Any]:  # type: ignore  # noqa: ANN002, ANN003, PGH003
        """Return a dictionary representation of the object."""
        ...

    @classmethod
    def model_validate_json(cls, *args, **kwargs) -> "t.Self":  # type: ignore  # noqa: ANN002, ANN003, PGH003
        """Return a dictionary representation of the object."""
        ...

    @classmethod
    def model_validate(cls, *args, **kwargs) -> "t.Self":  # type: ignore  # noqa: ANN002, ANN003, PGH003
        """Return a dictionary representation of the object."""
        ...


_T = t.TypeVar("_T", bound=SerializableModel)


@dataclass
class ValidationResult(t.Generic[_T]):
    """Disposition and reason for a validation failure."""

    error_msg: str = ""
    """An error message that is populated if validation fails."""
    item: _T | None = None
    """The deserialized workplan if validation succeeds."""

    @property
    def is_valid(self) -> bool:
        """Return `True` if the workplan was successfully validated.

        Returns
        -------
        bool
        """
        return self.item is not None


def read_json_to_raw(path: Path) -> dict[str, t.Any]:
    """Read and process content as a JSON file.

    Parameters
    ----------
    path : Path
        The path to a file containing JSON.

    Returns
    -------
    dict[str, t.Any]
    """
    with path.open("r", encoding="utf-8") as fp:
        json_data = fp.read()
        return from_json(json_data)


def read_yaml_to_raw(path: Path) -> dict[str, t.Any]:
    """Read and process content as a YAML file.

    Parameters
    ----------
    path : Path
        The path to a file containing YAML.

    Returns
    -------
    dict[str, t.Any]
    """
    with path.open("r", encoding="utf-8") as fp:
        try:
            model_dict = yaml.safe_load(fp)
        except ScannerError as e:
            msg = (
                f"Failed to load yaml from {path}. If are loading a remote YAML, your URL "
                f"may point to a HTML page instead of the raw YAML content."
            )
            raise RuntimeError(msg) from e
        return model_dict


def read_raw(
    path: Path,
    mode: PersistenceMode = PersistenceMode.auto,
) -> dict[str, t.Any]:
    """Given a path to a persisted model, use the file extension to identify
    the data format and load the model data into a raw dictionary.

    Parameters
    ----------
    path : Path
        The path to the persisted entity.

    Returns
    -------
    dict[str, t.Any]
    """
    if not path.exists():
        msg = f"Unable to read raw model from path: {path}"
        raise FileNotFoundError(msg)

    if mode == PersistenceMode.auto:
        mode = _mode_detect(path)

    if mode == PersistenceMode.json:
        return read_json_to_raw(path)

    return read_yaml_to_raw(path)


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
    model_dict = read_raw(path, PersistenceMode.json)
    return klass.model_validate(model_dict)  # type: ignore  # noqa: PGH003


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
    model_dict = read_raw(path, PersistenceMode.yaml)
    return klass.model_validate(model_dict)  # type: ignore  # noqa: PGH003


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


def set_representer(
    dumper: yaml.Dumper,
    data: set,
) -> yaml.SequenceNode:
    """Create a representer for converting sets values to a list representation."""
    return dumper.represent_list(list(data))


_RT = t.TypeVar("_RT", enum.IntEnum, enum.StrEnum, PosixPath)


def register_representer(
    model_type: type[_RT],
    conversion_fn: Callable[[yaml.Dumper, _RT], yaml.ScalarNode],
) -> None:
    """Register a yaml representer for the serialization of a specific entity type."""
    dumper = yaml.Dumper
    dumper.add_representer(model_type, conversion_fn)


def model_to_yaml(model: SerializableModel) -> str:
    """Serialize a model to yaml.

    Parameters
    ----------
    model : _T
        The model to be serialized.

    Returns
    -------
    str
        The serialized model
    """
    dumped = model.model_dump(exclude_defaults=True, by_alias=True)

    # 'application' is required by the base Blueprint type for deserialization
    # routing; always include it even when it equals the subclass default.
    if hasattr(model, "application"):
        dumped["application"] = str(getattr(model, "application"))  # noqa: B009

    dumper = yaml.Dumper
    dumper.ignore_aliases = lambda *_args: True  # type: ignore[method-assign]
    dumper.add_representer(set, set_representer)

    register_representer(PosixPath, path_representer)

    if schema_url := str(dumped.pop("$schema", "")):
        content = yaml.dump(dumped, sort_keys=False)
        content = f"# yaml-language-server: $schema={schema_url}\n{content}"
    else:
        content = yaml.dump(dumped, sort_keys=False)

    return content


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

    if mode == PersistenceMode.json:
        model = _read_json(path, klass)
    else:
        model = _read_yaml(path, klass)

    msg = f"Deserialized {str(path)!r} into: {model}"
    log.trace(msg)

    return model


def serialize(
    path: Path,
    model: SerializableModel,
    mode: PersistenceMode = PersistenceMode.yaml,
) -> int:
    """Serialize a model into a file.

    Parameters
    ----------
    path : Path
        The location to store the serialized model in
    model : _T
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

    if mode == PersistenceMode.json:
        content = model.model_dump_json(  # type: ignore[reportUnknownMemberType]
            by_alias=True
        )
    else:
        content = model_to_yaml(model)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open(mode="w") as fp:
        nbytes = fp.write(content)

    return nbytes


def validate_serialized_entity(
    path: str | Path,
    item_type: type[_T],
) -> ValidationResult[_T]:
    """Perform content validation on a deserialized `Workplan`.

    Parameters
    ----------
    path : str
        A path-like string supplied by a user.
    item_type : type[_TValidating]
        The type to deserialize the file content to

    Returns
    -------
    ValidationResult
        Result with a populated `error_msg` if validation has failed.
    """
    item: _T | None = None

    try:
        with local_copy(str(path)) as wp_path:
            item = deserialize(wp_path, item_type)
    except ValueError as ex:
        return ValidationResult(f"The {item_type.__name__} is invalid: {ex}")
    except FileNotFoundError:
        return ValidationResult(f"File not found at path: {path}")

    return ValidationResult(item=item)
