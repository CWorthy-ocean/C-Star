import asyncio
import enum
import errno
import os
import re
import typing as t
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path, PosixPath
from urllib.parse import urlsplit

import yaml
from yaml.scanner import ScannerError

from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.execution.file_system import local_copy
from cstar.system.migration import CstarSchemaTooNewError

log = get_logger(__name__)

PLACEHOLDER_RE = re.compile(r"\{\{([^}]+)\}\}")
"""Pattern matching double-brace template placeholders.

Captures the full content between braces so dispatch logic can distinguish
plain variable references (``{{my_var}}``) from scope references
(``{{<scope>: step_name}}``).
"""

LOCATION_KEY: t.Final[str] = "location"
"""Key under which a blueprint names where a resource (dataset, code, file) lives."""

_SAFE_LOADER: t.Final[type[yaml.SafeLoader]] = getattr(
    yaml, "CSafeLoader", yaml.SafeLoader
)
"""The safe YAML loader, libyaml's when PyYAML was built with it: about five times
faster on a large run manifest, which lists every product the run recorded."""


class SerializableModel(t.Protocol):
    """Protocol defining API required to serialize and deserialize objects.

    This is a stand-in for pydantic's BaseModel, as it can cause issues
    with metacalases in a protocol.
    """

    def model_dump(self, *args, **kwargs) -> dict[str, t.Any]:  # type: ignore
        """Return a dictionary representation of the object."""
        ...

    @classmethod
    def model_validate(cls, *args, **kwargs) -> "t.Self":  # type: ignore
        """Return a dictionary representation of the object."""
        ...


_T = t.TypeVar("_T", bound=SerializableModel)
_R = t.TypeVar("_R")


@dataclass
class ValidationResult(t.Generic[_T]):
    """Disposition and reason for a validation failure."""

    error_msg: str = ""
    """An error message that is populated if validation fails."""
    item: _T | None = None
    """The deserialized workplan if validation succeeds."""
    path: Path | None = None
    """The local file the item was read from (a temporary copy of a remote
    URI); `None` if validation fails."""
    error: ValueError | CstarSchemaTooNewError | None = None
    """The exception behind `error_msg`, when validation failed; a pydantic
    `ValidationError` locates each problem in the document, and a
    `CstarSchemaTooNewError` says the file was written by a newer release."""

    @property
    def is_valid(self) -> bool:
        """Return `True` if the workplan was successfully validated.

        Returns
        -------
        bool
        """
        return self.item is not None


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
        return yaml.load(fp, Loader=_SAFE_LOADER)


def read_raw(path: Path) -> dict[str, t.Any]:
    """Load a persisted model into a raw dictionary.

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
        raise FileNotFoundError(errno.ENOENT, msg, path)

    try:
        return read_yaml_to_raw(path)
    except ScannerError as ex:
        msg = (
            f"Failed to deserialize {str(path)!r}. If you are loading a remote "
            "document, your URL may point to an HTML page instead of the raw YAML "
            "content."
        )
        raise RuntimeError(msg) from ex


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


def enum_representer(
    dumper: yaml.Dumper,
    data: enum.Enum,
) -> yaml.Node:
    """Create a representer converting any Enum to its underlying value.

    Fallback for enums without a registered class-specific representer, e.g.
    those declared by external applications.
    """
    return dumper.represent_data(data.value)


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
        dumped["application"] = str(getattr(model, "application"))

    # 'schema_version' records which schema wrote the document; always include
    # it (even at the current default) so persisted artifacts self-describe.
    if hasattr(model, "schema_version"):
        dumped["schema_version"] = str(getattr(model, "schema_version"))

    dumper = yaml.Dumper
    dumper.ignore_aliases = lambda *_args: True  # type: ignore[method-assign]
    dumper.add_representer(set, set_representer)
    # class-specific representers registered above take precedence; this
    # catches enums declared by external applications
    dumper.add_multi_representer(enum.Enum, enum_representer)

    register_representer(PosixPath, path_representer)

    if schema_url := str(dumped.pop("$schema", "")):
        content = yaml.dump(dumped, sort_keys=False)
        content = f"# yaml-language-server: $schema={schema_url}\n{content}"
    else:
        content = yaml.dump(dumped, sort_keys=False)

    return content


def read_document(path: Path | str) -> dict[str, t.Any]:
    """Read a persisted document into a raw mapping, as `deserialize` reads it.

    Parameters
    ----------
    path : Path | str
        The location of the document.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    Returns
    -------
    dict[str, Any]
        The document, without its `$schema` key.
    """
    document = read_yaml_to_raw(Path(path))
    if document:
        document.pop("$schema", None)
    return document


def deserialize(path: Path | str, klass: type[_T]) -> _T:
    """Deserialize a blueprint into a Simulation instance.

    Parameters
    ----------
    path : Path
        The location of the blueprint
    klass : type[_T]
        The type to instantiate.

    Raises
    ------
    FileNotFoundError:
        If the blueprint file does not exist
    ValueError
        If the blueprint cannot be deserialized

    Returns
    -------
    Simulation
        The deserialized Simulation instance.

    """
    if isinstance(path, str):
        path = Path(path)

    if not path.exists():
        msg = f"No file found at path `{path}` to deserialize to `{klass.__name__}`"
        raise FileNotFoundError(errno.ENOENT, msg, path)

    model = klass.model_validate(read_document(path))  # type: ignore

    msg = f"Deserialized {str(path)!r} into: {model}"
    log.trace(msg)

    return model


def anchor_relative_path(text: str, base_dir: Path) -> Path | None:
    """Resolve a relative file path against a directory.

    Parameters
    ----------
    text : str
        The path as authored.
    base_dir : Path
        The directory a relative `text` is relative to.

    Returns
    -------
    Path | None
        The absolute path, or `None` when `text` is not a relative file path: it
        is empty, still holds a `{{placeholder}}`, is a URI (a remote resource, or
        a sentinel such as Forge's `cstar-forge:...` placeholders), or is already
        absolute. Symlinked directories along the way are resolved, but the final
        component is kept as written: a link at the path stays a link, so the
        name the file is staged and referred to by is the one the author chose.
    """
    text = text.strip()
    if not text or PLACEHOLDER_RE.search(text) or urlsplit(text).scheme:
        return None

    path = Path(text).expanduser()
    if path.is_absolute():
        return None

    joined = base_dir / path
    if joined.name in {"", ".", ".."}:
        return joined.resolve()
    return joined.parent.resolve() / joined.name


def _map_locations(node: t.Any, fn: Callable[[str], str]) -> t.Any:
    """Copy a raw mapping, replacing each string value under `LOCATION_KEY`.

    Parameters
    ----------
    node : Any
        The raw mapping (or any nested part of one); mappings and lists are walked.
    fn : Callable[[str], str]
        Produces the replacement for a location value.

    Returns
    -------
    Any
        The copy of `node`.
    """
    if isinstance(node, dict):
        return {
            key: fn(item)
            if key == LOCATION_KEY and isinstance(item, str)
            else _map_locations(item, fn)
            for key, item in node.items()
        }
    if isinstance(node, list):
        return [_map_locations(item, fn) for item in node]
    return node


def anchor_locations(node: t.Any, base_dir: Path, *, strict: bool = True) -> t.Any:
    """Make every relative `location` in a raw blueprint mapping absolute.

    Walks nested mappings and lists, replacing each string value found under the
    key `location` that `anchor_relative_path` anchors with the resolved POSIX path
    next to `base_dir`. Everything else, including absolute, remote and
    placeholder locations, is passed through.

    Parameters
    ----------
    node : Any
        The raw mapping (or any nested part of one) to anchor.
    base_dir : Path
        The directory relative locations are relative to; the blueprint file's.
    strict : bool
        Pass `False` to read a document that is knowingly partial, such as a
        blueprint Forge has persisted before its inputs exist: a location that
        does not exist is then anchored without the check described below.

    Returns
    -------
    Any
        A copy of `node` with its relative locations anchored.

    Raises
    ------
    CstarExpectationFailed
        If `strict` and a relative location does not exist next to `base_dir` but
        does exist relative to the working directory, which is where C-Star
        resolved it before locations were anchored to the blueprint; the message
        lists every such location with both candidate paths.
    """
    stranded: list[tuple[str, Path, Path]] = []

    def anchor(text: str) -> str:
        anchored = anchor_relative_path(text, base_dir)
        if anchored is None:
            return text

        in_cwd = Path.cwd() / text.strip()
        if strict and not anchored.exists() and in_cwd.exists():
            stranded.append((text, anchored, in_cwd))
        return anchored.as_posix()

    anchored_node = _map_locations(node, anchor)
    if stranded:
        found = "\n".join(
            f"  {text!r}: not at {anchored}, but found at {in_cwd}"
            for text, anchored, in_cwd in stranded
        )
        msg = (
            "Relative locations in a blueprint resolve against the directory of "
            f"the blueprint file ({base_dir}), not the working directory "
            f"({Path.cwd()}); these exist only relative to the working "
            f"directory:\n{found}\n"
            "Move the blueprint next to its data or use absolute locations."
        )
        raise CstarExpectationFailed(msg)

    return anchored_node


def relativize_locations(node: t.Any, root: Path, base_dir: Path) -> t.Any:
    """Make every absolute `location` inside a tree relative to a directory.

    The emit-side counterpart of `anchor_locations`: a blueprint written together
    with the files it names keeps working when the tree holding both is moved or
    copied as a unit, because its locations are then relative to the blueprint's
    directory. Only string values under the key `location` that are absolute
    paths lying under `root` are rewritten; everything else (URLs, sentinels,
    paths elsewhere) is passed through. Paths are compared where they physically
    are, with symlinked directories resolved but the final component kept.

    Parameters
    ----------
    node : Any
        The raw mapping (or any nested part of one) to rewrite.
    root : Path
        The directory containing everything that should be addressed relatively.
    base_dir : Path
        The directory the blueprint will be written to.

    Returns
    -------
    Any
        A copy of `node` with the locations under `root` relative to `base_dir`.
    """
    root = root.resolve()
    base_dir = base_dir.resolve()

    def relativize(text: str) -> str:
        path = Path(text)
        if urlsplit(text).scheme or not path.is_absolute():
            return text

        physical = path.parent.resolve() / path.name
        if not physical.is_relative_to(root):
            return text
        return Path(os.path.relpath(physical, base_dir)).as_posix()

    return _map_locations(node, relativize)


def read_blueprint_raw(path: Path | str, *, strict: bool = True) -> dict[str, t.Any]:
    """Read a blueprint file's raw mapping with its relative locations anchored.

    This is the single place a blueprint's `location` values acquire the
    directory of the file they were written in: everything downstream sees
    absolute locations. Relative locations resolve against the directory the
    file really lives in (a symlinked blueprint anchors where its target is,
    as `local_copy` does).

    Parameters
    ----------
    path : Path | str
        The local file holding the blueprint.
    strict : bool
        Whether a relative location that exists only relative to the working
        directory is rejected; see `anchor_locations`.

    Raises
    ------
    FileNotFoundError
        If the blueprint file does not exist.
    CstarExpectationFailed
        If `strict` and a relative location exists only relative to the working
        directory.

    Returns
    -------
    dict[str, t.Any]
    """
    path = Path(path).expanduser().resolve()
    if not path.exists():
        msg = f"No blueprint file found at path `{path}`"
        raise FileNotFoundError(errno.ENOENT, msg, path)

    return anchor_locations(read_document(path), path.parent, strict=strict)


def anchor_blueprint_file(path: Path, base_dir: Path) -> None:
    """Rewrite a blueprint file with its relative locations anchored to a directory.

    For a blueprint downloaded to a temporary copy, which has no directory of its
    own for its relative locations to be relative to: anchoring them to the
    directory they were always read against, the working directory, makes them
    absolute before file-based steps that re-read the copy anchor them again.
    Unlike `anchor_locations` as `load_blueprint` calls it, a location that does
    not exist is anchored too, since leaving it relative would let a later step
    anchor it somewhere else. The document is rewritten as YAML; comments are not
    kept.

    Parameters
    ----------
    path : Path
        The local file holding the blueprint.
    base_dir : Path
        The directory relative locations are relative to.
    """
    raw = read_yaml_to_raw(path)
    if not raw:
        return

    anchored = anchor_locations(raw, base_dir, strict=False)
    text = yaml.safe_dump(anchored, sort_keys=False, default_flow_style=False)
    path.write_text(text, encoding="utf-8")


def load_blueprint(path: Path | str, klass: type[_T], *, anchor: bool = True) -> _T:
    """Load a blueprint file into a model, anchoring its relative locations.

    The counterpart of `deserialize` for blueprints: identical, except that
    relative `location` values resolve against the directory of the blueprint
    file rather than the working directory (see `read_blueprint_raw`).

    Parameters
    ----------
    path : Path | str
        The local file holding the blueprint.
    klass : type[_T]
        The blueprint type to instantiate.
    anchor : bool
        Pass `False` for a blueprint fetched from a URL. It is read from a
        temporary copy, so there is no directory its relative locations could be
        relative to; they are left as authored, as `deserialize` leaves them.

    Raises
    ------
    FileNotFoundError
        If the blueprint file does not exist.
    CstarExpectationFailed
        If a relative location exists only relative to the working directory.

    Returns
    -------
    _T
    """
    if not anchor:
        return deserialize(path, klass)

    model = klass.model_validate(read_blueprint_raw(path))  # type: ignore

    msg = f"Loaded {str(path)!r} into: {model}"
    log.trace(msg)

    return model


def try_deserialize(path: Path | str, klass: type[_T]) -> _T | None:
    try:
        return deserialize(path, klass)
    except Exception:
        msg = f"try-deserialize failed loading {klass.__name__!r} from {str(path)!r}"
        log.debug(msg)
        return None


def rebase_paths(value: t.Any, old_root: str, new_root: str) -> t.Any:
    """Rebase every string under `old_root` in a parsed document onto `new_root`.

    A string is under `old_root` when it equals it or starts with it followed
    by a path separator; a string that merely contains `old_root` is left
    alone. Mapping keys are never rebased.

    Parameters
    ----------
    value : Any
        The parsed document, or a fragment of one.
    old_root : str
        The directory the document's paths were written under.
    new_root : str
        The directory to rebase them onto.

    Returns
    -------
    Any
        A rebased copy of `value`.
    """
    if isinstance(value, str):
        if value == old_root:
            return new_root
        prefix = old_root.rstrip(os.sep) + os.sep
        if value.startswith(prefix):
            return new_root.rstrip(os.sep) + os.sep + value[len(prefix) :]
        return value
    if isinstance(value, dict):
        return {k: rebase_paths(v, old_root, new_root) for k, v in value.items()}
    if isinstance(value, list):
        return [rebase_paths(v, old_root, new_root) for v in value]
    return value


async def read_all(
    paths: Sequence[Path | str],
    read: Callable[[Path], _R | None],
    limit: int,
    sem: asyncio.Semaphore | None = None,
) -> Sequence[_R | None]:
    """Read a collection of files concurrently, in input order.

    Each file is read in a worker thread, so `read` may block. Reads are
    bounded by `sem` when supplied and otherwise by a new semaphore permitting
    `limit` concurrent reads.

    Parameters
    ----------
    paths : Sequence[Path | str]
        The files to read.
    read : Callable[[Path], _R | None]
        Reads one file, returning `None` for one that cannot be read.
    limit : int
        The maximum number of concurrent reads; ignored when `sem` is supplied.
    sem : asyncio.Semaphore | None
        An externally owned semaphore used to bound concurrent reads.

    Returns
    -------
    Sequence[_R | None]
        What `read` returned for each file, in input order.
    """

    async def _bounded(p: Path, s: asyncio.Semaphore) -> _R | None:
        """Read the item with concurrency bounded."""
        async with s:
            return await asyncio.to_thread(read, p)

    sem = sem or asyncio.Semaphore(limit)
    return await asyncio.gather(*(_bounded(Path(p), sem) for p in paths))


async def deserialize_all(
    paths: list[Path],
    klass: type[_T],
    limit: int,
    sem: asyncio.Semaphore | None = None,
) -> Sequence[_T | None]:
    """Deserialize a collection of items of the same type.

    Reads are performed concurrently, bounded by `sem` when supplied and
    otherwise by a new semaphore permitting `limit` concurrent reads.

    Parameters
    ----------
    paths : list[Path]
        The paths to the serialized items.
    klass : type[_T]
        The model type to deserialize each item into.
    limit : int
        The maximum number of concurrent reads; ignored when `sem` is supplied.
    sem : asyncio.Semaphore | None
        An externally owned semaphore used to bound concurrent reads.

    Returns
    -------
    Sequence[_T | None]
        The deserialized items in input order; an item that cannot be read
        or fails validation is returned as `None`.
    """
    return await read_all(paths, lambda p: try_deserialize(p, klass), limit, sem)


def serialize(path: Path, model: SerializableModel) -> int:
    """Serialize a model into a YAML file.

    Parameters
    ----------
    path : Path
        The location to store the serialized model in
    model : _T
        The model to serialize

    Returns
    -------
    int
        The number of bytes written.
    """
    if path.exists():
        msg = f"Overwriting existing file at `{path}`"
        log.debug(msg)

    content = model_to_yaml(model)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open(mode="w") as fp:
        nbytes = fp.write(content)
        fp.flush()

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
        return ValidationResult(f"The {item_type.__name__} is invalid: {ex}", error=ex)
    except CstarSchemaTooNewError as ex:
        return ValidationResult(f"The file at {path} cannot be read: {ex}", error=ex)
    except FileNotFoundError:
        return ValidationResult(f"File not found at path: {path}")

    return ValidationResult(item=item, path=wp_path)
