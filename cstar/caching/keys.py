"""Deterministic cache-key computation for artifact-producing functions.

Cache keys are derived from a function's identity (module-qualified name plus
an explicit developer-managed version string) and its call arguments. Keys are:

- deterministic across processes, hosts, and Python sessions
- computable without executing the function
- independent of unstable runtime values (PIDs, timestamps, object ids)

Argument values are converted to JSON-safe *tokens* before hashing so the key
never depends on ``repr`` instability or pickle byte-layout. Unsupported
argument types raise :class:`CacheKeyError` naming the offending argument so
the developer can exclude it (``key_exclude``) or supply a custom
transformation (``key_by``).
"""

import datetime as dt
import enum
import hashlib
import inspect
import json
import typing as t
from collections.abc import Callable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from pathlib import Path, PurePath

from pydantic import BaseModel

JsonValue: t.TypeAlias = t.Any
"""A JSON-safe value produced by :func:`tokenize`."""

KeyExtra: t.TypeAlias = (
    Mapping[str, t.Any] | Callable[[Mapping[str, t.Any]], Mapping[str, t.Any]]
)
"""Additional key components: a static mapping or a callable over bound arguments."""

_FINGERPRINT_SAMPLE_BYTES: t.Final[int] = 1024 * 1024
"""Number of bytes sampled from each end of a file by `file_fingerprint`."""

_AUTO_EXCLUDED_PARAMS: t.Final[tuple[str, ...]] = ("self", "cls")
"""Parameter names always excluded from key computation."""


class CacheKeyError(TypeError):
    """Raised when an argument cannot be deterministically tokenized."""


def file_fingerprint(path: Path | str) -> dict[str, t.Any]:
    """Produce a content-sensitive token for a file argument.

    Samples the first and last 1 MiB plus the total size, so in-place content
    changes are detected without hashing TB-scale files end-to-end. Use as a
    per-argument transform: ``key_by={"grid_file": file_fingerprint}``.

    Parameters
    ----------
    path : Path | str
        The file to fingerprint. Must exist.

    Returns
    -------
    dict[str, t.Any]
        A JSON-safe mapping of path, size, and a sampled sha256 digest.
    """
    resolved = Path(path).expanduser().resolve()
    size = resolved.stat().st_size

    digest = hashlib.sha256()
    digest.update(str(size).encode())
    with resolved.open("rb") as fp:
        digest.update(fp.read(_FINGERPRINT_SAMPLE_BYTES))
        if size > 2 * _FINGERPRINT_SAMPLE_BYTES:
            fp.seek(-_FINGERPRINT_SAMPLE_BYTES, 2)
            digest.update(fp.read(_FINGERPRINT_SAMPLE_BYTES))

    # deliberately excludes the path itself: content-fingerprinted keys stay
    # identical across users/hosts whose copies of the data live at
    # different absolute paths, which is what makes group-cache sharing work
    return {
        "size": size,
        "sha256_sample": digest.hexdigest(),
    }


def tokenize(value: t.Any, arg_name: str = "") -> JsonValue:
    """Convert a value into a deterministic, JSON-safe token.

    Parameters
    ----------
    value : t.Any
        The value to tokenize.
    arg_name : str
        The name of the argument being tokenized, used in error messages.

    Returns
    -------
    JsonValue

    Raises
    ------
    CacheKeyError
        If the value (or a nested element) has no deterministic representation.
    """
    # bool/Enum checks must precede int/str: bool subclasses int, and
    # IntEnum/StrEnum subclass int/str respectively.
    if value is None or isinstance(value, bool):
        return value

    if isinstance(value, enum.Enum):
        # module-qualified so same-named enums in different modules don't collide
        enum_id = f"{type(value).__module__}.{type(value).__qualname__}"
        return {"__enum__": enum_id, "value": tokenize(value.value)}

    if isinstance(value, int | str):
        return value

    if isinstance(value, float):
        return {"__float__": repr(value)}

    if isinstance(value, bytes):
        return {"__bytes_sha256__": hashlib.sha256(value).hexdigest()}

    if isinstance(value, PurePath):
        return str(Path(value).expanduser().resolve())

    if isinstance(value, dt.datetime | dt.date | dt.time):
        # tagged so a datetime never collides with its isoformat string
        return {"__datetime__": value.isoformat()}

    if isinstance(value, dt.timedelta):
        return {"__timedelta__": value.total_seconds()}

    if isinstance(value, BaseModel):
        # python-mode dump: json-mode serializes set fields in hash-randomized
        # iteration order, which would make keys differ across processes.
        # Recursing on python objects routes sets through the sorting branch.
        return {
            "__model__": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": tokenize(value.model_dump(mode="python"), arg_name),
        }

    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                msg = (
                    f"Cannot compute a cache key for argument {arg_name!r}: "
                    f"mapping keys must be strings, found {type(key).__name__!r}."
                )
                raise CacheKeyError(msg)
            result[key] = tokenize(item, arg_name)
        return result

    if isinstance(value, AbstractSet):
        # tagged so a set never collides with the equivalent list; sorted by
        # canonical JSON so iteration (hash-randomization) order is irrelevant
        tokens = [tokenize(item, arg_name) for item in value]
        tokens.sort(key=lambda token: json.dumps(token, sort_keys=True))
        return {"__set__": tokens}

    if isinstance(value, Sequence):
        return [tokenize(item, arg_name) for item in value]

    msg = (
        f"Cannot compute a cache key for argument {arg_name!r} of type "
        f"{type(value).__name__!r}. Exclude it with `key_exclude`, or provide "
        "a deterministic transform via `key_by`."
    )
    raise CacheKeyError(msg)


def function_identity(fn: Callable[..., t.Any]) -> str:
    """Return the module-qualified name identifying a cached function."""
    return f"{fn.__module__}.{fn.__qualname__}"


def compute_key(
    fn: Callable[..., t.Any],
    args: tuple[t.Any, ...],
    kwargs: dict[str, t.Any],
    *,
    version: str,
    key_exclude: Sequence[str] = (),
    key_extra: KeyExtra | None = None,
    key_by: Mapping[str, Callable[[t.Any], t.Any]] | None = None,
    output_param: str = "",
) -> tuple[str, dict[str, t.Any]]:
    """Compute the cache key and its persisted key material for a call.

    Arguments are bound to the function signature with defaults applied, so
    keys are invariant to positional-vs-keyword call style, keyword ordering,
    and explicitly passing a default value.

    Parameters
    ----------
    fn : Callable
        The function being cached.
    args : tuple
        Positional arguments of the call.
    kwargs : dict
        Keyword arguments of the call.
    version : str
        Developer-managed version of the function; bump to invalidate.
    key_exclude : Sequence[str]
        Argument names excluded from the key.
    key_extra : KeyExtra | None
        Additional key components; a mapping, or a callable receiving the
        bound arguments and returning a mapping.
    key_by : Mapping[str, Callable] | None
        Per-argument transforms applied before tokenization.
    output_param : str
        Name of the output-directory parameter; always excluded.

    Returns
    -------
    tuple[str, dict[str, t.Any]]
        The sha256 hex key and the JSON-safe key material it was computed from.
    """
    signature = inspect.signature(fn)
    bound = signature.bind(*args, **kwargs)
    bound.apply_defaults()

    excluded = {*_AUTO_EXCLUDED_PARAMS, *key_exclude}
    if output_param:
        excluded.add(output_param)

    transforms = dict(key_by or {})
    arg_tokens: dict[str, JsonValue] = {}
    for name, value in bound.arguments.items():
        if name in excluded:
            continue

        # a key_by transform on a **kwargs parameter receives the whole mapping
        if transform := transforms.get(name):
            value = transform(value)

        arg_tokens[name] = tokenize(value, name)

    extra_source = (
        key_extra(dict(bound.arguments)) if callable(key_extra) else key_extra
    )
    extra_tokens = {
        name: tokenize(value, f"key_extra[{name}]")
        for name, value in (extra_source or {}).items()
    }

    material: dict[str, t.Any] = {
        "function": function_identity(fn),
        "version": version,
        "args": arg_tokens,
        "extra": extra_tokens,
    }

    canonical = json.dumps(material, sort_keys=True, separators=(",", ":"))
    key = hashlib.sha256(canonical.encode()).hexdigest()

    return key, material
