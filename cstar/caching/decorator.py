"""The `cached_artifact` decorator: opt-in artifact caching for functions.

A cached function must accept an output-directory parameter (``output_dir``
by default, configurable via ``output_param``) and write **all** of its file
outputs beneath it. The decorator intercepts that parameter:

- On a cache **miss**, the function is invoked with a cache-controlled staging
  directory instead of the caller's directory. Everything it wrote is
  captured into the personal cache atomically, and symlinks to the cached
  files are placed in the caller's directory. Because the staging directory
  starts empty, the captured file set is exhaustive by construction (sidecar
  files included).
- On a cache **hit** (group tier first, then personal), the function body is
  skipped and symlinks to the cached files are placed in the caller's
  directory.
- When caching is bypassed (``--no-cache`` / ``CSTAR_CACHE_DISABLE=1``), the
  function runs unmodified against the caller's directory and nothing is
  recorded.

In every case the wrapper returns a :class:`~cstar.caching.models.CacheHandle`
whose ``paths`` are the files in the caller's directory and whose ``result``
is the function's (restored) return value. Return values are restored on hits
only when they are cheaply reconstructable: paths under the output directory,
or JSON-serializable values. Anything else yields ``result=None`` on hits.

Note that the wrapped function always receives the output directory as a
`pathlib.Path`, regardless of what the caller passed.

Example
-------
>>> @cached_artifact(version="1", label="demo-tiles")
... def generate_tiles(name: str, count: int, output_dir: Path) -> list[Path]:
...     paths = []
...     for i in range(count):
...         path = output_dir / f"tile_{i:03d}.nc"
...         path.write_bytes(expensive_computation(name, i))
...         paths.append(path)
...     return paths
>>> handle = generate_tiles("gulf_of_maine", 3, output_dir=run_dir / "output")
>>> handle.hit, handle.tier, handle.paths, handle.result
"""

import functools
import inspect
import json
import shutil
import typing as t
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path, PurePath

from cstar.base.log import get_logger
from cstar.caching.config import caching_disabled
from cstar.caching.keys import (
    CacheKeyError,
    KeyExtra,
    compute_key,
    function_identity,
)
from cstar.caching.models import (
    MANIFEST_SCHEMA_VERSION,
    PAYLOAD_DIRNAME,
    CacheEntry,
    CacheFileRecord,
    CacheHandle,
    CacheManifest,
    CacheProvenance,
    ReturnKind,
    ReturnSpec,
)
from cstar.caching.store import CacheManager, function_slug, place_symlinks

log = get_logger(__name__)

_P = t.ParamSpec("_P")

_MISSING: t.Final = object()
"""Sentinel distinguishing "no fresh return value" (hit) from `None`."""


def _files_under(directory: Path) -> set[Path]:
    """Return all regular files beneath a directory (empty when absent)."""
    if not directory.is_dir():
        return set()
    return {path for path in directory.rglob("*") if path.is_file()}


def _snapshot_files(directory: Path) -> dict[Path, tuple[int, int]]:
    """Map each file beneath a directory to its (mtime_ns, size) signature."""
    snapshot: dict[Path, tuple[int, int]] = {}
    for path in _files_under(directory):
        stat = path.stat()
        snapshot[path] = (stat.st_mtime_ns, stat.st_size)
    return snapshot


def _unlink_cache_symlinks(directory: Path) -> None:
    """Remove symlinks beneath a directory that point into cache storage.

    A bypassed (``--no-cache``) call writes into the caller's output
    directory directly. If a previous cached run placed symlinks there, the
    function would write *through* them into the cache payload, silently
    corrupting cache entries. Links into either cache tier are removed
    up-front so the bypass writes real files; other symlinks are untouched.
    """
    from cstar.caching.config import (
        group_cache_root,
        personal_cache_root,
    )

    if not directory.is_dir():
        return

    roots = [personal_cache_root()]
    if group_root := group_cache_root():
        roots.append(group_root)

    for path in directory.rglob("*"):
        if not path.is_symlink():
            continue
        try:
            target = path.resolve()
        except OSError:
            continue
        if any(target.is_relative_to(root) for root in roots):
            msg = f"--no-cache: replacing cache symlink with fresh output: {path}"
            log.debug(msg)
            path.unlink()


def _relpath_under(path: t.Any, payload_dir: Path) -> str | None:
    """Return the payload-relative posix path for a value, or `None`.

    `None` is returned when the value is not path-like or does not point
    beneath the payload directory.
    """
    if not isinstance(path, PurePath):
        # plain strings are treated as values, not paths, to avoid
        # misclassifying ordinary string returns
        return None
    resolved = Path(path).expanduser().resolve()
    try:
        return resolved.relative_to(payload_dir.resolve()).as_posix()
    except ValueError:
        return None


def _build_return_spec(raw: t.Any, payload_dir: Path) -> ReturnSpec:
    """Classify a fresh return value into a persisted `ReturnSpec`."""
    if isinstance(raw, PurePath):
        if relpath := _relpath_under(raw, payload_dir):
            return ReturnSpec(kind=ReturnKind.path, relpaths=[relpath])
        return ReturnSpec(kind=ReturnKind.none)

    if isinstance(raw, Sequence) and not isinstance(raw, str | bytes):
        relpaths = [_relpath_under(item, payload_dir) for item in raw]
        if relpaths and all(relpath is not None for relpath in relpaths):
            return ReturnSpec(
                kind=ReturnKind.path_list,
                relpaths=t.cast("list[str]", relpaths),
            )

    if isinstance(raw, Mapping):
        keys = list(raw.keys())
        relpaths = [_relpath_under(value, payload_dir) for value in raw.values()]
        if (
            keys
            and all(isinstance(key, str) for key in keys)
            and all(relpath is not None for relpath in relpaths)
        ):
            return ReturnSpec(
                kind=ReturnKind.path_map,
                relpaths=t.cast("list[str]", relpaths),
                map_keys=keys,
            )

    try:
        serialized = json.dumps(raw)
    except (TypeError, ValueError):
        return ReturnSpec(kind=ReturnKind.none)

    if str(payload_dir) in serialized:
        # the value embeds a path *string* into the staging directory, which
        # is renamed away at commit — persisting it would serve dead paths
        msg = (
            "A cached return value contains a string path into the cache "
            "staging directory; it will not be restored on cache hits. "
            "Return pathlib.Path objects so paths can be rebased."
        )
        log.warning(msg)
        return ReturnSpec(kind=ReturnKind.none)

    # round-trip through JSON so only safe-YAML-representable structures are
    # persisted (a tuple or StrEnum passed through verbatim would be written
    # as a python-tagged YAML node that yaml.safe_load cannot read back,
    # permanently invalidating the entry)
    return ReturnSpec(kind=ReturnKind.json_value, value=json.loads(serialized))


def _restore_return(
    spec: ReturnSpec,
    output_dir: Path,
    function: str,
    raw: t.Any = _MISSING,
) -> t.Any:
    """Reconstruct a return value from a spec.

    On a miss, `raw` carries the fresh return value: it is preferred for the
    value-based kinds, while path-based kinds are rebased onto the output
    directory (the fresh paths point at the now-renamed staging directory).
    """
    if spec.kind == ReturnKind.path:
        return output_dir / spec.relpaths[0]

    if spec.kind == ReturnKind.path_list:
        return [output_dir / relpath for relpath in spec.relpaths]

    if spec.kind == ReturnKind.path_map:
        return {
            key: output_dir / relpath
            for key, relpath in zip(spec.map_keys, spec.relpaths, strict=True)
        }

    if spec.kind == ReturnKind.json_value:
        # the JSON-normalized value is returned on misses too, so hits and
        # misses agree (e.g. a returned tuple is a list in both cases)
        return spec.value

    # ReturnKind.none
    if raw is _MISSING:
        msg = (
            f"The return value of {function!r} is not restorable from cache; "
            "returning None. Return paths under the output directory or a "
            "JSON-serializable value to enable restoration."
        )
        log.warning(msg)
        return None
    return raw


class _CachedCall:
    """Per-invocation state machine shared by the sync and async wrappers."""

    def __init__(
        self,
        fn: Callable[..., t.Any],
        signature: inspect.Signature,
        args: tuple[t.Any, ...],
        kwargs: dict[str, t.Any],
        *,
        version: str,
        label: str,
        key_exclude: Sequence[str],
        key_extra: KeyExtra | None,
        key_by: Mapping[str, Callable[[t.Any], t.Any]] | None,
        output_param: str,
        manager_factory: Callable[[], CacheManager],
    ) -> None:
        self.fn = fn
        self.function = function_identity(fn)
        self.label = label
        self.version = version

        self.bound = signature.bind(*args, **kwargs)
        self.bound.apply_defaults()

        output_value = self.bound.arguments.get(output_param)
        if output_value is None:
            msg = (
                f"{self.function} was called without a value for its output "
                f"directory parameter {output_param!r}, which is required by "
                "@cached_artifact."
            )
            raise TypeError(msg)
        self.output_dir = Path(output_value).expanduser().resolve()
        self.output_param = output_param

        self.bypass = caching_disabled()

        try:
            self.key, self.key_material = compute_key(
                fn,
                args,
                kwargs,
                version=version,
                key_exclude=key_exclude,
                key_extra=key_extra,
                key_by=key_by,
                output_param=output_param,
            )
        except CacheKeyError:
            if not self.bypass:
                raise
            # --no-cache must remain usable even for calls whose arguments
            # cannot be keyed; the key is informational in a bypassed handle
            self.key, self.key_material = "", {}

        self.manager = None if self.bypass else manager_factory()
        self.staging: Path | None = None
        self._bypass_snapshot: dict[Path, tuple[int, int]] = {}

    # -- bypass (--no-cache) ------------------------------------------------

    def begin_bypass(self) -> tuple[tuple[t.Any, ...], dict[str, t.Any]]:
        """Prepare a pass-through call against the caller's output directory."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        _unlink_cache_symlinks(self.output_dir)
        self._bypass_snapshot = _snapshot_files(self.output_dir)
        self.bound.arguments[self.output_param] = self.output_dir
        return self.bound.args, self.bound.kwargs

    def finish_bypass(self, raw: t.Any) -> CacheHandle:
        """Build the handle for a pass-through call."""
        produced = sorted(
            path
            for path, signature in _snapshot_files(self.output_dir).items()
            if self._bypass_snapshot.get(path) != signature
        )
        return CacheHandle(
            key=self.key,
            function=self.function,
            hit=False,
            tier=None,
            paths=produced,
            result=raw,
        )

    # -- cache hit ------------------------------------------------------------

    def try_hit(self) -> CacheHandle | None:
        """Return a handle served from cache, or `None` on a miss."""
        assert self.manager is not None
        entry = self.manager.lookup(function_slug(self.function), self.key)
        if entry is None:
            return None

        msg = (
            f"Cache hit ({entry.tier}) for {self.function!r} "
            f"[key {self.key[:12]}]; skipping execution."
        )
        log.info(msg)
        return self._handle_from_entry(entry, hit=True, raw=_MISSING)

    # -- cache miss -----------------------------------------------------------

    def begin_miss(self) -> tuple[tuple[t.Any, ...], dict[str, t.Any]]:
        """Prepare a call redirected into a personal-cache staging directory."""
        assert self.manager is not None
        self.staging = self.manager.personal.begin_staging(self.key)
        self.bound.arguments[self.output_param] = self.staging / PAYLOAD_DIRNAME
        return self.bound.args, self.bound.kwargs

    def abort_miss(self) -> None:
        """Discard the staging directory after a function failure."""
        if self.staging is not None:
            shutil.rmtree(self.staging, ignore_errors=True)
            self.staging = None

    def finish_miss(self, raw: t.Any) -> CacheHandle:
        """Capture staged outputs into the personal cache and build the handle."""
        assert self.manager is not None
        assert self.staging is not None
        payload_dir = self.staging / PAYLOAD_DIRNAME

        files = sorted(_files_under(payload_dir))
        if not files:
            msg = (
                f"{self.function!r} produced no files under its output "
                "directory; caching the return value only."
            )
            log.warning(msg)

        records = [
            CacheFileRecord(
                relpath=path.relative_to(payload_dir).as_posix(),
                size_bytes=path.stat().st_size,
            )
            for path in files
        ]

        return_spec = _build_return_spec(raw, payload_dir)
        if return_spec.kind == ReturnKind.none and raw is not None:
            msg = (
                f"The return value of {self.function!r} is not restorable from "
                "cache; if it references files it wrote, those paths point at "
                "the staging directory and are invalid after this call."
            )
            log.warning(msg)

        manifest = CacheManifest(
            schema_version=MANIFEST_SCHEMA_VERSION,
            key=self.key,
            function=self.function,
            function_version=self.version,
            label=self.label,
            key_material=self.key_material,
            files=records,
            return_spec=return_spec,
            provenance=CacheProvenance.capture(),
        )

        entry = self.manager.personal.commit(self.staging, manifest)
        self.staging = None

        return self._handle_from_entry(entry, hit=False, raw=raw)

    # -- shared -----------------------------------------------------------------

    def _handle_from_entry(
        self,
        entry: CacheEntry,
        *,
        hit: bool,
        raw: t.Any,
    ) -> CacheHandle:
        links = place_symlinks(entry, self.output_dir)
        result = _restore_return(
            entry.manifest.return_spec,
            self.output_dir,
            self.function,
            raw=raw,
        )
        return CacheHandle(
            key=self.key,
            function=self.function,
            hit=hit,
            tier=entry.tier,
            paths=links,
            payload_paths=entry.payload_paths,
            created_at=entry.manifest.provenance.created_at,
            provenance=entry.manifest.provenance,
            result=result,
        )


def cached_artifact(
    *,
    version: str = "1",
    label: str = "",
    key_exclude: Sequence[str] = (),
    key_extra: KeyExtra | None = None,
    key_by: Mapping[str, Callable[[t.Any], t.Any]] | None = None,
    output_param: str = "output_dir",
    manager_factory: Callable[[], CacheManager] = CacheManager.from_env,
) -> Callable[[Callable[_P, t.Any]], Callable[_P, t.Any]]:
    """Wrap a file-producing function with artifact caching.

    Parameters
    ----------
    version : str
        Developer-managed version of the function's logic. Bump it whenever
        a change alters outputs for identical inputs; this invalidates all
        prior cache entries for the function.
    label : str
        Optional human-friendly label recorded in manifests and usable as a
        reference in the `cstar cache` CLI.
    key_exclude : Sequence[str]
        Argument names that do not affect outputs (e.g. verbosity flags).
    key_extra : Mapping | Callable | None
        Additional key components: a static mapping, or a callable receiving
        the bound arguments and returning a mapping.
    key_by : Mapping[str, Callable] | None
        Per-argument transforms applied before key tokenization (e.g.
        ``{"grid_file": file_fingerprint}`` for content-sensitive keys).
    output_param : str
        Name of the function's output-directory parameter. The function must
        write all of its file outputs beneath this directory, and it receives
        a `pathlib.Path` regardless of what the caller passed.
    manager_factory : Callable[[], CacheManager]
        Factory for the cache manager; overridable for testing.

    Returns
    -------
    Callable
        A decorator producing a wrapper that returns a `CacheHandle` in place
        of the function's raw return value (available as ``handle.result``).

    Raises
    ------
    TypeError
        At decoration time, when the function does not accept `output_param`.
    """

    def decorate(fn: Callable[_P, t.Any]) -> Callable[_P, t.Any]:
        signature = inspect.signature(fn)
        if output_param not in signature.parameters:
            msg = (
                f"@cached_artifact requires {fn.__qualname__!r} to accept an "
                f"output directory parameter named {output_param!r} "
                "(configurable via `output_param`)."
            )
            raise TypeError(msg)

        def build_call(
            args: tuple[t.Any, ...],
            kwargs: dict[str, t.Any],
        ) -> _CachedCall:
            return _CachedCall(
                fn,
                signature,
                args,
                kwargs,
                version=version,
                label=label,
                key_exclude=key_exclude,
                key_extra=key_extra,
                key_by=key_by,
                output_param=output_param,
                manager_factory=manager_factory,
            )

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: _P.args, **kwargs: _P.kwargs) -> CacheHandle:
                call = build_call(args, kwargs)

                if call.bypass:
                    fn_args, fn_kwargs = call.begin_bypass()
                    raw = await fn(*fn_args, **fn_kwargs)
                    return call.finish_bypass(raw)

                if handle := call.try_hit():
                    return handle

                fn_args, fn_kwargs = call.begin_miss()
                try:
                    raw = await fn(*fn_args, **fn_kwargs)
                except BaseException:
                    call.abort_miss()
                    raise
                return call.finish_miss(raw)

            return async_wrapper

        @functools.wraps(fn)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> CacheHandle:
            call = build_call(args, kwargs)

            if call.bypass:
                fn_args, fn_kwargs = call.begin_bypass()
                raw = fn(*fn_args, **fn_kwargs)
                return call.finish_bypass(raw)

            if handle := call.try_hit():
                return handle

            fn_args, fn_kwargs = call.begin_miss()
            try:
                raw = fn(*fn_args, **fn_kwargs)
            except BaseException:
                call.abort_miss()
                raise
            return call.finish_miss(raw)

        return wrapper

    return decorate
