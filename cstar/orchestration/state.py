import asyncio
import typing as t
from pathlib import Path

from cstar.execution.file_system import StateDirectoryManager
from cstar.orchestration.serialization import (
    PersistenceMode,
    SerializableModel,
    deserialize,
    serialize,
)

EXT_SENTINEL: t.Final[str] = "sentinel"
"""File exension used for persisting sentinels."""


class StateProxy(SerializableModel, t.Protocol):
    """Protocol defining API required to serialize and deserialize objects
    as a "sentinel" file.
    """

    @property
    def safe_name(self) -> str:
        """Return a path-safe name for a state proxy object"""
        ...


_TStateProxy = t.TypeVar("_TStateProxy", bound=StateProxy)
"""Type variable bounding `StateProxy` to any `SerializableModel` implementing
the `StateProxy` protocol.
"""


def sentinel_path(
    proxy: StateProxy, mode: PersistenceMode = PersistenceMode.yaml
) -> Path:
    """Get the path to a sentinel file for a given handle.

    Parameters
    ----------
    proxy : _TStateProxy
        The handle to serialize
    mode : PersistenceMode
        The persistence mode to use when serializing

    Returns
    -------
    Path
        The path to the sentinel file
    """
    return (
        StateDirectoryManager.run_state_dir()
        / f"{proxy.safe_name}.{EXT_SENTINEL}.{mode.value}"
    )


async def put_sentinel(
    proxy: StateProxy,
    *,
    mode: PersistenceMode = PersistenceMode.yaml,
) -> bool:
    """Store a sentinel file on disk.

    The sentinel indicates that a step has been previously executed.

    Parameters
    ----------
    proxy : _TStateProxy
        The handle to serialize
    mode : PersistenceMode
        The persistence mode to use when serializing

    Returns
    -------
    bool
        Whether the sentinel was successfully stored
    """
    persist_to = sentinel_path(proxy, mode)

    if persist_to.exists():
        persist_to.unlink()

    num_bytes = await asyncio.to_thread(serialize, persist_to, proxy, mode=mode)
    return num_bytes > 0


async def get_sentinel(
    persist_to: Path,
    klass: type[_TStateProxy],
    *,
    mode: PersistenceMode = PersistenceMode.yaml,
) -> _TStateProxy | None:
    """Read a sentinel file from disk.

    Parameters
    ----------
    persist_to : Path
        Path to a sentinel file
    klass : type[_TStateProxy]
        The type of handles to deserialize
    mode : PersistenceMode
        The persistence mode to use when deserializing

    Returns
    -------
    _TStateProxy | None
        Returns the handle loaded from disk if the file exists,
        otherwise `None`
    """
    if persist_to.exists():
        return await asyncio.to_thread(deserialize, persist_to, klass, mode=mode)
    return None


def find_sentinels(
    *,
    mode: PersistenceMode = PersistenceMode.yaml,
) -> t.Iterable[Path]:
    """Find all sentinel files located in the run directory.

    Parameters
    ----------
    mode : PersistenceMode
        The persistence mode to use when deserializing

    Returns
    -------
    list[Path]
    """
    state_dir = StateDirectoryManager.run_state_dir()
    filter = f"*.{EXT_SENTINEL}.{mode.value}"

    yield from state_dir.rglob(filter)


async def load_sentinels(
    klass: type[_TStateProxy],
    *,
    mode: PersistenceMode = PersistenceMode.yaml,
) -> list[_TStateProxy]:
    """Load all sentinel files located in the run directory.

    Parameters
    ----------
    klass : type[_TStateProxy]
        The type of handles to deserialize
    mode : PersistenceMode
        The persistence mode to use when deserializing

    Returns
    -------
    list[_TStateProxy]
        All previously persisted sentinels
    """
    sentinel_paths = find_sentinels(mode=mode)

    coros = [get_sentinel(p, klass, mode=mode) for p in sentinel_paths]
    results = await asyncio.gather(*coros)
    return [x for x in results if x]
