import asyncio
import typing as t
from pathlib import Path

from pydantic import BaseModel

from cstar.execution.file_system import StateDirectoryManager
from cstar.orchestration.serialization import PersistenceMode, deserialize, serialize

EXT_SENTINEL: t.Final[str] = "sentinel"
"""File exension used for persisting sentinels."""

_TStateProxy = t.TypeVar("_TStateProxy", bound=BaseModel)


async def put_sentinel(handle: _TStateProxy, persist_to: Path) -> bool:
    """Store a sentinel file on disk.

    The sentinel indicates that a step has been previously executed.
    """
    if persist_to.exists():
        persist_to.unlink()

    num_bytes = await asyncio.to_thread(
        serialize, persist_to, handle, mode=PersistenceMode.auto
    )
    return num_bytes > 0


async def get_sentinel(
    persist_to: Path,
    klass: type[_TStateProxy],
    *,
    mode: PersistenceMode = PersistenceMode.auto,
) -> _TStateProxy | None:
    """Read a sentinel file from disk.

    Parameters
    ----------
    persist_to : Path
        Path to a sentinel file

    Returns
    -------
    _TStateProxy | None
        Returns the handle loaded from disk if the file exists,
        otherwise `None`
    """
    if persist_to.exists():
        return await asyncio.to_thread(deserialize, persist_to, klass, mode=mode)
    return None


async def list_sentinels(
    persist_to: Path, klass: type[_TStateProxy]
) -> list[_TStateProxy]:
    """Find all sentinel files located in the specified run directory.

    Parameters
    ----------
    persist_to : Path
        The path to any asset in the output directory for the run.
    klass : type[_THandle]
        The type of handles to deserialize

    Returns
    -------
    list[_THandle]
        All previously persisted sentinels
    """
    state_dir = StateDirectoryManager.run_state()
    filter = f"*.{EXT_SENTINEL}"

    coros = [get_sentinel(p, klass) for p in state_dir.rglob(filter)]
    results = await asyncio.gather(*coros)
    return [x for x in results if x]
