import asyncio
import typing as t
from pathlib import Path

from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.execution.file_system import (
    LockTimeoutError,
    StateDirectoryManager,
    locked_if_supported,
    write_text_atomic,
)
from cstar.orchestration.serialization import (
    SerializableModel,
    deserialize,
    model_to_yaml,
)

log = get_logger(__name__)

EXT_SENTINEL: t.Final[str] = "sentinel"
"""File exension used for persisting sentinels."""

SENTINEL_LOCK_TIMEOUT: t.Final[float] = 60.0
"""Seconds a sentinel writer waits for another to finish before writing anyway."""


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


async def load_sentinels(
    klass: type[_TStateProxy],
) -> list[_TStateProxy]:
    """Load all sentinel files located in the run directory.

    Parameters
    ----------
    klass : type[_TStateProxy]
        The type of handles to deserialize

    Returns
    -------
    list[_TStateProxy]
        All previously persisted sentinels
    """
    repo = StateRepository()
    return await repo.list_sentinels(klass)


def _write_sentinel(path: Path, content: str) -> None:
    """Write a sentinel under its lock, or without it after `SENTINEL_LOCK_TIMEOUT`.

    The write goes beside the sentinel and is renamed into place: the local
    proxy script rewrites the sentinel's status line the moment it starts, and a
    reader that caught an in-place write mid-way would copy a truncated sentinel
    back over the complete one. The rename also makes writing without the lock
    safe.

    Parameters
    ----------
    path : Path
        The sentinel file.
    content : str
        The serialized handle.
    """
    # no fsync: the sentinel is rewritten on every status change
    try:
        with locked_if_supported(
            path.with_suffix(".lock"), timeout=SENTINEL_LOCK_TIMEOUT
        ):
            write_text_atomic(path, content, sync=False)
    except LockTimeoutError as ex:
        log.warning(f"{ex}; writing the sentinel {path} without it")
        write_text_atomic(path, content, sync=False)


class StateRepository:
    """API used to manage storage of state information related to a run.

    Contains standard CRUD operations for sentinel (task) records.
    """

    @classmethod
    def sentinel_name(
        cls,
        proxy: StateProxy | str,
    ):
        return f"{proxy}.{EXT_SENTINEL}.yaml"

    @classmethod
    def sentinel_path(
        cls,
        proxy: StateProxy | str,
        *,
        run_id: str | None = None,
    ) -> Path:
        """Get the path to a sentinel file for a given handle.

        Parameters
        ----------
        proxy : StateProxy | str
            The handle to serialize or the name of the handle/step.

        Returns
        -------
        Path
            The path to the sentinel file
        """
        if isinstance(proxy, str):
            proxy = slugify(proxy)
        else:
            proxy = proxy.safe_name

        state_dir = StateDirectoryManager.run_state_dir(run_id=run_id)
        return state_dir / StateRepository.sentinel_name(proxy)

    async def get_sentinel(
        self,
        proxy: StateProxy | str,
        klass: type[_TStateProxy],
    ) -> _TStateProxy | None:
        """Read a sentinel file from disk.

        Parameters
        ----------
        proxy : StateProxy | str
            The handle to serialize or the name of the handle/step.
        klass : type[_TStateProxy]
            The type of handles to deserialize

        Returns
        -------
        _TStateProxy | None
            Returns the handle loaded from disk if the file exists,
            otherwise `None`
        """
        if not isinstance(proxy, str):
            proxy = proxy.safe_name

        persist_to = StateRepository.sentinel_path(proxy)
        if persist_to.exists():
            return await asyncio.to_thread(deserialize, persist_to, klass)
        return None

    async def put_sentinel(
        self,
        proxy: StateProxy,
    ) -> Path | None:
        """Store a sentinel file on disk.

        The sentinel indicates that a step has been previously executed.

        Parameters
        ----------
        proxy : StateProxy
            The handle to serialize

        Returns
        -------
        bool
            Whether the sentinel was successfully stored
        """
        persist_to = self.sentinel_path(proxy)
        content = model_to_yaml(proxy)
        await asyncio.to_thread(_write_sentinel, persist_to, content)
        return persist_to if content else None

    async def list_sentinels(
        self,
        klass: type[_TStateProxy],
        *,
        run_id: str | None = None,
    ) -> list[_TStateProxy]:
        """Find all sentinel files located in the run directory.

        Parameters
        ----------
        run_id : str | None
            The run whose state directory is searched; the current run when `None`.

        Returns
        -------
        list[Path]
        """
        files = await self._list_sentinel_files(run_id=run_id)
        coros = [asyncio.to_thread(deserialize, path, klass) for path in files]
        return await asyncio.gather(*coros)

    async def has_sentinels(self, *, run_id: str) -> bool:
        """Report whether a run's state directory holds any sentinel.

        Parameters
        ----------
        run_id : str
            The run whose state directory is searched.

        Returns
        -------
        bool
        """
        return bool(await self._list_sentinel_files(run_id=run_id))

    async def _list_sentinel_files(
        self,
        *,
        run_id: str | None = None,
    ) -> list[Path]:
        """Find all sentinel files located in the run directory.

        Parameters
        ----------
        run_id : str | None
            The run whose state directory is searched; the current run when `None`.

        Returns
        -------
        list[Path]
        """
        state_dir = StateDirectoryManager.run_state_dir(run_id=run_id)
        pattern = StateRepository.sentinel_name("*")

        return list(state_dir.rglob(pattern))
