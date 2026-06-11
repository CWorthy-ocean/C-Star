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
    repo = StateRepository()
    return await repo.list_sentinels(klass, mode=mode)


class StateRepository:
    """API used to manage storage of state information related to a run.

    Contains standard CRUD operations for sentinel (task) records.
    """

    @classmethod
    def sentinel_name(
        cls,
        proxy: StateProxy | str,
        *,
        mode: PersistenceMode = PersistenceMode.yaml,
    ):
        return f"{proxy}.{EXT_SENTINEL}.{mode.value}"

    @classmethod
    def sentinel_path(
        cls,
        proxy: StateProxy | str,
        *,
        run_id: str | None = None,
        mode: PersistenceMode = PersistenceMode.yaml,
    ) -> Path:
        """Get the path to a sentinel file for a given handle.

        Parameters
        ----------
        proxy : StateProxy | str
            The handle to serialize or the name of the handle/step.
        mode : PersistenceMode
            The persistence mode to use when serializing

        Returns
        -------
        Path
            The path to the sentinel file
        """
        if not isinstance(proxy, str):
            proxy = proxy.safe_name

        state_dir = StateDirectoryManager.run_state_dir(run_id=run_id)
        return state_dir / StateRepository.sentinel_name(proxy, mode=mode)

    async def get_sentinel(
        self,
        proxy: StateProxy | str,
        klass: type[_TStateProxy],
        *,
        mode: PersistenceMode = PersistenceMode.yaml,
    ) -> _TStateProxy | None:
        """Read a sentinel file from disk.

        Parameters
        ----------
        proxy : StateProxy | str
            The handle to serialize or the name of the handle/step.
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
        if not isinstance(proxy, str):
            proxy = proxy.safe_name

        persist_to = StateRepository.sentinel_path(proxy)
        if persist_to.exists():
            return await asyncio.to_thread(deserialize, persist_to, klass, mode=mode)
        return None

    async def put_sentinel(
        self,
        proxy: StateProxy,
        *,
        mode: PersistenceMode = PersistenceMode.yaml,
    ) -> Path | None:
        """Store a sentinel file on disk.

        The sentinel indicates that a step has been previously executed.

        Parameters
        ----------
        proxy : StateProxy
            The handle to serialize
        mode : PersistenceMode
            The persistence mode to use when serializing

        Returns
        -------
        bool
            Whether the sentinel was successfully stored
        """
        persist_to = self.sentinel_path(proxy, mode=mode)

        if persist_to.exists():
            persist_to.unlink()

        num_bytes = await asyncio.to_thread(serialize, persist_to, proxy, mode=mode)
        return persist_to if num_bytes > 0 else None

    async def list_sentinels(
        self,
        klass: type[_TStateProxy],
        *,
        run_id: str | None = None,
        mode: PersistenceMode = PersistenceMode.yaml,
    ) -> list[_TStateProxy]:
        """Find all sentinel files located in the run directory.

        Parameters
        ----------
        mode : PersistenceMode
            The persistence mode to use when deserializing

        Returns
        -------
        list[Path]
        """
        files = await self._list_sentinel_files(run_id=run_id, mode=mode)
        coros = [
            asyncio.to_thread(deserialize, path, klass, mode=mode) for path in files
        ]
        return await asyncio.gather(*coros)

    async def _list_sentinel_files(
        self,
        *,
        run_id: str | None = None,
        mode: PersistenceMode = PersistenceMode.yaml,
    ) -> list[Path]:
        """Find all sentinel files located in the run directory.

        Parameters
        ----------
        mode : PersistenceMode
            The persistence mode to use when deserializing

        Returns
        -------
        list[Path]
        """
        state_dir = StateDirectoryManager.run_state_dir(run_id=run_id)
        pattern = StateRepository.sentinel_name("*", mode=mode)

        return list(state_dir.rglob(pattern))
