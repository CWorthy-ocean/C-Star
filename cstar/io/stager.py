import shutil
from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Final, cast

from cstar.base.log import get_logger
from cstar.base.utils import _run_cmd, slugify
from cstar.execution.file_system import (
    DirectoryManager,
    LockTimeoutError,
    LockUnsupportedError,
    locked,
    symlink_relative,
)
from cstar.io.constants import SourceClassification
from cstar.io.staged_data import StagedFile, StagedRepository

if TYPE_CHECKING:
    from cstar.io.retriever import RemoteRepositoryRetriever
    from cstar.io.source_data import SourceData

log = get_logger(__name__)

CACHE_LOCK_TIMEOUT: Final[float] = 30 * 60.0
"""Seconds a step waits for another step to finish with a repository cache before
cloning the repository straight into its own directory instead."""

_registry: dict[SourceClassification, type["Stager"]] = {}


def register_stager(
    wrapped_cls: type["Stager"],
) -> type["Stager"]:
    """Decorator that registers the decorated type as an available Stager"""
    _registry[wrapped_cls._classification] = wrapped_cls

    return wrapped_cls


def get_stager(source: "SourceData") -> "Stager":
    """Retrieve a Stager from the registry given a SourceData instance.

    Parameters
    ----------
    source (SourceData):
       The data for which to retrieve the stager

    Returns
    -------
    Stager
        The stager matching the source classification

    Raises
    ------
    ValueError
        if no registered stager is associated with this classification
    """
    classification = source.classification
    if stager := _registry.get(classification):
        return stager(source=source)
    raise ValueError(f"No stager for {classification}")


class Stager(ABC):
    """Class to handle the staging of data on the local filesystem for access by C-Star.

    Attributes
    ----------
    retriever:
       The Retriever associated with this Stager, for obtaining data before staging it

    Methods
    -------
    stage:
       Stages the data in a chosen local directory
    """

    _classification: ClassVar[SourceClassification]

    def __init__(self, source: "SourceData"):
        self.source = source

    def stage(self, target_dir: "Path") -> StagedFile | StagedRepository:
        """Stage this data using an appropriate strategy."""
        retrieved_path = self.source.retriever.save(target_dir=target_dir)
        return StagedFile(
            source=self.source,
            path=retrieved_path,
            sha256=self.source.file_hash,
            stat=None,
        )


@register_stager
class RemoteBinaryFileStager(Stager):
    _classification = SourceClassification.REMOTE_BINARY_FILE


@register_stager
class RemoteTextFileStager(Stager):
    _classification = SourceClassification.REMOTE_TEXT_FILE


@register_stager
class LocalBinaryFileStager(Stager):
    _classification = SourceClassification.LOCAL_BINARY_FILE

    # Used for e.g. a local netCDF InputDataset
    def stage(self, target_dir: "Path") -> "StagedFile":
        """Create a local symlink to a binary file on the current filesystem.

        Parameters
        ----------
        target_dir, Path:
            The local directory in which to stage the file
        """
        # symlink_to succeeds even when the target is absent, and the model would
        # then fail much later with an opaque I/O error; check up front instead.
        source_path = Path(self.source.location)
        if not source_path.exists():
            msg = f"Cannot stage input dataset: file does not exist: {source_path}"
            raise FileNotFoundError(msg)

        target_path = target_dir / self.source.basename
        symlink_relative(
            target_path, source_path.resolve(), within=DirectoryManager.data_home()
        )

        return StagedFile(
            source=self.source, path=target_path, sha256=(self.source.file_hash or None)
        )


@register_stager
class LocalTextFileStager(Stager):
    _classification = SourceClassification.LOCAL_TEXT_FILE


class RemoteRepositoryStager(Stager):
    _classification = SourceClassification.REMOTE_REPOSITORY

    # Used for e.g. an ExternalCodeBase
    def stage(self, target_dir: "Path") -> "StagedRepository":
        """Clone and checkout a git repository at a given target.

        Parameters
        ----------
        target_dir : Path
            The local directory in which to stage the repository
        """
        retrieved_path = self.source.retriever.save(target_dir=target_dir)
        retriever = cast("RemoteRepositoryRetriever", self.source.retriever)
        retriever.checkout(target_dir=target_dir)
        return StagedRepository(source=self.source, path=retrieved_path)


@register_stager
class CachedRemoteRepositoryStager(Stager):
    _classification = SourceClassification.REMOTE_REPOSITORY

    # Used for e.g. an ExternalCodeBase
    def stage(self, target_dir: "Path") -> "StagedRepository":
        """Clone and checkout a git repository at a given target.

        Store the result in a local repository cache for re-use. Refreshing the
        cache and copying it to `target_dir` hold an exclusive lock on a file
        beside the cache, so steps staging the same repository at once take turns.
        A step still refused the lock after `CACHE_LOCK_TIMEOUT` seconds clones the
        repository straight into `target_dir` and leaves the cache alone. On a file
        system without `flock` support the cache is used without the lock, with a
        warning.

        Parameters
        ----------
        target_dir : Path
            The local directory in which to stage the repository

        Returns
        -------
        StagedRepository
        """
        cache_path = self._get_cache_path()
        lock_path = cache_path.parent / f"{cache_path.name}.lock"
        try:
            with locked(lock_path, timeout=CACHE_LOCK_TIMEOUT):
                self._copy_from_cache(cache_path, target_dir)
        except LockUnsupportedError:
            log.warning(
                "File locking is not supported at %s; using the repository cache "
                "without a lock",
                lock_path,
            )
            self._copy_from_cache(cache_path, target_dir)
        except LockTimeoutError:
            log.warning(
                "The repository cache %s stayed locked for %.0f s; cloning %s "
                "straight into %s instead",
                cache_path,
                CACHE_LOCK_TIMEOUT,
                self.source.location,
                target_dir,
            )
            self.source.retriever.save(target_dir=target_dir)

        remote = cast("RemoteRepositoryRetriever", self.source.retriever)
        remote.checkout(target_dir=target_dir)

        return StagedRepository(source=self.source, path=target_dir)

    def _copy_from_cache(self, cache_path: "Path", target_dir: "Path") -> None:
        """Bring the repository cache up to date and copy it to `target_dir`.

        Parameters
        ----------
        cache_path : Path
            The repository cache, cloned here when it does not exist yet
        target_dir : Path
            The local directory in which to stage the repository
        """
        if not cache_path.exists():
            cache_path = self.source.retriever.save(target_dir=cache_path)
        elif not self.source.retriever.refresh(target_dir=cache_path):
            # if cache cannot be refreshed, it may be incomplete or altered
            shutil.rmtree(cache_path)
            cache_path = self.source.retriever.save(target_dir=cache_path)

        if target_dir.exists():
            target_dir.rmdir()

        _run_cmd(f"cp -av {cache_path}/ {target_dir}")

    def _get_cache_path(self) -> "Path":
        """Calculate the path where the stager will cache the repository.

        Returns
        -------
        Path
        """
        cache_dir = DirectoryManager.cache_home()
        source_key = slugify(self.source.location)
        return cache_dir / source_key
