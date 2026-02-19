from abc import ABC
from typing import TYPE_CHECKING, ClassVar, cast

from cstar.base.utils import _run_cmd
from cstar.execution.file_system import DirectoryManager
from cstar.io.retriever import RemoteRepositoryRetriever

if TYPE_CHECKING:
    from pathlib import Path

    from cstar.io.source_data import SourceData

from cstar.base.utils import slugify
from cstar.io.constants import SourceClassification
from cstar.io.staged_data import StagedFile, StagedRepository

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
        target_path = target_dir / self.source.basename
        target_path.symlink_to(self.source.location)

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

        Store the result in a local repository cache for re-use.

        Parameters
        ----------
        target_dir : Path
            The local directory in which to stage the repository

        Returns
        -------
        StagedRepository
        """
        cache_path = self._get_cache_path()
        if not cache_path.exists():
            cache_path = self.source.retriever.save(target_dir=cache_path)
        else:
            self.source.retriever.refresh(target_dir=cache_path)

        if target_dir.exists():
            target_dir.rmdir()

        _run_cmd(f"cp -av {cache_path}/ {target_dir}")
        remote = cast("RemoteRepositoryRetriever", self.source.retriever)
        remote.checkout(target_dir=target_dir)

        return StagedRepository(source=self.source, path=target_dir)

    def _get_cache_path(self) -> "Path":
        """Calculate the path where the stager will cache the repository.

        Returns
        -------
        Path
        """
        cache_dir = DirectoryManager.cache_home()
        source_key = slugify(self.source.location)
        return cache_dir / source_key
