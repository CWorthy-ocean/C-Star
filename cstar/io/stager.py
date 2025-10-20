from abc import ABC
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pathlib import Path

    from cstar.io.source_data import SourceData
from cstar.io.constants import SourceClassification
from cstar.io.retriever import Retriever, get_retriever
from cstar.io.staged_data import StagedFile, StagedRepository

_registry: dict[SourceClassification, type["Stager"]] = {}


def register_stager(
    wrapped_cls: type["Stager"],
) -> type["Stager"]:
    """Decorator that registers the decorated type as an available Stager"""
    _registry[wrapped_cls._classification] = wrapped_cls

    return wrapped_cls


def get_stager(classification: SourceClassification) -> "Stager":
    """Retrieve a Stager from the registry given a source classification.

    Parameters
    ----------
    classification (SourceClassification):
       The classification of the data for which to retrieve the stager

    Returns
    -------
    Stager
        The stager matching the source classification

    Raises
    ------
    ValueError
        if no registered stager is associated with this classification
    """
    if stager := _registry.get(classification):
        return stager()
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

    def stage(
        self, target_dir: "Path", source: "SourceData"
    ) -> StagedFile | StagedRepository:
        """Stage this data using an appropriate strategy."""
        retrieved_path = self.retriever.save(source=source, target_dir=target_dir)
        return StagedFile(
            source=source, path=retrieved_path, sha256=source.file_hash, stat=None
        )

    @property
    def retriever(self) -> Retriever:
        return get_retriever(self._classification)


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
    def stage(self, target_dir: "Path", source: "SourceData") -> "StagedFile":
        """Create a local symlink to a binary file on the current filesystem.

        Parameters
        ----------
        target_dir, Path:
            The local directory in which to stage the file
        source, SourceData:
            The SourceData instance tracking the file's source
        """
        target_path = target_dir / source.basename
        target_path.symlink_to(source.location)

        return StagedFile(
            source=source, path=target_path, sha256=(source.file_hash or None)
        )


@register_stager
class LocalTextFileStager(Stager):
    _classification = SourceClassification.LOCAL_TEXT_FILE


@register_stager
class RemoteRepositoryStager(Stager):
    _classification = SourceClassification.REMOTE_REPOSITORY

    # Used for e.g. an ExternalCodeBase
    def stage(self, target_dir: "Path", source: "SourceData") -> "StagedRepository":
        """Clone and checkout a git repository at a given target.

        Parameters
        ----------
        target_dir, Path:
            The local directory in which to stage the repository
        source, SourceData:
            The SourceData instance tracking the repository's source
        """
        retrieved_path = self.retriever.save(source=source, target_dir=target_dir)
        return StagedRepository(source=source, path=retrieved_path)
