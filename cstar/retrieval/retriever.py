from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from cstar.retrieval import (
        RetrievedData,
        RetrievedFile,
        RetrievedFileSet,
        RetrievedRepository,
        SourceData,
    )


class Retriever(ABC):
    @abstractmethod
    def retrieve(self, target_path: Path, source: "SourceData") -> "RetrievedData":
        """Retrieve this data using an appropriate strategy."""


class RemoteBinaryFileRetriever(Retriever):
    # Used for e.g. a remote netCDF InputDataset
    def retrieve(self, target_path: Path, source: "SourceData") -> "RetrievedFile":
        """Retrieve a remote binary file with hash verification using Pooch."""
        raise NotImplementedError


class RemoteTextFileRetriever(Retriever):
    # Used for e.g. a remote yaml file
    def retrieve(self, target_path: Path, source: "SourceData") -> "RetrievedFile":
        """Retrieve remote text directly using requests."""
        raise NotImplementedError


class LocalBinaryFileRetriever(Retriever):
    # Used for e.g. a local netCDF InputDataset
    def retrieve(self, target_path: Path, source: "SourceData") -> "RetrievedFile":
        """Create a local symlink to a binary file on the current filesystem."""
        raise NotImplementedError


class LocalTextFileRetriever(Retriever):
    # Used for e.g. a local yaml file
    def retrieve(self, target_path: Path, source: "SourceData") -> "RetrievedFile":
        """Create a local copy of a text file on the current filesystem."""
        raise NotImplementedError


class RemoteRepositoryRetriever(Retriever):
    # Used for e.g. an ExternalCodeBase
    def retrieve(
        self, target_path: Path, source: "SourceData"
    ) -> "RetrievedRepository":
        """Clone and checkout a git repository at a given target."""
        raise NotImplementedError


class LocalTextFileSetRetriever(Retriever):
    # Used for e.g. a local AdditionalCode dir
    def retrieve(self, target_path: Path, source: "SourceData") -> "RetrievedFileSet":
        """Copy a set of related text files from a location on the current
        filesystem."""
        raise NotImplementedError


class RemoteTextFileSetRetriever(Retriever):
    # Used for e.g. AdditionalCode in a repo
    def retrieve(self, target_path: Path, source: "SourceData") -> "RetrievedFileSet":
        """Obtain a set of related text files from a remote location."""
        raise NotImplementedError
