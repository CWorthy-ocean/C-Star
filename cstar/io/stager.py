from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from cstar.io import (
        LocalFileRetriever,
        RemoteBinaryFileRetriever,
        RemoteRepositoryRetriever,
        RemoteTextFileRetriever,
        SourceData,
        StagedData,
        StagedFile,
        StagedRepository,
    )


class Stager(ABC):
    @abstractmethod
    def stage(self, target_dir: Path, source: "SourceData") -> "StagedData":
        """Stage this data using an appropriate strategy."""


class RemoteBinaryFileStager(Stager):
    # Used for e.g. a remote netCDF InputDataset
    def stage(self, target_dir: Path, source: "SourceData") -> "StagedFile":
        """Stage a remote binary file."""
        retriever = RemoteBinaryFileRetriever()
        retrieved_path = retriever.save(source=source, target_dir=target_dir)

        return StagedFile(
            source=source,
            path=retrieved_path,
            sha256=(source.identifier or None),
            stat=None,
        )


class RemoteTextFileStager(Stager):
    # Used for e.g. a remote yaml file
    def stage(self, target_dir: Path, source: "SourceData") -> "StagedFile":
        """Stage a remote text file."""
        retriever = RemoteTextFileRetriever()
        retrieved_path = retriever.save(source=source, target_dir=target_dir)

        return StagedFile(
            source=source,
            path=retrieved_path,
            sha256=(source.identifier or None),
            stat=None,
        )


class LocalBinaryFileStager(Stager):
    # Used for e.g. a local netCDF InputDataset
    def stage(self, target_dir: Path, source: "SourceData") -> "StagedFile":
        """Create a local symlink to a binary file on the current filesystem."""
        target_path = target_dir / source.filename
        target_path.symlink_to(source.location)

        return StagedFile(
            source=source, path=target_dir, sha256=(source.identifier or None)
        )


class LocalTextFileStager(Stager):
    # Used for e.g. a local yaml file
    def stage(self, target_dir: Path, source: "SourceData") -> "StagedFile":
        """Create a local copy of a text file on the current filesystem."""
        retriever = LocalFileRetriever()
        retrieved_path = retriever.save(source=source, target_dir=target_dir)

        return StagedFile(
            source=source, path=retrieved_path, sha256=source.identifier or None
        )


class RemoteRepositoryStager(Stager):
    # Used for e.g. an ExternalCodeBase
    def stage(self, target_dir: Path, source: "SourceData") -> "StagedRepository":
        """Clone and checkout a git repository at a given target."""
        retriever = RemoteRepositoryRetriever()
        retrieved_path = retriever.save(source=source, target_dir=target_dir)

        return StagedRepository(source=source, path=retrieved_path)
