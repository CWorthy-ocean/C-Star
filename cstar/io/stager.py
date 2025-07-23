from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from cstar.io import (
        SourceData,
        StagedData,
        StagedFile,
        StagedFileSet,
        StagedRepository,
    )


class Stager(ABC):
    @abstractmethod
    def stage(self, target_path: Path, source: "SourceData") -> "StagedData":
        """Stage this data using an appropriate strategy."""


class RemoteBinaryFileStager(Stager):
    # Used for e.g. a remote netCDF InputDataset
    def stage(self, target_path: Path, source: "SourceData") -> "StagedFile":
        """Stage a remote binary file with hash verification using Pooch."""
        raise NotImplementedError


class RemoteTextFileStager(Stager):
    # Used for e.g. a remote yaml file
    def stage(self, target_path: Path, source: "SourceData") -> "StagedFile":
        """Stage remote text directly using requests."""
        raise NotImplementedError


class LocalBinaryFileStager(Stager):
    # Used for e.g. a local netCDF InputDataset
    def stage(self, target_path: Path, source: "SourceData") -> "StagedFile":
        """Create a local symlink to a binary file on the current filesystem."""
        raise NotImplementedError


class LocalTextFileStager(Stager):
    # Used for e.g. a local yaml file
    def stage(self, target_path: Path, source: "SourceData") -> "StagedFile":
        """Create a local copy of a text file on the current filesystem."""
        raise NotImplementedError


class RemoteRepositoryStager(Stager):
    # Used for e.g. an ExternalCodeBase
    def stage(self, target_path: Path, source: "SourceData") -> "StagedRepository":
        """Clone and checkout a git repository at a given target."""
        raise NotImplementedError


class LocalTextFileSetStager(Stager):
    # Used for e.g. a local AdditionalCode dir
    def stage(self, target_path: Path, source: "SourceData") -> "StagedFileSet":
        """Copy a set of related text files from a location on the current
        filesystem.
        """
        raise NotImplementedError


class RemoteTextFileSetStager(Stager):
    # Used for e.g. AdditionalCode in a repo
    def stage(self, target_path: Path, source: "SourceData") -> "StagedFileSet":
        """Obtain a set of related text files from a remote location."""
        raise NotImplementedError
