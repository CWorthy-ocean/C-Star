import os
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from cstar.io.source_data import SourceData


class StagedData(ABC):
    def __init__(self, source: "SourceData"):
        self._source = source

    @property
    def source(self) -> "SourceData":
        """The SourceData describing the source of the staged data."""
        return self._source

    @property
    @abstractmethod
    def paths(self) -> list[Path]:
        """The local path(s) of the staged data."""

    @property
    @abstractmethod
    def changed_from_source(self) -> bool:
        """Whether the data have been modified since staging."""

    @abstractmethod
    def reset(self):
        """Revert to original staged state if changed_from_source."""
        pass


class StagedFile(StagedData):
    def __init__(
        self,
        source: "SourceData",
        path: Path,
        sha256: str | None,
        stat: os.stat_result | None,
    ):
        super().__init__(source)
        self._path = path
        self._sha256 = sha256
        self._stat = stat

    # Abstract
    @property
    def changed_from_source(self) -> bool:
        """Check cached checksum, filesize, and modification time against current
        values.
        """
        if not self._path.exists():
            return True
        # etc.
        return False

    def reset(self):
        """Re-download this file if change_from_source."""
        pass

    @property
    def paths(self) -> list[Path]:
        return [
            self._path,
        ]

    # Additional
    @property
    def path(self) -> Path:
        """Semantic shortcut to 'paths' for user expecting a singular value."""
        return self._path


class StagedFileSet(StagedData):
    """Collection of related StagedFile instances."""

    def __init__(self, source: "SourceData", files: Sequence[StagedFile]):
        super().__init__(source)
        self._files = list(files)

    @property
    def paths(self) -> list[Path]:
        return [f.path for f in self._files]

    @property
    def changed_from_source(self) -> bool:
        return any(f.changed_from_source for f in self._files)

    def reset(self):
        for f in self._files:
            f.reset()


class StagedRepository(StagedData):
    def __init__(self, source: "SourceData", path: Path):
        super().__init__(source)
        self._source = source
        self._path = path

    # Abstract
    @property
    def paths(self) -> list[Path]:
        return [
            self._path,
        ]

    @property
    def changed_from_source(self) -> bool:
        """Check if dirty with a git subprocess."""
        if not self._path.exists():
            return True
        # etc.
        return False

    def reset(self):
        """Hard reset back to original checkout target."""

    # Additional
    @property
    def path(self) -> Path:
        """Semantic shortcut to 'paths' for user expecting a singular value."""
        return self._path
