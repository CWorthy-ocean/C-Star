import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Sequence

if TYPE_CHECKING:
    from pathlib import Path

    from cstar.retrieval.source_data import SourceData


class RetrievedData(ABC):
    def __init__(self, source: "SourceData"):
        self._source = source

    @property
    def source(self) -> "SourceData":
        """The SourceData describing the source of the retrieved data."""
        return self._source

    @property
    @abstractmethod
    def paths(self) -> list[Path]:
        """The local path(s) of the retrieved data."""

    @property
    @abstractmethod
    def changed_from_source(self) -> bool:
        """Whether the data have been modified since retrieval."""

    @abstractmethod
    def reset(self):
        """Revert to original retrieved state if changed_from_source."""
        pass


class RetrievedFile(RetrievedData):
    def __init__(
        self,
        source: "SourceData",
        path: Path,
        sha256: Optional[str],
        stat: Optional[os.stat_result],
    ):
        super().__init__(source)
        self._path = path
        self._sha256 = sha256
        self._stat = stat

    # Abstract
    @property
    def changed_from_source(self) -> bool:
        """Check cached checksum, filesize, and modification time against current
        values."""
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


class RetrievedFileSet(RetrievedData):
    """Collection of related RetrievedFile instances."""

    def __init__(self, source: "SourceData", files: Sequence[RetrievedFile]):
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


class RetrievedRepository(RetrievedData):
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
