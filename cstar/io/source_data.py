from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cstar.io import StagedData, Stager


class SourceType(Enum):
    FILE = "file"
    DIRECTORY = "directory"
    REPOSITORY = "repository"


class LocationType(Enum):
    URL = "url"
    PATH = "path"


class FileType(Enum):
    YAML = "yaml"
    NETCDF = "netcdf"


class FileEncoding(Enum):
    TEXT = "text"
    BINARY = "binary"


FILE_ENCODING_LOOKUP: dict[FileType, FileEncoding] = {
    FileType.YAML: FileEncoding.TEXT,
    FileType.NETCDF: FileEncoding.BINARY,
}


class SourceData:
    def __init__(
        self, location: str | Path, file_hash: str | None, checkout_target: str | None
    ):
        self._location = str(location)
        self._file_hash = file_hash
        self._checkout_target = checkout_target
        self._stager = self._select_stager()

    # non-public attrs:
    @property
    def location(self) -> str:
        return self._location

    @property
    def file_hash(self) -> str | None:
        return self._file_hash

    @property
    def checkout_target(self) -> str | None:
        return self._checkout_target

    # Inferred data characteristics
    @property
    def location_type(self) -> LocationType:
        """Determine type of location (path or URL) from 'location'."""
        raise NotImplementedError

    @property
    def source_type(self) -> SourceType:
        """Infer source type (file/directory/repository) from 'location'."""
        raise NotImplementedError

    @property
    def file_type(self) -> FileType | None:
        """Infer file type from 'location'."""
        raise NotImplementedError

    @property
    def file_encoding(self) -> FileEncoding | None:
        """Look up file encoding based on source_type."""
        return (
            FILE_ENCODING_LOOKUP.get(self.file_type, None) if self.file_type else None
        )

    # Staging logic
    def _select_stager(self) -> "Stager":
        """Logic to determine the correct stager based on the above
        characteristics.
        """
        raise NotImplementedError

    def get(self, target_path: str | Path) -> "StagedData":
        return self._stager.stage(target_path=Path(target_path), source=self)
