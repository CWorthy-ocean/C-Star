from enum import Enum
from pathlib import Path
from urllib.parse import urlparse

import charset_normalizer
import requests

from cstar.base.utils import _run_cmd
from cstar.io import (
    LocalBinaryFileStager,
    LocalTextFileStager,
    RemoteBinaryFileStager,
    RemoteRepositoryStager,
    RemoteTextFileStager,
    StagedData,
    Stager,
)


class SourceType(Enum):
    FILE = "file"
    DIRECTORY = "directory"
    REPOSITORY = "repository"


class LocationType(Enum):
    HTTP = "http"
    PATH = "path"


class FileEncoding(Enum):
    TEXT = "text"
    BINARY = "binary"


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
    def _location_as_path(self) -> Path:
        """Return self.location as a Path for parsing"""
        if self.location_type is LocationType.HTTP:
            return Path(urlparse(self.location).path)
        elif self.location_type is LocationType.PATH:
            return Path(self.location)
        raise ValueError(f"Cannot convert location {self.location} to Path")

    @property
    def filename(self) -> str:
        """Get the filename from `location`"""
        return self._location_as_path.name

    @property
    def suffix(self) -> str:
        """Get the extension from `location`"""
        return self._location_as_path.suffix

    @property
    def location_type(self) -> LocationType:
        """Get the location type (e.g. "path" or "url") from the "location"
        attribute.
        """
        urlparsed_location = urlparse(self.location)
        if all([urlparsed_location.scheme, urlparsed_location.netloc]):
            return LocationType.HTTP
        elif Path(self.location).expanduser().exists():
            return LocationType.PATH
        else:
            raise ValueError(
                f"{self.location} is not a recognised URL or local path pointing to an existing file or directory"
            )

    @property
    def _is_repository(self) -> bool:
        """Checks if self.location describes a repository using a git ls-remote subprocess."""
        try:
            _run_cmd(f"git ls-remote {self.location}", raise_on_error=True)
            return True
        except RuntimeError:
            return False

    @property
    def _http_is_html(self) -> bool:
        r = requests.head(self.location, allow_redirects=True, timeout=10)
        content_type = r.headers.get("Content-Type", "").lower()
        return content_type.startswith("text/html")

    @property
    def source_type(self) -> SourceType:
        """Infer source type (file/directory/repository) from 'location'."""
        if self._is_repository:
            return SourceType.REPOSITORY
        elif self.location_type is LocationType.HTTP:
            if (not self._http_is_html) and (self.suffix):
                return SourceType.FILE
        elif self.location_type is LocationType.PATH:
            resolved_path = Path(self.location).resolve()
            if resolved_path.is_file():
                return SourceType.FILE
            elif resolved_path.is_dir():
                return SourceType.DIRECTORY
        raise ValueError(
            f"{self.location} does not appear to point to a valid source type. "
            "Valid source types: \n"
            "\n".join([value.value for value in SourceType])
        )

    @property
    def file_encoding(self) -> FileEncoding | None:
        """Look up file encoding based on source_type."""
        if self.source_type is not SourceType.FILE:
            return None

        def get_header_bytes(location, n_bytes=512) -> bytes:
            if self.location_type is LocationType.HTTP:
                response = requests.get(location, stream=True, allow_redirects=True)
                response.raw.decode_content = True
                header_bytes = response.raw.read(n_bytes)
            elif self.location_type is LocationType.PATH:
                with open(location, "rb") as f:
                    header_bytes = f.read(n_bytes)
            return header_bytes

        best_encoding = charset_normalizer.from_bytes(
            get_header_bytes(self.location)
        ).best()
        if best_encoding:
            return FileEncoding.TEXT
        else:
            return FileEncoding.BINARY

    # Staging logic
    def _select_stager(self) -> "Stager":
        """Logic to determine the correct stager based on the above
        characteristics.
        """
        # Remote stagers
        if self.location_type is LocationType.HTTP:
            if self.source_type is SourceType.REPOSITORY:
                return RemoteRepositoryStager()

            if self.source_type is SourceType.FILE:
                if self.file_encoding is FileEncoding.BINARY:
                    return RemoteBinaryFileStager()
                elif self.file_encoding is FileEncoding.TEXT:
                    return RemoteTextFileStager()

        # Local stagers
        if (self.location_type is LocationType.PATH) and (
            self.source_type is SourceType.FILE
        ):
            if self.file_encoding is FileEncoding.TEXT:
                return LocalTextFileStager()
            elif self.file_encoding is FileEncoding.BINARY:
                return LocalBinaryFileStager()

        raise ValueError(
            f"Unable to determine an appropriate stager for data at {self.location}"
        )

    def get(self, target_dir: str | Path) -> "StagedData":
        return self._stager.stage(target_dir=Path(target_dir), source=self)


class SourceDataCollection:
    pass
