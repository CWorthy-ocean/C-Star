from collections.abc import Iterable, Iterator, Sequence
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

import charset_normalizer
import requests

from cstar.base.gitutils import _get_hash_from_checkout_target
from cstar.base.utils import _run_cmd
from cstar.io.constants import (
    FileEncoding,
    LocationType,
    SourceCharacteristics,
    SourceClassification,
    SourceType,
)
from cstar.io.staged_data import StagedData, StagedDataCollection
from cstar.io.stager import (
    LocalBinaryFileStager,
    LocalTextFileStager,
    RemoteBinaryFileStager,
    RemoteRepositoryStager,
    RemoteTextFileStager,
    Stager,
)


@lru_cache(maxsize=16)
def get_remote_header(location, n_bytes):
    response = requests.get(location, stream=True, allow_redirects=True)
    response.raw.decode_content = True
    header_bytes = response.raw.read(n_bytes)
    return header_bytes


class _SourceInspector:
    def __init__(self, location: str):
        self._location = location
        self._location_type: LocationType | None = None
        self._source_type: SourceType | None = None
        self._file_encoding: FileEncoding | None = None

    @property
    def location(self):
        return self._location

    @property
    def _location_as_path(self) -> Path:
        """Return self.location as a Path for parsing, whether it is a path, url, or str"""
        return Path(urlparse(self.location).path)

    @property
    def basename(self) -> str:
        """Get the basename from `location`"""
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
        if not self._location_type:
            urlparsed_location = urlparse(self.location)
            if all([urlparsed_location.scheme, urlparsed_location.netloc]):
                self._location_type = LocationType.HTTP
            elif self._location_as_path.exists():
                self._location_type = LocationType.PATH
            else:
                raise ValueError(
                    f"{self.location} is not a recognised URL or local path pointing to an existing file or directory"
                )
        return self._location_type

    @property
    def _is_repository(self) -> bool:
        """Checks if self.location describes a repository using a git ls-remote subprocess."""
        try:
            _run_cmd(f"git ls-remote {self.location}", raise_on_error=True)
            return True
        except RuntimeError:
            return False

    @property
    def source_type(self) -> SourceType:
        """Infer source type (file/directory/repository) from 'location'."""
        if not self._source_type:
            if self._is_repository:
                self._source_type = SourceType.REPOSITORY
            elif self.location_type is LocationType.HTTP:
                if (not self._http_is_html) and (self.suffix):
                    self._source_type = SourceType.FILE
            elif self.location_type is LocationType.PATH:
                resolved_path = Path(self.location).resolve()
                if resolved_path.is_file():
                    self._source_type = SourceType.FILE
                elif resolved_path.is_dir():
                    self._source_type = SourceType.DIRECTORY

        if not self._source_type:
            raise ValueError(
                f"{self.location} does not appear to point to a valid source type. "
                "Valid source types: \n"
                "\n".join([value.value for value in SourceType])
            )

        return self._source_type

    @property
    def _http_is_html(self) -> bool:
        """Determine if the location is a HTML page.

        As certain services provide URLs that resemble direct filepaths, but
        redirect to, e.g., login pages, this property queries whether the location
        is or is not HTML.
        """
        r = requests.head(self.location, allow_redirects=True, timeout=10)
        content_type = r.headers.get("Content-Type", "").lower()
        return content_type.startswith("text/html")

    @property
    def file_encoding(self) -> FileEncoding:
        """Look up file encoding based on source_type."""
        if not self._file_encoding:
            if self.source_type is not SourceType.FILE:
                self._file_encoding = FileEncoding.NA
            else:
                n_bytes = 512
                if self.location_type is LocationType.HTTP:
                    header_bytes = get_remote_header(self.location, n_bytes)
                elif self.location_type is LocationType.PATH:
                    with open(self.location, "rb") as f:
                        header_bytes = f.read(n_bytes)
                else:
                    raise ValueError(
                        f"Cannot determine file encoding for location type {self.location_type}"
                    )

                best_encoding = charset_normalizer.from_bytes(header_bytes).best()
                if best_encoding:
                    self._file_encoding = FileEncoding.TEXT
                else:
                    self._file_encoding = FileEncoding.BINARY

        return self._file_encoding

    @property
    def characteristics(self) -> SourceCharacteristics:
        return SourceCharacteristics(
            source_type=self.source_type,
            location_type=self.location_type,
            file_encoding=self.file_encoding,
        )

    def classify(self) -> SourceClassification:
        return SourceClassification(self.characteristics)


class SourceData:
    def __init__(self, location: str | Path, identifier: str | None = None):
        self._location = str(location)
        # NOTE : likely don't need to store _inspector; just use
        # self._classification to access source_type
        self._inspector = _SourceInspector(self._location)
        self._identifier = identifier
        self._classification = self._inspector.classify()
        self._stager = None

    @property
    def location(self) -> str:
        return self._location

    @property
    def basename(self) -> str:
        return self._inspector.basename

    @property
    def identifier(self) -> str | None:
        return self._identifier

    @property
    def checkout_target(self) -> str | None:
        if self._inspector.source_type is SourceType.REPOSITORY:
            return self.identifier
        return None

    @property
    def checkout_hash(self) -> str | None:
        if (self._inspector.source_type is SourceType.REPOSITORY) and (
            self.checkout_target
        ):
            return _get_hash_from_checkout_target(self.location, self.checkout_target)
        return None

    @property
    def file_hash(self) -> str | None:
        if self._inspector.source_type is SourceType.FILE:
            return self.identifier
        return None

    @property
    def stager(self) -> "Stager":
        if not self._stager:
            self._stager = self._select_stager()
        return self._stager

    # Staging logic
    def _select_stager(self) -> "Stager":
        """Logic to determine the correct stager based on the above
        characteristics.
        """
        match self._classification:
            case SourceClassification.REMOTE_REPOSITORY:
                return RemoteRepositoryStager()
            case SourceClassification.REMOTE_BINARY_FILE:
                return RemoteBinaryFileStager()
            case SourceClassification.REMOTE_TEXT_FILE:
                return RemoteTextFileStager()
            case SourceClassification.LOCAL_BINARY_FILE:
                return LocalBinaryFileStager()
            case SourceClassification.LOCAL_TEXT_FILE:
                return LocalTextFileStager()
            case _:
                raise ValueError(
                    f"Unable to determine an appropriate stager for data at {self.location}"
                )

    def stage(self, target_dir: str | Path) -> "StagedData":
        return self.stager.stage(target_dir=Path(target_dir), source=self)


class SourceDataCollection:
    def __init__(self, sources: Iterable[SourceData] = ()):
        self._sources: list[SourceData] = list(sources)
        self._validate()

    def _validate(self):
        for s in self._sources:
            if s.source_type in [SourceType.DIRECTORY, SourceType.REPOSITORY]:
                raise TypeError(
                    f"Cannot create SourceDataCollection with data of source type '{s.source_type.value}'"
                )

    def __len__(self) -> int:
        return len(self._sources)

    def __getitem__(self, idx: int) -> SourceData:
        return self._sources[idx]

    def __iter__(self) -> Iterator[SourceData]:
        return iter(self._sources)

    def append(self, source: SourceData) -> None:
        self._sources.append(source)
        self._validate()

    @property
    def locations(self) -> list[str]:
        return [s.location for s in self._sources]

    @classmethod
    def from_locations(
        cls,
        locations: Sequence[str | Path],
        identifiers: Sequence[str | None] | None = None,
    ) -> "SourceDataCollection":
        """Create a SourceDataCollection from a list of locations with optional
        parallel identifier list.
        """
        n = len(locations)
        identifiers = identifiers or [None] * n

        if not (len(identifiers) == n):
            raise ValueError("Length mismatch between inputs")

        sources = [
            SourceData(location=loc, identifier=idt)
            for loc, idt in zip(locations, identifiers)
        ]
        return cls(sources)

    @property
    def sources(self) -> list[SourceData]:
        return self._sources

    def stage(self, target_dir: str | Path) -> StagedDataCollection:
        staged_data_instances = []
        for s in self.sources:
            staged_data = s.stage(target_dir=target_dir)
            staged_data_instances.append(staged_data)
        return StagedDataCollection(items=staged_data_instances)
