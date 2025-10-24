from collections.abc import Iterable, Iterator, Sequence
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import charset_normalizer
import requests

from cstar.base.gitutils import _get_hash_from_checkout_target, git_location_to_raw
from cstar.base.utils import _run_cmd
from cstar.io.constants import (
    FileEncoding,
    LocationType,
    SourceCharacteristics,
    SourceClassification,
    SourceType,
)
from cstar.io.retriever import get_retriever
from cstar.io.staged_data import StagedData, StagedDataCollection
from cstar.io.stager import get_stager

if TYPE_CHECKING:
    from cstar.io.retriever import Retriever
    from cstar.io.stager import Stager


@lru_cache(maxsize=16)
def get_remote_header(location: str, n_bytes: int) -> bytes:
    """Get the header from a HTTP location, do determine the kind of data at the location."""
    response = requests.get(location, stream=True, allow_redirects=True)
    response.raw.decode_content = True
    header_bytes = response.raw.read(n_bytes)
    return header_bytes


def _location_as_path(location: str | Path) -> Path:
    """Return 'location' as a Path for parsing, whether it is a path, url, or str"""
    return Path(urlparse(str(location)).path)


class _SourceInspector:
    """
    Class for inspecting a location pointing to data and determining the classification of the data.

    Attributes
    ----------
    location (str)
       The location of the data, e.g. a URL or path
    suffix (str)
       The extension associated with 'location'
    location_type (LocationType)
       The type of 'location', e.g. path or URL
    source_type (SourceType)
       The type of source described by location, e.g. repository, directory
    file_encoding (FileEncoding)
       The encoding of the file (if any) described by location, e.g. text, binary
    characteristics (SourceCharacteristics):
        Enum combining source_type, location_type, and file_encoding

    Methods
    -------
    classify() -> SourceClassification:
        The classification of the data at this location, e.g. remote repository
    """

    def __init__(self, location: str | Path):
        """Initialize this _SourceInspector with a location.

        Parameters
        ----------
        location (str or Path):
           The location of the data (a local path or remote address)
        """
        self._location = str(location)
        self._location_type: LocationType | None = None
        self._source_type: SourceType | None = None
        self._file_encoding: FileEncoding | None = None

    @property
    def location(self) -> str:
        """The location associated with this _SourceInspector"""
        return self._location

    @property
    def suffix(self) -> str:
        """Get the extension from `location`"""
        return _location_as_path(self.location).suffix

    @property
    def location_type(self) -> LocationType:
        """Get the location type (e.g. "path" or "url") from the "location"
        attribute.
        """
        if not self._location_type:
            urlparsed_location = urlparse(self.location)
            if all([urlparsed_location.scheme, urlparsed_location.netloc]):
                self._location_type = LocationType.HTTP
            elif _location_as_path(self.location).exists():
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
            if self.suffix == ".git":
                self._source_type = SourceType.REPOSITORY
            elif not self.suffix and self._is_repository:
                self._source_type = SourceType.REPOSITORY
            elif self.location_type is LocationType.HTTP:
                if not self._http_is_html:
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
                + ("\n".join([value.value for value in SourceType]))
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
        """Collect the source type, location type, and file encoding into a single Enum value."""
        return SourceCharacteristics(
            source_type=self.source_type,
            location_type=self.location_type,
            file_encoding=self.file_encoding,
        )

    def classify(self) -> SourceClassification:
        """Determine the classification of the data at 'location'."""
        return SourceClassification(self.characteristics)


class SourceData:
    """Class for obtaining information about and acting on a source of data

    Attributes
    ----------
    location: str
        The location of the data source
    basename: str
        The basename of 'location', e.g. a filename
    identifier: str
        The identifier of the data, e.g. commit hash or checksum
    checkout_target: str or None
        Equivalent to 'identifier' if source is a repository
    checkout hash: str or None
        The hash associated with 'checkout_target', if source is a repository
    file_hash: str or None
        Equivalent to 'identifier' if source is a file
    stager: Stager
        The Stager subclass with which to handle staging of this data

    Methods
    -------
    stage(target_dir: str | Path) -> StagedData
        stages the data, making it available to C-Star
    """

    def __init__(self, location: str | Path, identifier: str | None = None):
        """Initialize a SourceData instance from a location

        Parameters
        ----------
        location (str or Path):
            The location of this data source
        identifier (str, optional, default None):
            The identifier of the data, e.g. commit hash or checksum
        """
        self._location = str(location)
        self._identifier: str | None = identifier
        self._classification: SourceClassification = _SourceInspector(
            location
        ).classify()
        self._stager: Stager | None = None
        self._retriever: Retriever | None = None

    @property
    def location(self) -> str:
        """The location of the data source"""
        return self._location

    @property
    def classification(self) -> SourceClassification:
        """The classification of the data source."""
        return self._classification

    @property
    def basename(self) -> str:
        """The basename of 'location', e.g. a filename"""
        return _location_as_path(self.location).name

    @property
    def identifier(self) -> str | None:
        """The identifier of the data, e.g. commit hash or checksum"""
        return self._identifier

    @property
    def file_hash(self) -> str | None:
        """Equivalent to 'identifier' if source is a file"""
        if self.classification.value.source_type is SourceType.FILE:
            return self.identifier
        return None

    @property
    def checkout_hash(self) -> str | None:
        """Equivalent to 'identifier' if source is a repository"""
        if (self.classification.value.source_type is SourceType.REPOSITORY) and (
            self.checkout_target
        ):
            return _get_hash_from_checkout_target(self.location, self.checkout_target)
        return None

    @property
    def checkout_target(self) -> str | None:
        """Equivalent to 'identifier' if source is a repository"""
        if self.classification.value.source_type is SourceType.REPOSITORY:
            return self.identifier
        return None

    @property
    def stager(self) -> "Stager":
        """The Stager subclass with which to handle staging of this data"""
        if not self._stager:
            self._stager = get_stager(self)
        return self._stager

    @property
    def retriever(self) -> "Retriever":
        if not self._retriever:
            self._retriever = get_retriever(self)
        return self._retriever

    def stage(self, target_dir: str | Path) -> "StagedData":
        """Stages the data, making it available to C-Star"""
        return self.stager.stage(target_dir=Path(target_dir))


class SourceDataCollection:
    """Single class to hold a collection of related SourceData instances"""

    def __init__(self, sources: Iterable[SourceData] = ()):
        """Initialize the SourceDataCollection from a list of SourceData instances"""
        self._sources: list[SourceData] = list(sources)
        self._validate()

    def _validate(self):
        """Confirm that the SourceData instances in this collection are valid"""
        for s in self._sources:
            if s.classification.value.source_type in [
                SourceType.DIRECTORY,
                SourceType.REPOSITORY,
            ]:
                raise TypeError(
                    f"Cannot create SourceDataCollection with data of source type '{s.classification.value.source_type.value}'"
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
        """Return the list of locations associated with this SourceDataCollection"""
        return [s.location for s in self._sources]

    @classmethod
    def from_locations(
        cls,
        locations: Sequence[str | Path],
        identifiers: Sequence[str | None] | None = None,
    ) -> "SourceDataCollection":
        """Create a SourceDataCollection from a list of locations with optional
        corresponding list of identifiers identifier list.

        Parameters
        ----------
        locations: list of str or Path
            The locations of each item
        identifiers: list of str
            The identifiers of each item
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

    @classmethod
    def from_common_location(
        cls,
        common_location: str,
        subdir: str = "",
        checkout_target: str = "",
        files: list[str] = [],
    ):
        common_location_classification = SourceData(common_location).classification
        match common_location_classification:
            case SourceClassification.REMOTE_REPOSITORY:
                return cls.from_locations(
                    locations=[
                        git_location_to_raw(common_location, checkout_target, f, subdir)
                        for f in files
                    ]
                )
            case SourceClassification.LOCAL_DIRECTORY:
                return cls.from_locations(
                    locations=[f"{common_location}/{subdir}/{f}" for f in files]
                )
            case _:
                raise ValueError(
                    f"Cannot create SourceDataCollection from common location with classification {common_location_classification}"
                )

    @property
    def sources(self) -> list[SourceData]:
        """Returns the list of SourceData instances associated with this SourceDataCollection"""
        return self._sources

    def stage(self, target_dir: str | Path) -> StagedDataCollection:
        """Stages each SourceData instance in this collection"""
        staged_data_instances = []
        for s in self.sources:
            staged_data = s.stage(target_dir=target_dir)
            staged_data_instances.append(staged_data)
        return StagedDataCollection(items=staged_data_instances)
