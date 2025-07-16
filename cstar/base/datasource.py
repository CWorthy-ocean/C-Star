from enum import Enum
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse


class LocationType(Enum):
    URL = "url"
    PATH = "path"


class SourceType(Enum):
    REPOSITORY = "repository"
    DIRECTORY = "directory"
    NETCDF = "netcdf"
    YAML = "yaml"


class DataSource:
    """Holds information on various types of data sources used by C-Star.

    Attributes:
    -----------
    location: str
       The location of the data, e.g. a URL or local path

    Properties:
    -----------
    location_type: LocationType (read-only)
       Enum describing location type (e.g. URL or path)
    source_type: SourceType (read only)
       Enum describing source type (e.g. netCDF, yaml, repository)
    basename: str (read-only)
       The basename of self.location, typically the file name
    """

    def __init__(self, location: str | Path, file_hash: Optional[str] = None):
        """Initialize a DataSource from a location string.

        Parameters:
        -----------
        location: str or Path
           The location of the data, e.g. a URL or local path

        Returns:
        --------
        DataSource
            An initialized DataSource
        """
        self._location = str(location)
        self._file_hash = file_hash

    @property
    def location(self) -> str:
        return self._location

    @property
    def file_hash(self) -> Optional[str]:
        return self._file_hash

    @property
    def location_type(self) -> LocationType:
        """Get the location type (e.g. "path" or "url") from the "location"
        attribute."""
        urlparsed_location = urlparse(self.location)
        if all([urlparsed_location.scheme, urlparsed_location.netloc]):
            return LocationType.URL
        elif Path(self.location).expanduser().exists():
            return LocationType.PATH
        else:
            raise ValueError(
                f"{self.location} is not a recognised URL or local path pointing to an existing file or directory"
            )

    @property
    def source_type(self) -> SourceType:
        """Get the source type (e.g. "netcdf") from the "location" attribute."""
        loc = Path(self.location).expanduser()

        if (loc.suffix.lower() == ".git") or ((loc / ".git").is_dir()):
            # TODO: a remote repository might not have a .git suffix, more advanced handling needed
            return SourceType.REPOSITORY
        elif loc.is_dir():
            return SourceType.DIRECTORY
        elif loc.suffix.lower() in {".yaml", ".yml"}:
            return SourceType.YAML
        elif loc.suffix.lower() == ".nc":
            return SourceType.NETCDF
        else:
            raise ValueError(
                f"{Path(self.location)} does not exist or is not a supported file type"
            )

    @property
    def basename(self) -> str:
        """Get the basename (typically a file name) from the location attribute."""
        return Path(self.location).name

    def __str__(self) -> str:
        base_str = f"{self.__class__.__name__}"
        base_str += "\n" + "-" * len(base_str)
        base_str += f"\n location: {self.location}"
        if self.file_hash is not None:
            base_str += f"\n file hash: {self.file_hash}"
        base_str += f"\n basename: {self.basename}"
        base_str += f"\n location type: {self.location_type.value.lower()}"
        base_str += f"\n source type: {self.source_type.value.lower()}"
        return base_str

    def __repr__(self) -> str:
        repr_str = f"{self.__class__.__name__}(location={self.location!r}"
        if self.file_hash is not None:
            repr_str += f", file_hash={self.file_hash!r}"
        repr_str += ")"

        return repr_str
