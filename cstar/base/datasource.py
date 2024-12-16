from urllib.parse import urlparse
from pathlib import Path


class DataSource:
    """Holds information on various types of data sources used by C-Star.

    Attributes:
    -----------
    location: str
       The location of the data, e.g. a URL or local path

    Properties:
    -----------
    location_type: str (read-only)
       "url" or "path"
    source_type: str (read only)
       Typically describes file type (e.g. "netcdf") but can also be "repository"
    basename: str (read-only)
       The basename of self.location, typically the file name
    """

    def __init__(self, location: str | Path):
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
        self.location = str(location)

    @property
    def location_type(self) -> str:
        """Get the location type (e.g. "path" or "url") from the "location"
        attribute."""
        urlparsed_location = urlparse(self.location)
        if all([urlparsed_location.scheme, urlparsed_location.netloc]):
            return "url"
        elif Path(self.location).exists():
            return "path"
        else:
            raise ValueError(
                f"{self.location} is not a recognised URL or local path pointing to an existing file or directory"
            )

    @property
    def source_type(self) -> str:
        """Get the source type (e.g. "netcdf") from the "location" attribute."""
        loc = Path(self.location)

        if (loc.suffix.lower() == ".git") or ((loc / ".git").is_dir()):
            # TODO: a remote repository might not have a .git suffix, more advanced handling needed
            return "repository"
        elif loc.is_dir():
            return "directory"
        elif loc.suffix.lower() in {".yaml", ".yml"}:
            return "yaml"
        elif loc.suffix.lower() == ".nc":
            return "netcdf"
        else:
            raise ValueError(
                f"{Path(self.location).suffix} is not a supported file type"
            )

    @property
    def basename(self) -> str:
        """Get the basename (typically a file name) from the location attribute."""
        return Path(self.location).name

    def __str__(self) -> str:
        base_str = f"{self.__class__.__name__}"
        base_str += "\n" + "-" * len(base_str)
        base_str += f"\n location: {self.location}"
        base_str += f"\n basename: {self.basename}"
        base_str += f"\n location type: {self.location_type}"
        base_str += f"\n source type: {self.source_type}"
        return base_str

    def __repr__(self) -> str:
        repr_str = f"{self.__class__.__name__}(location={self.location!r})"
        return repr_str
