import pooch
from abc import ABC
import datetime as dt
import dateutil.parser
from pathlib import Path
from urllib.parse import urljoin
from cstar.base.datasource import DataSource
from typing import Optional, List


class InputDataset(ABC):
    """Describes spatiotemporal data needed to run a unique instance of a model
    component.

    Attributes:
    -----------
    source: DataSource
        Describes the location and type of the source data
    file_hash: str, default None
        The 256 bit SHA sum associated with a (remote) file for verifying downloads
    working_path: Path or list of Paths, default None
        The path(s) where the input dataset is being worked with locally, set when `get()` is called.

    Methods:
    --------
    get(local_dir)
        Fetch the file containing this input dataset and save it to `local_dir`
    """

    def __init__(
        self,
        location: str,
        file_hash: Optional[str] = None,
        start_date: Optional[str | dt.datetime] = None,
        end_date: Optional[str | dt.datetime] = None,
    ):
        """Initialize an InputDataset object associated with a model component using a
        source URL and file hash.

        Parameters:
        -----------
        location: str
            URL or path pointing to a file either containing this dataset or instructions for creating it.
            Used to set the `source` attribute.
        file_hash: str, optional
            The 256 bit SHA sum associated with the file for verification if remote
        """

        self.source: DataSource = DataSource(location)
        self.file_hash: Optional[str] = file_hash
        self.working_path: Optional[Path | List[Path]] = None

        if (self.source.location_type == "url") and (self.file_hash is None):
            raise ValueError(
                f"Cannot create InputDataset for \n {self.source.location}:\n "
                + "InputDataset.file_hash cannot be None if InputDataset.source.location_type is 'url'.\n"
                + "A file hash is required to verify files downloaded from remote sources."
            )
        if isinstance(start_date, str):
            start_date = dateutil.parser.parse(start_date)
        self.start_date = start_date
        if isinstance(end_date, str):
            end_date = dateutil.parser.parse(end_date)
        self.end_date = end_date
        assert self.start_date is None or isinstance(self.start_date, dt.datetime)
        assert self.end_date is None or isinstance(self.end_date, dt.datetime)

    @property
    def exists_locally(self) -> bool:
        if self.working_path is None:
            return False
        elif isinstance(self.working_path, list):
            return True if all([f.exists() for f in self.working_path]) else False
        elif isinstance(self.working_path, Path):
            return self.working_path.exists()

    def __str__(self) -> str:
        name = self.__class__.__name__
        base_str = f"{name}"
        base_str = "-" * len(name) + "\n" + base_str
        base_str += "\n" + "-" * len(name)

        base_str += f"\nSource location: {self.source.location}"
        if self.file_hash is not None:
            base_str += f"\nfile_hash: {self.file_hash}"
        if self.start_date is not None:
            base_str += f"\nstart_date: {self.start_date}"
        if self.end_date is not None:
            base_str += f"\nend_date: {self.end_date}"
        base_str += f"\nWorking path: {self.working_path}"
        if self.exists_locally:
            base_str += " (exists)"
        else:
            base_str += " ( does not yet exist. Call InputDataset.get() )"

        return base_str

    def __repr__(self) -> str:
        # Constructor-style section:
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\nlocation = {self.source.location!r},"
        repr_str += f"\nfile_hash = {self.file_hash}"
        if self.start_date is not None:
            repr_str += f"\nstart_date = {self.start_date!r}"
        if self.end_date is not None:
            repr_str += f"\nend_date = {self.end_date!r}"
        repr_str += "\n)"
        info_str = ""
        if self.working_path is not None:
            info_str += f"working_path = {self.working_path}"
            if not self.exists_locally:
                info_str += " (does not exist)"
        if len(info_str) > 0:
            repr_str += f"\nState: <{info_str}>"
        # Additional info
        return repr_str

    def to_dict(self):
        """Represent this InputDataset object as a dictionary of kwargs.

        Returns:
        --------
        input_dataset_dict (dict):
           A dictionary of kwargs that can be used to initialize the
           InputDataset object.
        """
        input_dataset_dict = {}
        input_dataset_dict["location"] = self.source.location
        if self.file_hash is not None:
            input_dataset_dict["file_hash"] = self.file_hash
        if self.start_date is not None:
            input_dataset_dict["start_date"] = self.start_date.__str__()
        if self.end_date is not None:
            input_dataset_dict["end_date"] = self.end_date.__str__()

        return input_dataset_dict

    def get(self, local_dir: str | Path) -> None:
        """Make the file containing this input dataset available in `local_dir`

        If InputDataset.source.location_type is...
           - ...a local path: create a symbolic link to the file in `local_dir`.
           - ...a URL: fetch the file to `local_dir` using Pooch

        This method updates the `InputDataset.working_path` attribute with the new location.

        Parameters:
        -----------
        local_dir: str
            The local directory in which this input dataset will be saved.
        """
        Path(local_dir).mkdir(parents=True, exist_ok=True)
        target_path = Path(local_dir).resolve() / self.source.basename

        # If the file is somewhere else on the system, make a symbolic link where we want it
        if target_path.exists():
            print(
                f"A file by the name of {self.source.basename} "
                + f"already exists at {local_dir}"
            )
            if self.working_path is None:
                self.working_path = target_path
        else:
            if self.source.location_type == "path":
                target_path.symlink_to(Path(self.source.location).resolve())
            elif self.source.location_type == "url":
                if hasattr(self, "file_hash") and self.file_hash is not None:
                    downloader = pooch.HTTPDownloader(timeout=120)
                    to_fetch = pooch.create(
                        path=local_dir,
                        # urllib equivalent to Path.parent:
                        base_url=urljoin(self.source.location, "."),
                        registry={self.source.basename: self.file_hash},
                    )
                    to_fetch.fetch(self.source.basename, downloader=downloader)
                else:
                    raise ValueError(
                        "InputDataset.source.source_type is 'url' "
                        + "but no InputDataset.file_hash is not defined. "
                        + "Cannot proceed."
                    )

            self.working_path = target_path
