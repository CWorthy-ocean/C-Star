import datetime as dt
from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING

from cstar.base.log import LoggingMixin
from cstar.base.utils import coerce_datetime
from cstar.io.source_data import SourceData
from cstar.io.staged_data import StagedDataCollection, StagedFile

if TYPE_CHECKING:
    pass


class InputDataset(ABC, LoggingMixin):
    """Describes spatiotemporal data needed to run a unique instance of a model
    simulation.

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
        file_hash: str | None = None,
        start_date: str | dt.datetime | None = None,
        end_date: str | dt.datetime | None = None,
    ):
        """Initialize an InputDataset object associated with a model simulation using a
        source URL and file hash.

        Parameters:
        -----------
        location: str
            URL or path pointing to a file either containing this dataset or instructions for creating it.
            Used to set the `source` attribute.
        file_hash: str, optional
            The 256 bit SHA sum associated with the file for verification if remote
        """
        self.source: SourceData = SourceData(location=location, identifier=file_hash)
        self.start_date = coerce_datetime(start_date) if start_date else None
        self.end_date = coerce_datetime(start_date) if start_date else None

        # assert self.start_date is None or isinstance(self.start_date, dt.datetime)
        # assert self.end_date is None or isinstance(self.end_date, dt.datetime)

        # Initialize object state:
        self._working_copy: StagedFile | StagedDataCollection | None = None
        # Subclass-specific  confirmation that everything is set up correctly:
        self.validate()

    def validate(self):
        pass

    @property
    def working_copy(self) -> StagedFile | StagedDataCollection | None:
        return self._working_copy

    @property
    def exists_locally(self) -> bool:
        """Check if this InputDataset exists on the local filesystem.

        Returns
        -------
        bool: True if all files are known to exist locally, otherwise False
        """
        if (self.working_copy) and not (self.working_copy.changed_from_source):
            return True
        return False

    @property
    def _local(self) -> list[Path] | None:
        """Returns any paths associated with working_copy as a list"""
        if self.working_copy:
            if isinstance(self.working_copy, StagedDataCollection):
                return self.working_copy.paths
            return [
                self.working_copy.path,
            ]
        return None

    def __str__(self) -> str:
        name = self.__class__.__name__
        base_str = f"{name}"
        base_str = "-" * len(name) + "\n" + base_str
        base_str += "\n" + "-" * len(name)

        base_str += f"\nSource location: {self.source.location}"
        if self.source.file_hash is not None:
            base_str += f"\nSource file hash: {self.source.file_hash}"
        if self.start_date is not None:
            base_str += f"\nstart date: {self.start_date}"
        if self.end_date is not None:
            base_str += f"\nend date: {self.end_date}"
        base_str += f"\nLocal copy: {self._local}"
        return base_str

    def __repr__(self) -> str:
        # Constructor-style section:
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\nlocation = {self.source.location!r},"
        repr_str += f"\nfile_hash = {self.source.file_hash!r},"
        if self.start_date is not None:
            repr_str += f"\nstart_date = {self.start_date!r},"
        if self.end_date is not None:
            repr_str += f"\nend_date = {self.end_date!r}"
        repr_str += "\n)"
        info_str = ""
        # if self.working_path is not None:
        if self.working_copy:
            info_str += f"working_copy = {self.working_copy}"
        if len(info_str) > 0:
            repr_str += f"\nState: <{info_str}>"
        # Additional info
        return repr_str

    def to_dict(self) -> dict:
        """Represent this InputDataset object as a dictionary of kwargs.

        Returns:
        --------
        input_dataset_dict (dict):
           A dictionary of kwargs that can be used to initialize the
           InputDataset object.
        """
        input_dataset_dict = {}
        input_dataset_dict["location"] = self.source.location
        if self.source.file_hash is not None:
            input_dataset_dict["file_hash"] = self.source.file_hash
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

        This method updates the `InputDataset.working_path` attribute with the new location,
        and caches file metadata and checksum values.

        Parameters:
        -----------
        local_dir: str
            The local directory in which this input dataset will be saved.
        """
        # Path(local_dir).expanduser().mkdir(parents=True, exist_ok=True)
        target_path = Path(local_dir).expanduser().resolve() / self.source.basename

        if self.exists_locally:
            self.log.info(f"⏭️ {target_path} already exists, skipping.")
            return
        staged = self.source.stage(target_dir=local_dir)
        assert isinstance(staged, StagedFile) or isinstance(
            staged, StagedDataCollection
        )
        self._working_copy = staged
