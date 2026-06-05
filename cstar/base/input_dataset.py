import datetime as dt
from abc import ABC
from pathlib import Path
from typing import Any

from cstar.base.log import LoggingMixin
from cstar.base.utils import coerce_datetime
from cstar.io.source_data import SourceData
from cstar.io.staged_data import StagedDataCollection, StagedFile


class InputDataset(ABC, LoggingMixin):
    """Describes spatiotemporal data needed to run a unique instance of a model
    simulation.
    """

    source: SourceData
    """The location of and classifies the source data"""
    start_date: dt.datetime | None = None
    """The minimum date in the dataset."""
    end_date: dt.datetime | None = None
    """The maximum date in the dataset."""
    _working_copy: StagedFile | StagedDataCollection | None = None
    """The locally staged version (if any) of the data"""

    def __init__(
        self,
        location: str,
        file_hash: str | None = None,
        start_date: str | dt.datetime | None = None,
        end_date: str | dt.datetime | None = None,
    ) -> None:
        """Initialize an InputDataset object associated with a model simulation.

        Parameters:
        -----------
        location: str
            URL or path pointing to a file either containing this dataset or instructions for creating it.
            Used to set the `source` attribute.
        file_hash: str, optional, recommended for remote files
            The 256 bit SHA sum associated with the file for secure download verification
        """
        self.source: SourceData = SourceData(location=location, identifier=file_hash)
        self.start_date = coerce_datetime(start_date) if start_date else None
        self.end_date = coerce_datetime(end_date) if end_date else None

        # Initialize object state:
        self._working_copy: StagedFile | StagedDataCollection | None = None

        # Subclass-specific  confirmation that everything is set up correctly:
        self.validate()

    def validate(self) -> None:
        pass

    @property
    def working_copy(self) -> StagedFile | StagedDataCollection | None:
        """Describes the (if any) locally available copy of this InputDataset.

        Returns
        -------
        StagedFile | StagedDataCollection:
            Object tracking locally staged data associated with this InputDataset
        """
        return self._working_copy

    @property
    def exists_locally(self) -> bool:
        """Check if an unmodified version of this InputDataset exists on the local filesystem.

        Returns
        -------
        bool: True if all files are known to exist locally, otherwise False
        """
        return bool(self.working_copy and not self.working_copy.changed_from_source)

    @property
    def _local(self) -> list[Path]:
        """Returns any paths associated with working_copy as a list."""
        if self.working_copy:
            if isinstance(self.working_copy, StagedDataCollection):
                return self.working_copy.paths
            return [self.working_copy.path]
        return []

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
        strlocal = " ".join([str(p) for p in self._local]) if self._local else None
        base_str += f"\nLocal copy: {strlocal}"
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

        if self.working_copy:
            info_str += f"working_copy = {' '.join([str(p) for p in self._local])}"
        if len(info_str) > 0:
            repr_str += f"\nState: <{info_str}>"

        return repr_str

    def to_dict(self) -> dict[str, Any]:
        """Represent this InputDataset object as a dictionary of kwargs.

        Returns:
        --------
        input_dataset_dict (dict):
           A dictionary of kwargs that can be used to initialize the
           InputDataset object.
        """
        input_dataset_dict = {"location": self.source.location}

        if self.source.file_hash is not None:
            input_dataset_dict["file_hash"] = self.source.file_hash
        if self.start_date is not None:
            input_dataset_dict["start_date"] = str(self.start_date)
        if self.end_date is not None:
            input_dataset_dict["end_date"] = str(self.end_date)

        return input_dataset_dict

    def get(self, local_dir: str | Path) -> None:
        """Make this InputDataset locally available in `local_dir`.

        This method updates the `InputDataset.working_copy` attribute,

        Parameters:
        -----------
        local_dir: str
            The local directory in which this input dataset will be saved.
        """
        target_path = Path(local_dir).expanduser().resolve() / self.source.basename

        if self.exists_locally:
            msg = f"⏭️ {target_path} already exists, skipping."
            self.log.info(msg)
            return

        staged = self.source.stage(target_dir=local_dir)
        if not isinstance(staged, (StagedFile, StagedDataCollection)):
            msg = f"Require StagedFile or StagedDataCollection; received {type(staged)}"
            raise TypeError(msg)
        self._working_copy = staged
