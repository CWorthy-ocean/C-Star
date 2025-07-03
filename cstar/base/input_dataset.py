import datetime as dt
from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, Optional
from urllib.parse import urljoin

import dateutil.parser
import pooch

from cstar.base.datasource import DataSource
from cstar.base.local_file_stats import LocalFileStatistics
from cstar.base.log import LoggingMixin
from cstar.base.utils import _get_sha256_hash

if TYPE_CHECKING:
    import logging


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
        file_hash: Optional[str] = None,
        start_date: Optional[str | dt.datetime] = None,
        end_date: Optional[str | dt.datetime] = None,
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

        self.source: DataSource = DataSource(location=location, file_hash=file_hash)
        if (
            (self.source.location_type == "url")
            and (self.source.file_hash is None)
            and (self.source.source_type != "yaml")
        ):
            raise ValueError(
                f"Cannot create InputDataset for \n {self.source.location}:\n "
                + "InputDataset.source.file_hash cannot be None if InputDataset.source.location_type is 'url'.\n"
                + "A file hash is required to verify non-plaintext files downloaded from remote sources."
            )
        if isinstance(start_date, str):
            start_date = dateutil.parser.parse(start_date)
        self.start_date = start_date
        if isinstance(end_date, str):
            end_date = dateutil.parser.parse(end_date)
        self.end_date = end_date
        assert self.start_date is None or isinstance(self.start_date, dt.datetime)
        assert self.end_date is None or isinstance(self.end_date, dt.datetime)

        # Initialize object state:
        self.local_file_stats: Optional[LocalFileStatistics] = None

        # Subclass-specific  confirmation that everything is set up correctly:
        self.validate()

    def validate(self):
        pass

    @property
    def exists_locally(self):
        """Determines whether this InputDataset instance exists on the local system."""
        if not self.local_file_stats:
            return False
        try:
            self.local_file_stats.validate()
        except (FileNotFoundError, ValueError, KeyError):
            return False
        return True

    @property
    def working_path(self) -> Optional[Path | list[Path]]:
        """The current local path where this InputDataset exists on the local system, if
        it has been fetched."""
        if self.local_file_stats is None:
            return None
        if len(self.local_file_stats.paths) == 1:
            return self.local_file_stats.paths[0]
        return self.local_file_stats.paths

    def __str__(self) -> str:
        name = self.__class__.__name__
        base_str = f"{name}"
        base_str = "-" * len(name) + "\n" + base_str
        base_str += "\n" + "-" * len(name)

        base_str += f"\nSource location: {self.source.location}"
        if self.source.file_hash is not None:
            base_str += f"\nSource file hash: {self.source.file_hash}"
        if self.start_date is not None:
            base_str += f"\nstart_date: {self.start_date}"
        if self.end_date is not None:
            base_str += f"\nend_date: {self.end_date}"
        base_str += f"\nWorking path: {self.working_path}"
        if self.exists_locally:
            base_str += " (exists. Query local file statistics with InputDataset.local_file_stats)"
        else:
            base_str += " ( does not yet exist. Call InputDataset.get() )"
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
        if self.working_path is not None:
            info_str += f"working_path = {self.working_path}"
            if not self.exists_locally:
                info_str += " (does not exist)"
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
        Path(local_dir).expanduser().mkdir(parents=True, exist_ok=True)
        target_path = Path(local_dir).expanduser().resolve() / self.source.basename

        if (self.exists_locally) and (self.working_path == target_path):
            self.log.info(f"⏭️ {self.working_path} already exists, skipping.")
            return

        computed_file_hash = self._symlink_or_download_from_source(
            source_location=self.source.location,
            location_type=self.source.location_type,
            expected_file_hash=self.source.file_hash,
            target_path=target_path,
            logger=self.log,
        )

        p = target_path.absolute()
        self.local_file_stats = LocalFileStatistics(
            paths=[
                p,
            ],
            stats={p: p.stat()},
            hashes={p: computed_file_hash},
        )

    @staticmethod
    def _symlink_or_download_from_source(
        source_location: str | Path,
        location_type: str,
        expected_file_hash: str | None,
        target_path: Path,
        logger: "logging.Logger",
    ) -> str:
        """Helper method to either create a symbolic link to this InputDataset (if it
        exists on the local filesystem) or download it (if it is located remotely)."""
        if location_type == "path":
            source_location = Path(source_location).expanduser().resolve()
            # TODO when refactoring get(), avoid calculating hash here
            computed_file_hash = _get_sha256_hash(source_location)
            if (expected_file_hash is not None) and (
                expected_file_hash != computed_file_hash
            ):
                raise ValueError(
                    f"The provided file hash ({expected_file_hash}) does not match "
                    f"that of the file at {source_location} ({computed_file_hash}). "
                    "Note that as this input dataset exists on the local filesystem, "
                    "C-Star does not require a file hash to use it. Please either "
                    "update the file_hash entry or remove it."
                )

            target_path.symlink_to(source_location)

        elif location_type == "url":
            if expected_file_hash is not None:
                downloader = pooch.HTTPDownloader(timeout=120)
                to_fetch = pooch.create(
                    path=target_path.parent,
                    # urllib equivalent to Path.parent:
                    base_url=urljoin(str(source_location), "."),
                    registry={target_path.name: expected_file_hash},
                )

                to_fetch.fetch(target_path.name, downloader=downloader)
                # No need to compute hash as Pooch does this internaly:
                computed_file_hash = expected_file_hash

            else:
                raise ValueError(
                    "Source type is URL "
                    + "but no file hash was not provided. "
                    + "Cannot proceed."
                )
        return computed_file_hash

    def _clear(self):
        """Reset the internal state of this InputDataset, clearing any cached
        information such as local file statistics."""
        self.local_file_stats = None
