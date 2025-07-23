import datetime as dt
from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urljoin

import dateutil.parser
import pooch

from cstar.base.datasource import DataSource
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
        self.working_path: Path | list[Path] | None = None
        self._local_file_hash_cache: dict = {}
        self._local_file_stat_cache: dict = {}

        # Subclass-specific  confirmation that everything is set up correctly:
        self.validate()

    def validate(self):
        pass

    @property
    def exists_locally(self) -> bool:
        """Check if this InputDataset exists on the local filesystem.

        This method verifies the following for each file in `InputDataset.working_path`:
        1. The file exists at the specified path.
        2. The file's current size and modification date match values cached by `InputDataset.get()`
        3. If the size matches but the modification time does not, the file's SHA-256 hash
           is computed and compared against a value cached by `InputDataset.get()`

        Returns:
        --------
        exists_locally (bool): True if all files pass the existence, size, modification
            time, and (if necessary) hash checks. Returns False otherwise.

        Notes:
        ------
            If C-Star cannot access cached file statistics, it is impossible to verify
            whether the InputDataset is correct, and so `False` is returned.
        """
        if (self.working_path is None) or (not self._local_file_stat_cache):
            return False

        # Ensure working_path is a list for unified iteration
        paths = (
            self.working_path
            if isinstance(self.working_path, list)
            else [self.working_path]
        )

        for path in paths:
            # Check if the file exists
            if not path.exists():
                return False

            # Retrieve the cached stats
            cached_stats = self._local_file_stat_cache.get(path)
            if cached_stats is None:
                return False  # No stats cached for this file

            # Compare size first
            current_stats = path.stat()
            if current_stats.st_size != cached_stats.st_size:
                return False  # Size mismatch, no need to check further

            # Compare modification time, fallback to hash check if mismatched
            if current_stats.st_mtime != cached_stats.st_mtime:
                current_hash = _get_sha256_hash(path.resolve())
                if self._local_file_hash_cache.get(path, None) != current_hash:
                    return False

        return True

    @property
    def local_hash(self) -> dict | None:
        """Compute or retrieve the cached SHA-256 hash of the local dataset.

        This property calculates the SHA-256 hash for the dataset located at `working_path`.
        If the hash has been previously computed and cached by InputDataset.get(),
        it will return the cached value instead of recomputing it.

        If `working_path` is a list of paths, the hash is computed for each file
        individually. The hashes are stored as a dictionary mapping paths to their
        respective hash values.

        Returns
        -------
        local_hash (dict or None)
            - A dictionary where the keys are `Path` objects representing file paths
              and the values are their respective SHA-256 hashes.
            - `None` if `working_path` is not set or no files exist locally.
        """
        if self._local_file_hash_cache:
            return self._local_file_hash_cache

        if (not self.exists_locally) or (self.working_path is None):
            local_hash = {}
        elif isinstance(self.working_path, list):
            local_hash = {
                path: _get_sha256_hash(path.resolve()) for path in self.working_path
            }
        elif isinstance(self.working_path, Path):
            local_hash = {self.working_path: _get_sha256_hash(self.working_path)}

        self._local_file_hash_cache.update(local_hash)
        return local_hash

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
            base_str += " (exists)"
        else:
            base_str += " ( does not yet exist. Call InputDataset.get() )"

        if self.local_hash:
            base_str += f"\nLocal hash: {self.local_hash}"
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
        if self.local_hash:
            info_str += f", local_hash = {self.local_hash}"
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

        self.working_path = target_path
        self._local_file_hash_cache.update({target_path: computed_file_hash})  # 27
        self._local_file_stat_cache.update({target_path: target_path.stat()})

    @staticmethod
    def _symlink_or_download_from_source(
        source_location: str | Path,
        location_type: str,
        expected_file_hash: str | None,
        target_path: Path,
        logger: "logging.Logger",
    ) -> str:
        if location_type == "path":
            source_location = Path(source_location).expanduser().resolve()
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
