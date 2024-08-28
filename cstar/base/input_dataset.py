import os
import pooch
import hashlib
from abc import ABC
import datetime as dt
import dateutil.parser
from cstar.base.datasource import DataSource
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from cstar.base import BaseModel


class InputDataset(ABC):
    """
    Describes spatiotemporal data needed to run a unique instance of a base model

    Attributes:
    -----------
    base_model: BaseModel
        The base model with which this input dataset is associated
    source: DataSource
        Describes the location and type of the source data
    file_hash: str, default None
        The 256 bit SHA sum associated with the file for verifying downloads
    exists_locally: bool, default None
        True if the input dataset exists on the local machine, set by `check_exists_locally()` method if source is a URL
    local_path: str, default None
        The path where the input dataset exists locally, set when `get()` is called if source is a URL

    Methods:
    --------
    get(local_path)
        Fetch the file containing this input dataset and save it to `local_path`
    check_exists_locally(local_path)
        Verify whether the file containing this input dataset has been fetched to `local_path`
    """

    def __init__(
        self,
        base_model: "BaseModel",
        source: DataSource,
        file_hash: Optional[str] = None,
        start_date: Optional[str | dt.datetime] = None,
        end_date: Optional[str | dt.datetime] = None,
    ):
        """
        Initialize an InputDataset object associated with a base model using a source URL and file hash

        Parameters:
        -----------
        base_model: BaseModel
            The base model with which this input dataset is associated
        source: str
            URL or path pointing to the netCDF file containing this input dataset
        file_hash: str
            The 256 bit SHA sum associated with the file for verification

        """

        self.base_model: "BaseModel" = base_model

        self.source: DataSource = source
        self.file_hash: Optional[str] = file_hash

        if (self.file_hash is None) and (self.source.location_type == "url"):
            raise ValueError(
                f"Cannot create InputDataset for \n {self.source.location}:\n "
                + "InputDataset.file_hash cannot be None if InputDataset.source.location_type is 'url'.\n"
                + "A file hash is required to verify files downloaded from remote sources."
            )

        self.exists_locally: Optional[bool] = None
        self.local_path: Optional[str] = None

        # If the input dataset is on the machine, set local_path to its current location
        # this will be updated by .get() which creates a symlink in the user's chosen workspace:
        if self.source.location_type == "path":
            self.exists_locally = True
            self.local_path = self.source.location

        self.start_date = start_date
        self.end_date = end_date
        if isinstance(start_date, str):
            self.start_date = dateutil.parser.parse(start_date)
        if isinstance(end_date, str):
            self.end_date = dateutil.parser.parse(end_date)

        assert self.start_date is None or isinstance(self.start_date, dt.datetime)
        assert self.end_date is None or isinstance(self.end_date, dt.datetime)

    def __str__(self):
        name = self.__class__.__name__
        base_str = f"{name} object "
        base_str = "-" * (len(name) + 7) + "\n" + base_str
        base_str += "\n" + "-" * (len(name) + 7)

        base_str += f"\nBase model: {self.base_model.name}"
        base_str += f"\nsource: {self.source.location}"
        if self.start_date is not None:
            base_str += f"\nstart_date: {self.start_date}"
        if self.end_date is not None:
            base_str += f"\nend_date: {self.end_date}"
        if self.exists_locally is not None:
            base_str += f"\n Exists locally: {self.exists_locally}"
        if self.local_path is not None:
            base_str += f"\nLocal path: {self.local_path}"

        return base_str

    def __repr__(self):
        return self.__str__()

    def get(self, local_dir: str):
        """
        Make the file containing this input dataset available in `local_dir/input_datasets`

        If InputDataset.source.location_type is...
           - ...a local path: create a symbolic link to the file in `local_dir/input_datasets`.
           - ...a URL: fetch the file to `local_dir/input_datasets` using Pooch
                       (updating the `local_path` attribute of the calling InputDataset)

        Parameters:
        -----------
        local_dir: str
            The local directory in which this input dataset will be saved.

        """
        tgt_dir = local_dir + "/input_datasets/" + self.base_model.name + "/"
        os.makedirs(tgt_dir, exist_ok=True)
        tgt_path = tgt_dir + self.source.basename

        # If the file is somewhere else on the system, make a symbolic link where we want it
        if self.exists_locally:
            assert (
                self.local_path is not None
            ), "local_path should always be set when exists_locally is True"
            if os.path.abspath(self.local_path) != os.path.abspath(tgt_path):
                if os.path.exists(tgt_path):
                    raise FileExistsError(
                        f"A file by the name of {self.source.basename}"
                        + f"already exists at {tgt_dir}."
                    )
                    # TODO maybe this should check the hash and just `return` if it matches?
                else:
                    # QUESTION: Should this now update self.local_path to point to the symlink? 20240827 - YES
                    os.symlink(self.local_path, tgt_path)
                    self.local_path = tgt_path
                return
            else:
                # nothing to do as file is already at tgt_path
                return
        else:
            # Otherwise, download the file
            # NOTE: default timeout was leading to a lot of timeouterrors
            downloader = pooch.HTTPDownloader(timeout=120)
            to_fetch = pooch.create(
                path=tgt_dir,
                base_url=os.path.dirname(self.source.location),
                registry={self.source.basename: self.file_hash},
            )

            to_fetch.fetch(self.source.basename, downloader=downloader)
            self.exists_locally = True
            self.local_path = tgt_dir + "/" + self.source.basename

    def check_exists_locally(self, local_dir: str) -> bool:
        """
        Checks whether this InputDataset has already been fetched to the local machine

        Behaves similarly to get() but verifies that the actions of get() have been performed.
        Updates the "InputDataset.exists_locally" attribute.

        Parameters:
        -----------
        local_dir (str):
            The local directory in which to check for the existence of this input dataset

        Returns:
        --------
        exists_locally (bool):
            True if the method has verified the local existence of the dataset
        """

        if self.exists_locally is None:
            tgt_dir = local_dir + "/input_datasets/" + self.base_model.name + "/"
            fpath = tgt_dir + self.source.basename
            if os.path.exists(fpath):
                sha256_hash = hashlib.sha256()
                with open(fpath, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        sha256_hash.update(chunk)

                hash_hex = sha256_hash.hexdigest()
                if self.file_hash != hash_hex:
                    raise ValueError(
                        f"{fpath} exists locally but the local file hash {hash_hex}"
                        + "does not match that associated with this InputDataset object"
                        + f"{self.file_hash}"
                    )
                else:
                    self.exists_locally = True
                    self.local_path = tgt_dir
            else:
                self.exists_locally = False

        return self.exists_locally
