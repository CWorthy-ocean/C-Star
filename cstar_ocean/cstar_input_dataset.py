import os
import pooch
import hashlib
from cstar_ocean.cstar_base_model import BaseModel


class InputDataset:
    """
    Describes spatiotemporal data needed to run a unique instance of a base model

    Attributes:
    -----------
    base_model: BaseModel
        The base model with which this input dataset is associated
    source: str
        URL pointing to the netCDF file containing this input dataset
    file_hash: str
        The 256 bit SHA sum associated with the file for verifying downloads
    exists_locally: bool, default None
        True if the input dataset has been fetched to the local machine, set when `check_exists_locally()` method is called
    local_path: str, default None
        The path to where the input dataset has been fetched locally, set when `get()` method is called

    Methods:
    --------
    get(local_path)
        Fetch the file containing this input dataset and save it to `local_path`
    check_exists_locally(local_path)
        Verify whether the file containing this input dataset has been fetched to `local_path`
    """

    def __init__(self, base_model: BaseModel, source: str, file_hash: str):
        """
        Initialize an InputDataset object associated with a base model using a source URL and file hash

        Parameters:
        -----------
        base_model: BaseModel
            The base model with which this input dataset is associated
        source: str
            URL pointing to the netCDF file containing this input dataset
        file_hash: str
            The 256 bit SHA sum associated with the file for verifying downloads

        """

        self.base_model = base_model
        self.source = source
        self.file_hash = file_hash
        self.exists_locally = None
        self.local_path = None

    def __str__(self):
        name = self.__class__.__name__
        base_str = f"{name} object "
        base_str = "-" * (len(name) + 7) + "\n" + base_str
        base_str += "\n" + "-" * (len(name) + 7)

        base_str += f"\nBase model: {self.base_model.name}"
        base_str += f"\nRemote path URL: {self.source}"
        if self.exists_locally is not None:
            base_str += f"\n Exists locally: {self.exists_locally}"
        if self.local_path is not None:
            base_str += f"\nLocal path: {self.local_path}"

        return base_str

    def __repr__(self):
        return self.__str__()

    def get(self, local_path):
        """
        Fetch the file containing this input dataset and save it to `local_path` using Pooch.

        This method updates the `local_path` attribute of the calling InputDataset object

        Parameters:
        -----------
        local_path: str
            The local path where this input dataset will be saved.

        """

        tgt_dir = local_path + "/input_datasets/" + self.base_model.name + "/"
        os.makedirs(tgt_dir, exist_ok=True)

        # NOTE: default timeout was leading to a lot of timeouterrors
        downloader = pooch.HTTPDownloader(timeout=120)
        to_fetch = pooch.create(
            path=tgt_dir,
            base_url=os.path.dirname(self.source),
            registry={os.path.basename(self.source): self.file_hash},
        )

        to_fetch.fetch(os.path.basename(self.source), downloader=downloader)
        self.local_path = tgt_dir

    def check_exists_locally(self, local_path):
        """
        Checks whether this InputDataset has already been fetched to the local machine

        Behaves similarly to get() but verifies that the actions of get() have been performed.
        Updates the "InputDataset.exists_locally" attribute.

        Parameters:
        -----------
        local_path (str):
            The local path to check for the existence of this input dataset

        Returns:
        --------
        exists_locally (bool):
            True if the method has verified the local existence of the dataset
        """
        tgt_dir = local_path + "/input_datasets/" + self.base_model.name + "/"
        fpath = tgt_dir + os.path.basename(self.source)
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

        if self.exists_locally:
            self.local_path = tgt_dir
            return True
        else:
            self.exists_locally = False
            return False


class ModelGrid(InputDataset):
    """
    An implementation of the InputDataset class for model grid files.
    """

    pass


class InitialConditions(InputDataset):
    """
    An implementation of the InputDataset class for model initial condition files.
    """

    pass


class TidalForcing(InputDataset):
    """
    An implementation of the InputDataset class for model tidal forcing files.
    """

    pass


class BoundaryForcing(InputDataset):
    """
    An implementation of the InputDataset class for model boundary condition files.
    """

    pass


class SurfaceForcing(InputDataset):
    """
    An implementation of the InputDataset class for model surface forcing files.
    """

    pass
