import os
import pooch
from .cstar_base_model import BaseModel

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
    local_path: str, default None
        The path to where the input dataset has been fetched locally, set when `get()` method is called

    Methods:
    --------
    get(local_path)
        Fetch the file containing this input dataset and save it to `local_path`
    """
    
    def __init__(self, base_model: BaseModel,
                           source: str,
                        file_hash: str):
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
        self.local_path = None

    def get(self,local_path):
        """
        Fetch the file containing this input dataset and save it to `local_path` using Pooch.

        This method updates the `local_path` attribute of the calling InputDataset object

        Parameters:
        -----------
        local_path: str
            The local path where this input dataset will be saved.
        
        """
        
        
        #NOTE: default timeout was leading to a lot of timeouterrors
        downloader=pooch.HTTPDownloader(timeout=120)
        to_fetch=pooch.create(
            path=local_path,
            base_url=os.path.dirname(self.source),
            registry={os.path.basename(self.source):self.file_hash})

        to_fetch.fetch(os.path.basename(self.source),downloader=downloader)
        self.local_path=local_path

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
