import os
import pooch
from .cstar_base_model import BaseModel

class InputDataset:
    """Any spatiotemporal data needed by the model.
    For now this will be NetCDF only,
    but we can imagine interfacing with equivalent ROMS Tools classes"""

    def __init__(self, base_model: BaseModel,
                           source: str,
                        file_hash: str):
        
        self.base_model = base_model
        self.source = source
        self.file_hash = file_hash
        self.local_path = None

    def get(self,local_path):
        #NOTE: default timeout was leading to a lot of timeouterrors
        downloader=pooch.HTTPDownloader(timeout=120)
        to_fetch=pooch.create(
            path=local_path,
            base_url=os.path.dirname(self.source),
            registry={os.path.basename(self.source):self.file_hash})

        to_fetch.fetch(os.path.basename(self.source),downloader=downloader)
        self.local_path=local_path

class ModelGrid(InputDataset):
    pass


class InitialConditions(InputDataset):
    pass


class TidalForcing(InputDataset):
    pass


class BoundaryForcing(InputDataset):
    pass


class SurfaceForcing(InputDataset):
    pass
