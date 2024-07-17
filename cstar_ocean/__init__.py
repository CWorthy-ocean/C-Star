################################################################################
# Build module environment at import time
# NOTE: need to set ROMS_ROOT,MARBL_ROOT,CSTAR_ROOT,CSTAR_SYSTEM, and maybe modify PATH on conda install
from cstar_ocean.cstar_base_model import (
    BaseModel,
    ROMSBaseModel,
    MARBLBaseModel,
)
from cstar_ocean.cstar_additional_code import AdditionalCode
from cstar_ocean.cstar_input_dataset import (
    InputDataset,
    ModelGrid,
    TidalForcing,
    BoundaryForcing,
    SurfaceForcing,
    InitialConditions,
)

from cstar_ocean.cstar_component import (
    Component,
    ROMSComponent,
    MARBLComponent,
)
from cstar_ocean.cstar_case import Case

__all__ = [
    "BaseModel",
    "ROMSBaseModel",
    "MARBLBaseModel",
    "AdditionalCode",
    "InputDataset",
    "ModelGrid",
    "TidalForcing",
    "BoundaryForcing",
    "SurfaceForcing",
    "InitialConditions",
    "Component",
    "ROMSComponent",
    "MARBLComponent",
    "Case",
]
