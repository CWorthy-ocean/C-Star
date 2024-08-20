################################################################################
# Build module environment at import time
# NOTE: need to set ROMS_ROOT,MARBL_ROOT,CSTAR_ROOT,CSTAR_SYSTEM, and maybe modify PATH on conda install
from cstar.base_model import (
    BaseModel,
    ROMSBaseModel,
    MARBLBaseModel,
)
from cstar.additional_code import AdditionalCode
from cstar.input_dataset import (
    InputDataset,
    ModelGrid,
    TidalForcing,
    BoundaryForcing,
    SurfaceForcing,
    InitialConditions,
)

from cstar.component import (
    Component,
    ROMSComponent,
    MARBLComponent,
)
from cstar.cstar_case import Case

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
