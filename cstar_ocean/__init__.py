################################################################################
# Build module environment at import time
# NOTE: need to set ROMS_ROOT,MARBL_ROOT,CSTAR_ROOT,CSTAR_SYSTEM, and maybe modify PATH on conda install
from cstar_ocean.base_model import (
    BaseModel,
    ROMSBaseModel,
    MARBLBaseModel,
)
from cstar_ocean.additional_code import AdditionalCode
from cstar_ocean.input_dataset import (
    InputDataset,
    ROMSInputDataset,
    ROMSModelGrid,
    ROMSTidalForcing,
    ROMSBoundaryForcing,
    ROMSSurfaceForcing,
    ROMSInitialConditions,
)

from cstar_ocean.component import (
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
    "DataSource",
    "InputDataset",
    "ROMSInputDataset",
    "ROMSModelGrid",
    "ROMSTidalForcing",
    "ROMSBoundaryForcing",
    "ROMSSurfaceForcing",
    "ROMSInitialConditions",
    "Component",
    "ROMSComponent",
    "MARBLComponent",
    "Case",
]
