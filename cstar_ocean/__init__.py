################################################################################
# Build module environment at import time
# NOTE: need to set ROMS_ROOT,MARBL_ROOT,CSTAR_ROOT,CSTAR_SYSTEM, and maybe modify PATH on conda install
import os
import sys
import platform
import importlib.util

_CSTAR_ROOT = importlib.util.find_spec("cstar_ocean").submodule_search_locations[0]

if (
    (platform.system() == "Linux")
    and ("LMOD_DIR" in list(os.environ))
    and ("LMOD_SYSHOST" in list(os.environ))
):
    sys.path.append(os.environ["LMOD_DIR"] + "/../init")
    from env_modules_python import module

    syshost = os.environ["LMOD_SYSHOST"].casefold()
    module("purge")
    match syshost:
        case "expanse":
            sdsc_default_modules = "slurm sdsc DefaultModules shared"
            nc_prerequisite_modules = "cpu/0.15.4 intel/19.1.1.217 mvapich2/2.3.4"
            module("load", sdsc_default_modules)
            module("load", nc_prerequisite_modules)
            module("load", "netcdf-c/4.7.4")
            module("load", "netcdf-fortran/4.5.3")

            os.environ["NETCDFHOME"] = os.environ["NETCDF_FORTRANHOME"]
            os.environ["MPIHOME"] = os.environ["MVAPICH2HOME"]
            os.environ["NETCDF"] = os.environ["NETCDF_FORTRANHOME"]
            os.environ["MPI_ROOT"] = os.environ["MVAPICH2HOME"]
            _CSTAR_COMPILER = "intel"

        case "derecho":
            module("load" "intel")
            module("load", "netcdf")
            module("load", "cray-mpich/8.1.25")

            os.environ["MPIHOME"] = "/opt/cray/pe/mpich/8.1.25/ofi/intel/19.0/"
            os.environ["NETCDFHOME"] = os.environ["NETCDF"]
            os.environ["LD_LIBRARY_PATH"] = (
                os.environ.get("LD_LIBRARY_PATH", default="")
                + ":"
                + os.environ["NETCDFHOME"]
                + "/lib"
            )

            _CSTAR_COMPILER = "intel"

        case "perlmutter":
            module("load", "cpu")
            module("load", "cray-hdf5/1.12.2.9")
            module("load", "cray-netcdf/4.9.0.9")

            os.environ["MPIHOME"] = "/opt/cray/pe/mpich/8.1.28/ofi/gnu/12.3/"
            os.environ["NETCDFHOME"] = "/opt/cray/pe/netcdf/4.9.0.9/gnu/12.3/"
            os.environ["PATH"] = (
                os.environ.get("PATH", default="")
                + ":"
                + os.environ["NETCDFHOME"]
                + "/bin"
            )
            os.environ["LD_LIBRARY_PATH"] = (
                os.environ.get("LD_LIBRARY_PATH", default="")
                + ":"
                + os.environ["NETCDFHOME"]
                + "/lib"
            )
            os.environ["LIBRARY_PATH"] = (
                os.environ.get("LIBRARY_PATH", default="")
                + ":"
                + os.environ["NETCDFHOME"]
                + "/lib"
            )

            _CSTAR_COMPILER = "gnu"

elif (platform.system() == "Darwin") and (platform.machine() == "arm64"):
    # if on MacOS arm64 all dependencies should have been installed by conda

    os.environ["MPIHOME"] = os.environ["CONDA_PREFIX"]
    os.environ["NETCDFHOME"] = os.environ["CONDA_PREFIX"]
    os.environ["LD_LIBRARY_PATH"] = (
        os.environ.get("LD_LIBRARY_PATH", default="")
        + ":"
        + os.environ["NETCDFHOME"]
        + "/lib"
    )
    _CSTAR_COMPILER = "gnu"

# Now read the local/custom initialisation file
# This sets variables associated with external codebases that are not installed
# with C-Star (e.g. ROMS_ROOT)

# _CONFIG_FILE=_CSTAR_ROOT+'/cstar_local_config.py'
# if os.path.exists(_CONFIG_FILE):
#     from . import cstar_local_config

################################################################################


# from .core import _input_files
# from .core import ModelGrid
# from .core import InitialConditions
# from .core import BoundaryConditions
# from .core import SurfaceForcing
# from .core import TidalForcing
# from .core import ModelCode
# from .core import Component
# from .core import Blueprint
# from .core import Instance

from .core import BaseModel
from .core import AdditionalCode
from .core import InputDataset
from .core import ModelGrid
from .core import InitialConditions
from .core import TidalForcing
from .core import BoundaryForcing
from .core import SurfaceForcing
from .core import Component
from .core import Case
