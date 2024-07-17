import os
import sys
import platform
import importlib.util
from typing import Optional

spec = importlib.util.find_spec(__name__.split(".")[0])
if spec is not None:
    if isinstance(spec.submodule_search_locations, list):
        _CSTAR_ROOT: str = spec.submodule_search_locations[0]
else:
    raise ImportError


## Set environment variables according to system
_CSTAR_COMPILER: str
_CSTAR_SYSTEM: str
_CSTAR_SCHEDULER: Optional[str]
_CSTAR_SYSTEM_DEFAULT_PARTITION: Optional[str]
_CSTAR_SYSTEM_CORES_PER_NODE: Optional[int]
_CSTAR_SYSTEM_MEMGB_PER_NODE: Optional[int]
_CSTAR_SYSTEM_MAX_WALLTIME: Optional[str]


if (platform.system() == "Linux") and ("LMOD_DIR" in list(os.environ)):
    sys.path.append(os.environ["LMOD_DIR"] + "/../init")
    from env_modules_python import module

    if "LMOD_SYSHOST" in list(os.environ):
        sysname = os.environ["LMOD_SYSHOST"].casefold()
    elif "LMOD_SYSTEM_NAME" in list(os.environ):
        sysname = os.environ["LMOD_SYSTEM_NAME"].casefold()
    else:
        raise EnvironmentError(
            "unable to find LMOD_SYSHOST or LMOD_SYSTEM_NAME in environment. "
            + "Your system may be unsupported"
        )

    module("restore")
    match sysname:
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
            _CSTAR_SYSTEM = "sdsc_expanse"
            _CSTAR_SCHEDULER = "slurm"
            _CSTAR_SYSTEM_DEFAULT_PARTITION = "compute"
            _CSTAR_SYSTEM_CORES_PER_NODE = 128  # cpu nodes
            _CSTAR_SYSTEM_MEMGB_PER_NODE = 256  # cpu nodes
            _CSTAR_SYSTEM_MAX_WALLTIME = "48:00:00"

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
            _CSTAR_SYSTEM = "ncar_derecho"
            _CSTAR_SCHEDULER = "pbs"
            _CSTAR_SYSTEM_DEFAULT_PARTITION = "main"
            _CSTAR_SYSTEM_CORES_PER_NODE = 128  # cpu nodes
            _CSTAR_SYSTEM_MEMGB_PER_NODE = 256  # cpu nodes
            _CSTAR_SYSTEM_MAX_WALLTIME = "12:00:00"

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
            _CSTAR_SYSTEM = "nersc_perlmutter"
            _CSTAR_SCHEDULER = "slurm"
            _CSTAR_SYSTEM_DEFAULT_PARTITION = "regular"
            _CSTAR_SYSTEM_CORES_PER_NODE = 128  # cpu nodes
            _CSTAR_SYSTEM_MEMGB_PER_NODE = 512  # cpu nodes
            _CSTAR_SYSTEM_MAX_WALLTIME = "24:00:00"

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
    _CSTAR_SYSTEM = "osx_arm64"
    _CSTAR_SCHEDULER = None
    _CSTAR_SYSTEM_DEFAULT_PARTITION = None
    _CSTAR_SYSTEM_CORES_PER_NODE = 12  # get from sysctl -n hw.ncpu
    _CSTAR_SYSTEM_MEMGB_PER_NODE = None
    _CSTAR_SYSTEM_MAX_WALLTIME = None

# Now read the local/custom initialisation file
# This sets variables associated with external codebases that are not installed
# with C-Star (e.g. ROMS_ROOT)

_CSTAR_CONFIG_FILE = _CSTAR_ROOT + "/cstar_local_config.py"
if os.path.exists(_CSTAR_CONFIG_FILE):
    pass

################################################################################
