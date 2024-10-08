import io
import os
import platform
import importlib.util
from pathlib import Path
from typing import Optional
from contextlib import redirect_stderr, redirect_stdout

top_level_package_name = __name__.split(".")[0]
spec = importlib.util.find_spec(top_level_package_name)
if spec is not None:
    if isinstance(spec.submodule_search_locations, list):
        _CSTAR_ROOT: str = spec.submodule_search_locations[0]
else:
    raise ImportError(f"Top-level package '{top_level_package_name}' not found.")


## Set environment variables according to system
_CSTAR_COMPILER: str
_CSTAR_SYSTEM: str
_CSTAR_SCHEDULER: Optional[str]
_CSTAR_SYSTEM_DEFAULT_PARTITION: Optional[str]
_CSTAR_SYSTEM_CORES_PER_NODE: Optional[int]
_CSTAR_SYSTEM_MEMGB_PER_NODE: Optional[int]
_CSTAR_SYSTEM_MAX_WALLTIME: Optional[str]


if (platform.system() == "Linux") and ("LMOD_DIR" in list(os.environ)):
    # Dynamically load the env_modules_python module using pathlib
    module_path = Path(os.environ["LMOD_DIR"]).parent / "init" / "env_modules_python.py"
    spec = importlib.util.spec_from_file_location("env_modules_python", module_path)
    if (spec is None) or (spec.loader is None):
        raise EnvironmentError(
            f"Could not find env_modules_python on this machine at {module_path}"
        )
    env_modules = importlib.util.module_from_spec(spec)
    if env_modules is None:
        raise EnvironmentError(
            f"No module found by importlib corresponding to spec {spec}"
        )
    spec.loader.exec_module(env_modules)
    module = env_modules.module

    if "LMOD_SYSHOST" in list(os.environ):
        sysname = os.environ["LMOD_SYSHOST"].casefold()
    elif "LMOD_SYSTEM_NAME" in list(os.environ):
        sysname = os.environ["LMOD_SYSTEM_NAME"].casefold()
    else:
        raise EnvironmentError(
            "unable to find LMOD_SYSHOST or LMOD_SYSTEM_NAME in environment. "
            + "Your system may be unsupported"
        )

    module_stdout = io.StringIO()
    module_stderr = io.StringIO()
    with redirect_stdout(module_stdout), redirect_stderr(module_stderr):
        module("reset")

    match sysname:
        case "expanse":
            os.environ["NETCDFHOME"] = os.environ["NETCDF_FORTRANHOME"]
            os.environ["MPIHOME"] = os.environ["MVAPICH2HOME"]
            os.environ["NETCDF"] = os.environ["NETCDF_FORTRANHOME"]
            os.environ["MPI_ROOT"] = os.environ["MVAPICH2HOME"]
            _CSTAR_COMPILER = "intel"
            _CSTAR_SYSTEM = "sdsc_expanse"
            _CSTAR_SCHEDULER = (
                "slurm"  # can get this with `scontrol show config` or `sinfo --version`
            )
            _CSTAR_SYSTEM_DEFAULT_PARTITION = "compute"
            _CSTAR_SYSTEM_CORES_PER_NODE = (
                128  # cpu nodes, can get dynamically node-by-node
            )
            _CSTAR_SYSTEM_MEMGB_PER_NODE = 256  #  with `sinfo -o "%n %c %m %l"`
            _CSTAR_SYSTEM_MAX_WALLTIME = "48:00:00"  # (hostname/cpus/mem[MB]/walltime)

        case "derecho":
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
            _CSTAR_SCHEDULER = (
                "pbs"  # can determine dynamically by testing for `qstat --version`
            )
            _CSTAR_SYSTEM_DEFAULT_PARTITION = "main"
            _CSTAR_SYSTEM_CORES_PER_NODE = (
                128  # Harder to dynamically get this info on PBS
            )
            _CSTAR_SYSTEM_MEMGB_PER_NODE = (
                256  # Can combine `qstat -Qf` and `pbsnodes -a`
            )
            _CSTAR_SYSTEM_MAX_WALLTIME = "12:00:00"  # with grep or awk

        case "perlmutter":
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
            _CSTAR_SYSTEM_CORES_PER_NODE = (
                128  # cpu nodes, can get dynamically node-by-node
            )
            _CSTAR_SYSTEM_MEMGB_PER_NODE = 512  #  with `sinfo -o "%n %c %m %l"`
            _CSTAR_SYSTEM_MAX_WALLTIME = "24:00:00"  # (hostname/cpus/mem[MB]/walltime)

    # Load Linux Environment Modules for this machine:
    with redirect_stdout(module_stdout), redirect_stderr(module_stderr):
        module("reset")
        with open(
            f"{_CSTAR_ROOT}/additional_files/lmod_lists/{_CSTAR_SYSTEM}.lmod"
        ) as F:
            lmod_list = F.readlines()
        for mod in lmod_list:
            module("load", mod)

    if any(
        keyword in module_stderr.getvalue().casefold() for keyword in ["fail", "error"]
    ):
        raise EnvironmentError(
            "Error with linux environment modules: " + module_stderr.getvalue()
        )

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
    _CSTAR_SYSTEM_CORES_PER_NODE = os.cpu_count()
    _CSTAR_SYSTEM_MEMGB_PER_NODE = None
    _CSTAR_SYSTEM_MAX_WALLTIME = None

elif (
    (platform.system() == "Linux")
    and (platform.machine() == "x86_64")
    and ("LMOD_DIR" not in list(os.environ))
):
    os.environ["MPIHOME"] = os.environ["CONDA_PREFIX"]
    os.environ["NETCDFHOME"] = os.environ["CONDA_PREFIX"]
    os.environ["LD_LIBRARY_PATH"] = (
        os.environ.get("LD_LIBRARY_PATH", default="")
        + ":"
        + os.environ["NETCDFHOME"]
        + "/lib"
    )
    _CSTAR_COMPILER = "gnu"
    _CSTAR_SYSTEM = "linux_x86_64"
    _CSTAR_SCHEDULER = None
    _CSTAR_SYSTEM_DEFAULT_PARTITION = None
    _CSTAR_SYSTEM_CORES_PER_NODE = os.cpu_count()
    _CSTAR_SYSTEM_MEMGB_PER_NODE = None
    _CSTAR_SYSTEM_MAX_WALLTIME = None
    # TODO: lots of this is repeat code, can determine a lot of these vars using functions rather than hardcoding

# Now read the local/custom initialisation file
# This sets variables associated with external codebases that are not installed
# with C-Star (e.g. ROMS_ROOT)

_CSTAR_CONFIG_FILE = _CSTAR_ROOT + "/cstar_local_config.py"
if Path(_CSTAR_CONFIG_FILE).exists():
    from cstar.cstar_local_config import set_local_environment

    set_local_environment()


################################################################################
