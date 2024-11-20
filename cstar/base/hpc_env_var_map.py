import os

PERLMUTTER_NCDF_HOME_PATH = "/opt/cray/pe/netcdf/4.9.0.9/gnu/12.3/"


def values():
    return {
        "expanse": {
            "_CSTAR_ENVIRONMENT_VARIABLES": {
                "NETCDFHOME": os.environ["NETCDF_FORTRANHOME"],
                "MPIHOME": os.environ["MVAPICH2HOME"],
                "NETCDF": os.environ["NETCDF_FORTRANHOME"],
                "MPI_ROOT": os.environ["MVAPICH2HOME"],
            },
            "_CSTAR_COMPILER": "intel",
            "_CSTAR_SYSTEM": "expanse",
            "_CSTAR_SCHEDULER": (
                "slurm"  # can get this with `scontrol show config` or `sinfo --version`
            ),
            "_CSTAR_SYSTEM_DEFAULT_PARTITION": "compute",
            "_CSTAR_SYSTEM_MEMGB_PER_NODE": 256,  #  with `sinfo -o "%n %c %m %l"`
            "_CSTAR_SYSTEM_MAX_WALLTIME": "48:00:00",  # (hostname/cpus/mem[MB]/walltime)
        },
        "derecho": {
            "_CSTAR_ENVIRONMENT_VARIABLES": {
                "MPIHOME": ("/opt/cray/pe/mpich/8.1.25/ofi/intel/19.0/"),
                "NETCDFHOME": os.environ["NETCDF"],
                "LD_LIBRARY_PATH": (
                    os.environ.get("LD_LIBRARY_PATH", default="")
                    + ":"
                    + os.environ["NETCDF"]
                    + "/lib"
                ),
            },
            "_CSTAR_COMPILER": "intel",
            "_CSTAR_SYSTEM": "derecho",
            "_CSTAR_SCHEDULER": (
                "pbs"  # can determine dynamically by testing for `qstat --version`
            ),
            "_CSTAR_SYSTEM_DEFAULT_PARTITION": "main",
            "_CSTAR_SYSTEM_MEMGB_PER_NOD": (
                256  # Can combine `qstat -Qf` and `pbsnodes -a`
            ),
            "_CSTAR_SYSTEM_MAX_WALLTIME": "12:00:00",  # with grep or awk
        },
        "perlmutter": {
            "_CSTAR_ENVIRONMENT_VARIABLES": {
                "MPIHOME": ("/opt/cray/pe/mpich/8.1.28/ofi/gnu/12.3/"),
                "NETCDFHOME": PERLMUTTER_NCDF_HOME_PATH,
                "PATH": (
                    os.environ.get("PATH", default="")
                    + ":"
                    + PERLMUTTER_NCDF_HOME_PATH
                    + "/bin"
                ),
                "LD_LIBRARY_PATH": (
                    os.environ.get("LD_LIBRARY_PATH", default="")
                    + ":"
                    + PERLMUTTER_NCDF_HOME_PATH
                    + "/lib"
                ),
                "LIBRARY_PATH": (
                    os.environ.get("LIBRARY_PATH", default="")
                    + ":"
                    + PERLMUTTER_NCDF_HOME_PATH
                    + "/lib"
                ),
            },
            "_CSTAR_COMPILER": "gnu",
            "_CSTAR_SYSTEM": "perlmutter",
            "_CSTAR_SCHEDULER": "slurm",
            "_CSTAR_SYSTEM_DEFAULT_PARTITION": "regular",
            "_CSTAR_SYSTEM_MEMGB_PER_NODE": 512,  #  with `sinfo -o "%n %c %m %l"`
            "_CSTAR_SYSTEM_MAX_WALLTIME": "24:00:00",  # (hostname/cpus/mem[MB]/walltime)
        },
    }


commonValues = {
    "_CSTAR_SYSTEM_CORES_PER_NODE": (
        128  # cpu nodes, can get dynamically node-by-node
    ),
}


def determineHPCEnvVars(sysname):
    if sysname not in values.keys:
        raise EnvironmentError(
            f"Unable to configure environment variables for system: {sysname}"
        )

    return {**commonValues, **values()[sysname]}
