import platform
import os
import importlib.util
from pathlib import Path
from contextlib import redirect_stderr, redirect_stdout
import io
from cstar.base.hpc_env_var_map import determineHPCEnvVars


class RuntimeEnvConfig:
    def __init__(self, _CSTAR_ROOT):
        self.envVars = {}

        self.system = platform.system()

        self._CSTAR_ROOT = _CSTAR_ROOT

        self._CSTAR_COMPILER = ""
        self._CSTAR_SYSTEM = ""
        self._CSTAR_SCHEDULER = ""
        self._CSTAR_ENVIRONMENT_VARIABLES = ""
        self._CSTAR_SYSTEM_DEFAULT_PARTITION = ""
        self._CSTAR_SYSTEM_CORES_PER_NODE = ""
        self._CSTAR_SYSTEM_MEMGB_PER_NODE = ""
        self._CSTAR_SYSTEM_MAX_WALLTIME = ""

        if (self.system == "Linux") and ("LMOD_DIR" in list(os.environ)):
            self.configureHPCRuntimeEnv()
        else:
            self.configureLocalEnv()

    def loadEnvModule(self):
        module_path = (
            Path(os.environ["LMOD_DIR"]).parent / "init" / "env_modules_python.py"
        )
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
        return env_modules.module

    def loadLinuxEnvModules(self, module, sysname):
        module_stdout = io.StringIO()
        module_stderr = io.StringIO()

        # Load Linux Environment Modules for this machine:
        with redirect_stdout(module_stdout), redirect_stderr(module_stderr):
            module("reset")
            with open(
                f"{self._CSTAR_ROOT}/additional_files/lmod_lists/{sysname}.lmod"
            ) as F:
                lmod_list = F.readlines()
            for mod in lmod_list:
                module("load", mod)
        if any(
            keyword in module_stderr.getvalue().casefold()
            for keyword in ["fail", "error"]
        ):
            raise EnvironmentError(
                "Error with linux environment modules: " + module_stderr.getvalue()
            )

    def configureHPCRuntimeEnv(self):
        module = self.loadEnvModule()

        sysname = os.environ.get("LMOD_SYSHOST") or os.environ.get("LMOD_SYSTEM_NAME")
        if not sysname:
            raise EnvironmentError(
                "unable to find LMOD_SYSHOST or LMOD_SYSTEM_NAME in environment. "
                + "Your system may be unsupported"
            )

        self.loadLinuxEnvModules(module, sysname)

        self.envVars = determineHPCEnvVars(sysname)

    def determineCStarSystemFromLocalArch(self):
        localArch = platform.machine()

        if localArch == "arm64":
            cstarSystem = "osx_arm64"

        elif localArch == "x86_64":
            cstarSystem = "osx_x86_64" if self.system == "Darwin" else "linux_x86_64"

        return cstarSystem

    def configureLocalEnv(self):
        # if on MacOS / linux running locally, all dependencies should have been installed by conda
        condaPrefix = os.environ["CONDA_PREFIX"]

        self.envVars = {
            "_CSTAR_ENVIRONMENT_VARIABLES": {
                "MPIHOME": condaPrefix,
                "NETCDFHOME": condaPrefix,
                "LD_LIBRARY_PATH": (
                    os.environ.get("LD_LIBRARY_PATH", default="")
                    + ":"
                    + condaPrefix
                    + "/lib"
                ),
            },
            "_CSTAR_COMPILER": "gnu",
            "_CSTAR_SYSTEM": self.determineCStarSystemFromLocalArch(),
            "_CSTAR_SCHEDULER": None,
            "_CSTAR_SYSTEM_DEFAULT_PARTITION": None,
            "_CSTAR_SYSTEM_CORES_PER_NODE": os.cpu_count(),
            "_CSTAR_SYSTEM_MEMGB_PER_NODE": None,
            "_CSTAR_SYSTEM_MAX_WALLTIME": None,
        }
