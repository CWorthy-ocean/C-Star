import os
import importlib.util
from pathlib import Path
from typing import Optional
from cstar.base.runtime_env_config import RuntimeEnvConfig

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
_CSTAR_ENVIRONMENT_VARIABLES: dict = {}
_CSTAR_SYSTEM_DEFAULT_PARTITION: Optional[str]
_CSTAR_SYSTEM_CORES_PER_NODE: Optional[int]
_CSTAR_SYSTEM_MEMGB_PER_NODE: Optional[int]
_CSTAR_SYSTEM_MAX_WALLTIME: Optional[str]

runtimeConfig = RuntimeEnvConfig(_CSTAR_ROOT)

print('------ env vars: ')
for key, value in runtimeConfig.envVars.items():
    print((key, value))
print('------ env vars: ')

# TODO: what is a cleaner way to destructure this as one would in JS?
_CSTAR_COMPILER = runtimeConfig.envVars['_CSTAR_COMPILER']
_CSTAR_SYSTEM = runtimeConfig.envVars['_CSTAR_SYSTEM']
_CSTAR_SCHEDULER = runtimeConfig.envVars['_CSTAR_SCHEDULER']
_CSTAR_ENVIRONMENT_VARIABLES = runtimeConfig.envVars['_CSTAR_ENVIRONMENT_VARIABLES']
_CSTAR_SYSTEM_DEFAULT_PARTITION = runtimeConfig.envVars['_CSTAR_SYSTEM_DEFAULT_PARTITION']
_CSTAR_SYSTEM_CORES_PER_NODE = runtimeConfig.envVars['_CSTAR_SYSTEM_CORES_PER_NODE']
_CSTAR_SYSTEM_MEMGB_PER_NODE = runtimeConfig.envVars['_CSTAR_SYSTEM_MEMGB_PER_NODE']
_CSTAR_SYSTEM_MAX_WALLTIME = runtimeConfig.envVars['_CSTAR_SYSTEM_MAX_WALLTIME']

# Now read the local/custom initialisation file
# This sets variables associated with external codebases that are not installed
# with C-Star (e.g. ROMS_ROOT)

_CSTAR_CONFIG_FILE = _CSTAR_ROOT + "/cstar_local_config.py"
if Path(_CSTAR_CONFIG_FILE).exists():
    from cstar.cstar_local_config import get_user_environment

    get_user_environment()

for var, value in _CSTAR_ENVIRONMENT_VARIABLES.items():
    os.environ[var] = value

################################################################################
