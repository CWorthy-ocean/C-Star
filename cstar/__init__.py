################################################################################
# Build module environment at import time
# NOTE: need to set ROMS_ROOT,MARBL_ROOT,CSTAR_ROOT,CSTAR_SYSTEM, and maybe modify PATH on conda install

import os
from importlib.metadata import version as _version

# silence numba-based OMP warning:
# OMP: Info #276: omp_set_nested routine deprecated, please use omp_set_max_active_levels instead.
# see https://github.com/numba/numba/issues/5275
os.environ["KMP_WARNINGS"] = "off"


try:
    __version__ = _version("cstar-ocean")
except Exception:
    # Local copy or not installed with setuptools.
    # Disable minimum version checks on downstream libraries.
    __version__ = "9999"
