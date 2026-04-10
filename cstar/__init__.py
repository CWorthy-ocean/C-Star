################################################################################
# Build module environment at import time
# NOTE: need to set ROMS_ROOT,MARBL_ROOT,CSTAR_ROOT,CSTAR_SYSTEM, and maybe modify PATH on conda install

import multiprocessing
import os
from importlib.metadata import version as _version

# silence numba-based OMP warning:
# OMP: Info #276: omp_set_nested routine deprecated, please use omp_set_max_active_levels instead.
# see https://github.com/numba/numba/issues/5275
os.environ["KMP_WARNINGS"] = "off"

# Disable prefect analytics, see: https://docs.prefect.io/v3/concepts/telemetry
os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"
os.environ["DO_NOT_TRACK"] = "1"

multiprocessing.set_start_method("spawn")

try:
    __version__ = _version("cstar-ocean")
except Exception:
    # Local copy or not installed with setuptools.
    # Disable minimum version checks on downstream libraries.
    __version__ = "9999"
