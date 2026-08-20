################################################################################
# Build module environment at import time
# NOTE: need to set ROMS_ROOT,MARBL_ROOT,CSTAR_ROOT,CSTAR_SYSTEM, and maybe modify PATH on conda install

import logging
import os
from importlib.metadata import version as _version

# silence numba-based OMP warning:
# OMP: Info #276: omp_set_nested routine deprecated, please use omp_set_max_active_levels instead.
# see https://github.com/numba/numba/issues/5275
os.environ["KMP_WARNINGS"] = "off"

# Disable prefect analytics, see: https://docs.prefect.io/v3/concepts/telemetry
os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"
os.environ["DO_NOT_TRACK"] = "1"


try:
    __version__ = _version("cstar-ocean")
except Exception:
    # Local copy or not installed with setuptools.
    # Disable minimum version checks on downstream libraries.
    __version__ = "9999"

# Prefect's ephemeral-server teardown logs a harmless CancelledError-driven
# SQLAlchemy pool ERROR at process exit (prefect #16504 / sqlalchemy #12710).
# SQLAlchemy is only used via Prefect, so mute its log output entirely — via
# the propagate flag, which (unlike a level) reset_log_level cannot undo; the
# NullHandler keeps logging.lastResort from printing the record instead.
# Remove along with the Prefect dependency.
_sqlalchemy_logger = logging.getLogger("sqlalchemy")
_sqlalchemy_logger.propagate = False
_sqlalchemy_logger.addHandler(logging.NullHandler())
