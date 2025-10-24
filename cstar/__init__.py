################################################################################
# Build module environment at import time
# NOTE: need to set ROMS_ROOT,MARBL_ROOT,CSTAR_ROOT,CSTAR_SYSTEM, and maybe modify PATH on conda install

from importlib.metadata import version as _version

from cstar.simulation import Simulation

__all__ = [
    "Simulation",
]


try:
    __version__ = _version("cstar-ocean")
except Exception:
    # Local copy or not installed with setuptools.
    # Disable minimum version checks on downstream libraries.
    __version__ = "9999"
