import os
from pathlib import Path

from cstar.base.external_codebase import ExternalCodeBase
from cstar.base.utils import _run_cmd
from cstar.roms.build_verification import (
    assert_single_toolchain_stack,
    explicit_mpi_wrapper,
    rpath_link_flags,
)
from cstar.system.manager import get_sysmgr


class ROMSExternalCodeBase(ExternalCodeBase):
    """An implementation of the ExternalCodeBase class for the UCLA Regional Ocean
    Modeling System.

    This subclass sets unique values for ExternalCodeBase properties specific to ROMS, and overrides
    the get() method to compile ROMS-specific libraries.

    Methods:
    --------
    get()
        overrides ExternalCodeBase.get() to clone the UCLA ROMS repository, set environment, and compile libraries
    """

    @property
    def _default_source_repo(self) -> str:
        return "https://github.com/CWorthy-ocean/ucla-roms.git"

    @property
    def _default_checkout_target(self) -> str:
        return "main"

    @property
    def root_env_var(self) -> str:
        return "ROMS_ROOT"

    def _export_env(self) -> None:
        """Set ROMS_ROOT and prepend Tools-Roms to PATH."""
        super()._export_env()
        assert self.working_copy is not None  # verified by ExternalCodeBase._export_env
        roms_root = self.working_copy.path

        cstar_sysmgr = get_sysmgr()
        cstar_sysmgr.environment.set_env_var(
            "PATH", f"{roms_root / 'Tools-Roms'}:{os.environ.get('PATH')}"
        )

    def _configure(self) -> None:
        self._export_env()
        assert self.working_copy is not None  # verified by ExternalCodeBase.configure()
        roms_root = self.working_copy.path

        cstar_sysmgr = get_sysmgr()

        # Compile Tools-Roms
        mpi_wrapper = explicit_mpi_wrapper()
        assert_single_toolchain_stack(mpi_wrapper)
        wrapper_clause = (
            f"MPI_WRAPPER={mpi_wrapper} " if mpi_wrapper is not None else ""
        )
        rpath_flags = rpath_link_flags()
        rpath_clause = f"USER_LDFLAGS='{rpath_flags}' " if rpath_flags else ""

        _run_cmd(
            f"make {wrapper_clause}{rpath_clause}COMPILER={cstar_sysmgr.environment.compiler}",
            cwd=roms_root / "Tools-Roms",
            msg_pre="Compiling Tools-Roms package for UCLA ROMS...",
            msg_post="Compiled Tools-Roms",
            msg_err="Error when compiling Tools-Roms.",
            raise_on_error=True,
        )

    def _is_built_at(self, root: Path) -> bool:
        # Check fundamental Tools-Roms programs compiled
        return (root / "Tools-Roms/mpc").exists()
