from pathlib import Path

from cstar.base.external_codebase import ExternalCodeBase
from cstar.base.gitutils import _check_local_repo_changed_from_remote
from cstar.base.utils import _run_cmd
from cstar.system.manager import cstar_sysmgr


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

    def _configure(self) -> None:
        # Set env vars:
        assert self.working_copy is not None  # verified by ExternalCodeBase.configure()
        roms_root = self.working_copy.path
        cstar_sysmgr.environment.set_env_var(self.root_env_var, str(roms_root))
        cstar_sysmgr.environment.set_env_var(
            "PATH", f"${{PATH}}:{roms_root / 'Tools-Roms'}"
        )

        # Compile NHMG library
        _run_cmd(
            f"make nhmg COMPILER={cstar_sysmgr.environment.compiler}",
            cwd=roms_root / "Work",
            msg_pre="Compiling NHMG library...",
            msg_err="Error when compiling ROMS' NHMG library.",
            raise_on_error=True,
        )

        # Compile Tools-Roms
        _run_cmd(
            f"make COMPILER={cstar_sysmgr.environment.compiler}",
            cwd=roms_root / "Tools-Roms",
            msg_pre="Compiling Tools-Roms package for UCLA ROMS...",
            msg_post="Compiled Tools-Roms",
            msg_err="Error when compiling Tools-Roms.",
            raise_on_error=True,
        )

    @property
    def is_configured(self) -> bool:
        # Check ROMS_ROOT env var is set:
        roms_root = cstar_sysmgr.environment.environment_variables.get(
            self.root_env_var
        )
        if not roms_root:
            return False
        assert self.source.checkout_target is not None  # Cannot be for ExternalCodeBase
        #
        if _check_local_repo_changed_from_remote(
            remote_repo=self.source.location,
            local_repo=roms_root,
            checkout_target=self.source.checkout_target,
        ):
            return False

        # Check fundamental Tools-Roms programs compiled
        if not (Path(roms_root) / "Tools-Roms/mpc").exists():
            return False
        return True
