from pathlib import Path

from cstar.base import ExternalCodeBase
from cstar.base.gitutils import _check_local_repo_changed_from_remote
from cstar.base.utils import _run_cmd
from cstar.system.manager import cstar_sysmgr


class MARBLExternalCodeBase(ExternalCodeBase):
    """An implementation of the ExternalCodeBase class for the Marine Biogeochemistry
    Library.

    This subclass sets unique values for ExternalCodeBase properties specific to MARBL, and overrides
    the get() method to compile MARBL.

    Methods:
    --------
    get()
        overrides ExternalCodeBase.get() to clone the MARBL repository, set environment, and compile library.
    """

    @property
    def _default_source_repo(self) -> str:
        return "https://github.com/marbl-ecosys/MARBL.git"

    @property
    def _default_checkout_target(self) -> str:
        return "marbl0.45.0"

    @property
    def root_env_var(self) -> str:
        return "MARBL_ROOT"

    def _configure(self) -> None:
        """Configure the MARBL codebase on the local machine.

        This method compiles MARBL and adds necessary  variables to the environment.
        """
        assert self.working_copy is not None  # Has been verified by `configure()``
        marbl_root = self.working_copy.path
        # Set env var:
        cstar_sysmgr.environment.set_env_var(self.root_env_var, str(marbl_root))

        # Compile
        _run_cmd(
            f"make {cstar_sysmgr.environment.compiler} USEMPI=TRUE",
            cwd=marbl_root / "src",
            msg_pre="Compiling MARBL...",
            msg_post=f"MARBL successfully installed at {marbl_root}",
            msg_err="Error when compiling MARBL.",
            raise_on_error=True,
        )

    @property
    def is_configured(self) -> bool:
        """Determine whether MARBL is configured locally.

        This method confirms that:
        - Necessary environment variables are set
        - The repository has not changed from that described by `source`.
        - MARBL's `lib` and `inc` dirs exist and are populated
        """
        # Check MARBL_ROOT env var is set:
        marbl_root = cstar_sysmgr.environment.environment_variables.get(
            self.root_env_var
        )
        if not marbl_root:
            return False
        # Check MARBL repo hasn't changed:
        assert self.source.checkout_target is not None  # cannot be for ExternalCodeBase
        # NOTE can't use self.working_copy.changed_from_source here as ExternalCodeBase uses this property to set `working_copy`
        if _check_local_repo_changed_from_remote(
            remote_repo=self.source.location,
            local_repo=marbl_root,
            checkout_target=self.source.checkout_target,
        ):
            return False
        # Check library file exists for current compiler:
        if not (
            Path(marbl_root) / f"lib/libmarbl-{cstar_sysmgr.environment.compiler}-mpi.a"
        ).exists():
            return False
        # Check include dir is not empty:
        inc_dir = Path(marbl_root) / f"include/{cstar_sysmgr.environment.compiler}-mpi/"
        if not any(inc_dir.iterdir()):
            return False

        return True
