from pathlib import Path

from cstar.base.external_codebase import ExternalCodeBase
from cstar.base.utils import _run_cmd
from cstar.system.manager import get_sysmgr


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
        self._export_env()

        cstar_sysmgr = get_sysmgr()

        assert self.working_copy is not None  # Has been verified by `configure()``
        marbl_root = self.working_copy.path

        # Compile
        _run_cmd(
            f"make {cstar_sysmgr.environment.compiler} USEMPI=TRUE",
            cwd=marbl_root / "src",
            msg_pre="Compiling MARBL...",
            msg_post=f"MARBL successfully installed at {marbl_root}",
            msg_err="Error when compiling MARBL.",
            raise_on_error=True,
        )

    def _is_built_at(self, root: Path) -> bool:
        """Confirm MARBL's library for the current compiler exists and its
        include dir is populated.
        """
        compiler = get_sysmgr().environment.compiler
        if not (root / f"lib/libmarbl-{compiler}-mpi.a").exists():
            return False
        inc_dir = root / f"include/{compiler}-mpi/"
        return inc_dir.exists() and any(inc_dir.iterdir())
