from pathlib import Path

from cstar.base import ExternalCodeBase
from cstar.base.gitutils import _clone_and_checkout
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
    def default_source_repo(self) -> str:
        return "https://github.com/marbl-ecosys/MARBL.git"

    @property
    def default_checkout_target(self) -> str:
        return "marbl0.45.0"

    @property
    def expected_env_var(self) -> str:
        return "MARBL_ROOT"

    @property
    def prebuilt_env_var(self) -> str:
        """Environment variable indicating that this codebase is already built.

        Returns:
        -------
        prebuilt_env_var: str
            The name of the environment variable.
        """
        return "CSTAR_MARBL_PREBUILT"

    def get(self, target: str | Path) -> None:
        """Clone MARBL code to local machine, set environment, compile libraries.

        This method:
        1. clones MARBL from `source_repo`
        2. checks out the correct commit from `checkout_target`
        3. Sets environment variable MARBL_ROOT
        4. Compiles MARBL

        Parameters:
        -----------
        target: str
            The local path where MARBL will be cloned and compiled
        """
        _clone_and_checkout(
            source_repo=self.source_repo,
            local_path=Path(target),
            checkout_target=self.checkout_target,
        )
        # Set environment variables for this session:
        cstar_sysmgr.environment.set_env_var(self.expected_env_var, str(target))

        # Make things
        _run_cmd(
            f"make {cstar_sysmgr.environment.compiler} USEMPI=TRUE",
            cwd=Path(target) / "src",
            msg_pre="Compiling MARBL...",
            msg_post=f"MARBL successfully installed at {target}",
            msg_err="Error when compiling MARBL.",
            raise_on_error=True,
        )
