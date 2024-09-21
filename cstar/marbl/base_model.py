import os
import subprocess

from pathlib import Path
from cstar.base import BaseModel
from cstar.base.utils import (
    _clone_and_checkout,
    _write_to_config_file,
)
from cstar.base.environment import _CSTAR_COMPILER


class MARBLBaseModel(BaseModel):
    """
    An implementation of the BaseModel class for the Marine Biogeochemistry Library

    This subclass sets unique values for BaseModel properties specific to MARBL, and overrides
    the get() method to compile MARBL.

    Methods:
    -------
    get()
        overrides BaseModel.get() to clone the MARBL repository, set environment, and compile library.
    """

    @property
    def default_source_repo(self) -> str:
        return "https://github.com/marbl-ecosys/MARBL.git"

    @property
    def default_checkout_target(self) -> str:
        return "v0.45.0"

    @property
    def expected_env_var(self) -> str:
        return "MARBL_ROOT"

    def _base_model_adjustments(self) -> None:
        pass

    def get(self, target: str | Path) -> None:
        """
                Clone MARBL code to local machine, set environment, compile libraries

                This method:
                1. clones MARBL from `source_repo`
                2. checks out the correct commit from `checkout_target`
                3. Sets environment variable MARBL_ROOT
                4. Compiles MARBL

                Parameters:
        z        -----------
                target: str
                    The local path where MARBL will be cloned and compiled
        """
        _clone_and_checkout(
            source_repo=self.source_repo,
            local_path=target,
            checkout_target=self.checkout_target,
        )

        # Set environment variables for this session:
        os.environ["MARBL_ROOT"] = str(target)

        # Set the configuration file to be read by __init__.py for future sessions:
        # QUESTION: how better to handle this?
        config_file_str = f'\n    os.environ["MARBL_ROOT"]="{target}"\n'
        _write_to_config_file(config_file_str)

        # Make things
        print("Compiling MARBL...")
        make_marbl_result = subprocess.run(
            f"make {_CSTAR_COMPILER} USEMPI=TRUE",
            cwd=f"{target}/src",
            shell=True,
            text=True,
            capture_output=True,
        )
        if make_marbl_result.returncode != 0:
            raise RuntimeError(
                f"Error {make_marbl_result.returncode} when compiling MARBL. STDERR stream: "
                + f"\n {make_marbl_result.stderr}"
            )
        print(f"MARBL successfully installed at {target}")
