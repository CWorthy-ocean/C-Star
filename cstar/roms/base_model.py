import os
import shutil
import subprocess
from pathlib import Path
from cstar.base.base_model import BaseModel
from cstar.base.utils import (
    _clone_and_checkout,
    _write_to_config_file,
)
from cstar.base.environment import (
    _CSTAR_ROOT,
    _CSTAR_COMPILER,
    _CSTAR_ENVIRONMENT_VARIABLES,
)


class ROMSBaseModel(BaseModel):
    """An implementation of the BaseModel class for the UCLA Regional Ocean Modeling
    System.

    This subclass sets unique values for BaseModel properties specific to ROMS, and overrides
    the get() method to compile ROMS-specific libraries.

    Methods:
    -------
    get()
        overrides BaseModel.get() to clone the UCLA ROMS repository, set environment, and compile libraries
    """

    @property
    def default_source_repo(self) -> str:
        return "https://github.com/CESR-lab/ucla-roms.git"

    @property
    def default_checkout_target(self) -> str:
        return "main"

    @property
    def expected_env_var(self) -> str:
        return "ROMS_ROOT"

    def _base_model_adjustments(self) -> None:
        """Perform C-Star specific adjustments to stock ROMS code.

        In particular, this method replaces the default Makefiles with machine-agnostic
        versions, allowing C-Star to be used with ROMS across multiple different
        computing systems.
        """
        shutil.copytree(
            _CSTAR_ROOT + "/additional_files/ROMS_Makefiles/",
            os.environ[self.expected_env_var],
            dirs_exist_ok=True,
        )

    def get(self, target: str | Path) -> None:
        """Clone ROMS code to local machine, set environment, compile libraries.

        This method:
        1. clones ROMS from `source_repo`
        2. checks out the correct commit from `checkout_target`
        3. Sets environment variable ROMS_ROOT and appends $ROMS_ROOT/Tools-Roms to PATH
        4. Replaces ROMS Makefiles for machine-agnostic compilation
        5. Compiles the NHMG library
        6. Compiles the Tools-Roms package

        Parameters:
        -----------
        target: src
            the path where ROMS will be cloned and compiled
        """

        # TODO: Situation where environment variables like ROMS_ROOT are not set...
        # ... but repo already exists at local_path results in an error rather than a prompt
        _clone_and_checkout(
            source_repo=self.source_repo,
            local_path=target,
            checkout_target=self.checkout_target,
        )

        # Set environment variables for this session:

        os.environ["ROMS_ROOT"] = str(target)
        _CSTAR_ENVIRONMENT_VARIABLES["ROMS_ROOT"] = os.environ["ROMS_ROOT"]
        os.environ.setdefault("PATH", "")
        os.environ["PATH"] += f":{target}/Tools-Roms/"
        _CSTAR_ENVIRONMENT_VARIABLES["PATH"] = os.environ["PATH"]

        # Set the configuration file to be read by __init__.py for future sessions:
        config_file_str = (
            f'    _CSTAR_ENVIRONMENT_VARIABLES["ROMS_ROOT"]="{target}"'
            + '\n    _CSTAR_ENVIRONMENT_VARIABLES.setdefault("PATH",os.environ.get("PATH",default=""))'
            + '\n    _CSTAR_ENVIRONMENT_VARIABLES["PATH"]+=":'
            + f'{target}/Tools-Roms"\n'
        )

        _write_to_config_file(config_file_str)

        # Distribute custom makefiles for ROMS
        self._base_model_adjustments()

        # Make things
        print("Compiling UCLA ROMS' NHMG library...")
        make_nhmg_result = subprocess.run(
            f"make nhmg COMPILER={_CSTAR_COMPILER}",
            cwd=str(target) + "/Work",
            capture_output=True,
            text=True,
            shell=True,
        )
        if make_nhmg_result.returncode != 0:
            raise RuntimeError(
                f"Error {make_nhmg_result.returncode} when compiling ROMS' NHMG library. STDERR stream: "
                + f"\n {make_nhmg_result.stderr}"
            )
        print("Compiling Tools-Roms package for UCLA ROMS...")
        make_tools_roms_result = subprocess.run(
            f"make COMPILER={_CSTAR_COMPILER}",
            cwd=str(target) + "/Tools-Roms",
            shell=True,
            capture_output=True,
            text=True,
        )
        if make_tools_roms_result.returncode != 0:
            raise RuntimeError(
                f"Error {make_tools_roms_result.returncode} when compiling Tools-Roms. STDERR stream: "
                + f"\n {make_tools_roms_result.stderr}"
            )
        print(f"UCLA-ROMS is installed at {target}")
