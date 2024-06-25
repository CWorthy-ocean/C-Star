import os
import shutil
import subprocess
from abc import ABC, abstractmethod
from . import _CSTAR_ROOT, _CSTAR_COMPILER
from .utils import _get_hash_from_checkout_target

class BaseModel(ABC):
    """
    The model from which this component is derived (e.g. MARBL v0.45.0)

    Attributes
    -----------
    source_repo: str
        URL pointing to a git-controlled repository containing the base model source code
    checkout_target: str
        A tag, git hash, or other target to check out the source repo at the correct point in its history    
    checkout_hash: str
        The git hash associated with `checkout_target`
    repo_basename: str
        The basename of the repository, e.g. "repo" for "https://github.com/dev-team/repo.git

    Properties
    ----------
    default_source_repo: str
        The default value of `source_repo`
    default_checkout_target: str
        The default value of `checkout_target`    
    expected_env_var: str
        Environment variable pointing to the root of the base model
        indicating that the base model has been installed and configured on the local machine
        BaseModel.check() will look for this variable as a first check.
    
    Methods
    -------
    check()
        Perform a series of checks to ensure that the base model is configured on this machine
        in agreement with this BaseModel instance.
    get()
        Obtain and configure the base model on this machine if it is not already. check() will
        prompt the user to get() if checks fail.
    """

    

    def __init__(self, source_repo=None, checkout_target=None):
        """
        Initialize a BaseModel object manually from a source repository and checkout target.

        Parameters:
        -----------
        source_repo: str
            URL pointing to a git-controlled repository containing the base model source code
        checkout_target: str
            A tag, git hash, or other target to check out the source repo at the correct point in its history

        Returns:
        -------
        BaseModel
            An initialized BaseModel object
        """
        
        # TODO: Type check here
        self.source_repo = (
            source_repo if source_repo is not None else self.default_source_repo
        )
        self.checkout_target = (
            checkout_target
            if checkout_target is not None
            else self.default_checkout_target
        )
        self.checkout_hash = _get_hash_from_checkout_target(
            self.source_repo, self.checkout_target
        )
        self.repo_basename = os.path.basename(self.source_repo).replace(".git", "")

    @property
    @abstractmethod
    def name(self):
        """The name of the base model"""

    @property
    @abstractmethod
    def default_source_repo(self):
        """Default source repository, defined in subclasses, e.g. https://github.com/marbl-ecosys/MARBL.git"""

    @property
    @abstractmethod
    def default_checkout_target(self):
        """Default checkout target, defined in subclasses, e.g. marblv0.45.0"""

    @property
    @abstractmethod
    def expected_env_var(self):
        """environment variable associated with the base model, e.g. MARBL_ROOT"""

    @abstractmethod
    def _base_model_adjustments(self):
        """
        Perform any C-Star specific adjustments to the base model that would
        be needed after a clean checkout. 
        """
    
    def check(self):
        """
        Perform a series of checks to ensure that the base model is properly configured on this machine.

        The method proceeds as follows:
        1. Check `BaseModel.expected_env_var` is present in the environment
            (prompt installation of the base model if not)
        2. Check `BaseModel.expected_env_var` points to the correct remote repository
            (raise an EnvironmentError if not)
        3. Check the repository is checked out to the correct target
            (prompt checkout of the correct target if not)
        
        """

        # check 1: X_ROOT variable is in user's env
        env_var_exists = self.expected_env_var in os.environ

        # check 2: X_ROOT points to the correct repository
        if env_var_exists:
            local_root = os.environ[self.expected_env_var]
            env_var_repo_remote = subprocess.run(
                f"git -C {local_root} remote get-url origin",
                shell=True,
                capture_output=True,
                text=True,
            ).stdout.replace("\n", "")
            env_var_matches_repo = self.source_repo == env_var_repo_remote

            if not env_var_matches_repo:
                raise EnvironmentError(
                    "System environment variable "
                    + f"'{self.expected_env_var}' points to"
                    + "a github repository whose "
                    + f"remote: \n '{env_var_repo_remote}' \n"
                    + "does not match that expected by C-Star: \n"
                    + f"{self.source_repo}."
                    + "Your environment may be misconfigured."
                )
            else:
                # check 3: local basemodel repo HEAD matches correct checkout hash:
                head_hash = subprocess.run(
                    f"git -C {local_root} rev-parse HEAD",
                    shell=True,
                    capture_output=True,
                    text=True,
                ).stdout.replace("\n", "")
                head_hash_matches_checkout_hash = head_hash == self.checkout_hash
                if head_hash_matches_checkout_hash:
                    print(
                        "############################################################\n"
                        +f"C-STAR: {self.expected_env_var}, {os.environ[self.expected_env_var]} "
                        + f"points to the correct repo {self.source_repo} "
                        + f"at the correct hash {self.checkout_hash}. Proceeding"
                        +"\n############################################################" 
                    )
                else:
                    print(
                        "############################################################\n"
                        + f"C-STAR: {self.expected_env_var} points to the correct repo "
                        + f"{self.source_repo} but HEAD is at: \n"
                        + f"{head_hash}, rather than the hash associated with "
                        + f"checkout_target {self.checkout_target}:\n"
                        + f"{self.checkout_hash}"
                        + "\n############################################################"
                    )
                    while True:
                        yn = input("Would you like to checkout this target now?")                        
                        if yn.casefold() in ["y", "yes"]:
                            subprocess.run(
                                f"git -C {local_root} checkout {self.checkout_target}",
                                shell=True,
                            )
                            self._base_model_adjustments()
                            break
                        elif yn.casefold() in ["n","no"]:
                            raise EnvironmentError()
                        else:
                            print("invalid selection; enter 'y' or 'n'")
                            
        else:  # env_var_exists False (e.g. ROMS_ROOT not defined)
            ext_dir = _CSTAR_ROOT + "/externals/" + self.repo_basename
            print(
                "#######################################################\n"
                + f"C-STAR: {self.expected_env_var}"
                + " not found in current environment. \n"
                + "if this is your first time running a C-Star case that "
                + f"uses {self.name}, you will need to set it up.\n"
                + f"It is recommended that you install {self.name} in \n"
                + f"{ext_dir}"
                + "\n#######################################################"
            )
            while True:
                yn = input(
                    "Would you like to do this now? "
                    + "('y', 'n', or 'custom' to install at a custom path)\n"
            )
                if yn.casefold() in ["y", "yes", "ok"]:
                    self.get(ext_dir)
                    break
                elif yn.casefold in ['n','no']:
                    raise EnvironmentError()
                elif yn.casefold() == 'custom':
                    custom_path = input(
                        "Enter custom path for install:\n"
                    )
                    self.get(os.path.abspath(custom_path))
                    break
                else:
                    print("invalid selection; enter 'y','n',or 'custom'")
                


@abstractmethod
def get(self, target):
    """clone the basemodel code to your local machine"""


class ROMSBaseModel(BaseModel):
    """
    An implementation of the BaseModel class for the UCLA Regional Ocean Modeling System
    
    This subclass sets unique values for BaseModel properties specific to ROMS, and overrides
    the get() method to compile ROMS-specific libraries.

    Methods:
    -------
    get()
        overrides BaseModel.get() to clone the UCLA ROMS repository, set environment, and compile libraries
    """

    
    @property
    def name(self):
        return "ROMS"

    @property
    def default_source_repo(self):
        return "https://github.com/CESR-lab/ucla-roms.git"

    @property
    def default_checkout_target(self):
        return "main"

    @property
    def expected_env_var(self):
        return "ROMS_ROOT"

    def _base_model_adjustments(self):
        """
        Perform C-Star specific adjustments to stock ROMS code.
        
        In particular, this method replaces the default Makefiles with machine-agnostic
        versions, allowing C-Star to be used with ROMS across multiple different computing systems.
        """
        shutil.copytree(
            _CSTAR_ROOT + "/additional_files/ROMS_Makefiles/",
            os.environ[self.expected_env_var],
            dirs_exist_ok=True,
        )

    def get(self, target):
        """
        Clone ROMS code to local machine, set environment, compile libraries

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
        
        
        # Get the REPO and checkout the right version
        subprocess.run(f"git clone {self.source_repo} {target}", shell=True)
        subprocess.run(f"git -C {target} checkout {self.checkout_target}", shell=True)

        # Set environment variables for this session:
        os.environ["ROMS_ROOT"] = target
        os.environ["PATH"] += ":" + target + "/Tools-Roms/"

        # Set the configuration file to be read by __init__.py for future sessions:
        #                            === TODO ===
        # config_file_str=\
        # f'os.environ["ROMS_ROOT"]="{target}"\nos.environ["PATH"]+=":"+\
        # "{target}/Tools-Roms"\n'
        # if not os.path.exists(_CONFIG_FILE):
        # config_file_str='import os\n'+config_file_str
        # with open(_CONFIG_FILE,'w') as f:
        # f.write(config_file_str)

        # Distribute custom makefiles for ROMS
        self._base_model_adjustments()

        # Make things
        subprocess.run(
            f"make nhmg COMPILER={_CSTAR_COMPILER}", cwd=target + "/Work", shell=True
        )
        subprocess.run(
            f"make COMPILER={_CSTAR_COMPILER}", cwd=target + "/Tools-Roms", shell=True
        )


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
    def name(self):
        return "MARBL"

    @property
    def default_source_repo(self):
        return "https://github.com/marbl-ecosys/MARBL.git"

    @property
    def default_checkout_target(self):
        return "v0.45.0"

    @property
    def expected_env_var(self):
        return "MARBL_ROOT"

    def _base_model_adjustments(self):
        pass

    def get(self,target):
        """
        Clone MARBL code to local machine, set environment, compile libraries

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
        
        # FIXME: duplicate code from ROMSBaseModel.get()
        subprocess.run(f"git clone {self.source_repo} {target}", shell=True)
        subprocess.run(f"git -C {target} checkout {self.checkout_target}", shell=True)

        # Set environment variables for this session:
        os.environ["MARBL_ROOT"] = target

        # Set the configuration file to be read by __init__.py for future sessions:
        #                              ===TODO===
        # config_file_str=f'os.environ["MARBL_ROOT"]="{target}"\n'
        # if not os.path.exists(_CONFIG_FILE):
        #    config_file_str='import os\n'+config_file_str
        # with open(_CONFIG_FILE,'w') as f:
        #        f.write(config_file_str)

        # Make things
        subprocess.run(
            f"make {_CSTAR_COMPILER} USEMPI=TRUE", cwd=target + "/src", shell=True
        )

