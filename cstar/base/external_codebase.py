import os
from abc import ABC, abstractmethod
from pathlib import Path

from cstar.base.gitutils import (
    _checkout,
    _get_repo_head_hash,
    _get_repo_remote,
)
from cstar.base.log import LoggingMixin
from cstar.io.source_data import SourceData
from cstar.io.staged_data import StagedRepository
from cstar.system.environment import CSTAR_USER_ENV_PATH
from cstar.system.manager import cstar_sysmgr


class ExternalCodeBase(ABC, LoggingMixin):
    """Abstract base class to manage external non-python dependencies of C-Star.

    Attributes
    -----------
    source: SourceData:
        Information about the external codebase source repository
    checkout_hash: str
        The git hash associated with `checkout_target`
    repo_basename: str
        The basename of the repository, e.g. "repo" for "https://github.com/dev-team/repo.git
    local_config_status: int
        A value corresponding to how the external codebase has been configured on the local machine
        The value of local_config_status may be interpreted as follows.

            - 0: The expected environment variable is present, points to the correct repository remote, and is checked out at the correct hash
            - 1: The expected environment variable is present but does not point to the correct repository remote (unresolvable)
            - 2: The expected environment variable is present, points to the correct repository remote, but is checked out at the wrong hash
            - 3: The expected environment variable is not present and it is assumed the external codebase is not installed locally
    expected_env_var: str
        Environment variable pointing to the root of the external codebase
        indicating that the external codebase has been installed and configured on the local machine.

    Methods
    -------
    get_local_config_status()
        Perform a series of checks to determine how the external codebase is configured on this machine
        relative to this ExternalCodeBase instance.
    handle_local_config_status()
        Perform actions depending on the output of get_local_config_status()
    get()
        Obtain and configure the external codebase on this machine if it is not already.
        handle_local_config_status() prompts the user to run get() if the model cannot be found.
    """

    def __init__(
        self, source_repo: str | None = None, checkout_target: str | None = None
    ):
        """Initialize a ExternalCodeBase object manually from a source repository and
        checkout target.

        Parameters:
        -----------
        source_repo: str, Optional:
            URL pointing to a git-controlled repository containing the source code.
            A default value will be set by the subclass if not provided
        checkout_target: str, Optional
            A tag, git hash, or other target to check out the source repo at the correct point in its history.
            A default value will be set by the subclass if not provided

        Returns:
        -------
        ExternalCodeBase
            An initialized ExternalCodeBase object
        """
        if not source_repo:
            source_repo = self._default_source_repo
        if not checkout_target:
            checkout_target = self._default_checkout_target
        self._source = SourceData(location=source_repo, identifier=checkout_target)

        self._working_copy: StagedRepository | None = None  # updated by self.get()

    def __str__(self) -> str:
        base_str = f"{self.__class__.__name__}"
        base_str += "\n" + "-" * len(base_str)
        base_str += f"\nsource_repo : {self.source_repo}"
        if self.source_repo == self.default_source_repo:
            base_str += " (default)"

        base_str += f"\ncheckout_target : {self.checkout_target}"
        if self.checkout_target == self.default_checkout_target:
            base_str += " (default)"

        base_str += f"\nlocal_config_status: {self.local_config_status} "
        match self.local_config_status:
            case 0:
                base_str += f"(Environment variable {self.expected_env_var} is present, points to the correct repository remote, and is checked out at the correct hash)"
            case 1:
                base_str += f"(Environment variable {self.expected_env_var} is present but does not point to the correct repository remote [unresolvable])"
            case 2:
                base_str += f"(Environment variable {self.expected_env_var} is present, points to the correct repository remote, but is checked out at the wrong hash)"
            case 3:
                base_str += f"(Environment variable {self.expected_env_var} is not present and it is assumed the external codebase is not installed locally)"

        return base_str

    def __repr__(self) -> str:
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\nsource_repo = {self.source_repo!r},"
        repr_str += f"\ncheckout_target = {self.checkout_target!r}"
        repr_str += "\n)"
        repr_str += "\nState: <"
        repr_str += f"local_config_status = {self.local_config_status}>"
        return repr_str

    def to_dict(self) -> dict:
        return {
            "source_repo": self.source.location,
            "checkout_target": self.source.identifier,
        }

    @property
    def source(self) -> SourceData:
        return self._source

    @property
    @abstractmethod
    def _default_source_repo(self) -> str:
        """Default source repository, defined in subclasses, e.g. https://github.com/marbl-ecosys/MARBL.git"""

    @property
    @abstractmethod
    def _default_checkout_target(self) -> str:
        """Default checkout target, defined in subclasses, e.g. marblv0.45.0."""

    @property
    @abstractmethod
    def expected_env_var(self) -> str:
        """Environment variable associated with the external codebase, e.g.
        MARBL_ROOT.
        """

    @property
    def local_config_status(self) -> int:
        """Perform a series of checks to ensure that the external codebase is properly
        configured on this machine.

        The method proceeds as follows:
        1. Check `ExternalCodeBase.expected_env_var` is present in the environment
        2. Check `ExternalCodeBase.expected_env_var` points to the correct remote repository
        3. Check the repository is checked out to the correct target

        Returns:
        -------
        local_config_status: int
           The value of local_config_status may be interpreted as follows.
           0: The expected environment variable is present, points to the correct repository remote, and is checked out at the correct hash
           1: The expected environment variable is present but does not point to the correct repository remote (unresolvable)
           2: The expected environment variable is present, points to the correct repository remote, but is checked out at the wrong hash
           3: The expected environment variable is not present and it is assumed the external codebase is not installed locally
        """
        # check 1: X_ROOT variable is in user's env
        env_var_exists = (
            self.expected_env_var
            in cstar_sysmgr.environment.environment_variables.keys()
        )

        # check 2: X_ROOT points to the correct repository
        if env_var_exists:
            local_root = Path(
                cstar_sysmgr.environment.environment_variables[self.expected_env_var]
            )
            env_var_repo_remote = _get_repo_remote(local_root)
            env_var_matches_repo = self.source_repo == env_var_repo_remote
            if not env_var_matches_repo:
                return 1
            else:
                # check 3: local codebase repo HEAD matches correct checkout hash:
                head_hash = _get_repo_head_hash(local_root)
                head_hash_matches_checkout_hash = head_hash == self.checkout_hash
                if head_hash_matches_checkout_hash:
                    return 0
                else:
                    return 2

        else:  # env_var_exists False (e.g. ROMS_ROOT not defined)
            return 3

    @property
    def is_setup(self) -> bool:
        return True if self.local_config_status == 0 else False

    def handle_config_status(self) -> None:
        """Perform actions depending on the output of
        ExternalCodeBase.get_local_config_status()

        The config_status attribute should be set by the get_local_config_status method

        The method then proceeds as follows:
        config_status =

           - 0: The expected environment variable is present, points to the correct repository remote, and is checked out at the correct hash
               -> do nothing
           - 1: The expected environment variable is present but does not point to the correct repository remote (unresolvable)
               -> raise an EnvironmentError
           - 2: The expected environment variable is present, points to the correct repository remote, but is checked out at the wrong hash
               -> prompt checkout of correct hash
           - 3: The expected environment variable is not present and it is assumed the external codebase is not installed locally
               -> prompt installation of the external codebase
        """
        local_root = Path(
            cstar_sysmgr.environment.environment_variables.get(
                self.expected_env_var, ""
            )
        )

        interactive = bool(int(os.environ.get("CSTAR_INTERACTIVE", "1")))

        match self.local_config_status:
            case 0:
                self.log.info(
                    f"✅ {self.__class__.__name__} correctly configured. Nothing to be done"
                )
                return
            case 1:
                env_var_repo_remote = _get_repo_remote(local_root)

                raise OSError(
                    "System environment variable "
                    f"'{self.expected_env_var}' points to "
                    "a github repository whose "
                    f"remote: \n '{env_var_repo_remote}' \n"
                    "does not match that expected by C-Star: \n"
                    f"{self.source_repo}."
                    "Your environment may be misconfigured."
                )
            case 2:
                head_hash = _get_repo_head_hash(local_root)
                print(
                    "############################################################\n"
                    f"C-STAR: {self.expected_env_var} points to the correct repo "
                    f"{self.source_repo} but HEAD is at: \n"
                    f"{head_hash}, rather than the hash associated with "
                    f"checkout_target {self.checkout_target}:\n"
                    f"{self.checkout_hash}\n"
                    "############################################################"
                )
                while True:
                    yn = "y"
                    if interactive:
                        yn = input("Would you like to checkout this target now?")

                    if yn.casefold() in ["y", "yes"]:
                        try:
                            _checkout(
                                self.source_repo, local_root, self.checkout_target
                            )
                        except Exception as ex:
                            print(ex)
                        return
                    elif yn.casefold() in ["n", "no"]:
                        raise OSError()
                    else:
                        print("invalid selection; enter 'y' or 'n'")
            case 3:
                ext_dir = (
                    cstar_sysmgr.environment.package_root
                    / f"externals/{self.repo_basename}"
                )
                print(
                    "#######################################################\n"
                    f"C-STAR: {self.expected_env_var}"
                    " not found in current cstar_sysmgr.environment. \n"
                    "if this is your first time running C-Star with "
                    f"an instance of {self.__class__.__name__}, "
                    "you will need to set it up.\n"
                    "It is recommended that you install this external codebase in \n"
                    f"{ext_dir}\n"
                    f"This will also modify your `{CSTAR_USER_ENV_PATH}` file.\n"
                    "#######################################################"
                )
                while True:
                    if not ext_dir.exists():
                        ext_dir.mkdir(parents=True)

                    yn = "y"
                    if interactive:
                        yn = input(
                            "Would you like to do this now? "
                            "('y', 'n', or 'custom' to install at a custom path)\n"
                        )
                    if yn.casefold() in ["y", "yes", "ok"]:
                        self.get(ext_dir)
                        break
                    elif yn.casefold() in ["n", "no"]:
                        raise OSError()
                    elif yn.casefold() == "custom":
                        custom_path = input("Enter custom path for install:\n")
                        self.get(Path(custom_path).resolve())
                        break
                    else:
                        print("invalid selection; enter 'y','n',or 'custom'")

    @property
    def working_copy(self) -> StagedRepository:
        return self._working_copy

    def get(self, target_dir: Path | None = None) -> None:
        """Retrieve and stage this ExternalCodeBase"""
        if self.working_copy:
            raise ValueError(
                f"ExternalCodeBase is already staged at {self.working_copy.path}. "
                "Consider ExternalCodeBase.working_copy.reset or ExternalCodeBase.uninstall"
            )

        if not target_dir:
            target_dir = Path(
                cstar_sysmgr.environment.package_root
                / f"externals/{self.source.basename.replace('.git', '')}"
            )
            self.log.info(
                f"⚠️  No target_dir provided to ExternalCodeBase.get, defaulting to {target_dir}"
            )

        staged_repo = self.source.stage(target_dir=target_dir)
        self._working_copy = staged_repo

    @property
    @abstractmethod
    def is_configured(self) -> bool:
        """Returns True if this ExternalCodeBase exists locally and is correctly configured"""
        # Checks currently done:
        # Does X_ROOT point to a repo whose remote matches the source?
        # Does X_ROOT point to the correct repo, checked out at the wrong target?
        # Is X_ROOT defined at all?
        #
        # New checks: self.working_copy.changed_from_source() covers repo checks
        # other subclasses should implement their own checks.

    def configure(self) -> None:
        """Configure (set environment, compile, etc.) the external codebase on your local machine."""
        if not self.working_copy:
            raise FileNotFoundError(
                "Cannot configure ExternalCodeBase without a local copy. Call ExternalCodeBase.get()"
            )
        if self.is_configured:
            self.log.info(
                f"✅ {self.__class__.__name__} correctly configured. Nothing to be done"
            )
            return
        self._configure()

    @abstractmethod
    def _configure(self) -> None:
        """Must be implemented by subclasses"""

    def remove(self):
        raise NotImplementedError("TODO")
        # self.log.info(f"Removing local ExternalCodeBase from {self.working_copy.path}")
        # if self.working_copy:
        #     self.working_copy.unstage()
        #     self.working_copy = None
        # if self.
