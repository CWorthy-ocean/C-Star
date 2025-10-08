import os
from abc import ABC, abstractmethod
from pathlib import Path

from cstar.base.gitutils import (
    _checkout,
    _get_hash_from_checkout_target,
    _get_repo_head_hash,
    _get_repo_remote,
)
from cstar.base.log import LoggingMixin
from cstar.system.manager import cstar_sysmgr


class ExternalCodeBase(ABC, LoggingMixin):
    """Abstract base class to manage external non-python dependencies of C-Star.

    Attributes
    -----------
    source_repo: str
        URL pointing to a git-controlled repository containing the source code
    checkout_target: str
        A tag, git hash, or other target to check out the source repo at the correct point in its history
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
    default_source_repo: str
        The default value of `source_repo`
    default_checkout_target: str
        The default value of `checkout_target`
    expected_env_var: str
        Environment variable pointing to the root of the external codebase
        indicating that the external codebase has been installed and configured on the local machine.

    Methods
    -------
    local_config_status()
        Perform a series of checks to determine how the external codebase is configured on this machine
        relative to this ExternalCodeBase instance.
    handle_local_config_status()
        Perform actions depending on the output of local_config_status()
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
        source_repo: str
            URL pointing to a git-controlled repository containing the external codebase source code
        checkout_target: str
            A tag, git hash, or other target to check out the source repo at the correct point in its history

        Returns:
        -------
        ExternalCodeBase
            An initialized ExternalCodeBase object
        """
        self._source_repo = source_repo
        self._checkout_target = checkout_target

    def __str__(self) -> str:
        base_str = f"{self.__class__.__name__}"
        base_str += "\n" + "-" * len(base_str)
        base_str += f"\nsource_repo : {self.source_repo}"
        if self.source_repo == self.default_source_repo:
            base_str += " (default)"

        base_str += f"\ncheckout_target : {self.checkout_target}"
        if self.checkout_target != self.checkout_hash:
            base_str += f" (corresponding to hash {self.checkout_hash})"
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

    @property
    def source_repo(self) -> str:
        return (
            self._source_repo
            if self._source_repo is not None
            else self.default_source_repo
        )

    @property
    def checkout_target(self) -> str:
        return (
            self._checkout_target
            if self._checkout_target is not None
            else self.default_checkout_target
        )

    @property
    def repo_basename(self) -> str:
        return Path(self.source_repo).name.replace(".git", "")

    @property
    def checkout_hash(self) -> str:
        """Get the hash associated with the checkout target."""
        return _get_hash_from_checkout_target(self.source_repo, self.checkout_target)

    @property
    @abstractmethod
    def default_source_repo(self) -> str:
        """Default source repository, defined in subclasses, e.g. https://github.com/marbl-ecosys/MARBL.git"""

    @property
    @abstractmethod
    def default_checkout_target(self) -> str:
        """Default checkout target, defined in subclasses, e.g. marblv0.45.0."""

    @property
    @abstractmethod
    def expected_env_var(self) -> str:
        """Environment variable associated with the external codebase, e.g.
        MARBL_ROOT.
        """

    @property
    def default_externals_root(self) -> Path:
        """The default path of the root directory where external codebases are stored.

        Returns
        -------
        Path
            The path to the directory.
        """
        pkg_relative_path = f"externals/{self.repo_basename}"
        return cstar_sysmgr.environment.package_root / pkg_relative_path

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
        env_var = os.environ.get(self.expected_env_var, "")

        # check 2: X_ROOT points to the correct repository
        if env_var:
            local_root = Path(env_var)
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
        ExternalCodeBase.local_config_status()

        The config_status attribute should be set by the local_config_status method

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
        local_root = Path(os.environ.get(self.expected_env_var, ""))

        interactive = os.environ.get("CSTAR_INTERACTIVE", "1") == "1"

        match self.local_config_status:
            case 0:
                self.log.info(
                    f"✅ {self.__class__.__name__} correctly configured. Nothing to be done"
                )
                return
            case 1:
                local_root.mkdir(parents=True, exist_ok=True)
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
                user_env_path = cstar_sysmgr.environment.user_env_path
                print(
                    "#######################################################\n"
                    f"C-STAR: {self.expected_env_var}"
                    " not found in current cstar_sysmgr.environment. \n"
                    "if this is your first time running C-Star with "
                    f"an instance of {self.__class__.__name__}, "
                    "you will need to set it up.\n"
                    "It is recommended that you install this external codebase in \n"
                    f"{self.default_externals_root}\n"
                    f"This will also modify your `{user_env_path}` file.\n"
                    "#######################################################"
                )
                while True:
                    if not self.default_externals_root.exists():
                        self.default_externals_root.mkdir(parents=True)

                    yn = "y"
                    if interactive:
                        yn = input(
                            "Would you like to do this now? "
                            "('y', 'n', or 'custom' to install at a custom path)\n"
                        )
                    if yn.casefold() in ["y", "yes", "ok"]:
                        self.get(self.default_externals_root)
                        break
                    elif yn.casefold() in ["n", "no"]:
                        raise OSError()
                    elif yn.casefold() == "custom":
                        custom_path = input("Enter custom path for install:\n")
                        self.get(Path(custom_path).resolve())
                        break
                    else:
                        print("invalid selection; enter 'y','n',or 'custom'")

    @abstractmethod
    def get(self, target: str | Path) -> None:
        """Clone the external codebase to your local machine."""
