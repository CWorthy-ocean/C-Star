from abc import ABC, abstractmethod
from pathlib import Path

from cstar.base.log import LoggingMixin
from cstar.io.source_data import SourceData
from cstar.io.staged_data import StagedRepository
from cstar.system.manager import cstar_sysmgr


class ExternalCodeBase(ABC, LoggingMixin):
    """Abstract base class to manage external non-python dependencies of C-Star.

    Attributes
    -----------
    source: SourceData:
        Information about the external codebase source repository
    root_env_var: str
        Environment variable pointing to the root of the external codebase
    working_copy: StagedRepository
        Local clone of the codebase
    is_configured: bool
        True if the codebase has been retrieved and configured locally

    Methods
    -------
    get(target_dir: Path) -> StagedRepository:
        Clone the codebase locally at the intended checkout target. Updates `working_copy`
    configure():
        Perform any actions necessary to configure this codebase locally for use
    setup(target_dir: Path):
        Calls both `get()` and `configure()` in sequence
    """

    _working_copy: StagedRepository | None = None  # updated by self.get()

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

        if not self.is_configured:
            self._working_copy = None
        else:
            root_dir = str(
                cstar_sysmgr.environment.environment_variables.get(self.root_env_var)
            )
            self._working_copy = StagedRepository(
                source=self.source, path=Path(root_dir)
            )

    def __str__(self) -> str:
        base_str = f"{self.__class__.__name__}"
        base_str += "\n" + "-" * len(base_str)
        base_str += f"\nsource_repo : {self.source.location}"
        if self.source.location.lower() == self._default_source_repo.lower():
            base_str += " (default)"

        base_str += f"\ncheckout_target : {self.source.checkout_target}"
        if (
            str(self.source.checkout_target).lower()
            == self._default_checkout_target.lower()
        ):
            base_str += " (default)"

        return base_str

    def __repr__(self) -> str:
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\nsource_repo = {self.source.location!r},"
        repr_str += f"\ncheckout_target = {self.source.checkout_target!r}"
        repr_str += "\n)"
        return repr_str

    def to_dict(self) -> dict:
        """Return this ExternalCodeBase as a dictionary of kwargs to ExternalCodeBase.__init__"""
        return {
            "source_repo": self.source.location,
            "checkout_target": self.source.checkout_target,
        }

    @property
    def source(self) -> SourceData:
        """The SourceData instance associated with this ExternalCodeBase"""
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
    def root_env_var(self) -> str:
        """Environment variable pointing to the location of the codebase when configured, e.g.
        MARBL_ROOT
        """

    @property
    def working_copy(self) -> StagedRepository | None:
        """StagedRepository instance describing the local clone of this codebase (if it exists)"""
        return self._working_copy

    def get(self, target_dir: Path | None = None) -> None:
        """Retrieve and stage this ExternalCodeBase

        Parameters
        ----------
        target_dir: Path or None
            The directory to clone the codebase to on the local filesystem. Defaults to an internal
            directory in the C-Star package root.

        """
        if self.working_copy:
            self.log.info(
                f"ExternalCodeBase is already staged at {self.working_copy.path}. Skipping get() call"
            )
            return

        if not target_dir:
            target_dir = Path(
                cstar_sysmgr.environment.package_root
                / f"externals/{self.source.basename.replace('.git', '')}"
            )
            self.log.info(
                f"⚠️  No target_dir provided to ExternalCodeBase.get, defaulting to {target_dir}"
            )

        staged_repo = self.source.stage(target_dir=target_dir)
        assert isinstance(staged_repo, StagedRepository)
        self._working_copy = staged_repo

    @property
    @abstractmethod
    def is_configured(self) -> bool:
        """Returns True if this ExternalCodeBase exists locally and is correctly configured"""

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

    def setup(self, target_dir: Path | None = None) -> None:
        """Retrieve and configure this codebase in a single call"""
        self.get(target_dir)
        self.configure()
