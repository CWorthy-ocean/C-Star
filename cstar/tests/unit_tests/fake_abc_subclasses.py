from pathlib import Path

from cstar import Simulation
from cstar.base import ExternalCodeBase, InputDataset
from cstar.roms import ROMSInputDataset
from cstar.roms.runtime_settings import ROMSRuntimeSettingsSection


class FakeExternalCodeBase(ExternalCodeBase):
    """A minimal subclass of the `ExternalCodeBase` abstract base class used for testing
    purposes, without mocking any logic.

    Abstract methods are defined with simple no-op or user-specified return values.
    """

    _configured: bool = False

    def __init__(
        self,
        source_repo: str | None = None,
        checkout_target: str | None = None,
        configured: bool = False,
    ):
        self._configured = configured
        super().__init__()

    @property
    def root_env_var(self):
        return "TEST_ROOT"

    @property
    def _default_source_repo(self):
        return "https://github.com/test/repo.git"

    @property
    def _default_checkout_target(self):
        return "test_target"

    def _configure(self) -> None:
        return

    @property
    def is_configured(self):
        return self._configured


class FakeROMSRuntimeSettingsSection(ROMSRuntimeSettingsSection):
    """A simple ROMSRuntimeSettingsSection subclass for testing."""

    floats: list[float]
    paths: list[Path]
    others: list[str | int]
    floatval: float
    pathval: Path
    otherval: str


class FakeROMSRuntimeSettingsSectionEmpty(ROMSRuntimeSettingsSection):
    """No actual values defined, just used to test some of the formatting/joining
    functions.
    """

    pass


class FakeInputDataset(InputDataset):
    """Fake subclass of the InputDataset abstract base class.

    Since InputDataset is an abstract base class, this mock class is needed to allow
    instantiation for testing purposes. It inherits from InputDataset without adding any
    new behavior, serving only to allow tests to create and manipulate instances.
    """

    pass


class FakeROMSInputDataset(ROMSInputDataset):
    """A subclass of the ROMSInputDataset abstract base class."""

    pass


class StubSimulation(Simulation):
    """Fake subclass of `Simulation` ABC for testing purposes.

    This class provides a minimal implementation of `Simulation` that overrides
    abstract methods to allow for isolated unit testing without requiring actual
    simulation execution.
    """

    @property
    def default_codebase(self):
        """Minimal implementation of abstract method."""
        return FakeExternalCodeBase()

    @classmethod
    def from_dict(cls, simulation_dict, directory):
        """Minimal implementation of abstract method."""
        return cls(**simulation_dict)

    @classmethod
    def from_blueprint(cls, blueprint, directory):
        """No-op implementation of abstract method."""
        pass

    def to_blueprint(self, filename):
        """No-op implementation of abstract method."""
        pass

    def setup(self):
        """No-op implementation of abstract method."""
        pass

    def build(self, rebuild=False):
        """No-op implementation of abstract method."""
        pass

    def pre_run(self):
        """No-op implementation of abstract method."""
        pass

    def run(self):
        """No-op implementation of abstract method."""
        pass

    def post_run(self):
        """No-op implementation of abstract method."""
        pass
