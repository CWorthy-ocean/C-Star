from pathlib import Path

from cstar import Simulation
from cstar.base import ExternalCodeBase, InputDataset
from cstar.roms import ROMSInputDataset
from cstar.roms.runtime_settings import ROMSRuntimeSettingsSection


class MockExternalCodeBase(ExternalCodeBase):
    """A mock subclass of the `ExternalCodeBase` abstract base class used for testing
    purposes.
    """

    # def __init__(self, log: logging.Logger):
    #     super().__init__(None, None)
    #     self._log = log

    @property
    def expected_env_var(self):
        return "TEST_ROOT"

    @property
    def default_source_repo(self):
        return "https://github.com/test/repo.git"

    @property
    def default_checkout_target(self):
        return "test_target"

    def get(self, target: str | Path):
        self.log.info(f"mock installing ExternalCodeBase at {target}")
        pass


class MockSection(ROMSRuntimeSettingsSection):
    """A simple ROMSRuntimeSettingsSection subclass for testing."""

    floats: list[float]
    paths: list[Path]
    others: list[str | int]
    floatval: float
    pathval: Path
    otherval: str


class EmptySection(ROMSRuntimeSettingsSection):
    """No actual values defined, just used to test some of the formatting/joining
    functions.
    """


class MockInputDataset(InputDataset):
    """Mock subclass of the InputDataset abstract base class.

    Since InputDataset is an abstract base class, this mock class is needed to allow
    instantiation for testing purposes. It inherits from InputDataset without adding any
    new behavior, serving only to allow tests to create and manipulate instances.
    """

    pass


class MockROMSInputDataset(ROMSInputDataset):
    """A minimal example subclass of the ROMSInputDataset abstract base class."""

    pass


class MockSimulation(Simulation):
    """Mock subclass of `Simulation` for testing purposes.

    This class provides a minimal implementation of `Simulation` that overrides
    abstract methods to allow for isolated unit testing without requiring actual
    simulation execution.

    Attributes
    ----------
    default_codebase : ExternalCodeBase
        Returns an instance of `MockExternalCodeBase`.

    Methods
    -------
    from_dict(simulation_dict, directory)
        Minimal implementation of abstract method
    from_blueprint(blueprint, directory)
        No-op implementation of abstract method
    to_blueprint(filename)
        No-op implementation of abstract method
    setup()
        No-op implementation of abstract method
    build(rebuild)
        No-op implementation of abstract method
    pre_run()
        No-op implementation of abstract method
    run()
        No-op implementation of abstract method
    post_run()
        No-op implementation of abstract method
    """

    @property
    def default_codebase(self):
        return MockExternalCodeBase()

    @classmethod
    def from_dict(cls, simulation_dict, directory):
        return cls(**simulation_dict)

    @classmethod
    def from_blueprint(cls, blueprint, directory):
        pass

    def to_blueprint(self, filename):
        pass

    def setup(self):
        pass

    def build(self, rebuild=False):
        pass

    def pre_run(self):
        pass

    def run(self):
        pass

    def post_run(self):
        pass
