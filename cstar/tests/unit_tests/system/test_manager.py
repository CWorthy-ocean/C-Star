import os
from collections.abc import Generator
from unittest.mock import Mock, PropertyMock, patch

import pytest

from cstar.system.environment import CStarEnvironment
from cstar.system.manager import CStarSystemManager
from cstar.system.scheduler import PBSScheduler, Scheduler, SlurmScheduler


@pytest.fixture
def mock_environment_vars() -> Generator[dict[str, str], None, None]:
    """Fixture to mock environment variables."""
    with patch.dict(os.environ, {}, clear=True) as _:
        yield _


@pytest.fixture
def mock_system() -> Generator[dict[str, str], None, None]:
    """Fixture to mock system name get."""
    with patch("platform.system", return_value=None) as _:
        yield _


@pytest.fixture
def mock_machine() -> Generator[dict[str, str], None, None]:
    """Fixture to mock machine name get."""
    with patch("platform.machine", return_value=None) as _:
        yield _


class TestSystemName:
    """Tests for the `name` property of the `CStarSystemManager` class.

    Tests
    -----
    test_system_name
        Validates that the system name is correctly determined based on environment
        variables and platform properties.
    test_system_name_raise
        Ensures that an `EnvironmentError` is raised if the system name cannot be
        determined.
    test_unsupported_name
        Confirms that a `ValueError` is raised when an unsupported system name
        is encountered.
    """

    @pytest.mark.usefixtures("mock_environment_vars")
    @pytest.mark.parametrize(
        ("env_vars", "platform_values", "expected_sysname"),
        [
            ({"LMOD_SYSHOST": "expanse"}, None, "expanse"),
            ({"LMOD_SYSTEM_NAME": "perlmutter"}, None, "perlmutter"),
            ({}, ("Linux", "x86_64"), "linux_x86_64"),
        ],
    )
    @patch("platform.system", return_value=None)  # Default mock for platform.system
    @patch("platform.machine", return_value=None)  # Default mock for platform.machine
    def test_system_name(
        self,
        mock_machine: Mock,
        mock_system: Mock,
        env_vars: dict[str, str],
        platform_values: list[str] | None,
        expected_sysname: str,
    ) -> None:
        """Validates that the system name is correctly determined based on environment
        variables and platform properties.

        This test uses parameterization to evaluate multiple combinations of environment
        variables and platform values.

        Mocks
        -----
        platform.system
            Returns a mocked system name when platform detection is used.
        platform.machine
            Returns a mocked machine architecture when platform detection is used.
        os.environ
            Environment variables are patched to simulate specific configurations.

        Asserts
        -------
        - That the system name matches the expected value for various input scenarios.
        """
        # Override platform mocks if values are provided
        if platform_values:
            mock_system.return_value = platform_values[0]
            mock_machine.return_value = platform_values[1]

        # Patch environment variables
        with patch.dict(os.environ, env_vars):
            # Instantiate the system and compute the name
            system = CStarSystemManager()
            assert system.name == expected_sysname

    @pytest.mark.usefixtures("mock_environment_vars", "mock_machine", "mock_system")
    def test_system_name_raise(self) -> None:
        """Test that an error is raised when system name cannot be determined."""
        system = CStarSystemManager()
        with pytest.raises(
            EnvironmentError, match="C-Star cannot determine your system name"
        ):
            _ = system.name

    @pytest.mark.usefixtures("mock_environment_vars")
    def test_unsupported_name(self) -> None:
        """Test that an unsupported system name raises a ValueError."""
        system = CStarSystemManager()

        with (
            patch.object(
                CStarSystemManager,
                "name",
                new_callable=PropertyMock,
                return_value="unsupported_name",
            ),
            pytest.raises(
                ValueError, match="'unsupported_name' is not a valid SystemName"
            ),
        ):
            _ = system.environment


class TestEnvironmentProperty:
    """Tests for the `environment` property of the `CStarSystemManager` class.

    Tests
    -----
    test_environment_initialization
        Validates that the `CStarEnvironment` object is correctly initialized with
        the expected attributes based on the system name.
    """

    @pytest.mark.usefixtures("mock_environment_vars")
    @pytest.mark.parametrize(
        ("system_name", "expected_attributes"),
        [
            (
                "expanse",
                {
                    "mpi_exec_prefix": "srun --mpi=pmi2",
                    "compiler": "intel",
                },
            ),
            (
                "perlmutter",
                {
                    "mpi_exec_prefix": "srun",
                    "compiler": "gnu",
                },
            ),
            (
                "derecho",
                {
                    "mpi_exec_prefix": "mpirun",
                    "compiler": "intel",
                },
            ),
        ],
    )
    def test_environment_initialization(
        self, system_name: str, expected_attributes: dict[str, str]
    ) -> None:
        """Test that the environment is initialized with the correct attributes."""
        with patch.object(
            CStarSystemManager,
            "name",
            new_callable=PropertyMock,
            return_value=system_name,
        ):
            system = CStarSystemManager()
            environment = system.environment

            # Verify that the environment object matches the expected attributes
            assert isinstance(environment, CStarEnvironment)
            for attr, value in expected_attributes.items():
                assert getattr(environment, attr) == value


##


class TestSchedulerProperty:
    """Tests for the `scheduler` property of the `CStarSystemManager` class.

    Tests
    -----
    test_scheduler_initialization
        Validates that the scheduler is initialized with the correct type and queue names
        based on the system name.
    test_no_scheduler
        Ensures that the scheduler property is `None` for supported systems without a scheduler.
    test_scheduler_caching
        Confirms that the scheduler property is cached after the first access.
    """

    @pytest.mark.usefixtures("mock_environment_vars")
    @pytest.mark.parametrize(
        ("system_name", "expected_scheduler_type", "expected_queue_names"),
        [
            (
                "perlmutter",
                SlurmScheduler,
                ["regular", "shared", "debug"],
            ),
            (
                "derecho",
                PBSScheduler,
                ["main", "preempt", "develop"],
            ),
        ],
    )
    def test_scheduler_initialization(
        self,
        system_name: str,
        expected_scheduler_type: type[Scheduler],
        expected_queue_names: list[str],
    ) -> None:
        """Validates that the scheduler is initialized with the correct type and queue
        names based on the system name.

        This test uses parameterization to evaluate multiple system names and their
        corresponding scheduler types and queue names.

        Mocks
        -----
        CStarSystemManager.name
            Returns a mocked system name to simulate different environments.
        os.environ
            Environment variables are patched to simulate a clean environment.

        Asserts
        -------
        - That the scheduler is an instance of the expected type.
        - That the queue names of the scheduler match the expected names.
        """
        with patch.object(
            CStarSystemManager,
            "name",
            new_callable=PropertyMock,
            return_value=system_name,
        ):
            system = CStarSystemManager()
            scheduler: Scheduler | None = system.scheduler

            # Verify that the scheduler is of the expected type
            assert isinstance(scheduler, expected_scheduler_type)

            assert scheduler is not None
            assert scheduler.queues is not None
            # Verify that the queue names match
            assert {q.name for q in scheduler.queues} == set(expected_queue_names)

    @pytest.mark.usefixtures("mock_environment_vars")
    def test_no_scheduler(self) -> None:
        """Asserts that a supported system without a scheduler returns 'None'."""
        with patch.object(
            CStarSystemManager,
            "name",
            new_callable=PropertyMock,
            return_value="darwin_arm64",
        ):
            system = CStarSystemManager()
            assert system.scheduler is None

    @pytest.mark.usefixtures("mock_environment_vars")
    def test_scheduler_caching(self) -> None:
        """Asserts that the scheduler property is cached after first access."""
        with patch.object(
            CStarSystemManager,
            "name",
            new_callable=PropertyMock,
            return_value="perlmutter",
        ):
            system = CStarSystemManager()
            first_scheduler = system.scheduler
            second_scheduler = system.scheduler

            # Verify that the scheduler is cached
            assert first_scheduler is second_scheduler
