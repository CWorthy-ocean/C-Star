import os
from collections.abc import Generator
from typing import Optional
from unittest import mock
from unittest.mock import patch

import pytest

from cstar.system.environment import CStarEnvironment
from cstar.system.manager import CStarSystemManager, SystemName
from cstar.system.scheduler import PBSScheduler, SlurmScheduler


@pytest.fixture
def mock_environment_vars() -> Generator[None, None, None]:
    """Fixture to mock environment variables."""
    with patch.dict(os.environ, {}, clear=True):
        yield


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

    @pytest.mark.parametrize(
        ("env_vars", "platform_values", "expected_sysname"),
        [
            ({"LMOD_SYSHOST": "expanse"}, None, SystemName.EXPANSE),
            ({"LMOD_SYSTEM_NAME": "perlmutter"}, None, SystemName.PERLMUTTER),
            ({}, ("Linux", "x86_64"), SystemName.LINUX_X86_64),
        ],
    )
    @patch("platform.system", return_value=None)  # Default mock for platform.system
    @patch("platform.machine", return_value=None)  # Default mock for platform.machine
    def test_system_name(  # noqa: PLR0913
        self,
        mock_machine,
        mock_system,
        env_vars: dict[str, str],
        platform_values: Optional[tuple[str, str]],
        expected_sysname: SystemName,
        mock_environment_vars: dict[str, str],  # noqa: ARG002
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
            assert system.name == SystemName(expected_sysname)

    @patch("platform.system", return_value=None)
    @patch("platform.machine", return_value=None)
    def test_system_name_raise(self, mock_machine, mock_system, mock_environment_vars):
        """Test that an error is raised when system name cannot be determined."""
        with pytest.raises(
            EnvironmentError, match="C-Star cannot determine your system name"
        ):
            CStarSystemManager()

    def test_unsupported_name(self, mock_environment_vars) -> None:
        """Test that an unsupported system name raises a ValueError."""
        with (
            patch.object(
                CStarSystemManager,
                "_get_system_name",
                new_callable=mock.MagicMock,
                return_value="unsupported_name",
            ),
            pytest.raises(
                ValueError, match="'unsupported_name' is not a valid SystemName"
            ),
        ):
            CStarSystemManager()


class TestEnvironmentProperty:
    """Tests for the `environment` property of the `CStarSystemManager` class.

    Tests
    -----
    test_environment_initialization
        Validates that the `CStarEnvironment` object is correctly initialized with
        the expected attributes based on the system name.
    """

    @pytest.mark.parametrize(
        ("system_name", "expected_attributes"),
        [
            (
                SystemName.EXPANSE.value,
                {
                    "mpi_exec_prefix": "srun --mpi=pmi2",
                    "compiler": "intel",
                },
            ),
            (
                SystemName.PERLMUTTER.value,
                {
                    "mpi_exec_prefix": "srun",
                    "compiler": "gnu",
                },
            ),
            (
                SystemName.DERECHO.value,
                {
                    "mpi_exec_prefix": "mpirun",
                    "compiler": "intel",
                },
            ),
            (
                SystemName.DARWIN_ARM64.value,
                {
                    "mpi_exec_prefix": "mpirun",
                    "compiler": "gnu",
                },
            ),
        ],
    )
    def test_environment_initialization(
        self,
        system_name: str,
        expected_attributes: dict[str, str],
        mock_environment_vars: dict[str, str],  # noqa: ARG002
    ) -> None:
        """Verify that environment attributes are correctly initialized."""
        with patch.object(
            CStarSystemManager,
            "_get_system_name",
            new_callable=mock.MagicMock,
            return_value=system_name,
        ):
            system = CStarSystemManager()
            environment = system.environment

            # Compare the actual and expected attributes of the environment.
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

    @pytest.mark.parametrize(
        ("system_name", "expected_scheduler_type", "expected_queue_names"),
        [
            (
                "perlmutter",
                SlurmScheduler,
                {"regular", "shared", "debug"},
            ),
            (
                "derecho",
                PBSScheduler,
                {"main", "preempt", "develop"},
            ),
        ],
    )
    def test_scheduler_initialization(
        self,
        system_name: str,
        expected_scheduler_type: type,
        expected_queue_names: set[str],
        mock_environment_vars: dict[str, str],  # noqa: ARG002
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
            "_get_system_name",
            new_callable=mock.MagicMock,
            return_value=system_name,
        ):
            system = CStarSystemManager()
            scheduler = system.scheduler

            # Verify that the scheduler is of the expected type
            assert isinstance(scheduler, expected_scheduler_type)

            # Verify that the queue names match
            assert hasattr(scheduler, "queues")
            actual_queue_names = {q.name for q in scheduler.queues}
            assert actual_queue_names == expected_queue_names

    def test_no_scheduler(
        self,
        mock_environment_vars: dict[str, str],  # noqa: ARG002
    ) -> None:
        """Verify that a supported system without a scheduler returns 'None'."""
        with patch.object(
            CStarSystemManager,
            "_get_system_name",
            new_callable=mock.MagicMock,
            return_value="darwin_arm64",
        ):
            system = CStarSystemManager()
            assert system.scheduler is None

    def test_scheduler_caching(
        self,
        mock_environment_vars: dict[str, str],  # noqa: ARG002
    ) -> None:
        """Verify that the scheduler property is cached after first access."""
        with patch.object(
            CStarSystemManager,
            "_get_system_name",
            new_callable=mock.MagicMock,
            return_value="perlmutter",
        ):
            system = CStarSystemManager()
            first_scheduler = system.scheduler
            second_scheduler = system.scheduler

            # Verify that the scheduler is cached
            assert first_scheduler is second_scheduler
