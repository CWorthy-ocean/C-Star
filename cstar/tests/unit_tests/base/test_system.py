import pytest
import os
from unittest.mock import patch, PropertyMock
from cstar.base.system import CStarSystem
from cstar.base.scheduler import SlurmScheduler, PBSScheduler
from cstar.base.environment import CStarEnvironment


@pytest.fixture
def mock_environment_vars():
    """Fixture to mock environment variables."""
    with patch.dict(os.environ, {}, clear=True):
        yield


class TestSystemName:
    """Tests for the `name` property of the CStarSystem class."""

    @pytest.mark.parametrize(
        "env_vars, platform_values, expected_sysname",
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
        mock_machine,
        mock_system,
        env_vars,
        platform_values,
        expected_sysname,
        mock_environment_vars,
    ):
        """Test the system name determination logic."""
        # Override platform mocks if values are provided
        if platform_values:
            mock_system.return_value = platform_values[0]
            mock_machine.return_value = platform_values[1]

        # Patch environment variables
        with patch.dict(os.environ, env_vars):
            # Instantiate the system and compute the name
            system = CStarSystem()
            assert system.name == expected_sysname

    @patch("platform.system", return_value=None)
    @patch("platform.machine", return_value=None)
    def test_system_name_raise(self, mock_machine, mock_system, mock_environment_vars):
        """Test that an error is raised when system name cannot be determined."""
        with pytest.raises(
            EnvironmentError, match="C-Star cannot determine your system name"
        ):
            system = CStarSystem()
            _ = system.name


class TestEnvironment:
    """Tests for the `environment` property of the CStarSystem class."""

    @pytest.mark.parametrize(
        "system_name, expected_attributes",
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
        self, system_name, expected_attributes, mock_environment_vars
    ):
        """Test that the environment is initialized with the correct attributes."""
        with patch.object(
            CStarSystem, "name", new_callable=PropertyMock, return_value=system_name
        ):
            system = CStarSystem()
            environment = system.environment

            # Verify that the environment object matches the expected attributes
            assert isinstance(environment, CStarEnvironment)
            for attr, value in expected_attributes.items():
                assert getattr(environment, attr) == value

    def test_environment_unsupported_name(self, mock_environment_vars):
        """Test that an unsupported system name raises an error."""
        with patch.object(
            CStarSystem,
            "name",
            new_callable=PropertyMock,
            return_value="unsupported_name",
        ):
            with pytest.raises(EnvironmentError, match="Unsupported environment"):
                system = CStarSystem()
                _ = system.environment


##


class TestScheduler:
    """Tests for the `scheduler` property of the CStarSystem class."""

    @pytest.mark.parametrize(
        "system_name, expected_scheduler_type, expected_queue_names",
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
        system_name,
        expected_scheduler_type,
        expected_queue_names,
        mock_environment_vars,
    ):
        """Test that the scheduler is initialized with the correct type and queue
        names."""
        with patch.object(
            CStarSystem, "name", new_callable=PropertyMock, return_value=system_name
        ):
            system = CStarSystem()
            scheduler = system.scheduler

            # Verify that the scheduler is of the expected type
            assert isinstance(scheduler, expected_scheduler_type)

            # Verify that the queue names match
            assert set(q.name for q in scheduler.queues) == set(expected_queue_names)

    def test_scheduler_unsupported_name(self, mock_environment_vars):
        """Test that an unsupported system name results in no scheduler."""
        with patch.object(
            CStarSystem,
            "name",
            new_callable=PropertyMock,
            return_value="unsupported_name",
        ):
            system = CStarSystem()
            assert system.scheduler is None

    def test_scheduler_caching(self, mock_environment_vars):
        """Test that the scheduler property is cached after first access."""
        with patch.object(
            CStarSystem, "name", new_callable=PropertyMock, return_value="perlmutter"
        ):
            system = CStarSystem()
            first_scheduler = system.scheduler
            second_scheduler = system.scheduler

            # Verify that the scheduler is cached
            assert first_scheduler is second_scheduler
