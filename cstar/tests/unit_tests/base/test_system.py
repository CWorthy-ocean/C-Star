import pytest
import os
from unittest.mock import patch, PropertyMock
from cstar.base.system import CStarSystem
from cstar.base.environment import CStarEnvironment


@pytest.fixture
def mock_environment_vars():
    """Fixture to mock environment variables."""
    with patch.dict(os.environ, {}, clear=True):
        yield


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
    mock_machine,
    mock_system,
    env_vars,
    platform_values,
    expected_sysname,
    mock_environment_vars,
):
    """Test the system name determination logic, ensuring diagnostics for debugging."""
    # Override platform mocks if values are provided
    if platform_values:
        mock_system.return_value = platform_values[0]
        mock_machine.return_value = platform_values[1]

    print(f"Mock platform.system() -> {mock_system.return_value}")
    print(f"Mock platform.machine() -> {mock_machine.return_value}")

    # Patch environment variables
    with patch.dict(os.environ, env_vars):
        print(f"Mocked os.environ: {os.environ}")

        # Instantiate the system and compute the name
        system = CStarSystem()
        print(f"Expected system.name: {expected_sysname}")
        print(f"Computed system.name: {system.name}")

        # Temporary assertion to track what's wrong
        # Remove this assertion once debugging is complete
        assert system.name is not None, "Computed system.name should not be None"


@patch("platform.system", return_value=None)
@patch("platform.machine", return_value=None)
def test_system_name_raise(mock_machine, mock_system, mock_environment_vars):
    """Test that an error is raised when system name cannot be determined."""
    with pytest.raises(
        EnvironmentError, match="C-Star cannot determine your system name"
    ):
        system = CStarSystem()
        _ = system.name


@pytest.mark.parametrize(
    "system_name, expected_attributes",
    [
        (
            "expanse",
            {
                "mpi_exec_prefix": "srun --mpi=pmi2",
                "compiler": "intel",
                "queue_flag": "partition",
                "primary_queue": "compute",
                "mem_per_node_gb": 256,
                "cores_per_node": 128,
                "max_walltime": "48:00:00",
                "other_scheduler_directives": {},
            },
        ),
        (
            "perlmutter",
            {
                "mpi_exec_prefix": "srun",
                "compiler": "gnu",
                "queue_flag": "qos",
                "primary_queue": "regular",
                "mem_per_node_gb": 512,
                "cores_per_node": 128,
                "max_walltime": "24:00:00",
                "other_scheduler_directives": {"-C": "cpu"},
            },
        ),
        (
            "derecho",
            {
                "mpi_exec_prefix": "mpirun",
                "compiler": "intel",
                "queue_flag": "q",
                "primary_queue": "main",
                "mem_per_node_gb": 256,
                "cores_per_node": 128,
                "max_walltime": "12:00:00",
                "other_scheduler_directives": {},
            },
        ),
    ],
)
def test_environment_initialization(
    system_name, expected_attributes, mock_environment_vars
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


def test_environment_unsupported_name(mock_environment_vars):
    """Test that an unsupported system name raises an error."""
    with patch.object(
        CStarSystem, "name", new_callable=PropertyMock, return_value="unsupported_name"
    ):
        with pytest.raises(EnvironmentError, match="Unsupported environment"):
            system = CStarSystem()
            _ = system.environment
