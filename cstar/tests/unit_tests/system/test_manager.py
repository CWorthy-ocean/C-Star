import os
from collections.abc import Generator
from unittest import mock
from unittest.mock import patch

import pytest

from cstar.system.environment import CStarEnvironment, LmodEnvSettings
from cstar.system.manager import (
    CStarSystemManager,
    DerechoSystemContext,
    ExpanseSystemContext,
    LinuxSystemContext,
    MacOSSystemContext,
    PerlmutterSystemContext,
    SystemContext,
)


@pytest.fixture
def mock_environment_vars() -> Generator[None, None, None]:
    """Fixture to mock environment variables.

    Configures the environment to simulate execution on Perlmutter.

    """
    with patch.dict(
        os.environ,
        {
            LmodEnvSettings.variable("SYSHOST"): "",
            LmodEnvSettings.variable("SYSTEM_NAME"): "Perlmutter",
        },
        clear=True,
    ) as _:
        yield _


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
        ("system_ctx", "expected_attributes"),
        [
            (
                ExpanseSystemContext,
                {
                    "mpi_exec_prefix": "srun --mpi=pmi2",
                    "compiler": "intel",
                },
            ),
            (
                PerlmutterSystemContext,
                {
                    "mpi_exec_prefix": "srun",
                    "compiler": "gnu",
                },
            ),
            (
                DerechoSystemContext,
                {
                    "mpi_exec_prefix": "mpirun",
                    "compiler": "intel",
                },
            ),
            (
                MacOSSystemContext,
                {
                    "mpi_exec_prefix": "mpirun",
                    "compiler": "gnu",
                },
            ),
            (
                LinuxSystemContext,
                {
                    "mpi_exec_prefix": "mpirun",
                    "compiler": "gnu",
                },
            ),
        ],
    )
    def test_environment_initialization(
        self,
        system_ctx: type[SystemContext],
        expected_attributes: dict[str, str],
    ) -> None:
        """Verify that environment attributes are correctly initialized."""
        mock_get_sys_ctx = mock.MagicMock(return_value=system_ctx())

        with mock.patch("cstar.system.manager._get_system_context", mock_get_sys_ctx):
            system = CStarSystemManager()

        environment = system.environment

        # Compare the actual and expected attributes of the environment.
        assert isinstance(environment, CStarEnvironment)
        for attr, value in expected_attributes.items():
            assert getattr(environment, attr) == value


class TestSchedulerProperty:
    """Tests for the `scheduler` property of the `CStarSystemManager` class.

    Tests
    -----
    test_scheduler_caching
        Confirms that the scheduler property is cached after instantiation.
    """

    @pytest.mark.usefixtures("mock_environment_vars")
    def test_scheduler_caching(self) -> None:
        """Verify that the scheduler property is cached after first access."""
        system = CStarSystemManager()

        first_scheduler = system.scheduler
        second_scheduler = system.scheduler

        # Verify that the scheduler is cached
        assert first_scheduler is second_scheduler
